package connector

// handlereceipt.go -- Matrix -> Google Chat outbound read receipts
// (HandleMatrixReadReceipt, M4 Task 4). Ports mautrix_googlechat/user.py's
// mark_read (user.py:684-691):
//
//	async def mark_read(self, conversation_id: str, timestamp: int) -> None:
//	    await self.client.proto_mark_group_read_state(
//	        googlechat.MarkGroupReadstateRequest(
//	            request_header=self.client.gc_request_header,
//	            id=maugclib.parsers.group_id_from_id(conversation_id),
//	            last_read_time=int((timestamp or (time.time() * 1000)) * 1000),
//	        )
//	    )
//
// The only caller of THIS (outbound RPC) mark_read is matrix.py:106-113's
// handle_read_receipt (`await user.mark_read(portal.gcid, data.ts)`), fired
// on a genuine Matrix read receipt event. portal.py has two OTHER
// self.mark_read call sites (backfill.py:400, group_viewed at
// portal.py:556-557) that look identical by name but resolve to a
// DIFFERENT method -- Portal.mark_read (portal.py:1594-1598), which marks
// MATRIX as read (via a puppet/double-puppet intent), not Google Chat; see
// events.go's queueGroupViewed doc comment for that inbound half, which this
// file's HandleMatrixReadReceipt has no relationship to.
//
// bridgev2's own handleMatrixReadReceipt (mautrix-go
// bridgev2/portal.go:925-967) already does everything matrix.py's
// handle_read_receipt does BEFORE calling into the network connector:
// resolves the read Matrix event's target message from the DB (Python has
// no equivalent lookup -- data.ts is used directly, with no message
// resolution at all) and hands the result here as *bridgev2.MatrixReadReceipt.
// ReadReceiptHandlingNetworkAPI.HandleMatrixReadReceipt's own doc comment
// (networkinterface.go:658-662) requires this method to gracefully handle
// ExactMessage == nil (a receipt on a non-message event, or one bridgev2
// could not resolve a DB row for) -- readTimeMicros below handles that case
// explicitly rather than assuming ExactMessage is always set.
import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

var _ bridgev2.ReadReceiptHandlingNetworkAPI = (*GChatClient)(nil)

// readTimeMicros picks the µs last_read_time to send for msg, matching
// user.py:689's `int((timestamp or (time.time() * 1000)) * 1000)` -- Python
// multiplies its ms input by 1000 to reach Google Chat's µs convention.
//
// Python's single input (`timestamp`, ultimately data.ts, the read
// receipt's OWN Matrix timestamp) has no bridgev2 equivalent to read
// directly: bridgev2 instead hands this method either the exact target
// message (ExactMessage) or, failing that, the receipt itself (Receipt).
// This chooses between them:
//
//   - ExactMessage set: use the target's own stored microsecond create_time
//     (MessageMetadata.TimestampMicro, dbmeta.go) -- MORE precise than
//     deriving µs from ExactMessage.Timestamp, whose underlying value only
//     has millisecond resolution (bridgev2's message table stores Matrix
//     event timestamps, which are themselves ms-precision), and more
//     precise than the receipt's own timestamp, which is the reading
//     CLIENT's wall-clock, not the message's actual creation time on
//     Google Chat's own servers.
//   - ExactMessage set but its Metadata has no usable TimestampMicro (a
//     legacy pre-M3-Task-6 row, or an unexpected Metadata type -- the same
//     defensive scenario buildReplyTarget checks for, handlematrix.go):
//     fall back to gchatmeow.TimeToMicros(ExactMessage.Timestamp). Unlike
//     buildReplyTarget, a read receipt has no "just drop it" option (some
//     last_read_time must always be sent), so this still sends the best
//     available value rather than erroring out.
//   - ExactMessage == nil (the interface's own required tolerance): use the
//     receipt's own timestamp (msg.Receipt.Timestamp, ms -> µs) -- exactly
//     Python's data.ts input, the only case Python's own single-input
//     mark_read ever actually receives in practice.
//   - Every timestamp source above turns out to be the Go zero time.Time
//     (IsZero()): falls back to time.Now(), porting user.py:689's own
//     `timestamp or (time.time() * 1000)` defensive fallback for a
//     falsy/missing input. This matters here specifically because Go's zero
//     time.Time is year 1, not the Unix epoch -- TimeToMicros of an
//     unguarded zero time.Time would send a large NEGATIVE microsecond
//     value to mark_group_readstate instead of Python's "now" substitute.
func readTimeMicros(msg *bridgev2.MatrixReadReceipt) int64 {
	if msg.ExactMessage != nil {
		if meta, ok := msg.ExactMessage.Metadata.(*MessageMetadata); ok && meta != nil && meta.TimestampMicro != 0 {
			return meta.TimestampMicro
		}
		if ts := msg.ExactMessage.Timestamp; !ts.IsZero() {
			return gchatmeow.TimeToMicros(ts)
		}
	} else if ts := msg.Receipt.Timestamp; !ts.IsZero() {
		return gchatmeow.TimeToMicros(ts)
	}
	return gchatmeow.TimeToMicros(time.Now())
}

// HandleMatrixReadReceipt issues mark_group_readstate for a Matrix read
// receipt sent in a portal room, matching mark_read's body (user.py:684-691)
// field-by-field:
//
//   - id: gchatmeow.PartsToGroupID(group.ID, group.IsDM), where group is
//     gcid.ParsePortalID(msg.Portal.ID) -- Python's
//     maugclib.parsers.group_id_from_id(conversation_id), the same
//     GroupId-oneof derivation every other outbound call in this package
//     uses (handlematrix.go, handleedit.go, handleredact.go,
//     handlereaction.go).
//   - last_read_time: readTimeMicros(msg) above.
//
// request_header is deliberately NOT set here: every gchatmeow.Client RPC
// wrapper stamps it itself (pkg/gchatmeow/api.go's MarkGroupReadstate calls
// newRequestHeader), exactly like every other outbound RPC in this package
// (handleedit.go, handleredact.go, handlereaction.go all follow the same
// pattern -- see sendNewTopic's doc comment, handlematrix.go, for the
// "connector builds business fields only, gchatmeow owns the header" split).
func (c *GChatClient) HandleMatrixReadReceipt(ctx context.Context, msg *bridgev2.MatrixReadReceipt) error {
	group, err := gcid.ParsePortalID(msg.Portal.ID)
	if err != nil {
		return fmt.Errorf("googlechat: %w", err)
	}

	req := &pb.MarkGroupReadstateRequest{
		Id:           gchatmeow.PartsToGroupID(group.ID, group.IsDM),
		LastReadTime: proto.Int64(readTimeMicros(msg)),
	}

	send := c.markGroupReadstateFn
	if send == nil {
		conn := c.getConn()
		if conn == nil {
			return fmt.Errorf("googlechat: not connected")
		}
		send = conn.MarkGroupReadstate
	}

	if _, err := send(ctx, req); err != nil {
		return fmt.Errorf("googlechat: mark_group_readstate failed: %w", err)
	}
	return nil
}
