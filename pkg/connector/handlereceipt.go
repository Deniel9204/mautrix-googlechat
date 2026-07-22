package connector

// handlereceipt.go -- Matrix -> Google Chat outbound read receipts
// (HandleMatrixReadReceipt, M4 Task 4). Issues the mark_group_readstate RPC:
//
//   - id: the group id derived from the portal id.
//   - last_read_time: the read timestamp in microseconds, computed as
//     (timestamp_ms or now_ms) * 1000 -- a millisecond input scaled to
//     Google Chat's µs convention, with a "now" fallback for a missing
//     input.
//
// This method only marks GOOGLE CHAT as read, on a genuine Matrix read
// receipt event. The inbound half -- marking MATRIX as read in response to a
// Google Chat group_viewed -- is unrelated and lives in events.go's
// queueGroupViewed doc comment.
//
// bridgev2's own handleMatrixReadReceipt (mautrix-go
// bridgev2/portal.go:925-967) already does everything needed BEFORE calling
// into the network connector: it resolves the read Matrix event's target
// message from the DB and hands the result here as *bridgev2.MatrixReadReceipt.
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

// readTimeMicros picks the µs last_read_time to send for msg: the read
// timestamp in milliseconds multiplied by 1000 to reach Google Chat's µs
// convention.
//
// The read receipt's own Matrix timestamp is not read directly: bridgev2
// instead hands this method either the exact target message (ExactMessage)
// or, failing that, the receipt itself (Receipt). This chooses between them:
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
//     receipt's own timestamp (msg.Receipt.Timestamp, ms -> µs) -- the read
//     receipt's own Matrix timestamp, the only input available in this case.
//   - Every timestamp source above turns out to be the Go zero time.Time
//     (IsZero()): falls back to time.Now(), a defensive fallback for a
//     falsy/missing input. This matters here specifically because Go's zero
//     time.Time is year 1, not the Unix epoch -- TimeToMicros of an
//     unguarded zero time.Time would send a large NEGATIVE microsecond
//     value to mark_group_readstate instead of a "now" substitute.
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
// receipt sent in a portal room, building its fields:
//
//   - id: gchatmeow.PartsToGroupID(group.ID, group.IsDM), where group is
//     gcid.ParsePortalID(msg.Portal.ID) -- the same GroupId-oneof
//     derivation every other outbound call in this package uses
//     (handlematrix.go, handleedit.go, handleredact.go, handlereaction.go).
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
