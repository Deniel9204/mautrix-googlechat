package connector

// handlematrix.go -- Matrix -> Google Chat outbound message send
// (HandleMatrixMessage). Ports mautrix_googlechat/portal.py's
// handle_matrix_message + _handle_matrix_text (portal.py:880-931,1051-1079)
// together with maugclib/client.py's send_message (client.py:413-475),
// restricted to M2's plain-text scope: every send goes out as a brand-new
// top-level message via create_topic (Python's `if thread_id: ... else:
// CreateTopicRequest` branch, client.py:441-472, always takes the else arm
// here since M2 never computes a thread_id). Thread-aware create_message
// routing (a reply into an existing topic) is M3's job, once portal thread
// state exists to route on.
//
// Fidelity notes, ported field-by-field from send_message's else branch
// (client.py:459-472):
//
//   - request_header is deliberately NOT set on the request built here:
//     every gchatmeow.Client RPC wrapper stamps it itself
//     (pkg/gchatmeow/api.go's newRequestHeader, invoked from CreateTopic)
//     exactly like client.py stamps self.gc_request_header
//     (client.py:103-109, WEB client_type + client_version 2440378181258 +
//     spam_room_invites FULLY_SUPPORTED) on every request it builds.
//     sync.go's PaginatedWorldRequest follows the same "connector builds
//     business fields only, gchatmeow owns the header" split -- see its doc
//     comment and pkg/gchatmeow/api.go's "16 /api/* RPCs" comment.
//   - group_id: parsers.group_id_from_id(conversation_id) is
//     gchatmeow.PartsToGroupID(group.ID, group.IsDM) here, fed by
//     gcid.ParsePortalID(msg.Portal.ID) (the "dm:"/"space:" prefix half of
//     that same Python function lives in pkg/gcid, per ids.go's doc
//     comment).
//   - local_id: Python generates one random 64-bit id per send
//     (`local_id = f"mautrix-googlechat%{random.randint(0,
//     0xffffffffffffffff)}"`, portal.py:908, BEFORE dispatching to
//     _handle_matrix_text/_handle_matrix_media) and threads it through
//     self._local_dedup so the later inbound echo of this exact message can
//     be recognized and dropped before it round-trips back to Matrix as a
//     duplicate (portal.py:909,931, and the check at portal.py:1341). M2
//     Task 5 generated and sent that token (newLocalID, same prefix and
//     64-bit random range as Python's random.randint(0,
//     0xffffffffffffffff)); Task 6 (below) wires it into bridgev2's own
//     pending-transaction mechanism (msg.AddPendingToIgnore, via the
//     addPendingToIgnore/addPendingToIgnoreFn seam, client.go) as the Go
//     equivalent of self._local_dedup, registered before send() is called,
//     and matched against the echo's own local_id via
//     queueMessagePosted's TransactionID field (events.go) --
//     RemoteMessageWithTransactionID.GetTransactionID(), which bridgev2's
//     checkPendingMessage (portal.go) compares against the pending table.
//   - text_body: the plain-text body from msgConverter().FromMatrix
//     (pkg/msgconv/from-matrix.go), the M2 subset of fmt.matrix_to_googlechat
//     (portal.py:1059).
//   - history_v2=true is sent unconditionally, matching send_message's own
//     unconditional value (client.py:465) -- not gated on any config or
//     portal state.
//   - message_info.accept_format_annotations=true is likewise unconditional
//     (client.py:467-469, shared by both the create_topic and
//     create_message branches). message_info.reply_to is left unset: M2 has
//     no quote-reply support yet (M3), matching what send_message would
//     build with reply_to=None (client.py:423-437, reply_to_wrapped stays
//     nil in that case).
//   - annotations and retention_settings are left unset: annotation-based
//     formatting is M3 (send_message's `annotations` parameter, which
//     _handle_matrix_text always passes from matrix_to_googlechat, is empty
//     until M3 exists to populate it), and retention_settings is never set
//     by send_message at all (client.py:460-471 never mentions it).
//   - topic_and_message_id is never set by send_message either (proto field
//     7, unused by this call site in Python) and is likewise left unset
//     here.
//
// Known gap, tracked rather than fixed here (gchat-port-auditor, M2 Task 5
// review): portal.py's _handle_matrix_text also unconditionally calls
// `sender.client.mark_typing(self.gcid, typing=False)` immediately before
// every send (portal.py:1061-1065, best-effort/log-only on failure) to
// force-clear a lingering "typing..." indicator before the message lands.
// There is no equivalent call here -- this bridge has no
// TypingHandlingNetworkAPI/HandleMatrixTyping implementation yet for it to
// piggyback on, though the RPC primitive already exists
// (gchatmeow.Client.SetTypingState, pkg/gchatmeow/api.go). Pick this up
// alongside that future typing-support task rather than as a standalone
// fix: no data loss results from its absence (P2, not required for M2's
// plain-text send correctness).
import (
	"context"
	"fmt"
	"math/rand"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// HandleMatrixMessage sends a plain-text Matrix message to Google Chat as a
// brand-new top-level topic (create_topic; M2 scope, see file doc comment).
//
// Non-text/notice message types are rejected with
// bridgev2.ErrUnsupportedMessageType, matching handle_matrix_message's final
// else branch (`raise NotImplementedError(f"Unsupported msgtype
// {message.msgtype}")`, portal.py:923-924) for every msgtype that is
// neither TEXT/NOTICE nor is_media -- media sending itself is M5's job, so
// for now media messages are rejected the same way any other unhandled
// msgtype is (bridgev2's own checkMessageContentCaps, driven by
// GetCapabilities' empty File map, already rejects media before this method
// is ever reached in practice; this check is what actually mirrors Python's
// msgtype gate for anything that does reach here, e.g. m.emote).
func (c *GChatClient) HandleMatrixMessage(ctx context.Context, msg *bridgev2.MatrixMessage) (*bridgev2.MatrixMessageResponse, error) {
	if msg.Content.MsgType != event.MsgText && msg.Content.MsgType != event.MsgNotice {
		return nil, bridgev2.ErrUnsupportedMessageType
	}

	group, err := gcid.ParsePortalID(msg.Portal.ID)
	if err != nil {
		return nil, fmt.Errorf("googlechat: %w", err)
	}

	send := c.createTopicFn
	if send == nil {
		conn := c.getConn()
		if conn == nil {
			return nil, fmt.Errorf("googlechat: not connected")
		}
		send = conn.CreateTopic
	}

	text := c.msgConverter().FromMatrix(ctx, msg.Content)
	localID := newLocalID()
	txnID := networkid.TransactionID(localID)
	req := &pb.CreateTopicRequest{
		GroupId:   gchatmeow.PartsToGroupID(group.ID, group.IsDM),
		LocalId:   proto.String(localID),
		TextBody:  proto.String(text),
		HistoryV2: proto.Bool(true),
		MessageInfo: &pb.MessageInfo{
			AcceptFormatAnnotations: proto.Bool(true),
		},
	}

	// Register localID as a pending-to-ignore transaction BEFORE issuing the
	// RPC -- see addPendingToIgnoreFn's doc comment (client.go) for why the
	// ordering matters (Task 6: closes the race window the megabridge port
	// left open, docs/research/08b row 61). If the echo somehow reaches
	// queueMessagePosted (events.go) before send() below even returns, it is
	// already covered.
	c.addPendingToIgnore(msg, txnID)

	resp, err := send(ctx, req)
	if err != nil {
		// Undo the registration above via bridgev2's own purpose-built hook
		// (MatrixMessage.RemovePending, "should only be called if sending
		// the message fails" per its doc comment). Python's own local_id
		// leaks forever in self._local_dedup on this path (portal.py:925-931's
		// except branch never reaches the remove() call at portal.py:931) --
		// but that is an accidental limitation of Python's plain set(), not a
		// behavior worth reproducing: bridgev2 hands connectors exactly the
		// cleanup call Python never had, and both bridges are equally
		// long-running daemons, so leaving this unpaired would be unbounded
		// growth in outgoingMessages across every failed send for the life
		// of the process (gchat-port-auditor, Task 6 review).
		c.removePending(msg, txnID)
		return nil, fmt.Errorf("googlechat: create_topic failed: %w", err)
	}

	// _get_send_response's CreateTopicResponse arm (portal.py:1047-1048):
	// gcid=resp.topic.id.topic_id, timestamp=resp.topic.create_time_usec.
	topic := resp.GetTopic()
	createTimeUsec := topic.GetCreateTimeUsec()
	return &bridgev2.MatrixMessageResponse{
		DB: &database.Message{
			ID:        gcid.MakeMessageID(topic.GetId().GetTopicId()),
			SenderID:  c.ownUserID(),
			Timestamp: gchatmeow.MicrosToTime(createTimeUsec),
			Metadata:  &MessageMetadata{TimestampMicro: createTimeUsec},
		},
		// RemovePending mirrors portal.py:931's
		// `self._local_dedup.remove(local_id)` on the success path: once this
		// response is saved, the pending entry registered above is no longer
		// needed.
		RemovePending: txnID,
	}, nil
}

// newLocalID generates one send's local_id/dedup token, matching
// portal.py's `f"mautrix-googlechat%{random.randint(0,
// 0xffffffffffffffff)}"` (portal.py:908): the literal prefix
// "mautrix-googlechat%" followed by a uniformly random 64-bit integer.
// math/rand (not crypto/rand) matches Python's own use of the non-secure
// `random` module here -- this token only needs to be practically unique
// for in-flight dedup, not unpredictable.
func newLocalID() string {
	return fmt.Sprintf("mautrix-googlechat%%%d", rand.Uint64())
}
