package connector

// handleredact.go -- Matrix -> Google Chat outbound message deletion
// (HandleMatrixMessageRemove, M4 Task 2). Ports mautrix_googlechat/portal.py's
// handle_matrix_redaction (portal.py:800-814, the message-target branch --
// the reaction-target branch at portal.py:816-829 is M4 Task 3's territory)
// together with maugclib/client.py's delete_message (client.py:367-383).
//
// bridgev2's own handleMatrixRedaction (mautrix-go
// bridgev2/portal.go:2479-2549) already does everything portal.py's OWN
// Python-side gating does BEFORE calling into the network connector:
//
//   - resolves the redaction target from the DB by the redacted Matrix
//     event id (content.Redacts) -- Python's own
//     `DBMessage.get_by_mxid(target_id, self.mxid)` (portal.py:803),
//     dispatching to HandleMatrixMessageRemove only when that target is a
//     MESSAGE row (not a reaction row -- that's ReactionHandlingNetworkAPI's
//     HandleMatrixReactionRemove instead, portal.go:2517-2531, M4 Task 3);
//   - deletes the target row from the DB itself, AFTER this method returns
//     nil (portal.go:2542-2544's `if redactionTargetMsg != nil {
//     err = portal.Bridge.DB.Message.Delete(...) }`) -- the ORDER differs
//     from Python (which deletes the DB row BEFORE issuing delete_message,
//     portal.py:804-809), but the net effect is the same: a failed RPC
//     never leaves the DB row deleted (bridgev2 only deletes on a nil
//     error from this method; Python's own delete-then-try-RPC ordering
//     means a failed RPC there DOES still leave the row deleted -- an
//     existing Python quirk this Go port does not need to reproduce, since
//     bridgev2's ordering is strictly safer and no task or spec calls for
//     matching it).
//
// This method therefore only needs to build+send the delete_message RPC --
// matching delete_message's own request construction (client.py:367-383)
// field-by-field, exactly parallel to HandleMatrixEdit's edit_message
// construction (handleedit.go, M4 Task 1):
//
//   - message_id.parent_id.topic_id.{group_id,topic_id}: group_id is
//     gcid.ParsePortalID(msg.Portal.ID), the same derivation every other
//     outbound call uses (handlematrix.go); topic_id reuses
//     threadRootTopicID(msg.TargetMessage) (handlematrix.go, M3 Task 6) --
//     the target's own stored MessageMetadata.TopicID, falling back to the
//     target's own message id when that's empty. This is exactly Python's
//     `thread_id or message_id` (client.py:377), where Python's thread_id
//     argument IS target.gc_parent_id (portal.py:808) -- the same value
//     this bridge keeps in MessageMetadata.TopicID.
//   - message_id.message_id: gcid.ParseMessageID(msg.TargetMessage.ID) --
//     Python's message_id parameter (target.gcid, portal.py:808). Python
//     also falls back to `message_id or thread_id` (client.py:380) for a
//     degenerate empty-message_id case that cannot occur here: bridgev2
//     guarantees TargetMessage is a real, non-nil DB row with a non-empty
//     ID before ever calling this method (portal.go:2503-2519's
//     `redactionTargetMsg != nil` gate) -- the identical guarantee
//     HandleMatrixEdit's own doc comment already relies on for
//     EditTarget, and matches that method's own equally-omitted fallback.
//
// delete_message has no text/annotations/message_info fields at all
// (client.py:367-383) -- unlike edit_message, the request carries nothing
// but the target's MessageId.
import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

var _ bridgev2.RedactionHandlingNetworkAPI = (*GChatClient)(nil)

// HandleMatrixMessageRemove issues delete_message for a previously-bridged
// message redacted in a portal room, matching handle_matrix_redaction's
// message-target branch (portal.py:800-814) and delete_message's own
// request construction (client.py:367-383).
func (c *GChatClient) HandleMatrixMessageRemove(ctx context.Context, msg *bridgev2.MatrixMessageRemove) error {
	group, err := gcid.ParsePortalID(msg.Portal.ID)
	if err != nil {
		return fmt.Errorf("googlechat: %w", err)
	}

	messageID := gcid.ParseMessageID(msg.TargetMessage.ID)
	// ok is unused: msg.TargetMessage is never nil here (bridgev2's own
	// handleMatrixRedaction already checked redactionTargetMsg != nil
	// before ever calling this method, mautrix-go
	// bridgev2/portal.go:2503-2519).
	topicID, _ := threadRootTopicID(msg.TargetMessage)

	req := &pb.DeleteMessageRequest{
		MessageId: &pb.MessageId{
			ParentId: &pb.MessageParentId{
				Parent: &pb.MessageParentId_TopicId{
					TopicId: &pb.TopicId{
						GroupId: gchatmeow.PartsToGroupID(group.ID, group.IsDM),
						TopicId: proto.String(topicID),
					},
				},
			},
			MessageId: proto.String(messageID),
		},
	}

	send := c.deleteMessageFn
	if send == nil {
		conn := c.getConn()
		if conn == nil {
			return fmt.Errorf("googlechat: not connected")
		}
		send = conn.DeleteMessage
	}

	if _, err := send(ctx, req); err != nil {
		return fmt.Errorf("googlechat: delete_message failed: %w", err)
	}
	return nil
}
