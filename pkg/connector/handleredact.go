package connector

// handleredact.go -- Matrix -> Google Chat outbound message deletion
// (HandleMatrixMessageRemove, M4 Task 2). Issues the delete_message RPC for
// a message redacted in a portal room (the reaction-target branch is M4
// Task 3's territory).
//
// bridgev2's own handleMatrixRedaction (mautrix-go
// bridgev2/portal.go:2479-2549) already does all the gating BEFORE calling
// into the network connector:
//
//   - resolves the redaction target from the DB by the redacted Matrix
//     event id (content.Redacts), dispatching to HandleMatrixMessageRemove
//     only when that target is a MESSAGE row (not a reaction row -- that's
//     ReactionHandlingNetworkAPI's HandleMatrixReactionRemove instead,
//     portal.go:2517-2531, M4 Task 3);
//   - deletes the target row from the DB itself, AFTER this method returns
//     nil (portal.go:2542-2544's `if redactionTargetMsg != nil {
//     err = portal.Bridge.DB.Message.Delete(...) }`) -- so a failed RPC
//     never leaves the DB row deleted, since bridgev2 only deletes on a nil
//     error from this method.
//
// This method therefore only needs to build+send the delete_message RPC,
// exactly parallel to HandleMatrixEdit's edit_message construction
// (handleedit.go, M4 Task 1):
//
//   - message_id.parent_id.topic_id.{group_id,topic_id}: group_id is
//     gcid.ParsePortalID(msg.Portal.ID), the same derivation every other
//     outbound call uses (handlematrix.go); topic_id reuses
//     threadRootTopicID(msg.TargetMessage) (handlematrix.go, M3 Task 6) --
//     the target's own stored MessageMetadata.TopicID, falling back to the
//     target's own message id when that's empty (the `thread_id or
//     message_id` fallback).
//   - message_id.message_id: gcid.ParseMessageID(msg.TargetMessage.ID). The
//     degenerate `message_id or thread_id` fallback for an empty message_id
//     cannot occur here: bridgev2 guarantees TargetMessage is a real,
//     non-nil DB row with a non-empty ID before ever calling this method
//     (portal.go:2503-2519's `redactionTargetMsg != nil` gate) -- the
//     identical guarantee HandleMatrixEdit's own doc comment already relies
//     on for EditTarget, and matches that method's own equally-omitted
//     fallback.
//
// delete_message has no text/annotations/message_info fields at all --
// unlike edit_message, the request carries nothing but the target's
// MessageId.
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
// message redacted in a portal room, building the delete_message request's
// MessageId (see this file's top-of-file doc comment for the field mapping).
func (c *GChatClient) HandleMatrixMessageRemove(ctx context.Context, msg *bridgev2.MatrixMessageRemove) error {
	// Relay mode only: refuse to delete a message another relayed user
	// sent (relayauth.go).
	if err := checkRelayOwnership(msg.OrigSender, msg.TargetMessage.SenderMXID); err != nil {
		return err
	}

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
