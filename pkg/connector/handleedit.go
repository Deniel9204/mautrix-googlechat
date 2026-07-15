package connector

// handleedit.go -- Matrix -> Google Chat outbound message edit
// (HandleMatrixEdit, M4 Task 1). Ports mautrix_googlechat/portal.py's
// handle_matrix_edit (portal.py:840-878) together with maugclib/client.py's
// edit_message (client.py:385-411).
//
// bridgev2's own handleMatrixEdit (mautrix-go bridgev2/portal.go:1494-1573)
// already does everything portal.py's OWN Python-side gating does BEFORE
// calling into the network connector:
//
//   - fetches EditTarget from the DB (portal.py:843's
//     `DBMessage.get_by_mxid(message.get_edit(), self.mxid)`, and drops the
//     edit with no call to this method at all if it's not found -- Python's
//     `if not target: ... return`, portal.py:844-852);
//   - rejects a non-text/notice edit via checkMessageContentCaps
//     (portal.py:854-862's `if message.msgtype != MessageType.TEXT`, gated
//     here on capabilities.go's GetCapabilities -- this bridge's
//     File/State/MemberActions maps never claim anything beyond plain text,
//     matching Python's own text-only restriction, so no separate msgtype
//     check belongs in THIS method);
//   - swaps content for content.NewContent (portal.go:1507-1508) so
//     edit.Content below is ALREADY the unwrapped m.new_content, needing no
//     special edit-vs-new-message handling here;
//   - persists edit.EditTarget back to the DB once this method returns
//     (portal.go:1566, "the central bridge module will save the
//     *database.Message after this function returns" --
//     EditHandlingNetworkAPI.HandleMatrixEdit's own doc comment), so this
//     method mutates edit.EditTarget.Metadata in place and does NOT call
//     DB.Message.Update itself.
//
// capabilities.go (this task) adds Edit: event.CapLevelFullySupported to
// gchatCapsFlat (and, via Clone(), gchatCapsThreaded) so bridgev2 even
// routes edits to this method at all -- Edit previously sat at its zero
// value (CapLevelUnsupported), which would make portal.handleMatrixEdit's
// own `!caps.Edit.Partial()` gate silently drop every Matrix edit before it
// ever reached here (mautrix-go bridgev2/portal.go:1530-1532).
//
// This method therefore only needs to build+send the edit_message RPC and
// record the new last_edit_time -- matching Python's handle_matrix_edit body
// from `text, annotations = ...` (portal.py:864) onward.
import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

var _ bridgev2.EditHandlingNetworkAPI = (*GChatClient)(nil)

// HandleMatrixEdit issues edit_message for a previously-bridged message
// edited in a portal room, matching handle_matrix_edit's body
// (portal.py:864-878) and edit_message's own request construction
// (client.py:385-411) field-by-field:
//
//   - message_id.parent_id.topic_id.{group_id,topic_id}: group_id is
//     gcid.ParsePortalID(edit.Portal.ID), the same derivation every other
//     outbound call uses (handlematrix.go); topic_id reuses
//     threadRootTopicID(edit.EditTarget) (handlematrix.go, M3 Task 6) --
//     the target's own stored MessageMetadata.TopicID, falling back to the
//     target's own message id when that's empty. This is exactly Python's
//     `thread_id or message_id` (client.py:400), where Python's thread_id
//     argument IS target.gc_parent_id (portal.py:868) -- the same value
//     this bridge keeps in MessageMetadata.TopicID.
//   - message_id.message_id: gcid.ParseMessageID(edit.EditTarget.ID) --
//     Python's message_id parameter (target.gcid, portal.py:870).
//   - text_body + annotations: c.msgConverter().FromMatrix(ctx,
//     edit.Content, resolve), the SAME M3 outbound formatting path
//     (matrixfmt.Parse via the real newOutboundMentionResolver)
//     HandleMatrixMessage uses (handlematrix.go) -- Python's `text,
//     annotations = await fmt.matrix_to_googlechat(message)`
//     (portal.py:864).
//   - message_info.accept_format_annotations=true, unconditionally -- the
//     SAME field edit_message always sets (client.py:407-409); unlike
//     send_message, edit_message never sets message_info.reply_to at all
//     (client.py:394-410 has no reply_to key), so it stays nil here too --
//     an edit cannot change what a message is a reply to.
//
// On success, edit.EditTarget.Metadata's LastEditTime is bumped to the
// server's own resp.message.last_edit_time -- Python's
// `self._edit_dedup[target.gcid] = resp.message.last_edit_time`
// (portal.py:874), just persisted on the message row (dbmeta.go's
// MessageMetadata) instead of a process-local dict, so a later inbound echo
// of this exact edit (queueMessageEdit, events.go) correctly dedups against
// it even across a bridge restart. A failed RPC leaves LastEditTime
// untouched (matching Python: the assignment at portal.py:874 is inside the
// `try` block, never reached on an exception).
func (c *GChatClient) HandleMatrixEdit(ctx context.Context, edit *bridgev2.MatrixEdit) error {
	group, err := gcid.ParsePortalID(edit.Portal.ID)
	if err != nil {
		return fmt.Errorf("googlechat: %w", err)
	}

	messageID := gcid.ParseMessageID(edit.EditTarget.ID)
	// ok is unused: edit.EditTarget is never nil here (bridgev2's own
	// handleMatrixEdit already checked editTarget == nil before ever
	// calling this method, mautrix-go bridgev2/portal.go:1540-1542).
	topicID, _ := threadRootTopicID(edit.EditTarget)

	resolve := newOutboundMentionResolver(ctx, edit.Portal)
	text, annotations := c.msgConverter().FromMatrix(ctx, edit.Content, resolve)

	req := &pb.EditMessageRequest{
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
		TextBody:    proto.String(text),
		Annotations: annotations,
		MessageInfo: &pb.MessageInfo{
			AcceptFormatAnnotations: proto.Bool(true),
		},
	}

	send := c.editMessageFn
	if send == nil {
		conn := c.getConn()
		if conn == nil {
			return fmt.Errorf("googlechat: not connected")
		}
		send = conn.EditMessage
	}

	resp, err := send(ctx, req)
	if err != nil {
		return fmt.Errorf("googlechat: edit_message failed: %w", err)
	}

	lastEditTime := resp.GetMessage().GetLastEditTime()
	if meta, ok := edit.EditTarget.Metadata.(*MessageMetadata); ok && meta != nil {
		meta.LastEditTime = lastEditTime
	} else {
		edit.EditTarget.Metadata = &MessageMetadata{LastEditTime: lastEditTime}
	}

	return nil
}
