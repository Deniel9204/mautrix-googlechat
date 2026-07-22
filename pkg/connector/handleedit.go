package connector

// handleedit.go -- Matrix -> Google Chat outbound message edit
// (HandleMatrixEdit, M4 Task 1). Issues the edit_message RPC for a message
// edited in a portal room.
//
// bridgev2's own handleMatrixEdit (mautrix-go bridgev2/portal.go:1494-1573)
// already does all the gating BEFORE calling into the network connector:
//
//   - fetches EditTarget from the DB, and drops the edit with no call to
//     this method at all if it's not found;
//   - checks the edit against checkMessageContentCaps (mautrix-go
//     bridgev2/portal.go:1108-1146) -- but that check whitelists
//     MsgText/MsgNotice/MsgEmote ALL THREE with "no checks for now". This
//     connector is STRICTER for edits than for brand new sends: an edit's
//     new content must be literal TEXT, even though a brand new send accepts
//     TEXT or NOTICE (HandleMatrixMessage). bridgev2's generic cap check does
//     NOT enforce this asymmetry, so this method re-checks edit.Content.MsgType
//     itself, below, rather than relying on checkMessageContentCaps (a
//     gchat-port-auditor finding on an earlier revision of this file, which
//     had incorrectly assumed the generic cap check already covered this);
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
// This method therefore only needs to reject non-TEXT edits, then build+send
// the edit_message RPC and record the new last_edit_time.
import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

var _ bridgev2.EditHandlingNetworkAPI = (*GChatClient)(nil)

// HandleMatrixEdit issues edit_message for a previously-bridged message
// edited in a portal room, building the request field-by-field:
//
//   - message_id.parent_id.topic_id.{group_id,topic_id}: group_id is
//     gcid.ParsePortalID(edit.Portal.ID), the same derivation every other
//     outbound call uses (handlematrix.go); topic_id reuses
//     threadRootTopicID(edit.EditTarget) (handlematrix.go, M3 Task 6) --
//     the target's own stored MessageMetadata.TopicID, falling back to the
//     target's own message id when that's empty (a `thread_id or message_id`
//     fallback, where the thread id is the target's own stored topic id --
//     the same value this bridge keeps in MessageMetadata.TopicID).
//   - message_id.message_id: gcid.ParseMessageID(edit.EditTarget.ID).
//   - text_body + annotations: c.msgConverter().FromMatrix(ctx,
//     edit.Content, resolve), the SAME M3 outbound formatting path
//     (matrixfmt.Parse via the real newOutboundMentionResolver)
//     HandleMatrixMessage uses (handlematrix.go).
//   - message_info.accept_format_annotations=true, unconditionally (required
//     for outgoing formatting to render); unlike a brand new send,
//     edit_message never sets message_info.reply_to at all, so it stays nil
//     here too -- an edit cannot change what a message is a reply to.
//
// On success, edit.EditTarget.Metadata's LastEditTime is bumped to the
// server's own resp.message.last_edit_time, persisted on the message row
// (dbmeta.go's MessageMetadata) so a later inbound echo of this exact edit
// (queueMessageEdit, events.go) correctly dedups against it even across a
// bridge restart. A failed RPC leaves LastEditTime untouched (there is
// nothing to dedup against since the edit never reached the server).
func (c *GChatClient) HandleMatrixEdit(ctx context.Context, edit *bridgev2.MatrixEdit) error {
	// The "we don't support non-text edits yet" gate -- stricter than
	// HandleMatrixMessage's own TEXT-or-NOTICE acceptance for brand new
	// sends. bridgev2.ErrUnsupportedMessageType is the same sentinel
	// HandleMatrixMessage already uses for its own msgtype gate
	// (handlematrix.go), so this reports identically to Matrix (a
	// failed-to-send status on the edit event) rather than silently
	// no-op'ing.
	if edit.Content.MsgType != event.MsgText {
		return bridgev2.ErrUnsupportedMessageType
	}

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
