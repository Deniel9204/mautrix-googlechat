package connector

import (
	"context"
	"fmt"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv"
)

// convertMessageToMatrix wraps conv.ToMatrix and stamps a fresh MessageMetadata
// and a cloned m.mentions (from the mentions gchatfmt rendered) onto every part.
// A nil media bridges no attachments.
func convertMessageToMatrix(conv *msgconv.MessageConverter, media mediaFetcher) func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, msg *pb.Message) (*bridgev2.ConvertedMessage, error) {
	return func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, msg *pb.Message) (*bridgev2.ConvertedMessage, error) {
		resolve := newInboundMentionResolver(portal)
		threadsOnly := false
		if portal != nil {
			if meta, ok := portal.Metadata.(*PortalMetadata); ok && meta != nil {
				threadsOnly = meta.ThreadsOnly
			}
		}
		cm, parsed := conv.ToMatrix(ctx, msg, threadsOnly, resolve)
		ts := msg.GetCreateTime()
		// Stamped on every part so a later Matrix reply can find the topic.
		topicID := msg.GetId().GetParentId().GetTopicId().GetTopicId()
		mentions := mentionsFromParsed(parsed)
		for _, part := range cm.Parts {
			part.DBMetadata = &MessageMetadata{TimestampMicro: ts, TopicID: topicID}
			if mentions != nil {
				part.Content.Mentions = cloneMentions(mentions)
			}
		}

		// Attachment parts must not inherit m.mentions, or users get pinged twice.
		attachmentParts := convertAttachmentsToMatrix(ctx, portal, intent, msg, media)
		for _, part := range attachmentParts {
			part.DBMetadata = &MessageMetadata{TimestampMicro: ts, TopicID: topicID}
		}
		cm.Parts = append(cm.Parts, attachmentParts...)

		return cm, nil
	}
}

// convertEditToMatrix returns the ConvertEditFunc for MESSAGE_UPDATED. It dedups
// on the text part's stored LastEditTime and ignores edits with no text part or
// no text_body. Must find the text part by PartID "", never existing[0]: part
// order is not guaranteed (no ORDER BY). Mentions are left unset so edits don't re-ping.
func convertEditToMatrix(conv *msgconv.MessageConverter) func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, existing []*database.Message, msg *pb.Message) (*bridgev2.ConvertedEdit, error) {
	return func(ctx context.Context, portal *bridgev2.Portal, _ bridgev2.MatrixAPI, existing []*database.Message, msg *pb.Message) (*bridgev2.ConvertedEdit, error) {
		// Find the text part by PartID, never by position.
		var target *database.Message
		for _, part := range existing {
			if part.PartID == gcid.TextPartID {
				target = part
				break
			}
		}
		if target == nil {
			return nil, fmt.Errorf("%w: googlechat edit of non-text message (no text part to edit)", bridgev2.ErrIgnoringRemoteEvent)
		}

		editTS := msg.GetLastEditTime()
		if editTS == 0 {
			editTS = msg.GetLastUpdateTime()
		}
		if meta, ok := target.Metadata.(*MessageMetadata); ok && meta != nil && meta.LastEditTime >= editTS {
			return nil, fmt.Errorf("%w: duplicate/stale googlechat edit (edit_ts=%d, stored_last_edit_time=%d)", bridgev2.ErrIgnoringRemoteEvent, editTS, meta.LastEditTime)
		}

		resolve := newInboundMentionResolver(portal)
		threadsOnly := false
		if portal != nil {
			if meta, ok := portal.Metadata.(*PortalMetadata); ok && meta != nil {
				threadsOnly = meta.ThreadsOnly
			}
		}
		cm, _ := conv.ToMatrix(ctx, msg, threadsOnly, resolve)
		if len(cm.Parts) == 0 {
			// The edit removed all text; ignore it.
			return nil, fmt.Errorf("%w: googlechat edit has no text body", bridgev2.ErrIgnoringRemoteEvent)
		}

		editPart := cm.Parts[0].ToEditPart(target)

		if meta, ok := target.Metadata.(*MessageMetadata); ok && meta != nil {
			meta.LastEditTime = editTS
		} else {
			target.Metadata = &MessageMetadata{LastEditTime: editTS}
		}

		return &bridgev2.ConvertedEdit{ModifiedParts: []*bridgev2.ConvertedEditPart{editPart}}, nil
	}
}
