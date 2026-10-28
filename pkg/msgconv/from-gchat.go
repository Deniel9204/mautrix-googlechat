package msgconv

import (
	"context"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv/gchatfmt"
)

// ToMatrix converts a Google Chat message into a bridgev2.ConvertedMessage plus the
// mentions actually rendered. Replies (message_id != topic_id) always get a ThreadRoot;
// topic heads only when threadsOnly.
func (mc *MessageConverter) ToMatrix(ctx context.Context, msg *pb.Message, threadsOnly bool, mention gchatfmt.MentionResolver) (*bridgev2.ConvertedMessage, gchatfmt.ParsedMentions) {
	cm := &bridgev2.ConvertedMessage{
		Parts: make([]*bridgev2.ConvertedMessagePart, 0, 1),
	}

	topicID := msg.GetId().GetParentId().GetTopicId().GetTopicId()
	messageID := msg.GetId().GetMessageId()
	if topicID != "" && (topicID != messageID || threadsOnly) {
		threadRoot := networkid.MessageID(topicID)
		cm.ThreadRoot = &threadRoot
	}

	if replyToID := msg.GetReplyTo().GetId().GetMessageId(); replyToID != "" {
		cm.ReplyTo = &networkid.MessageOptionalPartID{MessageID: networkid.MessageID(replyToID)}
	}

	text := gchatfmt.AppendLinkAnnotations(msg.GetTextBody(), msg.GetAnnotations())
	// Card-only bot messages leave text_body empty, so check cards before treating it as empty.
	cardBody, cardHTML := gchatfmt.RenderCards(msg.GetAttachments())
	if text == "" && cardBody == "" {
		return cm, gchatfmt.ParsedMentions{}
	}

	var body, html string
	var mentions gchatfmt.ParsedMentions
	if text != "" {
		body, html, mentions = gchatfmt.Parse(ctx, text, msg.GetAnnotations(), mention)
	}
	if cardBody != "" {
		if body != "" {
			// Escape the plain text so appending the card HTML doesn't drop it.
			if html == "" {
				html = gchatfmt.EscapePlainToHTML(body)
			}
			body += "\n\n"
			html += "<br/><br/>"
		}
		body += cardBody
		html += cardHTML
	}

	content := &event.MessageEventContent{
		MsgType: event.MsgText,
		Body:    body,
	}
	if html != "" {
		content.Format = event.FormatHTML
		content.FormattedBody = html
	}

	// Empty PartID marks the first part; attachment parts are appended after it.
	cm.Parts = append(cm.Parts, &bridgev2.ConvertedMessagePart{
		Type:    event.EventMessage,
		Content: content,
	})
	return cm, mentions
}
