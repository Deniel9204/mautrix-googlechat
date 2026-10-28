package matrixfmt

import (
	"context"
	"html"
	"strings"

	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// MentionResolver maps a Matrix user to a Google Chat gaia id. A nil resolver or
// ok=false renders the pill as plain text.
type MentionResolver func(mxid id.UserID) (gaiaID string, ok bool)

// Parse converts Matrix message content into a Google Chat text_body and annotations.
func Parse(ctx context.Context, content *event.MessageEventContent, mention MentionResolver) (string, []*pb.Annotation) {
	formattedBody := content.FormattedBody
	// Plain bodies stay unformatted unless they contain "@room", which needs an annotation.
	if content.Format != event.FormatHTML || formattedBody == "" {
		if !strings.Contains(content.Body, mxRoomMention) {
			return content.Body, nil
		}
		formattedBody = html.EscapeString(content.Body)
	}

	parser := &HTMLParser{GetUIDFromMXID: mentionAdapter(mention)}
	parseCtx := NewContext(ctx)
	parseCtx.AllowedMentions = content.Mentions
	parsed := parser.Parse(formattedBody, parseCtx)
	if parsed == nil {
		return "", nil
	}

	var annotations []*pb.Annotation
	if len(parsed.Entities) > 0 {
		annotations = make([]*pb.Annotation, len(parsed.Entities))
		for i, ent := range parsed.Entities {
			annotations[i] = ent.Proto()
		}
	}
	return parsed.Text.String(), annotations
}

// mentionAdapter adapts MentionResolver to HTMLParser's shape; nil or !ok yields "".
func mentionAdapter(mention MentionResolver) func(context.Context, id.UserID) string {
	return func(_ context.Context, mxid id.UserID) string {
		if mention == nil {
			return ""
		}
		gaiaID, ok := mention(mxid)
		if !ok {
			return ""
		}
		return gaiaID
	}
}
