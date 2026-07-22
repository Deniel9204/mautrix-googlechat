package matrixfmt

import (
	"context"
	"html"
	"strings"

	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// MentionResolver maps a Matrix user (identified by their MXID, as found in
// a mention pill's matrix.to href) to the Google Chat gaia id that should
// receive a MENTION annotation. ok is false when the MXID can't be
// resolved to a gaia id the bridge knows about (not yet seen, not a bridge
// user/ghost, etc).
//
// nil-safe: a nil MentionResolver behaves exactly like one that always
// returns ok=false. In both cases, Parse renders the pill's own display
// text as plain text (no "MENTION_ALL"/"@" mangling) and emits no MENTION
// annotation for it -- nothing is silently dropped, and nothing is falsely
// pinged. M3 Task 3 supplies the real resolver; this task only wires the
// seam.
type MentionResolver func(mxid id.UserID) (gaiaID string, ok bool)

// Parse converts Matrix message content (HTML formatted_body, or plain
// body for the @room special case below) into a Google Chat text_body plus
// the list of annotations describing its formatting.
func Parse(ctx context.Context, content *event.MessageEventContent, mention MentionResolver) (string, []*pb.Annotation) {
	formattedBody := content.FormattedBody
	// A message with no HTML formatting (or an HTML format flag but an empty
	// formatted_body) is returned completely unformatted -- UNLESS its plain
	// body contains a literal "@room", in which case an HTML body (the plain
	// body, HTML-escaped) is synthesized purely so the @room -> MENTION_ALL
	// substitution below still fires. This is the only way a plain-text-only
	// message can gain an annotation.
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

// mentionAdapter adapts this package's (mxid) -> (gaiaID, ok)
// MentionResolver into the (ctx, mxid) -> gaiaID shape HTMLParser wants
// internally (html.go's linkToString), collapsing "nil resolver" and "ok
// == false" into the same "" sentinel -- both mean "render plain text, no
// MENTION annotation".
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
