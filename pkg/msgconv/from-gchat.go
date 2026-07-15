package msgconv

import (
	"context"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv/gchatfmt"
)

// ToMatrix converts a Google Chat Message proto into a bridgev2.
// ConvertedMessage. This is a port of from_googlechat.py's
// googlechat_to_matrix (from_googlechat.py:29-57), now covering its full
// M3 scope: text_body becomes the part's plain Content.Body, and -- unlike
// M2, which ignored them entirely -- msg.Annotations drive
// gchatfmt.Parse (Task 1) to additionally produce an HTML
// Content.FormattedBody whenever any are present. mention is the
// gaiaID->Matrix-pill resolver gchatfmt.Parse needs to render mention pills
// (fix B2); callers pass the real one built by
// pkg/connector/mentions.go's newInboundMentionResolver (Task 3) via the
// connector adapter (msgconv_adapter.go, Task 4) -- msgconv itself stays
// portal/network-ignorant (see msgconv.go's package doc comment), so
// mention is accepted as a plain function value here, not a *bridgev2.Portal.
//
// Plain-text extraction: the message body is exactly msg.GetTextBody()
// (proto field 10, TextBody in the generated Go struct). Python builds
// TextMessageEventContent(body=add_surrogate(evt.text_body)) and later,
// when there is no formatted_body, sets content.body =
// del_surrogate(content.body) -- i.e. round-trips text_body through UTF-16
// surrogate padding and back unchanged, which is a no-op for the plain
// Body value itself. Here text_body is taken verbatim as UTF-8: no
// surrogate encode/decode of Body happens in this file (gchatfmt.Parse
// does its own UTF-16 re-encoding internally, only for annotation offset
// math -- see its package doc comment). Astral-plane characters (e.g.
// emoji outside the BMP) are preserved because Go strings are already
// UTF-8 byte sequences.
//
// text_body presence gate: Python only creates a text event at all when
// evt.text_body is truthy (portal.py:1411, `if evt.text_body:`) -- an
// attachment-only message with an empty text_body gets zero text parts, not
// a part with an empty body. This mirrors Python's exact truthiness rule:
// the empty string is the only falsy case; a whitespace-only body (e.g.
// "   ") is truthy in Python and therefore still produces a part, verbatim
// and untrimmed.
//
// Format/FormattedBody gate: gchatfmt.Parse returns html == "" whenever
// annotations is empty (its own fix for the Python always-truthy-annotations
// bug at from_googlechat.py:45 -- see gchatfmt's package doc comment); this
// function mirrors that exactly by leaving Content.Format unset and
// Content.FormattedBody empty in that case, matching Matrix's own contract
// that a plain m.text event carries no format/formatted_body fields at all.
// When html != "", Content.Format is set to event.FormatHTML and
// Content.FormattedBody to html, while Content.Body always stays the plain
// body gchatfmt.Parse returns (which is text_body verbatim -- see its own
// doc comment on why plain-body derivation is left to the caller).
func (mc *MessageConverter) ToMatrix(ctx context.Context, msg *pb.Message, mention gchatfmt.MentionResolver) *bridgev2.ConvertedMessage {
	cm := &bridgev2.ConvertedMessage{
		Parts: make([]*bridgev2.ConvertedMessagePart, 0, 1),
	}
	text := msg.GetTextBody()
	if text == "" {
		return cm
	}

	body, html := gchatfmt.Parse(ctx, text, msg.GetAnnotations(), mention)
	content := &event.MessageEventContent{
		MsgType: event.MsgText,
		Body:    body,
	}
	if html != "" {
		content.Format = event.FormatHTML
		content.FormattedBody = html
	}

	// Empty PartID marks this as the first/only part, matching the wider
	// bridgev2 ecosystem convention (e.g. mautrix-meta's
	// metaid.MakeMessagePartID: index 0 -> ""). M5 can append further parts
	// (attachments) with non-empty sequential IDs alongside this one
	// without renumbering it -- appended, never inserted/overwritten, so a
	// future attachment part built elsewhere and merged into the same
	// bridgev2.ConvertedMessage (e.g. via bridgev2.MergeCaption) can never
	// be clobbered by this function building its own text part (see B4,
	// docs/research/08d-megabridge-msgconv.md §2.4, fixed on the outbound
	// side in pkg/connector/handlematrix.go's mergeAnnotations).
	cm.Parts = append(cm.Parts, &bridgev2.ConvertedMessagePart{
		Type:    event.EventMessage,
		Content: content,
	})
	return cm
}
