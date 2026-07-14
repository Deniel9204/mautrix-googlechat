package msgconv

import (
	"context"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// ToMatrix converts a Google Chat Message proto into a bridgev2.
// ConvertedMessage. This is a port of from_googlechat.py's
// googlechat_to_matrix, restricted to the plain-text subset that is M2's
// scope -- annotation-driven HTML formatting is M3, attachments are M5.
//
// Plain-text extraction: the message body is exactly msg.GetTextBody()
// (proto field 10, TextBody in the generated Go struct). Python builds
// TextMessageEventContent(body=add_surrogate(evt.text_body)) and later,
// when there is no formatted_body, sets content.body =
// del_surrogate(content.body) -- i.e. round-trips text_body through UTF-16
// surrogate padding and back unchanged. That padding only matters once
// annotation start_index/length (UTF-16 code-unit offsets) are used to
// slice the string, which M2 does not do. So here text_body is taken
// verbatim as UTF-8: no surrogate encode/decode, no offset math, no
// truncation. Astral-plane characters (e.g. emoji outside the BMP) are
// preserved because Go strings are already UTF-8 byte sequences -- there is
// no lossy intermediate representation to worry about.
//
// text_body presence gate: Python only creates a text event at all when
// evt.text_body is truthy (portal.py:1411, `if evt.text_body:`) -- an
// attachment-only message with an empty text_body gets zero text parts, not
// a part with an empty body. This mirrors Python's exact truthiness rule:
// the empty string is the only falsy case; a whitespace-only body (e.g.
// "   ") is truthy in Python and therefore still produces a part, verbatim
// and untrimmed.
//
// Annotations: msg.Annotations is read (so callers can pass messages that
// carry them, e.g. from a live wire capture) but is NOT interpreted for
// formatting in M2 -- their presence must never change the plain-text
// output. This is a deliberate divergence from the Python original, which
// has a bug at from_googlechat.py:45 (`if annotations:` tests the
// always-truthy `from __future__ import annotations` feature object
// instead of `evt.annotations`), so the Python bridge always takes the
// HTML-formatting code path, even for annotation-free messages. Do not
// port that: until M3 adds real annotation handling, ToMatrix has no
// formatting path to (incorrectly) always take.
func (mc *MessageConverter) ToMatrix(ctx context.Context, msg *pb.Message) *bridgev2.ConvertedMessage {
	cm := &bridgev2.ConvertedMessage{
		Parts: make([]*bridgev2.ConvertedMessagePart, 0, 1),
	}
	if text := msg.GetTextBody(); text != "" {
		cm.Parts = append(cm.Parts, &bridgev2.ConvertedMessagePart{
			// Empty PartID marks this as the first/only part, matching the
			// wider bridgev2 ecosystem convention (e.g. mautrix-meta's
			// metaid.MakeMessagePartID: index 0 -> ""). M5 can append
			// further parts (attachments) with non-empty sequential IDs
			// alongside this one without renumbering it.
			Type: event.EventMessage,
			Content: &event.MessageEventContent{
				MsgType: event.MsgText,
				Body:    text,
			},
		})
	}
	return cm
}
