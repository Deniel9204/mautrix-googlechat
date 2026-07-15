package msgconv_test

import (
	"context"
	"testing"

	"maunium.net/go/mautrix/event"

	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv"
)

// TestFromMatrix_PlainText covers the base case: content.Body becomes the
// Google Chat text_body verbatim.
func TestFromMatrix_PlainText(t *testing.T) {
	mc := msgconv.New()
	content := &event.MessageEventContent{
		MsgType: event.MsgText,
		Body:    "hello world",
	}

	got := mc.FromMatrix(context.Background(), content)

	if got != "hello world" {
		t.Errorf("FromMatrix = %q, want %q", got, "hello world")
	}
}

// TestFromMatrix_FormattedBodyIgnored pins M2's scope boundary: even when a
// formatted_body/HTML format is present (a client always sends both), only
// the plain Body is used. HTML-aware conversion is M3's job.
func TestFromMatrix_FormattedBodyIgnored(t *testing.T) {
	mc := msgconv.New()
	content := &event.MessageEventContent{
		MsgType:       event.MsgText,
		Body:          "bold text",
		Format:        event.FormatHTML,
		FormattedBody: "<strong>bold</strong> text",
	}

	got := mc.FromMatrix(context.Background(), content)

	if got != "bold text" {
		t.Errorf("FromMatrix = %q, want %q (formatted_body must be ignored in M2)", got, "bold text")
	}
}

// TestFromMatrix_EmptyBody: an empty body is passed through verbatim (no
// special-casing) -- unlike ToMatrix's text_body-presence gate, which is a
// GC->Matrix "should we even create this part" decision (from-gchat.go),
// FromMatrix is a pure string extraction with no such gate of its own.
func TestFromMatrix_EmptyBody(t *testing.T) {
	mc := msgconv.New()
	content := &event.MessageEventContent{MsgType: event.MsgText, Body: ""}

	got := mc.FromMatrix(context.Background(), content)

	if got != "" {
		t.Errorf("FromMatrix = %q, want empty string", got)
	}
}

// TestFromMatrix_UnicodeEmojiPreserved proves UTF-8 text (including
// astral-plane emoji) survives untouched -- no UTF-16 surrogate/offset math
// happens here (that's M3's annotation work).
func TestFromMatrix_UnicodeEmojiPreserved(t *testing.T) {
	mc := msgconv.New()
	want := "héllo 👋🏽 世界 🇺🇳"
	content := &event.MessageEventContent{MsgType: event.MsgText, Body: want}

	got := mc.FromMatrix(context.Background(), content)

	if got != want {
		t.Errorf("FromMatrix = %q, want %q", got, want)
	}
}

// TestFromMatrix_NoticeBody: m.notice messages carry the same Body field
// and must convert identically to m.text -- portal.py's handle_matrix_message
// routes both MessageType.TEXT and MessageType.NOTICE through the same
// _handle_matrix_text path (portal.py:915), and FromMatrix itself doesn't
// even look at MsgType (that gate lives in the connector's routing
// decision, handlematrix.go), so this just confirms Body extraction doesn't
// silently depend on MsgType.
func TestFromMatrix_NoticeBody(t *testing.T) {
	mc := msgconv.New()
	content := &event.MessageEventContent{MsgType: event.MsgNotice, Body: "a notice"}

	got := mc.FromMatrix(context.Background(), content)

	if got != "a notice" {
		t.Errorf("FromMatrix = %q, want %q", got, "a notice")
	}
}
