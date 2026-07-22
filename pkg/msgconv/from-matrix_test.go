package msgconv_test

import (
	"context"
	"testing"

	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv"
	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv/gchatfmt"
)

// TestFromMatrix_PlainText covers the base case: content.Body becomes the
// Google Chat text_body verbatim when there's no HTML formatting, with no
// annotations produced.
func TestFromMatrix_PlainText(t *testing.T) {
	mc := msgconv.New()
	content := &event.MessageEventContent{
		MsgType: event.MsgText,
		Body:    "hello world",
	}

	text, annotations := mc.FromMatrix(context.Background(), content, nil)

	if text != "hello world" {
		t.Errorf("text = %q, want %q", text, "hello world")
	}
	if annotations != nil {
		t.Errorf("annotations = %v, want nil for a plain-body message", annotations)
	}
}

// TestFromMatrix_FormattedBodyProducesAnnotations is the headline M3 Task 4
// outbound behavior: an HTML formatted_body must be run through
// matrixfmt.Parse, producing an annotation-stripped text_body plus the
// matching annotations list.
func TestFromMatrix_FormattedBodyProducesAnnotations(t *testing.T) {
	mc := msgconv.New()
	content := &event.MessageEventContent{
		MsgType:       event.MsgText,
		Body:          "bold text",
		Format:        event.FormatHTML,
		FormattedBody: "<strong>bold</strong> text",
	}

	text, annotations := mc.FromMatrix(context.Background(), content, nil)

	if text != "bold text" {
		t.Errorf("text = %q, want %q", text, "bold text")
	}
	want := []*pb.Annotation{gchatfmt.MakeFormatAnnotation(0, 4, pb.FormatMetadata_BOLD)}
	if len(annotations) != 1 || annotations[0].String() != want[0].String() {
		t.Errorf("annotations = %s, want %s", formatAnnotations(annotations), formatAnnotations(want))
	}
}

// TestFromMatrix_MentionUsesPassedResolver proves FromMatrix actually
// threads the mention parameter into matrixfmt.Parse (not just accepting
// and ignoring it): a resolver that knows the MXID must produce a MENTION
// annotation.
func TestFromMatrix_MentionUsesPassedResolver(t *testing.T) {
	mc := msgconv.New()
	content := &event.MessageEventContent{
		MsgType:       event.MsgText,
		Body:          "plain-text fallback",
		Format:        event.FormatHTML,
		FormattedBody: `Hi <a href="https://matrix.to/#/@200_ghost:example.com">Bob</a>!`,
	}
	resolve := func(mxid id.UserID) (string, bool) {
		if mxid == "@200_ghost:example.com" {
			return "200", true
		}
		return "", false
	}

	text, annotations := mc.FromMatrix(context.Background(), content, resolve)

	if text != "Hi @Bob!" {
		t.Errorf("text = %q, want %q", text, "Hi @Bob!")
	}
	want := []*pb.Annotation{gchatfmt.MakeMentionAnnotation(3, 4, "200")}
	if len(annotations) != 1 || annotations[0].String() != want[0].String() {
		t.Errorf("annotations = %s, want %s", formatAnnotations(annotations), formatAnnotations(want))
	}
}

// TestFromMatrix_NilResolverDoesNotPanic: a nil mention resolver must
// degrade to matrixfmt's own nil-safe fallback (render plain text, no
// MENTION annotation), not panic.
func TestFromMatrix_NilResolverDoesNotPanic(t *testing.T) {
	mc := msgconv.New()
	content := &event.MessageEventContent{
		MsgType:       event.MsgText,
		Body:          "plain-text fallback",
		Format:        event.FormatHTML,
		FormattedBody: `Hi <a href="https://matrix.to/#/@200_ghost:example.com">Bob</a>!`,
	}

	text, annotations := mc.FromMatrix(context.Background(), content, nil)

	if text != "Hi Bob!" {
		t.Errorf("text = %q, want %q", text, "Hi Bob!")
	}
	if annotations != nil {
		t.Errorf("annotations = %v, want nil (no resolver -> no MENTION)", annotations)
	}
}

// TestFromMatrix_EmptyBody: an empty body with no formatting produces an
// empty text_body and no annotations.
func TestFromMatrix_EmptyBody(t *testing.T) {
	mc := msgconv.New()
	content := &event.MessageEventContent{MsgType: event.MsgText, Body: ""}

	text, annotations := mc.FromMatrix(context.Background(), content, nil)

	if text != "" {
		t.Errorf("text = %q, want empty string", text)
	}
	if annotations != nil {
		t.Errorf("annotations = %v, want nil", annotations)
	}
}

// TestFromMatrix_UnicodeEmojiPreserved proves UTF-8 text (including
// astral-plane emoji) survives untouched through the plain-body path.
func TestFromMatrix_UnicodeEmojiPreserved(t *testing.T) {
	mc := msgconv.New()
	want := "héllo 👋🏽 世界 🇺🇳"
	content := &event.MessageEventContent{MsgType: event.MsgText, Body: want}

	text, _ := mc.FromMatrix(context.Background(), content, nil)

	if text != want {
		t.Errorf("text = %q, want %q", text, want)
	}
}

// TestFromMatrix_NoticeBody: m.notice messages carry the same Body field
// and must convert identically to m.text -- both m.text and m.notice route
// through the same text path, and FromMatrix itself doesn't even look at
// MsgType (that gate lives in the connector's routing decision,
// handlematrix.go), so this just confirms Body extraction doesn't silently
// depend on MsgType.
func TestFromMatrix_NoticeBody(t *testing.T) {
	mc := msgconv.New()
	content := &event.MessageEventContent{MsgType: event.MsgNotice, Body: "a notice"}

	text, _ := mc.FromMatrix(context.Background(), content, nil)

	if text != "a notice" {
		t.Errorf("text = %q, want %q", text, "a notice")
	}
}

func formatAnnotations(anns []*pb.Annotation) string {
	s := "["
	for i, a := range anns {
		if i > 0 {
			s += ", "
		}
		s += a.String()
	}
	return s + "]"
}
