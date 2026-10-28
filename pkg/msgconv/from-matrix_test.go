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

func TestFromMatrix_UnicodeEmojiPreserved(t *testing.T) {
	mc := msgconv.New()
	want := "héllo 👋🏽 世界 🇺🇳"
	content := &event.MessageEventContent{MsgType: event.MsgText, Body: want}

	text, _ := mc.FromMatrix(context.Background(), content, nil)

	if text != want {
		t.Errorf("text = %q, want %q", text, want)
	}
}

// FromMatrix ignores MsgType, so m.notice must convert exactly like m.text.
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
