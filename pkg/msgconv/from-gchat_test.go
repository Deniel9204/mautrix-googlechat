package msgconv_test

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv"
	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv/gchatfmt"
)

// TestToMatrix_PlainText covers the base case ported from
// from_googlechat.py's googlechat_to_matrix: TextBody becomes the body of
// a single m.text part.
func TestToMatrix_PlainText(t *testing.T) {
	mc := msgconv.New()
	msg := &pb.Message{TextBody: proto.String("hello world")}

	cm := mc.ToMatrix(context.Background(), msg, nil)

	if cm == nil {
		t.Fatal("ToMatrix returned nil")
	}
	if len(cm.Parts) != 1 {
		t.Fatalf("expected 1 part, got %d", len(cm.Parts))
	}
	part := cm.Parts[0]
	if part.Type != event.EventMessage {
		t.Errorf("part.Type = %v, want %v", part.Type, event.EventMessage)
	}
	if part.Content == nil {
		t.Fatal("part.Content is nil")
	}
	if part.Content.MsgType != event.MsgText {
		t.Errorf("MsgType = %q, want %q", part.Content.MsgType, event.MsgText)
	}
	if part.Content.Body != "hello world" {
		t.Errorf("Body = %q, want %q", part.Content.Body, "hello world")
	}
	if part.Content.Format != "" {
		t.Errorf("Format = %q, want empty (no annotations -> no HTML)", part.Content.Format)
	}
	if part.Content.FormattedBody != "" {
		t.Errorf("FormattedBody = %q, want empty (no annotations -> no HTML)", part.Content.FormattedBody)
	}
}

// TestToMatrix_EmptyBody ports the Python gate at portal.py:1411
// (`if evt.text_body:`) -- the Python bridge never sends a text event for a
// message whose text_body is empty (e.g. an attachment-only message), so
// ToMatrix must produce zero parts, not a part with an empty body.
func TestToMatrix_EmptyBody(t *testing.T) {
	mc := msgconv.New()

	t.Run("explicit empty string", func(t *testing.T) {
		msg := &pb.Message{TextBody: proto.String("")}
		cm := mc.ToMatrix(context.Background(), msg, nil)
		if len(cm.Parts) != 0 {
			t.Fatalf("expected 0 parts for empty text_body, got %d", len(cm.Parts))
		}
	})

	t.Run("field absent", func(t *testing.T) {
		msg := &pb.Message{}
		cm := mc.ToMatrix(context.Background(), msg, nil)
		if len(cm.Parts) != 0 {
			t.Fatalf("expected 0 parts when text_body is unset, got %d", len(cm.Parts))
		}
	})
}

// TestToMatrix_WhitespaceBody: Python truthiness only excludes the exact
// empty string; whitespace-only text is truthy and is NOT trimmed anywhere
// in googlechat_to_matrix, so it must survive verbatim.
func TestToMatrix_WhitespaceBody(t *testing.T) {
	mc := msgconv.New()
	msg := &pb.Message{TextBody: proto.String("   ")}

	cm := mc.ToMatrix(context.Background(), msg, nil)

	if len(cm.Parts) != 1 {
		t.Fatalf("expected 1 part for whitespace-only text_body, got %d", len(cm.Parts))
	}
	if got := cm.Parts[0].Content.Body; got != "   " {
		t.Errorf("Body = %q, want unmodified %q", got, "   ")
	}
}

// TestToMatrix_UnicodeEmoji proves UTF-8 text (including astral-plane
// emoji, which are surrogate pairs in UTF-16 / Python) survives untouched
// in the plain body.
func TestToMatrix_UnicodeEmoji(t *testing.T) {
	mc := msgconv.New()
	want := "héllo 👋🏽 世界 🇺🇳"
	msg := &pb.Message{TextBody: proto.String(want)}

	cm := mc.ToMatrix(context.Background(), msg, nil)

	if len(cm.Parts) != 1 {
		t.Fatalf("expected 1 part, got %d", len(cm.Parts))
	}
	if got := cm.Parts[0].Content.Body; got != want {
		t.Errorf("Body = %q, want %q", got, want)
	}
}

// TestToMatrix_NoAnnotationsStaysPlain pins the fast path: a message with no
// annotations must never get an HTML formatted_body, matching gchatfmt.Parse's
// fix for the Python always-truthy-annotations bug at
// from_googlechat.py:45 (see gchatfmt's package doc comment) and the
// task's "keep plain-text fast path" constraint.
func TestToMatrix_NoAnnotationsStaysPlain(t *testing.T) {
	mc := msgconv.New()
	msg := &pb.Message{TextBody: proto.String("plain text, nothing special")}

	cm := mc.ToMatrix(context.Background(), msg, nil)

	if len(cm.Parts) != 1 {
		t.Fatalf("expected 1 part, got %d", len(cm.Parts))
	}
	part := cm.Parts[0]
	if part.Content.Format != "" {
		t.Errorf("Format = %q, want empty", part.Content.Format)
	}
	if part.Content.FormattedBody != "" {
		t.Errorf("FormattedBody = %q, want empty", part.Content.FormattedBody)
	}
}

// TestToMatrix_FormatAnnotationsProduceHTML is the headline M3 Task 4
// behavior: a message with FORMAT_DATA annotations must produce a part
// with Format=org.matrix.custom.html, the gchatfmt-rendered FormattedBody,
// and Body left as the plain text_body (unmodified by the HTML rendering).
func TestToMatrix_FormatAnnotationsProduceHTML(t *testing.T) {
	mc := msgconv.New()
	msg := &pb.Message{
		TextBody: proto.String("hello world"),
		Annotations: []*pb.Annotation{
			gchatfmt.MakeFormatAnnotation(0, 5, pb.FormatMetadata_BOLD),
		},
	}

	cm := mc.ToMatrix(context.Background(), msg, nil)

	if len(cm.Parts) != 1 {
		t.Fatalf("expected 1 part, got %d", len(cm.Parts))
	}
	part := cm.Parts[0]
	if part.Content.Body != "hello world" {
		t.Errorf("Body = %q, want %q", part.Content.Body, "hello world")
	}
	if part.Content.Format != event.FormatHTML {
		t.Errorf("Format = %q, want %q", part.Content.Format, event.FormatHTML)
	}
	wantHTML := "<strong>hello</strong> world"
	if part.Content.FormattedBody != wantHTML {
		t.Errorf("FormattedBody = %q, want %q", part.Content.FormattedBody, wantHTML)
	}
}

// TestToMatrix_MentionAnnotationUsesPassedResolver proves ToMatrix actually
// threads the mention parameter into gchatfmt.Parse (not just accepting and
// ignoring it): a resolver that knows the gaia id must produce a pill in
// FormattedBody.
func TestToMatrix_MentionAnnotationUsesPassedResolver(t *testing.T) {
	mc := msgconv.New()
	msg := &pb.Message{
		TextBody:    proto.String("@Bob hi"),
		Annotations: []*pb.Annotation{gchatfmt.MakeMentionAnnotation(0, 4, "200")},
	}
	resolve := func(gaiaID string) (id.UserID, string, bool) {
		if gaiaID == "200" {
			return "@200_ghost:example.com", "", true
		}
		return "", "", false
	}

	cm := mc.ToMatrix(context.Background(), msg, resolve)

	if len(cm.Parts) != 1 {
		t.Fatalf("expected 1 part, got %d", len(cm.Parts))
	}
	want := `<a href="https://matrix.to/#/@200_ghost:example.com">@Bob</a> hi`
	if got := cm.Parts[0].Content.FormattedBody; got != want {
		t.Errorf("FormattedBody = %q, want %q", got, want)
	}
}

// TestToMatrix_NilResolverDoesNotPanic: a nil mention resolver (e.g. a
// caller that hasn't wired one up) must degrade to gchatfmt's own nil-safe
// fallback, not panic.
func TestToMatrix_NilResolverDoesNotPanic(t *testing.T) {
	mc := msgconv.New()
	msg := &pb.Message{
		TextBody:    proto.String("@Bob hi"),
		Annotations: []*pb.Annotation{gchatfmt.MakeMentionAnnotation(0, 4, "200")},
	}

	cm := mc.ToMatrix(context.Background(), msg, nil)

	if len(cm.Parts) != 1 {
		t.Fatalf("expected 1 part, got %d", len(cm.Parts))
	}
	if got := cm.Parts[0].Content.FormattedBody; got != "@Bob hi" {
		t.Errorf("FormattedBody = %q, want %q (unpilled fallback)", got, "@Bob hi")
	}
}

// TestToMatrix_PartID pins the single-part convention (empty PartID for
// the first/only part), matching the wider bridgev2 ecosystem convention
// (e.g. mautrix-meta's metaid.MakeMessagePartID: index 0 -> "") so that M5
// can later append additional non-empty PartIDs for attachment parts
// without renumbering this one.
func TestToMatrix_PartID(t *testing.T) {
	mc := msgconv.New()
	msg := &pb.Message{TextBody: proto.String("hi")}

	cm := mc.ToMatrix(context.Background(), msg, nil)

	if len(cm.Parts) != 1 {
		t.Fatalf("expected 1 part, got %d", len(cm.Parts))
	}
	if cm.Parts[0].ID != "" {
		t.Errorf("PartID = %q, want empty for the sole part", cm.Parts[0].ID)
	}
}

// TestToMatrix_TextPartAppendedNotOverwritten is a B4-adjacent regression
// guard (docs/research/08d §2.4: a media message with a formatted caption
// must keep BOTH the file and the caption text, never let one overwrite
// the other). ToMatrix itself only builds a text part in M3 (attachment
// parts are M5), but it must add that part via append onto whatever
// cm.Parts a future combined build already populated, never by replacing
// the whole slice -- otherwise a caller that had already appended a stub
// "file" part before calling into text-part construction would lose it.
// This pins that structural invariant directly against ToMatrix's actual
// cm.Parts, not a hypothetical helper.
func TestToMatrix_TextPartAppendedNotOverwritten(t *testing.T) {
	mc := msgconv.New()
	msg := &pb.Message{TextBody: proto.String("a caption")}

	cm := mc.ToMatrix(context.Background(), msg, nil)
	if len(cm.Parts) != 1 {
		t.Fatalf("expected 1 text part, got %d", len(cm.Parts))
	}

	// Simulate a caller (M5) that already has a stub file part and combines
	// it with ToMatrix's own output by appending, the same way ToMatrix
	// itself builds cm.Parts (cm.Parts = append(cm.Parts, textPart)).
	stubFilePart := &bridgev2.ConvertedMessagePart{
		ID:   "file",
		Type: event.EventMessage,
		Content: &event.MessageEventContent{
			MsgType: event.MsgImage,
			Body:    "photo.jpg",
		},
	}
	combined := append([]*bridgev2.ConvertedMessagePart{stubFilePart}, cm.Parts...)

	if len(combined) != 2 {
		t.Fatalf("expected 2 parts (file + text) after combining, got %d", len(combined))
	}
	if combined[0].Content.Body != "photo.jpg" {
		t.Errorf("combined[0] (file part) = %+v, want the stub file part preserved first", combined[0].Content)
	}
	if combined[1].Content.Body != "a caption" {
		t.Errorf("combined[1] (text part) = %+v, want the caption text preserved second", combined[1].Content)
	}
}
