package msgconv_test

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/event"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv"
)

// TestToMatrix_PlainText covers the base case ported from
// from_googlechat.py's googlechat_to_matrix: TextBody becomes the body of
// a single m.text part.
func TestToMatrix_PlainText(t *testing.T) {
	mc := msgconv.New()
	msg := &pb.Message{TextBody: proto.String("hello world")}

	cm := mc.ToMatrix(context.Background(), msg)

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
		t.Errorf("Format = %q, want empty (M2 is plain text, no HTML)", part.Content.Format)
	}
	if part.Content.FormattedBody != "" {
		t.Errorf("FormattedBody = %q, want empty (M2 is plain text)", part.Content.FormattedBody)
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
		cm := mc.ToMatrix(context.Background(), msg)
		if len(cm.Parts) != 0 {
			t.Fatalf("expected 0 parts for empty text_body, got %d", len(cm.Parts))
		}
	})

	t.Run("field absent", func(t *testing.T) {
		msg := &pb.Message{}
		cm := mc.ToMatrix(context.Background(), msg)
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

	cm := mc.ToMatrix(context.Background(), msg)

	if len(cm.Parts) != 1 {
		t.Fatalf("expected 1 part for whitespace-only text_body, got %d", len(cm.Parts))
	}
	if got := cm.Parts[0].Content.Body; got != "   " {
		t.Errorf("Body = %q, want unmodified %q", got, "   ")
	}
}

// TestToMatrix_UnicodeEmoji proves UTF-8 text (including astral-plane
// emoji, which are surrogate pairs in UTF-16 / Python) survives untouched.
// M2 does no UTF-16 offset math (that's M3's annotation work), so there is
// no surrogate encode/decode step here -- text_body passes through as-is.
func TestToMatrix_UnicodeEmoji(t *testing.T) {
	mc := msgconv.New()
	want := "héllo 👋🏽 世界 🇺🇳"
	msg := &pb.Message{TextBody: proto.String(want)}

	cm := mc.ToMatrix(context.Background(), msg)

	if len(cm.Parts) != 1 {
		t.Fatalf("expected 1 part, got %d", len(cm.Parts))
	}
	if got := cm.Parts[0].Content.Body; got != want {
		t.Errorf("Body = %q, want %q", got, want)
	}
}

// TestToMatrix_AnnotationsPresentIgnored: M2 must not crash when
// annotations are present, and must NOT replicate the Python bug at
// from_googlechat.py:45 (`if annotations:` tests the always-truthy
// `from __future__ import annotations` feature object instead of
// `evt.annotations`, so the Python bridge always runs the HTML-formatting
// path). Here we simply take text_body verbatim regardless of annotations
// -- formatting is M3's job -- so the output must stay plain (no Format,
// no FormattedBody) even though annotations are non-empty.
func TestToMatrix_AnnotationsPresentIgnored(t *testing.T) {
	mc := msgconv.New()
	msg := &pb.Message{
		TextBody: proto.String("bold text"),
		Annotations: []*pb.Annotation{
			{StartIndex: proto.Int32(0), Length: proto.Int32(4)},
		},
	}

	cm := mc.ToMatrix(context.Background(), msg)

	if len(cm.Parts) != 1 {
		t.Fatalf("expected 1 part, got %d", len(cm.Parts))
	}
	part := cm.Parts[0]
	if part.Content.Body != "bold text" {
		t.Errorf("Body = %q, want %q", part.Content.Body, "bold text")
	}
	if part.Content.Format != "" {
		t.Errorf("Format = %q, want empty -- M2 ignores annotation formatting entirely", part.Content.Format)
	}
	if part.Content.FormattedBody != "" {
		t.Errorf("FormattedBody = %q, want empty", part.Content.FormattedBody)
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

	cm := mc.ToMatrix(context.Background(), msg)

	if len(cm.Parts) != 1 {
		t.Fatalf("expected 1 part, got %d", len(cm.Parts))
	}
	if cm.Parts[0].ID != "" {
		t.Errorf("PartID = %q, want empty for the sole part", cm.Parts[0].ID)
	}
}
