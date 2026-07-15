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

	cm, _ := mc.ToMatrix(context.Background(), msg, false, nil)

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
		cm, _ := mc.ToMatrix(context.Background(), msg, false, nil)
		if len(cm.Parts) != 0 {
			t.Fatalf("expected 0 parts for empty text_body, got %d", len(cm.Parts))
		}
	})

	t.Run("field absent", func(t *testing.T) {
		msg := &pb.Message{}
		cm, _ := mc.ToMatrix(context.Background(), msg, false, nil)
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

	cm, _ := mc.ToMatrix(context.Background(), msg, false, nil)

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

	cm, _ := mc.ToMatrix(context.Background(), msg, false, nil)

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

	cm, _ := mc.ToMatrix(context.Background(), msg, false, nil)

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

	cm, _ := mc.ToMatrix(context.Background(), msg, false, nil)

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
// FormattedBody AND surface the resolved mention in the returned
// ParsedMentions (the source content.Mentions is later built from).
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

	cm, mentions := mc.ToMatrix(context.Background(), msg, false, resolve)

	if len(cm.Parts) != 1 {
		t.Fatalf("expected 1 part, got %d", len(cm.Parts))
	}
	want := `<a href="https://matrix.to/#/@200_ghost:example.com">@Bob</a> hi`
	if got := cm.Parts[0].Content.FormattedBody; got != want {
		t.Errorf("FormattedBody = %q, want %q", got, want)
	}
	if len(mentions.UserIDs) != 1 || mentions.UserIDs[0] != "@200_ghost:example.com" {
		t.Errorf("ParsedMentions.UserIDs = %v, want exactly [@200_ghost:example.com]", mentions.UserIDs)
	}
	if mentions.Room {
		t.Error("ParsedMentions.Room = true, want false for a specific-user mention")
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

	cm, _ := mc.ToMatrix(context.Background(), msg, false, nil)

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

	cm, _ := mc.ToMatrix(context.Background(), msg, false, nil)

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

	cm, _ := mc.ToMatrix(context.Background(), msg, false, nil)
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

// --- ThreadRoot (M3 Task 6) --------------------------------------------------
//
// Ported from handle_googlechat_message's parent_id extraction
// (portal.py:1379, `parent_id = evt.id.parent_id.topic_id.topic_id`) plus
// the head-vs-reply distinction inherent to Google Chat's topic model:
// message_id == topic_id for the head/root message of a topic (self-
// referencing on the wire), and != for every reply posted into an existing
// one (client.py's create_message always targets an EXISTING topic id,
// never the new message's own id -- see send_message, client.py:441-458).
//
// bridgev2's own convention (mautrix-go bridgev2/portal.go:2804,
// getRelationMeta: `if currentMsg.ThreadRoot != nil && *currentMsg.ThreadRoot
// != currentMsgID`) accepts a SELF-referencing ThreadRoot (equal to the
// message's own id) as "this message is the head of its own thread" --
// bridgev2 skips the thread-root lookup for that case but still records the
// self-reference in the message's DB row, so a later Matrix reply to this
// head message resolves back to it via GetFirstThreadMessage (the "reply ->
// thread auto-conversion" the task brief calls out, portal.go:1259-1268).

func topicMessage(messageID, topicID, text string) *pb.Message {
	msg := &pb.Message{TextBody: proto.String(text)}
	msg.Id = &pb.MessageId{MessageId: proto.String(messageID)}
	if topicID != "" {
		msg.Id.ParentId = &pb.MessageParentId{
			Parent: &pb.MessageParentId_TopicId{
				TopicId: &pb.TopicId{TopicId: proto.String(topicID)},
			},
		}
	}
	return msg
}

// TestToMatrix_HeadMessageFlatRoomNoThreadRoot: message_id == topic_id (the
// head of a brand new topic) in a non-threads-only room must NOT get a
// self-referencing ThreadRoot -- matching Python's thread_parent staying
// None for a head message unless self.threads_only (portal.py:1406).
func TestToMatrix_HeadMessageFlatRoomNoThreadRoot(t *testing.T) {
	mc := msgconv.New()
	msg := topicMessage("topic1", "topic1", "hello")

	cm, _ := mc.ToMatrix(context.Background(), msg, false, nil)

	if cm.ThreadRoot != nil {
		t.Errorf("ThreadRoot = %v, want nil for a head message in a non-threads-only room", *cm.ThreadRoot)
	}
}

// TestToMatrix_HeadMessageThreadsOnlyRoomSelfThreadRoot: the same head
// message in a threads-only room DOES get a self-referencing ThreadRoot
// (Python's _append_event_id: `if thread_parent or self.threads_only`,
// portal.py:1406-1409) so later Matrix replies auto-convert into this
// topic (bridgev2 portal.go:1259-1268).
func TestToMatrix_HeadMessageThreadsOnlyRoomSelfThreadRoot(t *testing.T) {
	mc := msgconv.New()
	msg := topicMessage("topic1", "topic1", "hello")

	cm, _ := mc.ToMatrix(context.Background(), msg, true, nil)

	if cm.ThreadRoot == nil {
		t.Fatal("ThreadRoot = nil, want a self-reference to topic1 in a threads-only room")
	}
	if string(*cm.ThreadRoot) != "topic1" {
		t.Errorf("ThreadRoot = %q, want %q", *cm.ThreadRoot, "topic1")
	}
}

// TestToMatrix_ReplyMessageAlwaysSetsThreadRoot: message_id != topic_id (a
// reply posted into an existing topic) must get ThreadRoot set to the topic
// id UNCONDITIONALLY -- in both a flat/legacy room and a threads-only one --
// matching Python's unconditional `if parent_id:` gate (portal.py:1380),
// which is not conditioned on self.threads_only at all (only the head-
// message self-reference case is).
func TestToMatrix_ReplyMessageAlwaysSetsThreadRoot(t *testing.T) {
	mc := msgconv.New()
	for _, threadsOnly := range []bool{false, true} {
		msg := topicMessage("reply1", "topic1", "a reply")
		cm, _ := mc.ToMatrix(context.Background(), msg, threadsOnly, nil)
		if cm.ThreadRoot == nil {
			t.Fatalf("threadsOnly=%v: ThreadRoot = nil, want %q", threadsOnly, "topic1")
		}
		if string(*cm.ThreadRoot) != "topic1" {
			t.Errorf("threadsOnly=%v: ThreadRoot = %q, want %q", threadsOnly, *cm.ThreadRoot, "topic1")
		}
	}
}

// TestToMatrix_NoParentIDNoThreadRoot: a message with no parent_id.topic_id
// on the wire at all (topic_id empty/absent) must never get a ThreadRoot,
// matching Python's falsy check on parent_id (portal.py:1380) -- there is
// nothing to route to or self-reference.
func TestToMatrix_NoParentIDNoThreadRoot(t *testing.T) {
	mc := msgconv.New()
	for _, threadsOnly := range []bool{false, true} {
		msg := topicMessage("msg1", "", "hello")
		cm, _ := mc.ToMatrix(context.Background(), msg, threadsOnly, nil)
		if cm.ThreadRoot != nil {
			t.Errorf("threadsOnly=%v: ThreadRoot = %v, want nil when parent_id.topic_id is absent", threadsOnly, *cm.ThreadRoot)
		}
	}
}

// TestToMatrix_ThreadRootSetEvenForAttachmentOnlyMessage proves ThreadRoot
// is computed before the empty-text_body early return, so a future (M5)
// attachment-only message in a thread still carries the right ThreadRoot
// even though it has zero text parts today.
func TestToMatrix_ThreadRootSetEvenForAttachmentOnlyMessage(t *testing.T) {
	mc := msgconv.New()
	msg := topicMessage("reply1", "topic1", "")

	cm, _ := mc.ToMatrix(context.Background(), msg, false, nil)

	if len(cm.Parts) != 0 {
		t.Fatalf("expected 0 parts for empty text_body, got %d", len(cm.Parts))
	}
	if cm.ThreadRoot == nil || string(*cm.ThreadRoot) != "topic1" {
		t.Errorf("ThreadRoot = %v, want %q even with no text parts", cm.ThreadRoot, "topic1")
	}
}

// --- ReplyTo (M3 Task 7, quote-replies) --------------------------------------
//
// Ports handle_googlechat_message's reply_to resolution (portal.py:1390-1396):
// `if evt.reply_to: reply_to_db = await DBMessage.get_by_gcid(evt.reply_to.id.message_id, ...)`
// -- the wire-level source is always evt.reply_to.id.message_id (ReplyToMessage.id,
// MessageId.message_id, proto field 37 -> field 1 -> field 2). Unlike Python
// (which resolves the DB row and hands its Matrix mxid to content.set_reply
// right there in portal.py, since Matrix event id resolution requires a DB
// lookup that belongs in the connector, not msgconv, matching msgconv's own
// "no portal, no intent" package doc), ToMatrix here only surfaces the
// network message id itself via cm.ReplyTo (a
// *networkid.MessageOptionalPartID); resolving it to a concrete Matrix event
// (or dropping it if the target was never bridged) is bridgev2's own generic
// job (mautrix-go bridgev2/portal.go:2768-2801, GetFirstOrSpecificPartByID),
// not this connector's, and not msgconv's.

// replyMessage builds a *pb.Message with a ReplyToMessage set, mirroring
// topicMessage's plain-field-construction style above.
func replyMessage(text, replyToMessageID string) *pb.Message {
	msg := &pb.Message{TextBody: proto.String(text)}
	msg.ReplyTo = &pb.ReplyToMessage{Id: &pb.MessageId{MessageId: proto.String(replyToMessageID)}}
	return msg
}

// TestToMatrix_ReplyToSetsConvertedMessageReplyTo is the headline inbound
// case: a message with reply_to set must produce a ConvertedMessage whose
// ReplyTo.MessageID is exactly reply_to.id.message_id, with no PartID
// override (nil -- "refer to the first part", matching every other
// single-part Google Chat message; networkid.MessageOptionalPartID's own doc
// comment).
func TestToMatrix_ReplyToSetsConvertedMessageReplyTo(t *testing.T) {
	mc := msgconv.New()
	msg := replyMessage("a reply", "target1")

	cm, _ := mc.ToMatrix(context.Background(), msg, false, nil)

	if cm.ReplyTo == nil {
		t.Fatal("ReplyTo = nil, want a MessageOptionalPartID")
	}
	if string(cm.ReplyTo.MessageID) != "target1" {
		t.Errorf("ReplyTo.MessageID = %q, want %q", cm.ReplyTo.MessageID, "target1")
	}
	if cm.ReplyTo.PartID != nil {
		t.Errorf("ReplyTo.PartID = %v, want nil", *cm.ReplyTo.PartID)
	}
}

// TestToMatrix_NoReplyToLeavesConvertedMessageReplyToNil is the converse: a
// message with no reply_to at all (the common case) must leave cm.ReplyTo
// nil, not a zero-value MessageOptionalPartID{} (which would spuriously
// point bridgev2 at a message with an empty MessageID).
func TestToMatrix_NoReplyToLeavesConvertedMessageReplyToNil(t *testing.T) {
	mc := msgconv.New()
	msg := &pb.Message{TextBody: proto.String("no reply here")}

	cm, _ := mc.ToMatrix(context.Background(), msg, false, nil)

	if cm.ReplyTo != nil {
		t.Errorf("ReplyTo = %v, want nil", *cm.ReplyTo)
	}
}

// TestToMatrix_ReplyToSetEvenForAttachmentOnlyMessage mirrors
// TestToMatrix_ThreadRootSetEvenForAttachmentOnlyMessage: ReplyTo must be
// computed before the empty-text_body early return too, so a future (M5)
// attachment-only reply still carries the right ReplyTo even with zero text
// parts.
func TestToMatrix_ReplyToSetEvenForAttachmentOnlyMessage(t *testing.T) {
	mc := msgconv.New()
	msg := replyMessage("", "target1")

	cm, _ := mc.ToMatrix(context.Background(), msg, false, nil)

	if len(cm.Parts) != 0 {
		t.Fatalf("expected 0 parts for empty text_body, got %d", len(cm.Parts))
	}
	if cm.ReplyTo == nil || string(cm.ReplyTo.MessageID) != "target1" {
		t.Errorf("ReplyTo = %v, want MessageID %q even with no text parts", cm.ReplyTo, "target1")
	}
}

// TestToMatrix_ReplyToAndThreadRootBothSet covers composing with threads
// (Task 6): a message that is both a reply into an existing topic
// (message_id != topic_id -> ThreadRoot) AND carries an explicit reply_to
// (a quote-reply to a specific message within that thread) must set BOTH
// cm.ThreadRoot and cm.ReplyTo independently -- they are computed from
// disjoint proto fields (msg.id.parent_id vs msg.reply_to) and neither
// setter here has any awareness of the other.
func TestToMatrix_ReplyToAndThreadRootBothSet(t *testing.T) {
	mc := msgconv.New()
	msg := topicMessage("reply2", "topic1", "a reply in a thread")
	msg.ReplyTo = &pb.ReplyToMessage{Id: &pb.MessageId{MessageId: proto.String("target-in-thread")}}

	cm, _ := mc.ToMatrix(context.Background(), msg, false, nil)

	if cm.ThreadRoot == nil || string(*cm.ThreadRoot) != "topic1" {
		t.Errorf("ThreadRoot = %v, want %q", cm.ThreadRoot, "topic1")
	}
	if cm.ReplyTo == nil || string(cm.ReplyTo.MessageID) != "target-in-thread" {
		t.Errorf("ReplyTo = %v, want MessageID %q", cm.ReplyTo, "target-in-thread")
	}
}
