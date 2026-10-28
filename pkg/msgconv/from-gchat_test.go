package msgconv_test

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv"
	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv/gchatfmt"
)

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

// Only the exact empty string is excluded; whitespace is kept verbatim.
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

// The text part must be appended to cm.Parts, never replace it.
func TestToMatrix_TextPartAppendedNotOverwritten(t *testing.T) {
	mc := msgconv.New()
	msg := &pb.Message{TextBody: proto.String("a caption")}

	cm, _ := mc.ToMatrix(context.Background(), msg, false, nil)
	if len(cm.Parts) != 1 {
		t.Fatalf("expected 1 text part, got %d", len(cm.Parts))
	}

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

// ThreadRoot: message_id == topic_id marks a topic head; replies differ.

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

func TestToMatrix_HeadMessageFlatRoomNoThreadRoot(t *testing.T) {
	mc := msgconv.New()
	msg := topicMessage("topic1", "topic1", "hello")

	cm, _ := mc.ToMatrix(context.Background(), msg, false, nil)

	if cm.ThreadRoot != nil {
		t.Errorf("ThreadRoot = %v, want nil for a head message in a non-threads-only room", *cm.ThreadRoot)
	}
}

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

// Replies get ThreadRoot regardless of threadsOnly.
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

// ThreadRoot is computed before the empty-text early return.
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

// ReplyTo comes from msg.reply_to.id.message_id (proto field 37 -> 1 -> 2).

func replyMessage(text, replyToMessageID string) *pb.Message {
	msg := &pb.Message{TextBody: proto.String(text)}
	msg.ReplyTo = &pb.ReplyToMessage{Id: &pb.MessageId{MessageId: proto.String(replyToMessageID)}}
	return msg
}

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

// Must be nil, not a zero-value MessageOptionalPartID.
func TestToMatrix_NoReplyToLeavesConvertedMessageReplyToNil(t *testing.T) {
	mc := msgconv.New()
	msg := &pb.Message{TextBody: proto.String("no reply here")}

	cm, _ := mc.ToMatrix(context.Background(), msg, false, nil)

	if cm.ReplyTo != nil {
		t.Errorf("ReplyTo = %v, want nil", *cm.ReplyTo)
	}
}

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

// ThreadRoot and ReplyTo come from disjoint fields and must both be set.
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

func driveMetadataAnnotation(id string) *pb.Annotation {
	return &pb.Annotation{
		Metadata: &pb.Annotation_DriveMetadata{
			DriveMetadata: &pb.DriveMetadata{Id: proto.String(id)},
		},
	}
}

func youtubeMetadataAnnotation(id string) *pb.Annotation {
	return &pb.Annotation{
		Metadata: &pb.Annotation_YoutubeMetadata{
			YoutubeMetadata: &pb.YoutubeMetadata{Id: proto.String(id)},
		},
	}
}

func videoCallMetadataAnnotation(meetingURL string) *pb.Annotation {
	return &pb.Annotation{
		Metadata: &pb.Annotation_VideoCallMetadata{
			VideoCallMetadata: &pb.VideoCallMetadata{
				MeetingSpace: &pb.MeetingSpace{MeetingUrl: proto.String(meetingURL)},
			},
		},
	}
}

func TestToMatrix_DriveMetadataAppendsURLToBody(t *testing.T) {
	mc := msgconv.New()
	msg := &pb.Message{
		TextBody:    proto.String("check this doc"),
		Annotations: []*pb.Annotation{driveMetadataAnnotation("doc1")},
	}

	cm, _ := mc.ToMatrix(context.Background(), msg, false, nil)

	if len(cm.Parts) != 1 {
		t.Fatalf("expected 1 part, got %d", len(cm.Parts))
	}
	want := "check this doc\n\nhttps://drive.google.com/open?id=doc1"
	if got := cm.Parts[0].Content.Body; got != want {
		t.Errorf("Body = %q, want %q", got, want)
	}
}

func TestToMatrix_YoutubeMetadataAppendsWatchURLToBody(t *testing.T) {
	mc := msgconv.New()
	msg := &pb.Message{
		TextBody:    proto.String("watch this"),
		Annotations: []*pb.Annotation{youtubeMetadataAnnotation("dQw4w9WgXcQ")},
	}

	cm, _ := mc.ToMatrix(context.Background(), msg, false, nil)

	if len(cm.Parts) != 1 {
		t.Fatalf("expected 1 part, got %d", len(cm.Parts))
	}
	want := "watch this\n\nhttps://www.youtube.com/watch?v=dQw4w9WgXcQ"
	if got := cm.Parts[0].Content.Body; got != want {
		t.Errorf("Body = %q, want %q", got, want)
	}
}

func TestToMatrix_VideoCallMetadataAppendsMeetingURLToBody(t *testing.T) {
	mc := msgconv.New()
	msg := &pb.Message{
		TextBody:    proto.String("join the call"),
		Annotations: []*pb.Annotation{videoCallMetadataAnnotation("https://meet.google.com/abc-defg-hij")},
	}

	cm, _ := mc.ToMatrix(context.Background(), msg, false, nil)

	if len(cm.Parts) != 1 {
		t.Fatalf("expected 1 part, got %d", len(cm.Parts))
	}
	want := "join the call\n\nhttps://meet.google.com/abc-defg-hij"
	if got := cm.Parts[0].Content.Body; got != want {
		t.Errorf("Body = %q, want %q", got, want)
	}
}

func TestToMatrix_DriveMetadataAlreadyPresentSkipsAppend(t *testing.T) {
	mc := msgconv.New()
	text := "see https://drive.google.com/open?id=doc1 for the doc"
	msg := &pb.Message{
		TextBody:    proto.String(text),
		Annotations: []*pb.Annotation{driveMetadataAnnotation("doc1")},
	}

	cm, _ := mc.ToMatrix(context.Background(), msg, false, nil)

	if len(cm.Parts) != 1 {
		t.Fatalf("expected 1 part, got %d", len(cm.Parts))
	}
	if got := cm.Parts[0].Content.Body; got != text {
		t.Errorf("Body = %q, want unchanged %q", got, text)
	}
}

// Link appending runs before the empty-text gate, so this must yield one part.
func TestToMatrix_EmptyTextBodyWithDriveAnnotationProducesTextPart(t *testing.T) {
	mc := msgconv.New()
	msg := &pb.Message{
		TextBody:    proto.String(""),
		Annotations: []*pb.Annotation{driveMetadataAnnotation("doc1")},
	}

	cm, _ := mc.ToMatrix(context.Background(), msg, false, nil)

	if len(cm.Parts) != 1 {
		t.Fatalf("expected 1 part for an empty text_body with a drive_metadata annotation, got %d", len(cm.Parts))
	}
	want := "https://drive.google.com/open?id=doc1"
	if got := cm.Parts[0].Content.Body; got != want {
		t.Errorf("Body = %q, want %q (set, not appended with a leading separator)", got, want)
	}
}

// An inline DO_NOT_RENDER url_metadata must not also be appended to the body.
func TestToMatrix_UrlMetadataInlineAnnotationNotDoubleAppended(t *testing.T) {
	mc := msgconv.New()
	text := "click here"
	msg := &pb.Message{
		TextBody: proto.String(text),
		Annotations: []*pb.Annotation{
			gchatfmt.MakeURLAnnotation(0, int32(len(text)), "https://example.com/page", pb.Annotation_DO_NOT_RENDER),
		},
	}

	cm, _ := mc.ToMatrix(context.Background(), msg, false, nil)

	if len(cm.Parts) != 1 {
		t.Fatalf("expected 1 part, got %d", len(cm.Parts))
	}
	part := cm.Parts[0]
	if part.Content.Body != text {
		t.Errorf("Body = %q, want unchanged %q (url_metadata must never be appended)", part.Content.Body, text)
	}
	wantHTML := `<a href="https://example.com/page">click here</a>`
	if part.Content.FormattedBody != wantHTML {
		t.Errorf("FormattedBody = %q, want %q", part.Content.FormattedBody, wantHTML)
	}
	if got := strings.Count(part.Content.FormattedBody, "https://example.com/page"); got != 1 {
		t.Errorf("the URL appears %d times in FormattedBody, want exactly 1 (no double-render)", got)
	}
}

// A RENDER chip url_metadata is neither appended nor rendered inline.
func TestToMatrix_UrlMetadataRenderChipNeverAppended(t *testing.T) {
	mc := msgconv.New()
	text := "check out this link"
	ann := gchatfmt.MakeURLAnnotation(0, int32(len(text)), "https://example.com/page", pb.Annotation_RENDER)
	ann.GetUrlMetadata().ImageUrl = proto.String("https://example.com/preview.png")
	msg := &pb.Message{
		TextBody:    proto.String(text),
		Annotations: []*pb.Annotation{ann},
	}

	cm, _ := mc.ToMatrix(context.Background(), msg, false, nil)

	if len(cm.Parts) != 1 {
		t.Fatalf("expected 1 part, got %d", len(cm.Parts))
	}
	if got := cm.Parts[0].Content.Body; got != text {
		t.Errorf("Body = %q, want unchanged %q", got, text)
	}
}

func TestToMatrix_NoLinkAnnotationsBodyUnchanged(t *testing.T) {
	mc := msgconv.New()
	msg := &pb.Message{TextBody: proto.String("hello world")}

	cm, _ := mc.ToMatrix(context.Background(), msg, false, nil)

	if len(cm.Parts) != 1 {
		t.Fatalf("expected 1 part, got %d", len(cm.Parts))
	}
	if got := cm.Parts[0].Content.Body; got != "hello world" {
		t.Errorf("Body = %q, want unchanged %q", got, "hello world")
	}
}

// cardOnlyAttachment builds a bot card with no text_body.
func cardOnlyAttachment(title, para, btnLabel, btnURL string) *pb.Attachment {
	ft := func(s string) *pb.JAddOnsFormattedText {
		return &pb.JAddOnsFormattedText{OriginalText: proto.String(s)}
	}
	return &pb.Attachment{
		Type: &pb.Attachment_AddOnData{AddOnData: &pb.JAddOnsContextualAddOn{
			Cards: []*pb.JAddOnsContextualAddOn_Card{{
				Header: &pb.JAddOnsContextualAddOn_Card_CardHeader{Title: ft(title)},
				Sections: []*pb.JAddOnsContextualAddOn_Card_Section{{
					Widgets: []*pb.JAddOnsWidget{
						{Data: &pb.JAddOnsWidget_TextParagraph_{
							TextParagraph: &pb.JAddOnsWidget_TextParagraph{Text: ft(para)},
						}},
						{Buttons: []*pb.JAddOnsWidget_Button{{
							Type: &pb.JAddOnsWidget_Button_TextButton{
								TextButton: &pb.JAddOnsWidget_TextButton{
									Text: ft(btnLabel),
									OnClick: &pb.JAddOnsOnClick{
										DataCase: &pb.JAddOnsOnClick_OpenLink{
											OpenLink: &pb.JAddOnsOpenLink{Url: proto.String(btnURL)},
										},
									},
								},
							},
						}}},
					},
				}},
			}},
		}},
	}
}

func TestToMatrix_CardOnlyMessageProducesAPart(t *testing.T) {
	mc := msgconv.New()
	msg := &pb.Message{
		Attachments: []*pb.Attachment{
			cardOnlyAttachment("Build failed", "3 tests failed", "Open build", "https://ci.example.org/b/1"),
		},
	}

	cm, _ := mc.ToMatrix(context.Background(), msg, false, nil)

	if len(cm.Parts) != 1 {
		t.Fatalf("got %d parts, want 1 -- a card-only message must still bridge", len(cm.Parts))
	}
	content := cm.Parts[0].Content
	for _, want := range []string{"Build failed", "3 tests failed", "Open build"} {
		if !strings.Contains(content.Body, want) {
			t.Errorf("body missing %q\nbody = %q", want, content.Body)
		}
	}
	if content.Format != event.FormatHTML || content.FormattedBody == "" {
		t.Errorf("card content did not produce HTML: format=%q formatted_body=%q", content.Format, content.FormattedBody)
	}
	if !strings.Contains(content.FormattedBody, `href="https://ci.example.org/b/1"`) {
		t.Errorf("formatted_body missing the button link\nhtml = %q", content.FormattedBody)
	}
}

func TestToMatrix_TextAndCardAreBothKept(t *testing.T) {
	mc := msgconv.New()
	msg := &pb.Message{
		TextBody:    proto.String("heads up"),
		Attachments: []*pb.Attachment{cardOnlyAttachment("Deploy", "prod is live", "View", "https://ex.example/v")},
	}

	cm, _ := mc.ToMatrix(context.Background(), msg, false, nil)

	if len(cm.Parts) != 1 {
		t.Fatalf("got %d parts, want 1", len(cm.Parts))
	}
	content := cm.Parts[0].Content
	for _, want := range []string{"heads up", "Deploy", "prod is live"} {
		if !strings.Contains(content.Body, want) {
			t.Errorf("body missing %q\nbody = %q", want, content.Body)
		}
	}
	// The unannotated text half must still be escaped into formatted_body.
	if !strings.Contains(content.FormattedBody, "heads up") {
		t.Errorf("formatted_body lost the plain text half\nhtml = %q", content.FormattedBody)
	}
}

func TestToMatrix_NoTextNoCardStillEmpty(t *testing.T) {
	mc := msgconv.New()
	cm, _ := mc.ToMatrix(context.Background(), &pb.Message{}, false, nil)
	if len(cm.Parts) != 0 {
		t.Errorf("got %d parts for an empty message, want 0", len(cm.Parts))
	}
}

// A text-less message with only a zero-length url_metadata (shared GIF) must not convert to zero parts.
func TestToMatrix_ZeroLengthURLAnnotationOnlyMessageIsNotDropped(t *testing.T) {
	const gifURL = "https://tenor.com/view/example-gif-12345"
	mc := msgconv.New()
	msg := &pb.Message{
		TextBody: proto.String(""),
		Annotations: []*pb.Annotation{{
			Length: proto.Int32(0),
			Metadata: &pb.Annotation_UrlMetadata{UrlMetadata: &pb.UrlMetadata{
				Url:      &pb.Url{Url: proto.String(gifURL)},
				ImageUrl: proto.String("https://media.example.com/example.gif"),
			}},
		}},
	}

	cm, _ := mc.ToMatrix(context.Background(), msg, false, nil)
	if cm == nil {
		t.Fatal("ToMatrix returned nil")
	}
	if len(cm.Parts) != 1 {
		t.Fatalf("got %d parts, want 1 -- zero parts means mautrix-go sends nothing and the message is silently dropped", len(cm.Parts))
	}
	if got := cm.Parts[0].Content.Body; got != gifURL {
		t.Errorf("body = %q, want the URL %q", got, gifURL)
	}
}
