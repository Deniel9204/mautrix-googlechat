package connector

// media_test.go -- TDD coverage for M5 Task 3 (inbound Google Chat
// UPLOAD_METADATA attachments -> Matrix media parts, media.go). Every
// network call (attachmentURL, downloadAttachment, UploadMediaStream) is
// injected via a fake, per this task's brief: no real gchatmeow.Client, no
// real Matrix homeserver.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"testing"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv"
	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv/gchatfmt"
)

// --- fakes ------------------------------------------------------------

// fakeMediaFetcher implements mediaFetcher with fully injectable behavior,
// so tests never touch gchatmeow or the network.
type fakeMediaFetcher struct {
	attachmentURLFn      func(meta *pb.UploadMetadata) (string, bool, error)
	downloadAttachmentFn func(ctx context.Context, urlStr string, maxSize int64) ([]byte, string, string, error)
	maxFileSizeVal       int64
}

func (f *fakeMediaFetcher) attachmentURL(meta *pb.UploadMetadata) (string, bool, error) {
	if f.attachmentURLFn != nil {
		return f.attachmentURLFn(meta)
	}
	return "https://chat.google.com/api/get_attachment_url?attachment_token=" + meta.GetAttachmentToken(), false, nil
}

func (f *fakeMediaFetcher) downloadAttachment(ctx context.Context, urlStr string, maxSize int64) ([]byte, string, string, error) {
	if f.downloadAttachmentFn != nil {
		return f.downloadAttachmentFn(ctx, urlStr, maxSize)
	}
	return nil, "", "", errors.New("fakeMediaFetcher: downloadAttachmentFn not set")
}

func (f *fakeMediaFetcher) maxFileSize() int64 { return f.maxFileSizeVal }

var _ mediaFetcher = (*fakeMediaFetcher)(nil)

// fakeUploadIntent implements bridgev2.MatrixAPI, overriding only
// UploadMediaStream -- same "embed nil interface, override one method"
// pattern as mentions_test.go's fakeMatrixAPI.
type fakeUploadIntent struct {
	bridgev2.MatrixAPI
	uploadFn func(ctx context.Context, roomID id.RoomID, size int64, requireFile bool, cb bridgev2.FileStreamCallback) (id.ContentURIString, *event.EncryptedFileInfo, error)
}

func (f fakeUploadIntent) UploadMediaStream(ctx context.Context, roomID id.RoomID, size int64, requireFile bool, cb bridgev2.FileStreamCallback) (id.ContentURIString, *event.EncryptedFileInfo, error) {
	if f.uploadFn != nil {
		return f.uploadFn(ctx, roomID, size, requireFile, cb)
	}
	var buf bytes.Buffer
	res, err := cb(&buf)
	if err != nil {
		return "", nil, err
	}
	return id.ContentURIString("mxc://example.com/" + res.FileName), nil, nil
}

// uploadIntentRecordingBody is a convenience fakeUploadIntent that records
// the bytes the FileStreamCallback wrote, so tests can assert the
// downloaded bytes actually reached UploadMediaStream unmodified.
func uploadIntentRecordingBody(dst *[]byte) fakeUploadIntent {
	return fakeUploadIntent{
		uploadFn: func(ctx context.Context, roomID id.RoomID, size int64, requireFile bool, cb bridgev2.FileStreamCallback) (id.ContentURIString, *event.EncryptedFileInfo, error) {
			var buf bytes.Buffer
			res, err := cb(&buf)
			if err != nil {
				return "", nil, err
			}
			*dst = buf.Bytes()
			return id.ContentURIString("mxc://example.com/" + res.FileName), nil, nil
		},
	}
}

func testPortal() *bridgev2.Portal {
	return &bridgev2.Portal{
		Portal: &database.Portal{MXID: id.RoomID("!room:example.com")},
		Bridge: &bridgev2.Bridge{Matrix: &fakeMatrixConnector{}},
	}
}

// makeUploadAnnotation builds an UPLOAD_METADATA *pb.Annotation, mirroring
// gchatfmt/utils.go's MakeFormatAnnotation/MakeMentionAnnotation helpers.
func makeUploadAnnotation(token, contentType, contentName string) *pb.Annotation {
	return &pb.Annotation{
		Type: pb.AnnotationType_UPLOAD_METADATA.Enum(),
		Metadata: &pb.Annotation_UploadMetadata{
			UploadMetadata: &pb.UploadMetadata{
				Payload:     &pb.UploadMetadata_AttachmentToken{AttachmentToken: token},
				ContentType: proto.String(contentType),
				ContentName: proto.String(contentName),
			},
		},
	}
}

// onePixelPNG encodes a tiny 3x2 PNG so image.DecodeConfig has real
// dimensions to report.
func onePixelPNG(t *testing.T) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 3, 2))
	img.Set(0, 0, color.RGBA{R: 255, A: 255})
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatalf("png.Encode: %v", err)
	}
	return buf.Bytes()
}

// --- convertOneAttachment / convertAttachmentsToMatrix -----------------

func TestConvertAttachmentsToMatrix_Image(t *testing.T) {
	pngBytes := onePixelPNG(t)
	msg := &pb.Message{
		Annotations: []*pb.Annotation{makeUploadAnnotation("TOK1", "image/png", "photo.png")},
	}
	media := &fakeMediaFetcher{
		downloadAttachmentFn: func(ctx context.Context, urlStr string, maxSize int64) ([]byte, string, string, error) {
			return pngBytes, "image/png", "photo.png", nil
		},
	}
	var uploaded []byte
	intent := uploadIntentRecordingBody(&uploaded)

	parts := convertAttachmentsToMatrix(context.Background(), testPortal(), intent, msg, media)
	if len(parts) != 1 {
		t.Fatalf("len(parts) = %d, want 1", len(parts))
	}
	part := parts[0]
	if part.ID != gcid.MakeAttachmentPartID(0) {
		t.Errorf("part.ID = %q, want att_0", part.ID)
	}
	if part.Content.MsgType != event.MsgImage {
		t.Errorf("MsgType = %q, want m.image", part.Content.MsgType)
	}
	if part.Content.Body != "photo.png" {
		t.Errorf("Body = %q, want photo.png", part.Content.Body)
	}
	if part.Content.Info == nil {
		t.Fatal("Info is nil")
	}
	if part.Content.Info.MimeType != "image/png" {
		t.Errorf("Info.MimeType = %q, want image/png", part.Content.Info.MimeType)
	}
	if part.Content.Info.Size != len(pngBytes) {
		t.Errorf("Info.Size = %d, want %d", part.Content.Info.Size, len(pngBytes))
	}
	if part.Content.Info.Width != 3 || part.Content.Info.Height != 2 {
		t.Errorf("Info = {Width:%d Height:%d}, want {3 2}", part.Content.Info.Width, part.Content.Info.Height)
	}
	if part.Content.URL != "mxc://example.com/photo.png" {
		t.Errorf("Content.URL = %q, want mxc://example.com/photo.png", part.Content.URL)
	}
	if !bytes.Equal(uploaded, pngBytes) {
		t.Error("bytes handed to UploadMediaStream's callback do not match the downloaded bytes")
	}
}

func TestConvertAttachmentsToMatrix_NonImageIsFile(t *testing.T) {
	msg := &pb.Message{
		Annotations: []*pb.Annotation{makeUploadAnnotation("TOK1", "application/pdf", "doc.pdf")},
	}
	media := &fakeMediaFetcher{
		downloadAttachmentFn: func(ctx context.Context, urlStr string, maxSize int64) ([]byte, string, string, error) {
			return []byte("%PDF-1.4 fake"), "application/pdf", "doc.pdf", nil
		},
	}
	parts := convertAttachmentsToMatrix(context.Background(), testPortal(), fakeUploadIntent{}, msg, media)
	if len(parts) != 1 {
		t.Fatalf("len(parts) = %d, want 1", len(parts))
	}
	if parts[0].Content.MsgType != event.MsgFile {
		t.Errorf("MsgType = %q, want m.file", parts[0].Content.MsgType)
	}
}

func TestConvertAttachmentsToMatrix_VideoAndAudioMsgTypes(t *testing.T) {
	cases := []struct {
		mime string
		want event.MessageType
	}{
		{"video/mp4", event.MsgVideo},
		{"audio/ogg", event.MsgAudio},
		{"text/plain", event.MsgFile}, // portal.py:1552-1553's explicit TEXT->FILE override
	}
	for _, tc := range cases {
		msg := &pb.Message{Annotations: []*pb.Annotation{makeUploadAnnotation("TOK", tc.mime, "f")}}
		media := &fakeMediaFetcher{
			downloadAttachmentFn: func(ctx context.Context, urlStr string, maxSize int64) ([]byte, string, string, error) {
				return []byte("data"), tc.mime, "f", nil
			},
		}
		parts := convertAttachmentsToMatrix(context.Background(), testPortal(), fakeUploadIntent{}, msg, media)
		if len(parts) != 1 {
			t.Fatalf("mime %q: len(parts) = %d, want 1", tc.mime, len(parts))
		}
		if parts[0].Content.MsgType != tc.want {
			t.Errorf("mime %q: MsgType = %q, want %q", tc.mime, parts[0].Content.MsgType, tc.want)
		}
	}
}

func TestConvertAttachmentsToMatrix_TwoAttachments(t *testing.T) {
	msg := &pb.Message{
		Annotations: []*pb.Annotation{
			makeUploadAnnotation("TOK1", "image/png", "a.png"),
			makeUploadAnnotation("TOK2", "application/pdf", "b.pdf"),
		},
	}
	call := 0
	media := &fakeMediaFetcher{
		downloadAttachmentFn: func(ctx context.Context, urlStr string, maxSize int64) ([]byte, string, string, error) {
			call++
			if call == 1 {
				return onePixelPNG(t), "image/png", "a.png", nil
			}
			return []byte("pdfdata"), "application/pdf", "b.pdf", nil
		},
	}
	parts := convertAttachmentsToMatrix(context.Background(), testPortal(), fakeUploadIntent{}, msg, media)
	if len(parts) != 2 {
		t.Fatalf("len(parts) = %d, want 2", len(parts))
	}
	if parts[0].ID != gcid.MakeAttachmentPartID(0) {
		t.Errorf("parts[0].ID = %q, want att_0", parts[0].ID)
	}
	if parts[1].ID != gcid.MakeAttachmentPartID(1) {
		t.Errorf("parts[1].ID = %q, want att_1", parts[1].ID)
	}
}

// TestConvertAttachmentsToMatrix_OversizeSkippedCleanly pins the size-cap
// requirement: a download that reports ErrFileTooLarge (gchatmeow's own
// sentinel, Task 1) must be skipped -- no part, no error returned, no panic.
func TestConvertAttachmentsToMatrix_OversizeSkippedCleanly(t *testing.T) {
	msg := &pb.Message{Annotations: []*pb.Annotation{makeUploadAnnotation("TOK1", "image/png", "huge.png")}}
	media := &fakeMediaFetcher{
		maxFileSizeVal: 1024,
		downloadAttachmentFn: func(ctx context.Context, urlStr string, maxSize int64) ([]byte, string, string, error) {
			if maxSize != 1024 {
				t.Errorf("downloadAttachment called with maxSize=%d, want 1024 (the connector's own cap)", maxSize)
			}
			return nil, "", "", fmt.Errorf("googlechat: attachment too large: %w", gchatmeow.ErrFileTooLarge)
		},
	}

	parts := convertAttachmentsToMatrix(context.Background(), testPortal(), fakeUploadIntent{}, msg, media)
	if len(parts) != 0 {
		t.Fatalf("len(parts) = %d, want 0 (oversize attachment must be skipped)", len(parts))
	}
}

// TestConvertAttachmentsToMatrix_TwoAttachmentsFirstOversizeIndexing pins
// the chosen indexing rule: att_<n> counts UPLOAD_METADATA annotations in
// ENCOUNTER order, whether or not each one was actually bridged -- so if the
// FIRST of two attachments is skipped (oversize) and the second succeeds,
// the lone surviving part keeps its original position, att_1, rather than
// being renumbered to att_0.
func TestConvertAttachmentsToMatrix_TwoAttachmentsFirstOversizeIndexing(t *testing.T) {
	msg := &pb.Message{
		Annotations: []*pb.Annotation{
			makeUploadAnnotation("TOK1", "image/png", "huge.png"),
			makeUploadAnnotation("TOK2", "application/pdf", "ok.pdf"),
		},
	}
	call := 0
	media := &fakeMediaFetcher{
		downloadAttachmentFn: func(ctx context.Context, urlStr string, maxSize int64) ([]byte, string, string, error) {
			call++
			if call == 1 {
				return nil, "", "", fmt.Errorf("googlechat: attachment too large: %w", gchatmeow.ErrFileTooLarge)
			}
			return []byte("pdfdata"), "application/pdf", "ok.pdf", nil
		},
	}
	parts := convertAttachmentsToMatrix(context.Background(), testPortal(), fakeUploadIntent{}, msg, media)
	if len(parts) != 1 {
		t.Fatalf("len(parts) = %d, want 1", len(parts))
	}
	if parts[0].ID != gcid.MakeAttachmentPartID(1) {
		t.Errorf("parts[0].ID = %q, want att_1 (encounter-order indexing survives a skip)", parts[0].ID)
	}
}

func TestConvertAttachmentsToMatrix_HTMLAttachmentSkipped(t *testing.T) {
	msg := &pb.Message{Annotations: []*pb.Annotation{makeUploadAnnotation("TOK1", "text/html", "preview")}}
	media := &fakeMediaFetcher{
		downloadAttachmentFn: func(ctx context.Context, urlStr string, maxSize int64) ([]byte, string, string, error) {
			return []byte("<html></html>"), "text/html", "preview", nil
		},
	}
	parts := convertAttachmentsToMatrix(context.Background(), testPortal(), fakeUploadIntent{}, msg, media)
	if len(parts) != 0 {
		t.Fatalf("len(parts) = %d, want 0 (text/html attachments are dropped, portal.py:1547-1549)", len(parts))
	}
}

func TestConvertAttachmentsToMatrix_AttachmentURLErrorSkipped(t *testing.T) {
	msg := &pb.Message{Annotations: []*pb.Annotation{makeUploadAnnotation("TOK1", "image/png", "a.png")}}
	media := &fakeMediaFetcher{
		attachmentURLFn: func(meta *pb.UploadMetadata) (string, bool, error) {
			return "", false, errors.New("boom")
		},
	}
	parts := convertAttachmentsToMatrix(context.Background(), testPortal(), fakeUploadIntent{}, msg, media)
	if len(parts) != 0 {
		t.Fatalf("len(parts) = %d, want 0", len(parts))
	}
}

func TestConvertAttachmentsToMatrix_DownloadErrorSkipped(t *testing.T) {
	msg := &pb.Message{Annotations: []*pb.Annotation{makeUploadAnnotation("TOK1", "image/png", "a.png")}}
	media := &fakeMediaFetcher{
		downloadAttachmentFn: func(ctx context.Context, urlStr string, maxSize int64) ([]byte, string, string, error) {
			return nil, "", "", errors.New("connection reset")
		},
	}
	parts := convertAttachmentsToMatrix(context.Background(), testPortal(), fakeUploadIntent{}, msg, media)
	if len(parts) != 0 {
		t.Fatalf("len(parts) = %d, want 0", len(parts))
	}
}

func TestConvertAttachmentsToMatrix_UploadFailureSkipped(t *testing.T) {
	msg := &pb.Message{Annotations: []*pb.Annotation{makeUploadAnnotation("TOK1", "image/png", "a.png")}}
	media := &fakeMediaFetcher{
		downloadAttachmentFn: func(ctx context.Context, urlStr string, maxSize int64) ([]byte, string, string, error) {
			return onePixelPNG(t), "image/png", "a.png", nil
		},
	}
	intent := fakeUploadIntent{uploadFn: func(ctx context.Context, roomID id.RoomID, size int64, requireFile bool, cb bridgev2.FileStreamCallback) (id.ContentURIString, *event.EncryptedFileInfo, error) {
		return "", nil, errors.New("upload failed")
	}}
	parts := convertAttachmentsToMatrix(context.Background(), testPortal(), intent, msg, media)
	if len(parts) != 0 {
		t.Fatalf("len(parts) = %d, want 0 (a reupload failure must skip, not panic)", len(parts))
	}
}

// TestConvertAttachmentsToMatrix_FilenameFallback pins portal.py:1554-1558:
// when the download gives no usable filename ("" or the request path's own
// "get_attachment_url"), fall back to content_name, then to
// "<msgtype><ext>".
func TestConvertAttachmentsToMatrix_FilenameFallback(t *testing.T) {
	t.Run("uses content_name", func(t *testing.T) {
		msg := &pb.Message{Annotations: []*pb.Annotation{makeUploadAnnotation("TOK1", "image/png", "fallback-name.png")}}
		media := &fakeMediaFetcher{
			downloadAttachmentFn: func(ctx context.Context, urlStr string, maxSize int64) ([]byte, string, string, error) {
				return onePixelPNG(t), "image/png", "get_attachment_url", nil
			},
		}
		parts := convertAttachmentsToMatrix(context.Background(), testPortal(), fakeUploadIntent{}, msg, media)
		if len(parts) != 1 {
			t.Fatalf("len(parts) = %d, want 1", len(parts))
		}
		if parts[0].Content.Body != "fallback-name.png" {
			t.Errorf("Body = %q, want fallback-name.png", parts[0].Content.Body)
		}
	})

	t.Run("generates a name when content_name is also empty", func(t *testing.T) {
		msg := &pb.Message{Annotations: []*pb.Annotation{makeUploadAnnotation("TOK1", "image/png", "")}}
		media := &fakeMediaFetcher{
			downloadAttachmentFn: func(ctx context.Context, urlStr string, maxSize int64) ([]byte, string, string, error) {
				return onePixelPNG(t), "image/png", "", nil
			},
		}
		parts := convertAttachmentsToMatrix(context.Background(), testPortal(), fakeUploadIntent{}, msg, media)
		if len(parts) != 1 {
			t.Fatalf("len(parts) = %d, want 1", len(parts))
		}
		if parts[0].Content.Body == "" {
			t.Error("Body is empty, want a generated fallback filename")
		}
	})
}

func TestConvertAttachmentsToMatrix_NilPortalReturnsNoParts(t *testing.T) {
	msg := &pb.Message{Annotations: []*pb.Annotation{makeUploadAnnotation("TOK1", "image/png", "a.png")}}
	media := &fakeMediaFetcher{}
	parts := convertAttachmentsToMatrix(context.Background(), nil, fakeUploadIntent{}, msg, media)
	if parts != nil {
		t.Errorf("parts = %v, want nil for a nil portal", parts)
	}
}

func TestConvertAttachmentsToMatrix_NilMediaReturnsNoParts(t *testing.T) {
	msg := &pb.Message{Annotations: []*pb.Annotation{makeUploadAnnotation("TOK1", "image/png", "a.png")}}
	parts := convertAttachmentsToMatrix(context.Background(), testPortal(), fakeUploadIntent{}, msg, nil)
	if parts != nil {
		t.Errorf("parts = %v, want nil for a nil mediaFetcher", parts)
	}
}

func TestConvertAttachmentsToMatrix_NonUploadAnnotationsIgnored(t *testing.T) {
	msg := &pb.Message{
		Annotations: []*pb.Annotation{
			{Type: pb.AnnotationType_FORMAT_DATA.Enum()},
		},
	}
	parts := convertAttachmentsToMatrix(context.Background(), testPortal(), fakeUploadIntent{}, msg, &fakeMediaFetcher{})
	if len(parts) != 0 {
		t.Fatalf("len(parts) = %d, want 0 for a message with no UPLOAD_METADATA annotations", len(parts))
	}
}

// --- convertMessageToMatrix composition (M3 B4: text + attachments) ----

// TestConvertMessageToMatrix_TextAndAttachmentBothPresent is the headline
// composition test: a message with BOTH text_body and an UPLOAD_METADATA
// annotation gets TWO parts -- "" (text) first, then att_0 -- matching
// Python's own ordering (portal.py:1411-1433 sends the text message first,
// then processes attachments).
func TestConvertMessageToMatrix_TextAndAttachmentBothPresent(t *testing.T) {
	msg := &pb.Message{
		TextBody:    proto.String("check this out"),
		CreateTime:  proto.Int64(999),
		Annotations: []*pb.Annotation{makeUploadAnnotation("TOK1", "image/png", "a.png")},
	}
	media := &fakeMediaFetcher{
		downloadAttachmentFn: func(ctx context.Context, urlStr string, maxSize int64) ([]byte, string, string, error) {
			return onePixelPNG(t), "image/png", "a.png", nil
		},
	}

	convert := convertMessageToMatrix(msgconv.New(), media)
	cm, err := convert(context.Background(), testPortal(), fakeUploadIntent{}, msg)
	if err != nil {
		t.Fatalf("convertMessageToMatrix returned error: %v", err)
	}
	if len(cm.Parts) != 2 {
		t.Fatalf("len(cm.Parts) = %d, want 2", len(cm.Parts))
	}
	if cm.Parts[0].ID != gcid.TextPartID {
		t.Errorf("cm.Parts[0].ID = %q, want the empty text part ID", cm.Parts[0].ID)
	}
	if cm.Parts[0].Content.Body != "check this out" {
		t.Errorf("cm.Parts[0].Content.Body = %q, want %q", cm.Parts[0].Content.Body, "check this out")
	}
	if cm.Parts[1].ID != gcid.MakeAttachmentPartID(0) {
		t.Errorf("cm.Parts[1].ID = %q, want att_0", cm.Parts[1].ID)
	}
	if cm.Parts[1].Content.MsgType != event.MsgImage {
		t.Errorf("cm.Parts[1].Content.MsgType = %q, want m.image", cm.Parts[1].Content.MsgType)
	}

	// Both parts share the same MessageMetadata (TimestampMicro/TopicID).
	for i, part := range cm.Parts {
		meta, ok := part.DBMetadata.(*MessageMetadata)
		if !ok || meta.TimestampMicro != 999 {
			t.Errorf("cm.Parts[%d].DBMetadata = %+v, want TimestampMicro=999", i, part.DBMetadata)
		}
	}
}

// TestConvertMessageToMatrix_AttachmentOnlyMessage: attachment-only (no
// text_body) yields exactly one part, att_0 -- no empty-body text part
// (mirrors ToMatrix's own text_body-presence gate, from-gchat.go).
func TestConvertMessageToMatrix_AttachmentOnlyMessage(t *testing.T) {
	msg := &pb.Message{
		CreateTime:  proto.Int64(1),
		Annotations: []*pb.Annotation{makeUploadAnnotation("TOK1", "application/pdf", "doc.pdf")},
	}
	media := &fakeMediaFetcher{
		downloadAttachmentFn: func(ctx context.Context, urlStr string, maxSize int64) ([]byte, string, string, error) {
			return []byte("pdfdata"), "application/pdf", "doc.pdf", nil
		},
	}

	convert := convertMessageToMatrix(msgconv.New(), media)
	cm, err := convert(context.Background(), testPortal(), fakeUploadIntent{}, msg)
	if err != nil {
		t.Fatalf("convertMessageToMatrix returned error: %v", err)
	}
	if len(cm.Parts) != 1 {
		t.Fatalf("len(cm.Parts) = %d, want 1", len(cm.Parts))
	}
	if cm.Parts[0].ID != gcid.MakeAttachmentPartID(0) {
		t.Errorf("cm.Parts[0].ID = %q, want att_0", cm.Parts[0].ID)
	}
}

// TestConvertMessageToMatrix_AttachmentPartsDoNotGetTextMentions is the
// double-ping regression: a message with a mention in its text AND an
// attachment must only carry content.Mentions on the TEXT part, not the
// attachment part(s).
func TestConvertMessageToMatrix_AttachmentPartsDoNotGetTextMentions(t *testing.T) {
	portal := &bridgev2.Portal{
		Portal: &database.Portal{MXID: id.RoomID("!room:example.com")},
		Bridge: &bridgev2.Bridge{Matrix: &fakeMatrixConnector{
			ghostIntent: func(gaiaID networkid.UserID) id.UserID { return id.UserID("@" + string(gaiaID) + ":example.com") },
		}},
	}
	msg := &pb.Message{
		TextBody:   proto.String("hi @Bob"), // "@Bob" is [3,7)
		CreateTime: proto.Int64(1),
		Annotations: []*pb.Annotation{
			gchatfmt.MakeMentionAnnotation(3, 4, "200"),
			makeUploadAnnotation("TOK1", "image/png", "a.png"),
		},
	}
	media := &fakeMediaFetcher{
		downloadAttachmentFn: func(ctx context.Context, urlStr string, maxSize int64) ([]byte, string, string, error) {
			return onePixelPNG(t), "image/png", "a.png", nil
		},
	}

	convert := convertMessageToMatrix(msgconv.New(), media)
	cm, err := convert(context.Background(), portal, fakeUploadIntent{}, msg)
	if err != nil {
		t.Fatalf("convertMessageToMatrix returned error: %v", err)
	}
	if len(cm.Parts) != 2 {
		t.Fatalf("len(cm.Parts) = %d, want 2", len(cm.Parts))
	}
	if cm.Parts[0].Content.Mentions == nil || !cm.Parts[0].Content.Mentions.Has("@200:example.com") {
		t.Fatalf("text part Content.Mentions = %+v, want to include @200:example.com (sanity check that this message really does ping)", cm.Parts[0].Content.Mentions)
	}
	if cm.Parts[1].Content.Mentions != nil {
		t.Errorf("attachment part Content.Mentions = %+v, want nil (only the text part should ping)", cm.Parts[1].Content.Mentions)
	}
}
