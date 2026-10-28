package connector

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"strings"
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

type fakeMediaFetcher struct {
	attachmentURLFn      func(meta *pb.UploadMetadata) (string, bool, error)
	downloadAttachmentFn func(ctx context.Context, urlStr string, maxSize int64) ([]byte, string, string, error)
	downloadExternalFn   func(ctx context.Context, urlStr string, maxSize int64) ([]byte, string, string, error)
	maxFileSizeVal       int64
	disableInlineURL     bool

	// The url_metadata path must use downloadExternal, never downloadAttachment.
	usedDownloadAttachment bool
	usedDownloadExternal   bool
}

func (f *fakeMediaFetcher) attachmentURL(meta *pb.UploadMetadata) (string, bool, error) {
	if f.attachmentURLFn != nil {
		return f.attachmentURLFn(meta)
	}
	return "https://chat.google.com/api/get_attachment_url?attachment_token=" + meta.GetAttachmentToken(), false, nil
}

func (f *fakeMediaFetcher) downloadAttachment(ctx context.Context, urlStr string, maxSize int64) ([]byte, string, string, error) {
	f.usedDownloadAttachment = true
	if f.downloadAttachmentFn != nil {
		return f.downloadAttachmentFn(ctx, urlStr, maxSize)
	}
	return nil, "", "", errors.New("fakeMediaFetcher: downloadAttachmentFn not set")
}

func (f *fakeMediaFetcher) downloadExternal(ctx context.Context, urlStr string, maxSize int64) ([]byte, string, string, error) {
	f.usedDownloadExternal = true
	if f.downloadExternalFn != nil {
		return f.downloadExternalFn(ctx, urlStr, maxSize)
	}
	return nil, "", "", errors.New("fakeMediaFetcher: downloadExternalFn not set")
}

func (f *fakeMediaFetcher) maxFileSize() int64 { return f.maxFileSizeVal }

func (f *fakeMediaFetcher) inlineURLMediaEnabled() bool { return !f.disableInlineURL }

var _ mediaFetcher = (*fakeMediaFetcher)(nil)

// fakeUploadIntent implements bridgev2.MatrixAPI, overriding only UploadMediaStream.
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

// uploadIntentRecordingBody records the bytes written to UploadMediaStream.
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

// onePixelPNG encodes a tiny 3x2 PNG so image.DecodeConfig has real dimensions.
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
		{"text/plain", event.MsgFile}, // explicit TEXT->FILE override
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

// TestConvertAttachmentsToMatrix_MimeMismatchUsesDownloadedType: MsgType and MimeType come from the download, not the annotation.
func TestConvertAttachmentsToMatrix_MimeMismatchUsesDownloadedType(t *testing.T) {
	msg := &pb.Message{
		Annotations: []*pb.Annotation{makeUploadAnnotation("TOK1", "image/png", "mystery")},
	}
	pdfBytes := []byte("%PDF-1.4 fake, but really downloaded as a pdf")
	media := &fakeMediaFetcher{
		downloadAttachmentFn: func(ctx context.Context, urlStr string, maxSize int64) ([]byte, string, string, error) {
			// The download disagrees with the annotation's declared image/png.
			return pdfBytes, "application/pdf", "mystery.pdf", nil
		},
	}

	parts := convertAttachmentsToMatrix(context.Background(), testPortal(), fakeUploadIntent{}, msg, media)
	if len(parts) != 1 {
		t.Fatalf("len(parts) = %d, want 1", len(parts))
	}
	part := parts[0]
	if part.Content.MsgType != event.MsgFile {
		t.Errorf("MsgType = %q, want m.file (from the DOWNLOADED mime, not the annotation's declared image/png)", part.Content.MsgType)
	}
	if part.Content.Info == nil || part.Content.Info.MimeType != "application/pdf" {
		t.Errorf("Info.MimeType = %v, want application/pdf (the downloaded mime)", part.Content.Info)
	}
	// Nor may the image-decode branch fire.
	if part.Content.Info.Width != 0 || part.Content.Info.Height != 0 {
		t.Errorf("Info = {Width:%d Height:%d}, want {0 0} -- must not treat a mismatched-mime PDF as an image", part.Content.Info.Width, part.Content.Info.Height)
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

// att_<n> counts annotations in encounter order, so a skipped first attachment leaves the second at att_1.
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
		t.Fatalf("len(parts) = %d, want 0 (text/html attachments are dropped)", len(parts))
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

// Text part comes first, then att_0.
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

	for i, part := range cm.Parts {
		meta, ok := part.DBMetadata.(*MessageMetadata)
		if !ok || meta.TimestampMicro != 999 {
			t.Errorf("cm.Parts[%d].DBMetadata = %+v, want TimestampMicro=999", i, part.DBMetadata)
		}
	}
}

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

// Double-ping regression: only the text part carries content.Mentions.
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

// makeURLAnnotation builds a url_metadata annotation; length 0 means a chip attached to the message.
func makeURLAnnotation(pageURL, imageURL, mimeType string, length int32, shouldNotRender bool) *pb.Annotation {
	return &pb.Annotation{
		Length: proto.Int32(length),
		Metadata: &pb.Annotation_UrlMetadata{
			UrlMetadata: &pb.UrlMetadata{
				Url:             &pb.Url{Url: proto.String(pageURL)},
				ImageUrl:        proto.String(imageURL),
				MimeType:        proto.String(mimeType),
				ShouldNotRender: proto.Bool(shouldNotRender),
			},
		},
	}
}

// TestInlineableURLMedia: the gate must stay narrow and never fire on an ordinary pasted link.
func TestInlineableURLMedia(t *testing.T) {
	tests := []struct {
		name string
		ann  *pb.Annotation
		want bool
	}{
		{
			// An ordinary in-sentence link: covers text, declares no media.
			name: "ordinary pasted link is not fetched",
			ann:  makeURLAnnotation("https://example.com/page", "", "", 24, false),
			want: false,
		},
		{
			name: "a preview image on a link that covers text is not fetched",
			ann:  makeURLAnnotation("https://example.com/page", "https://example.com/preview.png", "", 24, false),
			want: false,
		},
		{
			name: "should_not_render is honoured",
			ann:  makeURLAnnotation("https://example.com/g", "https://media.example.com/a.gif", "image/gif", 0, true),
			want: false,
		},
		{
			name: "no url_metadata at all",
			ann:  makeUploadAnnotation("TOK", "image/png", "a.png"),
			want: false,
		},
		{
			name: "declared image mime type",
			ann:  makeURLAnnotation("https://example.com/g", "https://media.example.com/a.gif", "image/gif", 24, false),
			want: true,
		},
		{
			name: "declared video mime type",
			ann:  makeURLAnnotation("https://example.com/g", "https://media.example.com/a.mp4", "video/mp4", 24, false),
			want: true,
		},
		{
			name: "preview image on a chip covering no text",
			ann:  makeURLAnnotation("https://example.com/g", "https://media.example.com/a.gif", "", 0, false),
			want: true,
		},
		{
			name: "annotation typed IMAGE by the server",
			ann: func() *pb.Annotation {
				a := makeURLAnnotation("https://example.com/g", "https://media.example.com/a", "", 24, false)
				a.Type = pb.AnnotationType_IMAGE.Enum()
				return a
			}(),
			want: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := inlineableURLMedia(tc.ann); got != tc.want {
				t.Errorf("inlineableURLMedia = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestConvertAttachmentsToMatrix_URLMediaBecomesImagePart(t *testing.T) {
	pngBytes := onePixelPNG(t)
	msg := &pb.Message{
		Annotations: []*pb.Annotation{
			makeURLAnnotation("https://example.com/view/gif", "https://media.example.com/a.gif", "", 0, false),
		},
	}
	var gotURL string
	var gotMaxSize int64
	media := &fakeMediaFetcher{
		maxFileSizeVal: 1024,
		downloadExternalFn: func(_ context.Context, urlStr string, maxSize int64) ([]byte, string, string, error) {
			gotURL, gotMaxSize = urlStr, maxSize
			return pngBytes, "image/png", "a.gif", nil
		},
	}
	var uploaded []byte
	parts := convertAttachmentsToMatrix(context.Background(), testPortal(), uploadIntentRecordingBody(&uploaded), msg, media)

	if len(parts) != 1 {
		t.Fatalf("len(parts) = %d, want 1", len(parts))
	}
	// image_url is preferred over the page url.
	if gotURL != "https://media.example.com/a.gif" {
		t.Errorf("fetched %q, want the image_url", gotURL)
	}
	// The homeserver's cap must reach the fetch; 0 would be an unbounded read.
	if gotMaxSize != 1024 {
		t.Errorf("downloadExternal got maxSize %d, want the homeserver cap 1024", gotMaxSize)
	}
	if parts[0].ID != gcid.MakeAttachmentPartID(0) {
		t.Errorf("part.ID = %q, want att_0", parts[0].ID)
	}
	if parts[0].Content.MsgType != event.MsgImage {
		t.Errorf("MsgType = %q, want m.image", parts[0].Content.MsgType)
	}
	if !bytes.Equal(uploaded, pngBytes) {
		t.Error("the reuploaded bytes are not the fetched bytes")
	}
}

// A remote-party URL must never go through downloadAttachment (env proxy, plain http).
func TestConvertOneURLMedia_UsesExternalFetcherNotAttachmentFetcher(t *testing.T) {
	msg := &pb.Message{
		Annotations: []*pb.Annotation{
			makeURLAnnotation("https://example.com/view/gif", "https://media.example.com/a.gif", "image/gif", 0, false),
		},
	}
	media := &fakeMediaFetcher{
		downloadExternalFn: func(context.Context, string, int64) ([]byte, string, string, error) {
			return onePixelPNG(t), "image/png", "a.gif", nil
		},
		downloadAttachmentFn: func(context.Context, string, int64) ([]byte, string, string, error) {
			return onePixelPNG(t), "image/png", "a.gif", nil
		},
	}
	var uploaded []byte
	convertAttachmentsToMatrix(context.Background(), testPortal(), uploadIntentRecordingBody(&uploaded), msg, media)

	if media.usedDownloadAttachment {
		t.Error("a remote-party-chosen URL was fetched with downloadAttachment (env proxy, http permitted)")
	}
	if !media.usedDownloadExternal {
		t.Error("downloadExternal was never called")
	}
}

func TestConvertOneURLMedia_NonMediaContentTypeSkipped(t *testing.T) {
	for _, mimeType := range []string{"text/html", "application/pdf", "application/octet-stream", ""} {
		t.Run(mimeType, func(t *testing.T) {
			msg := &pb.Message{
				Annotations: []*pb.Annotation{
					makeURLAnnotation("https://example.com/p", "https://example.com/p.bin", "", 0, false),
				},
			}
			media := &fakeMediaFetcher{
				downloadExternalFn: func(context.Context, string, int64) ([]byte, string, string, error) {
					return []byte("not media"), mimeType, "p.bin", nil
				},
			}
			var uploaded []byte
			parts := convertAttachmentsToMatrix(context.Background(), testPortal(), uploadIntentRecordingBody(&uploaded), msg, media)
			if len(parts) != 0 {
				t.Errorf("len(parts) = %d, want 0 for content type %q", len(parts), mimeType)
			}
		})
	}
}

// The link itself survives in the body via AppendLinkAnnotations.
func TestConvertOneURLMedia_FetchFailureProducesNoPart(t *testing.T) {
	msg := &pb.Message{
		Annotations: []*pb.Annotation{
			makeURLAnnotation("https://example.com/g", "https://media.example.com/a.gif", "image/gif", 0, false),
		},
	}
	media := &fakeMediaFetcher{
		downloadExternalFn: func(context.Context, string, int64) ([]byte, string, string, error) {
			return nil, "", "", errors.New("tenor is down")
		},
	}
	var uploaded []byte
	parts := convertAttachmentsToMatrix(context.Background(), testPortal(), uploadIntentRecordingBody(&uploaded), msg, media)
	if len(parts) != 0 {
		t.Fatalf("len(parts) = %d, want 0", len(parts))
	}
}

// att_<n> is one frozen namespace across both annotation kinds.
func TestConvertAttachmentsToMatrix_MixedUploadAndURLPartIDsAreSequential(t *testing.T) {
	pngBytes := onePixelPNG(t)
	msg := &pb.Message{
		Annotations: []*pb.Annotation{
			makeUploadAnnotation("TOK1", "image/png", "first.png"),
			makeURLAnnotation("https://example.com/g", "https://media.example.com/a.gif", "image/gif", 0, false),
			makeUploadAnnotation("TOK2", "image/png", "second.png"),
		},
	}
	media := &fakeMediaFetcher{
		downloadAttachmentFn: func(context.Context, string, int64) ([]byte, string, string, error) {
			return pngBytes, "image/png", "upload.png", nil
		},
		downloadExternalFn: func(context.Context, string, int64) ([]byte, string, string, error) {
			return pngBytes, "image/gif", "external.gif", nil
		},
	}
	var uploaded []byte
	parts := convertAttachmentsToMatrix(context.Background(), testPortal(), uploadIntentRecordingBody(&uploaded), msg, media)

	if len(parts) != 3 {
		t.Fatalf("len(parts) = %d, want 3", len(parts))
	}
	seen := map[networkid.PartID]bool{}
	for i, p := range parts {
		if want := gcid.MakeAttachmentPartID(i); p.ID != want {
			t.Errorf("parts[%d].ID = %q, want %q", i, p.ID, want)
		}
		if seen[p.ID] {
			t.Errorf("duplicate part ID %q", p.ID)
		}
		seen[p.ID] = true
	}
}

// No decoder is registered for webp/mp4, so the declared dimensions must be used.
func TestConvertOneURLMedia_DeclaredDimensionsRescueAnUndecodableFormat(t *testing.T) {
	ann := makeURLAnnotation("https://example.com/g", "https://media.example.com/a.webp", "image/webp", 0, false)
	ann.GetUrlMetadata().IntImageWidth = proto.Int32(480)
	ann.GetUrlMetadata().IntImageHeight = proto.Int32(270)
	msg := &pb.Message{Annotations: []*pb.Annotation{ann}}

	media := &fakeMediaFetcher{
		downloadExternalFn: func(context.Context, string, int64) ([]byte, string, string, error) {
			return []byte("RIFF....WEBPnot-really"), "image/webp", "a.webp", nil
		},
	}
	var uploaded []byte
	parts := convertAttachmentsToMatrix(context.Background(), testPortal(), uploadIntentRecordingBody(&uploaded), msg, media)
	if len(parts) != 1 {
		t.Fatalf("len(parts) = %d, want 1", len(parts))
	}
	if got := parts[0].Content.Info; got.Width != 480 || got.Height != 270 {
		t.Errorf("Info = {Width:%d Height:%d}, want {480 270} from the annotation", got.Width, got.Height)
	}
}

// The operator switch must stop the request itself, not just discard the result.
func TestConvertAttachmentsToMatrix_InlineURLMediaCanBeDisabled(t *testing.T) {
	msg := &pb.Message{
		Annotations: []*pb.Annotation{
			makeURLAnnotation("https://example.com/g", "https://media.example.com/a.gif", "image/gif", 0, false),
		},
	}
	media := &fakeMediaFetcher{
		disableInlineURL: true,
		downloadExternalFn: func(context.Context, string, int64) ([]byte, string, string, error) {
			return onePixelPNG(t), "image/gif", "a.gif", nil
		},
	}
	var uploaded []byte
	parts := convertAttachmentsToMatrix(context.Background(), testPortal(), uploadIntentRecordingBody(&uploaded), msg, media)

	if len(parts) != 0 {
		t.Errorf("len(parts) = %d, want 0 when inline URL media is disabled", len(parts))
	}
	if media.usedDownloadExternal {
		t.Error("the third-party host was contacted despite the operator disabling inline URL media")
	}
}

func TestInlineURLMediaEnabledDefaultsOn(t *testing.T) {
	if !(&GChatClient{}).inlineURLMediaEnabled() {
		t.Error("inlineURLMediaEnabled() = false for a nil Main, want true")
	}
	c := &GChatClient{Main: &GChatConnector{Config: Config{DisableInlineURLMedia: true}}}
	if c.inlineURLMediaEnabled() {
		t.Error("inlineURLMediaEnabled() = true despite DisableInlineURLMedia")
	}
}

// TestGChatClientDownloadExternalUsesTheHardenedFetcher pins the production wiring, which the fake cannot see.
// No network I/O: the hardened path rejects the http scheme before any dial.
func TestGChatClientDownloadExternalUsesTheHardenedFetcher(t *testing.T) {
	c := &GChatClient{}
	_, _, _, err := c.downloadExternal(context.Background(), "http://media.example.com/a.gif", 1024)
	if err == nil {
		t.Fatal("a plaintext URL was accepted")
	}
	if strings.Contains(err.Error(), "not connected") {
		t.Fatalf("error = %v; downloadExternal is delegating to the session-backed attachment path, "+
			"which keeps an environment proxy and permits plain http", err)
	}
	if !strings.Contains(err.Error(), "https required") {
		t.Errorf("error = %v, want the external path's scheme rejection", err)
	}
}

func TestConvertAttachmentsToMatrix_DisablingInlineURLMediaKeepsAttachments(t *testing.T) {
	pngBytes := onePixelPNG(t)
	msg := &pb.Message{
		Annotations: []*pb.Annotation{
			makeUploadAnnotation("TOK", "image/png", "a.png"),
			makeURLAnnotation("https://example.com/g", "https://media.example.com/a.gif", "image/gif", 0, false),
		},
	}
	media := &fakeMediaFetcher{
		disableInlineURL: true,
		downloadAttachmentFn: func(context.Context, string, int64) ([]byte, string, string, error) {
			return pngBytes, "image/png", "a.png", nil
		},
		downloadExternalFn: func(context.Context, string, int64) ([]byte, string, string, error) {
			return pngBytes, "image/gif", "a.gif", nil
		},
	}
	var uploaded []byte
	parts := convertAttachmentsToMatrix(context.Background(), testPortal(), uploadIntentRecordingBody(&uploaded), msg, media)

	if len(parts) != 1 {
		t.Fatalf("len(parts) = %d, want 1: disabling inline URL media must not stop ordinary attachments", len(parts))
	}
	if media.usedDownloadExternal {
		t.Error("the third-party host was contacted despite the switch")
	}
	if !media.usedDownloadAttachment {
		t.Error("the Google-hosted attachment was not fetched")
	}
}

func TestConvertAttachmentsToMatrix_ExternalFetchesAreBoundedPerMessage(t *testing.T) {
	pngBytes := onePixelPNG(t)
	var anns []*pb.Annotation
	for i := 0; i < maxExternalMediaPerMessage*5; i++ {
		anns = append(anns, makeURLAnnotation(
			fmt.Sprintf("https://example.com/p%d", i),
			fmt.Sprintf("https://media.example.com/a%d.gif", i),
			"image/gif", 0, false))
	}
	fetches := 0
	media := &fakeMediaFetcher{
		downloadExternalFn: func(context.Context, string, int64) ([]byte, string, string, error) {
			fetches++
			return pngBytes, "image/gif", "a.gif", nil
		},
	}
	var uploaded []byte
	parts := convertAttachmentsToMatrix(context.Background(), testPortal(), uploadIntentRecordingBody(&uploaded), &pb.Message{Annotations: anns}, media)

	if fetches != maxExternalMediaPerMessage {
		t.Errorf("issued %d external fetches, want at most %d", fetches, maxExternalMediaPerMessage)
	}
	if len(parts) != maxExternalMediaPerMessage {
		t.Errorf("len(parts) = %d, want %d", len(parts), maxExternalMediaPerMessage)
	}
}

func TestConvertAttachmentsToMatrix_RepeatedExternalURLFetchedOnce(t *testing.T) {
	pngBytes := onePixelPNG(t)
	ann := makeURLAnnotation("https://example.com/g", "https://media.example.com/same.gif", "image/gif", 0, false)
	msg := &pb.Message{Annotations: []*pb.Annotation{ann, ann, ann}}

	fetches := 0
	media := &fakeMediaFetcher{
		downloadExternalFn: func(context.Context, string, int64) ([]byte, string, string, error) {
			fetches++
			return pngBytes, "image/gif", "same.gif", nil
		},
	}
	var uploaded []byte
	parts := convertAttachmentsToMatrix(context.Background(), testPortal(), uploadIntentRecordingBody(&uploaded), msg, media)

	if fetches != 1 {
		t.Errorf("issued %d fetches for one repeated URL, want 1", fetches)
	}
	if len(parts) != 1 {
		t.Errorf("len(parts) = %d, want 1", len(parts))
	}
	// Numbering reflects position in the message, not fetch outcome.
	if parts[0].ID != gcid.MakeAttachmentPartID(0) {
		t.Errorf("part.ID = %q, want att_0", parts[0].ID)
	}
}

func TestConvertAttachmentsToMatrix_ExternalFetchesShareOneDeadline(t *testing.T) {
	msg := &pb.Message{Annotations: []*pb.Annotation{
		makeURLAnnotation("https://example.com/a", "https://media.example.com/a.gif", "image/gif", 0, false),
	}}
	var gotDeadline bool
	media := &fakeMediaFetcher{
		downloadExternalFn: func(ctx context.Context, _ string, _ int64) ([]byte, string, string, error) {
			_, gotDeadline = ctx.Deadline()
			return onePixelPNG(t), "image/gif", "a.gif", nil
		},
	}
	var uploaded []byte
	convertAttachmentsToMatrix(context.Background(), testPortal(), uploadIntentRecordingBody(&uploaded), msg, media)
	if !gotDeadline {
		t.Error("the external fetch ran with no deadline; nothing bounds the total time one message can spend")
	}
}

// A GIF host often serves mp4 (m.video), which still needs the declared dimensions.
func TestConvertOneURLMedia_DeclaredDimensionsReachVideoParts(t *testing.T) {
	ann := makeURLAnnotation("https://example.com/g", "https://media.example.com/a.mp4", "video/mp4", 0, false)
	ann.GetUrlMetadata().IntImageWidth = proto.Int32(498)
	ann.GetUrlMetadata().IntImageHeight = proto.Int32(280)
	msg := &pb.Message{Annotations: []*pb.Annotation{ann}}

	media := &fakeMediaFetcher{
		downloadExternalFn: func(context.Context, string, int64) ([]byte, string, string, error) {
			return []byte("not-really-an-mp4"), "video/mp4", "a.mp4", nil
		},
	}
	var uploaded []byte
	parts := convertAttachmentsToMatrix(context.Background(), testPortal(), uploadIntentRecordingBody(&uploaded), msg, media)
	if len(parts) != 1 {
		t.Fatalf("len(parts) = %d, want 1", len(parts))
	}
	if parts[0].Content.MsgType != event.MsgVideo {
		t.Fatalf("MsgType = %q, want m.video", parts[0].Content.MsgType)
	}
	if got := parts[0].Content.Info; got.Width != 498 || got.Height != 280 {
		t.Errorf("Info = {Width:%d Height:%d}, want {498 280} from the annotation", got.Width, got.Height)
	}
}

// TestInlineableURLMedia_NeverFetchesASenderChosenHost: only Google-supplied image_url is fetched, never url.url.
func TestInlineableURLMedia_NeverFetchesASenderChosenHost(t *testing.T) {
	tests := []struct {
		name string
		ann  *pb.Annotation
	}{
		{
			// The direct attack: a link to an image on the sender's host.
			name: "a pasted link to an image on the sender's host",
			ann:  makeURLAnnotation("https://sender-controlled.example/tracker.png", "", "image/png", 24, false),
		},
		{
			name: "the same, as a chip covering no text",
			ann:  makeURLAnnotation("https://sender-controlled.example/tracker.png", "", "image/png", 0, false),
		},
		{
			name: "an annotation the server typed IMAGE but gave no image_url",
			ann: func() *pb.Annotation {
				a := makeURLAnnotation("https://sender-controlled.example/x", "", "", 0, false)
				a.Type = pb.AnnotationType_IMAGE.Enum()
				return a
			}(),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if inlineableURLMedia(tc.ann) {
				t.Error("gate accepted an annotation with no Google-supplied image_url")
			}
			if got := externalMediaSrc(tc.ann.GetUrlMetadata()); got != "" {
				t.Errorf("externalMediaSrc = %q, want \"\": url.url is the sender's address and must never be fetched", got)
			}
		})
	}
}

func TestConvertAttachmentsToMatrix_NoImageURLMakesNoRequest(t *testing.T) {
	msg := &pb.Message{Annotations: []*pb.Annotation{
		makeURLAnnotation("https://sender-controlled.example/tracker.png", "", "image/png", 0, false),
	}}
	media := &fakeMediaFetcher{
		downloadExternalFn: func(context.Context, string, int64) ([]byte, string, string, error) {
			return onePixelPNG(t), "image/png", "x.png", nil
		},
	}
	var uploaded []byte
	parts := convertAttachmentsToMatrix(context.Background(), testPortal(), uploadIntentRecordingBody(&uploaded), msg, media)

	if media.usedDownloadExternal {
		t.Error("the sender's own host was contacted")
	}
	if len(parts) != 0 {
		t.Errorf("len(parts) = %d, want 0", len(parts))
	}
}

// capturedGIFAnnotation reproduces a live-captured shared GIF: type=URL (not IMAGE), length=0,
// mime_type image/gif, image_url on lh3.googleusercontent.com. Paths are invented.
func capturedGIFAnnotation() *pb.Annotation {
	return &pb.Annotation{
		Type:   pb.AnnotationType_URL.Enum(),
		Length: proto.Int32(0),
		Metadata: &pb.Annotation_UrlMetadata{UrlMetadata: &pb.UrlMetadata{
			Url:             &pb.Url{Url: proto.String("https://media.tenor.com/example/reaction.gif")},
			ImageUrl:        proto.String("https://lh3.googleusercontent.com/example-rehosted"),
			MimeType:        proto.String("image/gif"),
			ShouldNotRender: proto.Bool(false),
			IntImageWidth:   proto.Int32(498),
			IntImageHeight:  proto.Int32(280),
		}},
	}
}

func TestInlineableURLMedia_AcceptsTheCapturedGIFShape(t *testing.T) {
	ann := capturedGIFAnnotation()
	if !inlineableURLMedia(ann) {
		t.Fatal("the live-captured GIF shape is no longer accepted")
	}
	// The type is URL, so a gate reduced to the AnnotationType_IMAGE check breaks real GIFs.
	if ann.GetType() == pb.AnnotationType_IMAGE {
		t.Fatal("fixture drifted: the captured annotation type was URL, not IMAGE")
	}
	// Google's own address is what gets fetched.
	if got := externalMediaSrc(ann.GetUrlMetadata()); got != "https://lh3.googleusercontent.com/example-rehosted" {
		t.Errorf("externalMediaSrc = %q, want Google's rehosted address", got)
	}
}

// The link part is pinned in pkg/msgconv (TestToMatrix_ZeroLengthURLAnnotationOnlyMessageIsNotDropped).
func TestConvertAttachmentsToMatrix_CapturedGIFBecomesAnImagePart(t *testing.T) {
	media := &fakeMediaFetcher{
		maxFileSizeVal: 1 << 20,
		downloadExternalFn: func(_ context.Context, urlStr string, _ int64) ([]byte, string, string, error) {
			if !strings.HasPrefix(urlStr, "https://lh3.googleusercontent.com/") {
				t.Errorf("fetched %q, want Google's rehosted address", urlStr)
			}
			return onePixelPNG(t), "image/gif", "reaction.gif", nil
		},
	}
	var uploaded []byte
	parts := convertAttachmentsToMatrix(context.Background(), testPortal(),
		uploadIntentRecordingBody(&uploaded),
		&pb.Message{TextBody: proto.String(""), Annotations: []*pb.Annotation{capturedGIFAnnotation()}}, media)

	if len(parts) != 1 {
		t.Fatalf("len(parts) = %d, want 1", len(parts))
	}
	if parts[0].Content.MsgType != event.MsgImage {
		t.Errorf("MsgType = %q, want m.image", parts[0].Content.MsgType)
	}
}
