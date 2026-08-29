package connector

// media_test.go -- TDD coverage for inbound Google Chat UPLOAD_METADATA
// attachments -> Matrix media parts, media.go. Every network call
// (attachmentURL, downloadAttachment, UploadMediaStream) is injected via a
// fake: no real gchatmeow.Client, no real Matrix homeserver.

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

// --- fakes ------------------------------------------------------------

// fakeMediaFetcher implements mediaFetcher with fully injectable behavior,
// so tests never touch gchatmeow or the network.
type fakeMediaFetcher struct {
	attachmentURLFn      func(meta *pb.UploadMetadata) (string, bool, error)
	downloadAttachmentFn func(ctx context.Context, urlStr string, maxSize int64) ([]byte, string, string, error)
	downloadExternalFn   func(ctx context.Context, urlStr string, maxSize int64) ([]byte, string, string, error)
	maxFileSizeVal       int64
	disableInlineURL     bool

	// Which fetcher each path actually used. The url_metadata path MUST use
	// downloadExternal: downloadAttachment's client keeps an env proxy and
	// permits plain http, concessions that are only safe for Google's own
	// endpoint (gchatmeow/external.go).
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

// TestConvertAttachmentsToMatrix_MimeMismatchUsesDownloadedType is a
// regression test for the case when the
// UPLOAD_METADATA annotation's own declared content_type disagrees with the
// mime type the actual download reports (a stale/wrong annotation, or a
// server that recompresses/transcodes), the resulting Matrix event's
// MsgType and Info.MimeType must come from the DOWNLOADED mime -- never the
// annotation's -- matching convertOneAttachment's own documented contract
// (media.go: "msgType below is derived afresh from the DOWNLOADED
// response's actual Content-Type instead ... NOT the annotation's declared
// content_type"). The annotation here declares "image/png" (which would
// pick m.image if wrongly consulted); the download reports
// "application/pdf" (m.file) -- proving the downloaded type wins.
func TestConvertAttachmentsToMatrix_MimeMismatchUsesDownloadedType(t *testing.T) {
	msg := &pb.Message{
		Annotations: []*pb.Annotation{makeUploadAnnotation("TOK1", "image/png", "mystery")},
	}
	pdfBytes := []byte("%PDF-1.4 fake, but really downloaded as a pdf")
	media := &fakeMediaFetcher{
		downloadAttachmentFn: func(ctx context.Context, urlStr string, maxSize int64) ([]byte, string, string, error) {
			// The download's own Content-Type ("application/pdf") disagrees
			// with the annotation's declared content_type ("image/png").
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
	// The image-decode branch (Width/Height) must not fire either -- it's
	// gated on the same (correct) downloaded MsgType, not the annotation's
	// declared image/png.
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

// TestConvertAttachmentsToMatrix_OversizeSkippedCleanly pins the size-cap
// requirement: a download that reports ErrFileTooLarge (gchatmeow's own
// sentinel) must be skipped -- no part, no error returned, no panic.
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

// TestConvertAttachmentsToMatrix_FilenameFallback pins the filename-fallback
// logic: when the download gives no usable filename ("" or the request
// path's own "get_attachment_url"), fall back to content_name, then to
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

// --- convertMessageToMatrix composition (text + attachments) ----

// TestConvertMessageToMatrix_TextAndAttachmentBothPresent is the headline
// composition test: a message with BOTH text_body and an UPLOAD_METADATA
// annotation gets TWO parts -- "" (text) first, then att_0 -- the text
// message before the attachments.
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

// --- url_metadata (inline external media) ----------------------------------

// makeURLAnnotation builds a url_metadata annotation. length is load-bearing:
// > 0 means it decorates text already in the body, == 0 means a chip attached
// to the message itself.
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

// TestInlineableURLMedia is the anti-scope-creep control. Every reference
// client fetches EVERY url_metadata URL, which makes the bridge a link
// prefetcher that hands the operator's IP to any host a remote party names.
// This gate must stay narrow, and in particular must not fire on an ordinary
// pasted link.
func TestInlineableURLMedia(t *testing.T) {
	tests := []struct {
		name string
		ann  *pb.Annotation
		want bool
	}{
		{
			// The shape an ordinary in-sentence link arrives in: it covers
			// text, and declares no media.
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

// TestConvertAttachmentsToMatrix_URLMediaBecomesImagePart: an inlineable
// url_metadata annotation is downloaded and reuploaded exactly like an
// upload_metadata attachment.
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
	// image_url is preferred over the page url: it is the media itself.
	if gotURL != "https://media.example.com/a.gif" {
		t.Errorf("fetched %q, want the image_url", gotURL)
	}
	// The homeserver's cap must reach the fetch. Passing 0 would mean an
	// unbounded read of a remote-party-served body.
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

// TestConvertOneURLMedia_UsesExternalFetcherNotAttachmentFetcher is the
// security control. downloadAttachment's client keeps an env proxy (which
// reduces the guarded dialer to a no-op) and permits plain http -- both only
// safe for Google's own fixed endpoint. A remote-party URL must never go
// through it.
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

// TestConvertOneURLMedia_NonMediaContentTypeSkipped: the URL was chosen by a
// remote party, so only what we asked for is accepted -- tighter than the
// attachment path's text/html-only rejection.
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

// TestConvertOneURLMedia_FetchFailureProducesNoPart: a third-party host being
// down must cost the media part and nothing else. The link itself survives in
// the body via AppendLinkAnnotations, which is what stops the message
// vanishing (see the gchatfmt tests).
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

// TestConvertAttachmentsToMatrix_MixedUploadAndURLPartIDsAreSequential:
// gcid.MakeAttachmentPartID is FROZEN, so att_<n> must stay ONE namespace
// across both annotation kinds. A second counter would emit duplicate IDs.
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

// TestConvertOneURLMedia_DeclaredDimensionsRescueAnUndecodableFormat: Tenor
// commonly serves webp and mp4, for which no decoder is registered, so
// image.DecodeConfig fails and the dimensions would otherwise be 0 -- leaving
// clients to render an unsized placeholder.
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

// TestConvertAttachmentsToMatrix_InlineURLMediaCanBeDisabled: the operator
// switch must stop the outbound request to the third-party host entirely --
// not merely discard its result.
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

// TestInlineURLMediaEnabledDefaultsOn pins the default: a bare client (and so
// a bridge whose config predates the option) inlines media.
func TestInlineURLMediaEnabledDefaultsOn(t *testing.T) {
	if !(&GChatClient{}).inlineURLMediaEnabled() {
		t.Error("inlineURLMediaEnabled() = false for a nil Main, want true")
	}
	c := &GChatClient{Main: &GChatConnector{Config: Config{DisableInlineURLMedia: true}}}
	if c.inlineURLMediaEnabled() {
		t.Error("inlineURLMediaEnabled() = true despite DisableInlineURLMedia")
	}
}

// TestGChatClientDownloadExternalUsesTheHardenedFetcher pins the WIRING, not
// just the interface. Every other test here goes through the fake, which
// cannot see which fetcher the production method delegates to -- so rewiring
// downloadExternal to the attachment client (env proxy, plain http permitted)
// would pass the entire suite.
//
// No network I/O: a bare client has no conn, so the attachment path would
// answer "not connected", while the hardened path rejects the scheme first.
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

// TestConvertAttachmentsToMatrix_DisablingInlineURLMediaKeepsAttachments: the
// operator switch must not take ordinary Google-hosted attachments with it.
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

// TestConvertAttachmentsToMatrix_ExternalFetchesAreBoundedPerMessage: the
// annotation count is sender-controlled and each fetch blocks the portal's
// event goroutine, so one message must not be able to issue an unbounded
// number of them.
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

// TestConvertAttachmentsToMatrix_RepeatedExternalURLFetchedOnce: the same chip
// repeated across a message costs one request, not N.
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
	// Numbering must still reflect position in the message, not fetch outcome.
	if parts[0].ID != gcid.MakeAttachmentPartID(0) {
		t.Errorf("part.ID = %q, want att_0", parts[0].ID)
	}
}

// TestConvertAttachmentsToMatrix_ExternalFetchesShareOneDeadline: each fetch is
// individually bounded, but the sum is what a portal's event loop and a
// backfill batch care about.
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

// TestConvertOneURLMedia_DeclaredDimensionsReachVideoParts: a GIF host
// commonly serves mp4, which maps to m.video. Gating the declared-dimension
// fallback on m.image would drop them for exactly the case they were added for.
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

// --- the sender must never choose the host ---------------------------------

// TestInlineableURLMedia_NeverFetchesASenderChosenHost is the privacy control.
//
// url_metadata carries two addresses: image_url, which GOOGLE supplies (and,
// in the shape captured live, had already rehosted the media onto its own
// CDN), and url.url, which is whatever the SENDER typed. Fetching the latter
// would let anyone turn a message into an IP-address and delivery-time oracle
// by pasting a link to an image on a host they control.
//
// purple-googlechat -- otherwise far wider than this gate, since it fetches
// every link chip -- has the same restriction: it only ever fetches image_url.
func TestInlineableURLMedia_NeverFetchesASenderChosenHost(t *testing.T) {
	tests := []struct {
		name string
		ann  *pb.Annotation
	}{
		{
			// The direct attack: a link to an image on the sender's host.
			// Under the reading that mime_type describes the linked resource,
			// Google would set image/png here.
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

// TestConvertAttachmentsToMatrix_NoImageURLMakesNoRequest is the end-to-end
// half: no annotation lacking image_url may reach the network at all.
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

// capturedGIFAnnotation reproduces the shape a real shared GIF was observed to
// arrive in, field for field, from a live probe against Google Chat:
//
//	type=URL length=0 mime_type="image/gif" should_not_render=false (present)
//	url_host=media.tenor.com image_url_host=lh3.googleusercontent.com
//	dims=498x280, and the message's text_body was empty
//
// Note type=URL, not IMAGE: the annotation-type arm of the gate does NOT fire
// for a real GIF. Hosts are kept because they are generic CDN names and the
// distinction between them is the whole point; the paths are invented.
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

// TestInlineableURLMedia_AcceptsTheCapturedGIFShape pins the real thing, so a
// future simplification of the gate fails here rather than in production.
func TestInlineableURLMedia_AcceptsTheCapturedGIFShape(t *testing.T) {
	ann := capturedGIFAnnotation()
	if !inlineableURLMedia(ann) {
		t.Fatal("the live-captured GIF shape is no longer accepted")
	}
	// Which arm fires matters: the annotation type is URL, so anyone reducing
	// this gate to the AnnotationType_IMAGE check breaks every real GIF.
	if ann.GetType() == pb.AnnotationType_IMAGE {
		t.Fatal("fixture drifted: the captured annotation type was URL, not IMAGE")
	}
	// And Google's own address is what gets fetched, not the sender's.
	if got := externalMediaSrc(ann.GetUrlMetadata()); got != "https://lh3.googleusercontent.com/example-rehosted" {
		t.Errorf("externalMediaSrc = %q, want Google's rehosted address", got)
	}
}

// TestConvertAttachmentsToMatrix_CapturedGIFBecomesAnImagePart is the
// end-to-end outcome for the captured shape: the media part exists. The
// companion half -- that the message still carries the link, so it can never
// vanish -- is pinned in pkg/msgconv (TestToMatrix_ZeroLengthURLAnnotationOnlyMessageIsNotDropped).
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
