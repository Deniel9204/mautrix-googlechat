package connector

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/exmime"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// mediaFetcher is convertMessageToMatrix's seam onto attachment download and reupload.
type mediaFetcher interface {
	// attachmentURL builds the download URL for an UPLOAD_METADATA file.
	attachmentURL(meta *pb.UploadMetadata) (url string, isImage bool, err error)
	// downloadAttachment fetches the file, capped at maxSize.
	downloadAttachment(ctx context.Context, urlStr string, maxSize int64) (data []byte, mimeType string, filename string, err error)
	// downloadExternal fetches a sender-chosen URL via the hardened path; never use downloadAttachment
	// for it, whose client keeps an env proxy and permits plain http.
	downloadExternal(ctx context.Context, urlStr string, maxSize int64) (data []byte, mimeType string, filename string, err error)
	// maxFileSize is the homeserver's upload_size cap.
	maxFileSize() int64
	// inlineURLMediaEnabled reports whether remote-party media may be fetched (DisableInlineURLMedia).
	inlineURLMediaEnabled() bool
}

var _ mediaFetcher = (*GChatClient)(nil)

// attachmentURL implements mediaFetcher via the live gchatmeow.Client.
func (c *GChatClient) attachmentURL(meta *pb.UploadMetadata) (string, bool, error) {
	conn := c.getConn()
	if conn == nil {
		return "", false, errors.New("googlechat: not connected")
	}
	return conn.AttachmentURL(meta)
}

// downloadAttachment implements mediaFetcher via the live gchatmeow.Client.
func (c *GChatClient) downloadAttachment(ctx context.Context, urlStr string, maxSize int64) ([]byte, string, string, error) {
	conn := c.getConn()
	if conn == nil {
		return nil, "", "", errors.New("googlechat: not connected")
	}
	return conn.DownloadAttachment(ctx, urlStr, maxSize)
}

// downloadExternal implements mediaFetcher. It deliberately skips getConn: the fetch is session-less,
// so no Google credential can reach a third-party host.
func (c *GChatClient) downloadExternal(ctx context.Context, urlStr string, maxSize int64) ([]byte, string, string, error) {
	return gchatmeow.DownloadExternalMedia(ctx, urlStr, maxSize)
}

// inlineURLMediaEnabled implements mediaFetcher; enabled when Main is nil.
func (c *GChatClient) inlineURLMediaEnabled() bool {
	return c.Main == nil || !c.Main.Config.DisableInlineURLMedia
}

// maxFileSize returns the connector-wide upload cap, or 0 (no cap) when Main is nil.
func (c *GChatClient) maxFileSize() int64 {
	if c.Main != nil {
		return c.Main.MaxFileSize
	}
	return 0
}

// msgTypeFromMime maps image/video/audio MIME prefixes to their msgtype and everything else to m.file.
func msgTypeFromMime(mimeType string) event.MessageType {
	prefix, _, _ := strings.Cut(mimeType, "/")
	switch prefix {
	case "image":
		return event.MsgImage
	case "video":
		return event.MsgVideo
	case "audio":
		return event.MsgAudio
	default:
		return event.MsgFile
	}
}

// convertAttachmentsToMatrix downloads and reuploads msg's UPLOAD_METADATA and inlineable url_metadata
// annotations. Per-attachment failures are logged and skipped without dropping the rest of the message.
func convertAttachmentsToMatrix(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, msg *pb.Message, media mediaFetcher) []*bridgev2.ConvertedMessagePart {
	if portal == nil || media == nil {
		return nil
	}
	log := zerolog.Ctx(ctx)
	var parts []*bridgev2.ConvertedMessagePart
	// One counter across both kinds: att_<n> is a single frozen namespace, numbered by encounter order.
	index := 0
	// The index is consumed before any skip, so numbering never depends on a runtime outcome.
	externalFetches := 0
	externalSeen := make(map[string]bool)
	mediaCtx := ctx
	if media.inlineURLMediaEnabled() && hasInlineableURLMedia(msg) {
		var cancel context.CancelFunc
		mediaCtx, cancel = context.WithTimeout(ctx, externalMediaMessageBudget)
		defer cancel()
	}

	for _, ann := range msg.GetAnnotations() {
		var (
			part *bridgev2.ConvertedMessagePart
			ok   bool
		)
		switch {
		case ann.GetUploadMetadata() != nil:
			partIndex := index
			index++
			part, ok = convertOneAttachment(ctx, portal, intent, ann.GetUploadMetadata(), partIndex, media)
		case media.inlineURLMediaEnabled() && inlineableURLMedia(ann):
			partIndex := index
			index++

			src := externalMediaSrc(ann.GetUrlMetadata())
			if src == "" || externalSeen[src] {
				continue
			}
			if externalFetches >= maxExternalMediaPerMessage {
				log.Debug().Int("limit", maxExternalMediaPerMessage).
					Msg("googlechat: message exceeds the per-message external media limit, leaving the rest as links")
				continue
			}
			externalSeen[src] = true
			externalFetches++
			part, ok = convertOneURLMedia(mediaCtx, portal, intent, ann.GetUrlMetadata(), partIndex, media)
		default:
			continue
		}
		if ok {
			parts = append(parts, part)
		}
	}
	return parts
}

const (
	// maxExternalMediaPerMessage caps sender-controlled external fetches, which block the portal's event goroutine.
	maxExternalMediaPerMessage = 4

	// externalMediaMessageBudget bounds the total time one message may spend on external fetches.
	externalMediaMessageBudget = 30 * time.Second
)

func hasInlineableURLMedia(msg *pb.Message) bool {
	for _, ann := range msg.GetAnnotations() {
		if inlineableURLMedia(ann) {
			return true
		}
	}
	return false
}

// externalMediaSrc returns the URL to fetch for m: image_url only, never the sender-chosen url.url.
func externalMediaSrc(m *pb.UrlMetadata) string {
	return m.GetImageUrl()
}

// inlineableURLMedia reports whether a url_metadata annotation names media worth fetching.
// image_url is required: it is Google-supplied, whereas url.url is whatever the sender typed.
// A real shared GIF has type=URL (not IMAGE), length=0 and mime_type image/gif.
func inlineableURLMedia(a *pb.Annotation) bool {
	m := a.GetUrlMetadata()
	if m == nil || m.GetShouldNotRender() {
		return false
	}
	// Without a Google-supplied address, every arm below would accept a sender-chosen host.
	if m.GetImageUrl() == "" {
		return false
	}
	if a.GetType() == pb.AnnotationType_IMAGE {
		return true
	}
	if t := m.GetMimeType(); strings.HasPrefix(t, "image/") || strings.HasPrefix(t, "video/") {
		return true
	}
	return a.GetLength() == 0
}

// convertOneURLMedia builds an att_<index> part from a url_metadata annotation, or ok=false to skip;
// the URL is already in the body as a link.
func convertOneURLMedia(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, meta *pb.UrlMetadata, index int, media mediaFetcher) (*bridgev2.ConvertedMessagePart, bool) {
	log := zerolog.Ctx(ctx)

	src := externalMediaSrc(meta)
	if src == "" {
		return nil, false
	}

	data, mimeType, filename, err := media.downloadExternal(ctx, src, media.maxFileSize())
	if err != nil {
		// Third-party host failures are ordinary and must not cost the message.
		log.Debug().Err(err).Msg("googlechat: failed to fetch external media, leaving the link in the body")
		return nil, false
	}

	// Stricter than the upload path: the URL was chosen by a remote party.
	if !strings.HasPrefix(mimeType, "image/") && !strings.HasPrefix(mimeType, "video/") {
		log.Debug().Str("mime", mimeType).Msg("googlechat: external URL was not media, leaving the link in the body")
		return nil, false
	}

	return buildMediaPart(ctx, portal, intent, mediaPartInput{
		data:     data,
		mimeType: mimeType,
		filename: filename,
		// Declared dimensions are used only when decoding fails (e.g. webp, mp4).
		fallbackWidth:  int(meta.GetIntImageWidth()),
		fallbackHeight: int(meta.GetIntImageHeight()),
		index:          index,
	})
}

// convertOneAttachment builds an att_<index> part from meta, or ok=false to skip it.
func convertOneAttachment(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, meta *pb.UploadMetadata, index int, media mediaFetcher) (*bridgev2.ConvertedMessagePart, bool) {
	log := zerolog.Ctx(ctx)

	// isImage is ignored: msgType comes from the downloaded Content-Type, which may differ from the annotation's.
	urlStr, _, err := media.attachmentURL(meta)
	if err != nil {
		log.Warn().Err(err).Msg("googlechat: failed to build attachment URL, skipping attachment")
		return nil, false
	}

	data, mimeType, filename, err := media.downloadAttachment(ctx, urlStr, media.maxFileSize())
	if err != nil {
		if errors.Is(err, gchatmeow.ErrFileTooLarge) {
			log.Warn().Err(err).Msg("googlechat: attachment too large to bridge, skipping")
		} else {
			log.Warn().Err(err).Msg("googlechat: failed to download attachment, skipping")
		}
		return nil, false
	}

	if strings.HasPrefix(mimeType, "text/html") {
		log.Debug().Str("mime", mimeType).Msg("googlechat: ignoring HTML attachment")
		return nil, false
	}

	return buildMediaPart(ctx, portal, intent, mediaPartInput{
		data:        data,
		mimeType:    mimeType,
		filename:    filename,
		contentName: meta.GetContentName(),
		index:       index,
	})
}

// mediaPartInput is buildMediaPart's argument set.
type mediaPartInput struct {
	data     []byte
	mimeType string
	filename string
	// contentName is the annotation's declared name; empty for url_metadata.
	contentName string
	// fallbackWidth/Height are declared dimensions used when decoding fails (url_metadata only).
	fallbackWidth, fallbackHeight int
	index                         int
}

// buildMediaPart reuploads downloaded bytes and returns an att_<index> Matrix part.
func buildMediaPart(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, in mediaPartInput) (*bridgev2.ConvertedMessagePart, bool) {
	log := zerolog.Ctx(ctx)
	data, mimeType, filename := in.data, in.mimeType, in.filename

	msgType := msgTypeFromMime(mimeType)

	// "get_attachment_url" is the request path's last segment, not a real name.
	if filename == "" || filename == "get_attachment_url" {
		if in.contentName != "" {
			filename = in.contentName
		} else {
			filename = string(msgType) + exmime.ExtensionFromMimetype(mimeType)
		}
	}

	content := &event.MessageEventContent{
		MsgType: msgType,
		Body:    filename,
		Info: &event.FileInfo{
			MimeType: mimeType,
			Size:     len(data),
		},
	}

	// Best-effort dimensions; only gif/jpeg/png decoders are registered.
	if msgType == event.MsgImage {
		if cfg, _, decErr := image.DecodeConfig(bytes.NewReader(data)); decErr == nil {
			content.Info.Width = cfg.Width
			content.Info.Height = cfg.Height
		}
	}
	// Outside the image branch on purpose: an mp4 is m.video but still needs declared dimensions.
	if content.Info.Width == 0 && content.Info.Height == 0 {
		content.Info.Width = in.fallbackWidth
		content.Info.Height = in.fallbackHeight
	}

	// UploadMediaStream handles encrypted rooms itself (empty url, populated file).
	mxcURL, file, err := intent.UploadMediaStream(ctx, portal.MXID, int64(len(data)), false, func(w io.Writer) (*bridgev2.FileStreamResult, error) {
		if _, werr := w.Write(data); werr != nil {
			return nil, werr
		}
		return &bridgev2.FileStreamResult{FileName: filename, MimeType: mimeType}, nil
	})
	if err != nil {
		log.Warn().Err(err).Msg("googlechat: failed to reupload attachment to matrix, skipping")
		return nil, false
	}
	content.URL = mxcURL
	content.File = file

	return &bridgev2.ConvertedMessagePart{
		ID:      gcid.MakeAttachmentPartID(in.index),
		Type:    event.EventMessage,
		Content: content,
	}, true
}

// Outbound (Matrix -> Google Chat) media.

// isOutboundMediaMsgType reports whether t is an outbound media msgtype; must match capabilities.go's gchatFile.
// Stickers are excluded: Google Chat uploads have no sticker concept.
func isOutboundMediaMsgType(t event.MessageType) bool {
	switch t {
	case event.MsgImage, event.MsgVideo, event.MsgAudio, event.MsgFile:
		return true
	default:
		return false
	}
}

// hasOutboundCaption reports whether Body is a genuine caption rather than the filename,
// using the same rule as bridgev2's checkMessageContentCaps.
func hasOutboundCaption(content *event.MessageEventContent) bool {
	return content.FileName != "" && content.Body != content.FileName
}

// uploadFilename prefers FileName (MSC2530) and falls back to Body.
func uploadFilename(content *event.MessageEventContent) string {
	if content.FileName != "" {
		return content.FileName
	}
	return content.Body
}

// buildUploadAnnotation downloads msg's Matrix media, uploads it via UploadFile, and returns an
// UPLOAD_METADATA/RENDER annotation. UploadFile takes the plain numeric group.ID, not a *pb.GroupId.
// Failure is fatal for the send: never fall back to text-only and lose the file.
func (c *GChatClient) buildUploadAnnotation(ctx context.Context, msg *bridgev2.MatrixMessage, group gcid.GroupID) (*pb.Annotation, error) {
	data, err := c.downloadMatrixMedia(ctx, msg)
	if err != nil {
		return nil, fmt.Errorf("googlechat: failed to download matrix media: %w", err)
	}

	upload := c.uploadFileFn
	if upload == nil {
		conn := c.getConn()
		if conn == nil {
			return nil, errors.New("googlechat: not connected")
		}
		upload = conn.UploadFile
	}

	var mimeType string
	if msg.Content.Info != nil {
		mimeType = msg.Content.Info.MimeType
	}
	meta, err := upload(ctx, group.ID, data, uploadFilename(msg.Content), mimeType)
	if err != nil {
		return nil, fmt.Errorf("googlechat: failed to upload media to Google Chat (see https://github.com/mautrix/googlechat/issues/114): %w", err)
	}

	return &pb.Annotation{
		Type:           pb.AnnotationType_UPLOAD_METADATA.Enum(),
		ChipRenderType: pb.Annotation_RENDER.Enum(),
		Metadata:       &pb.Annotation_UploadMetadata{UploadMetadata: meta},
	}, nil
}
