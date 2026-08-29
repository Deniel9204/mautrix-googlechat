package connector

// media.go bridges inbound Google Chat media to Matrix media parts: for each
// media annotation it downloads the bytes capped at the homeserver's
// upload_size, picks a Matrix msgtype from the downloaded MIME type, and
// reuploads them to Matrix as a media event. Oversize downloads, HTTP errors,
// and non-media responses are logged and skipped, and the filename falls back
// to the annotation's content_name then to "<msgtype><ext>" when the download
// gives no usable name. The per-function comments below document each step.
//
// TWO kinds of annotation get here, and the difference is a security boundary
// rather than a detail:
//
//   - UPLOAD_METADATA -- a file hosted by Google. The URL is built from a
//     token and always points at Google's own fixed endpoint, which is what
//     lets gchatmeow's attachment client keep an environment proxy and accept
//     plain http.
//   - url_metadata accepted by inlineableURLMedia -- a link chip's media,
//     hosted by whoever the sender chose. Neither of those concessions is safe
//     for it, so it goes through gchatmeow.DownloadExternalMedia
//     (external.go): https-only on every hop, no proxy, no cookies, internal
//     addresses refused, size ceiling the caller cannot disable. Operators can
//     switch it off entirely with disable_inline_url_media.
//
// # Layering: why this lives in the connector, not msgconv
//
// pkg/msgconv/from-gchat.go's ToMatrix (Task 3's other half) stays a pure
// *pb.Message -> *bridgev2.ConvertedMessage function: no HTTP client, no
// bridgev2.MatrixAPI, no portal (msgconv.go's package doc, "no portal, no
// intent, no gchatmeow client"). Bridging an attachment needs BOTH of those
// -- a live gchatmeow.Client to build the download URL and fetch the bytes,
// and a bridgev2.MatrixAPI intent to reupload them to Matrix -- so that work
// happens entirely here, in the connector, exactly like mautrix-meta's own
// split (pkg/msgconv/mediadl/reupload.go does the HTTP+reupload; the pure
// pkg/msgconv only decides WHICH attachment record to convert).
//
// Rather than have convertAttachmentsToMatrix re-derive attachment records
// from msg via a new msgconv-side "extraction" step (a second, parallel
// annotation walk that would need to stay in lockstep with gchatfmt.Parse's
// OWN annotation walk over the same slice), this file reads
// msg.GetAnnotations() directly -- msg is already a plain proto value with
// no connector dependencies, so nothing about that read requires msgconv's
// involvement. gcid.MakeAttachmentPartID (already defined for this task by
// an earlier commit) gives the att_<n> part IDs; pkg/gchatmeow's Task 1/2
// (AttachmentURL, DownloadAttachment) supply the download half.
//
// # Testability: the mediaFetcher seam
//
// convertMessageToMatrix (msgconv_adapter.go) is exercised by ~15 existing
// unit tests that construct a bare *msgconv.MessageConverter and a nil
// bridgev2.MatrixAPI, none of which touch attachments. Rather than force
// every one of those to also wire a live gchatmeow.Client (impossible
// without a real Google Chat session) or a fake HTTP server, the unexported
// mediaFetcher interface below is convertMessageToMatrix's ONE new
// parameter: production wiring (events.go's queueMessagePosted) passes the
// real *GChatClient (which implements it by delegating to c.getConn()),
// while every attachment-focused test in media_test.go passes a small fake
// implementing the same three methods, and every pre-existing
// non-attachment test now passes a bare nil (safe: a nil mediaFetcher only
// ever gets dereferenced when msg actually carries an UPLOAD_METADATA
// annotation, and none of those tests' messages do).
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

// mediaFetcher is convertMessageToMatrix's seam onto the attachment
// download+reupload path (see this file's package doc comment above).
// Unexported: only *GChatClient (media.go, production) and the fakes in
// media_test.go (same package) ever need to implement it.
type mediaFetcher interface {
	// attachmentURL mirrors gchatmeow.Client.AttachmentURL: builds the URL
	// to fetch an UPLOAD_METADATA annotation's file from.
	attachmentURL(meta *pb.UploadMetadata) (url string, isImage bool, err error)
	// downloadAttachment mirrors gchatmeow.Client.DownloadAttachment: fetches
	// the file, capped at maxSize.
	downloadAttachment(ctx context.Context, urlStr string, maxSize int64) (data []byte, mimeType string, filename string, err error)
	// downloadExternal mirrors gchatmeow.DownloadExternalMedia: a hardened,
	// cookie-less, proxy-less fetch of a url_metadata annotation's
	// REMOTE-PARTY-CHOSEN URL. Deliberately NOT downloadAttachment, whose
	// client keeps an env proxy and permits plain http -- concessions that
	// are only safe for Google's own fixed endpoint (external.go).
	downloadExternal(ctx context.Context, urlStr string, maxSize int64) (data []byte, mimeType string, filename string, err error)
	// maxFileSize is the homeserver's media_config upload_size cap.
	maxFileSize() int64
	// inlineURLMediaEnabled reports whether the operator allows fetching
	// remote-party-chosen media at all (Config.DisableInlineURLMedia).
	inlineURLMediaEnabled() bool
}

var _ mediaFetcher = (*GChatClient)(nil)

// attachmentURL implements mediaFetcher by delegating to the live
// gchatmeow.Client. A nil/disconnected conn (c.getConn()) fails cleanly --
// consistent with every other gchatmeow-backed GChatClient method -- rather
// than relying on gchatmeow.Client.AttachmentURL's implementation detail of
// never dereferencing its receiver (Task 1's own tests call it via a bare
// &gchatmeow.Client{}, but this method does not lean on that).
func (c *GChatClient) attachmentURL(meta *pb.UploadMetadata) (string, bool, error) {
	conn := c.getConn()
	if conn == nil {
		return "", false, errors.New("googlechat: not connected")
	}
	return conn.AttachmentURL(meta)
}

// downloadAttachment implements mediaFetcher by delegating to the live
// gchatmeow.Client. The UPLOAD_METADATA annotation's URL is always a
// chat.google.com one (see AttachmentURL's own doc comment, download.go),
// so this bridge never needs a separate external-attachment download branch
// (that one only ever fires for url_metadata annotations, Task 4's scope,
// not this one's).
func (c *GChatClient) downloadAttachment(ctx context.Context, urlStr string, maxSize int64) ([]byte, string, string, error) {
	conn := c.getConn()
	if conn == nil {
		return nil, "", "", errors.New("googlechat: not connected")
	}
	return conn.DownloadAttachment(ctx, urlStr, maxSize)
}

// downloadExternal implements mediaFetcher. Unlike its siblings there is no
// c.getConn() check, and that is the point: DownloadExternalMedia is
// package-level with no session, so no Google credential can reach a
// third-party host. It therefore works even while disconnected.
func (c *GChatClient) downloadExternal(ctx context.Context, urlStr string, maxSize int64) ([]byte, string, string, error) {
	return gchatmeow.DownloadExternalMedia(ctx, urlStr, maxSize)
}

// inlineURLMediaEnabled implements mediaFetcher. Defaults to ENABLED when
// Main is nil, matching maxFileSize's bare-*GChatClient test fallback.
func (c *GChatClient) inlineURLMediaEnabled() bool {
	return c.Main == nil || !c.Main.Config.DisableInlineURLMedia
}

// maxFileSize reads this login's connector-wide cap (connector.go's
// GChatConnector.MaxFileSize, kept updated by bridgev2's
// MaxFileSizeingNetwork callback). Falls back to 0 ("no cap", per
// gchatmeow.DownloadAttachment's own maxSize<=0 contract) when Main is nil,
// matching msgConverter()'s identical bare-*GChatClient test fallback
// pattern (client.go).
func (c *GChatClient) maxFileSize() int64 {
	if c.Main != nil {
		return c.Main.MaxFileSize
	}
	return 0
}

// msgTypeFromMime picks the Matrix msgtype from mimeType's "/"-prefix:
//
// i.e. image/* -> m.image, video/* -> m.video, audio/* -> m.audio, and
// EVERY other prefix (including "text", explicitly overridden back to FILE,
// and anything unrecognized such as "application") -> m.file. mimeType may
// be "" (e.g. a response with no Content-Type header at all -- see
// downloadAttachment's doc comment); strings.Cut on an empty string yields
// "" for the prefix, which falls through to the same m.file default as any
// other unrecognized prefix.
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

// convertAttachmentsToMatrix walks msg's annotations, downloads and reuploads
// the media ones to Matrix, and returns the resulting
// bridgev2.ConvertedMessageParts.
//
// TWO annotation kinds are bridged into ONE att_<n> namespace
// (gcid.MakeAttachmentPartID, whose format is FROZEN):
//
//   - UPLOAD_METADATA -- a file hosted by Google, fetched from Google's own
//     fixed endpoint.
//   - url_metadata accepted by inlineableURLMedia, when the operator has not
//     set disable_inline_url_media -- media on a host a REMOTE PARTY chose,
//     fetched through gchatmeow's separately hardened external path.
//
// (The oneof accessors are nil-safe and return nil for every other annotation
// kind, e.g. the FORMAT_DATA/USER_MENTION ones gchatfmt.Parse handles.)
//
// IDs are att_0, att_1, ... in ENCOUNTER order across BOTH kinds: the index is
// consumed by every annotation that matches, whether or not it was
// successfully bridged, and before any dedup or per-message limit can skip it.
// So a part's number always reflects its original position among the message's
// media (stable for later reference, e.g. a reaction targeting a specific
// part) and never depends on a runtime outcome. Two counters would produce
// duplicate IDs on a message carrying both kinds.
//
// External fetches are additionally bounded per message -- capped in count
// (maxExternalMediaPerMessage), deduplicated by URL, and sharing one
// externalMediaMessageBudget deadline -- because the annotation count is
// sender-controlled and each fetch blocks the portal's event goroutine.
//
// A nil portal or nil media yields no parts at all (nothing to reupload
// into, or no seam to fetch with) rather than panicking -- mirrors
// convertMessageToMatrix's own existing nil-portal tolerance
// (msgconv_adapter.go, TestConvertMessageToMatrix_NilPortalDoesNotPanic).
//
// Per-attachment failures (oversize, HTTP error, an HTML "attachment", a
// reupload failure) are logged and SKIPPED -- that one attachment is
// dropped, but every other part of the same message (the text part, and
// any other attachment) still gets bridged. The `continue`-per-attachment
// isolation is deliberate: a real failure on attachment N must not abort
// attachments N+1..end, and a single bad attachment must never lose the rest
// of the message.
func convertAttachmentsToMatrix(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, msg *pb.Message, media mediaFetcher) []*bridgev2.ConvertedMessagePart {
	if portal == nil || media == nil {
		return nil
	}
	log := zerolog.Ctx(ctx)
	var parts []*bridgev2.ConvertedMessagePart
	// ONE counter across both annotation kinds: gcid.MakeAttachmentPartID is
	// FROZEN, so att_<n> must stay a single namespace. Two counters would
	// produce duplicate part IDs on a message carrying both kinds.
	index := 0
	// External-fetch bookkeeping. The index is consumed for every annotation
	// that MATCHES, before any of these can skip it, so att_<n> numbering
	// stays a function of the message's content and never of a runtime
	// outcome -- the same contract the fetch-failure path already had.
	externalFetches := 0
	externalSeen := make(map[string]bool)
	// One budget for every external fetch this message makes, taken only when
	// the message actually has some, so a message with none pays nothing.
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
			// Deduplicate on the URL actually requested: a preview chip
			// repeated across a message must not be fetched twice.
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
	// maxExternalMediaPerMessage bounds how many external fetches ONE message
	// can trigger. The annotation count is sender-controlled, and each fetch
	// is a synchronous call on the portal's single event goroutine, so without
	// a cap one message with a hundred link chips stalls that portal for as
	// long as the sender likes.
	maxExternalMediaPerMessage = 4

	// externalMediaMessageBudget bounds the TOTAL time one message may spend
	// fetching. DownloadExternalMedia bounds ONE fetch; this bounds the sum,
	// which is what a portal's event loop and a backfill batch actually care
	// about. Deliberately less than maxExternalMediaPerMessage times the
	// per-fetch timeout: the cap is for the pathological case, this is for the
	// common slow one.
	externalMediaMessageBudget = 30 * time.Second
)

// hasInlineableURLMedia reports whether msg carries any annotation the
// external-media path would fetch, so the per-message budget is only taken
// when there is something to spend it on.
func hasInlineableURLMedia(msg *pb.Message) bool {
	for _, ann := range msg.GetAnnotations() {
		if inlineableURLMedia(ann) {
			return true
		}
	}
	return false
}

// externalMediaSrc returns the URL convertOneURLMedia would fetch for m:
// image_url, and only image_url. Split out so the caller can deduplicate on
// the URL actually requested.
//
// The absence of a fallback to url.url is the point; see inlineableURLMedia,
// which refuses an annotation without an image_url for the same reason.
func externalMediaSrc(m *pb.UrlMetadata) string {
	return m.GetImageUrl()
}

// inlineableURLMedia reports whether a url_metadata annotation names media
// worth fetching and inlining.
//
// Deliberately NARROWER than every reference client. purple-googlechat, the
// Python upstream and megabridge all fetch EVERY url_metadata URL, which turns
// the bridge into a link prefetcher: it hands the operator's IP, and the
// timing of message receipt, to any host a remote party names -- on live
// traffic AND on every backfill.
//
// image_url is REQUIRED, before any other signal. It is the one address GOOGLE
// supplies -- in the shape captured live it was a googleusercontent.com URL,
// Google having already rehosted the media -- whereas url.url is whatever the
// SENDER typed. Requiring it means no fetch can be aimed at a host of the
// sender's choosing, and it matches purple exactly: despite being far wider,
// purple also only ever fetches image_url (googlechat_events.c).
//
// What this does NOT claim: that an ordinary link is never fetched. A link
// PASTED INTO TEXT is safe -- it covers text (length > 0) and its mime_type
// describes an HTML page. But a link CHIP attached to the message has the same
// shape as a GIF, so its preview image IS fetched, from Google's copy. That is
// what disable_inline_url_media turns off, and example-config.yaml says so.
//
// Live-captured shape of a real shared GIF, for anyone tempted to simplify
// this: type=URL (NOT IMAGE), length=0, mime_type="image/gif",
// should_not_render present and false, image_url on googleusercontent.com,
// int_image_width/height populated. Note the type: reducing this function to
// the AnnotationType_IMAGE check would break every real GIF.
//
// The one remaining inference is what mime_type describes. The proto's own
// naming (every preview-image field is image_-prefixed; mime_type is not), the
// AnnotationType URL/VIDEO/IMAGE/PDF taxonomy, and megabridge binding it to
// the page URL all say it is the LINKED RESOURCE, so an article link carries
// text/html and stays out. That has not been observed directly --
// TestLiveDumpURLMediaAnnotations run on a chat containing an ordinary article
// link would settle it.
func inlineableURLMedia(a *pb.Annotation) bool {
	m := a.GetUrlMetadata()
	if m == nil || m.GetShouldNotRender() {
		return false
	}
	// Google must have supplied the media address itself. Without this, every
	// arm below would also accept a sender-chosen host.
	if m.GetImageUrl() == "" {
		return false
	}
	// The server said so outright.
	if a.GetType() == pb.AnnotationType_IMAGE {
		return true
	}
	// The server declared a media type.
	if t := m.GetMimeType(); strings.HasPrefix(t, "image/") || strings.HasPrefix(t, "video/") {
		return true
	}
	// A chip attached to the message rather than decorating pasted text. This
	// is the shape purple keys its own Tenor handling on.
	return a.GetLength() == 0
}

// convertOneURLMedia builds an att_<index> part from a url_metadata
// annotation, or reports ok=false to skip it. A skip is not a lost message:
// AppendLinkAnnotations (pkg/msgconv/gchatfmt) has already put the URL in the
// body, so the message still delivers as a link.
func convertOneURLMedia(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, meta *pb.UrlMetadata, index int, media mediaFetcher) (*bridgev2.ConvertedMessagePart, bool) {
	log := zerolog.Ctx(ctx)

	src := externalMediaSrc(meta)
	if src == "" {
		return nil, false
	}

	data, mimeType, filename, err := media.downloadExternal(ctx, src, media.maxFileSize())
	if err != nil {
		// Third-party host: down, 404, oversize, or a blocked internal
		// address. All of them are ordinary and none should cost the message.
		log.Debug().Err(err).Msg("googlechat: failed to fetch external media, leaving the link in the body")
		return nil, false
	}

	// TIGHTER than convertOneAttachment's text/html check, because this URL
	// was chosen by a remote party: accept only what we asked for.
	if !strings.HasPrefix(mimeType, "image/") && !strings.HasPrefix(mimeType, "video/") {
		log.Debug().Str("mime", mimeType).Msg("googlechat: external URL was not media, leaving the link in the body")
		return nil, false
	}

	return buildMediaPart(ctx, portal, intent, mediaPartInput{
		data:     data,
		mimeType: mimeType,
		filename: filename,
		// UrlMetadata has no content_name, so there is no annotation-supplied
		// fallback; buildMediaPart synthesises one from the msgtype.
		//
		// Tenor commonly serves webp and mp4, for which no decoder is
		// registered, so seed the dimensions the annotation declared -- used
		// only when decoding fails.
		fallbackWidth:  int(meta.GetIntImageWidth()),
		fallbackHeight: int(meta.GetIntImageHeight()),
		index:          index,
	})
}

// convertOneAttachment builds a single att_<index> ConvertedMessagePart from
// meta, or reports ok=false if this attachment should be skipped (see
// convertAttachmentsToMatrix's doc comment on why a skip never aborts the
// rest of the message).
func convertOneAttachment(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, meta *pb.UploadMetadata, index int, media mediaFetcher) (*bridgev2.ConvertedMessagePart, bool) {
	log := zerolog.Ctx(ctx)

	// The second return value (isImage, derived from the ANNOTATION's own
	// content_type) only mattered inside AttachmentURL itself, to pick
	// FIFE_URL vs DOWNLOAD_URL (download.go). msgType below is derived
	// afresh from the DOWNLOADED response's actual Content-Type instead
	// (NOT the annotation's declared content_type) -- the two can differ (a
	// stale/wrong annotation, a server that recompresses/transcodes), so
	// this discards isImage rather than reusing it.
	urlStr, _, err := media.attachmentURL(meta)
	if err != nil {
		log.Warn().Err(err).Msg("googlechat: failed to build attachment URL, skipping attachment")
		return nil, false
	}

	// max size is the homeserver's media_config upload_size cap.
	data, mimeType, filename, err := media.downloadAttachment(ctx, urlStr, media.maxFileSize())
	if err != nil {
		// A too-large file -> warn + skip, not a crash (this task's own
		// size-cap requirement).
		if errors.Is(err, gchatmeow.ErrFileTooLarge) {
			log.Warn().Err(err).Msg("googlechat: attachment too large to bridge, skipping")
		} else {
			// An HTTP response error -> warn + skip; this port applies the
			// same treatment to ANY other download error (a broken/expired
			// attachment URL, a network blip) rather than distinguishing HTTP
			// status errors from everything else, since none of those are
			// more recoverable than the rest.
			log.Warn().Err(err).Msg("googlechat: failed to download attachment, skipping")
		}
		return nil, false
	}

	// An HTML "attachment" (a preview page, not a real file) is dropped
	// outright.
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

// mediaPartInput is buildMediaPart's argument set. A struct rather than seven
// positional parameters, several of which are strings that would be trivial to
// transpose.
type mediaPartInput struct {
	data     []byte
	mimeType string
	filename string
	// contentName is the annotation's own declared name, used when the
	// download supplied nothing usable. Empty for url_metadata, which has no
	// such field.
	contentName string
	// fallbackWidth/Height seed an image's dimensions when the bytes cannot be
	// decoded -- only url_metadata carries declared dimensions.
	fallbackWidth, fallbackHeight int
	index                         int
}

// buildMediaPart turns downloaded bytes into an att_<index> Matrix part,
// reuploading them to the homeserver. Shared by the upload_metadata and
// url_metadata paths so both run one tested code path; each caller does its
// own fetch and its own content-type policy first.
func buildMediaPart(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, in mediaPartInput) (*bridgev2.ConvertedMessagePart, bool) {
	log := zerolog.Ctx(ctx)
	data, mimeType, filename := in.data, in.mimeType, in.filename

	msgType := msgTypeFromMime(mimeType)

	// Filename fallback when the download gave us nothing usable (empty, or
	// literally the request path's last segment "get_attachment_url" -- see
	// attachmentFilename's doc comment, download.go).
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

	// Width/Height for images -- computed best-effort here (a feature this
	// task's brief explicitly asks to add). image.DecodeConfig only
	// recognizes the formats this file registers decoders for (gif/jpeg/png,
	// the blank imports above); any other/unrecognized image format just
	// leaves Width/Height at zero rather than failing the whole attachment.
	if msgType == event.MsgImage {
		if cfg, _, decErr := image.DecodeConfig(bytes.NewReader(data)); decErr == nil {
			content.Info.Width = cfg.Width
			content.Info.Height = cfg.Height
		}
	}
	// Only url_metadata supplies declared dimensions, and only they can rescue
	// a format nothing here decodes -- webp and mp4, which are exactly what a
	// GIF host commonly serves. Outside the image branch on purpose: an mp4
	// is an m.video, so an image-only guard would drop the dimensions for the
	// case they were added for. A no-op for upload_metadata, whose fallbacks
	// are always zero.
	if content.Info.Width == 0 && content.Info.Height == 0 {
		content.Info.Width = in.fallbackWidth
		content.Info.Height = in.fallbackHeight
	}

	// UploadMediaStream (matrixinterface.go) handles E2BE reupload itself
	// when portal.MXID is an encrypted room: it returns an empty url and a
	// populated *event.EncryptedFileInfo in that case -- nothing extra needed
	// here beyond assigning both return values through.
	//
	// Size is effectively checked twice -- once by media.maxFileSize() above
	// (rejecting at download time, so an oversize file is never fetched in
	// full), and again inside UploadMediaStream, which independently compares
	// size against the homeserver's MediaConfig.UploadSize and returns
	// bridgev2.ErrMediaTooLarge. Both derive from the SAME homeserver-reported
	// value (SetMaxFileSize is bridgev2's own plumbing of MediaConfig.UploadSize
	// into this connector, connector.go), so the second check is a redundant
	// safety net, not a second policy -- intentional, not an oversight.
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

// --- Outbound (Matrix -> Google Chat) media, M5 Task 5 ----------------------
//
// Downloads the Matrix media, uploads it to Google Chat via UploadFile, and
// wraps the result in an UPLOAD_METADATA/RENDER annotation attached to the
// outbound message.
//
// The decrypt/download half collapses into one call here: bridgev2's own
// DownloadMedia (client.go's downloadMatrixMedia, wrapping
// bridgev2.MatrixAPI.DownloadMedia) already handles both the encrypted
// (message.file set) and plain (message.url set) cases, decrypting
// internally whenever its file argument is non-nil -- see downloadMediaFn's
// doc comment (client.go).
//
// mime_type: this port never falls back to sniffing the downloaded bytes
// when content.Info is nil or its MimeType is empty -- the Matrix event's
// own declared MIME type (or "" if absent) is forwarded as-is.
// gchatmeow.Client.UploadFile sends it verbatim as the
// x-goog-upload-content-type header (upload.go); every well-behaved Matrix
// client sets info.mimetype on media uploads, and adding a
// magic-byte-sniffing dependency purely to cover the rare client that omits
// it -- on a feature already gated behind #114's live upload failure -- was
// judged not worth it (flagged for the gchat-port-auditor rather than
// silently skipped).
//
// Caption handling: a captioned media message keeps both its file and its
// caption. This port (a) uploads the file under its REAL name via
// uploadFilename below, preferring FileName when the Matrix client sent one,
// and (b) keeps the caption's own text and formatting annotations by having
// handlematrix.go's HandleMatrixMessage media branch route msg.Content
// through the SAME c.msgConverter().FromMatrix call a plain text message
// uses, then combine the two annotation sources via mergeAnnotations
// (handlematrix.go) rather than dropping either -- see hasOutboundCaption's
// doc comment for exactly when a caption is considered "genuine" (as opposed
// to Body merely repeating the filename, the common uncaptioned case).

// isOutboundMediaMsgType reports whether t is one of the four msgtypes
// handlematrix.go's HandleMatrixMessage media branch accepts, matching
// capabilities.go's gchatFile map one-for-one (both must stay in sync: a
// msgtype missing from gchatFile is rejected by bridgev2's own
// checkMessageContentCaps -- mautrix-go bridgev2/portal.go -- with
// ErrUnsupportedMessageType before HandleMatrixMessage is ever called, so
// this switch never actually needs to reject anything gchatFile doesn't
// already gate upstream; it exists so HandleMatrixMessage has an explicit,
// self-contained answer rather than relying on that upstream gate alone).
//
// Deliberately narrower than event.MessageType.IsMedia() (which also
// matches event.CapMsgSticker): Google Chat's upload pipeline
// (gchatmeow.Client.UploadFile) has no sticker concept, capabilities.go's
// gchatFile never advertises event.CapMsgSticker, and this bridge's own
// inbound half (msgTypeFromMime, above) never produces or expects a Matrix
// sticker.
func isOutboundMediaMsgType(t event.MessageType) bool {
	switch t {
	case event.MsgImage, event.MsgVideo, event.MsgAudio, event.MsgFile:
		return true
	default:
		return false
	}
}

// hasOutboundCaption reports whether content carries a genuine caption
// alongside its attached file, as opposed to the common case where Body is
// simply a repeat of the file's own name (no real caption at all). This
// mirrors, rather than re-derives independently of, the SAME distinction
// bridgev2 itself already relies on (mautrix-go bridgev2/portal.go's
// checkMessageContentCaps: `content.FileName != "" && content.Body !=
// content.FileName`) -- so this connector's idea of "has a caption" never
// disagrees with bridgev2's own.
//
// This "genuine caption" distinction is this port's own design (see this
// section's own doc comment above), not a ported requirement.
func hasOutboundCaption(content *event.MessageEventContent) bool {
	return content.FileName != "" && content.Body != content.FileName
}

// uploadFilename picks the name passed to UploadFile's own filename
// parameter -- preferring the real FileName field (set by well-behaved
// Matrix clients alongside a genuine caption in Body, MSC2530) when present,
// and falling back to Body otherwise (the common uncaptioned case, where
// Body already IS the filename). This uses the real file name even when Body
// holds an unrelated caption (see this section's own doc comment above).
func uploadFilename(content *event.MessageEventContent) string {
	if content.FileName != "" {
		return content.FileName
	}
	return content.Body
}

// buildUploadAnnotation downloads msg's Matrix media (decrypting it first
// if msg.Content.File is set -- handled internally by downloadMatrixMedia,
// client.go), uploads it to Google Chat via UploadFile (Task 2,
// pkg/gchatmeow/upload.go), and wraps the result in an
// UPLOAD_METADATA/RENDER annotation ready to attach to the outbound
// create_topic/create_message request -- see this section's own doc
// comment above.
//
// group is the portal's parsed gcid.GroupID; UploadFile's own group_id
// parameter is group.ID -- the PLAIN numeric id (no "dm:"/"space:" prefix),
// NOT gchatmeow.PartsToGroupID's *pb.GroupId oneof the
// CreateTopicRequest/CreateMessageRequest's own GroupId field uses -- see
// upload.go's UploadFile doc comment: it is a bare "group_id" query
// parameter on the /uploads endpoint, an entirely different wire shape from
// the /api/* JSON-RPC GroupId oneof.
//
// A download or upload failure returns a clean, wrapped error and NO
// annotation -- the caller (HandleMatrixMessage) must treat this as fatal
// and send nothing at all, never falling back to a text-only send that
// would silently lose the attached file. Uploads are verified working
// against Google's live endpoint (2026-07-22); the error message below still
// references issue #114 (https://github.com/mautrix/googlechat/issues/114) --
// a known upstream upload-500 request-shape bug this port doesn't share --
// so that if uploads ever regress, the failure shown to the user (via
// bridgev2's message-send-status translation) points at the known upstream
// context instead of being a bare, unexplained network error.
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
