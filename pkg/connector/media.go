package connector

// media.go bridges inbound Google Chat UPLOAD_METADATA attachments to
// Matrix media parts (M5 Task 3), porting portal.py's
// _process_googlechat_attachment (portal.py:1525-1585) plus the
// upload_metadata branch of its sibling _preprocess_annotations
// (portal.py:1465-1524, specifically 1470-1485):
//
//	if annotation.HasField("upload_metadata"):
//	    query = {"url_type": "DOWNLOAD_URL", "attachment_token": ...}
//	    if annotation.upload_metadata.content_type.startswith("image/"):
//	        query["url_type"] = "FIFE_URL"; query["sz"] = "w10000-h10000"; ...
//	    au = AttachmentURL(url=..., name=annotation.upload_metadata.content_name, mime=...)
//	...
//	async def _process_googlechat_attachment(self, att, source, intent, thread_parent, reply_to, ts):
//	    max_size = self.matrix.media_config.upload_size
//	    try:
//	        data, mime, filename = await source.client.download_attachment(att.url, max_size)
//	    except FileTooLargeError:
//	        self.log.warning("Can't upload too large attachment"); return None
//	    except aiohttp.ClientResponseError as e:
//	        self.log.warning(f"Failed to download attachment: {e}"); return None
//	    if mime.startswith("text/html"):
//	        self.log.debug(...); return None
//	    msgtype = getattr(MessageType, mime.split("/")[0].upper(), MessageType.FILE)
//	    if msgtype == MessageType.TEXT: msgtype = MessageType.FILE
//	    if not filename or filename == "get_attachment_url":
//	        filename = att.name or (msgtype.value + (mimetypes.guess_extension(mime) or ""))
//	    mxc_url = await intent.upload_media(data, mime_type=mime, filename=filename, ...)
//	    content = MediaMessageEventContent(url=mxc_url, body=filename, info=ImageInfo(size=len(data), mimetype=mime))
//	    content.msgtype = msgtype
//	    ...
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
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"strings"

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
	// to fetch an UPLOAD_METADATA annotation's file from (portal.py:1470-1485).
	attachmentURL(meta *pb.UploadMetadata) (url string, isImage bool, err error)
	// downloadAttachment mirrors gchatmeow.Client.DownloadAttachment: fetches
	// the file, capped at maxSize (client.py:182-236/238-273).
	downloadAttachment(ctx context.Context, urlStr string, maxSize int64) (data []byte, mimeType string, filename string, err error)
	// maxFileSize is the Go equivalent of portal.py:1534's
	// `self.matrix.media_config.upload_size`.
	maxFileSize() int64
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
// gchatmeow.Client, mirroring source.client.download_attachment
// (portal.py:1537) -- the UPLOAD_METADATA annotation's URL is always a
// chat.google.com one (see AttachmentURL's own doc comment, download.go),
// so this bridge never needs portal.py's separate
// _download_external_attachment branch (that one only ever fires for
// url_metadata annotations, Task 4's scope, not this one's).
func (c *GChatClient) downloadAttachment(ctx context.Context, urlStr string, maxSize int64) ([]byte, string, string, error) {
	conn := c.getConn()
	if conn == nil {
		return nil, "", "", errors.New("googlechat: not connected")
	}
	return conn.DownloadAttachment(ctx, urlStr, maxSize)
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

// msgTypeFromMime picks the Matrix msgtype from mimeType's "/"-prefix,
// mirroring portal.py:1551-1553 exactly:
//
//	msgtype = getattr(MessageType, mime.split("/")[0].upper(), MessageType.FILE)
//	if msgtype == MessageType.TEXT:
//	    msgtype = MessageType.FILE
//
// i.e. image/* -> m.image, video/* -> m.video, audio/* -> m.audio, and
// EVERY other prefix (including "text", explicitly overridden back to FILE
// by Python's own second line, and anything unrecognized such as
// "application") -> m.file. mimeType may be "" (e.g. a response with no
// Content-Type header at all -- see downloadAttachment's doc comment on why
// this can't KeyError the way Python's dict-style header access would);
// strings.Cut on an empty string yields "" for the prefix, which falls
// through to the same m.file default as any other unrecognized prefix.
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

// convertAttachmentsToMatrix scans msg's annotations for UPLOAD_METADATA
// ones (ann.GetUploadMetadata() != nil -- the proto oneof accessor is
// nil-safe and returns nil for every other annotation kind, e.g. the
// FORMAT_DATA/USER_MENTION ones gchatfmt.Parse handles), downloads and
// reuploads each to Matrix, and returns the resulting
// bridgev2.ConvertedMessageParts with IDs att_0, att_1, ... in ENCOUNTER
// order among UPLOAD_METADATA annotations (gcid.MakeAttachmentPartID) --
// i.e. index counts every such annotation seen, whether or not it was
// successfully bridged, so a part's number always reflects its original
// position among the message's attachments (stable for later reference,
// e.g. a reaction targeting a specific part) rather than being compacted
// after skips.
//
// A nil portal or nil media yields no parts at all (nothing to reupload
// into, or no seam to fetch with) rather than panicking -- mirrors
// convertMessageToMatrix's own existing nil-portal tolerance
// (msgconv_adapter.go, TestConvertMessageToMatrix_NilPortalDoesNotPanic).
//
// Per-attachment failures (oversize, HTTP error, an HTML "attachment", a
// reupload failure) are logged and SKIPPED -- that one attachment is
// dropped, but every other part of the same message (the text part, and
// any other attachment) still gets bridged. This mirrors portal.py:1420-1433's
// `try: for att in attachment_urls: ... except Exception: log.exception(...)`
// wrapping the whole loop in INTENT (a single bad attachment must not lose
// the rest of the message) but improves on its actual per-attachment
// isolation: Python's bare `except Exception` sits OUTSIDE the `for` loop,
// so a real exception on attachment N would abort attachments N+1..end too;
// this port's `continue`-per-attachment does not have that accidental
// side effect, which is a deliberate Go-side improvement, not a fidelity
// gap the brief asked to preserve.
func convertAttachmentsToMatrix(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, msg *pb.Message, media mediaFetcher) []*bridgev2.ConvertedMessagePart {
	if portal == nil || media == nil {
		return nil
	}
	var parts []*bridgev2.ConvertedMessagePart
	index := 0
	for _, ann := range msg.GetAnnotations() {
		meta := ann.GetUploadMetadata()
		if meta == nil {
			continue
		}
		partIndex := index
		index++
		part, ok := convertOneAttachment(ctx, portal, intent, meta, partIndex, media)
		if ok {
			parts = append(parts, part)
		}
	}
	return parts
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
	// (portal.py:1551's `mime.split("/")[0]`, where `mime` is the download
	// result, NOT the annotation's declared content_type) -- the two can
	// differ (a stale/wrong annotation, a server that recompresses/
	// transcodes), so this discards isImage rather than reusing it.
	urlStr, _, err := media.attachmentURL(meta)
	if err != nil {
		log.Warn().Err(err).Msg("googlechat: failed to build attachment URL, skipping attachment")
		return nil, false
	}

	// portal.py:1534: max_size = self.matrix.media_config.upload_size.
	data, mimeType, filename, err := media.downloadAttachment(ctx, urlStr, media.maxFileSize())
	if err != nil {
		// portal.py:1540-1543: FileTooLargeError -> warn + skip, not a
		// crash (this task's own size-cap requirement).
		if errors.Is(err, gchatmeow.ErrFileTooLarge) {
			log.Warn().Err(err).Msg("googlechat: attachment too large to bridge, skipping")
		} else {
			// portal.py:1544-1546: aiohttp.ClientResponseError -> warn +
			// skip; this port applies the same treatment to ANY other
			// download error (a broken/expired attachment URL, a network
			// blip) rather than distinguishing HTTP status errors from
			// everything else, since none of those are more recoverable
			// than the rest.
			log.Warn().Err(err).Msg("googlechat: failed to download attachment, skipping")
		}
		return nil, false
	}

	// portal.py:1547-1549: an HTML "attachment" (a preview page, not a real
	// file) is dropped outright.
	if strings.HasPrefix(mimeType, "text/html") {
		log.Debug().Str("mime", mimeType).Msg("googlechat: ignoring HTML attachment")
		return nil, false
	}

	msgType := msgTypeFromMime(mimeType)

	// portal.py:1554-1558: filename fallback when the download gave us
	// nothing usable (empty, or literally the request path's last segment
	// "get_attachment_url" -- see attachmentFilename's doc comment,
	// download.go).
	if filename == "" || filename == "get_attachment_url" {
		if name := meta.GetContentName(); name != "" {
			filename = name
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

	// Width/Height for images: Python's own ImageInfo never computes these
	// (portal.py:1577 only ever sets size+mimetype) -- an acknowledged gap
	// in the reference bridge this task's brief explicitly asks to close.
	// Best-effort: image.DecodeConfig only recognizes the formats this file
	// registers decoders for (gif/jpeg/png, the blank imports above); any
	// other/unrecognized image format just leaves Width/Height at zero
	// rather than failing the whole attachment.
	if msgType == event.MsgImage {
		if cfg, _, decErr := image.DecodeConfig(bytes.NewReader(data)); decErr == nil {
			content.Info.Width = cfg.Width
			content.Info.Height = cfg.Height
		}
	}

	// UploadMediaStream (matrixinterface.go) handles E2BE reupload itself
	// when portal.MXID is an encrypted room: it returns an empty url and a
	// populated *event.EncryptedFileInfo in that case (mirroring
	// portal.py:1561-1572's manual async_inplace_encrypt_attachment +
	// "mxc_url = None" dance) -- nothing extra needed here beyond assigning
	// both return values through.
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
		ID:      gcid.MakeAttachmentPartID(index),
		Type:    event.EventMessage,
		Content: content,
	}, true
}
