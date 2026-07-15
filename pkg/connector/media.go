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
	"fmt"
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
		ID:      gcid.MakeAttachmentPartID(index),
		Type:    event.EventMessage,
		Content: content,
	}, true
}

// --- Outbound (Matrix -> Google Chat) media, M5 Task 5 ----------------------
//
// Ports portal.py's _handle_matrix_media (portal.py:1081-1121) field by
// field:
//
//	if message.file and decrypt_attachment:
//	    data = await self.main_intent.download_media(message.file.url)
//	    data = decrypt_attachment(
//	        data, message.file.key.key, message.file.hashes.get("sha256"), message.file.iv
//	    )
//	elif message.url:
//	    data = await self.main_intent.download_media(message.url)
//	else:
//	    raise Exception("Failed to download media from matrix")
//	mime = message.info.mimetype or magic.mimetype(data)
//	upload = await sender.client.upload_file(
//	    data=data, group_id=self.gcid_plain, filename=message.body, mime_type=mime
//	)
//	annotations = [googlechat.Annotation(
//	    type=googlechat.UPLOAD_METADATA, upload_metadata=upload,
//	    chip_render_type=googlechat.Annotation.RENDER,
//	)]
//
// The decrypt/download half collapses into one call here: bridgev2's own
// DownloadMedia (client.go's downloadMatrixMedia, wrapping
// bridgev2.MatrixAPI.DownloadMedia) already handles both the encrypted
// (message.file set) and plain (message.url set) cases Python's own
// if/elif distinguishes manually, decrypting internally whenever its file
// argument is non-nil -- see downloadMediaFn's doc comment (client.go).
//
// mime_type: unlike Python, this port never falls back to sniffing the
// downloaded bytes (magic.mimetype) when content.Info is nil or its
// MimeType is empty -- the Matrix event's own declared MIME type (or "" if
// absent) is forwarded as-is. gchatmeow.Client.UploadFile sends it verbatim
// as the x-goog-upload-content-type header (upload.go); every
// well-behaved Matrix client sets info.mimetype on media uploads, and
// adding a magic-byte-sniffing dependency purely to cover the rare client
// that omits it -- on a feature already gated behind #114's live upload
// failure -- was judged not worth it (flagged for the gchat-port-auditor
// rather than silently skipped).
//
// Caption handling deliberately improves on Python (the M3 B4 pattern this
// task's brief calls out): Python drops any Matrix caption's TEXT outright
// and instead uploads the file under whatever `message.body` happens to
// hold, caption or not (portal.py:1100) -- so a captioned image's caption
// text is silently lost, and the file's reported name becomes the caption
// string instead of the real file name. This port instead (a) uploads the
// file under its REAL name via uploadFilename below, preferring FileName
// when the Matrix client sent one, and (b) keeps the caption's own text and
// formatting annotations by having handlematrix.go's HandleMatrixMessage
// media branch route msg.Content through the SAME
// c.msgConverter().FromMatrix call a plain text message uses, then combine
// the two annotation sources via mergeAnnotations (handlematrix.go) rather
// than dropping either -- see hasOutboundCaption's doc comment for exactly
// when a caption is considered "genuine" (as opposed to Body merely
// repeating the filename, the common uncaptioned case).

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
// gchatFile never advertises event.CapMsgSticker, and neither the Python
// bridge (handle_matrix_message's `message.msgtype.is_media` gate,
// portal.py:919 -- no sticker special-casing anywhere in portal.py) nor
// this bridge's own inbound half (msgTypeFromMime, above) ever produces or
// expects a Matrix sticker.
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
// Python has no equivalent concept: _handle_matrix_media never sends any
// text_body for a media message at all (see this section's own doc comment
// above), so there is no Python behavior to match here -- this is this
// port's own (improved) design, not a fidelity requirement.
func hasOutboundCaption(content *event.MessageEventContent) bool {
	return content.FileName != "" && content.Body != content.FileName
}

// uploadFilename picks the name passed to UploadFile's own filename
// parameter, ported from portal.py:1100's `filename=message.body` --
// preferring the real FileName field (set by well-behaved Matrix clients
// alongside a genuine caption in Body, MSC2530) when present, and falling
// back to Body otherwise (the common uncaptioned case, where Body already
// IS the filename) -- unlike Python, which always uses Body verbatim even
// when FileName holds the real name and Body holds an unrelated caption
// (see this section's own doc comment above).
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
// comment above for the full portal.py:1081-1121 field-by-field port.
//
// group is the portal's parsed gcid.GroupID; UploadFile's own group_id
// parameter is group.ID -- the PLAIN numeric id (Python's gcid_plain,
// portal.py:182-183's `gc_type, gcid = self.gcid.split(":")`, no "dm:"/
// "space:" prefix), NOT gchatmeow.PartsToGroupID's *pb.GroupId oneof the
// CreateTopicRequest/CreateMessageRequest's own GroupId field uses -- see
// upload.go's UploadFile doc comment: it is a bare "group_id" query
// parameter on the /uploads endpoint, an entirely different wire shape from
// the /api/* JSON-RPC GroupId oneof.
//
// A download or upload failure returns a clean, wrapped error and NO
// annotation -- the caller (HandleMatrixMessage) must treat this as fatal
// and send nothing at all, never falling back to a text-only send that
// would silently lose the attached file. An UploadFile failure here is an
// EXPECTED, not exceptional, outcome against Google's real servers today:
// issue #114 (https://github.com/mautrix/googlechat/issues/114) reports
// the /uploads endpoint returning HTTP 500 for every upload since ~Feb
// 2026 -- the error message below names it explicitly so the failure shown
// to the user (via bridgev2's own message-send-status translation) isn't a
// bare, unexplained network error.
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
