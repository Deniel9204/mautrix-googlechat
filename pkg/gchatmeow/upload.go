package gchatmeow

// Resumable file upload, ported from maugclib/client.py's Client.upload_file
// (client.py:275-321), cross-checked against purple-googlechat's actively
// maintained googlechat_conversation.c (~1745-1859, "Received the url to
// upload the image data to" / "Received the upload metadata of the sent
// image") for the CURRENT wire shape -- issue #114 tracks Google's
// chat.google.com/uploads endpoint reportedly 500ing since ~Feb 2026, and
// this task is the #114-risk path: a wire-shape DIVERGENCE between the two
// references is the signal of what Google changed.
//
// DIFF: Python (2023 snapshot) vs purple-googlechat (current, through 2026)
//
//   - URL: IDENTICAL. Both POST to "https://chat.google.com/uploads" with a
//     group_id query parameter (client.py:27's UPLOAD_URL + upload_file's
//     params={"group_id": group_id}; googlechat_conversation.c:1839's
//     literal "https://chat.google.com/uploads?group_id=%s").
//   - Start headers (upload-command headers): IDENTICAL.
//     x-goog-upload-protocol: resumable, x-goog-upload-command: start,
//     x-goog-upload-content-length, x-goog-upload-content-type,
//     x-goog-upload-file-name (client.py:282-288;
//     googlechat_conversation.c:1843-1847). Request body is empty in both
//     (client.py's data=None; purple never calls
//     purple_http_request_set_contents on the start request).
//   - Response: IDENTICAL. Both read the next hop from the response's
//     x-goog-upload-url header (client.py:298; conversation.c:1774).
//   - Finalize (single-shot case) upload-command headers: IDENTICAL.
//     x-goog-upload-command: "upload, finalize", x-goog-upload-protocol:
//     resumable, x-goog-upload-offset: 0, method PUT, body = the raw file
//     bytes (client.py:303-309; conversation.c:1797-1805 -- purple's
//     is_final_chunk branch, which is what fires when the whole file fits
//     in one chunk, exactly this port's single-shot behavior; purple's own
//     chunk_granularity is currently hard-coded to the full data size
//     (conversation.c:~1780-1782, the size-based branch is commented out),
//     so purple itself is single-shot 100% of the time today, not just for
//     "realistically-sized" attachments). Purple's code path still supports
//     splitting into multiple non-final "upload" chunks before the final
//     "upload, finalize" one if that were ever re-enabled
//     (conversation.c:1784-1821); that branch is NOT ported here -- the
//     brief's interface is single-shot, matching both Python and purple's
//     CURRENT actual behavior.
//   - Response body: IDENTICAL. base64(binary UploadMetadata proto)
//     (client.py:311-319's base64.b64decode + ParseFromString;
//     conversation.c:1693-1694's g_base64_decode + protobuf_c_message_unpack).
//
//   - DIVERGENCE 1 (auth header, caught on audit -- match purple): every
//     purple_http_request purple-googlechat sends, upload calls included
//     (conversation.c:1808 inside the finalize/chunk-PUT loop, :1851 for the
//     start POST), is first run through googlechat_set_auth_headers
//     (googlechat_connection.c:210-224), which -- whenever ha->access_token
//     is unset, the ONLY mode this bridge's cookie-based auth model uses,
//     matching this port having no OAuth Bearer-token concept anywhere --
//     attaches "X-Framework-XSRF-Token: <ha->xsrf_token>". Python's
//     upload_file calls Client._base_request directly (client.py:295,309),
//     NOT _pb_request/_gc_request (client.py:591-598), which is the only
//     place client.py sets "x-framework-xsrf-token" -- so Python's upload
//     requests never carry this header at all. This port matches purple
//     (the actively-maintained, current reference): both UploadFile requests
//     below set x-framework-xsrf-token via c.XSRFToken(), the exact same
//     accessor api.go's doRequestOnce (api.go:218-220) uses for every other
//     RPC.
//
//   - DIVERGENCE 2 (the #114-relevant query-param signal):
//     Python's upload_file routes through Client._base_request
//     (client.py:617-675), which UNCONDITIONALLY appends "alt=<response_type>"
//     and "key=<API_KEY>" query parameters onto EVERY request it sends --
//     including upload_file's two calls, even though response_type="" and
//     content_type=None mean this isn't a proto/JSON RPC at all
//     (client.py:658-666: `params.update({"alt": response_type, "key":
//     API_KEY})`, then merged onto the URL by aiohttp's `params=` kwarg on
//     BOTH the start POST and the finalize PUT -- so Python's actual wire
//     request is `POST /uploads?group_id=X&alt=&key=<API_KEY>`, and the
//     finalize PUT lands on `<upload_url>&alt=&key=<API_KEY>`).
//     purple-googlechat's dedicated upload path (conversation.c:1839,
//     1792-1806) builds the request with NEITHER "alt" NOR "key" anywhere --
//     just group_id on the start URL, and the bare upload_url handed back
//     by the server for the finalize PUT, untouched. Since purple-googlechat
//     is actively maintained against the real, current server (through
//     2026) and client.py is a frozen 2023 snapshot that inherits these two
//     params as a side effect of code reuse (not anything upload-specific),
//     this port matches purple: neither "alt" nor "key" is sent anywhere in
//     this file. This is a concrete, checked-in candidate explanation for
//     #114 ("/uploads returns 500 since ~Feb 2026"): Google's backend may
//     now reject/500 on an empty "alt" value or a JSON-RPC-scoped API key
//     presented to the dedicated upload endpoint, where the /api/* JSON-RPC
//     surface (api.go) still tolerates (and requires) both. Live
//     verification against the real endpoint is deferred to Task 5 /
//     Task 13 (test account throttled per the task brief); this file
//     implements the shape purple-googlechat currently uses.
import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"google.golang.org/protobuf/proto"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// uploadURL mirrors client.py:27's UPLOAD_URL and
// googlechat_conversation.c:1839's literal "https://chat.google.com/uploads"
// -- identical in both references. Overridable via Client.uploadBaseURL
// (api.go) for tests.
const uploadURL = "https://chat.google.com/uploads"

// stripWhitespace removes every whitespace byte from data, wherever it
// occurs -- not just leading/trailing (bytes.TrimSpace's coverage). Mirrors
// Python's base64.b64decode(s) default (validate=False) behavior: "If
// validate is false ... characters that are neither in the normal base-64
// alphabet nor the alternative alphabet are discarded prior to the padding
// check" -- client.py:311-319 relies on exactly this leniency, so a
// finalize response wrapped with embedded newlines (fixed-width base64 line
// wrapping) or mangled by an intermediary (stray CRLF) still decodes in
// Python. Go's encoding/base64.StdEncoding.DecodeString has no such
// tolerance -- it errors on ANY embedded whitespace -- so this port needs
// its own equivalent stripping step to match. Deliberately narrower than
// Python's "discard any non-alphabet character": that would also silently
// swallow genuine corruption (a stray non-whitespace byte that shouldn't be
// there); this only strips whitespace, the specific class of "harmless
// formatting" the task's brief calls out.
func stripWhitespace(data []byte) []byte {
	out := make([]byte, 0, len(data))
	for _, b := range data {
		// Strip exactly the ASCII whitespace set (space, tab, newline,
		// carriage return, vertical tab, form feed). Deliberately a byte-wise
		// check against those specific bytes rather than
		// unicode.IsSpace(rune(b)) -- the latter would also treat a lone byte
		// 0x85 (NEL) or 0xA0 (NBSP) as whitespace, which is meaningless here:
		// the finalize response is a base64 string, i.e. pure ASCII, so any
		// such high byte is data corruption, not formatting whitespace, and
		// should fall through to the base64 decoder as the error it is rather
		// than being silently removed.
		switch b {
		case ' ', '\t', '\n', '\r', '\v', '\f':
			continue
		default:
			out = append(out, b)
		}
	}
	return out
}

// UploadFile performs a resumable upload of data to Google Chat's /uploads
// endpoint and returns the parsed UploadMetadata, ready to attach as an
// UPLOAD_METADATA annotation on an outgoing message (Task 5). Ports
// maugclib/client.py's Client.upload_file (client.py:275-321), matching
// purple-googlechat's current wire shape where the two references diverge
// -- see this file's package doc comment for the diff.
//
// A non-200 status from either the start request or the finalize request
// (including the #114-reported 500 from /uploads) propagates as a clear,
// wrapped error via Session.Fetch's own error types (*UnexpectedStatusError
// / *NetworkError) -- Session.Fetch already retries a 5xx up to maxRetries
// times before giving up, same policy as every other RPC in this package.
func (c *Client) UploadFile(ctx context.Context, groupID string, data []byte, filename, mimeType string) (*pb.UploadMetadata, error) {
	// x-framework-xsrf-token on both requests below matches purple-googlechat's
	// googlechat_set_auth_headers, called unconditionally before every
	// purple_http_request including both upload calls (see this file's
	// package doc comment, Divergence 1) -- the same accessor api.go's
	// doRequestOnce uses for every other RPC (api.go:218-220).
	xsrfToken := c.XSRFToken()

	startHeaders := http.Header{}
	startHeaders.Set("x-goog-upload-protocol", "resumable")
	startHeaders.Set("x-goog-upload-command", "start")
	startHeaders.Set("x-goog-upload-content-length", strconv.Itoa(len(data)))
	startHeaders.Set("x-goog-upload-content-type", mimeType)
	startHeaders.Set("x-goog-upload-file-name", filename)
	if xsrfToken != "" {
		startHeaders.Set("x-framework-xsrf-token", xsrfToken)
	}

	base := c.uploadBaseURL
	if base == "" {
		base = uploadURL
	}
	startQuery := url.Values{}
	startQuery.Set("group_id", groupID)
	startURL := base + "?" + startQuery.Encode()

	// client.py's data=None for the start request -- no body.
	startResp, err := c.session.Fetch(ctx, http.MethodPost, startURL, startHeaders, nil)
	if err != nil {
		return nil, fmt.Errorf("gchatmeow: upload start request failed: %w", err)
	}

	nextURL := startResp.Header.Get("x-goog-upload-url")
	if nextURL == "" {
		// Mirrors client.py:297-300's explicit
		// `except KeyError: raise exceptions.NetworkError(...)`.
		return nil, fmt.Errorf("gchatmeow: upload start response missing x-goog-upload-url header")
	}

	finalizeHeaders := http.Header{}
	finalizeHeaders.Set("x-goog-upload-command", "upload, finalize")
	finalizeHeaders.Set("x-goog-upload-protocol", "resumable")
	finalizeHeaders.Set("x-goog-upload-offset", "0")
	if xsrfToken != "" {
		finalizeHeaders.Set("x-framework-xsrf-token", xsrfToken)
	}

	finalizeResp, err := c.session.Fetch(ctx, http.MethodPut, nextURL, finalizeHeaders, data)
	if err != nil {
		return nil, fmt.Errorf("gchatmeow: upload finalize request failed: %w", err)
	}

	decoded, err := base64.StdEncoding.DecodeString(string(stripWhitespace(finalizeResp.Body)))
	if err != nil {
		// Mirrors client.py:314-315's `except binascii.Error`.
		return nil, fmt.Errorf("gchatmeow: failed to decode base64 upload response: %w", err)
	}

	meta := &pb.UploadMetadata{}
	if err := proto.Unmarshal(decoded, meta); err != nil {
		// Mirrors client.py:316-319's `except proto.DecodeError`.
		return nil, fmt.Errorf("gchatmeow: failed to parse upload response protobuf: %w", err)
	}
	return meta, nil
}
