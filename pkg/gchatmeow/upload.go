package gchatmeow

// Resumable file upload to Google Chat's /uploads endpoint, following
// purple-googlechat's actively-maintained googlechat_conversation.c
// (~1745-1859, "Received the url to upload the image data to" / "Received
// the upload metadata of the sent image") for the CURRENT wire shape --
// issue #114 tracks Google's chat.google.com/uploads endpoint reportedly
// 500ing since ~Feb 2026, and this task is the #114-risk path.
//
// Wire shape (per purple-googlechat, current through 2026):
//
//   - URL: POST to "https://chat.google.com/uploads" with a group_id query
//     parameter (googlechat_conversation.c:1839's literal
//     "https://chat.google.com/uploads?group_id=%s").
//   - Start headers (upload-command headers): x-goog-upload-protocol:
//     resumable, x-goog-upload-command: start, x-goog-upload-content-length,
//     x-goog-upload-content-type, x-goog-upload-file-name
//     (googlechat_conversation.c:1843-1847). Request body is empty (purple
//     never calls purple_http_request_set_contents on the start request).
//   - Response: read the next hop from the response's x-goog-upload-url
//     header (conversation.c:1774).
//   - Finalize (single-shot case) upload-command headers:
//     x-goog-upload-command: "upload, finalize", x-goog-upload-protocol:
//     resumable, x-goog-upload-offset: 0, method PUT, body = the raw file
//     bytes (conversation.c:1797-1805 -- purple's is_final_chunk branch,
//     which is what fires when the whole file fits in one chunk, exactly
//     this port's single-shot behavior; purple's own chunk_granularity is
//     currently hard-coded to the full data size (conversation.c:~1780-1782,
//     the size-based branch is commented out), so purple itself is
//     single-shot 100% of the time today, not just for "realistically-sized"
//     attachments). Purple's code path still supports splitting into
//     multiple non-final "upload" chunks before the final "upload, finalize"
//     one if that were ever re-enabled (conversation.c:1784-1821); that
//     branch is NOT ported here -- the brief's interface is single-shot,
//     matching purple's CURRENT actual behavior.
//   - Response body: base64(binary UploadMetadata proto)
//     (conversation.c:1693-1694's g_base64_decode +
//     protobuf_c_message_unpack).
//
// Two details of this shape were caught on audit against purple as the
// authoritative current reference:
//
//   - Auth header: every purple_http_request purple-googlechat sends, upload
//     calls included (conversation.c:1808 inside the finalize/chunk-PUT loop,
//     :1851 for the start POST), is first run through
//     googlechat_set_auth_headers (googlechat_connection.c:210-224), which --
//     whenever ha->access_token is unset, the ONLY mode this bridge's
//     cookie-based auth model uses, matching this port having no OAuth
//     Bearer-token concept anywhere -- attaches "X-Framework-XSRF-Token:
//     <ha->xsrf_token>". This port matches: both UploadFile requests below
//     set x-framework-xsrf-token via c.XSRFToken(), the exact same accessor
//     api.go's doRequestOnce (api.go:218-220) uses for every other RPC.
//
//   - The #114-relevant query-param signal: purple-googlechat's dedicated
//     upload path (conversation.c:1839, 1792-1806) builds the request with
//     NEITHER "alt" NOR "key" anywhere -- just group_id on the start URL, and
//     the bare upload_url handed back by the server for the finalize PUT,
//     untouched. This port matches purple: neither "alt" nor "key" is sent
//     anywhere in this file. A generic request builder that unconditionally
//     appends "alt=<response_type>" and "key=<API_KEY>" to every /api/*
//     request would leak them onto the upload calls too (even though the
//     upload endpoint is not a proto/JSON RPC); this dedicated path
//     deliberately avoids that. It is a concrete, checked-in candidate
//     explanation for #114 ("/uploads returns 500 since ~Feb 2026"):
//     Google's backend may now reject/500 on an empty "alt" value or a
//     JSON-RPC-scoped API key presented to the dedicated upload endpoint,
//     where the /api/* JSON-RPC surface (api.go) still tolerates (and
//     requires) both. Live verification against the real endpoint is
//     deferred to Task 5 / Task 13 (test account throttled per the task
//     brief); this file implements the shape purple-googlechat currently
//     uses.
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

// uploadURL is googlechat_conversation.c:1839's literal
// "https://chat.google.com/uploads". Overridable via Client.uploadBaseURL
// (api.go) for tests.
const uploadURL = "https://chat.google.com/uploads"

// stripWhitespace removes every whitespace byte from data, wherever it
// occurs -- not just leading/trailing (bytes.TrimSpace's coverage). A
// finalize response can arrive wrapped with embedded newlines (fixed-width
// base64 line wrapping) or mangled by an intermediary (stray CRLF), and
// Go's encoding/base64.StdEncoding.DecodeString errors on ANY embedded
// whitespace, so this port strips whitespace first to tolerate that
// formatting. Deliberately narrower than "discard any non-alphabet
// character": that would also silently swallow genuine corruption (a stray
// non-whitespace byte that shouldn't be there); this only strips
// whitespace, the specific class of "harmless formatting" the task's brief
// calls out.
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
// UPLOAD_METADATA annotation on an outgoing message (Task 5). It follows
// purple-googlechat's current wire shape -- see this file's package doc
// comment for the details.
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
	// package doc comment, the auth-header note) -- the same accessor api.go's
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

	// No body on the start request.
	startResp, err := c.session.Fetch(ctx, http.MethodPost, startURL, startHeaders, nil)
	if err != nil {
		return nil, fmt.Errorf("gchatmeow: upload start request failed: %w", err)
	}

	nextURL := startResp.Header.Get("x-goog-upload-url")
	if nextURL == "" {
		return nil, fmt.Errorf("gchatmeow: upload start response missing x-goog-upload-url header")
	}

	// nextURL is SERVER-supplied (it came back in the start response's
	// header), so it is treated the way every other server-supplied URL in
	// this package is: Session-derived credentials are gated on the host
	// allowlist. Session.buildRequest already does that for the Cookie
	// header, but it attaches every caller-supplied header unconditionally,
	// so the anti-CSRF token has to be gated here instead -- otherwise an
	// upload URL pointing off google.com would be handed the token along
	// with the file bytes.
	nextParsed, err := url.Parse(nextURL)
	if err != nil {
		return nil, fmt.Errorf("gchatmeow: upload start response returned an unusable x-goog-upload-url %q: %w", nextURL, err)
	}

	finalizeHeaders := http.Header{}
	finalizeHeaders.Set("x-goog-upload-command", "upload, finalize")
	finalizeHeaders.Set("x-goog-upload-protocol", "resumable")
	finalizeHeaders.Set("x-goog-upload-offset", "0")
	if xsrfToken != "" && c.session.hostAllowed(nextParsed) {
		finalizeHeaders.Set("x-framework-xsrf-token", xsrfToken)
	}

	finalizeResp, err := c.session.Fetch(ctx, http.MethodPut, nextURL, finalizeHeaders, data)
	if err != nil {
		return nil, fmt.Errorf("gchatmeow: upload finalize request failed: %w", err)
	}

	decoded, err := base64.StdEncoding.DecodeString(string(stripWhitespace(finalizeResp.Body)))
	if err != nil {
		return nil, fmt.Errorf("gchatmeow: failed to decode base64 upload response: %w", err)
	}

	meta := &pb.UploadMetadata{}
	if err := proto.Unmarshal(decoded, meta); err != nil {
		return nil, fmt.Errorf("gchatmeow: failed to parse upload response protobuf: %w", err)
	}
	return meta, nil
}
