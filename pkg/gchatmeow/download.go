package gchatmeow

// Attachment download, ported from two Python sources:
//
//   - mautrix_googlechat/portal.py's _preprocess_annotations (portal.py:
//     1464-1524, upload_metadata branch at 1470-1485): builds the URL to
//     fetch an UPLOAD_METADATA annotation's file from.
//   - maugclib/client.py's Client.download_attachment (client.py:182-236)
//     and its helper Client.read_with_max_size (client.py:238-273): the
//     actual GET, with manual redirect-following, per-hop cookie gating,
//     and a size cap.
//
// portal.py:1536-1539 (_process_googlechat_attachment) picks between these
// two download paths itself, based on the SAME "*.google.com" host check
// used inside download_attachment:
//
//	if att.url.host.endswith(".google.com"):
//	    data, mime, filename = await source.client.download_attachment(att.url, max_size)
//	else:
//	    data, mime, filename = await self._download_external_attachment(att.url, max_size)
//
// i.e. Python has TWO nearly-identical implementations of "GET with a size
// cap" -- one that assumes a *.google.com host and always attaches cookies
// (_download_external_attachment, portal.py:1455-1462, backed by a fresh
// aiohttp.ClientSession -- no cookies, no redirect following at all, single
// hop only), and one used for both google.com AND non-google.com hosts
// inside a single call (download_attachment itself, which internally
// switches per-hop between the authenticated session and a fresh
// cookie-less one). DownloadAttachment below collapses both into ONE loop --
// matching download_attachment's per-hop branch -- since it is a strict
// superset: a URL that never leaves google.com just never takes the
// cookie-less branch, and a URL that's cookie-less end-to-end (the
// external-attachment case) behaves identically to
// _download_external_attachment except for also following redirects, which
// portal.py's external path never needed to (external CDN links don't
// redirect back to an authenticated Google endpoint) but doing so anyway is
// harmless and keeps this package's callers (Task 3) from needing to know
// which of the two Python code paths applies to a given URL up front.
import (
	"context"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

const (
	// getAttachmentURLEndpoint mirrors portal.py:1480's
	// `URL("https://chat.google.com/api/get_attachment_url")` verbatim.
	getAttachmentURLEndpoint = "https://chat.google.com/api/get_attachment_url"

	// fifeImageSize mirrors portal.py:1478's `query["sz"] = "w10000-h10000"`
	// -- an effectively-unbounded size request to Google's FIFE image proxy
	// so the full-resolution original is returned rather than a thumbnail.
	fifeImageSize = "w10000-h10000"

	// maxDownloadRedirects mirrors client.py:206's `while depth < 10` loop
	// bound (comment: "Usually there are 4 redirects for files and 1 for
	// images"). Unlike Python -- whose loop simply falls off the end and
	// implicitly returns None if the 10th response is STILL a redirect,
	// which would crash its caller trying to unpack a 3-tuple -- this port
	// deliberately raises an explicit error once the cap is reached, per
	// the brief's mandate ("capped at 10 -> error on the 11th"); see
	// DownloadAttachment's loop.
	maxDownloadRedirects = 10
)

// downloadHTTPClient performs each single-hop GET for DownloadAttachment,
// with automatic redirect-following disabled via CheckRedirect:
// DownloadAttachment's own loop inspects each redirect's Location header
// itself and decides afresh, at EVERY hop, whether the next hop's host
// should get cookies -- exactly mirroring client.py:217-223's comment
// ("Follow redirects manually in order to re-add authorization headers when
// redirected from googleusercontent.com back to chat.google.com"). Go's
// default automatic redirect-following would instead permanently strip the
// Cookie header on the FIRST cross-host hop (net/http's
// shouldCopyHeaderOnRedirect) and never restore it even if a later hop
// lands back on an allowed host. Overridable in tests, same seam pattern as
// avatar.go's avatarHTTPClient.
var downloadHTTPClient = &http.Client{
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

// AttachmentURL builds the URL to fetch an UPLOAD_METADATA annotation's file
// from, mirroring portal.py:1470-1485's _preprocess_annotations exactly for
// the `annotation.HasField("upload_metadata")` branch:
//
//	query = {"url_type": "DOWNLOAD_URL", "attachment_token": meta.attachment_token}
//	if meta.content_type.startswith("image/"):
//	    query["url_type"] = "FIFE_URL"
//	    query["sz"] = "w10000-h10000"
//	    query["content_type"] = meta.content_type
//	url = URL("https://chat.google.com/api/get_attachment_url").with_query(query)
//
// isImage reports whether the image branch was taken (content_type has an
// "image/" prefix), for callers (M5 Task 3) that need the same signal Python
// derives independently via mime.split("/")[0] on the download response.
func (c *Client) AttachmentURL(meta *pb.UploadMetadata) (string, bool, error) {
	if meta == nil {
		return "", false, fmt.Errorf("googlechat: nil upload metadata")
	}

	contentType := meta.GetContentType()
	isImage := strings.HasPrefix(contentType, "image/")

	q := url.Values{}
	q.Set("attachment_token", meta.GetAttachmentToken())
	if isImage {
		q.Set("url_type", "FIFE_URL")
		q.Set("sz", fifeImageSize)
		q.Set("content_type", contentType)
	} else {
		q.Set("url_type", "DOWNLOAD_URL")
	}

	u, err := url.Parse(getAttachmentURLEndpoint)
	if err != nil {
		// Unreachable in practice: getAttachmentURLEndpoint is a constant,
		// valid URL. Guarded anyway rather than panicking.
		return "", false, fmt.Errorf("googlechat: invalid get_attachment_url endpoint: %w", err)
	}
	u.RawQuery = q.Encode()
	return u.String(), isImage, nil
}

// DownloadAttachment fetches an attachment from urlStr, following redirects
// manually up to maxDownloadRedirects hops (client.py:182-236's
// download_attachment), sending auth cookies ONLY to hosts matching the
// Client's Session host allowlist (google.com; see session.go's
// hostAllowed/defaultAllowedHostSuffixes and its doc comment on why
// googleusercontent.com is deliberately excluded) and NONE otherwise
// (client.py:211-215's fresh, cookie-less aiohttp.ClientSession for
// non-google.com hops). maxSize, if greater than zero, caps the downloaded
// body: a Content-Length that already exceeds it is rejected before any
// body bytes are read, and the body read itself is ALSO capped in case
// Content-Length was absent or understated (client.py:238-273's
// read_with_max_size). maxSize <= 0 means no cap -- unlike Python's
// read_with_max_size, whose read loop does NOT special-case max_size == 0
// and would therefore raise FileTooLargeError on the first non-empty body
// even though its own docstring documents 0 as "unlimited" (client.py:190-193
// says a check only applies "If [max_size] is greater than zero"); this port
// implements the documented contract rather than replicating that latent
// bug, which no real caller trips over today since
// self.matrix.media_config.upload_size (portal.py:1534) is always a
// positive homeserver-configured limit.
//
// Returns the raw body, the mime type (from the Content-Type header) and
// the filename (from Content-Disposition, falling back to the URL's last
// path segment -- client.py:226-230).
func (c *Client) DownloadAttachment(ctx context.Context, urlStr string, maxSize int64) ([]byte, string, string, error) {
	depth := 0
	for {
		if depth >= maxDownloadRedirects {
			return nil, "", "", fmt.Errorf("googlechat: attachment download exceeded %d redirects", maxDownloadRedirects)
		}
		depth++

		u, err := url.Parse(urlStr)
		if err != nil {
			return nil, "", "", fmt.Errorf("googlechat: invalid attachment url %q: %w", urlStr, err)
		}

		// client.py:208-215: `if url.host.endswith(".google.com"): ... else: ...`.
		// c.session.hostAllowed reuses the SAME allowlist that gates cookie
		// attachment for every other RPC this Client makes (session.go); the
		// cookie-less branch below builds a plain request with NO
		// Session-derived headers at all (User-Agent, Connection), matching
		// Python's fresh aiohttp.ClientSession() having none of the
		// authenticated session's defaults either -- and mirroring this
		// package's existing avatar.go/DownloadAvatar cookie-less pattern.
		var req *http.Request
		if c.session.hostAllowed(u) {
			req, err = c.session.buildRequest(ctx, http.MethodGet, urlStr, nil, nil)
		} else {
			req, err = http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
		}
		if err != nil {
			return nil, "", "", fmt.Errorf("googlechat: building attachment request: %w", err)
		}

		resp, err := downloadHTTPClient.Do(req)
		if err != nil {
			return nil, "", "", fmt.Errorf("googlechat: attachment download failed: %w", err)
		}

		// client.py:220-223: only these four statuses are followed as
		// redirects (notably NOT 303, unlike net/http's default redirect
		// policy).
		if isRedirectStatus(resp.StatusCode) {
			loc := resp.Header.Get("Location")
			resp.Body.Close()
			if loc == "" {
				return nil, "", "", fmt.Errorf("googlechat: redirect response missing Location header")
			}
			next, err := u.Parse(loc)
			if err != nil {
				return nil, "", "", fmt.Errorf("googlechat: invalid redirect location %q: %w", loc, err)
			}
			urlStr = next.String()
			continue
		}

		// client.py:225: `resp.raise_for_status()` -- aiohttp raises for any
		// status >= 400, leaving 2xx/3xx (other than the four redirect
		// codes just handled) to fall through to the success path below,
		// same as here.
		if resp.StatusCode >= 400 {
			resp.Body.Close()
			return nil, "", "", &UnexpectedStatusError{URL: urlStr, Status: resp.StatusCode}
		}

		defer resp.Body.Close()

		mimeType := resp.Header.Get("Content-Type")
		filename := attachmentFilename(resp, u)

		data, err := readWithMaxSize(resp, maxSize)
		if err != nil {
			return nil, "", "", err
		}
		return data, mimeType, filename, nil
	}
}

// isRedirectStatus mirrors client.py:220's
// `resp.status in (301, 302, 307, 308)` verbatim -- 303 See Other is
// deliberately NOT included, matching Python.
func isRedirectStatus(status int) bool {
	switch status {
	case http.StatusMovedPermanently, http.StatusFound, http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
		return true
	default:
		return false
	}
}

// attachmentFilename extracts a filename from resp's Content-Disposition
// header, falling back to the last path segment of u -- mirrors
// client.py:226-230:
//
//	try:
//	    _, params = cgi.parse_header(resp.headers["Content-Disposition"])
//	    filename = params.get("filename") or url.path.split("/")[-1]
//	except KeyError:
//	    filename = url.path.split("/")[-1]
//
// The fallback is computed via a manual split rather than path.Base so an
// empty u.Path yields "" (matching Python's "".split("/")[-1] == "")
// instead of path.Base's "." for an empty string.
func attachmentFilename(resp *http.Response, u *url.URL) string {
	if cd := resp.Header.Get("Content-Disposition"); cd != "" {
		if _, params, err := mime.ParseMediaType(cd); err == nil {
			if fn := params["filename"]; fn != "" {
				return fn
			}
		}
	}
	segments := strings.Split(u.Path, "/")
	return segments[len(segments)-1]
}

// readWithMaxSize reads resp's body, enforcing maxSize if it is greater than
// zero -- mirrors client.py:238-273's read_with_max_size, minus the
// preallocated-bytearray bookkeeping (an aiohttp-specific optimization with
// no Go equivalent needed: io.LimitReader + io.ReadAll do the same job).
// Content-Length is checked first as a fast-path rejection before any body
// bytes are read (client.py:241-242); the read itself is ALSO capped via
// io.LimitReader(maxSize+1) in case Content-Length was absent (chunked
// transfer) or understated (client.py:248-255's "read one more byte than
// the cap, then fail if we got it").
func readWithMaxSize(resp *http.Response, maxSize int64) ([]byte, error) {
	if maxSize <= 0 {
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("googlechat: reading attachment body: %w", err)
		}
		return data, nil
	}

	if resp.ContentLength > maxSize {
		return nil, fmt.Errorf("googlechat: attachment Content-Length %d exceeds max %d: %w", resp.ContentLength, maxSize, ErrFileTooLarge)
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxSize+1))
	if err != nil {
		return nil, fmt.Errorf("googlechat: reading attachment body: %w", err)
	}
	if int64(len(data)) > maxSize {
		return nil, fmt.Errorf("googlechat: attachment body exceeds max %d bytes: %w", maxSize, ErrFileTooLarge)
	}
	return data, nil
}
