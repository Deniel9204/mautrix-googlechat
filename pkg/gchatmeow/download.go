package gchatmeow

// Attachment download: builds the URL to fetch an UPLOAD_METADATA
// annotation's file from, then performs the actual GET with manual
// redirect-following, per-hop cookie gating, and a size cap.
//
// The download path is chosen by a "*.google.com" host check: google.com
// hosts get the authenticated (cookie-bearing) session, and everything else
// gets a fresh cookie-less request. DownloadAttachment below collapses both
// cases into ONE loop, switching per-hop between the authenticated session
// and a cookie-less one. This is a strict superset of the two paths the
// protocol needs: a URL that never leaves google.com simply never takes the
// cookie-less branch, and a URL that's cookie-less end-to-end (an external
// CDN attachment) behaves like a plain single-session download except that
// it also follows redirects. External CDN links don't redirect back to an
// authenticated Google endpoint, so following redirects there is harmless --
// and it keeps this package's callers (Task 3) from needing to know which
// download path applies to a given URL up front.
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
	// getAttachmentURLEndpoint is Google Chat's attachment-URL API endpoint.
	getAttachmentURLEndpoint = "https://chat.google.com/api/get_attachment_url"

	// fifeImageSize is the `sz` query value -- an effectively-unbounded size
	// request to Google's FIFE image proxy so the full-resolution original is
	// returned rather than a thumbnail.
	fifeImageSize = "w10000-h10000"

	// maxDownloadRedirects caps manual redirect-following at 10 hops (usually
	// there are 4 redirects for files and 1 for images). On reaching the cap
	// this port deliberately raises an explicit error rather than returning
	// empty-handed, per the brief's mandate ("capped at 10 -> error on the
	// 11th"); see DownloadAttachment's loop.
	maxDownloadRedirects = 10
)

// downloadHTTPClient performs each single-hop GET for DownloadAttachment,
// with automatic redirect-following disabled via CheckRedirect:
// DownloadAttachment's own loop inspects each redirect's Location header
// itself and decides afresh, at EVERY hop, whether the next hop's host
// should get cookies. Redirects must be followed manually in order to re-add
// authorization headers when redirected from googleusercontent.com back to
// chat.google.com. Go's default automatic redirect-following would instead
// permanently strip the Cookie header on the FIRST cross-host hop (net/http's
// shouldCopyHeaderOnRedirect) and never restore it even if a later hop
// lands back on an allowed host. Overridable in tests, same seam pattern as
// avatar.go's avatarHTTPClient.
var downloadHTTPClient = &http.Client{
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

// AttachmentURL builds the URL to fetch an UPLOAD_METADATA annotation's file
// from. For an image content_type the query uses url_type=FIFE_URL with a
// sz bound and the content_type echoed back; otherwise url_type=DOWNLOAD_URL.
//
// isImage reports whether the image branch was taken (content_type has an
// "image/" prefix), for callers (M5 Task 3) that need the same signal
// derivable from the download response's mime type.
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
// manually up to maxDownloadRedirects hops, sending auth cookies ONLY to
// hosts matching the Client's Session host allowlist (google.com; see
// session.go's hostAllowed/defaultAllowedHostSuffixes and its doc comment on
// why googleusercontent.com is deliberately excluded) and NONE otherwise (a
// fresh, cookie-less request for non-google.com hops). maxSize, if greater
// than zero, caps the downloaded body: a Content-Length that already exceeds
// it is rejected before any body bytes are read, and the body read itself is
// ALSO capped in case Content-Length was absent or understated. maxSize <= 0
// means no cap: 0 is the documented "unlimited" contract. No real caller
// trips over this today since the homeserver-configured media upload_size
// limit is always positive.
//
// Returns the raw body, the mime type (from the Content-Type header) and
// the filename (from Content-Disposition, falling back to the URL's last
// path segment).
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

		// The ".google.com" host check decides cookie vs cookie-less:
		// c.session.hostAllowed reuses the SAME allowlist that gates cookie
		// attachment for every other RPC this Client makes (session.go); the
		// cookie-less branch below builds a plain request with NO
		// Session-derived headers at all (User-Agent, Connection) -- mirroring
		// this package's existing avatar.go/DownloadAvatar cookie-less pattern.
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

		// Only these four statuses are followed as redirects (notably NOT
		// 303, unlike net/http's default redirect policy).
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

		// Any status >= 400 is treated as an error; 2xx/3xx (other than the
		// four redirect codes just handled) fall through to the success path
		// below.
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

// isRedirectStatus reports whether status is one of 301, 302, 307, 308 --
// 303 See Other is deliberately NOT included.
func isRedirectStatus(status int) bool {
	switch status {
	case http.StatusMovedPermanently, http.StatusFound, http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
		return true
	default:
		return false
	}
}

// attachmentFilename extracts a filename from resp's Content-Disposition
// header, falling back to the last path segment of u.
//
// The fallback is computed via a manual split rather than path.Base so an
// empty u.Path yields "" instead of path.Base's "." for an empty string.
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
// zero. Content-Length is checked first as a fast-path rejection before any
// body bytes are read; the read itself is ALSO capped via
// io.LimitReader(maxSize+1) in case Content-Length was absent (chunked
// transfer) or understated -- read one more byte than the cap, then fail if
// we got it.
func readWithMaxSize(resp *http.Response, maxSize int64) ([]byte, error) {
	return readBodyWithMaxSize(resp, maxSize, "attachment")
}

// readBodyWithMaxSize is readWithMaxSize with the noun used in error
// messages made explicit, so the avatar path (avatar.go) does not report a
// size failure as an "attachment" problem and send an operator debugging a
// ghost-avatar sync down the wrong trail.
func readBodyWithMaxSize(resp *http.Response, maxSize int64, what string) ([]byte, error) {
	if maxSize <= 0 {
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("googlechat: reading %s body: %w", what, err)
		}
		return data, nil
	}

	if resp.ContentLength > maxSize {
		return nil, fmt.Errorf("googlechat: %s Content-Length %d exceeds max %d: %w", what, resp.ContentLength, maxSize, ErrFileTooLarge)
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxSize+1))
	if err != nil {
		return nil, fmt.Errorf("googlechat: reading %s body: %w", what, err)
	}
	if int64(len(data)) > maxSize {
		return nil, fmt.Errorf("googlechat: %s body exceeds max %d bytes: %w", what, maxSize, ErrFileTooLarge)
	}
	return data, nil
}
