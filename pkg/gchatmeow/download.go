package gchatmeow

import (
	"context"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

const (
	getAttachmentURLEndpoint = "https://chat.google.com/api/get_attachment_url"

	// fifeImageSize asks the FIFE proxy for the full-resolution original.
	fifeImageSize = "w10000-h10000"

	// maxDownloadRedirects caps manual redirects (usually 4 for files, 1 for images).
	maxDownloadRedirects = 10
)

// downloadHTTPClient never follows redirects itself: the loop must re-add cookies when a
// googleusercontent.com hop redirects back to chat.google.com, which net/http would not.
var downloadHTTPClient = newDownloadHTTPClient()

// downloadConnectTimeout bounds only dial and TLS; the transfer is bounded by ctx and maxSize.
const downloadConnectTimeout = 30 * time.Second

func newDownloadHTTPClient() *http.Client {
	return &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Transport: &http.Transport{
			// Proxy is kept (unlike avatar.go): losing all media behind a proxy is too costly, and the
			// URL here is Google's fixed endpoint, not remote-supplied.
			Proxy:               http.ProxyFromEnvironment,
			DialContext:         newGuardedDialer(downloadConnectTimeout).DialContext,
			TLSHandshakeTimeout: downloadConnectTimeout,
		},
	}
}

// AttachmentURL builds the fetch URL for an UPLOAD_METADATA annotation; isImage reports the FIFE_URL branch.
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
		return "", false, fmt.Errorf("googlechat: invalid get_attachment_url endpoint: %w", err)
	}
	u.RawQuery = q.Encode()
	return u.String(), isImage, nil
}

// DownloadAttachment follows redirects manually, sending cookies only to allowlisted hosts. maxSize > 0 caps
// the body (0 means unlimited). Returns body, mime type and filename.
func (c *Client) DownloadAttachment(ctx context.Context, urlStr string, maxSize int64) ([]byte, string, string, error) {
	// Force manual redirects so the per-hop cookie decision holds.
	client := *downloadHTTPClient
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}

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

		// Allowlisted hops use the Session; others get a plain request with no Session-derived headers.
		var req *http.Request
		if c.session.hostAllowed(u) {
			req, err = c.session.buildRequest(ctx, http.MethodGet, urlStr, nil, nil)
		} else {
			req, err = http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
		}
		if err != nil {
			return nil, "", "", fmt.Errorf("googlechat: building attachment request: %w", err)
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, "", "", fmt.Errorf("googlechat: attachment download failed: %w", err)
		}

		// 303 is deliberately not followed.
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

// isRedirectStatus reports 301, 302, 307, 308; 303 is deliberately excluded.
func isRedirectStatus(status int) bool {
	switch status {
	case http.StatusMovedPermanently, http.StatusFound, http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
		return true
	default:
		return false
	}
}

// attachmentFilename uses Content-Disposition, falling back to u's last path segment ("" for an empty path).
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

// readWithMaxSize enforces maxSize via Content-Length fast-reject plus a LimitReader(maxSize+1).
func readWithMaxSize(resp *http.Response, maxSize int64) ([]byte, error) {
	return readBodyWithMaxSize(resp, maxSize, "attachment")
}

// readBodyWithMaxSize is readWithMaxSize with the noun used in error messages.
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
