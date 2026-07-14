package gchatmeow

// Ghost/portal avatar image download, ported from
// mautrix_googlechat/puppet.py's _reupload_gc_photo (puppet.py:245-266):
// Python opens a FRESH, cookie-less aiohttp.ClientSession for this fetch
// rather than reusing the authenticated Google Chat session -- avatar URLs
// (typically lh3.googleusercontent.com) are unauthenticated CDN links, and
// are not covered by Session's allowedHostSuffixes gate (session.go), which
// is scoped to chat.google.com's /api endpoints. This file mirrors that: a
// plain, standalone HTTP GET with no cookies attached, living in this
// package (rather than pkg/connector) so pkg/connector never needs to touch
// net/http directly -- every outbound network call stays behind this
// package's surface, RPCs and avatar downloads alike.
//
// sha256 hashing and re-upload-skip-when-unchanged (puppet.py:251-256) is
// NOT ported here: bridgev2.Avatar.Reupload (ghost.go) already does that
// generically for every network connector, given just the raw bytes this
// file returns.
import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

// avatarHTTPClient is used by DownloadAvatar. Overridden in tests (this
// package's own _test.go files can reach the unexported var) so avatar
// download exercises real HTTP round-trips against an httptest server
// without needing a live network -- mirrors this codebase's other
// test-seam vars (api.go's baseURL, client.go's sleepFn).
var avatarHTTPClient = http.DefaultClient

// ForceHTTPS rewrites rawURL's scheme to https, matching puppet.py's
// URL(url).with_scheme("https") (puppet.py:249), applied before every avatar
// download regardless of what scheme the server-supplied avatar_url used.
// Malformed URLs are returned unchanged rather than erroring here --
// DownloadAvatar's own request construction still surfaces a clear error for
// anything genuinely unusable as a URL.
func ForceHTTPS(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	u.Scheme = "https"
	return u.String()
}

// DownloadAvatar fetches raw avatar image bytes from url via a plain,
// unauthenticated GET (see this file's package doc comment for why this
// bypasses Session). Callers are expected to have already run the URL
// through ForceHTTPS; DownloadAvatar itself does not rewrite the scheme, so
// its own tests can point it at a plain-http httptest server.
func DownloadAvatar(ctx context.Context, avatarURL string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, avatarURL, nil)
	if err != nil {
		return nil, fmt.Errorf("googlechat: invalid avatar url: %w", err)
	}
	resp, err := avatarHTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("googlechat: avatar download failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("googlechat: avatar download failed: unexpected status %s", resp.Status)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("googlechat: failed to read avatar response body: %w", err)
	}
	return data, nil
}
