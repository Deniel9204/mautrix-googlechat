package gchatmeow

// external.go -- fetching media whose URL a REMOTE PARTY chose.
//
// A url_metadata annotation (a link chip, and the shape a shared GIF arrives
// in) names an arbitrary third-party URL. That makes it a different security
// problem from every other download in this package:
//
//   - DownloadAttachment talks to Google's own fixed endpoint, which is why
//     downloadHTTPClient is allowed to keep Proxy: ProxyFromEnvironment and to
//     accept plain http. Neither concession is safe here -- see below.
//   - DownloadAvatar already fetches an untrusted URL (a Chat app can register
//     its own icon host) and is hardened for exactly that. This file applies
//     the SAME policy, differing only in the size cap (caller-supplied, since
//     media is bounded by the homeserver's upload limit rather than by what a
//     sensible avatar weighs) and in returning the content type and filename
//     the Matrix side needs.
//
// The policy, deliberately identical to avatar.go's:
//
//  1. https on the initial URL AND on every redirect hop, so an https host
//     cannot bounce the bridge into a plaintext fetch.
//  2. Every hop's RESOLVED address is checked by newGuardedDialer, which runs
//     after DNS -- so a hostname that resolves to an internal address (DNS
//     rebinding) is refused too.
//  3. No proxy. With one configured, net/http would CONNECT to the proxy and
//     the dialer would only ever see the proxy's address, reducing (2) to a
//     no-op. Failing closed is right: an operator whose egress requires a
//     proxy loses inline GIFs, rather than unknowingly losing the protection.
//  4. Redirects are followed manually and capped.
//  5. The body is size-capped by the caller's limit.
//  6. No Session, no cookies, no XSRF header. This is a package-level function
//     with no *Client receiver precisely so that attaching Google credentials
//     to a third-party request is structurally impossible -- a mistake the
//     reference client makes.
//
// There is no host allowlist, for the reason avatar.go already gives for the
// same class of input: Google can move its preview CDN at any time, so an
// allowlist silently drops legitimate media, while blocking internal
// destinations removes the actual impact.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

const (
	// maxExternalRedirects caps manual redirect-following. Preview CDNs
	// redirect once or twice; five is slack without being a loop.
	maxExternalRedirects = 5

	// externalConnectTimeout bounds the TCP dial and TLS handshake, so a
	// black-holed host fails fast rather than consuming the whole budget.
	externalConnectTimeout = 10 * time.Second
)

// externalRequestTimeout bounds an ENTIRE external fetch, redirect chain
// included. Shorter than avatarRequestTimeout because this runs on the message
// conversion path, including backfill: a chat being backfilled issues one
// fetch per media message, so a slow host must not stall the batch for long.
// A var so tests can shorten it.
var externalRequestTimeout = 15 * time.Second

var errTooManyExternalRedirects = errors.New("googlechat: too many redirects fetching external media")

// externalHTTPClient is the hardened client described in this file's doc
// comment. A var so tests can substitute one; note that substituting it also
// replaces the dial-time address check, which is why one test drives the
// PRODUCTION client at a loopback listener to prove the hook is really wired.
var externalHTTPClient = newExternalHTTPClient()

func newExternalHTTPClient() *http.Client {
	return &http.Client{
		Timeout: externalRequestTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Transport: &http.Transport{
			// Proxy deliberately NOT set. See point 3 above, and the longer
			// argument on newAvatarHTTPClient.
			DialContext:         newGuardedDialer(externalConnectTimeout).DialContext,
			TLSHandshakeTimeout: externalConnectTimeout,
		},
	}
}

// DownloadExternalMedia fetches media from a remote-party-chosen URL, applying
// the policy in this file's doc comment. It returns the bytes, the server's
// Content-Type, and a filename derived from Content-Disposition or the URL
// path.
//
// maxSize caps the body (0 means unlimited, matching readBodyWithMaxSize);
// exceeding it surfaces ErrFileTooLarge, shared with the attachment path so
// callers can treat oversize uniformly.
func DownloadExternalMedia(ctx context.Context, rawURL string, maxSize int64) (data []byte, mimeType, filename string, err error) {
	// One deadline for the whole chain: http.Client.Timeout is enforced per
	// Do() call, so on its own it would hand every hop a fresh budget.
	ctx, cancel := context.WithTimeout(ctx, externalRequestTimeout)
	defer cancel()

	// Copy the installed client so the manual redirect policy holds even if
	// the installed one would follow redirects itself -- the per-hop scheme
	// check is only meaningful if this loop does the following.
	client := *externalHTTPClient
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}

	next := rawURL
	for hop := 0; ; hop++ {
		if hop > maxExternalRedirects {
			return nil, "", "", fmt.Errorf("%w (%d)", errTooManyExternalRedirects, maxExternalRedirects)
		}

		u, err := url.Parse(next)
		if err != nil {
			return nil, "", "", fmt.Errorf("googlechat: invalid external media url: %w", err)
		}
		if u.Scheme != "https" {
			return nil, "", "", fmt.Errorf("googlechat: refusing to fetch external media over %q (https required): %s", u.Scheme, u.Redacted())
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return nil, "", "", fmt.Errorf("googlechat: invalid external media url: %w", err)
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, "", "", fmt.Errorf("googlechat: external media download failed: %w", err)
		}

		if isRedirectStatus(resp.StatusCode) {
			loc := resp.Header.Get("Location")
			// Discarded unread: attacker-supplied and uncapped, so it must
			// never be buffered.
			resp.Body.Close()
			if loc == "" {
				return nil, "", "", fmt.Errorf("googlechat: external media redirect response missing Location header")
			}
			nextURL, err := u.Parse(loc)
			if err != nil {
				return nil, "", "", fmt.Errorf("googlechat: invalid external media redirect location %q: %w", loc, err)
			}
			next = nextURL.String()
			continue
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, "", "", fmt.Errorf("googlechat: external media download failed: unexpected status %s", resp.Status)
		}

		data, err := readBodyWithMaxSize(resp, maxSize, "external media")
		if err != nil {
			resp.Body.Close()
			return nil, "", "", err
		}
		mimeType = resp.Header.Get("Content-Type")
		filename = attachmentFilename(resp, u)
		resp.Body.Close()
		return data, mimeType, filename, nil
	}
}
