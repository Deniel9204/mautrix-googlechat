package gchatmeow

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

const (
	maxExternalRedirects = 5

	externalConnectTimeout = 10 * time.Second

	// maxExternalMediaSize is a ceiling the caller cannot disable: the homeserver cap can be 0
	// ("unlimited") if bridgev2's one-shot media_config fetch failed.
	maxExternalMediaSize = 100 << 20
)

// externalRequestTimeout bounds the whole redirect chain; short because backfill fetches per message. A var for tests.
var externalRequestTimeout = 15 * time.Second

var errTooManyExternalRedirects = errors.New("googlechat: too many redirects fetching external media")

// externalHTTPClient is a test seam; substituting it also replaces the dial-time address check.
var externalHTTPClient = newExternalHTTPClient()

func newExternalHTTPClient() *http.Client {
	return &http.Client{
		Timeout: externalRequestTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Transport: &http.Transport{
			// Proxy deliberately NOT set; see newAvatarHTTPClient.
			DialContext:         newGuardedDialer(externalConnectTimeout).DialContext,
			TLSHandshakeTimeout: externalConnectTimeout,
		},
	}
}

// DownloadExternalMedia fetches a remote-party-chosen URL with avatar.go's policy (https every hop,
// no internal addresses, no proxy, no credentials). maxSize is clamped to maxExternalMediaSize; 0 is not unlimited.
func DownloadExternalMedia(ctx context.Context, rawURL string, maxSize int64) (data []byte, mimeType, filename string, err error) {
	ctx, cancel := context.WithTimeout(ctx, externalRequestTimeout)
	defer cancel()

	if maxSize <= 0 || maxSize > maxExternalMediaSize {
		maxSize = maxExternalMediaSize
	}

	// Force manual redirects so the per-hop scheme check holds.
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
			// Discard the attacker-supplied, uncapped redirect body unread.
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
