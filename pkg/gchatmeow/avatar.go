package gchatmeow

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"syscall"
	"time"
)

const (
	// maxAvatarSize caps a downloaded avatar body at 5 MiB.
	maxAvatarSize = 5 << 20

	maxAvatarRedirects = 5

	avatarConnectTimeout = 15 * time.Second
)

// avatarRequestTimeout bounds the whole redirect chain (via ctx) as well as each hop; a var for tests.
var avatarRequestTimeout = 30 * time.Second

var (
	errBlockedAddress = errors.New("googlechat: avatar address is not permitted")

	errTooManyAvatarRedirects = errors.New("googlechat: avatar download exceeded redirect limit")
)

// avatarHTTPClient is a test seam; swapping it also swaps out the dial-time address check.
var avatarHTTPClient = newAvatarHTTPClient()

// newGuardedDialer refuses to connect to any resolved address isDisallowedIP rejects, which also defeats DNS rebinding.
func newGuardedDialer(connectTimeout time.Duration) *net.Dialer {
	return &net.Dialer{
		Timeout: connectTimeout,
		Control: func(_, address string, _ syscall.RawConn) error {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return fmt.Errorf("%w: unparseable dial address %q", errBlockedAddress, address)
			}
			ip := net.ParseIP(host)
			if ip == nil {
				return fmt.Errorf("%w: unresolvable dial address %q", errBlockedAddress, host)
			}
			if isDisallowedIP(ip) {
				return fmt.Errorf("%w: %s", errBlockedAddress, ip)
			}
			return nil
		},
	}
}

// newAvatarHTTPClient builds a bounded client that never follows redirects and refuses internal addresses.
func newAvatarHTTPClient() *http.Client {
	return &http.Client{
		Timeout: avatarRequestTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Transport: &http.Transport{
			// Proxy is deliberately NOT set: with a proxy the Control hook would only validate the proxy's address.
			DialContext:         newGuardedDialer(avatarConnectTimeout).DialContext,
			TLSHandshakeTimeout: avatarConnectTimeout,
		},
	}
}

// isDisallowedIP reports whether ip is internal (non-global-unicast, private, CGNAT, or embedding such an IPv4).
func isDisallowedIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	if !ip.IsGlobalUnicast() {
		return true
	}
	if ip.IsPrivate() {
		return true
	}
	if ip4 := ip.To4(); ip4 != nil && ip4[0] == 100 && ip4[1] >= 64 && ip4[1] <= 127 {
		return true
	}
	// Some IPv6 encodings carry an IPv4 destination; judge them on that address.
	if embedded := embeddedIPv4(ip); embedded != nil {
		return isDisallowedIP(embedded)
	}
	return false
}

// embeddedIPv4 returns the IPv4 inside a NAT64 (64:ff9b::/96), 6to4 (2002::/16) or IPv4-compatible address, else nil.
// Callers must reject non-global-unicast (::1, ::) first, or they would match the IPv4-compatible form.
func embeddedIPv4(ip net.IP) net.IP {
	if ip.To4() != nil {
		// Already IPv4; terminates the recursion.
		return nil
	}
	ip16 := ip.To16()
	if ip16 == nil {
		return nil
	}
	switch {
	case ip16[0] == 0x20 && ip16[1] == 0x02: // 2002::/16, 6to4
		return net.IPv4(ip16[2], ip16[3], ip16[4], ip16[5])
	case ip16[0] == 0x00 && ip16[1] == 0x64 && ip16[2] == 0xff && ip16[3] == 0x9b: // 64:ff9b::/96, NAT64
		return net.IPv4(ip16[12], ip16[13], ip16[14], ip16[15])
	}
	for _, b := range ip16[:12] { // ::a.b.c.d, IPv4-compatible
		if b != 0 {
			return nil
		}
	}
	return net.IPv4(ip16[12], ip16[13], ip16[14], ip16[15])
}

// ForceHTTPS rewrites rawURL's scheme to https; malformed URLs are returned unchanged. Not the security boundary.
func ForceHTTPS(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	u.Scheme = "https"
	return u.String()
}

// DownloadAvatar fetches an untrusted avatar URL cookie-less, requiring https on every manually followed hop,
// refusing internal addresses, and capping the body at maxAvatarSize.
func DownloadAvatar(ctx context.Context, avatarURL string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, avatarRequestTimeout)
	defer cancel()

	// Force manual redirects even if the installed client would follow them.
	client := *avatarHTTPClient
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}

	next := avatarURL
	for hop := 0; ; hop++ {
		if hop > maxAvatarRedirects {
			return nil, fmt.Errorf("%w (%d)", errTooManyAvatarRedirects, maxAvatarRedirects)
		}

		u, err := url.Parse(next)
		if err != nil {
			return nil, fmt.Errorf("googlechat: invalid avatar url: %w", err)
		}
		if u.Scheme != "https" {
			return nil, fmt.Errorf("googlechat: refusing to fetch avatar over %q (https required): %s", u.Scheme, u.Redacted())
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return nil, fmt.Errorf("googlechat: invalid avatar url: %w", err)
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("googlechat: avatar download failed: %w", err)
		}

		if isRedirectStatus(resp.StatusCode) {
			loc := resp.Header.Get("Location")
			// Discard the attacker-supplied, uncapped redirect body unread.
			resp.Body.Close()
			if loc == "" {
				return nil, fmt.Errorf("googlechat: avatar redirect response missing Location header")
			}
			nextURL, err := u.Parse(loc)
			if err != nil {
				return nil, fmt.Errorf("googlechat: invalid avatar redirect location %q: %w", loc, err)
			}
			next = nextURL.String()
			continue
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("googlechat: avatar download failed: unexpected status %s", resp.Status)
		}

		data, err := readBodyWithMaxSize(resp, maxAvatarSize, "avatar")
		resp.Body.Close()
		if err != nil {
			return nil, err
		}
		return data, nil
	}
}
