package gchatmeow

// Ghost/portal avatar image download. Avatar fetches use a FRESH, cookie-less
// HTTP client rather than the authenticated Google Chat session -- avatar URLs
// (typically lh3.googleusercontent.com) are unauthenticated CDN links, and
// are not covered by Session's allowedHostSuffixes gate (session.go), which
// is scoped to chat.google.com's /api endpoints. Hence a plain, standalone
// HTTP GET with no cookies attached, living in this package (rather than
// pkg/connector) so pkg/connector never needs to touch net/http directly --
// every outbound network call stays behind this package's surface, RPCs and
// avatar downloads alike.
//
// sha256 hashing and re-upload-skip-when-unchanged is NOT done here:
// bridgev2.Avatar.Reupload (ghost.go) already does that generically for every
// network connector, given just the raw bytes this file returns.
//
// # Why this path is hardened
//
// avatar_url is remote, untrusted input: it comes straight off the wire in
// the User proto, and a Google Chat app/bot supplies its own icon URL. An
// earlier revision fetched it with http.DefaultClient and io.ReadAll, which
// meant (a) redirects were auto-followed to ANY host and scheme, so a
// crafted avatar_url could steer the bridge at the operator's own network
// (RFC1918, 127.0.0.1, or the 169.254.169.254 cloud-metadata endpoint) and
// have the response re-uploaded into Matrix as a ghost avatar, and (b) the
// response body was buffered with no cap, so a single oversized/endless
// response was an out-of-memory vector. Four things close that:
//
//  1. https is required on the initial URL AND on every redirect hop, so an
//     https->http downgrade toward http-only cloud metadata is refused.
//  2. Redirects are followed MANUALLY (never by the http.Client), capped, so
//     each hop is re-validated instead of trusted.
//  3. The dialer's Control hook rejects any connection whose RESOLVED
//     address is loopback/link-local/private/CGNAT/multicast/unspecified.
//     Checking the dialed address rather than the hostname is what also
//     defeats DNS rebinding, where a public name resolves to an internal IP.
//  4. The body is size-capped (maxAvatarSize) via the same Content-Length
//     fast-reject plus LimitReader read the attachment path uses.
//
// Deliberately NOT done: a Google-CDN host allowlist. Google Chat apps can
// register their own icon URLs, so an allowlist risks silently dropping
// legitimate bot avatars; blocking internal destinations removes the actual
// impact (reaching the operator's network) without that breakage.
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
	// maxAvatarSize caps a downloaded avatar body at 5 MiB. Real Google
	// avatars are a few hundred KB at most, so this is generous headroom
	// while still bounding what a hostile avatar host can make the bridge
	// allocate. Deliberately a constant rather than a config knob: there is
	// no legitimate reason for an operator to need a different value.
	maxAvatarSize = 5 << 20

	// maxAvatarRedirects caps manual redirect-following. Avatar CDN links
	// redirect zero or one times in practice; 5 is slack for Google adding a
	// hop without being a useful chain for an attacker to walk.
	maxAvatarRedirects = 5

	// avatarConnectTimeout bounds the TCP dial and TLS handshake separately,
	// mirroring session.go's apiConnectTimeout split.
	avatarConnectTimeout = 15 * time.Second
)

// avatarRequestTimeout bounds an ENTIRE avatar fetch, redirect chain
// included. It is applied twice over: as http.Client.Timeout (which net/http
// enforces per Do() call, i.e. per hop) and as a context deadline spanning
// DownloadAvatar's whole loop -- without the latter, each of the
// maxAvatarRedirects+1 hops would get its own fresh budget and a chain of
// slow redirects could hold the fetch open for a multiple of this value.
// http.DefaultClient, used previously, had NO timeout at all.
//
// A var rather than a const purely so tests can shrink it.
var avatarRequestTimeout = 30 * time.Second

var (
	// errBlockedAvatarAddress is returned (wrapped in the dial error) when an
	// avatar URL resolves to an address the bridge must never connect to.
	errBlockedAvatarAddress = errors.New("googlechat: avatar address is not permitted")

	// errTooManyAvatarRedirects is returned when the redirect chain exceeds
	// maxAvatarRedirects.
	errTooManyAvatarRedirects = errors.New("googlechat: avatar download exceeded redirect limit")
)

// avatarHTTPClient is used by DownloadAvatar. Overridden in tests (this
// package's own _test.go files can reach the unexported var) so avatar
// download exercises real HTTP round-trips against an httptest server
// without needing a live network -- mirrors this codebase's other
// test-seam vars (api.go's baseURL, client.go's sleepFn).
//
// A test that swaps this out necessarily swaps out the dial-time address
// check with it (that check lives in the Transport), which is why the
// address policy is ALSO unit-tested directly via isDisallowedIP, and why
// one test drives this production client at a loopback listener to prove
// the hook is actually wired up.
var avatarHTTPClient = newAvatarHTTPClient()

// newAvatarHTTPClient builds the hardened client described in this file's
// doc comment: bounded in time, never following redirects on its own, and
// refusing to connect to internal addresses.
func newAvatarHTTPClient() *http.Client {
	dialer := &net.Dialer{
		Timeout: avatarConnectTimeout,
		// Control runs after name resolution with the concrete address about
		// to be dialed, which is precisely why it -- rather than a check on
		// the URL's hostname -- is the right place for this: a hostname that
		// resolves to an internal IP (DNS rebinding) is caught here too.
		//
		Control: func(_, address string, _ syscall.RawConn) error {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return fmt.Errorf("%w: unparseable dial address %q", errBlockedAvatarAddress, address)
			}
			ip := net.ParseIP(host)
			if ip == nil {
				return fmt.Errorf("%w: unresolvable dial address %q", errBlockedAvatarAddress, host)
			}
			if isDisallowedIP(ip) {
				return fmt.Errorf("%w: %s", errBlockedAvatarAddress, ip)
			}
			return nil
		},
	}
	return &http.Client{
		Timeout: avatarRequestTimeout,
		// Automatic redirect-following is disabled so DownloadAvatar's own
		// loop re-validates every hop's scheme. DownloadAvatar re-asserts
		// this per call, so the guarantee does not depend on this field
		// surviving a test override.
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Transport: &http.Transport{
			// Proxy is deliberately NOT set (unlike session.go's transport,
			// and unlike the http.DefaultClient this replaced). With
			// $HTTPS_PROXY configured, net/http dials the PROXY and asks it to
			// CONNECT to the real target, so the Control hook below would only
			// ever validate the proxy's own address -- silently reducing this
			// entire defence to a no-op for the RFC1918/link-local ranges it
			// exists to block (Go only bypasses a proxy for literal loopback
			// targets by default). Failing closed is the right trade for a
			// cosmetic feature: an operator whose egress REQUIRES a proxy
			// loses ghost avatars, rather than unknowingly losing the
			// protection.
			DialContext:         dialer.DialContext,
			TLSHandshakeTimeout: avatarConnectTimeout,
		},
	}
}

// isDisallowedIP reports whether ip is an address the bridge must refuse to
// connect to when fetching remote-supplied content.
//
// IsGlobalUnicast is the broad gate: it is already false for loopback,
// link-local unicast (which covers the 169.254.169.254 metadata endpoint),
// multicast, the unspecified address, and the IPv4 broadcast address. Only
// two categories are routable-looking enough to slip past it and still be
// internal: RFC1918 / RFC4193 unique-local (IsPrivate), and RFC6598 carrier
// -grade NAT space, which IsPrivate does not cover.
//
// Every net.IP predicate used here normalizes IPv4-in-IPv6 internally, so a
// v4-mapped address such as "::ffff:127.0.0.1" -- the usual way to smuggle a
// loopback address past a v4-only check -- is classified on its IPv4 value.
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
	// RFC6598 carrier-grade NAT: 100.64.0.0/10.
	if ip4 := ip.To4(); ip4 != nil && ip4[0] == 100 && ip4[1] >= 64 && ip4[1] <= 127 {
		return true
	}
	// Several IPv6 encodings carry an IPv4 destination that the checks above
	// cannot see, because net.IP classifies them on their IPv6 form. Judge
	// them on the address they actually reach.
	if embedded := embeddedIPv4(ip); embedded != nil {
		return isDisallowedIP(embedded)
	}
	return false
}

// embeddedIPv4 returns the IPv4 address an IPv6 address carries, for the
// encodings that route to it, or nil when there is none.
//
// net.IP only understands the IPv4-MAPPED form (::ffff:a.b.c.d) as IPv4;
// every other v4-in-v6 encoding is classified purely on its IPv6 bits and so
// reads as an ordinary global-unicast, non-private address. Three of them
// reach an IPv4 destination:
//
//   - 64:ff9b::/96, the RFC6052 well-known NAT64 prefix. This is the one that
//     matters in practice: on a network with DNS64 (common for IPv6-only
//     egress), an ordinary hostname whose only A record is internal is
//     synthesised into a 64:ff9b::<v4> AAAA answer and the NAT64 gateway
//     translates it back -- no attacker DNS control needed.
//   - 2002::/16, 6to4, which embeds its IPv4 in the next 32 bits.
//   - ::a.b.c.d, the deprecated IPv4-compatible form.
//
// Callers must classify non-global-unicast addresses (::1, ::) BEFORE
// consulting this, since those also have 96 zero bits and would otherwise be
// mistaken for the IPv4-compatible form.
func embeddedIPv4(ip net.IP) net.IP {
	if ip.To4() != nil {
		// Already IPv4 (or the v4-mapped form net.IP handles natively);
		// nothing to unwrap, and this is what terminates the recursion above.
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

// ForceHTTPS rewrites rawURL's scheme to https, applied before every avatar
// download regardless of what scheme the server-supplied avatar_url used.
// Malformed URLs are returned unchanged rather than erroring here --
// DownloadAvatar's own request construction still surfaces a clear error for
// anything genuinely unusable as a URL.
//
// This is a convenience for the common case (an http:// avatar_url that is
// perfectly fetchable over https); it is NOT the security boundary.
// DownloadAvatar independently REQUIRES https on the URL it is handed and on
// every redirect hop, so a caller that forgets this helper gets a clean
// rejection rather than a plaintext fetch.
func ForceHTTPS(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	u.Scheme = "https"
	return u.String()
}

// DownloadAvatar fetches raw avatar image bytes from avatarURL via a plain,
// unauthenticated GET (see this file's doc comment for why this bypasses
// Session, and for the hardening applied to this untrusted URL).
//
// Redirects are followed manually, up to maxAvatarRedirects, so that every
// hop is re-checked for scheme; the dialer independently refuses internal
// addresses on every hop. The body is capped at maxAvatarSize, surfacing
// ErrFileTooLarge (shared with the attachment path) when exceeded.
func DownloadAvatar(ctx context.Context, avatarURL string) ([]byte, error) {
	// One deadline for the entire chain. http.Client.Timeout is enforced per
	// Do() call, so on its own it would hand every redirect hop a fresh
	// budget; this is what actually bounds the whole fetch.
	ctx, cancel := context.WithTimeout(ctx, avatarRequestTimeout)
	defer cancel()

	// Copy the installed client so this function's redirect policy holds
	// even if the client it was handed would follow redirects itself --
	// the per-hop scheme check below is only meaningful if this loop is
	// the thing doing the following.
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
			// The redirect body is discarded unread: it is attacker-supplied
			// and uncapped, so it must never be buffered.
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
