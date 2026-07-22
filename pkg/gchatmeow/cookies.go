package gchatmeow

import (
	"net/http"
	"sort"
	"strings"
	"sync"
)

// RequiredCookies are the cookies the login UI must collect, on domain
// chat.google.com: compass/ssid/sid/osid/hsid, installed uppercased as
// COMPASS/SSID/SID/OSID/HSID.
var RequiredCookies = []string{"COMPASS", "SSID", "SID", "OSID", "HSID"}

// CookieIsDomainSpecific reports whether a required cookie's NAME is also used
// on OTHER Google subdomains, so the browser-side login UI must be told which
// domain's copy to extract. Only COMPASS and OSID collide this way; SSID, SID
// and HSID exist only under the parent .google.com and must NOT be pinned to
// chat.google.com (hinting the wrong domain would make the extension miss or
// grab the wrong value). Copied verbatim from the cleared reference
// googlechat-megabridge/pkg/gchatmeow/cookies.go's CookieIsDomainSpecific.
// This is a login-UI extraction hint only -- it is unrelated to the
// outbound cookie JAR, which installs all 5 flat under chat.google.com
// (see cookieJar's doc comment).
func CookieIsDomainSpecific(cookieName string) bool {
	return cookieName == "COMPASS" || cookieName == "OSID"
}

// cookieJar is a minimal, flat, name-keyed cookie store.
//
// Unlike net/http/cookiejar.Jar, it does NOT do RFC 6265 domain/path
// scoping per cookie: the bridge only ever talks to Google's *.google.com
// family, and a flat store is all this port needs -- all 5 auth cookies are
// installed on the single domain chat.google.com and read back purely by
// name. Host-based access control lives one level up, in
// Session.allowedHostSuffixes -- it gates whether the jar's cookies are
// attached to / absorbed from a given request at all, not which subset of
// cookies apply to which sub-path.
//
// It also never quotes values when serializing the Cookie header. Go's
// net/http quotes a cookie value that contains a space or comma
// (net/http's sanitizeCookieValue) to keep the header RFC-syntactically
// valid, but Google's server does not accept that -- it wants the raw
// value verbatim. To guarantee this, the jar is
// never wired into http.Client.Jar: it is not an http.CookieJar at all,
// and the Cookie header is built by hand in Session.buildRequest.
type cookieJar struct {
	mu      sync.RWMutex
	cookies map[string]string // name (as stored) -> value
}

func newCookieJar() *cookieJar {
	return &cookieJar{cookies: make(map[string]string)}
}

// set stores or rotates a single cookie value.
func (j *cookieJar) set(name, value string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.cookies[name] = value
}

// absorb ingests every Set-Cookie the response carried, capturing rotation
// (e.g. Google issuing a new SID after login: Google rotates cookies over
// time, so the jar picks up any Set-Cookie responses).
func (j *cookieJar) absorb(resp *http.Response) {
	for _, c := range resp.Cookies() {
		j.set(c.Name, c.Value)
	}
}

// header builds the raw, UNQUOTED "Cookie" request-header value from the
// current cookie set: "name=value" pairs joined by "; ", sorted by name
// for deterministic output. Never quotes a value, regardless of its
// contents (see type doc comment).
func (j *cookieJar) header() string {
	j.mu.RLock()
	defer j.mu.RUnlock()
	if len(j.cookies) == 0 {
		return ""
	}
	names := make([]string, 0, len(j.cookies))
	for name := range j.cookies {
		names = append(names, name)
	}
	sort.Strings(names)
	pairs := make([]string, len(names))
	for i, name := range names {
		pairs[i] = name + "=" + j.cookies[name]
	}
	return strings.Join(pairs, "; ")
}

// snapshot returns the current value of each name in names that is present
// in the jar; missing ones are simply omitted from the result. Used by
// Session.Cookies() to read back RequiredCookies post-rotation.
func (j *cookieJar) snapshot(names []string) map[string]string {
	j.mu.RLock()
	defer j.mu.RUnlock()
	out := make(map[string]string, len(names))
	for _, name := range names {
		if v, ok := j.cookies[name]; ok {
			out[name] = v
		}
	}
	return out
}
