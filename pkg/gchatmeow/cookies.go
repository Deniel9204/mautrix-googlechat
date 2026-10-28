package gchatmeow

import (
	"net/http"
	"sort"
	"strings"
	"sync"
)

// RequiredCookies are the cookies the login UI must collect (uppercased).
var RequiredCookies = []string{"COMPASS", "SSID", "SID", "OSID", "HSID"}

// CookieIsDomainSpecific reports whether the login UI must extract the chat.google.com copy of
// a cookie; SSID/SID/HSID live only on .google.com and must NOT be pinned.
func CookieIsDomainSpecific(cookieName string) bool {
	return cookieName == "COMPASS" || cookieName == "OSID"
}

// cookieJar is a flat, name-keyed store with no domain scoping (host gating is in Session).
// It never quotes values, which Google rejects, so it is not wired into http.Client.Jar.
type cookieJar struct {
	mu      sync.RWMutex
	cookies map[string]string // name (as stored) -> value
}

func newCookieJar() *cookieJar {
	return &cookieJar{cookies: make(map[string]string)}
}

func (j *cookieJar) set(name, value string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.cookies[name] = value
}

// absorb ingests every Set-Cookie, capturing rotation.
func (j *cookieJar) absorb(resp *http.Response) {
	for _, c := range resp.Cookies() {
		j.set(c.Name, c.Value)
	}
}

// header builds the unquoted, name-sorted Cookie header value.
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

// snapshotAll returns a copy of the entire jar, including rotation extras.
func (j *cookieJar) snapshotAll() map[string]string {
	j.mu.RLock()
	defer j.mu.RUnlock()
	out := make(map[string]string, len(j.cookies))
	for name, v := range j.cookies {
		out[name] = v
	}
	return out
}
