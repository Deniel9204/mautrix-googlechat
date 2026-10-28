package gchatmeow

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testServerHost(t *testing.T, rawURL string) string {
	t.Helper()
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("url.Parse(%q): %v", rawURL, err)
	}
	return u.Hostname()
}

// newLoopbackServer binds a distinct loopback address so two servers differ by host.
func newLoopbackServer(t *testing.T, ip string, handler http.Handler) *httptest.Server {
	t.Helper()
	ln, err := net.Listen("tcp", ip+":0")
	if err != nil {
		t.Skipf("cannot bind %s:0 (sandboxed network?): %v", ip, err)
	}
	srv := httptest.NewUnstartedServer(handler)
	srv.Listener.Close()
	srv.Listener = ln
	srv.Start()
	return srv
}

// TestCookiesSentUnquoted: Go's cookie jar would quote values with spaces/commas, which Google rejects.
func TestCookiesSentUnquoted(t *testing.T) {
	var gotCookie string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCookie = r.Header.Get("Cookie")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sess, err := NewSession(map[string]string{"SID": "has space,and,commas"}, "")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	sess.allowedHostSuffixes = []string{testServerHost(t, srv.URL)}

	resp, err := sess.Fetch(context.Background(), http.MethodGet, srv.URL, nil, nil)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("StatusCode = %d, want 200", resp.StatusCode)
	}

	const want = "SID=has space,and,commas"
	if !strings.Contains(gotCookie, want) {
		t.Errorf("Cookie header = %q, want it to contain unquoted %q", gotCookie, want)
	}
	if strings.Contains(gotCookie, `"`) {
		t.Errorf("Cookie header = %q, must not contain quotes", gotCookie)
	}
}

func TestCookieRotationReadback(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "SID", Value: "rotated2", Path: "/"})
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sess, err := NewSession(map[string]string{
		"COMPASS": "c0", "SSID": "s0", "SID": "original", "OSID": "o0", "HSID": "h0",
	}, "")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	sess.allowedHostSuffixes = []string{testServerHost(t, srv.URL)}

	if _, err := sess.Fetch(context.Background(), http.MethodGet, srv.URL, nil, nil); err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	got := sess.Cookies()
	if got["SID"] != "rotated2" {
		t.Errorf(`Cookies()["SID"] = %q, want "rotated2"`, got["SID"])
	}
	if got["COMPASS"] != "c0" {
		t.Errorf(`Cookies()["COMPASS"] = %q, want "c0"`, got["COMPASS"])
	}
}

func TestCookiesNotSentCrossDomain(t *testing.T) {
	var allowedCookie, outsideCookie string
	var allowedSeen, outsideSeen bool

	allowedSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		allowedCookie = r.Header.Get("Cookie")
		allowedSeen = true
		w.WriteHeader(http.StatusOK)
	}))
	defer allowedSrv.Close()

	outsideSrv := newLoopbackServer(t, "127.0.0.2", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		outsideCookie = r.Header.Get("Cookie")
		outsideSeen = true
		w.WriteHeader(http.StatusOK)
	}))
	defer outsideSrv.Close()

	sess, err := NewSession(map[string]string{"SID": "secret"}, "")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	sess.allowedHostSuffixes = []string{testServerHost(t, allowedSrv.URL)}

	if _, err := sess.Fetch(context.Background(), http.MethodGet, allowedSrv.URL, nil, nil); err != nil {
		t.Fatalf("Fetch(allowed): %v", err)
	}
	if _, err := sess.Fetch(context.Background(), http.MethodGet, outsideSrv.URL, nil, nil); err != nil {
		t.Fatalf("Fetch(outside): %v", err)
	}

	if !allowedSeen || !strings.Contains(allowedCookie, "SID=secret") {
		t.Errorf("allowlisted server Cookie = %q, want it to contain SID=secret", allowedCookie)
	}
	if !outsideSeen {
		t.Fatal("non-allowlisted server was never hit")
	}
	if outsideCookie != "" {
		t.Errorf("non-allowlisted server Cookie = %q, want empty", outsideCookie)
	}
}

func TestFetchRetries(t *testing.T) {
	var mu sync.Mutex
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		n := requests
		mu.Unlock()
		if n < 3 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))
	defer srv.Close()

	sess, err := NewSession(nil, "")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	sess.allowedHostSuffixes = []string{testServerHost(t, srv.URL)}

	resp, err := sess.Fetch(context.Background(), http.MethodGet, srv.URL, nil, nil)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if resp.StatusCode != http.StatusOK || string(resp.Body) != "ok" {
		t.Errorf("resp = %+v, want 200 body \"ok\"", resp)
	}

	mu.Lock()
	defer mu.Unlock()
	if requests != 3 {
		t.Errorf("requests = %d, want 3", requests)
	}
}

func TestFetchRetriesExhaustedReturnsNetworkError(t *testing.T) {
	var requests int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	sess, err := NewSession(nil, "")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	sess.allowedHostSuffixes = []string{testServerHost(t, srv.URL)}

	_, err = sess.Fetch(context.Background(), http.MethodGet, srv.URL, nil, nil)
	var netErr *NetworkError
	if !errors.As(err, &netErr) {
		t.Fatalf("err = %v (%T), want *NetworkError", err, err)
	}
	if got := atomic.LoadInt32(&requests); got != maxRetries {
		t.Errorf("requests = %d, want %d", got, maxRetries)
	}
	var use *UnexpectedStatusError
	if !errors.As(err, &use) {
		t.Fatalf("err = %v, want it to unwrap to an *UnexpectedStatusError", err)
	}
	if use.Status != http.StatusBadGateway {
		t.Errorf("unwrapped Status = %d, want %d", use.Status, http.StatusBadGateway)
	}
}

func TestFetchNonRetryableStatusReturnsUnexpectedStatusError(t *testing.T) {
	var requests int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"error":"invalid_grant"}`))
	}))
	defer srv.Close()

	sess, err := NewSession(nil, "")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	sess.allowedHostSuffixes = []string{testServerHost(t, srv.URL)}

	_, err = sess.Fetch(context.Background(), http.MethodGet, srv.URL, nil, nil)
	var use *UnexpectedStatusError
	if !errors.As(err, &use) {
		t.Fatalf("err = %v (%T), want *UnexpectedStatusError", err, err)
	}
	if use.Status != http.StatusUnauthorized {
		t.Errorf("Status = %d, want 401", use.Status)
	}
	if use.ErrorCode != "invalid_grant" {
		t.Errorf("ErrorCode = %q, want invalid_grant", use.ErrorCode)
	}
	if got := atomic.LoadInt32(&requests); got != 1 {
		t.Errorf("requests = %d, want 1 (401 must not be retried)", got)
	}
}

func TestConnectionKeepAliveForced(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("Connection")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sess, err := NewSession(nil, "")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	sess.allowedHostSuffixes = []string{testServerHost(t, srv.URL)}

	hdr := http.Header{"Connection": []string{"close"}}
	if _, err := sess.Fetch(context.Background(), http.MethodGet, srv.URL, hdr, nil); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if got != "Keep-Alive" {
		t.Errorf("Connection header = %q, want %q", got, "Keep-Alive")
	}
}

func TestTLSVerificationEnabled(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sess, err := NewSession(nil, "")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	sess.allowedHostSuffixes = []string{testServerHost(t, srv.URL)}

	_, err = sess.Fetch(context.Background(), http.MethodGet, srv.URL, nil, nil)
	if err == nil {
		t.Fatal("Fetch against an untrusted TLS server unexpectedly succeeded -- TLS verification appears disabled")
	}
	msg := strings.ToLower(err.Error())
	if !strings.Contains(msg, "certificate") && !strings.Contains(msg, "x509") && !strings.Contains(msg, "tls") {
		t.Errorf("error = %q, want it to mention a certificate/x509/tls failure", err.Error())
	}
}

// TestNoClientTimeout: a client Timeout would kill every long poll after a fixed duration.
func TestNoClientTimeout(t *testing.T) {
	sess, err := NewSession(nil, "")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if sess.pollClient.Timeout != 0 {
		t.Errorf("pollClient.Timeout = %v, want 0", sess.pollClient.Timeout)
	}
}

func TestFetchRawReturnsRawResponseNoRetry(t *testing.T) {
	var requests int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	sess, err := NewSession(nil, "")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	sess.allowedHostSuffixes = []string{testServerHost(t, srv.URL)}

	resp, err := sess.FetchRaw(context.Background(), http.MethodGet, srv.URL, nil, nil)
	if err != nil {
		t.Fatalf("FetchRaw: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("StatusCode = %d, want 502 (FetchRaw returns the raw response verbatim)", resp.StatusCode)
	}
	if got := atomic.LoadInt32(&requests); got != 1 {
		t.Errorf("requests = %d, want 1 (FetchRaw must not retry)", got)
	}
}

func TestUserAgentVersionRewrite(t *testing.T) {
	sess, err := NewSession(nil, "Mozilla/5.0 Chrome/100.0.4321.10 Safari/537.36")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if !strings.Contains(sess.userAgent, "Chrome/"+latestChromeVersion+".0.0.0") {
		t.Errorf("userAgent = %q, want Chrome version rewritten to %s.0.0.0", sess.userAgent, latestChromeVersion)
	}
	if strings.Contains(sess.userAgent, "Chrome/100.") {
		t.Errorf("userAgent = %q still carries the caller's Chrome version", sess.userAgent)
	}
}

func TestDefaultUserAgent(t *testing.T) {
	sess, err := NewSession(nil, "")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if sess.userAgent != DefaultUserAgent() || !strings.Contains(sess.userAgent, "Windows NT 10.0") {
		t.Errorf("userAgent = %q, want the default Windows/Chrome UA", sess.userAgent)
	}
}

func TestRequiredCookies(t *testing.T) {
	want := []string{"COMPASS", "SSID", "SID", "OSID", "HSID"}
	if len(RequiredCookies) != len(want) {
		t.Fatalf("RequiredCookies = %v, want %v", RequiredCookies, want)
	}
	for i, name := range want {
		if RequiredCookies[i] != name {
			t.Errorf("RequiredCookies[%d] = %q, want %q", i, RequiredCookies[i], name)
		}
	}
}

// TestDefaultAllowedHostSuffixes pins the real default; other tests override it.
func TestDefaultAllowedHostSuffixes(t *testing.T) {
	sess, err := NewSession(nil, "")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	want := []string{"google.com"}
	if len(sess.allowedHostSuffixes) != len(want) {
		t.Fatalf("allowedHostSuffixes = %v, want %v", sess.allowedHostSuffixes, want)
	}
	for i, suffix := range want {
		if sess.allowedHostSuffixes[i] != suffix {
			t.Errorf("allowedHostSuffixes[%d] = %q, want %q", i, sess.allowedHostSuffixes[i], suffix)
		}
	}
	for _, host := range []string{"chat.google.com", "accounts.google.com", "google.com"} {
		u, _ := url.Parse("https://" + host + "/")
		if !sess.hostAllowed(u) {
			t.Errorf("hostAllowed(%q) = false, want true", host)
		}
	}
	for _, host := range []string{"lh3.googleusercontent.com", "googleusercontent.com", "evil.example.com", "notgoogle.com"} {
		u, _ := url.Parse("https://" + host + "/")
		if sess.hostAllowed(u) {
			t.Errorf("hostAllowed(%q) = true, want false", host)
		}
	}
}

// TestRedirectSetCookieNotAbsorbed: absorption is gated on the post-redirect origin.
func TestRedirectSetCookieNotAbsorbed(t *testing.T) {
	var evilCookieHeader string
	var evilHits int32
	evilSrv := newLoopbackServer(t, "127.0.0.2", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&evilHits, 1)
		evilCookieHeader = r.Header.Get("Cookie")
		http.SetCookie(w, &http.Cookie{Name: "SID", Value: "attacker", Path: "/"})
		w.WriteHeader(http.StatusOK)
	}))
	defer evilSrv.Close()

	allowedSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, evilSrv.URL+"/landing", http.StatusFound)
	}))
	defer allowedSrv.Close()

	run := func(t *testing.T, do func(sess *Session) error) {
		sess, err := NewSession(map[string]string{"SID": "secret"}, "")
		if err != nil {
			t.Fatalf("NewSession: %v", err)
		}
		sess.allowedHostSuffixes = []string{testServerHost(t, allowedSrv.URL)}

		if err := do(sess); err != nil {
			t.Fatalf("request: %v", err)
		}
		if atomic.LoadInt32(&evilHits) == 0 {
			t.Fatal("redirect target was never hit")
		}
		if got := sess.Cookies()["SID"]; got != "secret" {
			t.Errorf(`Cookies()["SID"] = %q, want "secret" (Set-Cookie from post-redirect non-allowlisted host must not be absorbed)`, got)
		}
		if evilCookieHeader != "" {
			t.Errorf("redirect target received Cookie = %q, want none", evilCookieHeader)
		}
	}

	t.Run("Fetch", func(t *testing.T) {
		run(t, func(sess *Session) error {
			resp, err := sess.Fetch(context.Background(), http.MethodGet, allowedSrv.URL, nil, nil)
			if err != nil {
				return err
			}
			if resp.StatusCode != http.StatusOK {
				t.Errorf("StatusCode = %d, want 200 (final response after redirect)", resp.StatusCode)
			}
			return nil
		})
	})

	t.Run("FetchRaw", func(t *testing.T) {
		run(t, func(sess *Session) error {
			resp, err := sess.FetchRaw(context.Background(), http.MethodGet, allowedSrv.URL, nil, nil)
			if err != nil {
				return err
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Errorf("StatusCode = %d, want 200 (final response after redirect)", resp.StatusCode)
			}
			return nil
		})
	})
}

// TestGoogleusercontentGetsNoCookies exercises the real default allowlist against googleusercontent.com.
func TestGoogleusercontentGetsNoCookies(t *testing.T) {
	var gotCookie string
	var seen bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCookie = r.Header.Get("Cookie")
		seen = true
		http.SetCookie(w, &http.Cookie{Name: "SID", Value: "poisoned", Path: "/"})
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sess, err := NewSession(map[string]string{"SID": "secret"}, "")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	srvAddr := srv.Listener.Addr().String()
	dialer := &net.Dialer{}
	sess.apiClient.Transport = &http.Transport{
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, network, srvAddr)
		},
	}

	resp, err := sess.Fetch(context.Background(), http.MethodGet, "http://lh3.googleusercontent.com/some/attachment", nil, nil)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("StatusCode = %d, want 200", resp.StatusCode)
	}
	if !seen {
		t.Fatal("test server was never hit")
	}
	if gotCookie != "" {
		t.Errorf("googleusercontent.com request carried Cookie = %q, want none", gotCookie)
	}
	if got := sess.Cookies()["SID"]; got != "secret" {
		t.Errorf(`Cookies()["SID"] = %q, want "secret" (Set-Cookie from a non-allowlisted host must not be absorbed)`, got)
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		in   string
		want time.Duration
	}{
		{"absent", "", 0},
		{"delay seconds", "5", 5 * time.Second},
		{"delay seconds padded", "  7 ", 7 * time.Second},
		{"zero", "0", 0},
		{"negative ignored", "-3", 0},
		{"garbage ignored", "soon", 0},
		{"http date in the future", now.Add(9 * time.Second).Format(http.TimeFormat), 9 * time.Second},
		{"http date in the past", now.Add(-time.Minute).Format(http.TimeFormat), 0},
		{"absurd value capped", "999999", maxRetryAfter},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseRetryAfter(tc.in, now); got != tc.want {
				t.Errorf("parseRetryAfter(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestFetchRetriesOn429(t *testing.T) {
	var mu sync.Mutex
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		n := requests
		mu.Unlock()
		if n < 2 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	sess, err := NewSession(nil, "")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	sess.allowedHostSuffixes = []string{testServerHost(t, srv.URL)}

	resp, err := sess.Fetch(context.Background(), http.MethodGet, srv.URL, nil, nil)
	if err != nil {
		t.Fatalf("Fetch: %v (a 429 must be retried, not surfaced immediately)", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	mu.Lock()
	defer mu.Unlock()
	if requests != 2 {
		t.Errorf("requests = %d, want 2", requests)
	}
}

func TestFetchWaitsForRetryAfter(t *testing.T) {
	var mu sync.Mutex
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		n := requests
		mu.Unlock()
		if n < 2 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sess, err := NewSession(nil, "")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	sess.allowedHostSuffixes = []string{testServerHost(t, srv.URL)}

	start := time.Now()
	if _, err := sess.Fetch(context.Background(), http.MethodGet, srv.URL, nil, nil); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	// Only honouring Retry-After can produce a wait near a second.
	if elapsed := time.Since(start); elapsed < 900*time.Millisecond {
		t.Errorf("retried after %v, want >= ~1s: Retry-After was ignored", elapsed)
	}
}

// TestFetchNonIdempotentDoesNotRetry5xx: retrying a create after a 5xx risks a double send.
func TestFetchNonIdempotentDoesNotRetry5xx(t *testing.T) {
	var requests int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	sess, err := NewSession(nil, "")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	sess.allowedHostSuffixes = []string{testServerHost(t, srv.URL)}

	_, err = sess.FetchNonIdempotent(context.Background(), http.MethodPost, srv.URL, nil, []byte("body"))
	var statusErr *UnexpectedStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("error = %v (%T), want *UnexpectedStatusError", err, err)
	}
	if statusErr.Status != http.StatusInternalServerError {
		t.Errorf("Status = %d, want 500", statusErr.Status)
	}
	if got := atomic.LoadInt32(&requests); got != 1 {
		t.Errorf("requests = %d, want 1: a non-idempotent request must not be retried on 5xx", got)
	}
}

func TestFetchNonIdempotentStillRetries429(t *testing.T) {
	var mu sync.Mutex
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		n := requests
		mu.Unlock()
		if n < 2 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sess, err := NewSession(nil, "")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	sess.allowedHostSuffixes = []string{testServerHost(t, srv.URL)}

	if _, err := sess.FetchNonIdempotent(context.Background(), http.MethodPost, srv.URL, nil, nil); err != nil {
		t.Fatalf("FetchNonIdempotent: %v (429 is a rejection and must still be retried)", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if requests != 2 {
		t.Errorf("requests = %d, want 2", requests)
	}
}

func TestFetchPopulatesUnexpectedStatusErrorBody(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		wantLen  int
		wantShow string
	}{
		{
			name:     "short body is kept whole and rendered",
			body:     "service rejected: invitee not found",
			wantLen:  len("service rejected: invitee not found"),
			wantShow: "invitee not found",
		},
		{
			// maxErrorBodyBytes caps the struct; Error() has its own cap.
			name:    "long body is cut to maxErrorBodyBytes on the struct",
			body:    strings.Repeat("z", 700),
			wantLen: maxErrorBodyBytes,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()

			sess, err := NewSession(nil, "")
			if err != nil {
				t.Fatalf("NewSession: %v", err)
			}
			sess.allowedHostSuffixes = []string{testServerHost(t, srv.URL)}

			_, err = sess.Fetch(context.Background(), http.MethodGet, srv.URL, nil, nil)
			var use *UnexpectedStatusError
			if !errors.As(err, &use) {
				t.Fatalf("err = %v (%T), want *UnexpectedStatusError", err, err)
			}
			if len(use.Body) != tc.wantLen {
				t.Errorf("len(Body) = %d, want %d", len(use.Body), tc.wantLen)
			}
			if tc.wantShow != "" {
				if use.Body != tc.body {
					t.Errorf("Body = %q, want %q", use.Body, tc.body)
				}
				if !strings.Contains(err.Error(), tc.wantShow) {
					t.Errorf("error text %q does not surface the server's reason %q", err, tc.wantShow)
				}
			}
		})
	}
}

func TestCookiesPersistsTheFullJar(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "SIDCC", Value: "issued-after-login", Path: "/"})
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	sess, err := NewSession(map[string]string{
		"COMPASS": "c0", "SSID": "s0", "SID": "sid0", "OSID": "o0", "HSID": "h0",
	}, "")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	sess.allowedHostSuffixes = []string{testServerHost(t, srv.URL)}

	if _, err := sess.Fetch(context.Background(), http.MethodGet, srv.URL, nil, nil); err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	got := sess.Cookies()
	if got["SIDCC"] != "issued-after-login" {
		t.Errorf(`Cookies()["SIDCC"] = %q, want the server-issued extra to be persisted`, got["SIDCC"])
	}
	for _, name := range RequiredCookies {
		if got[name] == "" {
			t.Errorf("Cookies()[%q] is empty; the login five must survive alongside the extras", name)
		}
	}
}
