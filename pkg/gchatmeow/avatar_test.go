package gchatmeow

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

// avatarTLSClient builds a client trusting exactly the given test servers'
// certificates, with a CheckRedirect that FAILS the fetch if it is ever
// consulted. DownloadAvatar overrides CheckRedirect on every call so its own
// manual loop does the following; if that override is ever removed, this trap
// fires and the redirect tests break loudly instead of silently passing on
// the injected client's auto-follow.
func avatarTLSClient(t *testing.T, srvs ...*httptest.Server) *http.Client {
	t.Helper()
	pool := x509.NewCertPool()
	for _, s := range srvs {
		pool.AddCert(s.Certificate())
	}
	return &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}},
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return fmt.Errorf("injected client followed a redirect itself: DownloadAvatar must impose its own redirect policy")
		},
	}
}

// useAvatarClient swaps in c for the duration of the test.
func useAvatarClient(t *testing.T, c *http.Client) {
	t.Helper()
	orig := avatarHTTPClient
	avatarHTTPClient = c
	t.Cleanup(func() { avatarHTTPClient = orig })
}

// --- ForceHTTPS --------------------------------------------------------

func TestForceHTTPS(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"http upgraded", "http://lh3.googleusercontent.com/a/foo", "https://lh3.googleusercontent.com/a/foo"},
		{"already https unchanged", "https://lh3.googleusercontent.com/a/foo", "https://lh3.googleusercontent.com/a/foo"},
		{"scheme-less gets https", "//lh3.googleusercontent.com/a/foo", "https://lh3.googleusercontent.com/a/foo"},
		{"malformed url returned as-is", "://not a url", "://not a url"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ForceHTTPS(tc.in); got != tc.want {
				t.Errorf("ForceHTTPS(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// --- isDisallowedIP ------------------------------------------------------

// TestIsDisallowedIP pins the SSRF blocklist: every range that can reach the
// bridge operator's own host or private network must be refused, and
// ordinary public addresses must not be. The IPv4-mapped IPv6 cases matter
// because "::ffff:127.0.0.1" is the classic way to smuggle a loopback
// address past a naive v4-only check.
func TestIsDisallowedIP(t *testing.T) {
	blocked := []string{
		"127.0.0.1",              // loopback
		"127.1.2.3",              // loopback (whole /8)
		"::1",                    // loopback v6
		"169.254.169.254",        // link-local: cloud metadata
		"169.254.0.1",            // link-local
		"fe80::1",                // link-local v6
		"10.0.0.1",               // RFC1918
		"172.16.0.1",             // RFC1918
		"172.31.255.255",         // RFC1918
		"192.168.1.1",            // RFC1918
		"fc00::1",                // unique-local v6
		"fd12:3456:789a:1::1",    // unique-local v6
		"100.64.0.1",             // CGNAT
		"100.127.255.255",        // CGNAT
		"0.0.0.0",                // unspecified
		"::",                     // unspecified v6
		"224.0.0.1",              // multicast
		"255.255.255.255",        // broadcast
		"::ffff:127.0.0.1",       // v4-mapped loopback
		"::ffff:169.254.169.254", // v4-mapped metadata
		"::ffff:10.0.0.1",        // v4-mapped RFC1918
		"::127.0.0.1",            // IPv4-compatible (deprecated) loopback
		"::169.254.169.254",      // IPv4-compatible metadata
		"::10.0.0.1",             // IPv4-compatible RFC1918
		"64:ff9b::7f00:1",        // NAT64 well-known prefix -> 127.0.0.1
		"64:ff9b::a9fe:a9fe",     // NAT64 -> 169.254.169.254 (metadata via DNS64)
		"64:ff9b::a00:1",         // NAT64 -> 10.0.0.1
		"2002:7f00:0001::",       // 6to4 -> 127.0.0.1
		"2002:a9fe:a9fe::",       // 6to4 -> 169.254.169.254
	}
	for _, s := range blocked {
		t.Run("blocked/"+s, func(t *testing.T) {
			ip := net.ParseIP(s)
			if ip == nil {
				t.Fatalf("test bug: %q is not a valid IP", s)
			}
			if !isDisallowedIP(ip) {
				t.Errorf("isDisallowedIP(%s) = false, want true", s)
			}
		})
	}

	allowed := []string{
		"8.8.8.8",
		"1.1.1.1",
		"142.250.185.174",      // a real googleusercontent-class public address
		"2001:4860:4860::8888", // public v6
		"172.32.0.1",           // just outside RFC1918
		"100.128.0.1",          // just outside CGNAT
		"64:ff9b::808:808",     // NAT64 wrapping a PUBLIC v4 (8.8.8.8) stays allowed
		"2002:0808:0808::",     // 6to4 wrapping a public v4 stays allowed
	}
	for _, s := range allowed {
		t.Run("allowed/"+s, func(t *testing.T) {
			ip := net.ParseIP(s)
			if ip == nil {
				t.Fatalf("test bug: %q is not a valid IP", s)
			}
			if isDisallowedIP(ip) {
				t.Errorf("isDisallowedIP(%s) = true, want false", s)
			}
		})
	}
}

// --- DownloadAvatar: transport hardening ---------------------------------

// TestAvatarHTTPClientIsHardened pins the production client's configuration.
// http.DefaultClient (the previous value) has no timeout and follows
// redirects automatically to any host -- both of which this fix removes.
func TestAvatarHTTPClientIsHardened(t *testing.T) {
	if avatarHTTPClient == http.DefaultClient {
		t.Fatal("avatarHTTPClient is http.DefaultClient: no timeout, auto-follows redirects anywhere")
	}
	if avatarHTTPClient.Timeout <= 0 {
		t.Errorf("avatarHTTPClient.Timeout = %v, want a positive bound", avatarHTTPClient.Timeout)
	}
	if avatarHTTPClient.CheckRedirect == nil {
		t.Error("avatarHTTPClient.CheckRedirect = nil, want automatic redirect-following disabled")
	}

	tr, ok := avatarHTTPClient.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("avatarHTTPClient.Transport = %T, want *http.Transport", avatarHTTPClient.Transport)
	}
	// A forward proxy is dialed INSTEAD of the real target and reaches it via
	// CONNECT, so the dial-time address check would only ever inspect the
	// proxy. Honouring $HTTPS_PROXY here would silently reduce the SSRF
	// defence to a no-op for exactly the ranges it exists to block.
	if tr.Proxy != nil {
		t.Error("avatarHTTPClient Transport.Proxy is set: proxy CONNECT tunnelling bypasses the dial-time address check")
	}
	if tr.DialContext == nil {
		t.Error("avatarHTTPClient Transport.DialContext = nil, want the address-checking dialer")
	}
	if tr.TLSHandshakeTimeout <= 0 {
		t.Error("avatarHTTPClient Transport.TLSHandshakeTimeout = 0, want a positive bound")
	}
}

// TestDownloadAvatarBoundsWholeChain pins that DownloadAvatar imposes its
// OWN deadline across the whole redirect chain. http.Client.Timeout applies
// per Do() call, so with a caller-supplied context that carries no deadline
// (the framework does not always set one) a chain of slow redirects would
// otherwise get a fresh budget on every hop and run for
// avatarRequestTimeout * (maxAvatarRedirects+1).
func TestDownloadAvatarBoundsWholeChain(t *testing.T) {
	const hopDelay = 120 * time.Millisecond

	origTimeout := avatarRequestTimeout
	// Comfortably longer than one hop, far shorter than the whole chain.
	avatarRequestTimeout = 2 * hopDelay
	t.Cleanup(func() { avatarRequestTimeout = origTimeout })

	var srv *httptest.Server
	srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(hopDelay)
		http.Redirect(w, r, srv.URL+"/next", http.StatusFound)
	}))
	defer srv.Close()
	useAvatarClient(t, avatarTLSClient(t, srv))

	// Deliberately deadline-free: the bound under test must come from
	// DownloadAvatar itself, not from the caller.
	start := time.Now()
	_, err := DownloadAvatar(context.Background(), srv.URL)
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("DownloadAvatar(slow redirect chain) error = %v, want context.DeadlineExceeded", err)
	}
	if budget := hopDelay * time.Duration(maxAvatarRedirects+1); elapsed >= budget {
		t.Errorf("fetch ran %v (>= the %v a per-hop-only bound would allow); deadline is not spanning the chain", elapsed, budget)
	}
}

// TestDownloadAvatarBlocksLoopbackWithProductionClient drives the REAL
// production client (no override) at a loopback listener. The dial-time
// Control hook must refuse it before any TLS handshake, which is what also
// defeats DNS rebinding: the check sees the address actually being dialed,
// not the hostname.
func TestDownloadAvatarBlocksLoopbackWithProductionClient(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("server was reached; the dial should have been blocked")
	}))
	defer srv.Close()

	_, err := DownloadAvatar(context.Background(), srv.URL)
	if !errors.Is(err, errBlockedAddress) {
		t.Fatalf("DownloadAvatar(loopback) error = %v, want errBlockedAddress", err)
	}
}

// --- DownloadAvatar: scheme enforcement ----------------------------------

// TestDownloadAvatarRejectsNonHTTPSURL: ForceHTTPS only rewrites the URL the
// caller starts with, so DownloadAvatar must refuse a plaintext URL outright
// rather than trusting that rewrite happened.
func TestDownloadAvatarRejectsNonHTTPSURL(t *testing.T) {
	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		_, _ = w.Write([]byte("should never be fetched"))
	}))
	defer srv.Close()
	useAvatarClient(t, srv.Client())

	_, err := DownloadAvatar(context.Background(), srv.URL) // http://...
	if err == nil {
		t.Fatal("DownloadAvatar(http url) = nil error, want a scheme rejection")
	}
	if hits != 0 {
		t.Errorf("plaintext server was hit %d times, want 0 (rejected before any request)", hits)
	}
}

// TestDownloadAvatarRejectsRedirectToHTTP covers the downgrade path: an
// https avatar URL that 302s to plaintext. Cloud metadata (IMDSv1) is
// http-only, so accepting this hop would undo the scheme requirement.
func TestDownloadAvatarRejectsRedirectToHTTP(t *testing.T) {
	var plainHits int
	plain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		plainHits++
		_, _ = w.Write([]byte("downgraded"))
	}))
	defer plain.Close()

	tlsSrv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, plain.URL, http.StatusFound)
	}))
	defer tlsSrv.Close()
	useAvatarClient(t, avatarTLSClient(t, tlsSrv))

	_, err := DownloadAvatar(context.Background(), tlsSrv.URL)
	if err == nil {
		t.Fatal("DownloadAvatar(https -> http redirect) = nil error, want a scheme rejection")
	}
	if plainHits != 0 {
		t.Errorf("plaintext redirect target was hit %d times, want 0", plainHits)
	}
}

// --- DownloadAvatar: redirects -------------------------------------------

// TestDownloadAvatarFollowsHTTPSRedirect is a regression guard: hardening
// must not stop ordinary CDN redirects from working.
func TestDownloadAvatarFollowsHTTPSRedirect(t *testing.T) {
	want := "final-avatar-bytes"
	final := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(want))
	}))
	defer final.Close()

	first := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, final.URL, http.StatusFound)
	}))
	defer first.Close()
	useAvatarClient(t, avatarTLSClient(t, first, final))

	got, err := DownloadAvatar(context.Background(), first.URL)
	if err != nil {
		t.Fatalf("DownloadAvatar: %v", err)
	}
	if string(got) != want {
		t.Errorf("body = %q, want %q", got, want)
	}
}

// TestDownloadAvatarRedirectCap: a server that redirects to itself forever
// must be stopped by our own cap, with our own sentinel -- not left to the
// injected client's default policy.
func TestDownloadAvatarRedirectCap(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, srv.URL+"/next", http.StatusFound)
	}))
	defer srv.Close()
	useAvatarClient(t, avatarTLSClient(t, srv))

	// Bounded independently of the production timeout so that deleting the
	// cap fails this test in seconds with a readable diff, instead of
	// spinning until go test's global -timeout kills the process.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := DownloadAvatar(ctx, srv.URL)
	if !errors.Is(err, errTooManyAvatarRedirects) {
		t.Fatalf("DownloadAvatar(redirect loop) error = %v, want errTooManyAvatarRedirects", err)
	}
}

// --- DownloadAvatar: size cap --------------------------------------------

// TestDownloadAvatarRejectsOversizedContentLength: an over-cap
// Content-Length must be refused before any body byte is buffered.
func TestDownloadAvatarRejectsOversizedContentLength(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.FormatInt(maxAvatarSize+1, 10))
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	useAvatarClient(t, avatarTLSClient(t, srv))

	_, err := DownloadAvatar(context.Background(), srv.URL)
	if !errors.Is(err, ErrFileTooLarge) {
		t.Fatalf("DownloadAvatar(oversized Content-Length) error = %v, want ErrFileTooLarge", err)
	}
}

// TestDownloadAvatarRejectsOversizedChunkedBody is the case the
// Content-Length check cannot catch: a chunked response that just keeps
// going. This is the actual memory-exhaustion vector -- io.ReadAll with no
// LimitReader would buffer the whole thing.
func TestDownloadAvatarRejectsOversizedChunkedBody(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK) // no Content-Length -> chunked
		chunk := make([]byte, 64*1024)
		for written := int64(0); written <= maxAvatarSize; written += int64(len(chunk)) {
			if _, err := w.Write(chunk); err != nil {
				return
			}
		}
	}))
	defer srv.Close()
	useAvatarClient(t, avatarTLSClient(t, srv))

	_, err := DownloadAvatar(context.Background(), srv.URL)
	if !errors.Is(err, ErrFileTooLarge) {
		t.Fatalf("DownloadAvatar(oversized chunked body) error = %v, want ErrFileTooLarge", err)
	}
}

// TestDownloadAvatarRejectsDecompressionBomb covers the case a
// Content-Length check alone cannot: a small gzip payload that expands past
// the cap. Go's transport decompresses transparently and reports
// ContentLength = -1, so the fast-reject never fires -- the cap holds only
// because the LimitReader wraps the DECOMPRESSED stream. Deleting that
// LimitReader would let a ~50KB response allocate 50MiB here.
func TestDownloadAvatarRejectsDecompressionBomb(t *testing.T) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(bytes.Repeat([]byte("A"), 50<<20)); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	payload := buf.Bytes()

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(payload)
	}))
	defer srv.Close()
	useAvatarClient(t, avatarTLSClient(t, srv))

	if _, err := DownloadAvatar(context.Background(), srv.URL); !errors.Is(err, ErrFileTooLarge) {
		t.Fatalf("DownloadAvatar(gzip bomb) error = %v, want ErrFileTooLarge", err)
	}
}

// TestDownloadAvatarAcceptsBodyAtCap proves the cap is inclusive, so a
// legitimate avatar exactly at the limit is not spuriously rejected.
func TestDownloadAvatarAcceptsBodyAtCap(t *testing.T) {
	body := make([]byte, maxAvatarSize)
	for i := range body {
		body[i] = 'a'
	}
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body)
	}))
	defer srv.Close()
	useAvatarClient(t, avatarTLSClient(t, srv))

	got, err := DownloadAvatar(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("DownloadAvatar(body exactly at cap): %v", err)
	}
	if len(got) != len(body) {
		t.Errorf("len(body) = %d, want %d", len(got), len(body))
	}
}

// --- DownloadAvatar: existing behaviour ----------------------------------

func TestDownloadAvatarRoundTrip(t *testing.T) {
	want := []byte("fake-avatar-bytes")
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(want)
	}))
	defer srv.Close()
	useAvatarClient(t, avatarTLSClient(t, srv))

	got, err := DownloadAvatar(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("DownloadAvatar: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("DownloadAvatar body = %q, want %q", got, want)
	}
}

func TestDownloadAvatarNon200IsError(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	useAvatarClient(t, avatarTLSClient(t, srv))

	_, err := DownloadAvatar(context.Background(), srv.URL)
	if err == nil {
		t.Fatal("DownloadAvatar with a 404 response = nil error, want non-nil")
	}
}

func TestDownloadAvatarInvalidURLIsError(t *testing.T) {
	_, err := DownloadAvatar(context.Background(), "://not a url")
	if err == nil {
		t.Fatal("DownloadAvatar with a malformed url = nil error, want non-nil")
	}
}
