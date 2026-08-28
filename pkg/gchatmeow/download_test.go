package gchatmeow

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// --- AttachmentURL -----------------------------------------------------

// TestAttachmentURLImage verifies the FIFE_URL branch: an image content_type
// gets url_type=FIFE_URL, sz=w10000-h10000, and content_type echoed back,
// all against the get_attachment_url endpoint.
func TestAttachmentURLImage(t *testing.T) {
	meta := &pb.UploadMetadata{
		Payload:     &pb.UploadMetadata_AttachmentToken{AttachmentToken: "TOK123"},
		ContentType: strPtr("image/jpeg"),
		ContentName: strPtr("photo.jpg"),
	}

	rawURL, isImage, err := (&Client{}).AttachmentURL(meta)
	if err != nil {
		t.Fatalf("AttachmentURL: %v", err)
	}
	if !isImage {
		t.Fatal("isImage = false, want true for image/jpeg")
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("url.Parse(%q): %v", rawURL, err)
	}
	if got := u.Scheme + "://" + u.Host + u.Path; got != "https://chat.google.com/api/get_attachment_url" {
		t.Errorf("base URL = %q, want https://chat.google.com/api/get_attachment_url", got)
	}
	q := u.Query()
	if got := q.Get("url_type"); got != "FIFE_URL" {
		t.Errorf("url_type = %q, want FIFE_URL", got)
	}
	if got := q.Get("sz"); got != "w10000-h10000" {
		t.Errorf("sz = %q, want w10000-h10000", got)
	}
	if got := q.Get("content_type"); got != "image/jpeg" {
		t.Errorf("content_type = %q, want image/jpeg", got)
	}
	if got := q.Get("attachment_token"); got != "TOK123" {
		t.Errorf("attachment_token = %q, want TOK123", got)
	}
}

// TestAttachmentURLNonImage verifies the DOWNLOAD_URL branch (the default
// query), and that NEITHER sz NOR content_type are present -- those are only
// added inside the image branch.
func TestAttachmentURLNonImage(t *testing.T) {
	meta := &pb.UploadMetadata{
		Payload:     &pb.UploadMetadata_AttachmentToken{AttachmentToken: "TOK456"},
		ContentType: strPtr("application/pdf"),
	}

	rawURL, isImage, err := (&Client{}).AttachmentURL(meta)
	if err != nil {
		t.Fatalf("AttachmentURL: %v", err)
	}
	if isImage {
		t.Fatal("isImage = true, want false for application/pdf")
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("url.Parse(%q): %v", rawURL, err)
	}
	q := u.Query()
	if got := q.Get("url_type"); got != "DOWNLOAD_URL" {
		t.Errorf("url_type = %q, want DOWNLOAD_URL", got)
	}
	if got := q.Get("attachment_token"); got != "TOK456" {
		t.Errorf("attachment_token = %q, want TOK456", got)
	}
	if q.Has("sz") {
		t.Errorf("sz present = %q, want absent for non-image", q.Get("sz"))
	}
	if q.Has("content_type") {
		t.Errorf("content_type present = %q, want absent for non-image", q.Get("content_type"))
	}
}

// TestAttachmentURLNilMeta verifies a nil UploadMetadata errors rather than
// panicking on a nil dereference.
func TestAttachmentURLNilMeta(t *testing.T) {
	if _, _, err := (&Client{}).AttachmentURL(nil); err == nil {
		t.Fatal("AttachmentURL(nil) = nil error, want an error")
	}
}

func strPtr(s string) *string { return &s }

// --- DownloadAttachment --------------------------------------------------

func newTestDownloadClient(t *testing.T, cookies map[string]string, allowedHost string) *Client {
	t.Helper()
	c, err := NewClient(ClientOpts{Cookies: cookies})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if allowedHost != "" {
		c.session.allowedHostSuffixes = []string{allowedHost}
	}
	return c
}

// TestDownloadAttachmentRedirectChain verifies a multi-hop 302 chain is
// followed to its final 200 response, and mime/filename/data are extracted
// correctly (usually there are 4 redirects for files).
func TestDownloadAttachmentRedirectChain(t *testing.T) {
	useDownloadClient(t)
	const body = "hello attachment bytes"
	final := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/pdf")
		w.Header().Set("Content-Disposition", `attachment; filename="report.pdf"`)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}))
	defer final.Close()

	var hop2 *httptest.Server
	hop2 = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, final.URL, http.StatusFound)
	}))
	defer hop2.Close()

	var hop1 *httptest.Server
	hop1 = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, hop2.URL, http.StatusMovedPermanently)
	}))
	defer hop1.Close()

	c := newTestDownloadClient(t, nil, "")
	data, mimeType, filename, err := c.DownloadAttachment(context.Background(), hop1.URL, 0)
	if err != nil {
		t.Fatalf("DownloadAttachment: %v", err)
	}
	if string(data) != body {
		t.Errorf("data = %q, want %q", data, body)
	}
	if mimeType != "application/pdf" {
		t.Errorf("mime = %q, want application/pdf", mimeType)
	}
	if filename != "report.pdf" {
		t.Errorf("filename = %q, want report.pdf", filename)
	}
}

// TestDownloadAttachmentRedirectCapExceeded verifies a redirect chain that
// never terminates is capped at maxDownloadRedirects hops and errors
// instead of looping forever or silently falling off the end of the loop.
func TestDownloadAttachmentRedirectCapExceeded(t *testing.T) {
	useDownloadClient(t)
	var hits int32
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		http.Redirect(w, r, srv.URL, http.StatusFound)
	}))
	defer srv.Close()

	c := newTestDownloadClient(t, nil, "")
	_, _, _, err := c.DownloadAttachment(context.Background(), srv.URL, 0)
	if err == nil {
		t.Fatal("DownloadAttachment = nil error, want redirect-cap error")
	}
	if got := atomic.LoadInt32(&hits); got != maxDownloadRedirects {
		t.Errorf("server hit %d times, want exactly %d (cap reached, no 11th request)", got, maxDownloadRedirects)
	}
}

// TestDownloadAttachmentMaxSizeContentLength verifies a Content-Length that
// already exceeds maxSize is rejected via ErrFileTooLarge before the body
// would need to be read.
func TestDownloadAttachmentMaxSizeContentLength(t *testing.T) {
	useDownloadClient(t)
	body := strings.Repeat("x", 1000)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	c := newTestDownloadClient(t, nil, "")
	_, _, _, err := c.DownloadAttachment(context.Background(), srv.URL, 10)
	if !errors.Is(err, ErrFileTooLarge) {
		t.Fatalf("err = %v, want errors.Is(err, ErrFileTooLarge)", err)
	}
}

// TestDownloadAttachmentMaxSizeChunked verifies the read-loop cap catches an
// oversized body even when Content-Length is unknown (chunked transfer, no
// upfront length to fast-path reject on) -- the "read one more byte than the
// cap, then fail if we got it" safety net.
func TestDownloadAttachmentMaxSizeChunked(t *testing.T) {
	useDownloadClient(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fl, ok := w.(http.Flusher)
		if !ok {
			t.Fatal("ResponseWriter does not support flushing")
		}
		w.WriteHeader(http.StatusOK)
		for i := 0; i < 5; i++ {
			_, _ = w.Write([]byte("chunk"))
			fl.Flush()
		}
	}))
	defer srv.Close()

	c := newTestDownloadClient(t, nil, "")
	_, _, _, err := c.DownloadAttachment(context.Background(), srv.URL, 10)
	if !errors.Is(err, ErrFileTooLarge) {
		t.Fatalf("err = %v, want errors.Is(err, ErrFileTooLarge)", err)
	}
}

// TestDownloadAttachmentMaxSizeZeroMeansUnlimited verifies maxSize <= 0
// disables the cap entirely -- the documented "unlimited" contract (see
// download.go's DownloadAttachment doc comment).
func TestDownloadAttachmentMaxSizeZeroMeansUnlimited(t *testing.T) {
	useDownloadClient(t)
	body := strings.Repeat("y", 5000)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	c := newTestDownloadClient(t, nil, "")
	data, _, _, err := c.DownloadAttachment(context.Background(), srv.URL, 0)
	if err != nil {
		t.Fatalf("DownloadAttachment: %v", err)
	}
	if string(data) != body {
		t.Errorf("data length = %d, want %d", len(data), len(body))
	}
}

// TestDownloadAttachmentCookiesPerHost verifies auth cookies are sent to an
// allowlisted host but withheld from one outside the allowlist -- the M5
// download flow's core requirement (session.go's hostAllowed).
func TestDownloadAttachmentCookiesPerHost(t *testing.T) {
	useDownloadClient(t)
	var allowedCookie, outsideCookie string
	var allowedSeen, outsideSeen bool

	allowedSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		allowedCookie = r.Header.Get("Cookie")
		allowedSeen = true
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer allowedSrv.Close()

	outsideSrv := newLoopbackServer(t, "127.0.0.2", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		outsideCookie = r.Header.Get("Cookie")
		outsideSeen = true
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer outsideSrv.Close()

	c := newTestDownloadClient(t, map[string]string{"SID": "secret"}, testServerHost(t, allowedSrv.URL))

	if _, _, _, err := c.DownloadAttachment(context.Background(), allowedSrv.URL, 0); err != nil {
		t.Fatalf("DownloadAttachment(allowed): %v", err)
	}
	if _, _, _, err := c.DownloadAttachment(context.Background(), outsideSrv.URL, 0); err != nil {
		t.Fatalf("DownloadAttachment(outside): %v", err)
	}

	if !allowedSeen || !strings.Contains(allowedCookie, "SID=secret") {
		t.Errorf("allowlisted host Cookie = %q, want it to contain SID=secret", allowedCookie)
	}
	if !outsideSeen {
		t.Fatal("non-allowlisted server was never hit")
	}
	if outsideCookie != "" {
		t.Errorf("non-allowlisted host Cookie = %q, want empty", outsideCookie)
	}
}

// TestDownloadAttachmentCookiesAcrossRedirectChain verifies the core reason
// DownloadAttachment follows redirects manually instead of relying on
// net/http's default policy: a single call whose redirect chain crosses
// allowlisted -> non-allowlisted -> allowlisted hosts must attach cookies on
// the first and third hops and withhold them on the second, and -- critically
// -- must RE-attach cookies on the third hop after they were withheld on the
// second. This exact scenario arises when a download is redirected from
// googleusercontent.com back to chat.google.com; Go's default automatic
// redirect-following would instead permanently strip the Cookie header on
// the first cross-host hop and never restore it.
func TestDownloadAttachmentCookiesAcrossRedirectChain(t *testing.T) {
	useDownloadClient(t)
	var cookieAtHop1, cookieAtHop2, cookieAtHop3 string

	var hop2, hop3 *httptest.Server

	hop3 = newLoopbackServer(t, "127.0.0.12", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cookieAtHop3 = r.Header.Get("Cookie")
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("final"))
	}))
	defer hop3.Close()

	hop2 = newLoopbackServer(t, "127.0.0.11", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cookieAtHop2 = r.Header.Get("Cookie")
		http.Redirect(w, r, hop3.URL, http.StatusFound)
	}))
	defer hop2.Close()

	hop1 := newLoopbackServer(t, "127.0.0.10", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cookieAtHop1 = r.Header.Get("Cookie")
		http.Redirect(w, r, hop2.URL, http.StatusFound)
	}))
	defer hop1.Close()

	c, err := NewClient(ClientOpts{Cookies: map[string]string{"SID": "secret"}})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	// Only hop1 and hop3's hosts are allowlisted; hop2's is deliberately
	// left out so it plays the role of a googleusercontent.com-style hop in
	// the middle of the chain.
	c.session.allowedHostSuffixes = []string{
		testServerHost(t, hop1.URL),
		testServerHost(t, hop3.URL),
	}

	data, _, _, err := c.DownloadAttachment(context.Background(), hop1.URL, 0)
	if err != nil {
		t.Fatalf("DownloadAttachment: %v", err)
	}
	if string(data) != "final" {
		t.Errorf("data = %q, want %q", data, "final")
	}

	if !strings.Contains(cookieAtHop1, "SID=secret") {
		t.Errorf("hop1 (allowlisted) Cookie = %q, want it to contain SID=secret", cookieAtHop1)
	}
	if cookieAtHop2 != "" {
		t.Errorf("hop2 (non-allowlisted) Cookie = %q, want empty", cookieAtHop2)
	}
	if !strings.Contains(cookieAtHop3, "SID=secret") {
		t.Errorf("hop3 (allowlisted again) Cookie = %q, want it to contain SID=secret (must be re-attached after crossing back)", cookieAtHop3)
	}
}

// TestDownloadAttachmentFilenameEmptyPathFallback verifies the
// Content-Disposition-absent fallback for a URL with NO path segment at all
// yields an empty filename -- not path.Base's "." for an empty string, which
// download.go's doc comment explicitly calls out as the reason it uses a
// manual strings.Split instead of Go's path.Base.
func TestDownloadAttachmentFilenameEmptyPathFallback(t *testing.T) {
	useDownloadClient(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data"))
	}))
	defer srv.Close()

	c := newTestDownloadClient(t, nil, "")
	_, _, filename, err := c.DownloadAttachment(context.Background(), srv.URL, 0)
	if err != nil {
		t.Fatalf("DownloadAttachment: %v", err)
	}
	if filename != "" {
		t.Errorf("filename = %q, want empty string for a URL with no path segment", filename)
	}
}

// TestDownloadAttachmentFilenameFallback verifies the filename falls back to
// the URL's last path segment when Content-Disposition is absent.
func TestDownloadAttachmentFilenameFallback(t *testing.T) {
	useDownloadClient(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("pngdata"))
	}))
	defer srv.Close()

	c := newTestDownloadClient(t, nil, "")
	_, mimeType, filename, err := c.DownloadAttachment(context.Background(), srv.URL+"/some/path/avatar.png", 0)
	if err != nil {
		t.Fatalf("DownloadAttachment: %v", err)
	}
	if mimeType != "image/png" {
		t.Errorf("mime = %q, want image/png", mimeType)
	}
	if filename != "avatar.png" {
		t.Errorf("filename = %q, want avatar.png", filename)
	}
}

// TestDownloadAttachmentErrorStatus verifies a non-redirect, >=400 response
// is surfaced as an error rather than being read as a successful body.
func TestDownloadAttachmentErrorStatus(t *testing.T) {
	useDownloadClient(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	c := newTestDownloadClient(t, nil, "")
	if _, _, _, err := c.DownloadAttachment(context.Background(), srv.URL, 0); err == nil {
		t.Fatal("DownloadAttachment = nil error, want an error for HTTP 404")
	}
}

// useDownloadClient installs a plain client for the duration of the test.
//
// The shipped downloadHTTPClient refuses to dial internal addresses, which
// every test in this file would otherwise trip: httptest servers live on
// loopback. Swapping the client is the same seam avatar.go's tests use, and
// it is safe for the redirect tests specifically because DownloadAttachment
// imposes its own CheckRedirect per call rather than inheriting the
// installed client's policy.
func useDownloadClient(t *testing.T) {
	t.Helper()
	orig := downloadHTTPClient
	downloadHTTPClient = &http.Client{}
	t.Cleanup(func() { downloadHTTPClient = orig })
}

// TestDownloadAttachmentBlocksInternalHopWithProductionClient drives the REAL
// shipped client at a loopback listener. The attachment URL itself is always
// Google's fixed endpoint, so this is defence in depth: it is what stops a
// redirect anywhere in the chain -- an open redirect on Google's side, a
// compromised CDN hop, an on-path injection -- from steering the bridge at
// its operator's own network.
func TestDownloadAttachmentBlocksInternalHopWithProductionClient(t *testing.T) {
	srv := newLoopbackServer(t, "127.0.0.20", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("server was reached; the dial should have been blocked")
	}))
	defer srv.Close()

	c, err := NewClient(ClientOpts{})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	_, _, _, err = c.DownloadAttachment(context.Background(), srv.URL, 0)
	if !errors.Is(err, errBlockedAddress) {
		t.Fatalf("DownloadAttachment(loopback) error = %v, want errBlockedAddress", err)
	}
}
