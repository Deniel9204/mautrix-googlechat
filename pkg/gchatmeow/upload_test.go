package gchatmeow

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/protobuf/proto"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// newTestUploadClient builds a Client wired to srv via the unexported
// uploadBaseURL override (api.go), with the httptest host trusted by the
// Session's allowlist so cookies actually get attached -- upload.go's
// package comment requires the resumable upload go through the SAME
// authenticated Session as every other RPC (chat.google.com needs cookies).
func newTestUploadClient(t *testing.T, srv *httptest.Server, cookies map[string]string) *Client {
	t.Helper()
	c, err := NewClient(ClientOpts{Cookies: cookies})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	c.session.allowedHostSuffixes = []string{testServerHost(t, srv.URL)}
	c.uploadBaseURL = srv.URL + "/uploads"
	return c
}

// TestUploadFileRoundTrip is the RED/GREEN anchor for the whole resumable
// flow: start (POST /uploads, resumable-start headers) -> read x-goog-upload-url
// from the response header -> finalize (PUT, "upload, finalize" + offset 0)
// -> base64(binary UploadMetadata) response body decoded back into a
// *pb.UploadMetadata.
//
// It also asserts the ONE concrete divergence this task's diff turned up
// (see upload.go's package doc comment): Python's client.py:295's
// _base_request unconditionally tacks "alt=" and "key=<API_KEY>" onto BOTH
// requests' query string (client.py:658-666), but purple-googlechat's
// actively-maintained upload path (googlechat_conversation.c:1839,
// 1792-1806) sends neither. This port matches purple: neither "alt" nor
// "key" should appear on either request.
func TestUploadFileRoundTrip(t *testing.T) {
	const (
		groupID  = "space/AAAAgroup123"
		filename = "photo.jpg"
		mimeType = "image/jpeg"
	)
	data := []byte("fake jpeg bytes")

	var (
		startMethod  string
		startPath    string
		startQuery   map[string][]string
		startHeaders http.Header
		startBody    []byte

		finalizeMethod  string
		finalizePath    string
		finalizeQuery   map[string][]string
		finalizeHeaders http.Header
		finalizeBody    []byte
	)

	wantMeta := &pb.UploadMetadata{
		Payload:     &pb.UploadMetadata_AttachmentToken{AttachmentToken: "TOK-abc"},
		ContentName: strPtr(filename),
		ContentType: strPtr(mimeType),
	}
	wantMetaBytes, err := proto.Marshal(wantMeta)
	if err != nil {
		t.Fatalf("marshal test UploadMetadata: %v", err)
	}
	wantMetaB64 := base64.StdEncoding.EncodeToString(wantMetaBytes)

	mux := http.NewServeMux()
	var srv *httptest.Server
	mux.HandleFunc("/uploads", func(w http.ResponseWriter, r *http.Request) {
		startMethod = r.Method
		startPath = r.URL.Path
		startQuery = map[string][]string(r.URL.Query())
		startHeaders = r.Header.Clone()
		b, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("reading start request body: %v", err)
		}
		startBody = b

		w.Header().Set("x-goog-upload-url", srv.URL+"/uploads/continue")
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/uploads/continue", func(w http.ResponseWriter, r *http.Request) {
		finalizeMethod = r.Method
		finalizePath = r.URL.Path
		finalizeQuery = map[string][]string(r.URL.Query())
		finalizeHeaders = r.Header.Clone()
		b, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("reading finalize request body: %v", err)
		}
		finalizeBody = b

		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, wantMetaB64)
	})
	srv = httptest.NewServer(mux)
	defer srv.Close()

	const wantXSRFToken = "test-xsrf-token"
	c := newTestUploadClient(t, srv, map[string]string{"sid": "s", "ssid": "s", "osid": "s", "hsid": "s", "compass": "s"})
	c.SetXSRFToken(wantXSRFToken)

	got, err := c.UploadFile(context.Background(), groupID, data, filename, mimeType)
	if err != nil {
		t.Fatalf("UploadFile: %v", err)
	}

	// -- start request --
	if startMethod != http.MethodPost {
		t.Errorf("start method = %q, want POST", startMethod)
	}
	if startPath != "/uploads" {
		t.Errorf("start path = %q, want /uploads", startPath)
	}
	if len(startBody) != 0 {
		t.Errorf("start body = %q, want empty (Python passes data=None for the start request)", startBody)
	}
	if got := startQuery["group_id"]; len(got) != 1 || got[0] != groupID {
		t.Errorf("start group_id = %v, want [%q]", got, groupID)
	}
	if _, present := startQuery["alt"]; present {
		t.Errorf(`start query has "alt" param %v -- purple-googlechat's current upload path does not send one (Python's _base_request does; this is the #114-relevant divergence)`, startQuery["alt"])
	}
	if _, present := startQuery["key"]; present {
		t.Errorf(`start query has "key" param %v -- purple-googlechat's current upload path does not send one (Python's _base_request does; this is the #114-relevant divergence)`, startQuery["key"])
	}
	wantStartHeaders := map[string]string{
		"X-Goog-Upload-Protocol":       "resumable",
		"X-Goog-Upload-Command":        "start",
		"X-Goog-Upload-Content-Length": "15", // len(data)
		"X-Goog-Upload-Content-Type":   mimeType,
		"X-Goog-Upload-File-Name":      filename,
	}
	for k, want := range wantStartHeaders {
		if got := startHeaders.Get(k); got != want {
			t.Errorf("start header %s = %q, want %q", k, got, want)
		}
	}
	if got := startHeaders.Get("Cookie"); got == "" {
		t.Errorf("start request has no Cookie header, want cookies attached (chat.google.com is an authenticated endpoint)")
	}
	if got := startHeaders.Get("x-framework-xsrf-token"); got != wantXSRFToken {
		t.Errorf("start x-framework-xsrf-token = %q, want %q (matches purple-googlechat's googlechat_set_auth_headers, called on every upload request)", got, wantXSRFToken)
	}

	// -- finalize request --
	if finalizeMethod != http.MethodPut {
		t.Errorf("finalize method = %q, want PUT", finalizeMethod)
	}
	if finalizePath != "/uploads/continue" {
		t.Errorf("finalize path = %q, want /uploads/continue", finalizePath)
	}
	if string(finalizeBody) != string(data) {
		t.Errorf("finalize body = %q, want %q", finalizeBody, data)
	}
	if _, present := finalizeQuery["alt"]; present {
		t.Errorf(`finalize query has "alt" param %v, want absent`, finalizeQuery["alt"])
	}
	if _, present := finalizeQuery["key"]; present {
		t.Errorf(`finalize query has "key" param %v, want absent`, finalizeQuery["key"])
	}
	wantFinalizeHeaders := map[string]string{
		"X-Goog-Upload-Command":  "upload, finalize",
		"X-Goog-Upload-Protocol": "resumable",
		"X-Goog-Upload-Offset":   "0",
	}
	for k, want := range wantFinalizeHeaders {
		if got := finalizeHeaders.Get(k); got != want {
			t.Errorf("finalize header %s = %q, want %q", k, got, want)
		}
	}
	if got := finalizeHeaders.Get("Cookie"); got == "" {
		t.Errorf("finalize request has no Cookie header, want cookies attached")
	}
	if got := finalizeHeaders.Get("x-framework-xsrf-token"); got != wantXSRFToken {
		t.Errorf("finalize x-framework-xsrf-token = %q, want %q (matches purple-googlechat's googlechat_set_auth_headers, called on every upload request)", got, wantXSRFToken)
	}

	// -- decoded response --
	if got.GetAttachmentToken() != wantMeta.GetAttachmentToken() {
		t.Errorf("AttachmentToken = %q, want %q", got.GetAttachmentToken(), wantMeta.GetAttachmentToken())
	}
	if got.GetContentName() != filename {
		t.Errorf("ContentName = %q, want %q", got.GetContentName(), filename)
	}
	if got.GetContentType() != mimeType {
		t.Errorf("ContentType = %q, want %q", got.GetContentType(), mimeType)
	}
}

// TestUploadFileNoXSRFTokenOmitsHeader verifies that before any XSRF token
// has been set (Client's zero value -- e.g. before the very first
// FetchXSRFToken/mole-world round trip), UploadFile simply omits the
// x-framework-xsrf-token header instead of sending an empty one, mirroring
// api.go's doRequestOnce (`if token := c.XSRFToken(); token != ""`).
func TestUploadFileNoXSRFTokenOmitsHeader(t *testing.T) {
	var startHeaders, finalizeHeaders http.Header
	mux := http.NewServeMux()
	var srv *httptest.Server
	mux.HandleFunc("/uploads", func(w http.ResponseWriter, r *http.Request) {
		startHeaders = r.Header.Clone()
		w.Header().Set("x-goog-upload-url", srv.URL+"/uploads/continue")
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/uploads/continue", func(w http.ResponseWriter, r *http.Request) {
		finalizeHeaders = r.Header.Clone()
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, base64.StdEncoding.EncodeToString(mustMarshalUploadMetadata(t)))
	})
	srv = httptest.NewServer(mux)
	defer srv.Close()

	c := newTestUploadClient(t, srv, nil)
	if _, err := c.UploadFile(context.Background(), "group1", []byte("data"), "f.jpg", "image/jpeg"); err != nil {
		t.Fatalf("UploadFile: %v", err)
	}

	if _, present := startHeaders["X-Framework-Xsrf-Token"]; present {
		t.Errorf("start request has x-framework-xsrf-token header, want absent when no token has been set")
	}
	if _, present := finalizeHeaders["X-Framework-Xsrf-Token"]; present {
		t.Errorf("finalize request has x-framework-xsrf-token header, want absent when no token has been set")
	}
}

func mustMarshalUploadMetadata(t *testing.T) []byte {
	t.Helper()
	b, err := proto.Marshal(&pb.UploadMetadata{})
	if err != nil {
		t.Fatalf("marshal empty UploadMetadata: %v", err)
	}
	return b
}

// TestUploadFileStartServerError verifies a 500 from the /uploads start
// request surfaces as a clear, non-nil error (the #114 path: Google's
// /uploads endpoint reportedly 500s) rather than panicking or silently
// returning a zero-value UploadMetadata.
func TestUploadFileStartServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, "internal error")
	}))
	defer srv.Close()

	c := newTestUploadClient(t, srv, nil)
	got, err := c.UploadFile(context.Background(), "group1", []byte("data"), "f.jpg", "image/jpeg")
	if err == nil {
		t.Fatal("UploadFile = nil error, want an error for a 500 on start")
	}
	if got != nil {
		t.Errorf("UploadFile returned non-nil metadata %v alongside an error", got)
	}
}

// TestUploadFileFinalizeServerError verifies a 500 from the finalize
// PUT (start succeeds and hands back a valid upload URL) also surfaces as a
// clear error.
func TestUploadFileFinalizeServerError(t *testing.T) {
	mux := http.NewServeMux()
	var srv *httptest.Server
	mux.HandleFunc("/uploads", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-goog-upload-url", srv.URL+"/uploads/continue")
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/uploads/continue", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, "internal error")
	})
	srv = httptest.NewServer(mux)
	defer srv.Close()

	c := newTestUploadClient(t, srv, nil)
	got, err := c.UploadFile(context.Background(), "group1", []byte("data"), "f.jpg", "image/jpeg")
	if err == nil {
		t.Fatal("UploadFile = nil error, want an error for a 500 on finalize")
	}
	if got != nil {
		t.Errorf("UploadFile returned non-nil metadata %v alongside an error", got)
	}
}

// TestUploadFileMissingUploadURLHeader verifies a 200 start response that
// omits x-goog-upload-url (the only way the client learns where to PUT the
// bytes) is a clear error rather than a panic on an empty PUT target,
// mirroring client.py:297-300's explicit KeyError->NetworkError translation.
func TestUploadFileMissingUploadURLHeader(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := newTestUploadClient(t, srv, nil)
	_, err := c.UploadFile(context.Background(), "group1", []byte("data"), "f.jpg", "image/jpeg")
	if err == nil {
		t.Fatal("UploadFile = nil error, want an error when x-goog-upload-url is missing")
	}
}

// TestUploadFileBadBase64 verifies a finalize response body that isn't
// valid base64 is a clear decode error, mirroring client.py:313-315's
// binascii.Error -> NetworkError translation.
func TestUploadFileBadBase64(t *testing.T) {
	mux := http.NewServeMux()
	var srv *httptest.Server
	mux.HandleFunc("/uploads", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-goog-upload-url", srv.URL+"/uploads/continue")
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/uploads/continue", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "!!! not base64 !!!")
	})
	srv = httptest.NewServer(mux)
	defer srv.Close()

	c := newTestUploadClient(t, srv, nil)
	_, err := c.UploadFile(context.Background(), "group1", []byte("data"), "f.jpg", "image/jpeg")
	if err == nil {
		t.Fatal("UploadFile = nil error, want a base64 decode error")
	}
}

// TestUploadFileBadProto verifies a base64-valid but non-protobuf payload
// is a clear decode error, mirroring client.py:316-319's
// proto.DecodeError -> NetworkError translation.
func TestUploadFileBadProto(t *testing.T) {
	// A single 0x00 byte is tag=0 (field number 0), which golang/protobuf
	// deterministically rejects ("illegal tag 0 (wire type 0)") -- protobuf's
	// wire format is otherwise too permissive to guarantee a parse failure
	// (see api.go's unmarshalAPIResponse doc comment on this exact risk).
	badProto := []byte{0x00}
	badProtoB64 := base64.StdEncoding.EncodeToString(badProto)

	mux := http.NewServeMux()
	var srv *httptest.Server
	mux.HandleFunc("/uploads", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-goog-upload-url", srv.URL+"/uploads/continue")
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/uploads/continue", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, badProtoB64)
	})
	srv = httptest.NewServer(mux)
	defer srv.Close()

	c := newTestUploadClient(t, srv, nil)
	_, err := c.UploadFile(context.Background(), "group1", []byte("data"), "f.jpg", "image/jpeg")
	if err == nil {
		t.Fatal("UploadFile = nil error, want a protobuf decode error")
	}
}

// TestUploadFileContextCanceled verifies a canceled context aborts the
// upload with a wrapped context error rather than hanging or panicking --
// no direct Python equivalent (aiohttp has no first-class context
// cancellation), but table stakes for a Go network call.
func TestUploadFileContextCanceled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := newTestUploadClient(t, srv, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := c.UploadFile(ctx, "group1", []byte("data"), "f.jpg", "image/jpeg")
	if err == nil {
		t.Fatal("UploadFile = nil error, want an error for a canceled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want errors.Is(err, context.Canceled)", err)
	}
}
