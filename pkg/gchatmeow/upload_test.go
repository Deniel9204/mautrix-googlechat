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

// newTestUploadClient wires a Client to srv with srv's host allowlisted so cookies attach.
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

// TestUploadFileRoundTrip covers start -> finalize -> decode, and that neither "alt" nor "key" is sent.
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

	if startMethod != http.MethodPost {
		t.Errorf("start method = %q, want POST", startMethod)
	}
	if startPath != "/uploads" {
		t.Errorf("start path = %q, want /uploads", startPath)
	}
	if len(startBody) != 0 {
		t.Errorf("start body = %q, want empty (the start request carries no body; the data goes in the finalize PUT)", startBody)
	}
	if got := startQuery["group_id"]; len(got) != 1 || got[0] != groupID {
		t.Errorf("start group_id = %v, want [%q]", got, groupID)
	}
	if _, present := startQuery["alt"]; present {
		t.Errorf(`start query has "alt" param %v -- purple-googlechat's current upload path does not send one (this is the #114-relevant divergence)`, startQuery["alt"])
	}
	if _, present := startQuery["key"]; present {
		t.Errorf(`start query has "key" param %v -- purple-googlechat's current upload path does not send one (this is the #114-relevant divergence)`, startQuery["key"])
	}
	wantStartHeaders := map[string]string{
		"X-Goog-Upload-Protocol":       "resumable",
		"X-Goog-Upload-Command":        "start",
		"X-Goog-Upload-Content-Length": "15",
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

func TestUploadFileFinalizeResponseToleratesEmbeddedWhitespace(t *testing.T) {
	wantMeta := &pb.UploadMetadata{
		Payload:     &pb.UploadMetadata_AttachmentToken{AttachmentToken: "TOK-whitespace"},
		ContentName: strPtr("f.jpg"),
	}
	wantMetaBytes, err := proto.Marshal(wantMeta)
	if err != nil {
		t.Fatalf("marshal test UploadMetadata: %v", err)
	}
	b64 := base64.StdEncoding.EncodeToString(wantMetaBytes)
	wrapped := b64[:len(b64)/2] + "\n" + b64[len(b64)/2:len(b64)-2] + " " + b64[len(b64)-2:] + "\n"

	mux := http.NewServeMux()
	var srv *httptest.Server
	mux.HandleFunc("/uploads", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-goog-upload-url", srv.URL+"/uploads/continue")
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/uploads/continue", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, wrapped)
	})
	srv = httptest.NewServer(mux)
	defer srv.Close()

	c := newTestUploadClient(t, srv, nil)
	got, err := c.UploadFile(context.Background(), "group1", []byte("data"), "f.jpg", "image/jpeg")
	if err != nil {
		t.Fatalf("UploadFile: %v, want the embedded whitespace tolerated in base64 decoding", err)
	}
	if got.GetAttachmentToken() != "TOK-whitespace" {
		t.Errorf("AttachmentToken = %q, want %q", got.GetAttachmentToken(), "TOK-whitespace")
	}
}

func TestUploadFileBadProto(t *testing.T) {
	// 0x00 is tag 0, which protobuf deterministically rejects.
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

// TestUploadFileWithholdsXSRFFromOffAllowlistUploadURL: the server-supplied upload URL must not receive the token.
func TestUploadFileWithholdsXSRFFromOffAllowlistUploadURL(t *testing.T) {
	var finalizeHeaders http.Header
	// Must differ by host, not port: hostAllowed matches on hostname.
	elsewhere := newLoopbackServer(t, "127.0.0.2", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		finalizeHeaders = r.Header.Clone()
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, base64.StdEncoding.EncodeToString(mustMarshalUploadMetadata(t)))
	}))
	defer elsewhere.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/uploads", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-goog-upload-url", elsewhere.URL+"/take-my-token")
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := newTestUploadClient(t, srv, nil)
	c.SetXSRFToken("secret-xsrf-token")

	if _, err := c.UploadFile(context.Background(), "group1", []byte("data"), "f.jpg", "image/jpeg"); err != nil {
		t.Fatalf("UploadFile: %v", err)
	}

	if got := finalizeHeaders.Get("x-framework-xsrf-token"); got != "" {
		t.Errorf("finalize request to an off-allowlist host carried x-framework-xsrf-token = %q, want it withheld", got)
	}
}

func TestUploadFileSendsXSRFToAllowlistedUploadURL(t *testing.T) {
	const token = "secret-xsrf-token"
	var finalizeHeaders http.Header
	mux := http.NewServeMux()
	var srv *httptest.Server
	mux.HandleFunc("/uploads", func(w http.ResponseWriter, r *http.Request) {
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
	c.SetXSRFToken(token)

	if _, err := c.UploadFile(context.Background(), "group1", []byte("data"), "f.jpg", "image/jpeg"); err != nil {
		t.Fatalf("UploadFile: %v", err)
	}
	if got := finalizeHeaders.Get("x-framework-xsrf-token"); got != token {
		t.Errorf("finalize x-framework-xsrf-token = %q, want %q", got, token)
	}
}
