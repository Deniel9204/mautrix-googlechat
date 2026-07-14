package gchatmeow

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

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

// --- DownloadAvatar ------------------------------------------------------

func TestDownloadAvatarRoundTrip(t *testing.T) {
	want := []byte("fake-avatar-bytes")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(want)
	}))
	defer srv.Close()

	origClient := avatarHTTPClient
	avatarHTTPClient = srv.Client()
	defer func() { avatarHTTPClient = origClient }()

	got, err := DownloadAvatar(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("DownloadAvatar: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("DownloadAvatar body = %q, want %q", got, want)
	}
}

func TestDownloadAvatarNon200IsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	origClient := avatarHTTPClient
	avatarHTTPClient = srv.Client()
	defer func() { avatarHTTPClient = origClient }()

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
