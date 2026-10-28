package gchatmeow

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

// externalTLSClient trusts srvs and never follows redirects itself.
func externalTLSClient(t *testing.T, srvs ...*httptest.Server) *http.Client {
	t.Helper()
	pool := x509.NewCertPool()
	for _, s := range srvs {
		pool.AddCert(s.Certificate())
	}
	return &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}},
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return fmt.Errorf("injected client followed a redirect itself: DownloadExternalMedia must impose its own redirect policy")
		},
	}
}

func useExternalClient(t *testing.T, c *http.Client) {
	t.Helper()
	orig := externalHTTPClient
	externalHTTPClient = c
	t.Cleanup(func() { externalHTTPClient = orig })
}

func TestExternalHTTPClientHasNoProxyAndGuardedDialer(t *testing.T) {
	c := newExternalHTTPClient()
	tr, ok := c.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("Transport = %T, want *http.Transport", c.Transport)
	}
	if tr.Proxy != nil {
		t.Error("Transport.Proxy is set; a CONNECT proxy reduces the guarded dialer to a no-op for internal ranges")
	}
	if tr.DialContext == nil {
		t.Error("Transport.DialContext is nil; the address check is not wired in")
	}
	if c.CheckRedirect == nil {
		t.Fatal("CheckRedirect is nil; the client would follow redirects without re-checking each hop's scheme")
	}
	if err := c.CheckRedirect(nil, nil); !errors.Is(err, http.ErrUseLastResponse) {
		t.Errorf("CheckRedirect = %v, want http.ErrUseLastResponse", err)
	}
}

// Drives the production client to prove the guarded dialer is installed.
func TestDownloadExternalMediaBlocksLoopbackWithProductionClient(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("server was reached; the dial should have been blocked")
	}))
	defer srv.Close()

	_, _, _, err := DownloadExternalMedia(context.Background(), srv.URL, 1<<20)
	if !errors.Is(err, errBlockedAddress) {
		t.Fatalf("error = %v, want errBlockedAddress", err)
	}
}

func TestDownloadExternalMediaRejectsNonHTTPSURL(t *testing.T) {
	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		_, _ = w.Write([]byte("should never be fetched"))
	}))
	defer srv.Close()
	useExternalClient(t, srv.Client())

	_, _, _, err := DownloadExternalMedia(context.Background(), srv.URL, 1<<20) // http://
	if err == nil {
		t.Fatal("http URL accepted, want a scheme rejection")
	}
	if hits != 0 {
		t.Errorf("plaintext server was hit %d times, want 0", hits)
	}
}

func TestDownloadExternalMediaRejectsRedirectToHTTP(t *testing.T) {
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
	useExternalClient(t, externalTLSClient(t, tlsSrv))

	_, _, _, err := DownloadExternalMedia(context.Background(), tlsSrv.URL, 1<<20)
	if err == nil {
		t.Fatal("https -> http redirect accepted, want a scheme rejection")
	}
	if plainHits != 0 {
		t.Errorf("plaintext redirect target was hit %d times, want 0", plainHits)
	}
}

func TestDownloadExternalMediaFollowsHTTPSRedirect(t *testing.T) {
	const want = "final-gif-bytes"
	final := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/gif")
		_, _ = w.Write([]byte(want))
	}))
	defer final.Close()
	first := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, final.URL, http.StatusFound)
	}))
	defer first.Close()
	useExternalClient(t, externalTLSClient(t, first, final))

	data, mimeType, _, err := DownloadExternalMedia(context.Background(), first.URL, 1<<20)
	if err != nil {
		t.Fatalf("DownloadExternalMedia: %v", err)
	}
	if string(data) != want {
		t.Errorf("data = %q, want %q", data, want)
	}
	if mimeType != "image/gif" {
		t.Errorf("mimeType = %q, want image/gif", mimeType)
	}
}

func TestDownloadExternalMediaCapsRedirects(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, srv.URL, http.StatusFound) // loops forever
	}))
	defer srv.Close()
	useExternalClient(t, externalTLSClient(t, srv))

	_, _, _, err := DownloadExternalMedia(context.Background(), srv.URL, 1<<20)
	if !errors.Is(err, errTooManyExternalRedirects) {
		t.Fatalf("error = %v, want errTooManyExternalRedirects", err)
	}
}

func TestDownloadExternalMediaRejectsOversizedBody(t *testing.T) {
	const maxSize = 1 << 10

	t.Run("declared Content-Length", func(t *testing.T) {
		srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", strconv.FormatInt(maxSize+1, 10))
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()
		useExternalClient(t, externalTLSClient(t, srv))

		_, _, _, err := DownloadExternalMedia(context.Background(), srv.URL, maxSize)
		if !errors.Is(err, ErrFileTooLarge) {
			t.Fatalf("error = %v, want ErrFileTooLarge", err)
		}
	})

	t.Run("chunked, no declared length", func(t *testing.T) {
		srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(strings.Repeat("A", maxSize*4)))
		}))
		defer srv.Close()
		useExternalClient(t, externalTLSClient(t, srv))

		_, _, _, err := DownloadExternalMedia(context.Background(), srv.URL, maxSize)
		if !errors.Is(err, ErrFileTooLarge) {
			t.Fatalf("error = %v, want ErrFileTooLarge", err)
		}
	})
}

func TestDownloadExternalMediaReturnsFilename(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/gif")
		w.Header().Set("Content-Disposition", `attachment; filename="reaction.gif"`)
		_, _ = w.Write([]byte("gif"))
	}))
	defer srv.Close()
	useExternalClient(t, externalTLSClient(t, srv))

	_, _, filename, err := DownloadExternalMedia(context.Background(), srv.URL, 1<<20)
	if err != nil {
		t.Fatalf("DownloadExternalMedia: %v", err)
	}
	if filename != "reaction.gif" {
		t.Errorf("filename = %q, want reaction.gif", filename)
	}
}

// TestDownloadExternalMediaCapsEvenWhenCallerSaysUnlimited: maxSize 0 must not mean unbounded here.
func TestDownloadExternalMediaCapsEvenWhenCallerSaysUnlimited(t *testing.T) {
	for _, callerMax := range []int64{0, -1, maxExternalMediaSize * 4} {
		t.Run(strconv.FormatInt(callerMax, 10), func(t *testing.T) {
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Length", strconv.FormatInt(maxExternalMediaSize+1, 10))
				w.WriteHeader(http.StatusOK)
			}))
			defer srv.Close()
			useExternalClient(t, externalTLSClient(t, srv))

			_, _, _, err := DownloadExternalMedia(context.Background(), srv.URL, callerMax)
			if !errors.Is(err, ErrFileTooLarge) {
				t.Fatalf("maxSize=%d: error = %v, want ErrFileTooLarge; the ceiling must not be disableable", callerMax, err)
			}
		})
	}
}
