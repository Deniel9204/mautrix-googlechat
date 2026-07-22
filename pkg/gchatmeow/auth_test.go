package gchatmeow

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Minimal WIZ blobs using the exact keys the token fetch reads: "qwAQke"
// (login-state key) and "SMqcke" (xsrf token key), embedded in the same
// `>window.WIZ_global_data = {...};</script>` shape wizGlobalDataPattern
// expects, inside a plausible surrounding HTML page.

const loggedInWizHTML = `<!doctype html><html><head><script>
window._wizdata = {};
</script><script>window.WIZ_global_data = {"qwAQke":"SomeOtherUi","SMqcke":"the-xsrf-token-value","cfb2h":"boot"};</script>
</head><body>ok</body></html>`

const signInWizHTML = `<!doctype html><html><head>
<script>window.WIZ_global_data = {"qwAQke":"AccountsSignInUi","cfb2h":"boot"};</script>
</head><body>please sign in</body></html>`

const garbageNoWizHTML = `<!doctype html><html><body>this page has no WIZ data at all</body></html>`

const garbageMalformedWizHTML = `<!doctype html><html><head>
<script>window.WIZ_global_data = {not valid json};</script>
</head><body>broken</body></html>`

// newXSRFTestSession builds a Session pointed at an httptest server via the
// unexported moleWorldBaseURL override (auth.go / session.go), mirroring
// how session_test.go overrides allowedHostSuffixes for the same purpose.
func newXSRFTestSession(t *testing.T, srv *httptest.Server) *Session {
	t.Helper()
	sess, err := NewSession(nil, "")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	sess.moleWorldBaseURL = srv.URL
	sess.allowedHostSuffixes = []string{testServerHost(t, srv.URL)}
	return sess
}

func TestFetchXSRFToken_Success(t *testing.T) {
	var gotPath string
	var gotQuery, gotAuthority, gotRefer string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		gotAuthority = r.Header.Get("authority")
		gotRefer = r.Header.Get("refer")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(loggedInWizHTML))
	}))
	defer srv.Close()

	sess := newXSRFTestSession(t, srv)

	token, err := sess.FetchXSRFToken(context.Background())
	if err != nil {
		t.Fatalf("FetchXSRFToken() error = %v, want nil", err)
	}
	if token != "the-xsrf-token-value" {
		t.Errorf("FetchXSRFToken() = %q, want %q", token, "the-xsrf-token-value")
	}

	if gotPath != "/mole/world" {
		t.Errorf("request path = %q, want %q", gotPath, "/mole/world")
	}
	// Query params required by the /mole/world endpoint.
	for _, want := range []string{
		"origin=https%3A%2F%2Fmail.google.com",
		"shell=9",
		"hl=en",
		"wfi=gtn-roster-iframe-id",
		"hs=",
	} {
		if !strings.Contains(gotQuery, want) {
			t.Errorf("request query = %q, want it to contain %q", gotQuery, want)
		}
	}
	// Headers for the request, including the misspelled "refer".
	if gotAuthority != "chat.google.com" {
		t.Errorf("authority header = %q, want %q", gotAuthority, "chat.google.com")
	}
	if gotRefer != "https://mail.google.com/" {
		t.Errorf("refer header = %q, want %q", gotRefer, "https://mail.google.com/")
	}
}

func TestFetchXSRFToken_NotLoggedIn(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(signInWizHTML))
	}))
	defer srv.Close()

	sess := newXSRFTestSession(t, srv)

	_, err := sess.FetchXSRFToken(context.Background())
	if !errors.Is(err, ErrNotLoggedIn) {
		t.Fatalf("FetchXSRFToken() error = %v, want errors.Is(err, ErrNotLoggedIn)", err)
	}
}

func TestFetchXSRFToken_GarbageNoWizData(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(garbageNoWizHTML))
	}))
	defer srv.Close()

	sess := newXSRFTestSession(t, srv)

	_, err := sess.FetchXSRFToken(context.Background())
	if err == nil {
		t.Fatal("FetchXSRFToken() error = nil, want a descriptive error")
	}
	if !strings.Contains(err.Error(), "WIZ_global_data") {
		t.Errorf("FetchXSRFToken() error = %q, want it to mention WIZ_global_data", err.Error())
	}
	if errors.Is(err, ErrNotLoggedIn) {
		t.Errorf("FetchXSRFToken() error = %q, should NOT be ErrNotLoggedIn for a garbage page", err.Error())
	}
}

func TestFetchXSRFToken_GarbageMalformedJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(garbageMalformedWizHTML))
	}))
	defer srv.Close()

	sess := newXSRFTestSession(t, srv)

	_, err := sess.FetchXSRFToken(context.Background())
	if err == nil {
		t.Fatal("FetchXSRFToken() error = nil, want a descriptive error")
	}
	if !strings.Contains(err.Error(), "WIZ_global_data") {
		t.Errorf("FetchXSRFToken() error = %q, want it to mention WIZ_global_data", err.Error())
	}
}

func TestFetchXSRFToken_MissingSMqckeKey(t *testing.T) {
	// qwAQke present and not the sign-in sentinel, but SMqcke absent entirely
	// -- a missing SMqcke is a descriptive error rather than silently
	// yielding an empty token.
	const html = `<!doctype html><html><head>
<script>window.WIZ_global_data = {"qwAQke":"SomeOtherUi"};</script>
</head><body>ok</body></html>`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(html))
	}))
	defer srv.Close()

	sess := newXSRFTestSession(t, srv)

	_, err := sess.FetchXSRFToken(context.Background())
	if err == nil {
		t.Fatal("FetchXSRFToken() error = nil, want a descriptive error")
	}
	if !strings.Contains(err.Error(), "SMqcke") {
		t.Errorf("FetchXSRFToken() error = %q, want it to mention SMqcke", err.Error())
	}
}

func TestFetchXSRFToken_NonOKStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	sess := newXSRFTestSession(t, srv)
	// A single 500 well within Session.Fetch's retry budget still ends up a
	// NetworkError once retries are exhausted; assert we surface *an* error
	// rather than a token, proving FetchXSRFToken propagates Session.Fetch's
	// failure instead of swallowing it.
	_, err := sess.FetchXSRFToken(context.Background())
	if err == nil {
		t.Fatal("FetchXSRFToken() error = nil, want an error for a non-200 /mole/world response")
	}
}
