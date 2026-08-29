package connector

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// TestLoginStartStepShape pins the cookies LoginStep Start() returns: exactly
// the 5 RequiredCookies fields (COMPASS/SSID/SID/OSID/HSID, in that order),
// each scoped to chat.google.com via a single "cookie" source, with COMPASS
// alone carrying the dynamite-ui= pattern hint. Ported from
// googlechat-megabridge/pkg/connector/login.go's Start; all 5 -- not a
// subset -- are domain-specific to chat.google.com.
func TestLoginStartStepShape(t *testing.T) {
	gl := &GChatLogin{}
	step, err := gl.Start(context.Background())
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if step.Type != bridgev2.LoginStepTypeCookies {
		t.Errorf("step.Type = %v, want LoginStepTypeCookies", step.Type)
	}
	if step.StepID != LoginStepIDCookies {
		t.Errorf("step.StepID = %q, want %q", step.StepID, LoginStepIDCookies)
	}
	if step.CookiesParams == nil {
		t.Fatal("step.CookiesParams is nil")
	}
	if step.CookiesParams.URL != "https://chat.google.com/" {
		t.Errorf("CookiesParams.URL = %q, want https://chat.google.com/", step.CookiesParams.URL)
	}
	// The UA a webview client browses with must be the SAME one the bridge
	// replays for a session that supplies none -- one fingerprint, mint to
	// replay.
	if step.CookiesParams.UserAgent != gchatmeow.DefaultUserAgent() {
		t.Errorf("CookiesParams.UserAgent = %q, want gchatmeow's default", step.CookiesParams.UserAgent)
	}
	// The auto-close pattern must match where auth LANDS and not where it
	// happens: closing on accounts.google.com would cut the login short.
	re, err := regexp.Compile(step.CookiesParams.WaitForURLPattern)
	if err != nil {
		t.Fatalf("WaitForURLPattern %q does not compile: %v", step.CookiesParams.WaitForURLPattern, err)
	}
	if !re.MatchString("https://chat.google.com/") || !re.MatchString("https://chat.google.com/u/0/") {
		t.Errorf("WaitForURLPattern %q does not match the post-login chat URL", re)
	}
	if re.MatchString("https://accounts.google.com/signin") || re.MatchString("https://evil.example/https://chat.google.com/") {
		t.Errorf("WaitForURLPattern %q matches a URL it must not", re)
	}

	fields := step.CookiesParams.Fields
	// The 5 required cookies plus the optional User-Agent pseudo-field.
	if len(fields) != len(gchatmeow.RequiredCookies)+1 {
		t.Fatalf("got %d fields, want %d", len(fields), len(gchatmeow.RequiredCookies)+1)
	}
	for i, wantID := range gchatmeow.RequiredCookies {
		f := fields[i]
		if f.ID != wantID {
			t.Errorf("field[%d].ID = %q, want %q", i, f.ID, wantID)
		}
		if !f.Required {
			t.Errorf("field[%d] (%s).Required = false, want true", i, f.ID)
		}
		if len(f.Sources) != 1 {
			t.Fatalf("field[%d] (%s) has %d sources, want 1", i, f.ID, len(f.Sources))
		}
		src := f.Sources[0]
		if src.Type != bridgev2.LoginCookieTypeCookie {
			t.Errorf("field[%d] (%s) source.Type = %q, want %q", i, f.ID, src.Type, bridgev2.LoginCookieTypeCookie)
		}
		if src.Name != f.ID {
			t.Errorf("field[%d] source.Name = %q, want %q", i, src.Name, f.ID)
		}
		// COMPASS and OSID collide across Google subdomains and carry the
		// chat.google.com hint; SSID/SID/HSID live on the PARENT domain and
		// are pinned to .google.com -- the same trio-pinning gmessages ships
		// in production. An empty domain is ambiguous: a client reading it as
		// "the URL's host only" would silently miss all three.
		wantDomain := ".google.com"
		if f.ID == "COMPASS" || f.ID == "OSID" {
			wantDomain = "chat.google.com"
		}
		if src.CookieDomain != wantDomain {
			t.Errorf("field[%d] (%s) source.CookieDomain = %q, want %q", i, f.ID, src.CookieDomain, wantDomain)
		}
		// NO field carries a Pattern. bridgev2's bot command runs the Pattern
		// check BEFORE its missing-keys check and echoes the submitted value
		// into the room unredacted on mismatch -- so COMPASS's old
		// "dynamite-ui=" hint turned a merely-missing cookie into a
		// misleading regex error, and a wrong one into a credential leak.
		if f.Pattern != "" {
			t.Errorf("field[%d] (%s).Pattern = %q, want empty (a pattern mismatch echoes the value unredacted)", i, f.ID, f.Pattern)
		}
	}

	// The last field is the optional User-Agent pseudo-field, extracted from
	// a cURL paste's request headers so the session replays under the UA
	// family that minted the cookies.
	ua := fields[len(fields)-1]
	if ua.ID != loginFieldUserAgent {
		t.Fatalf("last field ID = %q, want %q", ua.ID, loginFieldUserAgent)
	}
	if ua.Required {
		t.Error("the User-Agent pseudo-field is Required; a plain JSON paste would stop working")
	}
	if len(ua.Sources) != 1 || ua.Sources[0].Type != bridgev2.LoginCookieTypeRequestHeader || ua.Sources[0].Name != "User-Agent" {
		t.Errorf("User-Agent field sources = %+v, want one request_header source named User-Agent", ua.Sources)
	}
}

// TestClassifyXSRFError pins the mapping from a client.FetchXSRFToken failure
// to the user-facing error returned by SubmitCookies: gchatmeow.ErrNotLoggedIn
// (Google rejected the cookies, /mole/world's AccountsSignInUi check) must
// produce the specific ErrLoginCookiesInvalid message "Those cookies don't
// seem to be valid" -- while every other error
// (network failure, malformed response, ...) collapses to the generic
// ErrLoginFailed rather than leaking a raw transport error to the user.
func TestClassifyXSRFError(t *testing.T) {
	if got := classifyXSRFError(gchatmeow.ErrNotLoggedIn); !errors.Is(got, ErrLoginCookiesInvalid) {
		t.Errorf("classifyXSRFError(ErrNotLoggedIn) = %v, want ErrLoginCookiesInvalid", got)
	}
	wrapped := fmt.Errorf("refreshing token: %w", gchatmeow.ErrNotLoggedIn)
	if got := classifyXSRFError(wrapped); !errors.Is(got, ErrLoginCookiesInvalid) {
		t.Errorf("classifyXSRFError(wrapped ErrNotLoggedIn) = %v, want ErrLoginCookiesInvalid", got)
	}
	generic := errors.New("dial tcp 127.0.0.1:1: connect: connection refused")
	if got := classifyXSRFError(generic); !errors.Is(got, ErrLoginFailed) {
		t.Errorf("classifyXSRFError(generic) = %v, want ErrLoginFailed", got)
	}
}

// TestExtractGaiaID pins gaia-ID extraction out of a GetSelfUserStatus
// response: the happy path, and an explicit error --
// never a silently-empty string -- when the ID is missing at any level,
// including a nil response.
func TestExtractGaiaID(t *testing.T) {
	resp := &pb.GetSelfUserStatusResponse{
		UserStatus: &pb.UserStatus{
			UserId: &pb.UserId{Id: proto.String("112233445566778899")},
		},
	}
	id, err := extractGaiaID(resp)
	if err != nil {
		t.Fatalf("extractGaiaID() error = %v", err)
	}
	if id != "112233445566778899" {
		t.Errorf("id = %q, want 112233445566778899", id)
	}

	if _, err := extractGaiaID(&pb.GetSelfUserStatusResponse{}); err == nil {
		t.Error("extractGaiaID(empty response) error = nil, want error")
	}
	if _, err := extractGaiaID(&pb.GetSelfUserStatusResponse{UserStatus: &pb.UserStatus{}}); err == nil {
		t.Error("extractGaiaID(response with no UserId) error = nil, want error")
	}
	if _, err := extractGaiaID(nil); err == nil {
		t.Error("extractGaiaID(nil) error = nil, want error")
	}
}

// TestSubmitCookiesUnreachableFailsCleanly exercises the real SubmitCookies
// method's error path with a pre-cancelled context, which makes
// client.FetchXSRFToken fail before any network I/O is attempted (Session's
// retry loop checks ctx.Err() before the first request). This is used
// instead of pointing at an httptest server because gchatmeow deliberately
// keeps its /mole/world base-URL override package-private (session.go's
// moleWorldBaseURL doc comment: "so only this package's own tests can point
// it at an httptest server") -- there is no public seam to do that from this
// package, and adding one for a single connector test isn't warranted.
//
// This still proves the two properties that matter: (1) the returned error
// is the friendly ErrLoginFailed, never the raw context/network error, and
// (2) User.NewLogin is never reached -- gl.User and gl.Main are non-nil
// zero-value stubs whose nested Bridge field is nil, so any accidental call
// into NewLogin (which immediately dereferences user.Bridge.cacheLock) would
// nil-pointer-panic the test instead of silently creating a login row. The
// full happy-path flow (real cookies, real GetSelfUserStatus, a real
// database-backed User) is integration-tested live, not here -- no
// in-memory bridgev2 test harness exists yet in $REF/mautrix-go or
// $REF/meta to drive it standalone.
func TestSubmitCookiesUnreachableFailsCleanly(t *testing.T) {
	gl := &GChatLogin{
		User: &bridgev2.User{},
		Main: &GChatConnector{},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	step, err := gl.SubmitCookies(ctx, map[string]string{
		"COMPASS": "dynamite-ui=abc",
		"SSID":    "a",
		"SID":     "b",
		"OSID":    "c",
		"HSID":    "d",
	})
	if step != nil {
		t.Errorf("step = %+v, want nil", step)
	}
	if !errors.Is(err, ErrLoginFailed) {
		t.Fatalf("err = %v, want ErrLoginFailed", err)
	}
}

// TestAttachAndConnectUsesWarmClient is a regression guard for exactly this
// bug: it is easy to accidentally write "go gc.Connect(ctx)" (the
// *GChatClient's own bridgev2.NetworkAPI method, which now BUILDS A NEW
// gchatmeow.Client from persisted metadata) instead of
// "gc.wireAndStart(ctx, client)" (which installs and starts the given warm,
// already-validated client) -- both compile, since *GChatClient happens to
// have its own Connect method too.
//
// gc.UserLogin is deliberately left nil: the WRONG wiring (gc.Connect's real
// body, client.go) unconditionally dereferences c.UserLogin.Metadata and
// would nil-pointer-panic this test; the CORRECT wiring (wireAndStart ->
// gchatmeow.Client.Connect) never touches GChatClient.UserLogin at all before
// a connection-state transition actually occurs, which the pre-cancelled
// context below prevents (gchatmeow.Client.Connect's loop returns almost
// immediately -- its first check is ctx.Err() != nil -- instead of attempting
// a real network call or ever invoking OnConnectionState/OnStreamEvent).
func TestAttachAndConnectUsesWarmClient(t *testing.T) {
	client, err := gchatmeow.NewClient(gchatmeow.ClientOpts{Cookies: map[string]string{}})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	gc := &GChatClient{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	attachAndConnect(gc, client, ctx)

	if gc.conn != client {
		t.Fatalf("gc.conn = %p, want the warm client %p", gc.conn, client)
	}
	// Give the background goroutine a moment to run (and, if the wiring
	// regresses to the stub, to panic) before the test process exits.
	time.Sleep(50 * time.Millisecond)
}

// TestCreateLoginReturnsGChatLogin wires CreateLogin to the "cookies" flow.
func TestCreateLoginReturnsGChatLogin(t *testing.T) {
	gc := &GChatConnector{}
	user := &bridgev2.User{}
	proc, err := gc.CreateLogin(context.Background(), user, "cookies")
	if err != nil {
		t.Fatalf("CreateLogin() error = %v", err)
	}
	gl, ok := proc.(*GChatLogin)
	if !ok {
		t.Fatalf("CreateLogin() returned %T, want *GChatLogin", proc)
	}
	if gl.User != user {
		t.Errorf("GChatLogin.User = %v, want %v", gl.User, user)
	}
	if gl.Main != gc {
		t.Errorf("GChatLogin.Main = %v, want %v", gl.Main, gc)
	}
}

func TestCreateLoginUnknownFlow(t *testing.T) {
	gc := &GChatConnector{}
	_, err := gc.CreateLogin(context.Background(), &bridgev2.User{}, "bogus")
	if err == nil {
		t.Fatal("CreateLogin(bogus) error = nil, want error")
	}
}

// TestLoginInstructionsPointAtTheCookieDocs: the login step is the one place
// in this bridge where a link renders, and the one moment the user needs the
// cookie-extraction guide. Without this nothing connects the login flow to the
// document that explains it.
func TestLoginInstructionsPointAtTheCookieDocs(t *testing.T) {
	step, err := (&GChatLogin{}).Start(context.Background())
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if !strings.Contains(step.Instructions, "docs/authentication.md") {
		t.Errorf("login instructions %q do not link the cookie-extraction guide", step.Instructions)
	}
}

// --- sanitizeLoginCookies ---------------------------------------------------

// TestSanitizeLoginCookies pins the local pre-checks that keep a
// nameably-wrong paste from costing a round trip to Google and coming back as
// the generic "cookies don't seem to be valid".
func TestSanitizeLoginCookies(t *testing.T) {
	full := func(overrides map[string]string) map[string]string {
		m := map[string]string{
			"COMPASS": "dynamite-ui=abc", "SSID": "s1", "SID": "s2", "OSID": "o1", "HSID": "h1",
		}
		for k, v := range overrides {
			m[k] = v
		}
		return m
	}

	t.Run("clean input passes through", func(t *testing.T) {
		cookies, ua, missing := sanitizeLoginCookies(full(nil))
		if len(missing) != 0 {
			t.Fatalf("missing = %v, want none", missing)
		}
		if ua != "" {
			t.Errorf("userAgent = %q, want empty when not supplied", ua)
		}
		if cookies["SID"] != "s2" {
			t.Errorf("SID = %q, want untouched", cookies["SID"])
		}
	})

	t.Run("whitespace and wrapping quotes are stripped", func(t *testing.T) {
		cookies, _, missing := sanitizeLoginCookies(full(map[string]string{
			"SID":  "  s2  ",
			"SSID": `"s1"`,
			"HSID": "'h1'",
		}))
		if len(missing) != 0 {
			t.Fatalf("missing = %v, want none: padding is a paste artifact, not an error", missing)
		}
		for name, want := range map[string]string{"SID": "s2", "SSID": "s1", "HSID": "h1"} {
			if cookies[name] != want {
				t.Errorf("%s = %q, want %q", name, cookies[name], want)
			}
		}
	})

	t.Run("missing and empty-after-trim cookies are named", func(t *testing.T) {
		in := full(map[string]string{"HSID": "   "})
		delete(in, "COMPASS")
		_, _, missing := sanitizeLoginCookies(in)
		want := map[string]bool{"COMPASS": true, "HSID": true}
		if len(missing) != len(want) {
			t.Fatalf("missing = %v, want exactly COMPASS and HSID", missing)
		}
		for _, name := range missing {
			if !want[name] {
				t.Errorf("missing names %q, which was present", name)
			}
		}
	})

	t.Run("the User-Agent pseudo-field is popped, never jarred", func(t *testing.T) {
		cookies, ua, missing := sanitizeLoginCookies(full(map[string]string{
			loginFieldUserAgent: "Mozilla/5.0 (X11; Linux x86_64) TestBrowser/1.0",
		}))
		if len(missing) != 0 {
			t.Fatalf("missing = %v, want none", missing)
		}
		if ua != "Mozilla/5.0 (X11; Linux x86_64) TestBrowser/1.0" {
			t.Errorf("userAgent = %q, want the submitted value", ua)
		}
		if _, ok := cookies[loginFieldUserAgent]; ok {
			t.Error("the User-Agent pseudo-field is still in the cookie map; it would be jarred and sent to Google as a fake cookie")
		}
	})
}

// TestSubmitCookiesNamesMissingCookiesWithoutARoundTrip: the pre-check must
// answer BEFORE any client is built, so a short paste cannot cost a network
// round trip -- which is also what lets this test run without one.
func TestSubmitCookiesNamesMissingCookiesWithoutARoundTrip(t *testing.T) {
	gl := &GChatLogin{}
	_, err := gl.SubmitCookies(context.Background(), map[string]string{
		"SID": "s2", "SSID": "s1",
	})
	if err == nil {
		t.Fatal("SubmitCookies succeeded with three cookies missing")
	}
	for _, name := range []string{"COMPASS", "OSID", "HSID"} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("error %q does not name missing cookie %s", err, name)
		}
	}
	if strings.Contains(err.Error(), "don't seem to be valid") {
		t.Errorf("error %q is the generic Google-rejection text; the local pre-check should have answered first", err)
	}
}

// TestSubmitCookiesPassesSanitizedValuesToTheClient pins the wiring between
// the sanitize step and the client build: trimmed cookie values and the popped
// User-Agent must be what actually reaches ClientOpts. Without this seam test,
// dropping the UserAgent field from the ClientOpts literal -- reverting to the
// hardcoded default for every session -- would pass the whole suite.
func TestSubmitCookiesPassesSanitizedValuesToTheClient(t *testing.T) {
	var got gchatmeow.ClientOpts
	abort := errors.New("stop before any network")
	gl := &GChatLogin{
		newClientFn: func(opts gchatmeow.ClientOpts) (*gchatmeow.Client, error) {
			got = opts
			return nil, abort
		},
	}
	_, err := gl.SubmitCookies(context.Background(), map[string]string{
		"COMPASS": "dynamite-ui=abc", "SSID": "s1", "SID": `  "s2"  `, "OSID": "o1", "HSID": "h1",
		loginFieldUserAgent: "Mozilla/5.0 (X11; Linux x86_64) TestBrowser/1.0",
	})
	if !errors.Is(err, abort) {
		t.Fatalf("err = %v, want the seam's abort error", err)
	}
	if got.UserAgent != "Mozilla/5.0 (X11; Linux x86_64) TestBrowser/1.0" {
		t.Errorf("ClientOpts.UserAgent = %q, want the captured browser UA", got.UserAgent)
	}
	if got.Cookies["SID"] != "s2" {
		t.Errorf(`ClientOpts.Cookies["SID"] = %q, want the sanitized "s2"`, got.Cookies["SID"])
	}
	if _, ok := got.Cookies[loginFieldUserAgent]; ok {
		t.Error("the User-Agent pseudo-field reached ClientOpts.Cookies")
	}
}
