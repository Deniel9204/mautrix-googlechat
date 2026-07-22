package connector

import (
	"context"
	"errors"
	"fmt"
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

	fields := step.CookiesParams.Fields
	wantIDs := gchatmeow.RequiredCookies
	if len(fields) != len(wantIDs) {
		t.Fatalf("got %d fields, want exactly %d (%v)", len(fields), len(wantIDs), wantIDs)
	}
	for i, f := range fields {
		if f.ID != wantIDs[i] {
			t.Errorf("field[%d].ID = %q, want %q", i, f.ID, wantIDs[i])
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
		// Only COMPASS and OSID collide across Google subdomains and so carry
		// the chat.google.com extraction hint (matching megabridge's
		// CookieIsDomainSpecific); SSID/SID/HSID must have an EMPTY domain so
		// the client extracts them from their real parent-.google.com home.
		// Hinting the wrong domain for those would break real logins while
		// leaving every unit test that only checks "domain is set" green --
		// hence the exact per-field pin below.
		wantDomain := ""
		if f.ID == "COMPASS" || f.ID == "OSID" {
			wantDomain = "chat.google.com"
		}
		if src.CookieDomain != wantDomain {
			t.Errorf("field[%d] (%s) source.CookieDomain = %q, want %q", i, f.ID, src.CookieDomain, wantDomain)
		}
		if f.ID == "COMPASS" {
			if f.Pattern != "dynamite-ui=" {
				t.Errorf("COMPASS field.Pattern = %q, want dynamite-ui=", f.Pattern)
			}
		} else if f.Pattern != "" {
			t.Errorf("field[%d] (%s).Pattern = %q, want empty (only COMPASS carries a hint)", i, f.ID, f.Pattern)
		}
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
