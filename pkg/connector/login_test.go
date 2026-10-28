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
	// UserAgent must be empty: a forced UA trips Google's browser-security block.
	if step.CookiesParams.UserAgent != "" {
		t.Errorf("CookiesParams.UserAgent = %q, want empty (a forced UA breaks webview sign-in)", step.CookiesParams.UserAgent)
	}
	// Must match where auth lands, not accounts.google.com.
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
		// COMPASS/OSID carry the chat.google.com hint; SSID/SID/HSID use .google.com.
		wantDomain := ".google.com"
		if f.ID == "COMPASS" || f.ID == "OSID" {
			wantDomain = "chat.google.com"
		}
		if src.CookieDomain != wantDomain {
			t.Errorf("field[%d] (%s) source.CookieDomain = %q, want %q", i, f.ID, src.CookieDomain, wantDomain)
		}
		// No Pattern: bridgev2 checks it first and echoes the value unredacted.
		if f.Pattern != "" {
			t.Errorf("field[%d] (%s).Pattern = %q, want empty (a pattern mismatch echoes the value unredacted)", i, f.ID, f.Pattern)
		}
	}

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

// TestClassifyXSRFError: ErrNotLoggedIn maps to ErrLoginCookiesInvalid; anything else to ErrLoginFailed.
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

// TestSubmitCookiesUnreachableFailsCleanly uses a pre-cancelled ctx (no public
// base-URL seam). A nil Bridge makes any NewLogin call panic.
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

// TestAttachAndConnectUsesWarmClient guards against calling gc.Connect instead
// of wireAndStart; nil UserLogin makes the wrong wiring panic.
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

	time.Sleep(50 * time.Millisecond)
}

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

func TestLoginInstructionsPointAtTheCookieDocs(t *testing.T) {
	step, err := (&GChatLogin{}).Start(context.Background())
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if !strings.Contains(step.Instructions, "docs/authentication.md") {
		t.Errorf("login instructions %q do not link the cookie-extraction guide", step.Instructions)
	}
}

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

// TestSubmitCookiesNamesMissingCookiesWithoutARoundTrip: the pre-check runs before any client is built.
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

// TestSubmitCookiesPassesSanitizedValuesToTheClient: trimmed cookies and the UA must reach ClientOpts.
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
