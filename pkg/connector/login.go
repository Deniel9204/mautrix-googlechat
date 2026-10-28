package connector

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

const (
	LoginStepIDCookies  = "fi.mau.googlechat.cookies"
	LoginStepIDComplete = "fi.mau.googlechat.complete"
)

// ErrLoginCookiesInvalid is returned when Google rejects the submitted cookies.
var ErrLoginCookiesInvalid = bridgev2.RespError{
	ErrCode: "FI.MAU.GOOGLECHAT.INVALID_COOKIES",
	Err:     "Those cookies don't seem to be valid. Please log into https://chat.google.com in a browser and extract fresh ones.",
	// Literal 400 so the connector imports no HTTP package.
	StatusCode: 400,
}

// ErrLoginFailed is returned for any other validation failure; the real error is only logged.
var ErrLoginFailed = bridgev2.RespError{
	ErrCode:    "FI.MAU.GOOGLECHAT.LOGIN_FAILED",
	Err:        "Failed to log into Google Chat with the provided cookies",
	StatusCode: 400, // see ErrLoginCookiesInvalid: literal to keep net/http out of the connector
}

// GChatLogin implements bridgev2.LoginProcessCookies.
type GChatLogin struct {
	User *bridgev2.User
	Main *GChatConnector

	// newClientFn defaults to gchatmeow.NewClient; a test seam.
	newClientFn func(gchatmeow.ClientOpts) (*gchatmeow.Client, error)
}

var _ bridgev2.LoginProcessCookies = (*GChatLogin)(nil)

// loginFieldUserAgent is an optional pseudo-field carrying the browser UA; not a cookie.
const loginFieldUserAgent = "user_agent"

// loginCookieFields returns the 5 required cookies plus the optional UA field.
// CookieDomain is a browser-side extraction hint, not the jar domain: SSID, SID
// and HSID live on .google.com. COMPASS has no Pattern because bridgev2 checks it
// before missing keys and echoes the value into the room unredacted.
func loginCookieFields() []bridgev2.LoginCookieField {
	fields := make([]bridgev2.LoginCookieField, 0, len(gchatmeow.RequiredCookies)+1)
	for _, key := range gchatmeow.RequiredCookies {
		cookieDomain := ".google.com"
		if gchatmeow.CookieIsDomainSpecific(key) {
			cookieDomain = "chat.google.com"
		}
		fields = append(fields, bridgev2.LoginCookieField{
			ID:       key,
			Required: true,
			Sources: []bridgev2.LoginCookieFieldSource{
				{
					Type:         bridgev2.LoginCookieTypeCookie,
					Name:         key,
					CookieDomain: cookieDomain,
				},
			},
		})
	}
	// Optional: replay the session under the browser UA that minted the cookies.
	fields = append(fields, bridgev2.LoginCookieField{
		ID:       loginFieldUserAgent,
		Required: false,
		Sources: []bridgev2.LoginCookieFieldSource{
			{
				Type: bridgev2.LoginCookieTypeRequestHeader,
				Name: "User-Agent",
			},
		},
	})
	return fields
}

// Start returns the cookies step describing what the login UI must collect.
func (gl *GChatLogin) Start(_ context.Context) (*bridgev2.LoginStep, error) {
	return &bridgev2.LoginStep{
		Type:   bridgev2.LoginStepTypeCookies,
		StepID: LoginStepIDCookies,
		// Rendered once as markdown, so a link is fine here.
		Instructions: "Enter a JSON object with your cookies, or a cURL command copied from browser devtools.\n\n" +
			"Step-by-step instructions for extracting the five cookies (COMPASS, SSID, SID, OSID, HSID) from Chrome or Firefox: " +
			"https://github.com/Deniel9204/mautrix-googlechat/blob/main/docs/authentication.md",
		CookiesParams: &bridgev2.LoginCookiesParams{
			URL: "https://chat.google.com/",
			// UserAgent deliberately unset: forcing our default UA onto a webview
			// trips Google's "browser may not be secure" block. The real UA is
			// captured via the user_agent field instead.
			Fields: loginCookieFields(),
			// Landing back on chat.google.com means sign-in finished.
			WaitForURLPattern: `^https://chat\.google\.com/`,
		},
	}, nil
}

// Cancel is a no-op: Start allocates nothing.
func (gl *GChatLogin) Cancel() {}

// classifyXSRFError maps a FetchXSRFToken failure to the user-facing error.
func classifyXSRFError(err error) error {
	if errors.Is(err, gchatmeow.ErrNotLoggedIn) {
		return ErrLoginCookiesInvalid
	}
	return ErrLoginFailed
}

// extractGaiaID returns the caller's Gaia ID, erroring if it is missing.
func extractGaiaID(resp *pb.GetSelfUserStatusResponse) (string, error) {
	id := resp.GetUserStatus().GetUserId().GetId()
	if id == "" {
		return "", fmt.Errorf("get_self_user_status response is missing a gaia id")
	}
	return id, nil
}

// sanitizeLoginCookies trims whitespace and one layer of quotes from values,
// pops the UA pseudo-field, and reports missing required cookies by name.
func sanitizeLoginCookies(input map[string]string) (cookies map[string]string, userAgent string, missing []string) {
	cookies = make(map[string]string, len(input))
	for name, value := range input {
		value = strings.TrimSpace(value)
		if len(value) >= 2 {
			if (value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'') {
				value = strings.TrimSpace(value[1 : len(value)-1])
			}
		}
		if name == loginFieldUserAgent {
			userAgent = value
			continue
		}
		if value == "" {
			continue
		}
		cookies[name] = value
	}
	for _, name := range gchatmeow.RequiredCookies {
		if cookies[name] == "" {
			missing = append(missing, name)
		}
	}
	return cookies, userAgent, missing
}

// errMissingCookies names the missing required cookies as a 400 RespError.
func errMissingCookies(missing []string) error {
	return bridgev2.WrapRespErrManual(
		fmt.Errorf("googlechat: missing required cookies: %s -- all five (COMPASS, SSID, SID, OSID, HSID) are needed", strings.Join(missing, ", ")),
		"FI.MAU.GOOGLECHAT.MISSING_COOKIES", 400)
}

// attachAndConnect starts the already-validated client via wireAndStart.
// Must not call gc.Connect, which would build a second, cold client.
func attachAndConnect(gc *GChatClient, client *gchatmeow.Client, connectCtx context.Context) {
	gc.wireAndStart(connectCtx, client)
}

// SubmitCookies validates the cookies, resolves the Gaia ID, persists the
// login, and starts the connection.
func (gl *GChatLogin) SubmitCookies(ctx context.Context, cookies map[string]string) (*bridgev2.LoginStep, error) {
	// Pre-check locally so a nameably-wrong paste costs no round trip.
	cookies, userAgent, missing := sanitizeLoginCookies(cookies)
	if len(missing) > 0 {
		return nil, errMissingCookies(missing)
	}
	newClient := gl.newClientFn
	if newClient == nil {
		newClient = gchatmeow.NewClient
	}

	client, err := newClient(gchatmeow.ClientOpts{Cookies: cookies, UserAgent: userAgent})
	if err != nil {
		return nil, fmt.Errorf("failed to build Google Chat client: %w", err)
	}

	if err := client.FetchXSRFToken(ctx); err != nil {
		gl.User.Log.Err(err).Msg("googlechat login: failed to validate cookies via /mole/world")
		return nil, classifyXSRFError(err)
	}

	resp, err := client.GetSelfUserStatus(ctx, &pb.GetSelfUserStatusRequest{})
	if err != nil {
		gl.User.Log.Err(err).Msg("googlechat login: get_self_user_status failed")
		return nil, ErrLoginFailed
	}
	gaiaID, err := extractGaiaID(resp)
	if err != nil {
		gl.User.Log.Err(err).Msg("googlechat login: get_self_user_status returned no usable gaia id")
		return nil, ErrLoginFailed
	}

	// Persist the post-rotation cookie snapshot and UA.
	ul, err := gl.User.NewLogin(ctx, &database.UserLogin{
		ID: gcid.MakeUserLoginID(gaiaID),
		Metadata: &UserLoginMetadata{
			Cookies:   client.Cookies(),
			UserAgent: client.UserAgent(),
		},
	}, &bridgev2.NewLoginParams{
		// Re-pasting cookies moves the login to whichever Matrix user submits them.
		DeleteOnConflict: true,
		// Reuse this Matrix user's login: re-submitting refreshes the session.
		DontReuseExisting: false,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to save new login: %w", err)
	}

	// Use the warm client, not a second one, and the bridge's background
	// context, since ctx ends when SubmitCookies returns.
	gc := ul.Client.(*GChatClient)
	attachAndConnect(gc, client, ul.Log.WithContext(gl.Main.Bridge.BackgroundCtx))

	return &bridgev2.LoginStep{
		Type:         bridgev2.LoginStepTypeComplete,
		StepID:       LoginStepIDComplete,
		Instructions: fmt.Sprintf("Successfully logged in as %s", gaiaID),
		CompleteParams: &bridgev2.LoginCompleteParams{
			UserLoginID: ul.ID,
			UserLogin:   ul,
		},
	}, nil
}
