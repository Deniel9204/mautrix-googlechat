package connector

// The Google Chat cookie login flow.
//
// Adopted from googlechat-megabridge/pkg/connector/login.go for FLOW SHAPE:
// the LoginProcessCookies Start/SubmitCookies/Cancel shape, the 5 required
// cookie fields (COMPASS/SSID/SID/OSID/HSID) each scoped to chat.google.com,
// and the COMPASS "dynamite-ui=" pattern hint. The underlying behavior being
// ported: build a client, validate cookies via /mole/world, fetch the
// caller's own Gaia ID via get_self_user_status, persist, then start the
// long-poll loop (a 5-step sequence).
//
// Two things are DELIBERATELY NOT copied from megabridge:
//
//   - Cookie persistence: megabridge writes UserLoginMetadata.Cookies once at
//     login and never again -- no login.Save() call exists anywhere in that
//     connector, so any cookie Google rotates after login is lost on restart.
//     This port persists client.Cookies() (the POST-VALIDATION,
//     POST-ROTATION snapshot) into Metadata right here; re-persisting after
//     every connect keeps later rotations alive across a restart too.
//   - The login client-swap race: megabridge calls Connect() on the COLD
//     client LoadUserLogin built from (already-stale) metadata, THEN
//     overwrites the login's client field with the WARM, already-validated
//     one -- so observers/the channel start on one client while all
//     subsequent RPCs go through a different one, with two live cookie jars
//     and XSRF tokens. This port never builds a second client: LoadUserLogin
//     (connector.go) only allocates an empty *GChatClient shell, and
//     SubmitCookies attaches the one and only warm client to it directly
//     before starting it.
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

// ErrLoginCookiesInvalid is returned when Google rejects the submitted
// cookies (gchatmeow.ErrNotLoggedIn, from /mole/world's qwAQke ==
// "AccountsSignInUi" check). The user-facing reply for this condition is
// "Those cookies don't seem to be valid".
var ErrLoginCookiesInvalid = bridgev2.RespError{
	ErrCode: "FI.MAU.GOOGLECHAT.INVALID_COOKIES",
	Err:     "Those cookies don't seem to be valid. Please log into https://chat.google.com in a browser and extract fresh ones.",
	// 400 Bad Request. Literal rather than the net/http status constant so this
	// package imports no HTTP client at all (the connector-does-no-HTTP layering
	// rule is CI-enforced; the connector talks to Google only via gchatmeow).
	StatusCode: 400,
}

// ErrLoginFailed is returned for any other failure while validating the
// submitted cookies (network failure, malformed /mole/world response, a
// GetSelfUserStatus RPC error, ...) -- a generic, human-readable stand-in for
// the underlying error, which is logged (via the login's User.Log) for
// debugging but never surfaced to the API/Matrix client verbatim.
var ErrLoginFailed = bridgev2.RespError{
	ErrCode:    "FI.MAU.GOOGLECHAT.LOGIN_FAILED",
	Err:        "Failed to log into Google Chat with the provided cookies",
	StatusCode: 400, // see ErrLoginCookiesInvalid: literal to keep net/http out of the connector
}

// GChatLogin implements bridgev2.LoginProcessCookies: the cookie-paste login
// flow (the only login flow this bridge supports -- there is no interactive
// login web page; a browser extension / manual cookie extraction feeds
// either the command or this API).
type GChatLogin struct {
	User *bridgev2.User
	Main *GChatConnector

	// newClientFn builds the gchatmeow client from the sanitized cookies and
	// captured UA. Defaults to gchatmeow.NewClient; a test seam in the same
	// style as GChatClient's *Fn fields, so SubmitCookies' wiring (which
	// values actually reach ClientOpts) is pinnable without a network.
	newClientFn func(gchatmeow.ClientOpts) (*gchatmeow.Client, error)
}

var _ bridgev2.LoginProcessCookies = (*GChatLogin)(nil)

// loginFieldUserAgent is the ID of the optional pseudo-field that carries the
// submitting browser's User-Agent alongside the 5 cookies. It is NOT a cookie:
// SubmitCookies pops it from the map before the rest is jarred.
const loginFieldUserAgent = "user_agent"

// loginCookieFields builds the LoginCookieField descriptors the login UI must
// collect: the 5 required cookies (gchatmeow.RequiredCookies) plus the
// optional User-Agent pseudo-field.
//
// The CookieDomain here is a BROWSER-SIDE EXTRACTION HINT telling the login
// client which domain's copy of a same-named cookie to grab -- it is NOT the
// outbound cookie-jar domain (all 5 are installed flat under chat.google.com
// for requests, doc 01 §1.2). COMPASS and OSID collide across Google
// subdomains and need the chat.google.com hint
// (gchatmeow.CookieIsDomainSpecific); SSID, SID and HSID live on the PARENT
// domain, so they are pinned to .google.com -- the same three-cookie pinning
// gmessages ships in production for the identical cookie trio. An empty
// domain (what this used to declare) is ambiguous: a client interpreting it
// as "the URL's host only" would silently miss all three.
//
// COMPASS deliberately carries NO Pattern. bridgev2's bot command validates
// Pattern BEFORE its missing-keys check and its error echoes the submitted
// value into the room unredacted -- so a merely MISSING COMPASS produced a
// misleading regex error, and a wrong-but-present one leaked a credential
// into the room. The /mole/world validation catches a bad COMPASS either way.
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
	// Optional: the real browser's User-Agent, extracted from a cURL paste's
	// request headers. Replaying the session under the same UA family the
	// cookies were minted under is what a real browser does; without it every
	// session replays under gchatmeow's hardcoded default regardless of what
	// browser produced the cookies (the Python bridge captured and replayed
	// the login browser's UA for years for exactly this reason). Optional so
	// a plain JSON paste keeps working unchanged.
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
		// The one place a link belongs: this is rendered as markdown in the
		// management room and shown once, unlike the bridge-state strings
		// (bridgestate.go), which replay as raw text in every portal.
		Instructions: "Enter a JSON object with your cookies, or a cURL command copied from browser devtools.\n\n" +
			"Step-by-step instructions for extracting the five cookies (COMPASS, SSID, SID, OSID, HSID) from Chrome or Firefox: " +
			"https://github.com/Deniel9204/mautrix-googlechat/blob/main/docs/authentication.md",
		CookiesParams: &bridgev2.LoginCookiesParams{
			URL: "https://chat.google.com/",
			// CookiesParams.UserAgent is deliberately NOT set. It would tell a
			// webview-based client to browse Google's sign-in under this UA,
			// but gchatmeow's default is a Windows Chrome string -- forcing it
			// onto a client whose actual engine is a different Chromium on a
			// different OS is a browser fingerprint mismatch, exactly what
			// trips Google's "this browser may not be secure" block during
			// sign-in. gmessages, which logs into the same google.com cookie
			// family via a webview, sets no UserAgent here for the same reason.
			// The session should be MINTED under the client's own real UA;
			// that real UA is still captured and REPLAYED, via the optional
			// user_agent request-header field above, so nothing is lost.
			Fields: loginCookieFields(),
			// Google's login lives on accounts.google.com; landing back on
			// chat.google.com means auth finished, so a webview client may
			// close itself once the cookies are also collected. The framework
			// contract makes a wrong pattern mild -- no auto-close, the user
			// closes by hand, i.e. exactly today's behaviour.
			WaitForURLPattern: `^https://chat\.google\.com/`,
		},
	}, nil
}

// Cancel is a no-op: Start doesn't open any external session or allocate any
// resource that would need cleanup (unlike a QR/display-and-wait flow).
func (gl *GChatLogin) Cancel() {}

// classifyXSRFError maps a client.FetchXSRFToken failure to the user-facing
// error SubmitCookies returns. See ErrLoginCookiesInvalid/ErrLoginFailed's
// doc comments for what each branch means.
func classifyXSRFError(err error) error {
	if errors.Is(err, gchatmeow.ErrNotLoggedIn) {
		return ErrLoginCookiesInvalid
	}
	return ErrLoginFailed
}

// extractGaiaID pulls the caller's own Gaia ID out of a GetSelfUserStatus
// response (the get_self_user_status RPC returns the user's own Gaia ID).
// proto's generated Get* accessors are nil-safe at
// every level, but a missing ID is still surfaced as an explicit error
// rather than silently producing an empty-string UserLoginID.
func extractGaiaID(resp *pb.GetSelfUserStatusResponse) (string, error) {
	id := resp.GetUserStatus().GetUserId().GetId()
	if id == "" {
		return "", fmt.Errorf("get_self_user_status response is missing a gaia id")
	}
	return id, nil
}

// sanitizeLoginCookies cleans a submitted cookie map in place of trusting it:
// values are whitespace-trimmed and stripped of one layer of wrapping quotes
// (devtools double-click selection and JSON-ish hand edits add both; RFC 6265
// forbids quotes and spaces inside Google's actual values, so this can never
// corrupt a real one), the optional User-Agent pseudo-field is POPPED OUT so
// it is never jarred and replayed as a fake cookie, and every required cookie
// that is absent or empty after trimming is reported by name.
//
// The by-name report is the point. A short or slightly-wrong paste used to go
// all the way to Google and come back as the generic "cookies don't seem to
// be valid", indistinguishable from a real rejection -- for a mistake the
// bridge could have named without a round trip.
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

// errMissingCookies names exactly which of the five required cookies were
// absent or empty. Dynamic, so it cannot be a package sentinel; the RespError
// wrap makes the provisioning API answer 400 rather than an internal error.
func errMissingCookies(missing []string) error {
	return bridgev2.WrapRespErrManual(
		fmt.Errorf("googlechat: missing required cookies: %s -- all five (COMPASS, SSID, SID, OSID, HSID) are needed", strings.Join(missing, ", ")),
		"FI.MAU.GOOGLECHAT.MISSING_COOKIES", 400)
}

// attachAndConnect hands the warm, already-validated gchatmeow.Client to gc
// and starts ITS real supervision/long-poll loop via GChatClient.wireAndStart
// (client.go), which wires the bridge-state and event callbacks and
// runs conn.Connect (Task 8) in the background. Split out of SubmitCookies as
// its own named step because "Connect" is ambiguous here: *GChatClient also
// has its own Connect method (the bridgev2.NetworkAPI entry point bridgev2
// itself calls on every restart, which BUILDS A NEW gchatmeow.Client from
// persisted metadata) -- calling gc.Connect(connectCtx) here instead of
// wireAndStart would discard the freshly validated warm client and cold-start
// a redundant one from (currently identical, but conceptually stale) metadata
// instead.
func attachAndConnect(gc *GChatClient, client *gchatmeow.Client, connectCtx context.Context) {
	gc.wireAndStart(connectCtx, client)
}

// SubmitCookies validates the submitted cookies against Google Chat, resolves
// the caller's Gaia ID, creates (or reuses) the UserLogin row with the
// validated cookies persisted, and starts the connection. Follows a 5-step
// sequence (build client -> validate cookies via /mole/world
// -> get_self_user_status -> persist cookies -> spawn the long-poll loop),
// adjusted for bridgev2's login-step shape.
func (gl *GChatLogin) SubmitCookies(ctx context.Context, cookies map[string]string) (*bridgev2.LoginStep, error) {
	// Step 1: sanitize and pre-check locally -- a nameably-wrong paste must
	// not cost a round trip to Google and come back as a generic rejection.
	cookies, userAgent, missing := sanitizeLoginCookies(cookies)
	if len(missing) > 0 {
		return nil, errMissingCookies(missing)
	}
	newClient := gl.newClientFn
	if newClient == nil {
		newClient = gchatmeow.NewClient
	}
	// The captured UA (if any) rides along so the session replays under the
	// same browser fingerprint that minted the cookies.
	client, err := newClient(gchatmeow.ClientOpts{Cookies: cookies, UserAgent: userAgent})
	if err != nil {
		return nil, fmt.Errorf("failed to build Google Chat client: %w", err)
	}

	// Step 2: validate the cookies and obtain an XSRF token (the primary
	// login validity check).
	if err := client.FetchXSRFToken(ctx); err != nil {
		gl.User.Log.Err(err).Msg("googlechat login: failed to validate cookies via /mole/world")
		return nil, classifyXSRFError(err)
	}

	// Step 3: resolve the caller's own Gaia ID.
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

	// Step 4: persist the validated cookies (POST-rotation snapshot, i.e.
	// AFTER the /mole/world + get_self_user_status round trips above, which
	// may themselves have rotated cookies via Set-Cookie) and the resolved UA
	// fingerprint into UserLoginMetadata. Re-persisting after every connect
	// keeps later rotations alive across a restart too (see this file's top
	// comment on the megabridge defect this fixes).
	ul, err := gl.User.NewLogin(ctx, &database.UserLogin{
		ID: gcid.MakeUserLoginID(gaiaID),
		Metadata: &UserLoginMetadata{
			Cookies:   client.Cookies(),
			UserAgent: client.UserAgent(),
		},
	}, &bridgev2.NewLoginParams{
		// A conflicting login for this Gaia ID belonging to ANOTHER Matrix
		// user is deleted rather than rejected: re-pasting cookies for the
		// same Google account is expected to move the login to whichever
		// Matrix user submits them.
		DeleteOnConflict: true,
		// A conflicting login for THIS Matrix user is reused (its metadata
		// updated in place) rather than erroring -- re-submitting cookies is
		// exactly how a user refreshes an expired session.
		DontReuseExisting: false,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to save new login: %w", err)
	}

	// Hand the WARM, already-validated client to the login's GChatClient --
	// deliberately the ONLY client this login process ever builds; see this
	// file's top comment on the client-swap race this avoids. Step 5: spawn
	// the long-poll loop. Uses the bridge's long-lived background context
	// (not ctx, which is tied to this HTTP/provisioning request and may be
	// cancelled the moment SubmitCookies returns) -- mirrors
	// $REF/meta/pkg/connector/login.go's loginWithCookies.
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
