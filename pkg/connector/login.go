package connector

// The Google Chat cookie login flow.
//
// Adopted from googlechat-megabridge/pkg/connector/login.go, which
// docs/research/08b/08c clear for FLOW SHAPE: the LoginProcessCookies
// Start/SubmitCookies/Cancel shape, the 5 required cookie fields
// (COMPASS/SSID/SID/OSID/HSID) each scoped to chat.google.com, and the
// COMPASS "dynamite-ui=" pattern hint. See docs/research/01 §1 (maugclib
// auth) and 03 §2 (Python bridge login flow) for the underlying Python
// behavior being ported: build a client, validate cookies via /mole/world
// (Client.refresh_tokens), fetch the caller's own Gaia ID via
// get_self_user_status, persist, then start the long-poll loop
// (doc 01 §1.4's 5-step sequence).
//
// Two things are DELIBERATELY NOT copied from megabridge, per
// docs/research/08b §1.3's audit of that file:
//
//   - Cookie persistence: megabridge writes UserLoginMetadata.Cookies once at
//     login and never again -- no login.Save() call exists anywhere in that
//     connector, so any cookie Google rotates after login is lost on restart
//     (08b: "Cookie rotation persistence -- ABSENT"). This port persists
//     client.Cookies() (the POST-VALIDATION, POST-ROTATION snapshot) into
//     Metadata right here; Task 11 re-persists after every connect so later
//     rotations survive a restart too.
//   - The login client-swap race: megabridge calls Connect() on the COLD
//     client LoadUserLogin built from (already-stale) metadata, THEN
//     overwrites the login's client field with the WARM, already-validated
//     one -- so observers/the channel start on one client while all
//     subsequent RPCs go through a different one, with two live cookie jars
//     and XSRF tokens (08b: "Post-login connect -- SUSPICIOUS ... Works by
//     accident at best"). This port never builds a second client: LoadUserLogin
//     (connector.go) only allocates an empty *GChatClient shell, and
//     SubmitCookies attaches the one and only warm client to it directly
//     before starting it.
import (
	"context"
	"errors"
	"fmt"
	"net/http"

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
// "AccountsSignInUi" check, client.py:536-537). Mirrors the Python bridge's
// login-cookie command reply for the same condition (docs/research/03 §2.1,
// commands/auth.py's `except NotLoggedInError` branch: "Those cookies don't
// seem to be valid").
var ErrLoginCookiesInvalid = bridgev2.RespError{
	ErrCode:    "FI.MAU.GOOGLECHAT.INVALID_COOKIES",
	Err:        "Those cookies don't seem to be valid. Please log into https://chat.google.com in a browser and extract fresh ones.",
	StatusCode: http.StatusBadRequest,
}

// ErrLoginFailed is returned for any other failure while validating the
// submitted cookies (network failure, malformed /mole/world response, a
// GetSelfUserStatus RPC error, ...) -- a generic, human-readable stand-in for
// the underlying error, which is logged (via the login's User.Log) for
// debugging but never surfaced to the API/Matrix client verbatim.
var ErrLoginFailed = bridgev2.RespError{
	ErrCode:    "FI.MAU.GOOGLECHAT.LOGIN_FAILED",
	Err:        "Failed to log into Google Chat with the provided cookies",
	StatusCode: http.StatusBadRequest,
}

// GChatLogin implements bridgev2.LoginProcessCookies: the cookie-paste login
// flow (the only login flow this bridge supports, matching Python -- doc 03
// §2: "There is no interactive login web page ... a browser extension /
// manual cookie extraction feeds either the command or this API").
type GChatLogin struct {
	User *bridgev2.User
	Main *GChatConnector
}

var _ bridgev2.LoginProcessCookies = (*GChatLogin)(nil)

// loginCookieFields builds the 5 LoginCookieField descriptors the login UI
// must collect: COMPASS, SSID, SID, OSID, HSID (gchatmeow.RequiredCookies).
// The CookieDomain here is a BROWSER-SIDE EXTRACTION HINT telling the login
// client which subdomain's copy of a same-named cookie to grab -- it is NOT
// the outbound cookie-jar domain (all 5 are installed flat under
// chat.google.com for requests, doc 01 §1.2). Only COMPASS and OSID collide
// across Google subdomains and so need the chat.google.com hint
// (gchatmeow.CookieIsDomainSpecific); SSID, SID and HSID exist only under the
// parent .google.com and are left with an EMPTY domain so the client extracts
// them from wherever they live -- pinning them to chat.google.com would make
// the extension miss them and break real logins. Copied from the cleared
// reference googlechat-megabridge/pkg/connector/login.go:53-71 (per
// docs/research/08b-megabridge-connector.md §1.3). COMPASS additionally
// carries a client-side validation hint: real COMPASS cookie values contain a
// "dynamite-ui=" segment.
func loginCookieFields() []bridgev2.LoginCookieField {
	fields := make([]bridgev2.LoginCookieField, len(gchatmeow.RequiredCookies))
	for i, key := range gchatmeow.RequiredCookies {
		var cookieDomain string
		if gchatmeow.CookieIsDomainSpecific(key) {
			cookieDomain = "chat.google.com"
		}
		fields[i] = bridgev2.LoginCookieField{
			ID:       key,
			Required: true,
			Sources: []bridgev2.LoginCookieFieldSource{
				{
					Type:         bridgev2.LoginCookieTypeCookie,
					Name:         key,
					CookieDomain: cookieDomain,
				},
			},
		}
		if key == "COMPASS" {
			fields[i].Pattern = "dynamite-ui="
		}
	}
	return fields
}

// Start returns the cookies step describing what the login UI must collect.
func (gl *GChatLogin) Start(_ context.Context) (*bridgev2.LoginStep, error) {
	return &bridgev2.LoginStep{
		Type:         bridgev2.LoginStepTypeCookies,
		StepID:       LoginStepIDCookies,
		Instructions: "Enter a JSON object with your cookies, or a cURL command copied from browser devtools.",
		CookiesParams: &bridgev2.LoginCookiesParams{
			URL:    "https://chat.google.com/",
			Fields: loginCookieFields(),
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
// response (client.py:710, proto_get_self_user_status; doc 01 §1.4 step 3:
// "proto_get_self_user_status fetches the user's own Gaia ID"). proto's
// generated Get* accessors are nil-safe at every level, but a missing ID is
// still surfaced as an explicit error rather than silently producing an
// empty-string UserLoginID.
func extractGaiaID(resp *pb.GetSelfUserStatusResponse) (string, error) {
	id := resp.GetUserStatus().GetUserId().GetId()
	if id == "" {
		return "", fmt.Errorf("get_self_user_status response is missing a gaia id")
	}
	return id, nil
}

// attachAndConnect hands the warm, already-validated gchatmeow.Client to gc
// and starts ITS real supervision/long-poll loop via GChatClient.wireAndStart
// (client.go, Task 11), which wires the bridge-state and event callbacks and
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
// validated cookies persisted, and starts the connection. Ports doc 01
// §1.4's 5-step sequence (build Client -> refresh_tokens -> get_self_user_status
// -> persist cookies -> spawn the long-poll loop), adjusted for bridgev2's
// login-step shape.
func (gl *GChatLogin) SubmitCookies(ctx context.Context, cookies map[string]string) (*bridgev2.LoginStep, error) {
	client, err := gchatmeow.NewClient(gchatmeow.ClientOpts{Cookies: cookies})
	if err != nil {
		return nil, fmt.Errorf("failed to build Google Chat client: %w", err)
	}

	// Step 2: validate the cookies and obtain an XSRF token (client.py:499-539,
	// doc 01 §1.3's "primary login validity check").
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
	// fingerprint into UserLoginMetadata. Task 11 re-persists after every
	// connect so later rotations survive a restart too (see this file's top
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
		// Matrix user submits them, matching the task's specified params.
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
