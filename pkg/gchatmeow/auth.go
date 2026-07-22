package gchatmeow

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
)

// Constants for the /mole/world XSRF-token request.
const (
	// defaultMoleWorldBaseURL is the base URL for the /mole/world request.
	defaultMoleWorldBaseURL = "https://chat.google.com/u/0"

	// moleWorldHS is the hardcoded gmail shell-state blob sent as the "hs"
	// query param. Some of these values actually arrive via redirect during
	// login and should probably be used instead of hard-coding -- that is
	// out of scope here; the value is reproduced as-is.
	moleWorldHS = `["h_hs",null,null,[1,0],null,null,"gmail.pinto-server_20230730.06_p0",1,null,[15,38,36,35,26,30,41,18,24,11,21,14,6],null,null,"3Mu86PSulM4.en..es5",0,null,null,[0]]`

	// accountsSignInUi is the qwAQke value that signals invalid/expired
	// cookies -- the primary login-validity check.
	accountsSignInUi = "AccountsSignInUi"
)

// wizGlobalDataPattern extracts the inline WIZ_global_data JSON blob from an
// HTML page, including its two unescaped '.' wildcards (each matches any
// single character, not just a literal '.') -- kept for fidelity (see
// session.go's firefoxVersionRegex for the same policy); harmless in
// practice since real responses only ever have literal '.' at those
// positions.
var wizGlobalDataPattern = regexp.MustCompile(`>window.WIZ_global_data = ({.+?});</script>`)

// FetchXSRFToken GETs /mole/world and scrapes the inline WIZ_global_data
// JSON blob out of the HTML response to obtain the XSRF token. This is ONLY
// the fetch+extract step: the 24h/post-error refresh scheduling and the
// assignment of the token onto the Client are the caller's responsibility
// (client orchestration), not this method's.
//
// Returns ErrNotLoggedIn when the WIZ data's "qwAQke" key equals
// "AccountsSignInUi" -- this is the primary check that the caller's cookies
// are still valid. Any other extraction failure (missing WIZ_global_data
// block, malformed JSON, missing keys) is returned as a plain descriptive
// error -- none of them is NotLoggedInError, so callers must not treat them
// as a logout signal.
func (s *Session) FetchXSRFToken(ctx context.Context) (string, error) {
	baseURL := s.moleWorldBaseURL
	if baseURL == "" {
		baseURL = defaultMoleWorldBaseURL
	}

	// Query params required by the /mole/world endpoint.
	qs := url.Values{
		"origin": {"https://mail.google.com"},
		"shell":  {"9"},
		"hl":     {"en"},
		"wfi":    {"gtn-roster-iframe-id"},
		"hs":     {moleWorldHS},
	}
	reqURL := baseURL + "/mole/world?" + qs.Encode()

	// Headers for the /mole/world request, including the literally-misspelled
	// "refer" header name (not "referer") the real client sends.
	headers := http.Header{
		"authority": {"chat.google.com"},
		"refer":     {"https://mail.google.com/"},
	}

	resp, err := s.Fetch(ctx, http.MethodGet, reqURL, headers, nil)
	if err != nil {
		return "", err
	}

	match := wizGlobalDataPattern.FindSubmatch(resp.Body)
	if match == nil {
		return "", fmt.Errorf("gchatmeow: didn't find WIZ_global_data in /mole/world response")
	}

	var wizData map[string]any
	if err := json.Unmarshal(match[1], &wizData); err != nil {
		return "", fmt.Errorf("gchatmeow: non-JSON WIZ_global_data in /mole/world response: %w", err)
	}

	// A missing "qwAQke" key returns a descriptive error rather than being
	// silently treated the same as an empty/mismatched value.
	qwAQke, ok := wizData["qwAQke"]
	if !ok {
		return "", fmt.Errorf("gchatmeow: WIZ_global_data missing %q key in /mole/world response", "qwAQke")
	}
	if qwAQke == accountsSignInUi {
		return "", ErrNotLoggedIn
	}

	// "SMqcke" holds the xsrf token value.
	smQcke, ok := wizData["SMqcke"]
	if !ok {
		return "", fmt.Errorf("gchatmeow: WIZ_global_data missing %q key in /mole/world response", "SMqcke")
	}
	token, ok := smQcke.(string)
	if !ok {
		return "", fmt.Errorf("gchatmeow: WIZ_global_data %q is not a string in /mole/world response", "SMqcke")
	}

	return token, nil
}
