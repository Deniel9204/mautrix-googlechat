package gchatmeow

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
)

// Constants ported from maugclib/client.py, per
// docs/research/01-maugclib-client-library.md §1.3 (read 2026-07-13).
const (
	// defaultMoleWorldBaseURL mirrors GC_BASE_URL, client.py:31.
	defaultMoleWorldBaseURL = "https://chat.google.com/u/0"

	// moleWorldHS is the hardcoded gmail shell-state blob sent as the "hs"
	// query param, copied verbatim from client.py:513. Python's own comment
	// there notes some of these values actually arrive via redirect during
	// login and should probably be used instead of hard-coding -- that is
	// out of scope for this port; the value is reproduced as-is.
	moleWorldHS = `["h_hs",null,null,[1,0],null,null,"gmail.pinto-server_20230730.06_p0",1,null,[15,38,36,35,26,30,41,18,24,11,21,14,6],null,null,"3Mu86PSulM4.en..es5",0,null,null,[0]]`

	// accountsSignInUi is the qwAQke value that signals invalid/expired
	// cookies -- the primary login-validity check, client.py:536.
	accountsSignInUi = "AccountsSignInUi"
)

// wizGlobalDataPattern extracts the inline WIZ_global_data JSON blob from an
// HTML page. Copied verbatim from client.py:34 (wiz_pattern), including its
// two unescaped '.' wildcards (each matches any single character, not just a
// literal '.') -- kept for fidelity per port-module discipline (see
// session.go's firefoxVersionRegex for the same policy); harmless in
// practice since real responses only ever have literal '.' at those
// positions.
var wizGlobalDataPattern = regexp.MustCompile(`>window.WIZ_global_data = ({.+?});</script>`)

// FetchXSRFToken GETs /mole/world and scrapes the inline WIZ_global_data
// JSON blob out of the HTML response to obtain the XSRF token. Ports
// Client.refresh_tokens (client.py:499-539) -- but ONLY the fetch+extract
// step: the 24h/post-error refresh scheduling and assignment to
// Client.xsrf_token (client.py:539) are the caller's responsibility (Task
// 8's client orchestration), not this method's.
//
// Returns ErrNotLoggedIn when the WIZ data's "qwAQke" key equals
// "AccountsSignInUi" (client.py:536-537) -- this is the primary check that
// the caller's cookies are still valid. Any other extraction failure
// (missing WIZ_global_data block, malformed JSON, missing keys) is returned
// as a plain descriptive error, matching Python's bare `raise Exception(...)`
// / unhandled KeyError for those same paths (client.py:530-538) -- neither
// is NotLoggedInError, so callers must not treat them as a logout signal.
func (s *Session) FetchXSRFToken(ctx context.Context) (string, error) {
	baseURL := s.moleWorldBaseURL
	if baseURL == "" {
		baseURL = defaultMoleWorldBaseURL
	}

	// Query params mirror client.py:506-514 exactly.
	qs := url.Values{
		"origin": {"https://mail.google.com"},
		"shell":  {"9"},
		"hl":     {"en"},
		"wfi":    {"gtn-roster-iframe-id"},
		"hs":     {moleWorldHS},
	}
	reqURL := baseURL + "/mole/world?" + qs.Encode()

	// Headers mirror client.py:515-518, including the literally-misspelled
	// "refer" header name (not "referer") that Python sends.
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

	// Mirrors Python's direct `wiz_data["qwAQke"]` dict index
	// (client.py:536), which raises KeyError if the key is absent; a
	// descriptive Go error replaces that KeyError rather than silently
	// treating a missing key the same as an empty/mismatched value.
	qwAQke, ok := wizData["qwAQke"]
	if !ok {
		return "", fmt.Errorf("gchatmeow: WIZ_global_data missing %q key in /mole/world response", "qwAQke")
	}
	if qwAQke == accountsSignInUi {
		return "", ErrNotLoggedIn
	}

	// Mirrors Python's direct `wiz_data["SMqcke"]` dict index (client.py:538).
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
