package gchatmeow

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
)

const (
	defaultMoleWorldBaseURL = "https://chat.google.com/u/0"

	// moleWorldHS is the hardcoded gmail shell-state blob sent as the "hs" query param.
	moleWorldHS = `["h_hs",null,null,[1,0],null,null,"gmail.pinto-server_20230730.06_p0",1,null,[15,38,36,35,26,30,41,18,24,11,21,14,6],null,null,"3Mu86PSulM4.en..es5",0,null,null,[0]]`

	// accountsSignInUi is the qwAQke value that signals invalid/expired cookies.
	accountsSignInUi = "AccountsSignInUi"
)

var wizGlobalDataPattern = regexp.MustCompile(`>window.WIZ_global_data = ({.+?});</script>`)

// FetchXSRFToken scrapes the XSRF token from /mole/world's WIZ_global_data. Returns ErrNotLoggedIn
// only when qwAQke is AccountsSignInUi; other extraction failures are not a logout signal.
func (s *Session) FetchXSRFToken(ctx context.Context) (string, error) {
	baseURL := s.moleWorldBaseURL
	if baseURL == "" {
		baseURL = defaultMoleWorldBaseURL
	}

	qs := url.Values{
		"origin": {"https://mail.google.com"},
		"shell":  {"9"},
		"hl":     {"en"},
		"wfi":    {"gtn-roster-iframe-id"},
		"hs":     {moleWorldHS},
	}
	reqURL := baseURL + "/mole/world?" + qs.Encode()

	// "refer" (not "referer") is the misspelled header the real client sends.
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

	qwAQke, ok := wizData["qwAQke"]
	if !ok {
		return "", fmt.Errorf("gchatmeow: WIZ_global_data missing %q key in /mole/world response", "qwAQke")
	}
	if qwAQke == accountsSignInUi {
		return "", ErrNotLoggedIn
	}

	// "SMqcke" holds the xsrf token.
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
