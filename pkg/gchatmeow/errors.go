package gchatmeow

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

var (
	ErrNotLoggedIn            = errors.New("not logged in")            // /mole/world shows AccountsSignInUi
	ErrChannelLifetimeExpired = errors.New("channel lifetime expired") // intentional 1.5h recycle
	ErrSIDExpiring            = errors.New("SID expiring")             // payload error -> re-register, no backoff

	// ErrChannelNotReady is returned by SendStreamEvent when the channel has no SID yet.
	ErrChannelNotReady = errors.New("gchatmeow: channel is not ready to send (no SID)")
	ErrSIDInvalid      = errors.New("SID invalid") // HTTP 400 "Unknown SID"

	// ErrFileTooLarge is returned (wrapped) when a download exceeds the caller's max size.
	ErrFileTooLarge = errors.New("googlechat: file size larger than maximum")
)

// NetworkError wraps transient transport failures (timeouts, conn resets).
type NetworkError struct {
	Err error
}

func (e *NetworkError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("network error: %v", e.Err)
	}
	return "network error"
}

func (e *NetworkError) Unwrap() error {
	return e.Err
}

// UnexpectedStatusError preserves the HTTP status and the start of the response body.
type UnexpectedStatusError struct {
	URL       string
	Status    int
	ErrorCode string // e.g. "invalid_grant" parsed from body if present
	Body      string // first 512 bytes
}

// maxErrorBodyMessageBytes caps the body excerpt in Error(), which may be posted into a Matrix room.
const maxErrorBodyMessageBytes = 256

func (e *UnexpectedStatusError) Error() string {
	msg := fmt.Sprintf("unexpected status %d", e.Status)
	if u := redactURL(e.URL); u != "" {
		msg = fmt.Sprintf("%s from %s", msg, u)
	}
	if e.ErrorCode != "" {
		msg = fmt.Sprintf("%s: %s", msg, e.ErrorCode)
	}
	if excerpt, truncated := bodyExcerpt(e.Body); excerpt != "" {
		// %q keeps a binary/HTML body on one printable line.
		msg = fmt.Sprintf("%s -- body %q", msg, excerpt)
		if truncated {
			msg += " (truncated)"
		}
	}
	return msg
}

// redactURL reduces a URL to scheme://host/path; the query can carry an attachment_token capability.
func redactURL(rawURL string) string {
	if rawURL == "" {
		return ""
	}
	if u, err := url.Parse(rawURL); err == nil {
		u.RawQuery = ""
		u.ForceQuery = false
		u.Fragment = ""
		u.RawFragment = ""
		u.User = nil
		return u.String()
	}
	if i := strings.IndexByte(rawURL, '?'); i >= 0 {
		return rawURL[:i]
	}
	return rawURL
}

// bodyExcerpt returns the trimmed, capped body; byte slicing is safe because it is rendered with %q.
func bodyExcerpt(body string) (string, bool) {
	body = strings.TrimSpace(body)
	if len(body) > maxErrorBodyMessageBytes {
		return body[:maxErrorBodyMessageBytes], true
	}
	return body, false
}

// IsAuthError reports whether err means the session is dead (401, invalid_grant, or ErrNotLoggedIn).
func IsAuthError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, ErrNotLoggedIn) {
		return true
	}

	var ue *UnexpectedStatusError
	if errors.As(err, &ue) {
		if ue.Status == 401 {
			return true
		}
		if ue.ErrorCode == "invalid_grant" {
			return true
		}
	}

	return false
}
