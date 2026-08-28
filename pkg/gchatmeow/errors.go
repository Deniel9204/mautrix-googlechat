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

	// ErrChannelNotReady is returned by SendStreamEvent when the channel
	// has no SID yet -- either before the first register completes or during
	// a re-register, which clears the SID for the whole round trip. Sending
	// in that window would put an empty SID on the wire.
	ErrChannelNotReady = errors.New("gchatmeow: channel is not ready to send (no SID)")
	ErrSIDInvalid      = errors.New("SID invalid") // HTTP 400 "Unknown SID"

	// ErrFileTooLarge is returned when an attachment's Content-Length -- or,
	// absent that, its actual body size -- exceeds the caller-supplied
	// max_size. download.go's DownloadAttachment wraps this sentinel with
	// context via %w so callers use errors.Is(err, ErrFileTooLarge) to skip
	// uploading an oversized attachment instead of failing the whole message.
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

// UnexpectedStatusError preserves the HTTP status, and the start of the
// server's own response body -- which for an /api/* rejection is usually the
// only thing that says WHY (create_dm's 400s carry an inner reason, unlike
// create_group's; see CreateGroup's doc comment). Error() renders both.
type UnexpectedStatusError struct {
	URL       string
	Status    int
	ErrorCode string // e.g. "invalid_grant" parsed from body if present
	Body      string // first 512 bytes
}

// maxErrorBodyMessageBytes caps how much of Body is rendered into Error()'s
// string. Body itself keeps up to maxErrorBodyBytes (session.go) for
// programmatic inspection; the rendered form is shorter because a human reads
// it -- and, via bridgev2's start-chat handler, it is posted verbatim into a
// Matrix room. Keep the two in step.
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
		// %q, not %s: an /api/* error body may be binary protobuf or an HTML
		// page. Quoting escapes newlines, NULs and invalid UTF-8, so the
		// message stays one printable line whether it lands in a structured
		// log field or in a Matrix notice.
		msg = fmt.Sprintf("%s -- body %q", msg, excerpt)
		if truncated {
			msg += " (truncated)"
		}
	}
	return msg
}

// redactURL reduces a request URL to scheme://host/path, dropping the query
// string, fragment and any userinfo.
//
// Every query parameter this client sends is either noise or a token, and
// neither belongs in an error a human reads: api.go's doRequestOnce sends
// `key` (the PUBLIC Hangouts web-client key -- see apiKey's doc comment, not a
// secret, just 39 characters of nothing), the `c` request counter and the
// `rt`/`alt` encoding selectors; download.go's attachment URL carries an
// `attachment_token`, which IS a capability token. The part that says WHICH
// request failed -- the endpoint name -- lives in the path, and survives.
//
// A URL that will not parse has everything from the first "?" cut off
// instead, so a malformed URL cannot smuggle its query through.
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

// bodyExcerpt returns the part of body to render into Error(), plus whether it
// had to be cut. Byte slicing is safe here because the caller renders the
// result with %q, which escapes a half rune rather than emitting invalid
// UTF-8.
func bodyExcerpt(body string) (string, bool) {
	body = strings.TrimSpace(body)
	if len(body) > maxErrorBodyMessageBytes {
		return body[:maxErrorBodyMessageBytes], true
	}
	return body, false
}

// IsAuthError reports whether err means the session is dead (401, or
// ErrorCode "invalid_grant", or ErrNotLoggedIn) -> connector maps to BAD_CREDENTIALS.
func IsAuthError(err error) bool {
	if err == nil {
		return false
	}

	// Check for unwrapped ErrNotLoggedIn
	if errors.Is(err, ErrNotLoggedIn) {
		return true
	}

	// Check for UnexpectedStatusError with 401 or invalid_grant
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
