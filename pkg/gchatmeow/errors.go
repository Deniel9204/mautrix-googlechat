package gchatmeow

import (
	"errors"
	"fmt"
)

var (
	ErrNotLoggedIn            = errors.New("not logged in")            // /mole/world shows AccountsSignInUi
	ErrChannelLifetimeExpired = errors.New("channel lifetime expired") // intentional 1.5h recycle
	ErrSIDExpiring            = errors.New("SID expiring")             // payload error -> re-register, no backoff
	ErrSIDInvalid             = errors.New("SID invalid")              // HTTP 400 "Unknown SID"

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

// UnexpectedStatusError preserves the HTTP status.
type UnexpectedStatusError struct {
	URL       string
	Status    int
	ErrorCode string // e.g. "invalid_grant" parsed from body if present
	Body      string // first 512 bytes
}

func (e *UnexpectedStatusError) Error() string {
	msg := fmt.Sprintf("unexpected status %d", e.Status)
	if e.URL != "" {
		msg = fmt.Sprintf("%s from %s", msg, e.URL)
	}
	if e.ErrorCode != "" {
		msg = fmt.Sprintf("%s: %s", msg, e.ErrorCode)
	}
	return msg
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
