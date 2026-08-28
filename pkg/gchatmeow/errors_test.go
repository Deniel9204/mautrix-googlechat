package gchatmeow

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestIsAuthError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "401 UnexpectedStatusError",
			err: &UnexpectedStatusError{
				URL:    "https://example.com/api",
				Status: 401,
				Body:   "Unauthorized",
			},
			want: true,
		},
		{
			name: "500 UnexpectedStatusError",
			err: &UnexpectedStatusError{
				URL:    "https://example.com/api",
				Status: 500,
				Body:   "Internal Server Error",
			},
			want: false,
		},
		{
			name: "wrapped ErrNotLoggedIn",
			err:  fmt.Errorf("request failed: %w", ErrNotLoggedIn),
			want: true,
		},
		{
			name: "invalid_grant error code",
			err: &UnexpectedStatusError{
				URL:       "https://example.com/token",
				Status:    400,
				ErrorCode: "invalid_grant",
				Body:      `{"error":"invalid_grant"}`,
			},
			want: true,
		},
		{
			name: "plain NetworkError",
			err:  &NetworkError{Err: errors.New("connection reset")},
			want: false,
		},
		{
			name: "ErrNotLoggedIn directly",
			err:  ErrNotLoggedIn,
			want: true,
		},
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsAuthError(tt.err); got != tt.want {
				t.Errorf("IsAuthError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestNetworkErrorUnwrap(t *testing.T) {
	originalErr := context.DeadlineExceeded
	ne := &NetworkError{Err: originalErr}

	if !errors.Is(ne, context.DeadlineExceeded) {
		t.Errorf("errors.Is(&NetworkError{Err: context.DeadlineExceeded}, context.DeadlineExceeded) = false, want true")
	}

	// Verify Unwrap returns the wrapped error
	if ne.Unwrap() != originalErr {
		t.Errorf("NetworkError.Unwrap() = %v, want %v", ne.Unwrap(), originalErr)
	}
}

// --- UnexpectedStatusError.Error() ------------------------------------------
//
// Three controls are pinned below, all on data this client does not author:
// the request URL's query string (noise at best, a capability token at worst),
// and the server's response body (the only part that usually says WHY, but
// arbitrary bytes that end up in a Matrix notice).

// TestUnexpectedStatusErrorMessageOmitsQueryParams: the endpoint name is the
// only informative part of the URL. The query carries the public web-client
// `key`, a per-process request counter -- and, for an attachment download, an
// `attachment_token` that is a real capability.
func TestUnexpectedStatusErrorMessageOmitsQueryParams(t *testing.T) {
	tests := []struct {
		name       string
		url        string
		wantSubstr string
		notSubstrs []string
	}{
		{
			name:       "api endpoint keeps its path, loses the key",
			url:        "https://chat.google.com/u/0/api/create_dm?alt=proto&c=1&key=AIzaSyD7InnYR3VKdb4j2rMUEbTCIr2VyEazl6k&rt=b",
			wantSubstr: "https://chat.google.com/u/0/api/create_dm",
			notSubstrs: []string{"key=", "AIzaSy", "alt=proto", "?"},
		},
		{
			name:       "attachment token never reaches the message",
			url:        "https://chat.google.com/api/get_attachment_url?attachment_token=SECRETCAP&url_type=DOWNLOAD_URL",
			wantSubstr: "https://chat.google.com/api/get_attachment_url",
			notSubstrs: []string{"attachment_token", "SECRETCAP"},
		},
		{
			// url.Parse rejects this, so the "cut at the first ?" fallback is
			// what has to hold the line.
			name:       "unparseable URL still loses its query",
			url:        "://nonsense?key=AIzaSyX",
			wantSubstr: "://nonsense",
			notSubstrs: []string{"AIzaSyX", "key="},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := (&UnexpectedStatusError{Status: 400, URL: tc.url}).Error()
			if !strings.Contains(msg, tc.wantSubstr) {
				t.Errorf("message %q does not contain %q -- the endpoint name must survive redaction", msg, tc.wantSubstr)
			}
			for _, bad := range tc.notSubstrs {
				if strings.Contains(msg, bad) {
					t.Errorf("message %q contains %q, which redactURL should have stripped", msg, bad)
				}
			}
		})
	}
}

// TestUnexpectedStatusErrorMessageIncludesBodyExcerpt: the server's own reason
// is the thing the message used to throw away.
func TestUnexpectedStatusErrorMessageIncludesBodyExcerpt(t *testing.T) {
	msg := (&UnexpectedStatusError{
		Status: 400,
		URL:    "https://chat.google.com/u/0/api/create_dm?key=AIzaSyX",
		Body:   "INVALID_ARGUMENT: user not found",
	}).Error()
	if !strings.Contains(msg, "INVALID_ARGUMENT: user not found") {
		t.Errorf("message %q dropped the server's error body", msg)
	}

	// A body that is only whitespace is not a reason; it must not produce a
	// dangling `-- body ""`.
	blank := (&UnexpectedStatusError{Status: 400, Body: "   \n\t  "}).Error()
	if strings.Contains(blank, "body") {
		t.Errorf("message %q rendered a whitespace-only body", blank)
	}
}

// TestUnexpectedStatusErrorBodyExcerptIsCappedAndEscaped pins the two safety
// controls on bytes Google chose: the excerpt is bounded, and it can never
// break the message onto a second line or smuggle control characters into a
// log or a Matrix notice.
func TestUnexpectedStatusErrorBodyExcerptIsCappedAndEscaped(t *testing.T) {
	t.Run("capped", func(t *testing.T) {
		msg := (&UnexpectedStatusError{
			Status: 400,
			Body:   strings.Repeat("A", 400) + "TAILMARKER",
		}).Error()
		if strings.Contains(msg, "TAILMARKER") {
			t.Error("the tail of an over-long body reached the message")
		}
		if !strings.Contains(msg, "(truncated)") {
			t.Errorf("message %q cut the body without saying so", msg)
		}
		if len(msg) > 600 {
			t.Errorf("message is %d bytes, want it bounded near maxErrorBodyMessageBytes", len(msg))
		}
	})
	t.Run("escaped", func(t *testing.T) {
		msg := (&UnexpectedStatusError{
			Status: 400,
			Body:   "line1\nline2\x00\x1b[31mred\xff",
		}).Error()
		if strings.ContainsAny(msg, "\n\r\x00\x1b") {
			t.Errorf("message %q carries raw control characters from the body", msg)
		}
		if !utf8.ValidString(msg) {
			t.Errorf("message %q is not valid UTF-8", msg)
		}
	})
}
