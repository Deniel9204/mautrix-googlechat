package gchatmeow

import (
	"context"
	"errors"
	"fmt"
	"testing"
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
