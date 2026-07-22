package connector

import (
	"errors"
	"testing"

	"maunium.net/go/mautrix/bridgev2/status"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
)

// TestConnStateToBridgeState pins the connection-state -> bridge-state
// mapping table.
func TestConnStateToBridgeState(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name      string
		state     gchatmeow.ConnState
		err       error
		wantEvent status.BridgeStateEvent
		wantError status.BridgeStateErrorCode
	}{
		{"connected", gchatmeow.ConnStateConnected, nil, status.StateConnected, ""},
		{"connected with stray err ignored", gchatmeow.ConnStateConnected, boom, status.StateConnected, ""},
		{"transient", gchatmeow.ConnStateTransient, boom, status.StateTransientDisconnect, GChatTransientDisconnect},
		{"transient nil err", gchatmeow.ConnStateTransient, nil, status.StateTransientDisconnect, GChatTransientDisconnect},
		{"bad credentials", gchatmeow.ConnStateBadCredentials, boom, status.StateBadCredentials, GChatBadCredentials},
		{"fatal", gchatmeow.ConnStateFatal, boom, status.StateUnknownError, GChatFatalError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := connStateToBridgeState(tt.state, tt.err)
			if got.StateEvent != tt.wantEvent {
				t.Errorf("StateEvent = %v, want %v", got.StateEvent, tt.wantEvent)
			}
			if got.Error != tt.wantError {
				t.Errorf("Error = %v, want %v", got.Error, tt.wantError)
			}
		})
	}
}

// TestConnStateToBridgeStateUnknown pins the fallback for any future/unknown
// ConnState value: it must not panic, and must map to an error state rather
// than silently reporting CONNECTED.
func TestConnStateToBridgeStateUnknown(t *testing.T) {
	got := connStateToBridgeState(gchatmeow.ConnState(99), nil)
	if got.StateEvent != status.StateUnknownError {
		t.Errorf("StateEvent = %v, want StateUnknownError", got.StateEvent)
	}
	if got.Error != GChatFatalError {
		t.Errorf("Error = %v, want GChatFatalError", got.Error)
	}
}

// TestBridgeStateHumanErrorsRegistered mirrors meta's
// handlemeta.go init() registration pattern: every error code this connector
// defines must have a human-readable message registered, since
// status.BridgeState.Fill only fills Message when the code is present in the
// registry.
func TestBridgeStateHumanErrorsRegistered(t *testing.T) {
	codes := []status.BridgeStateErrorCode{
		GChatNotImplemented,
		GChatBadCredentials,
		GChatTransientDisconnect,
		GChatFatalError,
	}
	for _, code := range codes {
		msg, ok := status.BridgeStateHumanErrors[code]
		if !ok || msg == "" {
			t.Errorf("no human-readable message registered for %v", code)
		}
	}
}
