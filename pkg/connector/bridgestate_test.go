package connector

import (
	"errors"
	"strings"
	"testing"

	"maunium.net/go/mautrix/bridgev2/status"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
)

// TestConnStateToBridgeState pins the connection-state -> bridge-state
// mapping table.
func TestConnStateToBridgeState(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name       string
		state      gchatmeow.ConnState
		err        error
		wantEvent  status.BridgeStateEvent
		wantError  status.BridgeStateErrorCode
		wantAction status.BridgeStateUserAction
	}{
		{"connected", gchatmeow.ConnStateConnected, nil, status.StateConnected, "", ""},
		{"connected with stray err ignored", gchatmeow.ConnStateConnected, boom, status.StateConnected, "", ""},
		{"transient", gchatmeow.ConnStateTransient, boom, status.StateTransientDisconnect, GChatTransientDisconnect, ""},
		{"transient nil err", gchatmeow.ConnStateTransient, nil, status.StateTransientDisconnect, GChatTransientDisconnect, ""},
		{"bad credentials", gchatmeow.ConnStateBadCredentials, boom, status.StateBadCredentials, GChatBadCredentials, status.UserActionRelogin},
		{"fatal", gchatmeow.ConnStateFatal, boom, status.StateUnknownError, GChatFatalError, status.UserActionRelogin},
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
			// Pasting fresh cookies is the only recovery this login model
			// has, so every failing state must say so in the payload.
			if got.UserAction != tt.wantAction {
				t.Errorf("UserAction = %v, want %v", got.UserAction, tt.wantAction)
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
		GChatBadCredentials,
		GChatCookiesMissing,
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

// TestHumanErrorMessagesAreActionable: a bridge state the user sees is only
// useful if it says what to DO. Every failure this connector reports has the
// same remedy -- paste fresh cookies -- so every message must name it.
func TestHumanErrorMessagesAreActionable(t *testing.T) {
	for _, code := range []status.BridgeStateErrorCode{GChatBadCredentials, GChatCookiesMissing, GChatFatalError} {
		msg := status.BridgeStateHumanErrors[code]
		if !strings.Contains(msg, "login command") {
			t.Errorf("%v message %q does not tell the user what to run", code, msg)
		}
		if !strings.Contains(msg, "cookies") {
			t.Errorf("%v message %q does not say what to supply", code, msg)
		}
	}
}

// TestHumanErrorMessagesAreMarkdownFree pins a constraint that is invisible at
// the call site: these strings have two surfaces. bridgev2 renders them as
// MARKDOWN in the management room, but emits them as RAW PLAIN TEXT in portal
// rooms on every failed send. Anything needing a renderer is therefore wrong
// on one of the two.
func TestHumanErrorMessagesAreMarkdownFree(t *testing.T) {
	for _, code := range []status.BridgeStateErrorCode{
		GChatBadCredentials, GChatCookiesMissing, GChatTransientDisconnect, GChatFatalError,
	} {
		msg := status.BridgeStateHumanErrors[code]
		if i := strings.IndexAny(msg, "`*_[]()<>"); i >= 0 {
			t.Errorf("%v message contains markdown character %q at %d: %q; it would be shown raw in portal rooms",
				code, msg[i], i, msg)
		}
	}
}

// TestFatalMapsToUnknownErrorNotBadCredentials guards the one remapping a
// future contributor is most likely to make. StateBadCredentials would look
// better -- bridgev2 substitutes the human message into portal send failures
// for it -- but StateUnknownError is the only state for which bridgev2
// schedules a reconnect, so the swap silently removes the fatal path's only
// route back to a working connection.
func TestFatalMapsToUnknownErrorNotBadCredentials(t *testing.T) {
	got := connStateToBridgeState(gchatmeow.ConnStateFatal, errors.New("boom"))
	if got.StateEvent != status.StateUnknownError {
		t.Errorf("StateEvent = %v, want StateUnknownError; StateBadCredentials would disable unknownErrorReconnect", got.StateEvent)
	}
}

// TestMissingCookiesBridgeState: a login with nothing stored is a different
// diagnosis from one Google rejected, even though the advice is identical.
func TestMissingCookiesBridgeState(t *testing.T) {
	got := missingCookiesBridgeState()
	if got.StateEvent != status.StateBadCredentials {
		t.Errorf("StateEvent = %v, want StateBadCredentials", got.StateEvent)
	}
	if got.Error != GChatCookiesMissing {
		t.Errorf("Error = %v, want %v", got.Error, GChatCookiesMissing)
	}
	if got.UserAction != status.UserActionRelogin {
		t.Errorf("UserAction = %v, want relogin", got.UserAction)
	}
	if GChatCookiesMissing == GChatBadCredentials {
		t.Error("the two credential codes are the same value; the diagnosis is lost")
	}
}
