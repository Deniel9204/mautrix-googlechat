package connector

// Connection-state -> bridge-state mapping and the human-readable error
// catalog for this connector. Mirrors $REF/meta/pkg/connector/handlemeta.go's
// init()-time status.BridgeStateHumanErrors.Update(...) registration pattern.
import (
	"maunium.net/go/mautrix/bridgev2/status"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
)

const (
	// GChatNotImplemented marks a feature that hasn't been built yet. Not
	// currently emitted by Connect (kept for parity with the task's stated
	// constant set / potential future stub paths).
	GChatNotImplemented status.BridgeStateErrorCode = "gchat-not-implemented"
	// GChatBadCredentials is sent for gchatmeow.ConnStateBadCredentials (401 /
	// invalid_grant / not-logged-in -- the session is dead) and for the
	// pre-flight check in Connect when UserLoginMetadata has no usable
	// cookies at all.
	GChatBadCredentials status.BridgeStateErrorCode = "gchat-bad-credentials"
	// GChatTransientDisconnect is sent for gchatmeow.ConnStateTransient: a
	// recoverable disconnect that the client library is already backing off
	// and retrying internally.
	GChatTransientDisconnect status.BridgeStateErrorCode = "gchat-transient-disconnect"
	// GChatFatalError is sent for gchatmeow.ConnStateFatal (e.g. a
	// SID-invalid storm exceeding the resync cap) -- an unrecoverable
	// condition for the current connection attempt that most likely needs a
	// user-initiated relogin.
	GChatFatalError status.BridgeStateErrorCode = "gchat-fatal-error"
)

func init() {
	status.BridgeStateHumanErrors.Update(status.BridgeStateErrorMap{
		GChatNotImplemented:      "This feature is not implemented yet",
		GChatBadCredentials:      "Logged out of Google Chat, please log in again",
		GChatTransientDisconnect: "Disconnected from Google Chat, reconnecting",
		GChatFatalError:          "Google Chat connection failed and could not recover; please log in again",
	})
}

// connStateToBridgeState maps a gchatmeow connection-state transition to the
// bridgev2 BridgeState the connector should send:
//
//	ConnStateConnected      -> StateConnected
//	ConnStateTransient      -> StateTransientDisconnect + GChatTransientDisconnect
//	ConnStateBadCredentials -> StateBadCredentials      + GChatBadCredentials
//	ConnStateFatal          -> StateUnknownError         + GChatFatalError
//
// Pure and I/O-free so it is directly table-testable. err is accepted (and
// logged separately by the caller, handleConnState) for parity with
// gchatmeow's OnConnectionState signature, but is deliberately NOT embedded
// into BridgeState.Message: every code above is registered in
// BridgeStateHumanErrors, and status.BridgeState.Fill unconditionally
// overwrites Message from that registry when the code is registered
// (bridgestate.go:157-165 in $REF/mautrix-go), so any custom Message set here
// would be silently discarded before the state reaches a client anyway.
func connStateToBridgeState(state gchatmeow.ConnState, err error) status.BridgeState {
	switch state {
	case gchatmeow.ConnStateConnected:
		return status.BridgeState{StateEvent: status.StateConnected}
	case gchatmeow.ConnStateTransient:
		return status.BridgeState{StateEvent: status.StateTransientDisconnect, Error: GChatTransientDisconnect}
	case gchatmeow.ConnStateBadCredentials:
		return status.BridgeState{StateEvent: status.StateBadCredentials, Error: GChatBadCredentials}
	case gchatmeow.ConnStateFatal:
		return status.BridgeState{StateEvent: status.StateUnknownError, Error: GChatFatalError}
	default:
		return status.BridgeState{StateEvent: status.StateUnknownError, Error: GChatFatalError}
	}
}
