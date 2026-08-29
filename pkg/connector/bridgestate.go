package connector

// Connection-state -> bridge-state mapping and the human-readable error
// catalog for this connector. Mirrors $REF/meta/pkg/connector/handlemeta.go's
// init()-time status.BridgeStateHumanErrors.Update(...) registration pattern.
import (
	"maunium.net/go/mautrix/bridgev2/status"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
)

const (
	// GChatBadCredentials is sent for gchatmeow.ConnStateBadCredentials: 401 /
	// invalid_grant / not-logged-in, i.e. Google actively rejected a cookie
	// set the bridge did have.
	GChatBadCredentials status.BridgeStateErrorCode = "gchat-bad-credentials"
	// GChatCookiesMissing is sent by Connect's pre-flight when
	// UserLoginMetadata holds no usable cookie set at all. Distinct from
	// GChatBadCredentials -- same advice to the user, different diagnosis for
	// whoever reads the log -- following the sibling connectors' pattern of
	// fine-grained codes with coarse-grained advice.
	GChatCookiesMissing status.BridgeStateErrorCode = "gchat-cookies-missing"
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

// These strings have TWO surfaces with different rules, which is the
// constraint to keep in mind before editing them:
//
//   - the management room, where bridgev2 renders them as MARKDOWN; and
//   - every portal room, as raw PLAIN TEXT, replayed on each failed send
//     ("Your message was not bridged: <string>").
//
// So: no backticks, no links, no brackets or angle brackets -- anything that
// needs rendering is wrong on the second surface. Keep them to one sentence of
// diagnosis plus one instruction, because after a session expires they repeat
// in every portal the user has. The step-by-step cookie-extraction link lives
// in the login command's instructions instead (login.go), which is
// markdown-rendered and shown once.
func init() {
	status.BridgeStateHumanErrors.Update(status.BridgeStateErrorMap{
		GChatBadCredentials:      "Your Google Chat session has expired. Send the login command to the bridge bot and paste a fresh set of browser cookies to reconnect.",
		GChatCookiesMissing:      "The bridge has no stored Google Chat cookies for this login. Send the login command to the bridge bot and paste a fresh set of browser cookies to reconnect.",
		GChatTransientDisconnect: "Disconnected from Google Chat, reconnecting",
		GChatFatalError:          "Lost the connection to Google Chat and could not re-establish it. The bridge will not retry on its own. Send the login command to the bridge bot and paste a fresh set of browser cookies to reconnect.",
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
// Every failing state also carries UserAction: status.UserActionRelogin, since
// pasting fresh cookies is the only recovery this bridge's login model has.
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
		return status.BridgeState{StateEvent: status.StateBadCredentials, Error: GChatBadCredentials, UserAction: status.UserActionRelogin}
	case gchatmeow.ConnStateFatal:
		// StateUnknownError, NOT StateBadCredentials, and this is load-bearing.
		// Mapping a fatal to StateBadCredentials is tempting -- it would make
		// bridgev2 substitute the human message into portal-room send failures
		// -- but StateUnknownError is the ONLY state for which bridgev2
		// schedules unknownErrorReconnect, so the swap would delete the fatal
		// path's only route back to a working connection for any operator who
		// has enabled it.
		return status.BridgeState{StateEvent: status.StateUnknownError, Error: GChatFatalError, UserAction: status.UserActionRelogin}
	default:
		return status.BridgeState{StateEvent: status.StateUnknownError, Error: GChatFatalError, UserAction: status.UserActionRelogin}
	}
}

// missingCookiesBridgeState is Connect's pre-flight state: the login row holds
// no usable cookie set. Not derivable from a gchatmeow.ConnState, because
// gchatmeow never observes this -- it is a connector-side database fact, found
// before any connection is attempted.
func missingCookiesBridgeState() status.BridgeState {
	return status.BridgeState{StateEvent: status.StateBadCredentials, Error: GChatCookiesMissing, UserAction: status.UserActionRelogin}
}
