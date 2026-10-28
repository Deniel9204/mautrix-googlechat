package connector

import (
	"maunium.net/go/mautrix/bridgev2/status"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
)

const (
	// GChatBadCredentials: Google rejected the stored cookies (401 / invalid_grant).
	GChatBadCredentials status.BridgeStateErrorCode = "gchat-bad-credentials"
	// GChatCookiesMissing: the login has no stored cookie set at all.
	GChatCookiesMissing status.BridgeStateErrorCode = "gchat-cookies-missing"
	// GChatTransientDisconnect: recoverable disconnect; the client is retrying.
	GChatTransientDisconnect status.BridgeStateErrorCode = "gchat-transient-disconnect"
	// GChatFatalError: unrecoverable for this connection; likely needs a relogin.
	GChatFatalError status.BridgeStateErrorCode = "gchat-fatal-error"
)

// These strings render as markdown in the management room but as plain text in
// portal rooms on every failed send: no backticks, links or brackets.
func init() {
	status.BridgeStateHumanErrors.Update(status.BridgeStateErrorMap{
		GChatBadCredentials:      "Your Google Chat session has expired. Send the login command to the bridge bot and paste a fresh set of browser cookies to reconnect.",
		GChatCookiesMissing:      "The bridge has no stored Google Chat cookies for this login. Send the login command to the bridge bot and paste a fresh set of browser cookies to reconnect.",
		GChatTransientDisconnect: "Disconnected from Google Chat, reconnecting",
		GChatFatalError:          "Lost the connection to Google Chat. The bridge keeps retrying in the background, slowly. If it does not come back, send the login command to the bridge bot and paste a fresh set of browser cookies.",
	})
}

// connStateToBridgeState maps a gchatmeow connection state to a BridgeState.
// err is not embedded: Fill overwrites Message for registered error codes.
func connStateToBridgeState(state gchatmeow.ConnState, err error) status.BridgeState {
	switch state {
	case gchatmeow.ConnStateConnected:
		return status.BridgeState{StateEvent: status.StateConnected}
	case gchatmeow.ConnStateTransient:
		return status.BridgeState{StateEvent: status.StateTransientDisconnect, Error: GChatTransientDisconnect}
	case gchatmeow.ConnStateBadCredentials:
		return status.BridgeState{StateEvent: status.StateBadCredentials, Error: GChatBadCredentials, UserAction: status.UserActionRelogin}
	case gchatmeow.ConnStateFatal:
		// StateUnknownError, not StateBadCredentials: only it triggers bridgev2's unknownErrorReconnect.
		return status.BridgeState{StateEvent: status.StateUnknownError, Error: GChatFatalError, UserAction: status.UserActionRelogin}
	default:
		return status.BridgeState{StateEvent: status.StateUnknownError, Error: GChatFatalError, UserAction: status.UserActionRelogin}
	}
}

// missingCookiesBridgeState is Connect's pre-flight state when the login has no cookies.
func missingCookiesBridgeState() status.BridgeState {
	return status.BridgeState{StateEvent: status.StateBadCredentials, Error: GChatCookiesMissing, UserAction: status.UserActionRelogin}
}
