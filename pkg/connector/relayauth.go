package connector

// relayauth.go -- sender-ownership enforcement for outbound actions that
// mutate content somebody else may have authored (edit, delete, reaction
// removal).
//
// # Why this is needed only in relay mode
//
// Normally each Matrix user drives their OWN Google Chat login, so Google's
// per-account authorization is the real check: the server simply refuses an
// edit or delete of a message the acting account did not author, and the
// bridge needs no opinion of its own.
//
// Relay mode breaks that assumption. Every relayed user's action is
// dispatched through ONE shared Google Chat account (bridgev2 falls back to
// portal.Relay when no login owns the sender, setting OrigSender to the real
// Matrix user). To Google, all of that content genuinely belongs to the relay
// account, so edit_message / delete_message / update_reaction(REMOVE) SUCCEED
// against another relayed user's content -- the server has no way to tell the
// two apart. That makes the bridge the only place the distinction survives.
//
// Matrix's own permissions do not cover this either. Sending an m.replace
// edit relation requires no power level at all, so without this check ANY
// room member could rewrite another relayed user's message; redaction needs
// only the (moderator-level, and entirely Matrix-side) redact power, which
// says nothing about who authored the underlying Google Chat content.
// bridgev2 does not enforce ownership before dispatching either -- its own
// handleMatrixRedaction carries a standing "TODO ignore if sender doesn't
// match?" on exactly this path -- so the connector enforces it here.
//
// The identity to compare against is the bridge's own record: bridgev2 fills
// database.Message.SenderMXID / database.Reaction.SenderMXID from the real
// Matrix event sender when the connector leaves it unset (portal.go's
// fillDBMessage), which this connector does. So a relayed user's rows carry
// their own MXID, not the relay account's, and an inbound Google Chat message
// carries the ghost's MXID -- neither of which can equal a different relayed
// user's MXID.
import (
	"errors"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

// errRelayNotYourContent is returned when a relayed user targets content that
// the bridge did not record them as the sender of. Reported as
// MessageStatusNoPermission rather than a generic failure so the Matrix
// client shows the user why it was refused, and mirrors the static-rejection
// shape handlematrix.go's errOutboundMediaDisabled already uses.
var errRelayNotYourContent = bridgev2.WrapErrorInStatus(errors.New("relayed user does not own the target content")).
	WithMessage("You can only edit or delete content you sent yourself through this relay").
	WithIsCertain(true).
	WithSendNotice(true).
	WithErrorReason(event.MessageStatusNoPermission)

// checkRelayOwnership reports whether an outbound mutation may proceed.
//
// origSender is nil for everything except a relayed event, so the ordinary
// (own-login) path is unaffected and keeps relying on Google's own
// authorization -- see this file's doc comment.
//
// An empty targetSenderMXID fails CLOSED. It should not occur (bridgev2 fills
// the field for every row it writes), but an unset value is precisely the
// case where ownership cannot be established, and silently permitting it
// would reinstate the hole this check exists to close.
func checkRelayOwnership(origSender *bridgev2.OrigSender, targetSenderMXID id.UserID) error {
	if origSender == nil {
		return nil
	}
	if targetSenderMXID == "" || targetSenderMXID != origSender.UserID {
		return errRelayNotYourContent
	}
	return nil
}
