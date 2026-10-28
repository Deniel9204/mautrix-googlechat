package connector

// In relay mode all relayed users share one Google Chat account, so Google can't enforce
// authorship; the bridge checks the SenderMXID it recorded instead.
import (
	"errors"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

// errRelayNotYourContent is returned when a relayed user targets content they didn't send.
var errRelayNotYourContent = bridgev2.WrapErrorInStatus(errors.New("relayed user does not own the target content")).
	WithMessage("You can only edit or delete content you sent yourself through this relay").
	WithIsCertain(true).
	WithSendNotice(true).
	WithErrorReason(event.MessageStatusNoPermission)

// checkRelayOwnership allows the mutation unless relayed; an empty targetSenderMXID fails closed.
func checkRelayOwnership(origSender *bridgev2.OrigSender, targetSenderMXID id.UserID) error {
	if origSender == nil {
		return nil
	}
	if targetSenderMXID == "" || targetSenderMXID != origSender.UserID {
		return errRelayNotYourContent
	}
	return nil
}
