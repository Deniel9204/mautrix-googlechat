// Package gcid defines the network ID formats for the Google Chat bridge.
//
// These values are permanent database contents and must match the Python
// bridge's formats exactly (docs/research/07-gap-analysis.md §1.3) — do not
// change them after first deployment.
package gcid

import (
	"fmt"
	"strings"

	"maunium.net/go/mautrix/bridgev2/networkid"
)

const (
	dmPrefix    = "dm:"
	spacePrefix = "space:"

	// TextPartID is the part ID of the text body of a multi-part message.
	TextPartID networkid.PartID = ""
)

// GroupID is a parsed Google Chat conversation identifier.
type GroupID struct {
	ID   string
	IsDM bool
}

func MakeUserID(gaiaID string) networkid.UserID {
	return networkid.UserID(gaiaID)
}

func MakeUserLoginID(gaiaID string) networkid.UserLoginID {
	return networkid.UserLoginID(gaiaID)
}

func MakeMessageID(msgID string) networkid.MessageID {
	return networkid.MessageID(msgID)
}

func ParseMessageID(id networkid.MessageID) string {
	return string(id)
}

func MakePortalID(g GroupID) networkid.PortalID {
	if g.IsDM {
		return networkid.PortalID(dmPrefix + g.ID)
	}
	return networkid.PortalID(spacePrefix + g.ID)
}

func ParsePortalID(id networkid.PortalID) (GroupID, error) {
	s := string(id)
	if rest, ok := strings.CutPrefix(s, dmPrefix); ok && rest != "" {
		return GroupID{ID: rest, IsDM: true}, nil
	}
	if rest, ok := strings.CutPrefix(s, spacePrefix); ok && rest != "" {
		return GroupID{ID: rest, IsDM: false}, nil
	}
	return GroupID{}, fmt.Errorf("invalid portal ID %q", s)
}

// MakePortalKey builds the bridgev2 portal key. DMs are scoped to the owner
// login (matching the Python bridge's gc_receiver column); spaces are global.
func MakePortalKey(g GroupID, receiver networkid.UserLoginID) networkid.PortalKey {
	key := networkid.PortalKey{ID: MakePortalID(g)}
	if g.IsDM {
		key.Receiver = receiver
	}
	return key
}

func MakeAttachmentPartID(index int) networkid.PartID {
	return networkid.PartID(fmt.Sprintf("att_%d", index))
}
