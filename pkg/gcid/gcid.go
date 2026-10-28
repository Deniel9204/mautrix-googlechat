// Package gcid defines the bridge's network ID formats. They are FROZEN (stored in the DB).
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

// MakePortalKey builds the portal key; DMs are scoped to the receiver login, spaces are global.
func MakePortalKey(g GroupID, receiver networkid.UserLoginID) networkid.PortalKey {
	key := networkid.PortalKey{ID: MakePortalID(g)}
	if g.IsDM {
		key.Receiver = receiver
	}
	return key
}

// MakeAttachmentPartID builds the part ID for the index-th UPLOAD_METADATA attachment.
// Not zero-padded, so "att_10" sorts before "att_9" in bridgev2's part_id ordering (known, left as-is).
func MakeAttachmentPartID(index int) networkid.PartID {
	return networkid.PartID(fmt.Sprintf("att_%d", index))
}
