// Package gcid defines the network ID formats for the Google Chat bridge.
//
// These values are permanent database contents and their format is FROZEN
// — do not change them after first deployment.
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
// login (the portal's receiver); spaces are global.
func MakePortalKey(g GroupID, receiver networkid.UserLoginID) networkid.PortalKey {
	key := networkid.PortalKey{ID: MakePortalID(g)}
	if g.IsDM {
		key.Receiver = receiver
	}
	return key
}

// MakeAttachmentPartID builds the part ID for the index-th UPLOAD_METADATA
// attachment of a message (media.go's convertAttachmentsToMatrix).
//
// KNOWN, DELIBERATELY UN-FIXED LEXICAL-SORT GAP: this format is NOT
// zero-padded, so "att_10" sorts lexically BEFORE "att_9" as a string. That
// matters because mautrix-go bridgev2's own database layer treats PartID as
// exactly that -- a sortable string, not an opaque token
// (networkid.PartID's doc comment, bridgev2/networkid/bridgeid.go: "If the
// part ID is not set, this should refer to the first part ID sorted
// alphabetically") -- and several of its DB queries pick "the last part of
// a message" via a literal `ORDER BY ... part_id DESC LIMIT 1`
// (bridgev2/database/message.go's getLastMessagePartByIDQuery/
// getLastMessageInThread/getLastMessagePartAtOrBeforeTimeQuery/
// getLastNonFakeMessagePartAtOrBeforeTimeQuery), reachable from this
// connector via GetLastPartByID (read-receipt/reaction target resolution
// when no specific part is targeted, portal.go:3696,3715) and
// GetLastThreadMessage (thread-reply anchoring, message.go:170-172, fed by
// msgconv/from-gchat.go's ThreadRoot handling). For a single message with
// 11+ UPLOAD_METADATA attachments (indices 0..10+), that DESC ordering
// picks "att_9" -- lexically greatest among any single/double-digit
// att_<N> -- instead of the numerically-last part, misrouting the
// read-marker/thread-anchor to the wrong Matrix event.
//
// This lexical-sort risk is specific to bridgev2's own PartID-as-string
// contract: an INTEGER index column would sort numerically regardless of
// digit count, but this Go port must satisfy a string-sorted PartID
// contract instead. No attachment-count cap was found anywhere (Google
// Chat's own proto has an unbounded `repeated Annotation annotations`, and
// the purple reference documents no limit), so this is a real, if narrow
// (11+ same-message attachments, a single send/paste of 11+ files at once),
// reachable gap -- not a false alarm.
//
// gcid formats are FROZEN (package doc comment) -- zero-padding this format
// now would be an ad hoc migration decision without the scrutiny a real
// format change deserves, so this is intentionally left AS-IS and flagged
// rather than silently changed: the messages + reactions migration
// (index->part_id) is where the legacy integer index column gets translated
// into this exact string format for the very first time for migrated rows,
// making it the natural, well-timed place to decide (before any real
// deployment accumulates 11+-attachment messages under the current scheme)
// whether to zero-pad (e.g. "att_%04d") going forward.
func MakeAttachmentPartID(index int) networkid.PartID {
	return networkid.PartID(fmt.Sprintf("att_%d", index))
}
