package connector

import (
	"context"
	"testing"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
)

func portalWithMeta(meta *PortalMetadata) *bridgev2.Portal {
	var m any
	if meta != nil {
		m = meta
	}
	return &bridgev2.Portal{Portal: &database.Portal{Metadata: m}}
}

func TestGetCapabilitiesThreadedSpaceAdvertisesThread(t *testing.T) {
	portal := portalWithMeta(&PortalMetadata{ThreadsOnly: true})

	caps := (&GChatClient{}).GetCapabilities(context.Background(), portal)

	if !caps.Thread.Full() {
		t.Errorf("Thread = %v, want fully supported", caps.Thread)
	}
	// Reply must stay supported alongside Thread, or bridgev2 drops every reply pointer.
	if !caps.Reply.Full() {
		t.Errorf("Reply = %v, want fully supported (GC composes reply_to with thread messages)", caps.Reply)
	}
}

func TestGetCapabilitiesFlatRoomAdvertisesReply(t *testing.T) {
	portal := portalWithMeta(&PortalMetadata{ThreadsOnly: false})

	caps := (&GChatClient{}).GetCapabilities(context.Background(), portal)

	if !caps.Reply.Full() {
		t.Errorf("Reply = %v, want fully supported", caps.Reply)
	}
	if caps.Thread.Partial() {
		t.Errorf("Thread = %v, want NOT partial/full for a flat room", caps.Thread)
	}
}

// TestGetCapabilitiesLegacyThreadsEnabledWithoutThreadsOnlyAdvertisesThread: legacy threaded
// spaces need Thread too, or bridgev2 never sets ThreadRoot.
func TestGetCapabilitiesLegacyThreadsEnabledWithoutThreadsOnlyAdvertisesThread(t *testing.T) {
	portal := portalWithMeta(&PortalMetadata{ThreadsOnly: false, ThreadsEnabled: true})

	caps := (&GChatClient{}).GetCapabilities(context.Background(), portal)

	if !caps.Reply.Full() {
		t.Errorf("Reply = %v, want fully supported", caps.Reply)
	}
	if !caps.Thread.Full() {
		t.Errorf("Thread = %v, want fully supported (ThreadsEnabled alone is the real threads_enabled gate)", caps.Thread)
	}
}

func TestGetCapabilitiesNeitherFlagSetIsFlat(t *testing.T) {
	portal := portalWithMeta(&PortalMetadata{ThreadsOnly: false, ThreadsEnabled: false})

	caps := (&GChatClient{}).GetCapabilities(context.Background(), portal)

	if !caps.Reply.Full() {
		t.Errorf("Reply = %v, want fully supported", caps.Reply)
	}
	if caps.Thread.Partial() {
		t.Errorf("Thread = %v, want NOT partial/full", caps.Thread)
	}
}

func TestGetCapabilitiesDMPerItsFlags(t *testing.T) {
	portal := portalWithMeta(&PortalMetadata{})

	caps := (&GChatClient{}).GetCapabilities(context.Background(), portal)

	if !caps.Reply.Full() {
		t.Errorf("Reply = %v, want fully supported for a DM", caps.Reply)
	}
	if caps.Thread.Partial() {
		t.Errorf("Thread = %v, want NOT partial/full for a DM", caps.Thread)
	}
}

func TestGetCapabilitiesNilMetadataDefaultsToFlat(t *testing.T) {
	portal := portalWithMeta(nil)

	caps := (&GChatClient{}).GetCapabilities(context.Background(), portal)

	if caps == nil {
		t.Fatal("GetCapabilities returned nil for nil metadata")
	}
	if !caps.Reply.Full() {
		t.Errorf("Reply = %v, want fully supported (sane default)", caps.Reply)
	}
	if caps.Thread.Partial() {
		t.Errorf("Thread = %v, want NOT partial/full (sane default)", caps.Thread)
	}
	if caps.MaxTextLength != 4096 {
		t.Errorf("MaxTextLength = %d, want 4096", caps.MaxTextLength)
	}
}

func TestGetCapabilitiesWrongMetadataTypeDoesNotPanic(t *testing.T) {
	portal := &bridgev2.Portal{Portal: &database.Portal{Metadata: "not a PortalMetadata"}}

	caps := (&GChatClient{}).GetCapabilities(context.Background(), portal)

	if caps == nil {
		t.Fatal("GetCapabilities returned nil for mistyped metadata")
	}
}

func TestGetCapabilitiesNilPortalDoesNotPanic(t *testing.T) {
	caps := roomFeatures(nil)

	if caps == nil {
		t.Fatal("roomFeatures(nil) returned nil")
	}
	if !caps.Reply.Full() {
		t.Errorf("Reply = %v, want fully supported (nil portal falls back to flat)", caps.Reply)
	}
}

func TestGetCapabilitiesTypedNilMetadataDoesNotPanic(t *testing.T) {
	// A typed nil passes the type assertion, so roomFeatures' nil check must catch it.
	var typedNil *PortalMetadata
	portal := &bridgev2.Portal{Portal: &database.Portal{Metadata: typedNil}}

	caps := (&GChatClient{}).GetCapabilities(context.Background(), portal)

	if caps == nil {
		t.Fatal("GetCapabilities returned nil for typed-nil metadata")
	}
	if !caps.Reply.Full() {
		t.Errorf("Reply = %v, want fully supported (typed-nil metadata falls back to flat)", caps.Reply)
	}
}

func TestGetCapabilitiesMaxTextLength(t *testing.T) {
	for _, threadsOnly := range []bool{true, false} {
		portal := portalWithMeta(&PortalMetadata{ThreadsOnly: threadsOnly})
		caps := (&GChatClient{}).GetCapabilities(context.Background(), portal)
		if caps.MaxTextLength != 4096 {
			t.Errorf("ThreadsOnly=%v: MaxTextLength = %d, want 4096", threadsOnly, caps.MaxTextLength)
		}
	}
}

func TestGetCapabilitiesFormattingSupportedKeys(t *testing.T) {
	supported := []event.FormattingFeature{
		event.FmtBold,
		event.FmtItalic,
		event.FmtUnderline,
		event.FmtStrikethrough,
		event.FmtInlineCode,
		event.FmtCodeBlock,
		event.FmtUserLink,
		event.FmtInlineLink,
		event.FmtAtRoomMention,
		event.FmtTextForegroundColor,
		event.FmtUnorderedList,
	}

	portal := portalWithMeta(&PortalMetadata{})
	caps := (&GChatClient{}).GetCapabilities(context.Background(), portal)

	for _, key := range supported {
		if level, ok := caps.Formatting[key]; !ok || !level.Full() {
			t.Errorf("Formatting[%s] = %v, ok=%v, want fully supported", key, level, ok)
		}
	}
}

// TestGetCapabilitiesFormattingUnsupportedKeysNotClaimed: blockquote, headers and spoiler are lossy.
func TestGetCapabilitiesFormattingUnsupportedKeysNotClaimed(t *testing.T) {
	unsupported := []event.FormattingFeature{
		event.FmtBlockquote,
		event.FmtHeaders,
		event.FmtSpoiler,
		event.FmtSyntaxHighlighting,
		event.FmtOrderedList,
		event.FmtTextBackgroundColor,
		event.FmtRoomLink,
		event.FmtEventLink,
		event.FmtTable,
		event.FmtMath,
	}

	portal := portalWithMeta(&PortalMetadata{})
	caps := (&GChatClient{}).GetCapabilities(context.Background(), portal)

	for _, key := range unsupported {
		if level := caps.Formatting[key]; level.Partial() {
			t.Errorf("Formatting[%s] = %v, want NOT partial/full (unclaimed)", key, level)
		}
	}
}

// TestGetCapabilitiesEditFullySupportedNoLimits: without Edit, bridgev2 drops every Matrix edit.
func TestGetCapabilitiesEditFullySupportedNoLimits(t *testing.T) {
	for _, threadsOnly := range []bool{true, false} {
		portal := portalWithMeta(&PortalMetadata{ThreadsOnly: threadsOnly})
		caps := (&GChatClient{}).GetCapabilities(context.Background(), portal)
		if !caps.Edit.Full() {
			t.Errorf("ThreadsOnly=%v: Edit = %v, want fully supported", threadsOnly, caps.Edit)
		}
		if caps.EditMaxCount != 0 {
			t.Errorf("EditMaxCount = %d, want 0 (unlimited, never enforced)", caps.EditMaxCount)
		}
		if caps.EditMaxAge != nil {
			t.Errorf("EditMaxAge = %v, want nil (unlimited)", caps.EditMaxAge)
		}
	}
}

func TestGetCapabilitiesM4FeaturesFullySupported(t *testing.T) {
	for _, threadsOnly := range []bool{true, false} {
		portal := portalWithMeta(&PortalMetadata{ThreadsOnly: threadsOnly})
		caps := (&GChatClient{}).GetCapabilities(context.Background(), portal)

		if !caps.Delete.Full() {
			t.Errorf("ThreadsOnly=%v: Delete = %v, want fully supported", threadsOnly, caps.Delete)
		}
		if !caps.Reaction.Full() {
			t.Errorf("ThreadsOnly=%v: Reaction = %v, want fully supported", threadsOnly, caps.Reaction)
		}
		if !caps.ReadReceipts {
			t.Errorf("ThreadsOnly=%v: ReadReceipts = %v, want true", threadsOnly, caps.ReadReceipts)
		}
		if !caps.TypingNotifications {
			t.Errorf("ThreadsOnly=%v: TypingNotifications = %v, want true", threadsOnly, caps.TypingNotifications)
		}
	}
}

func TestGetCapabilitiesReactionUnrestricted(t *testing.T) {
	portal := portalWithMeta(&PortalMetadata{})
	caps := (&GChatClient{}).GetCapabilities(context.Background(), portal)

	if caps.ReactionCount != 0 {
		t.Errorf("ReactionCount = %d, want 0 (unlimited)", caps.ReactionCount)
	}
	if caps.AllowedReactions != nil {
		t.Errorf("AllowedReactions = %v, want nil (unrestricted)", caps.AllowedReactions)
	}
}

// TestGetCapabilitiesFileMediaTypesFullySupported: a msgtype missing from File is rejected by bridgev2.
func TestGetCapabilitiesFileMediaTypesFullySupported(t *testing.T) {
	for _, threadsOnly := range []bool{true, false} {
		portal := portalWithMeta(&PortalMetadata{ThreadsOnly: threadsOnly})
		caps := (&GChatClient{}).GetCapabilities(context.Background(), portal)

		for _, msgtype := range []event.MessageType{event.MsgImage, event.MsgVideo, event.MsgAudio, event.MsgFile} {
			feat, ok := caps.File[msgtype]
			if !ok || feat == nil {
				t.Fatalf("ThreadsOnly=%v: File[%s] missing, want an entry (otherwise bridgev2 rejects it before HandleMatrixMessage)", threadsOnly, msgtype)
			}
			if !feat.Caption.Full() {
				t.Errorf("ThreadsOnly=%v: File[%s].Caption = %v, want fully supported", threadsOnly, msgtype, feat.Caption)
			}
			if lvl := feat.GetMimeSupport("application/octet-stream"); !lvl.Full() {
				t.Errorf("ThreadsOnly=%v: File[%s] MimeTypes has no */* catch-all (GetMimeSupport = %v)", threadsOnly, msgtype, lvl)
			}
		}
	}
}

// TestGetCapabilitiesFileVoiceAndGIFSupported: GetCapMsgType promotes voice and GIF, so both need entries.
func TestGetCapabilitiesFileVoiceAndGIFSupported(t *testing.T) {
	caps := (&GChatClient{}).GetCapabilities(context.Background(), portalWithMeta(&PortalMetadata{}))

	for _, capType := range []event.CapabilityMsgType{event.CapMsgVoice, event.CapMsgGIF} {
		feat, ok := caps.File[capType]
		if !ok || feat == nil {
			t.Fatalf("File[%s] missing, want an entry (otherwise bridgev2 rejects it before HandleMatrixMessage)", capType)
		}
		if lvl := feat.GetMimeSupport("application/octet-stream"); !lvl.Full() {
			t.Errorf("File[%s] has no */* catch-all (GetMimeSupport = %v)", capType, lvl)
		}
	}
}

func TestGetCapabilitiesFileDoesNotClaimSticker(t *testing.T) {
	caps := (&GChatClient{}).GetCapabilities(context.Background(), portalWithMeta(&PortalMetadata{}))

	if _, ok := caps.File[event.CapMsgSticker]; ok {
		t.Error("File[CapMsgSticker] is set, want absent (no sticker concept on Google Chat's upload pipeline)")
	}
}

func TestGetCapabilitiesFormattingSameAcrossThreadModes(t *testing.T) {
	flat := (&GChatClient{}).GetCapabilities(context.Background(), portalWithMeta(&PortalMetadata{ThreadsOnly: false}))
	threaded := (&GChatClient{}).GetCapabilities(context.Background(), portalWithMeta(&PortalMetadata{ThreadsOnly: true}))

	for key, level := range flat.Formatting {
		if threaded.Formatting[key] != level {
			t.Errorf("Formatting[%s]: flat=%v threaded=%v, want equal", key, level, threaded.Formatting[key])
		}
	}
}

func portalWithIDAndMeta(id string, isDM bool, meta *PortalMetadata) *bridgev2.Portal {
	var m any
	if meta != nil {
		m = meta
	}
	return &bridgev2.Portal{Portal: &database.Portal{
		PortalKey: networkid.PortalKey{ID: gcid.MakePortalID(gcid.GroupID{ID: id, IsDM: isDM})},
		Metadata:  m,
	}}
}

func TestGetCapabilitiesSpaceAdvertisesMemberActions(t *testing.T) {
	portal := portalWithIDAndMeta("space1", false, &PortalMetadata{})

	caps := (&GChatClient{}).GetCapabilities(context.Background(), portal)

	for _, action := range []event.MemberAction{event.MemberActionInvite, event.MemberActionKick, event.MemberActionLeave} {
		if !caps.MemberActions[action].Full() {
			t.Errorf("MemberActions[%s] = %v, want fully supported", action, caps.MemberActions[action])
		}
	}
	if _, present := caps.MemberActions[event.MemberActionBan]; present {
		t.Error("MemberActions advertises ban, which HandleMatrixMembership rejects")
	}
}

func TestGetCapabilitiesSpaceAdvertisesRoomNameAndTopic(t *testing.T) {
	portal := portalWithIDAndMeta("space1", false, &PortalMetadata{})

	caps := (&GChatClient{}).GetCapabilities(context.Background(), portal)

	for _, typ := range []string{event.StateRoomName.Type, event.StateTopic.Type} {
		if feature := caps.State[typ]; feature == nil || !feature.Level.Full() {
			t.Errorf("State[%s] = %v, want fully supported", typ, feature)
		}
	}
	// A space icon is an emoji; a Matrix image avatar cannot express it.
	if _, present := caps.State[event.StateRoomAvatar.Type]; present {
		t.Error("State advertises m.room.avatar, which is not implemented")
	}
}

// TestGetCapabilitiesDMOmitsSpaceOnlyActions: the handlers reject these actions in DMs.
func TestGetCapabilitiesDMOmitsSpaceOnlyActions(t *testing.T) {
	portal := portalWithIDAndMeta("dm1", true, &PortalMetadata{})

	caps := (&GChatClient{}).GetCapabilities(context.Background(), portal)

	if len(caps.MemberActions) != 0 {
		t.Errorf("MemberActions = %v for a DM, want none (they are rejected in DMs)", caps.MemberActions)
	}
	if len(caps.State) != 0 {
		t.Errorf("State = %v for a DM, want none (a DM has no name or description to change)", caps.State)
	}
	if !caps.Reply.Full() {
		t.Error("Reply = not full for a DM, want fully supported")
	}
}

func TestGetCapabilitiesThreadedSpaceKeepsMemberActions(t *testing.T) {
	portal := portalWithIDAndMeta("space1", false, &PortalMetadata{ThreadsOnly: true})

	caps := (&GChatClient{}).GetCapabilities(context.Background(), portal)

	if !caps.Thread.Full() {
		t.Error("Thread = not full for a threaded space, want fully supported")
	}
	if !caps.MemberActions[event.MemberActionInvite].Full() {
		t.Error("a threaded space lost MemberActions")
	}
	if f := caps.State[event.StateRoomName.Type]; f == nil || !f.Level.Full() {
		t.Error("a threaded space lost the m.room.name capability")
	}
}

func TestGetCapabilitiesUnparseablePortalIDOmitsSpaceOnlyActions(t *testing.T) {
	portal := &bridgev2.Portal{Portal: &database.Portal{
		PortalKey: networkid.PortalKey{ID: "not-a-valid-portal-id"},
		Metadata:  &PortalMetadata{},
	}}

	caps := (&GChatClient{}).GetCapabilities(context.Background(), portal)

	if len(caps.MemberActions) != 0 || len(caps.State) != 0 {
		t.Errorf("MemberActions=%v State=%v for an unparseable portal id, want neither", caps.MemberActions, caps.State)
	}
}
