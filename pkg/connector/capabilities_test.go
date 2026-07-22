package connector

import (
	"context"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/event"
)

// portalWithMeta builds a bare *bridgev2.Portal carrying the given
// PortalMetadata, matching the construction pattern chatinfo_test.go already
// uses for ExtraUpdater tests.
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
	// Reply must ALSO stay fully supported in a threaded space: Google
	// Chat's wire protocol composes reply_to with a thread message (the
	// send_message RPC sets message_info.reply_to on both the threaded and
	// flat request branches), and a plain reply to a standalone (non-thread)
	// message is sent as a genuine cross-topic reply with no thread at all.
	// Forcing Reply=Unsupported here would make bridgev2 unconditionally
	// discard every reply pointer and always start a new thread instead --
	// see capabilities.go's gchatCapsThreaded doc comment.
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

// TestGetCapabilitiesLegacyThreadsEnabledWithoutThreadsOnlyAdvertisesThread
// pins a fix: a legacy
// topic-threaded space (ThreadsEnabled without the 2023+ ThreadsOnly model)
// must ALSO advertise Thread -- an earlier revision of roomFeatures keyed
// purely off ThreadsOnly and routed these rooms exactly like a flat one,
// which silently made every genuine GC thread reply in such a room
// unreachable via bridgev2's own outbound resolution (mautrix-go
// bridgev2/portal.go:1235-1244 never extracts relatesTo.GetThreadParent()
// into MatrixMessage.ThreadRoot unless caps.Thread.Partial()), so Task 6's
// create_message routing (handlematrix.go's sendThreadedMessage) could
// never be reached for this room type at all.
//
// These two room types are NOT distinguished at all for outbound routing
// purposes: the whole thread_id computation is gated on threads_enabled (=
// flat_threads_enabled OR threads_only), the SAME boolean for both the 2023+
// threads-only model and legacy flat-with-threads-enabled spaces --
// ThreadsOnly only matters separately for the INBOUND self-referencing
// head-message case (pkg/msgconv/from-gchat.go's ToMatrix, deliberately NOT
// changed by this fix).
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

// TestGetCapabilitiesNeitherFlagSetIsFlat: a portal with BOTH ThreadsOnly
// and ThreadsEnabled false (a genuinely non-threaded space, or a portal
// whose chat_info sync hasn't run yet) must still be flat -- the
// ThreadsEnabled fix above must not accidentally widen to "always
// threaded".
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
	// A DM has no space/thread concept in Google Chat -- its PortalMetadata
	// never sets ThreadsOnly, so it must fall into the flat/Reply branch
	// exactly like a flat space.
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
	// A typed-nil *PortalMetadata boxed into the Metadata `any` field is a
	// distinct case from an untyped-nil interface (portalWithMeta(nil)
	// above): the type assertion `meta, ok := portal.Metadata.(*PortalMetadata)`
	// succeeds with ok=true and meta==nil here, so roomFeatures' `meta !=
	// nil` guard must catch it before dereferencing meta.ThreadsOnly.
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

// TestGetCapabilitiesFormattingSupportedKeys asserts every formatting
// feature the gchatfmt/matrixfmt pair actually implements (bold, italic,
// underline, strikethrough, inline code, code block, hyperlink, user
// mention, @room, foreground color) is advertised as fully supported.
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

// TestGetCapabilitiesFormattingUnsupportedKeysNotClaimed asserts formatting
// features matrixfmt/gchatfmt do NOT genuinely support (blockquote is a
// lossy "> " text-prefix best-effort, headers become "#### " + bold text,
// spoiler is silently dropped -- see matrixfmt/html.go's blockquoteToString,
// headerToString, and spanToString's "data-mx-spoiler is intentionally
// ignored" comment) are never advertised as fully (or even partially)
// supported.
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

// TestGetCapabilitiesEditFullySupportedNoLimits pins the Edit capability
// wiring: Edit must be fully supported (otherwise bridgev2's own
// handleMatrixEdit, mautrix-go bridgev2/portal.go:1530-1532, drops every
// Matrix edit before ever calling HandleMatrixEdit), with no
// EditMaxCount/EditMaxAge limit -- neither is ever enforced.
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

// TestGetCapabilitiesM4FeaturesFullySupported pins the
// fix for gchatCapsFlat (inherited by gchatCapsThreaded via Clone()): every
// capability -- delete, reaction, read receipts, and typing notifications --
// must be advertised as supported
// in BOTH threaded and flat portals, exactly like Edit already was.
// Before this fix, capability-honoring Matrix clients that read
// com.beeper.room_features would hide reaction/redact/receipt/typing UI in
// every Google Chat room even though all four are fully implemented.
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

// TestGetCapabilitiesReactionUnrestricted pins that Google Chat's per-emoji
// (not one-per-user) reaction model is advertised as unrestricted: no
// ReactionCount cap (handlereaction.go's MaxReactions is likewise left at 0)
// and no AllowedReactions allow-list, since any Unicode emoji is valid.
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

// TestGetCapabilitiesFileMediaTypesFullySupported pins the outbound
// media capability wiring: without a File map entry for a given msgtype,
// bridgev2's own checkMessageContentCaps (mautrix-go bridgev2/portal.go)
// rejects the message with ErrUnsupportedMessageType BEFORE
// HandleMatrixMessage is ever called -- so this map must advertise exactly
// the four msgtypes isOutboundMediaMsgType (media.go) accepts, with
// captions fully supported (this bridge keeps both the file and a genuine
// caption, never drops either).
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

// TestGetCapabilitiesFileVoiceAndGIFSupported pins the voice/GIF fix:
// bridgev2's checkMessageContentCaps keys caps.File lookups on
// content.GetCapMsgType() (mautrix-go event/message.go:155-176), which
// promotes an m.audio MSC3245 voice message to CapMsgVoice and an m.video
// GIF to CapMsgGIF -- so without explicit CapMsgVoice/CapMsgGIF entries,
// bridgev2 hard-rejects both (voice messages from Element are common) even
// though Google Chat's generic /uploads endpoint accepts the bytes fine and
// isOutboundMediaMsgType (switching on the raw m.audio/m.video) accepts them.
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

// TestGetCapabilitiesFileDoesNotClaimSticker pins that event.CapMsgSticker
// is deliberately absent -- see isOutboundMediaMsgType's doc comment
// (media.go) for why: Google Chat's upload pipeline has no sticker concept,
// and this bridge's inbound half never produces one either.
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
