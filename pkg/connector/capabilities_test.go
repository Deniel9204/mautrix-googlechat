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
	// Chat's wire protocol composes reply_to with a thread message
	// (maugclib/client.py's send_message sets message_info.reply_to on
	// both the threaded and flat request branches), and Python's own
	// handle_matrix_message (portal.py:891-907) sends a plain reply to a
	// standalone (non-thread) message as a genuine cross-topic reply with
	// no thread at all. Forcing Reply=Unsupported here would make
	// bridgev2 unconditionally discard every reply pointer and always
	// start a new thread instead -- see capabilities.go's gchatCapsThreaded
	// doc comment.
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

func TestGetCapabilitiesLegacyThreadsEnabledWithoutThreadsOnlyStillFlat(t *testing.T) {
	// ThreadsEnabled (legacy topic-based threading) without ThreadsOnly is
	// still a flat room for reply/thread routing purposes -- only the 2023+
	// threaded-space model (ThreadsOnly) switches the portal into Thread
	// mode; M3 Task 6's routing is keyed off ThreadsOnly alone.
	portal := portalWithMeta(&PortalMetadata{ThreadsOnly: false, ThreadsEnabled: true})

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
// feature M3's gchatfmt/matrixfmt pair actually implements (docs/superpowers/
// plans/2026-07-15-m3-formatting-threads.md Task 1-4: bold, italic,
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

func TestGetCapabilitiesFormattingSameAcrossThreadModes(t *testing.T) {
	flat := (&GChatClient{}).GetCapabilities(context.Background(), portalWithMeta(&PortalMetadata{ThreadsOnly: false}))
	threaded := (&GChatClient{}).GetCapabilities(context.Background(), portalWithMeta(&PortalMetadata{ThreadsOnly: true}))

	for key, level := range flat.Formatting {
		if threaded.Formatting[key] != level {
			t.Errorf("Formatting[%s]: flat=%v threaded=%v, want equal", key, level, threaded.Formatting[key])
		}
	}
}
