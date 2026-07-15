package connector

// Per-portal event.RoomFeatures advertisement (M3 Task 5). bridgev2 uses
// this to pre-validate outgoing Matrix events -- e.g. portal.go's
// checkMessageContentCaps rejects/downgrades formatting the Formatting map
// doesn't claim, and its reply/thread routing (see roomFeatures' doc
// comment below) decides whether a Matrix reply becomes a GC reply or a new
// GC thread purely from caps.Reply/caps.Thread.
//
// Mirrors mautrix-meta's own capabilities.go idiom (package-level singleton
// *event.RoomFeatures values built once via Clone(), picked per-portal by a
// cheap switch) rather than allocating a fresh RoomFeatures on every call.

import (
	"context"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"
)

// MaxTextLength is the outgoing text length bridgev2 will accept before
// truncating/rejecting a message. Google Chat's own server-side limit isn't
// documented in this project's protocol research (docs/research), so this
// keeps M2's original stub value.
const MaxTextLength = 4096

// gchatFormatting lists every event.FormattingFeature this bridge's
// pkg/msgconv/gchatfmt + pkg/msgconv/matrixfmt pair actually implements as a
// real, lossless (or Google-Chat-native) conversion -- ported per
// docs/superpowers/plans/2026-07-15-m3-formatting-threads.md Tasks 1-4:
//
//   - FmtBold/FmtItalic/FmtUnderline/FmtStrikethrough: FORMAT_DATA
//     BOLD/ITALIC/UNDERLINE/STRIKE both ways (matrixfmt/tags.go's Style
//     type, gchatfmt/convert.go's renderFormat).
//   - FmtInlineCode/FmtCodeBlock: FORMAT_DATA MONOSPACE/MONOSPACE_BLOCK
//     both ways (<code>/<pre>).
//   - FmtUserLink: a matrix.to user pill -> USER_MENTION/MENTION and back
//     to a ghost pill (matrixfmt/html.go's linkToString, gchatfmt's
//     renderMention).
//   - FmtInlineLink: <a href> -> a URL annotation carrying the href, both
//     ways (matrixfmt/tags.go's URL type -- the fix for the megabridge
//     URL-loss bug).
//   - FmtAtRoomMention: literal "@room" <-> USER_MENTION/MENTION_ALL
//     ("@all") both ways.
//   - FmtTextForegroundColor: data-mx-color/color=/style="color:" <->
//     FORMAT_DATA FONT_COLOR via the (rgb+2^31)&0xFFFFFF transform, both
//     ways (matrixfmt/html.go's colorAttribute+colorToFontColor,
//     gchatfmt's FONT_COLOR case).
//   - FmtUnorderedList: FORMAT_DATA BULLETED_LIST/BULLETED_LIST_ITEM both
//     ways (matrixfmt/html.go's unorderedListToString + tags.go's
//     StyleList/StyleListItem, gchatfmt/convert.go's renderFormat) -- a
//     real annotation, unlike FmtOrderedList below.
//
// Deliberately NOT claimed here (left absent, which bridgev2 treats as
// CapLevelUnsupported -- "may have a fallback" -- the accurate default for
// all of these, since none of them loses the message, just its structure):
//
//   - FmtBlockquote: matrixfmt's blockquoteToString renders a lossy literal
//     "> " text prefix per line, no GC annotation at all.
//   - FmtHeaders: headerToString renders a literal "#### " prefix wrapped
//     in bold, no GC heading concept exists.
//   - FmtSpoiler: spanToString's doc comment states data-mx-spoiler "is
//     intentionally ignored" -- rendered as plain unmarked text, Google
//     Chat has no spoiler concept.
//   - FmtOrderedList: orderedListToString is pure "N. " text-prefix, no GC
//     annotation ("Google Chat has no ordered-list format type").
//   - FmtSyntaxHighlighting, FmtTextBackgroundColor, FmtRoomLink,
//     FmtEventLink, FmtTable, FmtMath, FmtSuperscript, FmtSubscript,
//     FmtDetailsSummary, FmtHorizontalLine, FmtCustomEmoji,
//     FmtListStart/FmtListJumpValue: no GC wire representation exists at
//     all for any of these; gchatfmt/matrixfmt never produce or expect
//     them.
var gchatFormatting = event.FormattingFeatureMap{
	event.FmtBold:                event.CapLevelFullySupported,
	event.FmtItalic:              event.CapLevelFullySupported,
	event.FmtUnderline:           event.CapLevelFullySupported,
	event.FmtStrikethrough:       event.CapLevelFullySupported,
	event.FmtInlineCode:          event.CapLevelFullySupported,
	event.FmtCodeBlock:           event.CapLevelFullySupported,
	event.FmtUserLink:            event.CapLevelFullySupported,
	event.FmtInlineLink:          event.CapLevelFullySupported,
	event.FmtAtRoomMention:       event.CapLevelFullySupported,
	event.FmtTextForegroundColor: event.CapLevelFullySupported,
	event.FmtUnorderedList:       event.CapLevelFullySupported,
}

// gchatCapsFlat is advertised for every portal that is not a 2023+
// "threads only" space: plain (non-threaded) spaces, legacy
// topic-threading-enabled spaces (PortalMetadata.ThreadsEnabled without
// ThreadsOnly -- M3 Task 6's routing keys off ThreadsOnly alone, so these
// are routed exactly like a flat room), and DMs (which never have a thread
// concept in Google Chat at all). Reply is fully supported; Thread is left
// at its zero value (CapLevelUnsupported), which is what makes bridgev2's
// portal.go route a Matrix reply as caps.Reply.Partial() rather than
// starting a new GC thread.
var gchatCapsFlat = &event.RoomFeatures{
	Formatting:    gchatFormatting,
	MaxTextLength: MaxTextLength,
	Reply:         event.CapLevelFullySupported,
}

// gchatCapsThreaded is advertised for a threaded space
// (PortalMetadata.ThreadsOnly == true, the 2023+ "threads only" model).
// Both Thread AND Reply are fully supported here -- they are NOT mutually
// exclusive on Google Chat's wire protocol: maugclib/client.py's
// send_message sets message_info.reply_to on BOTH the threaded
// (CreateMessageRequest, parent_id.topic_id set) and flat
// (CreateTopicRequest) branches, and the Python bridge's own
// handle_matrix_message (portal.py:891-907) composes them per-message:
//   - an explicit Matrix thread reply always becomes a GC thread message,
//     and ALSO keeps any additional non-fallback reply pointer;
//   - a plain (non-thread) Matrix reply whose target is itself already
//     inside a GC thread gets redirected into that thread (dropping the
//     reply pointer, since GC groups it structurally instead);
//   - a plain Matrix reply to a standalone/top-level message stays a
//     genuine cross-topic quote-reply, with NO thread at all.
//
// Setting Reply=Unsupported here (an earlier revision of this file did,
// mirroring a misreading of "reply-in-thread auto-conversion is bridgev2's
// job") would make bridgev2's generic HandleMatrixMessage
// (bridgev2/portal.go) unconditionally discard every reply pointer via its
// `if !caps.Reply.Partial() { replyTo = nil }` and always start a brand
// new thread, even for the common "quote-reply to a top-level message"
// case Python instead sends as a flat cross-topic reply. Keeping both
// capabilities fully supported instead matches mautrix-meta's own
// precedent (metaCapsWithThreads keeps Reply fully supported alongside
// Thread) and lets bridgev2 hand Task 6/7's connector code BOTH ThreadRoot
// and ReplyTo when relevant, so the exact portal.py:891-907 composition
// logic above can be replicated precisely at the point that actually
// builds the CreateMessageRequest/CreateTopicRequest (M3 Task 6/7), rather
// than being lossily approximated by this generic per-portal toggle.
var gchatCapsThreaded *event.RoomFeatures

func init() {
	gchatCapsThreaded = gchatCapsFlat.Clone()
	gchatCapsThreaded.Thread = event.CapLevelFullySupported
}

// roomFeatures picks the RoomFeatures singleton for portal, reading
// PortalMetadata.ThreadsOnly (set by chatinfo.go's threadingExtraUpdater,
// M1 Task 12) nil-safely: a nil portal, nil Metadata, or a Metadata of an
// unexpected type all fall back to gchatCapsFlat, the same default a brand
// new portal (metadata not yet synced) gets.
func roomFeatures(portal *bridgev2.Portal) *event.RoomFeatures {
	if portal == nil {
		return gchatCapsFlat
	}
	if meta, ok := portal.Metadata.(*PortalMetadata); ok && meta != nil && meta.ThreadsOnly {
		return gchatCapsThreaded
	}
	return gchatCapsFlat
}

// GetCapabilities implements bridgev2.NetworkAPI.GetCapabilities (formerly
// the client.go stub that only ever returned MaxTextLength). ctx is unused
// -- capability selection needs no I/O, only the already-loaded portal.
func (c *GChatClient) GetCapabilities(_ context.Context, portal *bridgev2.Portal) *event.RoomFeatures {
	return roomFeatures(portal)
}
