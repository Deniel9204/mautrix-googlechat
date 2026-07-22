package connector

// Per-portal event.RoomFeatures advertisement. bridgev2 uses
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
// documented in this project's protocol research, so this keeps the original
// stub value.
const MaxTextLength = 4096

// gchatFormatting lists every event.FormattingFeature this bridge's
// pkg/msgconv/gchatfmt + pkg/msgconv/matrixfmt pair actually implements as a
// real, lossless (or Google-Chat-native) conversion:
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

// gchatFileFeatures is shared by every outbound msgtype gchatFile below
// advertises: Google Chat's /uploads endpoint (gchatmeow.Client.UploadFile,
// pkg/gchatmeow/upload.go) is a single generic file-upload
// pipeline with no per-msgtype mime allow-list of its own -- it uploads
// whatever bytes/mime a Matrix media event carries unconditionally, with no
// format restriction. "*/*": FullySupported mirrors that permissiveness (event
// .FileFeatures.GetMimeSupport falls back to CapLevelRejected for anything
// not matched by an explicit entry when the map lacks a "*/*" catch-all, so
// this entry is required, not merely permissive flavor).
//
// Caption: FullySupported -- unlike megabridge's audio arm
// (event.CapLevelDropped, mautrix-meta style), this port's handlematrix.go
// media branch keeps BOTH the uploaded file AND a genuine caption's own
// text+formatting (via mergeAnnotations) for every media msgtype uniformly --
// there is no
// Google-Chat-side reason to treat audio differently the way Meta's own
// Messenger/WhatsApp integration does.
//
// No MaxSize/MaxCaptionLength cap is set (both zero -- "unlimited"): this
// project's protocol research does not document any server-side limit Google
// Chat's own upload endpoint enforces, and adding
// an invented number here would be pure guesswork.
var gchatFileFeatures = &event.FileFeatures{
	MimeTypes: map[string]event.CapabilitySupportLevel{
		"*/*": event.CapLevelFullySupported,
	},
	Caption: event.CapLevelFullySupported,
}

// gchatFile is this bridge's outbound File capability map: the
// msgtypes handlematrix.go's HandleMatrixMessage media branch actually
// accepts (isOutboundMediaMsgType, media.go) -- image/video/audio/file.
// bridgev2 rejects any msgtype missing from this map (mautrix-go
// bridgev2/portal.go's checkMessageContentCaps) with ErrUnsupportedMessageType
// BEFORE HandleMatrixMessage is ever called, keyed on
// content.GetCapMsgType() -- NOT the raw MsgType. That distinction is why
// CapMsgVoice and CapMsgGIF must be listed EXPLICITLY here alongside the
// four base types: GetCapMsgType (mautrix-go event/message.go:155-176)
// promotes an m.audio carrying an MSC3245 `org.matrix.msc3245.voice` marker
// to CapMsgVoice, and an m.video whose Info.MauGIF is set to CapMsgGIF, so a
// map with only event.MsgAudio/event.MsgVideo would make bridgev2
// hard-reject a voice message (common from Element) or an animated GIF even
// though isOutboundMediaMsgType (which switches on the RAW m.audio/m.video
// MsgType, unaffected by the cap-type promotion) accepts them fine and
// Google Chat's generic /uploads endpoint takes the bytes without caring
// what kind of media they are. event.CapMsgSticker is still deliberately
// absent (see isOutboundMediaMsgType's doc comment). This map and
// isOutboundMediaMsgType's own switch must be kept in sync.
var gchatFile = event.FileFeatureMap{
	event.MsgImage:    gchatFileFeatures,
	event.MsgVideo:    gchatFileFeatures,
	event.MsgAudio:    gchatFileFeatures,
	event.MsgFile:     gchatFileFeatures,
	event.CapMsgVoice: gchatFileFeatures,
	event.CapMsgGIF:   gchatFileFeatures,
}

// gchatCapsFlat is advertised for every portal with no topic-threading
// concept at all: plain (non-threaded) spaces and DMs (which never have a
// thread concept in Google Chat). Reply is fully supported; Thread is left
// at its zero value (CapLevelUnsupported), which is what makes bridgev2's
// portal.go route a Matrix reply as caps.Reply.Partial() rather than
// starting a new GC thread.
// Edit is fully supported with no age/count limit: there is no check on how
// old the target message is or how many times it has already been edited --
// unlike mautrix-meta's Messenger capabilities (EditMaxCount: 5, EditMaxAge:
// 15m), which mirror real Messenger-imposed server limits that have no Google
// Chat equivalent in this project's protocol research.
// Leaving EditMaxCount/EditMaxAge at their zero values means "unlimited",
// rather than inventing a limit Google Chat never enforced.
//
// Delete is fully supported: handleredact.go's HandleMatrixMessageRemove
// calls delete_message unconditionally, with no age/count limit of its own
// to advertise -- mirroring Edit's own "leave the
// limit fields at zero" reasoning above. DeleteForMe is left at
// its zero value (false): Google Chat has no delete-for-me-only concept
// distinct from a real delete_message, unlike Messenger/WhatsApp.
//
// Reaction is fully supported, with NO count/allow-list restriction:
// handlereaction.go's own top-of-file doc comment explains Google Chat
// reactions are per-emoji (not one-per-user), so a single sender may apply
// any number of distinct emoji simultaneously -- ReactionCount is left at
// its zero value (0 = unlimited, mirroring MaxReactions: 0 in
// PreHandleMatrixReaction) and AllowedReactions stays nil (unrestricted).
// CustomEmojiReactions is left at its zero value (false): Google Chat's own
// wire form (proto Emoji.unicode) never carries a custom/non-Unicode emoji,
// so there is nothing for this connector to advertise support for.
//
// ReadReceipts is true: handlereceipt.go's HandleMatrixReadReceipt calls
// mark_group_readstate, and events.go's queueReadReceiptChanged/
// queueGroupViewed deliver GC's own read state back to Matrix -- both
// directions are wired.
//
// TypingNotifications is true: handletyping.go's HandleMatrixTyping calls
// mark_typing, and events.go's queueTypingStateChanged (this file's sibling)
// delivers GC's own typing state back to Matrix -- both directions are wired.
//
// File is gchatFile: image/video/audio/file all fully
// supported, with captions kept alongside the file rather than dropped --
// see gchatFileFeatures/gchatFile's own doc comments above. This is
// advertised unconditionally, even though Config.DisableOutboundMedia or a
// live #114 upload failure can still turn a specific send into a clean
// rejection (handlematrix.go) -- bridgev2's own RoomFeatures has no
// "conditionally supported" level for that distinction, and it would be
// wrong to make the CAPABILITY itself config-dependent: the send path is
// genuinely implemented and correct, independent of whether THIS operator's
// account happens to be hitting the upstream outage.
var gchatCapsFlat = &event.RoomFeatures{
	Formatting:          gchatFormatting,
	File:                gchatFile,
	MaxTextLength:       MaxTextLength,
	Reply:               event.CapLevelFullySupported,
	Edit:                event.CapLevelFullySupported,
	Delete:              event.CapLevelFullySupported,
	Reaction:            event.CapLevelFullySupported,
	ReadReceipts:        true,
	TypingNotifications: true,
}

// gchatCapsThreaded is advertised for any portal with PortalMetadata.
// ThreadsEnabled == true -- BOTH the 2023+ "threads only" model
// (ThreadsOnly) AND legacy topic-threading-enabled spaces
// (flat_threads_enabled, ThreadsEnabled without ThreadsOnly). These two room
// types are NOT distinguished for outbound routing purposes at all: the
// entire thread_id computation is gated on the single threads_enabled
// boolean (= flat_threads_enabled OR threads_only) -- ThreadsOnly only
// matters separately for the INBOUND self-referencing head-message case
// (pkg/msgconv/from-gchat.go's ToMatrix). An earlier revision of
// this file keyed roomFeatures purely off ThreadsOnly, silently making
// every genuine GC thread reply in a legacy (non-ThreadsOnly)
// threads-enabled room unreachable via bridgev2's own outbound resolution
// (mautrix-go bridgev2/portal.go:1235-1244 never extracts
// relatesTo.GetThreadParent() into MatrixMessage.ThreadRoot unless
// caps.Thread.Partial()).
//
// Both Thread AND Reply are fully supported here -- they are NOT mutually
// exclusive on Google Chat's wire protocol: the send_message RPC sets
// message_info.reply_to on BOTH the threaded (CreateMessageRequest,
// parent_id.topic_id set) and flat (CreateTopicRequest) branches, and they
// compose per-message:
//   - an explicit Matrix thread reply always becomes a GC thread message,
//     and ALSO keeps any additional non-fallback reply pointer;
//   - a plain (non-thread) Matrix reply whose target is itself already
//     inside a GC thread gets redirected into that thread, dropping the
//     reply pointer (reply_to = None), since GC groups it structurally
//     instead;
//   - a plain Matrix reply to a standalone/top-level message stays a
//     genuine cross-topic quote-reply, with NO thread at all.
//
// Setting Reply=Unsupported here (an earlier revision of this file did,
// mirroring a misreading of "reply-in-thread auto-conversion is bridgev2's
// job") would make bridgev2's generic HandleMatrixMessage
// (bridgev2/portal.go) unconditionally discard every reply pointer via its
// `if !caps.Reply.Partial() { replyTo = nil }` and always start a brand
// new thread, even for the common "quote-reply to a top-level message"
// case, which should instead stay a flat cross-topic reply. Keeping both
// capabilities fully supported instead matches mautrix-meta's own
// precedent (metaCapsWithThreads keeps Reply fully supported alongside
// Thread) and lets bridgev2 hand the connector code BOTH ThreadRoot
// and ReplyTo when relevant.
//
// buildReplyTarget (handlematrix.go) deliberately does NOT
// replicate the second bullet's "dropping the reply pointer" clause,
// though: rather than explicitly clearing reply_to when rerouting a plain
// reply into an existing thread, bridgev2's own generic auto-derivation
// (mautrix-go bridgev2/portal.go:1248-1273) only clears ReplyTo when the
// connector's Reply capability is NOT supported (`if !caps.Reply.Partial()
// { replyTo = nil }`) -- which never applies here, since Reply stays fully
// supported alongside Thread per this doc comment's own reasoning above.
// So this connector's ThreadRoot+ReplyTo composition is a strict superset of
// the three bullets above: the first and third are replicated precisely
// (see buildReplyTarget's own doc comment and
// TestHandleMatrixMessageReplyAndThreadBothSet), while the second bullet's
// case keeps its quote-reply decoration instead of losing it -- a
// deliberate, tested deviation, not a
// gap: the resulting SendReplyTarget is well-formed either way, since
// buildReplyTarget's "thread_id or reply_to" fallback means a reply
// auto-rerouted into a thread gets the SAME nested topic_id a genuine
// explicit-thread-reply would.
var gchatCapsThreaded *event.RoomFeatures

func init() {
	gchatCapsThreaded = gchatCapsFlat.Clone()
	gchatCapsThreaded.Thread = event.CapLevelFullySupported
}

// roomFeatures picks the RoomFeatures singleton for portal, reading
// PortalMetadata.ThreadsOnly || ThreadsEnabled (both set by chatinfo.go's
// threadingExtraUpdater) nil-safely: a nil portal, nil
// Metadata, or a Metadata of an unexpected type all fall back to
// gchatCapsFlat, the same default a brand new portal (metadata not yet
// synced) gets. The explicit `||` (rather than relying on ThreadsEnabled
// alone, which chatinfo.go always computes as `flat_threads_enabled ||
// threads_only` and so is already a superset of ThreadsOnly in practice)
// matches the threads_enabled = flat_threads_enabled OR threads_only formula
// literally, so this stays correct even if the two
// fields were ever set independently (e.g. directly in a test) rather than
// through chatinfo.go's normal sync path; see gchatCapsThreaded's doc
// comment for why both room types get the same RoomFeatures.
func roomFeatures(portal *bridgev2.Portal) *event.RoomFeatures {
	if portal == nil {
		return gchatCapsFlat
	}
	if meta, ok := portal.Metadata.(*PortalMetadata); ok && meta != nil && (meta.ThreadsOnly || meta.ThreadsEnabled) {
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
