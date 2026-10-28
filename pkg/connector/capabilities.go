package connector

import (
	"context"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// MaxTextLength is the outgoing text length limit; Google Chat's real limit is undocumented.
const MaxTextLength = 4096

// gchatFormatting lists the formatting features with a real Google Chat annotation;
// blockquotes, headers, spoilers and ordered lists only have lossy text fallbacks.
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

// gchatFileFeatures needs the "*/*" entry: without a catch-all, unmatched mimes are rejected.
var gchatFileFeatures = &event.FileFeatures{
	MimeTypes: map[string]event.CapabilitySupportLevel{
		"*/*": event.CapLevelFullySupported,
	},
	Caption: event.CapLevelFullySupported,
}

// gchatFile must list CapMsgVoice and CapMsgGIF explicitly: bridgev2 checks GetCapMsgType, which
// promotes voice m.audio and GIF m.video. Keep in sync with isOutboundMediaMsgType.
var gchatFile = event.FileFeatureMap{
	event.MsgImage:    gchatFileFeatures,
	event.MsgVideo:    gchatFileFeatures,
	event.MsgAudio:    gchatFileFeatures,
	event.MsgFile:     gchatFileFeatures,
	event.CapMsgVoice: gchatFileFeatures,
	event.CapMsgGIF:   gchatFileFeatures,
}

// gchatCapsFlat is used for DMs and non-threaded spaces. Zero-valued limits mean unlimited.
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

// gchatCapsThreaded is used whenever ThreadsEnabled or ThreadsOnly is set. Reply stays fully
// supported alongside Thread, otherwise bridgev2 drops every reply pointer.
var gchatCapsThreaded *event.RoomFeatures

// gchatCapsSpace / gchatCapsSpaceThreaded add membership and name/topic state, which DMs
// cannot support.
var (
	gchatCapsSpace         *event.RoomFeatures
	gchatCapsSpaceThreaded *event.RoomFeatures
)

func init() {
	gchatCapsThreaded = gchatCapsFlat.Clone()
	gchatCapsThreaded.Thread = event.CapLevelFullySupported

	gchatCapsSpace = gchatCapsFlat.Clone()
	gchatCapsSpace.MemberActions = event.MemberFeatureMap{
		event.MemberActionInvite: event.CapLevelFullySupported,
		event.MemberActionKick:   event.CapLevelFullySupported,
		event.MemberActionLeave:  event.CapLevelFullySupported,
	}
	gchatCapsSpace.State = event.StateFeatureMap{
		event.StateRoomName.Type: {Level: event.CapLevelFullySupported},
		event.StateTopic.Type:    {Level: event.CapLevelFullySupported},
	}

	gchatCapsSpaceThreaded = gchatCapsSpace.Clone()
	gchatCapsSpaceThreaded.Thread = event.CapLevelFullySupported
}

// roomFeatures picks the RoomFeatures for portal, defaulting to gchatCapsFlat.
func roomFeatures(portal *bridgev2.Portal) *event.RoomFeatures {
	if portal == nil {
		return gchatCapsFlat
	}

	threaded := false
	if meta, ok := portal.Metadata.(*PortalMetadata); ok && meta != nil && (meta.ThreadsOnly || meta.ThreadsEnabled) {
		threaded = true
	}

	// An unparseable id falls back to the DM-safe set.
	isSpace := false
	if group, err := gcid.ParsePortalID(portal.ID); err == nil {
		isSpace = !group.IsDM
	}

	switch {
	case isSpace && threaded:
		return gchatCapsSpaceThreaded
	case isSpace:
		return gchatCapsSpace
	case threaded:
		return gchatCapsThreaded
	default:
		return gchatCapsFlat
	}
}

// GetCapabilities implements bridgev2.NetworkAPI.
func (c *GChatClient) GetCapabilities(_ context.Context, portal *bridgev2.Portal) *event.RoomFeatures {
	return roomFeatures(portal)
}
