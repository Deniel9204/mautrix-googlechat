package matrixfmt

import (
	"fmt"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// BodyRangeValue is one formatting entity produced while walking a Matrix
// HTML tree (a bold span, a hyperlink, a mention, ...). Every concrete type
// below knows how to turn itself, plus a UTF-16 code-unit [start, start+
// length) span computed by the EntityString machinery in html.go, into a
// fully-populated *pb.Annotation -- mirroring how gc_message.py's
// GCEntity.__init__ builds a googlechat.Annotation from a (type, offset,
// length, extra_info) tuple.
//
// Every annotation produced by this package sets chip_render_type =
// DO_NOT_RENDER, matching gc_message.py:87,106,114 (GCEntity always sets
// this for FORMAT_DATA/USER_MENTION/URL) -- these are inline-formatting
// annotations we are asking Google Chat to apply to text that is already
// present, not link/upload preview chips (those are RENDER/
// RENDER_IF_POSSIBLE and out of scope until M5).
type BodyRangeValue interface {
	fmt.Stringer
	// Annotation builds the *pb.Annotation for this value at the given
	// UTF-16 code-unit start/length.
	Annotation(start, length int32) *pb.Annotation
}

// Style is a formatting annotation with no associated data beyond its
// FormatMetadata.format_type -- bold/italic/strike/underline/monospace/
// monospace-block/list/list-item. FontColor (which needs an associated RGB
// value) is its own type below; megabridge's single Style type could not
// carry a color value at all (docs/research/08d-megabridge-msgconv.md
// §2.1's "Style.Proto() cannot even carry a color value" finding) -- that
// gap is why FontColor is split out here instead.
type Style int

const (
	StyleBold Style = iota + 1
	StyleItalic
	StyleStrikethrough
	StyleUnderline
	StyleMonospace
	StyleMonospaceBlock
	StyleList
	StyleListItem
)

// formatType maps a Style to its wire FormatMetadata_FormatType via an
// explicit switch rather than relying on Style's iota ordering matching the
// proto enum's numbering (megabridge did the latter -- tags.go:53-59 in the
// reference tree -- and the audit flagged it "fragile-but-correct"; an
// explicit table removes that fragility at essentially no cost).
func (s Style) formatType() pb.FormatMetadata_FormatType {
	switch s {
	case StyleBold:
		return pb.FormatMetadata_BOLD
	case StyleItalic:
		return pb.FormatMetadata_ITALIC
	case StyleStrikethrough:
		return pb.FormatMetadata_STRIKE
	case StyleUnderline:
		return pb.FormatMetadata_UNDERLINE
	case StyleMonospace:
		return pb.FormatMetadata_MONOSPACE
	case StyleMonospaceBlock:
		return pb.FormatMetadata_MONOSPACE_BLOCK
	case StyleList:
		return pb.FormatMetadata_BULLETED_LIST
	case StyleListItem:
		return pb.FormatMetadata_BULLETED_LIST_ITEM
	default:
		return pb.FormatMetadata_TYPE_UNSPECIFIED
	}
}

func (s Style) String() string {
	return fmt.Sprintf("Style(%d)", s)
}

func (s Style) Annotation(start, length int32) *pb.Annotation {
	formatType := s.formatType()
	return &pb.Annotation{
		Type:           pb.AnnotationType_FORMAT_DATA.Enum(),
		StartIndex:     &start,
		Length:         &length,
		ChipRenderType: pb.Annotation_DO_NOT_RENDER.Enum(),
		Metadata: &pb.Annotation_FormatMetadata{
			FormatMetadata: &pb.FormatMetadata{FormatType: formatType.Enum()},
		},
	}
}

// FontColor is a FORMAT_DATA/FONT_COLOR annotation carrying the wire-encoded
// RGB value colorToFontColor (html.go) computed -- the exact inverse of
// gchatfmt's (rgb+2^31)&0xFFFFFF transform (pkg/msgconv/gchatfmt/convert.go's
// renderFormat, FONT_COLOR case), ported from
// mautrix_googlechat/formatter/from_matrix/parser.py:40-47's
// color_to_fstring.
type FontColor struct {
	RGB int32
}

func (c FontColor) String() string {
	return fmt.Sprintf("FontColor{RGB: %d}", c.RGB)
}

func (c FontColor) Annotation(start, length int32) *pb.Annotation {
	formatType := pb.FormatMetadata_FONT_COLOR
	rgb := c.RGB
	return &pb.Annotation{
		Type:           pb.AnnotationType_FORMAT_DATA.Enum(),
		StartIndex:     &start,
		Length:         &length,
		ChipRenderType: pb.Annotation_DO_NOT_RENDER.Enum(),
		Metadata: &pb.Annotation_FormatMetadata{
			FormatMetadata: &pb.FormatMetadata{FormatType: &formatType, FontColor: &rgb},
		},
	}
}

// Mention is a USER_MENTION/MENTION annotation for a specific Google Chat
// user, resolved from a Matrix pill's mxid via this package's
// MentionResolver seam. Ports gc_message.py:95-101's GCUserMentionType.MENTION
// branch (id + type, no display_name -- see html.go's linkToString doc
// comment for why the "@"+displayname text lives in the rendered string
// instead of UserMentionMetadata.display_name).
type Mention struct {
	GaiaID string
}

func (m Mention) String() string {
	return fmt.Sprintf("Mention{GaiaID: %s}", m.GaiaID)
}

func (m Mention) Annotation(start, length int32) *pb.Annotation {
	mentionType := pb.UserMentionMetadata_MENTION
	gaiaID := m.GaiaID
	return &pb.Annotation{
		Type:           pb.AnnotationType_USER_MENTION.Enum(),
		StartIndex:     &start,
		Length:         &length,
		ChipRenderType: pb.Annotation_DO_NOT_RENDER.Enum(),
		Metadata: &pb.Annotation_UserMentionMetadata{
			UserMentionMetadata: &pb.UserMentionMetadata{
				Id:   &pb.UserId{Id: &gaiaID},
				Type: &mentionType,
			},
		},
	}
}

// MentionAll is a USER_MENTION/MENTION_ALL annotation -- Google Chat's
// "@all", produced whenever a Matrix message's plain text contains the
// literal substring "@room" (parser.py:88-95's text_to_fstring override).
type MentionAll struct{}

func (MentionAll) String() string { return "MentionAll{}" }

func (MentionAll) Annotation(start, length int32) *pb.Annotation {
	mentionType := pb.UserMentionMetadata_MENTION_ALL
	return &pb.Annotation{
		Type:           pb.AnnotationType_USER_MENTION.Enum(),
		StartIndex:     &start,
		Length:         &length,
		ChipRenderType: pb.Annotation_DO_NOT_RENDER.Enum(),
		Metadata: &pb.Annotation_UserMentionMetadata{
			UserMentionMetadata: &pb.UserMentionMetadata{Type: &mentionType},
		},
	}
}

// URL is a URL annotation carrying the anchor's href. This is the type that
// did not exist at all in megabridge's matrixfmt (docs/research/08d
// §2.1/§6: "no URL annotation -- renders 'text (url)' plain text; zero
// UrlMetadata references in matrixfmt") -- its introduction, plus
// html.go's linkToString unconditionally using it for every non-mention,
// non-mailto anchor (including the "text equals href" case megabridge
// special-cased into silent loss), is the fix for the outgoing-hyperlink
// URL-loss bug this task exists to close.
type URL struct {
	Href string
}

func (u URL) String() string {
	return fmt.Sprintf("URL{Href: %s}", u.Href)
}

func (u URL) Annotation(start, length int32) *pb.Annotation {
	href := u.Href
	return &pb.Annotation{
		Type:           pb.AnnotationType_URL.Enum(),
		StartIndex:     &start,
		Length:         &length,
		ChipRenderType: pb.Annotation_DO_NOT_RENDER.Enum(),
		Metadata: &pb.Annotation_UrlMetadata{
			UrlMetadata: &pb.UrlMetadata{Url: &pb.Url{Url: &href}},
		},
	}
}
