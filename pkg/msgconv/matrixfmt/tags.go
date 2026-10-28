package matrixfmt

import (
	"fmt"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// BodyRangeValue is one formatting entity. All annotations produced here are
// DO_NOT_RENDER inline formatting, never preview chips.
type BodyRangeValue interface {
	fmt.Stringer
	// Annotation builds the annotation at the given UTF-16 code-unit span.
	Annotation(start, length int32) *pb.Annotation
}

// Style is a formatting annotation with only a format_type.
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

// formatType maps a Style to its wire format type explicitly, not via iota order.
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

// FontColor is a FONT_COLOR annotation with the wire-encoded RGB value.
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

// Mention is a USER_MENTION annotation; the "@Name" text lives in the body, not display_name.
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

// MentionAll is a MENTION_ALL ("@all") annotation, produced from "@room".
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

// URL is a URL annotation carrying the anchor's href.
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
