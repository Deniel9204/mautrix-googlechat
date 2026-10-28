package gchatfmt

import (
	"html"
	"unicode/utf16"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"strings"
)

// utf16Encode encodes s as UTF-16 code units (JS String indexing), the space annotation offsets use.
func utf16Encode(s string) []uint16 {
	return utf16.Encode([]rune(s))
}

// utf16Decode decodes UTF-16 code units; unpaired surrogates become U+FFFD.
func utf16Decode(units []uint16) string {
	return string(utf16.Decode(units))
}

// escapeUnits HTML-escapes UTF-16 code units as text content.
func escapeUnits(units []uint16) string {
	return html.EscapeString(utf16Decode(units))
}

// escapeAttr escapes s for an HTML attribute value (either quote style).
func escapeAttr(s string) string {
	return html.EscapeString(s)
}

// Test-fixture constructors. ChipRenderType must be set: unset decodes to UNKNOWN, not DO_NOT_RENDER.

// MakeFormatAnnotation builds a DO_NOT_RENDER FORMAT_DATA annotation.
func MakeFormatAnnotation(start, length int32, formatType pb.FormatMetadata_FormatType) *pb.Annotation {
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

// MakeFontColorAnnotation builds a FONT_COLOR annotation; rgb is the raw signed wire value.
func MakeFontColorAnnotation(start, length, rgb int32) *pb.Annotation {
	formatType := pb.FormatMetadata_FONT_COLOR
	return &pb.Annotation{
		Type:           pb.AnnotationType_FORMAT_DATA.Enum(),
		StartIndex:     &start,
		Length:         &length,
		ChipRenderType: pb.Annotation_DO_NOT_RENDER.Enum(),
		Metadata: &pb.Annotation_FormatMetadata{
			FormatMetadata: &pb.FormatMetadata{
				FormatType: &formatType,
				FontColor:  &rgb,
			},
		},
	}
}

// MakeURLAnnotation builds a URL annotation with the given chip_render_type.
func MakeURLAnnotation(start, length int32, href string, chipRenderType pb.Annotation_ChipRenderType) *pb.Annotation {
	return &pb.Annotation{
		Type:           pb.AnnotationType_URL.Enum(),
		StartIndex:     &start,
		Length:         &length,
		ChipRenderType: chipRenderType.Enum(),
		Metadata: &pb.Annotation_UrlMetadata{
			UrlMetadata: &pb.UrlMetadata{
				Url: &pb.Url{Url: &href},
			},
		},
	}
}

// MakeMentionAnnotation builds a MENTION annotation for gaiaID.
func MakeMentionAnnotation(start, length int32, gaiaID string) *pb.Annotation {
	mentionType := pb.UserMentionMetadata_MENTION
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

// MakeMentionAllAnnotation builds a MENTION_ALL ("@room") annotation.
func MakeMentionAllAnnotation(start, length int32) *pb.Annotation {
	mentionType := pb.UserMentionMetadata_MENTION_ALL
	return &pb.Annotation{
		Type:           pb.AnnotationType_USER_MENTION.Enum(),
		StartIndex:     &start,
		Length:         &length,
		ChipRenderType: pb.Annotation_DO_NOT_RENDER.Enum(),
		Metadata: &pb.Annotation_UserMentionMetadata{
			UserMentionMetadata: &pb.UserMentionMetadata{
				Type: &mentionType,
			},
		},
	}
}

// EscapePlainToHTML escapes s as HTML with newlines as <br/>.
func EscapePlainToHTML(s string) string {
	return strings.ReplaceAll(html.EscapeString(s), "\n", "<br/>")
}
