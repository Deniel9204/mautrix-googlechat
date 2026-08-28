package gchatfmt

import (
	"html"
	"unicode/utf16"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"strings"
)

// utf16Encode re-encodes a UTF-8 Go string into UTF-16 code units, matching
// JavaScript's String indexing / .length semantics -- the same space Google
// Chat's annotation start_index/length fields are measured in. Go strings are
// UTF-8 byte sequences, so this explicit re-encode step is needed to index by
// UTF-16 code unit. Astral characters (outside the Basic Multilingual Plane,
// e.g. most emoji) encode to a surrogate PAIR here -- two code units --
// exactly as they would in the JS UTF-16 string model. This function is
// called exactly once per Parse call; all annotation offset arithmetic
// downstream operates on the resulting []uint16, never on Go byte or rune
// indices.
func utf16Encode(s string) []uint16 {
	return utf16.Encode([]rune(s))
}

// utf16Decode is the inverse of utf16Encode: turns a UTF-16 code-unit slice
// back into a UTF-8 Go string. Invalid/unpaired surrogates decode to
// U+FFFD via utf16.Decode's standard replacement behavior.
func utf16Decode(units []uint16) string {
	return string(utf16.Decode(units))
}

// escapeUnits HTML-escapes a UTF-16 code-unit slice as plain text content
// (not an attribute value -- see escapeAttr for that).
func escapeUnits(units []uint16) string {
	return html.EscapeString(utf16Decode(units))
}

// escapeAttr HTML-escapes a string for safe use inside a double-quoted HTML
// attribute value (href="..."). This is the fix for a megabridge bug: the
// megabridge port built hrefs with fmt.Fprintf("<a href='%s'>", url) using
// the raw, unescaped Google Chat-controlled URL/mxid string, so a URL like
// `https://x.com/'><script>` breaks out of the attribute and injects
// markup into the Matrix event. Go's html.EscapeString escapes all five of
// < > & ' " , so it is safe to use as an attribute escaper regardless of
// whether the attribute is single- or double-quoted; every href/mention
// target in this package is escaped before being written.
func escapeAttr(s string) string {
	return html.EscapeString(s)
}

// --- Test-fixture constructors -------------------------------------------
//
// These build well-formed *pb.Annotation values with the fields the wire
// protocol always sets in practice (particularly ChipRenderType, which
// defaults to the zero value Annotation_UNKNOWN if left unset -- NOT
// DO_NOT_RENDER -- so a naive test fixture that forgets to set it would
// silently be skipped by the chip-render filter in convert.go). They are
// exported for use from convert_test.go (external test package) and any
// future msgconv tests that need to build annotation fixtures.

// MakeFormatAnnotation builds a FORMAT_DATA annotation (bold/italic/etc.)
// covering [start, start+length) with chip_render_type=DO_NOT_RENDER, the
// value real formatting annotations carry on the wire.
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

// MakeFontColorAnnotation builds a FORMAT_DATA/FONT_COLOR annotation. rgb is
// the raw font_color wire value (already in Google Chat's signed encoding,
// i.e. what convert.go's (rgb+2^31)&0xFFFFFF transform expects as input).
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

// MakeURLAnnotation builds a URL annotation with the given href and
// chip_render_type (DO_NOT_RENDER for an inline-formatted hyperlink;
// RENDER/RENDER_IF_POSSIBLE for a link-preview chip that gchatfmt must skip).
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

// MakeMentionAnnotation builds a USER_MENTION annotation for a specific
// Google Chat user (gaiaID), type MENTION.
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

// MakeMentionAllAnnotation builds a USER_MENTION annotation of type
// MENTION_ALL, i.e. an "@all"/"@room" mention.
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

// EscapePlainToHTML renders a plain-text string as HTML: escaped, with
// newlines as <br/>. Used when a message's text half carried no formatting
// of its own but has to be combined with HTML produced elsewhere (card
// rendering), so the two halves end up in one well-formed formatted_body.
func EscapePlainToHTML(s string) string {
	return strings.ReplaceAll(html.EscapeString(s), "\n", "<br/>")
}
