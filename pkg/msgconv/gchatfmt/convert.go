// Package gchatfmt converts a Google Chat message's text + annotations into Matrix HTML.
// Annotation offsets are UTF-16 code units.
package gchatfmt

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/rs/zerolog"
	ggproto "google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/id"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// MentionResolver maps a Google Chat gaiaID to a Matrix pill target; ok=false if unknown.
// A nil MentionResolver is treated as always returning ok=false.
type MentionResolver func(gaiaID string) (mxid id.UserID, name string, ok bool)

// ParsedMentions is the set of mentions a message actually pings: users rendered as
// pills (deduplicated) and Room for MENTION_ALL.
type ParsedMentions struct {
	UserIDs []id.UserID
	Room    bool
}

func (pm *ParsedMentions) addUser(mxid id.UserID) {
	for _, existing := range pm.UserIDs {
		if existing == mxid {
			return
		}
	}
	pm.UserIDs = append(pm.UserIDs, mxid)
}

// Parse renders text + annotations into Matrix HTML. body is always text unmodified;
// html is "" when annotations is empty. mentions is computed in a separate pass so a
// valid mention still pings even if the HTML render falls back to plain text.
func Parse(ctx context.Context, text string, annotations []*pb.Annotation, mention MentionResolver) (body, html string, mentions ParsedMentions) {
	body = text
	if len(annotations) == 0 {
		return body, "", ParsedMentions{}
	}

	units := utf16Encode(text)
	mentions = collectMentions(units, annotations, mention)
	cloned := cloneAnnotations(annotations)
	rendered, err := renderAnnotations(mention, units, cloned, 0, int32(len(units)))
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).
			Msg("googlechat: gchatfmt: annotation conversion failed, falling back to plain text")
		return body, "", mentions
	}
	if rendered == "" {
		return body, "", mentions
	}

	html = newlinesToBR(rendered)
	return body, html, mentions
}

// newlinesToBR turns newlines into <br/> except inside <pre> blocks. Only this
// package's own <pre> tags are unescaped, since all message text goes through escapeUnits.
func newlinesToBR(rendered string) string {
	const openTag, closeTag = "<pre>", "</pre>"

	var out strings.Builder
	rest := rendered
	for {
		start := strings.Index(rest, openTag)
		if start < 0 {
			out.WriteString(strings.ReplaceAll(rest, "\n", "<br/>"))
			return out.String()
		}
		out.WriteString(strings.ReplaceAll(rest[:start], "\n", "<br/>"))

		block := rest[start:]
		end := strings.Index(block, closeTag)
		if end < 0 {
			out.WriteString(block)
			return out.String()
		}
		end += len(closeTag)
		out.WriteString(block[:end]) // verbatim: whitespace is significant here
		rest = block[end:]
	}
}

// spanWithinParent reports whether [start, start+length) fits inside the parent span.
// int64 math because StartIndex/Length are wire-controlled int32 and could overflow.
func spanWithinParent(start, length, offset, parentLen int32) bool {
	if start < offset || length < 0 {
		return false
	}
	return int64(start)+int64(length) <= int64(offset)+int64(parentLen)
}

// collectMentions returns the mentions a successful render would pill, applying the
// same gate as renderAnnotations/renderMention.
func collectMentions(units []uint16, annotations []*pb.Annotation, mention MentionResolver) ParsedMentions {
	var pm ParsedMentions
	textLen := int32(len(units))
	for _, a := range annotations {
		if a.GetChipRenderType() != pb.Annotation_DO_NOT_RENDER {
			continue
		}
		um := a.GetUserMentionMetadata()
		if um == nil {
			continue
		}
		if !spanWithinParent(a.GetStartIndex(), a.GetLength(), 0, textLen) {
			continue
		}
		if um.GetType() == pb.UserMentionMetadata_MENTION_ALL {
			pm.Room = true
			continue
		}
		if mention == nil {
			continue
		}
		if mxid, _, ok := mention(um.GetId().GetId()); ok {
			pm.addUser(mxid)
		}
	}
	return pm
}

// cloneAnnotations deep-copies annotations so normalizeAnnotations can mutate them.
func cloneAnnotations(annotations []*pb.Annotation) []*pb.Annotation {
	cloned := make([]*pb.Annotation, len(annotations))
	for i, a := range annotations {
		cloned[i] = ggproto.Clone(a).(*pb.Annotation)
	}
	return cloned
}

// normalizeAnnotations sorts by (start asc, length desc) and splits overlapping spans
// so they nest. It mutates the slice in place; callers must clone first.
func normalizeAnnotations(annotations []*pb.Annotation) []*pb.Annotation {
	if len(annotations) == 0 {
		return annotations
	}

	sort.SliceStable(annotations, func(i, j int) bool {
		si, sj := annotations[i].GetStartIndex(), annotations[j].GetStartIndex()
		if si == sj {
			return annotations[i].GetLength() > annotations[j].GetLength()
		}
		return si < sj
	})

	i := 0
	var insertAnnotations []*pb.Annotation
	for i < len(annotations) {
		cur := annotations[i]
		// int64: StartIndex/Length are wire-controlled int32 fields and could overflow.
		end := int64(cur.GetStartIndex()) + int64(cur.GetLength())

		foundBreak := false
		for i2, annotation := range annotations[i+1:] {
			start := int64(annotation.GetStartIndex())
			length := int64(annotation.GetLength())
			if start >= end {
				splitAt := i + 1 + i2
				next := make([]*pb.Annotation, 0, len(annotations)+len(insertAnnotations))
				next = append(next, annotations[:splitAt]...)
				next = append(next, insertAnnotations...)
				next = append(next, annotations[splitAt:]...)
				annotations = next
				insertAnnotations = nil
				i = splitAt
				foundBreak = true
				break
			} else if start+length > end {
				// The int32 narrowing is safe only because renderAnnotations's bounds check
				// rejects an oversized `cur` before any tail derived from it. Don't reorder that check.
				tail := ggproto.Clone(annotation).(*pb.Annotation)
				truncatedLength := int32(end - start)
				annotation.Length = &truncatedLength
				tailStart := int32(start + int64(truncatedLength))
				tailLength := int32(length - int64(truncatedLength))
				tail.StartIndex = &tailStart
				tail.Length = &tailLength
				insertAnnotations = append(insertAnnotations, tail)
			}
		}
		if !foundBreak {
			i++
		}
	}

	if len(insertAnnotations) > 0 {
		annotations = append(annotations, insertAnnotations...)
	}
	return annotations
}

// renderAnnotations recursively renders text (UTF-16 code units covering
// [offset, offset+length)) with the given candidate annotations.
func renderAnnotations(
	mention MentionResolver,
	text []uint16,
	annotations []*pb.Annotation,
	offset, length int32,
) (string, error) {
	if len(annotations) == 0 {
		return escapeUnits(text), nil
	}
	if length == 0 {
		length = int32(len(text))
	}

	var out strings.Builder
	var lastOffset int32

	annotations = normalizeAnnotations(annotations)

	for i, annotation := range annotations {
		start := annotation.GetStartIndex()
		annLen := annotation.GetLength()

		if start >= offset+length {
			break
		}
		if annotation.GetChipRenderType() != pb.Annotation_DO_NOT_RENDER && !inlineURLChip(annotation) {
			// RENDER / RENDER_IF_POSSIBLE chips render separately, except url_metadata:
			// Google Chat sets RENDER_IF_POSSIBLE on ordinary pasted links.
			continue
		}
		// Shared bounds gate with collectMentions; the backstop against untrusted server data.
		if !spanWithinParent(start, annLen, offset, length) {
			// Must not panic on untrusted input; Parse falls back to plain text.
			annEnd := int64(start) + int64(annLen)
			return "", fmt.Errorf("gchatfmt: annotation [%d,%d) out of parent bounds [%d,%d)", start, annEnd, offset, offset+length)
		}

		relStart := start - offset
		if relStart > lastOffset {
			out.WriteString(escapeUnits(text[lastOffset:relStart]))
		} else if relStart < lastOffset {
			continue
		}

		entityText, err := renderAnnotations(mention, text[relStart:relStart+annLen], annotations[i+1:], start, annLen)
		if err != nil {
			return "", err
		}

		skipEntity := renderOne(&out, mention, annotation, entityText)

		if skipEntity {
			lastOffset = relStart
		} else {
			lastOffset = relStart + annLen
		}
	}

	out.WriteString(escapeUnits(text[lastOffset:]))
	return out.String(), nil
}

// renderOne dispatches a single annotation to its HTML rendering. Returns skipEntity.
func renderOne(out *strings.Builder, mention MentionResolver, annotation *pb.Annotation, entityText string) (skipEntity bool) {
	switch {
	case annotation.GetFormatMetadata() != nil:
		return renderFormat(out, annotation.GetFormatMetadata(), entityText)
	case annotation.GetUrlMetadata() != nil:
		return renderURL(out, annotation.GetUrlMetadata(), entityText)
	case annotation.GetUserMentionMetadata() != nil:
		renderMention(out, mention, annotation.GetUserMentionMetadata(), entityText)
		return false
	default:
		return true
	}
}

func renderFormat(out *strings.Builder, fm *pb.FormatMetadata, entityText string) (skipEntity bool) {
	switch fm.GetFormatType() {
	case pb.FormatMetadata_HIDDEN:
		// Don't append the text -- it's meant to be invisible.
	case pb.FormatMetadata_BOLD:
		fmt.Fprintf(out, "<strong>%s</strong>", entityText)
	case pb.FormatMetadata_ITALIC:
		fmt.Fprintf(out, "<em>%s</em>", entityText)
	case pb.FormatMetadata_UNDERLINE:
		fmt.Fprintf(out, "<u>%s</u>", entityText)
	case pb.FormatMetadata_STRIKE:
		fmt.Fprintf(out, "<del>%s</del>", entityText)
	case pb.FormatMetadata_MONOSPACE:
		fmt.Fprintf(out, "<code>%s</code>", entityText)
	case pb.FormatMetadata_MONOSPACE_BLOCK:
		fmt.Fprintf(out, "<pre><code>%s</code></pre>", entityText)
	case pb.FormatMetadata_FONT_COLOR:
		// rgb_int + 2**31 in int64 (1<<31 overflows int32), masked to the low 24 bits.
		rgb := int64(fm.GetFontColor())
		color := (rgb + (1 << 31)) & 0xFFFFFF
		fmt.Fprintf(out, `<span data-mx-color="#%06x">%s</span>`, color, entityText)
	case pb.FormatMetadata_BULLETED_LIST_ITEM:
		fmt.Fprintf(out, "<li>%s</li>", entityText)
	case pb.FormatMetadata_BULLETED_LIST:
		fmt.Fprintf(out, "<ul>%s</ul>", entityText)
	default:
		// SOURCE_CODE, CLIENT_HIDDEN, TYPE_UNSPECIFIED: render as plain text.
		return true
	}
	return false
}

// dangerousURLSchemes are link schemes that execute script when followed.
var dangerousURLSchemes = []string{"javascript:", "data:"}

// isDangerousURLScheme reports whether href has a dangerous scheme, normalizing like a
// browser's URL parser: strip tab/CR/LF anywhere, trim leading C0/space, ignore case.
func isDangerousURLScheme(href string) bool {
	cleaned := strings.Map(func(r rune) rune {
		switch r {
		case '\t', '\n', '\r':
			return -1 // drop
		default:
			return r
		}
	}, href)
	cleaned = strings.TrimLeftFunc(cleaned, func(r rune) bool {
		return r <= ' '
	})
	lower := strings.ToLower(cleaned)
	for _, scheme := range dangerousURLSchemes {
		if strings.HasPrefix(lower, scheme) {
			return true
		}
	}
	return false
}

// renderURL renders a hyperlink with an escaped href. Dangerous schemes are left as
// plain text (skipEntity=true).
func renderURL(out *strings.Builder, urlMeta *pb.UrlMetadata, entityText string) (skipEntity bool) {
	href := urlMeta.GetUrl().GetUrl()
	if isDangerousURLScheme(href) {
		return true
	}
	fmt.Fprintf(out, `<a href="%s">%s</a>`, escapeAttr(href), entityText)
	return false
}

// renderMention renders a mention: MENTION_ALL as "@room", a resolved user as a pill,
// otherwise the resolver's name or the original text.
func renderMention(out *strings.Builder, mention MentionResolver, m *pb.UserMentionMetadata, entityText string) {
	if m.GetType() == pb.UserMentionMetadata_MENTION_ALL {
		out.WriteString("@room")
		return
	}

	gaiaID := m.GetId().GetId()
	label := entityText
	href := ""
	if mention != nil {
		if mxid, name, ok := mention(gaiaID); ok {
			href = escapeAttr(mxid.URI().MatrixToURL())
			if name != "" {
				label = escapeAttr(name)
			}
		} else if name != "" {
			label = escapeAttr(name)
		}
	}

	if href != "" {
		fmt.Fprintf(out, `<a href="%s">%s</a>`, href, label)
	} else {
		out.WriteString(label)
	}
}

// inlineURLChip reports whether a non-DO_NOT_RENDER annotation is a url_metadata that
// covers text and so should still render inline as a link.
func inlineURLChip(a *pb.Annotation) bool {
	return a.GetUrlMetadata() != nil && a.GetLength() > 0
}
