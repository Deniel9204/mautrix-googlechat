// Package gchatfmt converts a Google Chat message's text + annotations into
// Matrix HTML. It is a port of
// _reference/googlechat-python/mautrix_googlechat/formatter/from_googlechat.py,
// structurally adopted from
// _reference/googlechat-megabridge/pkg/msgconv/gchatfmt/{convert,utils}.go
// with two fixes required before adoption (docs/research/08d-megabridge-msgconv.md):
//
//   - B1 (security): megabridge built <a href='%s'> with the raw,
//     unescaped Google-Chat-controlled URL/mxid string, so a URL such as
//     `https://x.com/'><script>` could break out of the attribute and
//     inject markup into the rendered Matrix event. Every href value here
//     goes through escapeAttr (html.EscapeString) first.
//   - B2 (correctness): megabridge resolved incoming mentions by looking up
//     a shared DM portal's room MXID (wrong target -- a room pill, not a
//     user pill -- that only worked by coincidence) or a logged-in bridge
//     user's own MXID (self-mentions only), and never pilled ghosts at
//     all. This package instead takes a MentionResolver callback and
//     leaves real gaiaID -> ghost/user MXID resolution to the caller (M3
//     Task 3); this task only wires the seam and the nil-safe fallback.
//
// # Behavior inventory (from_googlechat.py, read in full)
//
//   - googlechat_to_matrix (:29-57): body = text_body verbatim; iff
//     evt.annotations is non-empty, format=HTML and formatted_body is
//     computed from annotations. **Bug NOT ported**: line 45 tests
//     `if annotations:`, the module-level `from __future__ import
//     annotations` feature object (always truthy) instead of
//     `evt.annotations` -- so the Python bridge always takes the
//     HTML-formatting branch, even for annotation-free messages. Here the
//     gate is len(annotations) > 0, matching the evidently-intended
//     behavior (also documented at pkg/msgconv/from-gchat.go and the M2
//     test TestToMatrix_AnnotationsPresentIgnored).
//   - Once formatted, Python replaces literal "\n" with "<br/>" in the
//     final HTML string (:52) -- a blanket string replace done AFTER
//     rendering, not HTML/context aware (e.g. it would also rewrite a
//     newline that happened to land inside a <pre><code> block). Ported
//     verbatim in Parse below for fidelity; not "fixed" because the task
//     only calls out B1/B2.
//   - _annotation_key (:69-80) is defined but never called anywhere in the
//     Python codebase (grep-verified against the full _reference tree) --
//     dead code, not ported.
//   - _normalize_annotations (:84-113): sorts by (start_index asc, length
//     desc), then for each annotation in turn treats it as the "current"
//     span and walks forward through the remaining (already-sorted)
//     annotations: any subsequent annotation that starts before the
//     current span's end but extends past it is split in place -- the
//     original is truncated to end exactly at the current span's end, and
//     a copy covering the remainder is queued for insertion once a
//     non-overlapping annotation (or the end of the list) is reached. This
//     is the overlapping-span normalization algorithm; ported field-for-
//     field into normalizeAnnotations below (see its doc comment for the
//     index arithmetic proof). Unlike both Python and megabridge, this
//     port never mutates the caller's annotation slice/objects -- Parse
//     deep-clones every annotation before normalizing (megabridge's
//     convert.go both sorts AND truncates the caller's own
//     *proto.Annotation objects in place, flagged in 08d §1.3 as a
//     "footgun" since msgconv re-iterates the same slice for attachments
//     in a later milestone).
//   - _gc_annotations_to_matrix (:116-201): the recursive interval
//     renderer. Ported as renderAnnotations below with the same recursion
//     shape: for annotation i (after chip-render + bounds filtering),
//     emit any plain gap text since last_offset, recurse into the
//     annotation's own span with annotations[i+1:] as the candidate
//     nested set (this is what lets BOLD-containing-ITALIC render as
//     nested tags), then wrap the recursed text per annotation type.
//     chip_render_type != DO_NOT_RENDER annotations (link/upload preview
//     chips) are skipped entirely (`continue`) -- they render separately,
//     M5. relative_offset < last_offset annotations (fully consumed by a
//     wider sibling already rendered) are also skipped. The
//     `assert start+length <= offset+length` becomes a returned error here
//     (Go must not panic on malformed/out-of-bounds server data).
//   - Annotation type dispatch (:153-197): FormatMetadata HIDDEN (drop
//     text), BOLD/ITALIC/UNDERLINE/STRIKE/MONOSPACE/MONOSPACE_BLOCK/
//     BULLETED_LIST/BULLETED_LIST_ITEM (wrap), FONT_COLOR
//     ((rgb+2^31)&0xFFFFFF -> hex), anything else format-typed (SOURCE_CODE,
//     CLIENT_HIDDEN, TYPE_UNSPECIFIED) -> skip_entity (render as plain,
//     unwrapped text, still recursing into nested annotations).
//     url_metadata -> <a href>. user_mention_metadata: MENTION_ALL -> the
//     literal "@room"; otherwise a mention pill (ported behavior: pill to
//     the resolved MXID with the resolved display name; :190-194's
//     Matrix-room-member-displayname override has no equivalent here since
//     that requires a live state store lookup outside msgconv's layering --
//     MentionResolver is the intended seam for M3 Task 3 to supply
//     whatever name it prefers). No metadata at all -> skip_entity.
//   - FONT_COLOR: rgb_int + 2**31 in Python is exact-precision integer
//     arithmetic (never overflows); ported here as int64 arithmetic before
//     masking to avoid Go's int32 add overflowing the literal 1<<31 (which
//     does not even fit in a signed int32 constant).
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

// MentionResolver maps a Google Chat user id (gaiaID) to a Matrix mention
// pill target (MXID + display name). ok is false when the gaiaID cannot be
// resolved to a known ghost/user (e.g. not yet seen by the bridge); in that
// case mxid and name are ignored by Parse.
//
// nil-safe: a nil MentionResolver is treated identically to a resolver that
// always returns ok=false -- Parse never dereferences it without a nil
// check.
type MentionResolver func(gaiaID string) (mxid id.UserID, name string, ok bool)

// Parse renders a Google Chat message's text + annotations into Matrix
// HTML. Returns (plainBody, htmlBody).
//
// body is always text, unmodified -- see the package doc comment's note on
// googlechat_to_matrix; unlike Python (which re-derives content.body from
// the rendered HTML via mautrix.util.formatter.parse_html once
// formatted_body is set), this package leaves plain-body derivation to the
// caller/M2's existing convention (pkg/msgconv/from-gchat.go: "text_body
// taken verbatim"). That HTML round-trip is a nice-to-have plaintext
// fallback improvement, not a fidelity requirement, and pulling in an
// HTML->text engine here would add a dependency this package does not
// otherwise need.
//
// html is "" whenever annotations is empty -- this is the fix for the
// Python always-truthy-annotations bug documented in the package comment
// and docs/research/07-gap-analysis.md §1.2/§5 risk #5: Google Chat
// messages with zero annotations must never get an HTML formatted_body.
func Parse(ctx context.Context, text string, annotations []*pb.Annotation, mention MentionResolver) (body, html string) {
	body = text
	if len(annotations) == 0 {
		return body, ""
	}

	units := utf16Encode(text)
	cloned := cloneAnnotations(annotations)
	rendered, err := renderAnnotations(mention, units, cloned, 0, int32(len(units)))
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).
			Msg("googlechat: gchatfmt: annotation conversion failed, falling back to plain text")
		return body, ""
	}
	if rendered == "" {
		return body, ""
	}

	// from_googlechat.py:52 -- literal newlines become <br/> in the final
	// HTML string. A blanket post-hoc replace, not HTML-context-aware;
	// ported as-is (see package doc comment).
	html = strings.ReplaceAll(rendered, "\n", "<br/>")
	return body, html
}

// cloneAnnotations deep-copies every annotation so normalizeAnnotations can
// freely sort and mutate (StartIndex/Length truncation, split-copy
// insertion) without touching the caller's slice or the *pb.Annotation
// objects within it. See the package doc comment for why this deviates
// from both Python and megabridge (which both mutate in place).
func cloneAnnotations(annotations []*pb.Annotation) []*pb.Annotation {
	cloned := make([]*pb.Annotation, len(annotations))
	for i, a := range annotations {
		cloned[i] = ggproto.Clone(a).(*pb.Annotation)
	}
	return cloned
}

// normalizeAnnotations is a field-for-field port of from_googlechat.py's
// _normalize_annotations (see the package doc comment for the algorithm
// walkthrough). It mutates and reorders the annotations slice it is given
// in place -- callers that don't own the slice must clone first (Parse
// does, via cloneAnnotations).
//
// Index-arithmetic proof that this Go port matches Python exactly:
// Python's `i += 1 + i2; annotations[i:i] = insert_annotations` inserts
// insert_annotations immediately before the first annotation found to
// start at/after the current span's end (annotations[old_i+1+i2] in the
// pre-insertion list). The Go version computes the same split point
// (i+1+i2, with i still holding its pre-branch value at that point) and
// rebuilds the slice as annotations[:i+1+i2] ++ insertAnnotations ++
// annotations[i+1+i2:] before advancing i to i+1+i2 -- the same insertion
// position, expressed as three concatenated slices instead of Python's
// list splice. When the inner loop exhausts without ever finding a
// non-overlapping annotation (Python's for/else `else: i += 1`), Go's
// `if !foundBreak { i++ }` matches, leaving insertAnnotations queued for
// the next outer iteration exactly as Python does. Both loops terminate
// with i == len(annotations) (each branch either sets i to a valid
// in-bounds index and continues, or increments by exactly 1 until the
// while/for condition fails), so Python's final `annotations[i:i] =
// insert_annotations` (insert at the list's own length, i.e. append) is
// equivalent to Go's final unconditional append of any still-queued
// insertAnnotations.
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
		// int64: StartIndex/Length are wire-controlled int32 fields: Python's
		// equivalent addition (from_googlechat.py:93) is on arbitrary-
		// precision ints and can never overflow. A malformed/adversarial
		// annotation (e.g. Length near int32 max) would silently wrap an
		// int32 sum, producing a bogus `end` and wrong (not just
		// Python-divergent) truncation decisions below -- see the identical
		// concern in renderAnnotations's bounds check, which is the actual
		// panic backstop for any annotation that reaches it un-truncated.
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

// renderAnnotations is the recursive interval renderer, a port of
// _gc_annotations_to_matrix (see package doc comment). text is the UTF-16
// code-unit slice covering exactly [offset, offset+length) of the overall
// message; annotations is the candidate set for this recursion level
// (normalized once at the top level, and re-normalized -- a no-op on an
// already-sorted-and-split slice, matching Python's re-normalize-every-call
// behavior -- at each nested level since the candidate set shrinks each
// recursion).
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
		if annotation.GetChipRenderType() != pb.Annotation_DO_NOT_RENDER {
			// RENDER / RENDER_IF_POSSIBLE chips (link previews, upload
			// previews, etc.) are rendered separately from formatting --
			// M5. Leave the underlying text as plain, unwrapped content.
			continue
		}
		// int64: start/annLen are wire-controlled int32 fields, and a
		// malformed/adversarial annotation (e.g. Length near int32 max)
		// makes the naive int32 sum `start+annLen` silently wrap, which can
		// produce a negative value that passes the `> offset+length` check
		// and then panics as an out-of-range slice bound below. Python's
		// equivalent assert (from_googlechat.py:137) can't overflow since
		// Python ints are arbitrary precision; this promotion is Go's
		// equivalent safety net -- the actual backstop against untrusted
		// server data, since normalizeAnnotations only truncates an
		// annotation when it finds ANOTHER one to compare it against.
		annEnd := int64(start) + int64(annLen)
		if start < offset || annLen < 0 || annEnd > int64(offset)+int64(length) {
			// Overlapping/out-of-bounds annotations should have been
			// resolved by normalizeAnnotations; malformed server data
			// (or a bug) could still violate this. Python asserts here;
			// Go must not panic on untrusted input, so this degrades to
			// an error that Parse catches and falls back to plain text
			// for the whole message.
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

// renderOne dispatches a single annotation to its HTML rendering (or
// records that it should be skipped, i.e. rendered as unwrapped plain
// text). Returns skipEntity, matching from_googlechat.py's skip_entity
// local.
func renderOne(out *strings.Builder, mention MentionResolver, annotation *pb.Annotation, entityText string) (skipEntity bool) {
	switch {
	case annotation.GetFormatMetadata() != nil:
		return renderFormat(out, annotation.GetFormatMetadata(), entityText)
	case annotation.GetUrlMetadata() != nil:
		renderURL(out, annotation.GetUrlMetadata(), entityText)
		return false
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
		// from_googlechat.py:171-173 -- rgb_int + 2**31 is Python
		// arbitrary-precision arithmetic that never overflows; done here
		// in int64 (an int32 add of the literal 1<<31 doesn't even
		// compile -- 2147483648 overflows int32) before masking to the
		// low 24 bits.
		rgb := int64(fm.GetFontColor())
		color := (rgb + (1 << 31)) & 0xFFFFFF
		fmt.Fprintf(out, `<span data-mx-color="#%06x">%s</span>`, color, entityText)
	case pb.FormatMetadata_BULLETED_LIST_ITEM:
		fmt.Fprintf(out, "<li>%s</li>", entityText)
	case pb.FormatMetadata_BULLETED_LIST:
		fmt.Fprintf(out, "<ul>%s</ul>", entityText)
	default:
		// SOURCE_CODE, CLIENT_HIDDEN, TYPE_UNSPECIFIED: Python has no
		// case for these either -- render as plain, unwrapped text.
		return true
	}
	return false
}

// renderURL renders a hyperlink annotation. Fix for B1: href is
// HTML-attribute-escaped before being written, so a malicious/malformed
// Google-Chat-controlled URL (e.g. `https://x.com/"><script>alert(1)</script>`)
// cannot break out of the href attribute and inject markup into the
// rendered Matrix event.
func renderURL(out *strings.Builder, urlMeta *pb.UrlMetadata, entityText string) {
	href := urlMeta.GetUrl().GetUrl()
	fmt.Fprintf(out, `<a href="%s">%s</a>`, escapeAttr(href), entityText)
}

// renderMention renders a user_mention_metadata annotation. MENTION_ALL
// (Google Chat's "@all") always renders as the literal "@room", matching
// Matrix's room-wide mention convention (from_googlechat.py:184-185).
//
// For a specific-user mention (fix for B2): the resolver seam this task
// wires up. mention may be nil (M3 Task 1 has no real resolver yet -- Task
// 3 provides one); when nil, or when it returns ok=false, no pill is
// rendered:
//   - if the resolver still supplied a name (ok=false but name != ""), that
//     name is used as the plain-text rendering (HTML-escaped);
//   - otherwise entityText -- the original annotation text already present
//     in the message body (typically "@Full Name"), escaped by the
//     recursive render -- is kept verbatim, so no text is silently
//     dropped.
//
// When the resolver returns ok=true, a pill (<a href="https://matrix.to/#/...">)
// is rendered; the label prefers the resolved display name over entityText
// when one is supplied. Both the mxid (via its matrix.to URL) and the name
// are attribute-escaped/text-escaped respectively -- unlike
// from_googlechat.py:195, which interpolates a Matrix room member's
// displayname into the href-adjacent text completely unescaped.
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
