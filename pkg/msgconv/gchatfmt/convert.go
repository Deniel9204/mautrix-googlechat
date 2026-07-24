// Package gchatfmt converts a Google Chat message's text + annotations into
// Matrix HTML. It is structurally adopted from
// _reference/googlechat-megabridge/pkg/msgconv/gchatfmt/{convert,utils}.go
// with two fixes required before adoption:
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
// # Behavior inventory
//
//   - body = text_body verbatim; iff annotations is non-empty, format=HTML
//     and formatted_body is computed from annotations. The gate here is
//     deliberately len(annotations) > 0: a message with zero annotations
//     must never get an HTML formatted_body (also documented at
//     pkg/msgconv/from-gchat.go and the test
//     TestToMatrix_NoAnnotationsStaysPlain, pkg/msgconv/from-gchat_test.go).
//   - Once formatted, literal "\n" is replaced with "<br/>" in the final
//     HTML string -- a blanket string replace done AFTER rendering, not
//     HTML/context aware (e.g. it would also rewrite a newline that happened
//     to land inside a <pre><code> block). Done in Parse below; deliberately
//     not "fixed" here.
//   - The overlapping-span normalization algorithm: sort by (start_index
//     asc, length desc), then for each annotation in turn treat it as the
//     "current" span and walk forward through the remaining (already-sorted)
//     annotations: any subsequent annotation that starts before the
//     current span's end but extends past it is split in place -- the
//     original is truncated to end exactly at the current span's end, and
//     a copy covering the remainder is queued for insertion once a
//     non-overlapping annotation (or the end of the list) is reached.
//     Implemented in normalizeAnnotations below (see its doc comment for the
//     index arithmetic proof). Unlike megabridge, this code never mutates
//     the caller's annotation slice/objects -- Parse deep-clones every
//     annotation before normalizing (megabridge's convert.go both sorts AND
//     truncates the caller's own *proto.Annotation objects in place, a
//     "footgun" since msgconv re-iterates the same slice for attachments in
//     a later milestone).
//   - renderAnnotations below is the recursive interval renderer: for
//     annotation i (after chip-render + bounds filtering), emit any plain
//     gap text since last_offset, recurse into the annotation's own span
//     with annotations[i+1:] as the candidate nested set (this is what lets
//     BOLD-containing-ITALIC render as nested tags), then wrap the recursed
//     text per annotation type. chip_render_type != DO_NOT_RENDER
//     annotations (upload/preview chips) are skipped entirely (`continue`)
//     -- they render separately. The ONE exception is a url_metadata
//     annotation covering text (inlineURLChip): Google Chat stamps
//     RENDER_IF_POSSIBLE on an ordinary pasted link, so hyperlinks are
//     rendered for every chip_render_type or received links would arrive as
//     plain, unlinkified text. relative_offset <
//     last_offset annotations (fully consumed by a wider sibling already
//     rendered) are also skipped. The start+length <= offset+length bounds
//     check returns an error here rather than panicking on malformed/out-of-
//     bounds server data.
//   - Annotation type dispatch: FormatMetadata HIDDEN (drop text),
//     BOLD/ITALIC/UNDERLINE/STRIKE/MONOSPACE/MONOSPACE_BLOCK/
//     BULLETED_LIST/BULLETED_LIST_ITEM (wrap), FONT_COLOR
//     ((rgb+2^31)&0xFFFFFF -> hex), anything else format-typed (SOURCE_CODE,
//     CLIENT_HIDDEN, TYPE_UNSPECIFIED) -> skip_entity (render as plain,
//     unwrapped text, still recursing into nested annotations).
//     url_metadata -> <a href>. user_mention_metadata: MENTION_ALL -> the
//     literal "@room"; otherwise a mention pill (pill to the resolved MXID
//     with the resolved display name; there is no Matrix-room-member-
//     displayname override here since that requires a live state store
//     lookup outside msgconv's layering -- MentionResolver is the intended
//     seam for the caller to supply whatever name it prefers). No metadata
//     at all -> skip_entity.
//   - FONT_COLOR: rgb_int + 2**31 must be exact-precision integer arithmetic
//     that never overflows; done here as int64 arithmetic before masking to
//     avoid Go's int32 add overflowing the literal 1<<31 (which does not
//     even fit in a signed int32 constant).
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

// ParsedMentions is the set of mentions Parse determined a message genuinely
// mentions: the resolved Matrix user MXIDs it would emit pills for (UserIDs,
// deduplicated) and Room=true when it saw a valid MENTION_ALL ("@room").
//
// This is the single source of truth for "who should this message ping"
// (content.Mentions / m.mentions), replacing the connector's former
// independent annotation re-walk (pkg/connector/mentions.go's inboundMentions,
// removed in the phantom-ping fix). A gaia id is in UserIDs iff the SAME
// per-annotation validity gate the HTML renderer applies (spanWithinParent
// bounds check + chip_render_type == DO_NOT_RENDER + resolver ok==true) would
// have emitted a pill for it -- so a malformed/out-of-bounds mention
// annotation (bogus StartIndex/Length, no corresponding body text) is never
// in UserIDs, eliminating phantom pings where the body contains zero
// reference to the pinged user.
type ParsedMentions struct {
	UserIDs []id.UserID
	Room    bool
}

// addUser appends mxid unless it is already present (dedup), matching
// event.Mentions.Add's own dedup contract.
func (pm *ParsedMentions) addUser(mxid id.UserID) {
	for _, existing := range pm.UserIDs {
		if existing == mxid {
			return
		}
	}
	pm.UserIDs = append(pm.UserIDs, mxid)
}

// Parse renders a Google Chat message's text + annotations into Matrix HTML.
// Returns (plainBody, htmlBody, mentions).
//
// body is always text, unmodified -- see the package doc comment's note on
// body derivation above; this package leaves plain-body derivation to the
// caller's existing convention (pkg/msgconv/from-gchat.go: "text_body
// taken verbatim") rather than re-deriving content.body from the rendered
// HTML. That HTML round-trip is a nice-to-have plaintext fallback
// improvement, not a fidelity requirement, and pulling in an HTML->text
// engine here would add a dependency this package does not otherwise need.
//
// html is "" whenever annotations is empty -- the always-truthy-annotations
// hazard documented in the package comment: Google Chat messages with zero
// annotations must never get an HTML formatted_body.
//
// mentions (ParsedMentions) is computed by collectMentions, a DELIBERATELY
// SEPARATE deterministic pass over the annotations, run BEFORE the recursive
// HTML render and returned even when the render itself falls back to plain
// text. It is not accumulated inside renderMention during the render walk
// for one specific reason: a message can validly ping a resolvable mention
// while STILL falling back to plain HTML because of an UNRELATED malformed
// formatting annotation elsewhere (renderAnnotations aborts the whole render
// on the first out-of-bounds annotation, in annotation-sorted order, so a
// perfectly valid mention that happens to sort after the bad annotation
// would never reach renderMention). In that case the plain body still
// literally contains the mention's "@Name" text, so pinging that user is
// correct, not phantom -- collectMentions keys the ping on each mention
// annotation's OWN validity, independent of whether the overall HTML render
// succeeded, so the valid mention pings regardless. The two walks share the
// same validity gate (spanWithinParent + the chip filter + the resolver), so
// for a normally-rendering message the returned UserIDs exactly match the
// pills the HTML actually contains.
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

	// literal newlines become <br/> in the final HTML string. A blanket
	// post-hoc replace, not HTML-context-aware (see package doc comment).
	html = strings.ReplaceAll(rendered, "\n", "<br/>")
	return body, html, mentions
}

// spanWithinParent reports whether an annotation covering [start, start+length)
// fits entirely inside the parent span [offset, offset+parentLen) -- the
// single bounds/validity gate shared by renderAnnotations (which errors and
// falls back to plain text when it fails) and collectMentions (which skips a
// mention that fails it, so it is never pinged). Arithmetic is promoted to
// int64 before the sum because StartIndex/Length are wire-controlled int32
// fields: a malformed/adversarial annotation with Length near int32 max would
// otherwise overflow a naive int32 sum and wrap to a value that spuriously
// passes the check, then panics as an out-of-range slice bound downstream.
// The equivalent bounds check in an arbitrary-precision-integer language
// can't overflow; int64 here is Go's equivalent safety net.
func spanWithinParent(start, length, offset, parentLen int32) bool {
	if start < offset || length < 0 {
		return false
	}
	return int64(start)+int64(length) <= int64(offset)+int64(parentLen)
}

// collectMentions walks annotations once, independently of the HTML render,
// and returns exactly the mentions that would produce a pill / "@room" in a
// successful render (see Parse's doc comment for why this is a separate pass).
// It applies the identical gate renderAnnotations/renderMention apply, in the
// same order: chip_render_type must be DO_NOT_RENDER (RENDER/RENDER_IF_POSSIBLE
// chips are link/upload previews, M5, never inline mentions); the annotation's
// span must be in-bounds (spanWithinParent, at the top level offset=0); and a
// specific-user MENTION must resolve via mention (ok==true) -- MENTION_ALL sets
// Room without consulting the resolver, exactly as renderMention hardcodes
// "@room" for it. text length is measured in UTF-16 code units (units), the
// same space annotation offsets live in.
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

// cloneAnnotations deep-copies every annotation so normalizeAnnotations can
// freely sort and mutate (StartIndex/Length truncation, split-copy
// insertion) without touching the caller's slice or the *pb.Annotation
// objects within it. See the package doc comment for why this deviates
// from megabridge (which mutates in place).
func cloneAnnotations(annotations []*pb.Annotation) []*pb.Annotation {
	cloned := make([]*pb.Annotation, len(annotations))
	for i, a := range annotations {
		cloned[i] = ggproto.Clone(a).(*pb.Annotation)
	}
	return cloned
}

// normalizeAnnotations implements the overlapping-span normalization
// algorithm (see the package doc comment for the walkthrough). It mutates
// and reorders the annotations slice it is given in place -- callers that
// don't own the slice must clone first (Parse does, via cloneAnnotations).
//
// Index-arithmetic notes: the split point i+1+i2 (with i still holding its
// pre-branch value at that point) is where insertAnnotations is spliced in --
// immediately before the first annotation found to start at/after the current
// span's end. The slice is rebuilt as annotations[:i+1+i2] ++
// insertAnnotations ++ annotations[i+1+i2:], then i advances to i+1+i2. When
// the inner loop exhausts without ever finding a non-overlapping annotation,
// `if !foundBreak { i++ }` leaves insertAnnotations queued for the next outer
// iteration. Both loops terminate with i == len(annotations) (each branch
// either sets i to a valid in-bounds index and continues, or increments by
// exactly 1 until the loop condition fails), so the final unconditional
// append of any still-queued insertAnnotations lands them at the slice's own
// length.
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
		// int64: StartIndex/Length are wire-controlled int32 fields. A
		// malformed/adversarial annotation (e.g. Length near int32 max) would
		// silently wrap an int32 sum, producing a bogus `end` and wrong
		// truncation decisions below -- see the identical concern in
		// renderAnnotations's bounds check, which is the actual panic backstop
		// for any annotation that reaches it un-truncated.
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
				// The int64->int32 narrowing below (the proto schema is
				// int32) is safe without its own overflow check ONLY
				// because of an invariant enforced elsewhere, not anything
				// local to this branch: `cur` (whose StartIndex/Length
				// produced `end`) always precedes any tail spliced from it
				// in the flattened annotations slice, and
				// renderAnnotations's bounds check (the int64-promoted
				// `annEnd > int64(offset)+int64(length)` guard) always
				// evaluates `cur` itself before it can ever reach a
				// corrupted tail derived from an adversarially huge `cur`
				// -- so a `cur` large enough to make this narrowing wrap
				// is guaranteed to be rejected first. Do not weaken or
				// reorder that check without re-deriving this safety
				// argument.
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

// renderAnnotations is the recursive interval renderer (see package doc
// comment). text is the UTF-16 code-unit slice covering exactly [offset,
// offset+length) of the overall message; annotations is the candidate set for
// this recursion level (normalized once at the top level, and re-normalized --
// a no-op on an already-sorted-and-split slice -- at each nested level since
// the candidate set shrinks each recursion).
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
			// RENDER / RENDER_IF_POSSIBLE chips (link previews, upload
			// previews, etc.) are rendered separately from formatting.
			// Leave the underlying text as plain, unwrapped content.
			//
			// url_metadata is deliberately exempt: Google Chat sets
			// chip_render_type=RENDER_IF_POSSIBLE on the URL annotation of an
			// ordinary pasted link (verified live), and an unset field decodes
			// to the proto2 zero value UNKNOWN -- so gating links on
			// DO_NOT_RENDER left every RECEIVED link as unlinkified plain text
			// while sent links worked (matrixfmt sets DO_NOT_RENDER itself).
			// Rendering it inline cannot double-render: linkappend.go never
			// appends a url_metadata URL to the body, and this package never
			// renders a preview chip, so the inline <a href> below is the only
			// rendering a url_metadata span ever gets.
			continue
		}
		// spanWithinParent is the shared int64-promoted bounds gate (see its
		// doc comment) -- the actual backstop against untrusted server data,
		// since normalizeAnnotations only truncates an annotation when it
		// finds ANOTHER one to compare it against. It is the SAME gate
		// collectMentions applies, so a mention that survives here (renders a
		// pill) is exactly one collectMentions would have collected, and one
		// rejected here is one it drops -- the two can never disagree about
		// which mentions the message "contains".
		if !spanWithinParent(start, annLen, offset, length) {
			// Overlapping/out-of-bounds annotations should have been
			// resolved by normalizeAnnotations; malformed server data
			// (or a bug) could still violate this. Go must not panic on
			// untrusted input, so this degrades to an error that Parse
			// catches and falls back to plain text for the whole message.
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

// renderOne dispatches a single annotation to its HTML rendering (or
// records that it should be skipped, i.e. rendered as unwrapped plain
// text). Returns skipEntity.
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
		// rgb_int + 2**31 must be exact-precision arithmetic that never
		// overflows; done here in int64 (an int32 add of the literal 1<<31
		// doesn't even compile -- 2147483648 overflows int32) before masking
		// to the low 24 bits.
		rgb := int64(fm.GetFontColor())
		color := (rgb + (1 << 31)) & 0xFFFFFF
		fmt.Fprintf(out, `<span data-mx-color="#%06x">%s</span>`, color, entityText)
	case pb.FormatMetadata_BULLETED_LIST_ITEM:
		fmt.Fprintf(out, "<li>%s</li>", entityText)
	case pb.FormatMetadata_BULLETED_LIST:
		fmt.Fprintf(out, "<ul>%s</ul>", entityText)
	default:
		// SOURCE_CODE, CLIENT_HIDDEN, TYPE_UNSPECIFIED: no case for these --
		// render as plain, unwrapped text.
		return true
	}
	return false
}

// dangerousURLSchemes are link schemes that, unlike a normal navigation
// target (http/https/mailto/...), execute script or embed arbitrary
// script-capable content when a client follows them -- most notoriously
// "javascript:" (arbitrary JS execution in the clicking client's own page/
// webview context) and "data:" (an inline document, e.g.
// "data:text/html,<script>...</script>", that many web-based Matrix clients
// will render as if it were a normal page). urlMeta.Url is an arbitrary,
// externally-supplied string straight off the Google Chat wire -- the same
// untrusted field B1 (escapeAttr, below) already treats as attacker-
// controlled -- so a malicious/compromised sender could plant either scheme
// here. Escaping (B1) only stops the value from breaking OUT of the href
// attribute; it does nothing to stop a syntactically well-formed
// javascript:/data: URL from becoming a live, clickable link. This is that
// scheme-side sibling fix.
var dangerousURLSchemes = []string{"javascript:", "data:"}

// isDangerousURLScheme reports whether href's scheme matches one of
// dangerousURLSchemes, applying the SAME normalization a browser's WHATWG
// URL parser applies before it recognizes a scheme -- so a link that a
// browser would execute as javascript:/data: is caught here even when the
// raw href string doesn't literally start with that scheme:
//
//   - Every ASCII tab (U+0009), LF (U+000A), and CR (U+000D) is removed from
//     ANYWHERE in the string first. This mirrors the WHATWG URL Standard's
//     "basic URL parser" preprocessing ("remove all ASCII tab or newline
//     from input"), which runs BEFORE scheme parsing -- so the textbook
//     filter bypass "java&#9;script:alert(1)" (an embedded tab, or CR/LF)
//     still parses as, and executes as, javascript: in Chromium/Firefox/
//     WebKit. A leading-only trim (an earlier version of this function)
//     misses exactly this.
//   - Then any leading C0-control-or-space is stripped (the parser's
//     scheme-start-state leading-trim), catching "  javascript:..." /
//     "\njavascript:...".
//   - Then the comparison is case-insensitive ("JavaScript:" == "javascript:").
//
// A literal ASCII space embedded IN the scheme (e.g. "java script:") is
// deliberately NOT stripped -- it isn't tab/CR/LF, and the URL parser treats
// it as an invalid scheme character that aborts scheme parsing entirely
// (the string is then a relative URL, not a javascript: one), so it is not a
// bypass and needs no handling here.
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

// renderURL renders a hyperlink annotation. Fix for B1: href is
// HTML-attribute-escaped before being written, so a malicious/malformed
// Google-Chat-controlled URL (e.g. `https://x.com/"><script>alert(1)</script>`)
// cannot break out of the href attribute and inject markup into the
// rendered Matrix event.
//
// A dangerous scheme (isDangerousURLScheme -- javascript:/data:) is
// neutralized instead of linkified: renderURL reports skipEntity=true
// (mirroring renderFormat's identical contract for FormatMetadata_HIDDEN/
// unrecognized types) so the caller leaves this span's ORIGINAL text
// unconsumed, to be emitted later as plain, HTML-escaped text by
// renderAnnotations' own gap-fill/tail logic -- never as any kind of <a>
// tag, and never dropped.
func renderURL(out *strings.Builder, urlMeta *pb.UrlMetadata, entityText string) (skipEntity bool) {
	href := urlMeta.GetUrl().GetUrl()
	if isDangerousURLScheme(href) {
		return true
	}
	fmt.Fprintf(out, `<a href="%s">%s</a>`, escapeAttr(href), entityText)
	return false
}

// renderMention renders a user_mention_metadata annotation. MENTION_ALL
// (Google Chat's "@all") always renders as the literal "@room", matching
// Matrix's room-wide mention convention.
//
// For a specific-user mention (fix for B2): the resolver seam. mention may
// be nil (no real resolver may be wired up yet); when nil, or when it
// returns ok=false, no pill is rendered:
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
// are attribute-escaped/text-escaped respectively, so a mention target can
// never inject markup into the rendered event.
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

// inlineURLChip reports whether a non-DO_NOT_RENDER annotation should still be
// rendered inline as a hyperlink: a url_metadata annotation that actually
// covers text.
//
// Google Chat sets chip_render_type=RENDER_IF_POSSIBLE on the URL annotation
// of an ordinary pasted link, so gating hyperlinks on DO_NOT_RENDER alone left
// every received link as plain, unlinkified text. The length check keeps a
// preview chip that covers NO text (a card attached to the message rather than
// to a span) from emitting a stray, empty <a href></a>.
func inlineURLChip(a *pb.Annotation) bool {
	return a.GetUrlMetadata() != nil && a.GetLength() > 0
}
