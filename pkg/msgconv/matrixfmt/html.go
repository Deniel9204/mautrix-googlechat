// Package matrixfmt converts a Matrix message's HTML formatted_body (or
// plain body, for the @room special case) into Google Chat's text_body +
// annotations representation. It is a port of
// _reference/googlechat-python/mautrix_googlechat/formatter/from_matrix/
// (__init__.py, parser.py, gc_message.py), structurally adopted from
// _reference/googlechat-megabridge/pkg/msgconv/matrixfmt/
// {html,tree,tags,convert}.go per docs/research/08d-megabridge-msgconv.md
// §2/§6 ("Adopt-and-fix: yes").
//
// # UTF-16 offsets: EntityString, not a placeholder-locator
//
// docs/research/07-gap-analysis.md:39,350 describe the outgoing-mention
// offset problem as needing "the mention placeholder-locator trick" (insert
// a random placeholder token, render the full string, search for the
// placeholder, compute its UTF-16 offset, substitute the real text). That
// is what mautrix-meta's textfmt.parseMetaMentions actually does (see
// docs/research/05-meta-bridge-blueprint.md:441-447 and
// _reference/meta/pkg/msgconv/textfmt/mentions.go) -- but it is not what
// this bridge's own Python source does. mautrix_googlechat's from_matrix
// package is built on mautrix.util.formatter.{MatrixParser,EntityString}
// (confirmed by downloading the exact pinned dependency,
// mautrix==0.20.8 -- googlechat-python/setup.py pins
// "mautrix>=0.20.8,<0.21" -- and reading
// mautrix/util/formatter/{parser,entity_string}.py from the wheel): every
// HTML node is converted bottom-up into an EntityString (text + a list of
// entities, each an offset/length/type), and EntityString.append/prepend/
// split/join (entity_string.py:94-162) shift every existing entity's
// offset by the length of whatever text is being spliced in, in the same
// operation. There is never a placeholder token anywhere in this pipeline
// -- offsets are exact by construction the moment two pieces are joined,
// because "how long is the text so far" is always known precisely (in
// UTF-16 code units here, vs. Python's str length after its own
// surrogate-pair-expansion trick, add_surrogate/del_surrogate in
// ../util.py, which is Python's version of the same "operate in UTF-16
// code-unit space, never code points" invariant).
//
// docs/research/08d-megabridge-msgconv.md:95 independently reaches the same
// conclusion reviewing megabridge's Go port: "This is a different mechanism
// than the 'mention placeholder-locator trick' ... but it solves the same
// problem -- offsets are computed natively in UTF-16 code units at
// composition time -- and is architecturally sounder." EntityString
// (below) is that mechanism, ported here with the UTF-16 correctness
// property this package's tests exist to prove (see convert_test.go's
// astral-emoji-before-a-mention case): every join/split/trim/append
// operation adjusts every entity's Start in the same "how many UTF-16 code
// units came before this" arithmetic, so an offset is never computed by
// scanning rendered output for a marker -- it falls out of the tree walk
// automatically, correctly, every time.
//
// # Fixes applied over megabridge (docs/research/08d-megabridge-msgconv.md §2.1, §6)
//
//   - URL-loss bug (the reason this task exists): megabridge's linkToString
//     either matched the mention branch, returned the anchor text unchanged
//     when it equaled the href (silently dropping the href entirely for
//     the extremely common "bare URL" case, e.g. a Matrix client
//     auto-linkifying "https://example.com" text as
//     <a href="https://example.com">https://example.com</a>), or appended
//     " (href)" as unannotated plain text. Real Python's url_to_fstring
//     (mautrix/util/formatter/parser.py:163-164) always calls
//     msg.format(self.e.URL, url=url) -- unconditionally, regardless of
//     whether the displayed text equals the href. linkToString here does
//     the same: every non-empty, non-mailto:, non-user-pill href becomes a
//     URL{Href: href} annotation, full stop.
//   - <u>/<ins> dropped entirely (megabridge's basicFormatToString had a
//     case "u", "ins": return str with no .Format call) -- fixed to
//     .Format(StyleUnderline), matching basic_format_to_fstring's
//     UNDERLINE branch (parser.py:127-128) and GCEntityType.UNDERLINE
//     (gc_message.py:52) existing on the wire.
//   - <pre> emitted inline MONOSPACE (5) instead of MONOSPACE_BLOCK (7) --
//     fixed; see preToString.
//   - Color (data-mx-color / <font color> / this task's own additional
//     "style=color:#hex" ask) was not read at all -- spanToString did not
//     even look at node attributes. Added: colorAttribute + FontColor
//     (tags.go), ported from node_to_fstring's font/span branch
//     (mautrix/util/formatter/parser.py:218-236) and
//     color_to_fstring (from_matrix/parser.py:40-47).
//   - list annotations: megabridge rendered every <li> as literal "* "/
//     "N. " prefixed text for both <ul> and <ol>, with zero
//     BULLETED_LIST/BULLETED_LIST_ITEM annotations. Real Python's GC
//     MatrixParser.list_to_fstring (from_matrix/parser.py:62-71) only
//     does that for <ol> (delegating to the base class); <ul> instead
//     wraps each <li> in a LIST_ITEM entity and the whole joined list in
//     a LIST entity, with NO text prefix at all (Google Chat's own client
//     renders the bullet UI from the annotation). Ported exactly --
//     unorderedListToString vs. orderedListToString below.
//   - @room -> MENTION_ALL: entirely absent from megabridge. Ported from
//     the GC MatrixParser's text_to_fstring override (from_matrix/
//     parser.py:85-98) as textToEntityString's mxRoomMention branch.
//   - mailto: links: megabridge had no special case (a mailto: link fell
//     through to the generic "(href)" plain-text branch). Real Python's
//     link_to_fstring returns the bare address with NO annotation at all,
//     discarding the anchor's own text (mautrix/util/formatter/
//     parser.py:137-138 combined with GCMessage.format's EMAIL no-op,
//     gc_message.py:169-171) -- ported verbatim in linkToString.
//   - <tt> is not a Google Chat monospace trigger. megabridge treated
//     "tt", "code" identically (both -> StyleMonospace). Real Python's
//     basic_format_to_fstring tag list (parser.py:121-128) does not
//     include "tt" or "code" at all; "code" is handled by a completely
//     separate branch in node_to_fstring (parser.py:248-249) that also
//     switches into a whitespace-preserving RecursionContext
//     (ctx.enter_code_block()) -- "tt" has no case anywhere and falls to
//     the default (plain recursion, no formatting, normal whitespace
//     collapsing). Ported exactly: codeToString handles "code" only; "tt"
//     is not special-cased.
//
// # Deliberately out of scope here (left to later M3 tasks / the connector)
//
//   - <mx-reply> fallback stripping (parser.py:196-197 in the real base
//     class): standard mautrix-go bridgev2 practice is for the connector
//     to call content.RemoveReplyFallback() before ever handing content to
//     a formatter; Task 7 (quote-replies) owns that seam. Parse here
//     assumes any <mx-reply> has already been stripped by the caller.
//   - text_to_fstring's strip_leading_whitespace nuance (collapsing a
//     leading whitespace run in the text immediately following a block
//     tag's closing tag, parser.py:268-273): a purely cosmetic
//     whitespace-collapsing refinement with no effect on offsets,
//     annotations, or any behavior this task's tests exercise.
//   - parse_node's default " " join separator for <pre>/<code> content
//     with 2+ direct child nodes (entity_string.py:152, see nodeToString's
//     doc comment): reproducing it would inject unrequested spaces into
//     monospace content, which is the wrong direction to copy a quirk in;
//     documented as a deliberate deviation rather than silently ignored.
package matrixfmt

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf16"

	"golang.org/x/net/html"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

// utf16Text is a UTF-16 code-unit sequence, matching Google Chat's
// annotation start_index/length semantics (JS/Java String indexing, the
// same space Python's add_surrogate() trick puts str operations into). All
// EntityString offset arithmetic below operates on this type, never on Go
// byte or rune indices -- astral-plane characters (outside the Basic
// Multilingual Plane, e.g. most emoji) encode to a surrogate PAIR here, two
// code units, exactly as they would in JS/Python's UTF-16 string model.
type utf16Text []uint16

func newUTF16Text(s string) utf16Text {
	return utf16.Encode([]rune(s))
}

func (t utf16Text) String() string {
	return string(utf16.Decode(t))
}

// EntityString pairs a UTF-16 code-unit string with the list of formatting
// entities covering it. It is the Go analogue of
// mautrix.util.formatter.entity_string.EntityString: every operation that
// changes the string (Split, TrimSpace, Append, AppendString,
// JoinEntityString) adjusts every entity's Start/Length in the same step,
// so offsets are exact by construction -- see the package doc comment.
type EntityString struct {
	Text     utf16Text
	Entities BodyRangeList
}

func NewEntityString(val string) *EntityString {
	return &EntityString{Text: newUTF16Text(val)}
}

// TextString returns es.Text.String(), safe to call on a nil *EntityString
// (returns "") -- every EntityString-returning function here can return nil
// for empty content (e.g. an empty anchor), and megabridge's equivalent
// (str.String.String()) would panic on that nil-field-access in the same
// situation; this fixes that latent nil-dereference risk.
func (es *EntityString) TextString() string {
	if es == nil {
		return ""
	}
	return es.Text.String()
}

// Split splits es on every occurrence of the ASCII rune at (Google Chat
// annotation text never needs to split on anything outside ASCII -- this
// package only ever splits on '\n'), truncating/dropping/shifting every
// entity that crosses a split point exactly like EntityString.split
// (entity_string.py:137-150), just working in code units instead of code
// points.
func (es *EntityString) Split(at uint16) []*EntityString {
	if at > 0x7F {
		panic("cannot split at non-ASCII character")
	}
	if es == nil {
		return []*EntityString{}
	}
	var output []*EntityString
	prevSplit := 0
	doSplit := func(i int) *EntityString {
		newES := &EntityString{Text: es.Text[prevSplit:i]}
		for _, entity := range es.Entities {
			if (entity.End() <= i || entity.End() > prevSplit) && (entity.Start >= prevSplit || entity.Start < i) {
				entity = *entity.TruncateStart(prevSplit).TruncateEnd(i).Offset(-prevSplit)
				if entity.Length > 0 {
					newES.Entities = append(newES.Entities, entity)
				}
			}
		}
		return newES
	}
	for i, chr := range es.Text {
		if chr != at {
			continue
		}
		output = append(output, doSplit(i))
		prevSplit = i + 1
	}
	if prevSplit == 0 {
		return []*EntityString{es}
	}
	if prevSplit != len(es.Text) {
		output = append(output, doSplit(len(es.Text)))
	}
	return output
}

// asciiSpace matches the ASCII whitespace set entity_string doesn't need to
// enumerate itself (Python's str.strip() default set); TrimSpace mirrors
// EntityString.trim (entity_string.py:129-135).
func isTrimmableSpace(u uint16) bool {
	switch u {
	case '\t', '\n', '\v', '\f', '\r', ' ', 0x85, 0xA0:
		return true
	default:
		return false
	}
}

func (es *EntityString) TrimSpace() *EntityString {
	if es == nil {
		return nil
	}
	cutStart := 0
	for ; cutStart < len(es.Text); cutStart++ {
		if !isTrimmableSpace(es.Text[cutStart]) {
			break
		}
	}
	cutEnd := len(es.Text)
	for ; cutEnd > cutStart; cutEnd-- {
		if !isTrimmableSpace(es.Text[cutEnd-1]) {
			break
		}
	}
	if cutEnd == cutStart {
		return NewEntityString("")
	}
	if cutStart == 0 && cutEnd == len(es.Text) {
		return es
	}
	// Clamp each entity into the [cutStart, cutEnd) window being kept
	// BEFORE shifting into the trimmed string's coordinate space -- the
	// same TruncateStart/TruncateEnd-then-Offset order Split's doSplit
	// uses. megabridge's TrimSpace (and an earlier attempt here) called
	// Offset(-cutStart) FIRST and only ever TruncateEnd (no
	// TruncateStart), which silently produces a negative Start and/or an
	// out-of-bounds Length for any entity that started before cutStart --
	// caught by a standalone probe of this method (an entity spanning the
	// entire pre-trim string "  bold  " came out as {Start:-2 Length:6}
	// instead of {Start:0 Length:4}).
	newEntities := es.Entities[:0]
	for _, ent := range es.Entities {
		ent = *ent.TruncateStart(cutStart).TruncateEnd(cutEnd).Offset(-cutStart)
		if ent.Length > 0 {
			newEntities = append(newEntities, ent)
		}
	}
	es.Text = es.Text[cutStart:cutEnd]
	es.Entities = newEntities
	return es
}

// JoinEntityString joins strings with the separator with between each pair,
// shifting each string's entities by the running length so far -- the Go
// equivalent of EntityString.join (entity_string.py:152-162).
func JoinEntityString(with string, strings ...*EntityString) *EntityString {
	withUnits := newUTF16Text(with)
	totalLen := 0
	totalEntities := 0
	for _, s := range strings {
		if s == nil {
			continue
		}
		totalLen += len(s.Text)
		totalEntities += len(s.Entities)
	}
	text := make(utf16Text, 0, totalLen+len(strings)*len(withUnits))
	entities := make(BodyRangeList, 0, totalEntities)
	wroteAny := false
	for _, s := range strings {
		// entity_string.py:154-159 -- join writes `separator` for EVERY
		// item unconditionally, even one with empty text (Python's
		// EntityString objects are never nil/None; the empty case there
		// is just an EntityString with text=""). An earlier version of
		// this port (caught by the gchat-port-auditor re-audit of the
		// JoinEntityString fix above) instead `continue`d past any item
		// with zero-length text -- including a nil *EntityString, this
		// package's sentinel for "empty/absent content" (e.g. an empty
		// <li></li>, whose rendered content is nil because
		// nodeToTagAwareString(nil, ctx) has nothing to iterate) --
		// which skipped its separator slot entirely, silently merging it
		// into whatever came before/after instead of preserving it as a
		// blank line. <ul><li>foo</li><li></li><li>bar</li></ul> used to
		// render "foo\nbar" (the empty middle item vanished); it must
		// render "foo\n\nbar" (the empty item's own blank line survives).
		//
		// This does NOT fabricate an entity for the empty item -- unlike
		// Python's format(), which appends an entity even for
		// zero-length content (offset=0, length=0), this port's Format()
		// deliberately treats a nil receiver as "nothing to format" (see
		// its own doc comment) since a zero-length annotation is a
		// meaningless no-op on the wire; only the item's structural
		// position in the text (and hence its separator) needs to
		// survive, not a content-free entity.
		if s != nil {
			for _, entity := range s.Entities {
				entity.Start += len(text)
				entities = append(entities, entity)
			}
			text = append(text, s.Text...)
		}
		text = append(text, withUnits...)
		wroteAny = true
	}
	// entity_string.py:160-161 -- join appends `separator` after EVERY
	// item, then strips exactly ONE trailing occurrence once the loop
	// ends, not per item. A first attempt at this port (caught by the
	// gchat-port-auditor review) omitted that trailing strip entirely:
	// every join with a non-empty separator -- every list/blockquote/
	// ordered-list call site, all joined with "\n" -- left a dangling
	// separator at the end. That corrupted both the rendered text (an
	// extra newline whenever more content follows, e.g. a <ul> immediately
	// followed by a <p>) and, for <ul>, the enclosing LIST annotation's
	// own span: .Format() (unorderedListToString) wraps [0, len(text))
	// AFTER this trailing "\n" was appended, so the annotation ended up
	// one UTF-16 code unit too long. The bug was invisible in every
	// existing test because a list/blockquote as the ONLY/LAST content in
	// a message gets the dangling separator trimmed away by Parse's outer
	// TrimSpace anyway -- see convert_test.go's
	// "list or blockquote followed by more content" cases, added
	// specifically to catch a regression here.
	if wroteAny && len(withUnits) > 0 {
		text = text[:len(text)-len(withUnits)]
	}
	return &EntityString{Text: text, Entities: entities}
}

// Format wraps the ENTIRE current string in a new entity of the given
// value, mirroring EntityString.format (entity_string.py:116-127). A nil
// receiver (empty/absent content) is left nil -- formatting zero-length
// content produces a meaningless annotation, so it's dropped rather than
// fabricated.
func (es *EntityString) Format(value BodyRangeValue) *EntityString {
	if es == nil {
		return nil
	}
	newEntity := BodyRange{Start: 0, Length: len(es.Text), Value: value}
	es.Entities = append(BodyRangeList{newEntity}, es.Entities...)
	return es
}

// Append concatenates other onto es, shifting other's entities by len(es.Text)
// first -- EntityString.append (entity_string.py:94-101).
func (es *EntityString) Append(other *EntityString) *EntityString {
	if es == nil {
		return other
	} else if other == nil {
		return es
	}
	for _, entity := range other.Entities {
		entity.Start += len(es.Text)
		es.Entities = append(es.Entities, entity)
	}
	es.Text = append(es.Text, other.Text...)
	return es
}

// AppendString appends a plain (unformatted) string.
func (es *EntityString) AppendString(other string) *EntityString {
	if es == nil {
		return NewEntityString(other)
	} else if len(other) == 0 {
		return es
	}
	es.Text = append(es.Text, newUTF16Text(other)...)
	return es
}

// TagStack tracks the chain of enclosing HTML tag names during the tree
// walk (mirrors mautrix.util.formatter's RecursionContext.tag_stack
// concept as implemented by the sibling maunium.net/go/mautrix/format
// package's TagStack -- not currently read by anything in this package,
// kept for parity with megabridge and future use, e.g. detecting "are we
// inside a <pre>" without a dedicated context field).
type TagStack []string

func (ts TagStack) Has(tag string) bool {
	for i := len(ts) - 1; i >= 0; i-- {
		if ts[i] == tag {
			return true
		}
	}
	return false
}

// Context carries per-recursion state through the tree walk: the request
// context.Context, the Matrix event's m.mentions allow-list (nil means "no
// restriction" -- see linkToString), the enclosing tag chain, and whether
// whitespace should be preserved verbatim (set inside <pre>/<code>, mirrors
// RecursionContext.preserve_whitespace / enter_code_block(),
// parser.py:37-38).
type Context struct {
	Ctx                context.Context
	AllowedMentions    *event.Mentions
	TagStack           TagStack
	PreserveWhitespace bool
}

func NewContext(ctx context.Context) Context {
	return Context{Ctx: ctx, TagStack: make(TagStack, 0, 4)}
}

func (ctx Context) WithTag(tag string) Context {
	ctx.TagStack = append(ctx.TagStack, tag)
	return ctx
}

func (ctx Context) WithWhitespace() Context {
	ctx.PreserveWhitespace = true
	return ctx
}

// HTMLParser is a Matrix HTML -> EntityString parser, the Go analogue of
// the googlechat mautrix_googlechat.formatter.from_matrix.parser.MatrixParser
// class (which itself subclasses mautrix.util.formatter.MatrixParser).
type HTMLParser struct {
	// GetUIDFromMXID resolves a Matrix user ID (from a pill's
	// matrix.to href) to a Google Chat gaia id, or "" if unresolved.
	// May be nil (treated as always-unresolved). Wraps this package's
	// MentionResolver (see convert.go) into the shape linkToString wants.
	GetUIDFromMXID func(context.Context, id.UserID) string
}

// TaggedString pairs an *EntityString with the HTML tag name it came from,
// used by list/blockquote handling to filter for "li" children and by
// nodeToTagAwareString to decide whether to add block-tag newline padding.
type TaggedString struct {
	*EntityString
	tag string
}

func (parser *HTMLParser) maybeGetAttribute(node *html.Node, attribute string) (string, bool) {
	for _, attr := range node.Attr {
		if attr.Key == attribute {
			return attr.Val, true
		}
	}
	return "", false
}

func (parser *HTMLParser) getAttribute(node *html.Node, attribute string) string {
	val, _ := parser.maybeGetAttribute(node, attribute)
	return val
}

// Digits counts the number of digits (and the sign, if negative) in num --
// used for ordered-list continuation-line indent width, matching Python's
// len(str(longest_index)) (parser.py:86).
func Digits(num int) int {
	if num == 0 {
		return 1
	} else if num < 0 {
		return Digits(-num) + 1
	}
	return int(math.Floor(math.Log10(float64(num))) + 1)
}

// listToString dispatches <ol> to orderedListToString (numbered text
// prefix, no annotations) and <ul> to unorderedListToString
// (LIST/LIST_ITEM annotations, no text prefix) -- the GC
// MatrixParser.list_to_fstring override (from_matrix/parser.py:62-71):
// "if node.tag == 'ol': return await super().list_to_fstring(...)"
// (base class numbered-prefix behavior) vs. its own annotation-based ul
// handling.
func (parser *HTMLParser) listToString(node *html.Node, ctx Context) *EntityString {
	if node.Data == "ol" {
		return parser.orderedListToString(node, ctx)
	}
	return parser.unorderedListToString(node, ctx)
}

// orderedListToString ports mautrix.util.formatter.MatrixParser.list_to_fstring
// (parser.py:74-103) restricted to the ordered branch: "N. " prefixes
// (honoring a start="" attribute), continuation lines of a multi-line <li>
// indented to align under the prefix, no annotations at all (Google Chat
// has no ordered-list format type).
func (parser *HTMLParser) orderedListToString(node *html.Node, ctx Context) *EntityString {
	taggedChildren := parser.nodeToTaggedStrings(node.FirstChild, ctx)
	counter := 1
	if start := parser.getAttribute(node, "start"); start != "" {
		if v, err := strconv.Atoi(start); err == nil {
			counter = v
		}
	}
	longestIndex := (counter - 1) + len(taggedChildren)
	indentLength := Digits(longestIndex)
	indent := strings.Repeat(" ", indentLength+2)

	var children []*EntityString
	for _, child := range taggedChildren {
		if child.tag != "li" {
			continue
		}
		indexPadding := indentLength - Digits(counter)
		if indexPadding < 0 {
			// Can happen with a negative start= where longestIndex ends up
			// wrong; matches megabridge's identical guard.
			indexPadding = 0
		}
		prefix := fmt.Sprintf("%d. %s", counter, strings.Repeat(" ", indexPadding))
		counter++

		es := NewEntityString(prefix).Append(child.EntityString)
		parts := es.Split('\n')
		for i, part := range parts[1:] {
			parts[i+1] = NewEntityString(indent).Append(part)
		}
		children = append(children, parts...)
	}
	// children already contains every line of every <li> in order, each
	// continuation line pre-indented -- joining the whole flat list with
	// "\n" reproduces the same text as Python's per-li join-then-outer-join
	// (string concatenation with a uniform separator is associative), see
	// the package doc comment.
	return JoinEntityString("\n", children...)
}

// unorderedListToString ports the GC-specific <ul> branch of
// list_to_fstring (from_matrix/parser.py:65-71): each <li>'s content is
// wrapped in a LIST_ITEM entity (no text prefix), the whole thing joined
// with "\n" and wrapped in a LIST entity.
func (parser *HTMLParser) unorderedListToString(node *html.Node, ctx Context) *EntityString {
	taggedChildren := parser.nodeToTaggedStrings(node.FirstChild, ctx)
	var items []*EntityString
	for _, child := range taggedChildren {
		if child.tag != "li" {
			continue
		}
		items = append(items, child.EntityString.Format(StyleListItem))
	}
	return JoinEntityString("\n", items...).Format(StyleList)
}

// basicFormatToString handles b/strong, i/em, s/strike/del, u/ins -- ports
// basic_format_to_fstring (parser.py:117-129). "tt"/"code" are
// deliberately NOT handled here -- see the package doc comment and
// codeToString.
func (parser *HTMLParser) basicFormatToString(node *html.Node, ctx Context) *EntityString {
	str := parser.nodeToTagAwareString(node.FirstChild, ctx)
	switch node.Data {
	case "b", "strong":
		return str.Format(StyleBold)
	case "i", "em":
		return str.Format(StyleItalic)
	case "s", "strike", "del":
		return str.Format(StyleStrikethrough)
	case "u", "ins":
		return str.Format(StyleUnderline)
	}
	return str
}

// codeToString handles inline <code> -- ports node_to_fstring's dedicated
// "code" branch (parser.py:248-249): whitespace-preserving recursion
// (ctx.enter_code_block()) wrapped in INLINE_CODE (MONOSPACE). Unlike
// <pre>, there's no code-inside-code unwrapping to do.
func (parser *HTMLParser) codeToString(node *html.Node, ctx Context) *EntityString {
	return parser.nodeToString(node.FirstChild, ctx.WithWhitespace()).Format(StyleMonospace)
}

// preToString handles <pre> -- ports node_to_fstring's "pre" branch
// (parser.py:237-247): if the sole/first child is a <code> element, unwrap
// it (use ITS content, not a nested "code inside pre" render) and use its
// language class only to detect the unwrap, since GC's FormatMetadata
// carries no language field; whitespace preserved; wrapped in
// PREFORMATTED (MONOSPACE_BLOCK) -- megabridge emitted plain MONOSPACE
// here (docs/research/08d-megabridge-msgconv.md §2.1), fixed.
func (parser *HTMLParser) preToString(node *html.Node, ctx Context) *EntityString {
	inner := node
	if node.FirstChild != nil && node.FirstChild.Type == html.ElementNode && node.FirstChild.Data == "code" {
		inner = node.FirstChild
	}
	return parser.nodeToString(inner.FirstChild, ctx.WithWhitespace()).Format(StyleMonospaceBlock)
}

// colorAttribute extracts a color value from a <span>/<font> node. Ports
// node_to_fstring's font/span branch (mautrix/util/formatter/parser.py:
// 227-233): "color" attribute wins over "data-mx-color" when both are
// present. Beyond Python (per this task's explicit ask): falls back to a
// CSS "style=color:#hex" declaration as a third tier -- real Matrix
// clients (e.g. Element) sometimes only set style=, not data-mx-color.
func (parser *HTMLParser) colorAttribute(node *html.Node) (string, bool) {
	if v, ok := parser.maybeGetAttribute(node, "color"); ok && v != "" {
		return v, true
	}
	if v, ok := parser.maybeGetAttribute(node, "data-mx-color"); ok && v != "" {
		return v, true
	}
	if style, ok := parser.maybeGetAttribute(node, "style"); ok {
		if v, ok := extractCSSColor(style); ok {
			return v, true
		}
	}
	return "", false
}

// extractCSSColor finds a "color:" declaration (not "background-color:")
// in a CSS style attribute value. Deliberately minimal -- not a general CSS
// parser, just enough to pull "#rrggbb" (or similar) out of
// `style="color:#ff0000;font-weight:bold"`.
func extractCSSColor(style string) (string, bool) {
	for _, decl := range strings.Split(style, ";") {
		parts := strings.SplitN(decl, ":", 2)
		if len(parts) != 2 {
			continue
		}
		if strings.TrimSpace(parts[0]) == "color" {
			return strings.TrimSpace(parts[1]), true
		}
	}
	return "", false
}

// colorToFontColor is the exact inverse of gchatfmt's
// (rgb+2^31)&0xFFFFFF transform (pkg/msgconv/gchatfmt/convert.go's
// renderFormat FONT_COLOR case), porting color_to_fstring
// (from_matrix/parser.py:40-47) exactly: parse the (optionally
// "#"-prefixed) hex string as an integer, OR it with 0x7F000000, subtract
// 2**31. Python does this with arbitrary-precision integers; the int64
// intermediate here avoids the int32 overflow a literal 1<<31 would hit
// (mirrors gchatfmt's own int64-before-mask comment for the same reason).
// A malformed color is not an error (matches Python's `except ValueError:
// return msg` -- the message is left unformatted, never panics or
// substitutes a bogus value).
func colorToFontColor(color string) (int32, bool) {
	color = strings.TrimLeft(color, "#")
	parsed, err := strconv.ParseInt(color, 16, 64)
	if err != nil {
		return 0, false
	}
	rgbInt := (parsed | 0x7F000000) - (1 << 31)
	return int32(rgbInt), true
}

// spanToString handles <span> and <font>. data-mx-spoiler is intentionally
// ignored -- Google Chat has no spoiler concept, and the GC MatrixParser's
// own spoiler_to_fstring override is a no-op (from_matrix/parser.py:59-60:
// "return msg", i.e. render the content plainly). Color is read via
// colorAttribute and turned into a FontColor entity when parseable.
func (parser *HTMLParser) spanToString(node *html.Node, ctx Context) *EntityString {
	str := parser.nodeToTagAwareString(node.FirstChild, ctx)
	color, ok := parser.colorAttribute(node)
	if !ok {
		return str
	}
	rgb, ok := colorToFontColor(color)
	if !ok {
		return str
	}
	return str.Format(FontColor{RGB: rgb})
}

func (parser *HTMLParser) headerToString(node *html.Node, ctx Context) *EntityString {
	length := int(node.Data[1] - '0')
	prefix := strings.Repeat("#", length) + " "
	return NewEntityString(prefix).Append(parser.nodeToString(node.FirstChild, ctx)).Format(StyleBold)
}

func (parser *HTMLParser) blockquoteToString(node *html.Node, ctx Context) *EntityString {
	str := parser.nodeToTagAwareString(node.FirstChild, ctx)
	children := str.TrimSpace().Split('\n')
	for i, child := range children {
		children[i] = NewEntityString("> ").Append(child)
	}
	return JoinEntityString("\n", children...)
}

// linkToString handles <a href>. Ports link_to_fstring (mautrix/util/
// formatter/parser.py:131-161) combined with the GC MatrixParser's
// user_pill_to_fstring/room_pill_to_fstring overrides
// (from_matrix/parser.py:49-57):
//
//  1. No href -> just the anchor's rendered content (link_to_fstring:134-135).
//  2. mailto: -> the bare address (href with "mailto:" stripped), discarding
//     the anchor's own text entirely and producing NO annotation --
//     link_to_fstring:137-138 formats with EntityType.EMAIL, and
//     GCMessage.format no-ops for EMAIL (gc_message.py:169-171:
//     "if entity_type == GCEntityType.EMAIL: return self").
//  3. A matrix.to/matrix: URI naming a user (sigil '@') -> a mention pill,
//     gated by content.Mentions (the m.mentions allow-list, when present)
//     and by the MentionResolver seam (GetUIDFromMXID). "@" + the anchor's
//     own rendered text becomes a Mention entity; the "@" prefix is a
//     deliberate deviation from the real (self-admittedly incomplete --
//     see its "# TODO remove potential Google Chat suffix from
//     displayname" / "# TODO convert Matrix mentions of Google Chat users
//     to GC mentions" comments, from_matrix/parser.py:50-51)
//     user_pill_to_fstring, matching megabridge's better-than-Python
//     behavior instead (docs/research/08d-megabridge-msgconv.md §2.1:
//     "better than Python: ghosts and real Matrix users") and Google
//     Chat's own convention of always writing "@Name" as the literal text
//     under a MENTION annotation (confirmed by gchatfmt's own test
//     fixtures, e.g. pkg/msgconv/gchatfmt/convert_test.go's "@Bob hi").
//     Any other Matrix URI (room, event) falls through to case 4, exactly
//     like Python (room_pill_to_fstring/event_link_to_fstring return None
//     in the base class and neither is overridden by GC, so link_to_fstring
//     falls through to url_to_fstring with the original href).
//  4. Otherwise -> a URL{Href: href} annotation, UNCONDITIONALLY -- this is
//     the fix for the URL-loss bug (see the package doc comment).
func (parser *HTMLParser) linkToString(node *html.Node, ctx Context) *EntityString {
	str := parser.nodeToTagAwareString(node.FirstChild, ctx)
	href := parser.getAttribute(node, "href")
	if href == "" {
		return str
	}
	if addr, ok := strings.CutPrefix(href, "mailto:"); ok {
		return NewEntityString(addr)
	}
	if parsedMatrix, err := id.ParseMatrixURIOrMatrixToURL(href); err == nil && parsedMatrix != nil && parsedMatrix.Sigil1 == '@' {
		mxid := parsedMatrix.UserID()
		if ctx.AllowedMentions == nil || ctx.AllowedMentions.Has(mxid) {
			if parser.GetUIDFromMXID != nil {
				if gaiaID := parser.GetUIDFromMXID(ctx.Ctx, mxid); gaiaID != "" {
					return NewEntityString("@" + str.TextString()).Format(Mention{GaiaID: gaiaID})
				}
			}
		}
		// Mention not allowed/resolvable -- fall through to plain text,
		// per this task's MentionResolver contract: no annotation, nothing
		// dropped.
		return str
	}
	return str.Format(URL{Href: href})
}

func (parser *HTMLParser) tagToString(node *html.Node, ctx Context) *EntityString {
	ctx = ctx.WithTag(node.Data)
	switch node.Data {
	case "blockquote":
		return parser.blockquoteToString(node, ctx)
	case "ol", "ul":
		return parser.listToString(node, ctx)
	case "h1", "h2", "h3", "h4", "h5", "h6":
		return parser.headerToString(node, ctx)
	case "br":
		return NewEntityString("\n")
	case "b", "strong", "i", "em", "s", "strike", "del", "u", "ins":
		return parser.basicFormatToString(node, ctx)
	case "code":
		return parser.codeToString(node, ctx)
	case "span", "font":
		return parser.spanToString(node, ctx)
	case "a":
		return parser.linkToString(node, ctx)
	case "p":
		// parser.py:216-217: "(await self.tag_aware_parse_node(node, ctx)).append('\n')"
		// -- an extra trailing newline on top of the generic block-tag
		// wrapping nodeToTagAwareString already adds one level up,
		// producing a blank line between consecutive paragraphs.
		return parser.nodeToTagAwareString(node.FirstChild, ctx).AppendString("\n")
	case "hr":
		return NewEntityString("---")
	case "pre":
		return parser.preToString(node, ctx)
	default:
		return parser.nodeToTagAwareString(node.FirstChild, ctx)
	}
}

// mxRoomMention / gcRoomMention: ported from
// mautrix_googlechat/formatter/from_matrix/parser.py's module-level
// MX_ROOM_MENTION / GC_ROOM_MENTION constants.
const (
	mxRoomMention = "@room"
	gcRoomMention = "@all"
)

var whitespaceRun = regexp.MustCompile(`\s+`)

func collapseWhitespace(s string) string {
	return whitespaceRun.ReplaceAllString(s, " ")
}

// textToEntityString converts a single HTML text node into an EntityString,
// porting the GC MatrixParser's text_to_fstring override
// (from_matrix/parser.py:85-98) exactly, in the same order: check for a
// literal "@room" BEFORE collapsing whitespace (and only when not
// preserving whitespace, i.e. not inside <pre>/<code>); if found, split
// around the FIRST occurrence, recurse on the prefix and suffix (so
// multiple "@room"s in one text node each become their own MENTION_ALL),
// and concatenate prefix + "@all"-as-MentionAll + suffix. Otherwise (or
// once no more "@room" is found), collapse runs of whitespace into a
// single space, matching mautrix.util.formatter's base text_to_fstring
// (parser.py:252-257) -- the strip_leading_whitespace parameter it also
// takes is not ported, see the package doc comment.
func textToEntityString(text string, preserveWhitespace bool) *EntityString {
	if !preserveWhitespace {
		if idx := strings.Index(text, mxRoomMention); idx >= 0 {
			prefix := text[:idx]
			suffix := text[idx+len(mxRoomMention):]
			return JoinEntityString("",
				textToEntityString(prefix, preserveWhitespace),
				NewEntityString(gcRoomMention).Format(MentionAll{}),
				textToEntityString(suffix, preserveWhitespace),
			)
		}
		text = collapseWhitespace(text)
	}
	return NewEntityString(text)
}

func (parser *HTMLParser) singleNodeToString(node *html.Node, ctx Context) TaggedString {
	switch node.Type {
	case html.TextNode:
		return TaggedString{textToEntityString(node.Data, ctx.PreserveWhitespace), "text"}
	case html.ElementNode:
		return TaggedString{parser.tagToString(node, ctx), node.Data}
	case html.DocumentNode:
		return TaggedString{parser.nodeToTagAwareString(node.FirstChild, ctx), "html"}
	default:
		return TaggedString{&EntityString{}, "unknown"}
	}
}

func (parser *HTMLParser) nodeToTaggedStrings(node *html.Node, ctx Context) (strs []TaggedString) {
	for ; node != nil; node = node.NextSibling {
		strs = append(strs, parser.singleNodeToString(node, ctx))
	}
	return
}

// BlockTags mirrors mautrix.util.formatter.MatrixParser.block_tags
// (parser.py:47-63) exactly, including "li" (which in practice is always
// consumed by listToString before nodeToTagAwareString ever sees it as a
// direct child, but is included for fidelity with a stray/malformed <li>
// outside a list).
var BlockTags = []string{"p", "pre", "blockquote", "ol", "ul", "li", "h1", "h2", "h3", "h4", "h5", "h6", "div", "hr", "table"}

func (parser *HTMLParser) isBlockTag(tag string) bool {
	for _, blockTag := range BlockTags {
		if tag == blockTag {
			return true
		}
	}
	return false
}

func (parser *HTMLParser) nodeToTagAwareString(node *html.Node, ctx Context) *EntityString {
	strs := parser.nodeToTaggedStrings(node, ctx)
	var output *EntityString
	// prevWasBlock ports tag_aware_parse_node's prev_was_block
	// (parser.py:283-289) EXACTLY, including its surprising real-Python
	// behavior: it is a one-way latch, never reset back to false for a
	// later non-block sibling. Every block-tag child always gets its
	// trailing "\n"; only the FIRST block-tag child seen among ALL of
	// node's children (block or not) also gets a leading "\n" prepended.
	// An earlier version of this port (caught by the gchat-port-auditor
	// review) unconditionally prepended AND appended "\n" for every block
	// child with no state tracking at all, which double-counts the
	// leading newline for every block sibling after the first: two
	// adjacent <p> elements (the extremely common case a Matrix client
	// emits for a blank-line-separated multi-paragraph plain-text
	// message) rendered as "a\n\n\nb" (two blank lines) instead of the
	// correct "a\n\nb" (one) -- and compounded further whenever a
	// list/blockquote (whose own handlers, e.g. <p>'s trailing
	// AppendString("\n"), already contribute one of the two newlines a
	// block wrap is supposed to produce) was one of the siblings.
	prevWasBlock := false
	for _, str := range strs {
		tstr := str.EntityString
		if parser.isBlockTag(str.tag) {
			tstr = tstr.AppendString("\n")
			if !prevWasBlock {
				tstr = NewEntityString("\n").Append(tstr)
			}
			prevWasBlock = true
		}
		if output == nil {
			output = tstr
		} else {
			output = output.Append(tstr)
		}
	}
	return output.TrimSpace()
}

func (parser *HTMLParser) nodeToStrings(node *html.Node, ctx Context) (strs []*EntityString) {
	for ; node != nil; node = node.NextSibling {
		strs = append(strs, parser.singleNodeToString(node, ctx).EntityString)
	}
	return
}

// nodeToString joins node's children with NO separator. Used by
// headerToString (matches Python: header_to_fstring calls
// self.fs.join(children, "") explicitly, parser.py:115) and by
// codeToString/preToString (a deliberate, acknowledged deviation: Python's
// parse_node, used only for <pre>/<code>, calls self.fs.join(items) with
// NO separator argument, which defaults to " " (entity_string.py:152) --
// for the overwhelmingly common case of a single flat text child this
// makes no observable difference, but a <code>/<pre> containing 2+ direct
// child nodes, e.g. <code>foo<b>bar</b>baz</code>, would get real Python's
// spurious injected spaces ("foo bar baz") that this port does not
// reproduce ("foobarbaz", matching the source exactly). Flagged by the
// gchat-port-auditor review as undocumented; left as Go's behavior rather
// than "fixed" to match Python, since inserting unrequested whitespace
// into a monospace/code block is arguably the wrong direction to copy a
// quirk in.
func (parser *HTMLParser) nodeToString(node *html.Node, ctx Context) *EntityString {
	return JoinEntityString("", parser.nodeToStrings(node, ctx)...)
}

// Parse converts Matrix HTML into an EntityString using this parser's
// settings.
func (parser *HTMLParser) Parse(htmlData string, ctx Context) *EntityString {
	node, _ := html.Parse(strings.NewReader(htmlData))
	return parser.nodeToTagAwareString(node, ctx)
}
