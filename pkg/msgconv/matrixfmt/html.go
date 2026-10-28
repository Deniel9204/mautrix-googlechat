// Package matrixfmt converts Matrix HTML (or a plain @room body) into Google Chat
// text_body + annotations. Offsets are UTF-16 code units, tracked exactly by EntityString.
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

// utf16Text is a UTF-16 code-unit sequence, the unit Google Chat annotation offsets use.
type utf16Text []uint16

func newUTF16Text(s string) utf16Text {
	return utf16.Encode([]rune(s))
}

func (t utf16Text) String() string {
	return string(utf16.Decode(t))
}

// EntityString pairs UTF-16 text with the entities covering it; every mutation shifts
// entity offsets in the same step.
type EntityString struct {
	Text     utf16Text
	Entities BodyRangeList
}

func NewEntityString(val string) *EntityString {
	return &EntityString{Text: newUTF16Text(val)}
}

// TextString returns the text, or "" for a nil receiver.
func (es *EntityString) TextString() string {
	if es == nil {
		return ""
	}
	return es.Text.String()
}

// Split splits es on the ASCII code unit at, truncating and shifting entities.
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

// isTrimmableSpace reports whether u is ASCII whitespace, NEL or NBSP.
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
	// Clamp entities to [cutStart, cutEnd) before shifting, or Start can go negative.
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

// JoinEntityString joins strings with a separator, shifting entities by the running length.
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
		// Write the separator even for nil/empty items so empty list items keep their blank line.
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
	// Strip the one trailing separator, or a list's LIST annotation ends up one unit too long.
	if wroteAny && len(withUnits) > 0 {
		text = text[:len(text)-len(withUnits)]
	}
	return &EntityString{Text: text, Entities: entities}
}

// Format wraps the whole string in a new entity. A nil receiver stays nil.
func (es *EntityString) Format(value BodyRangeValue) *EntityString {
	if es == nil {
		return nil
	}
	newEntity := BodyRange{Start: 0, Length: len(es.Text), Value: value}
	es.Entities = append(BodyRangeList{newEntity}, es.Entities...)
	return es
}

// Append concatenates other onto es, shifting other's entities.
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

// TagStack tracks the enclosing HTML tag names during the tree walk.
type TagStack []string

func (ts TagStack) Has(tag string) bool {
	for i := len(ts) - 1; i >= 0; i-- {
		if ts[i] == tag {
			return true
		}
	}
	return false
}

// Context carries per-recursion state. A nil AllowedMentions means no restriction.
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

// HTMLParser is a Matrix HTML -> EntityString parser.
type HTMLParser struct {
	// GetUIDFromMXID resolves a Matrix user ID to a gaia id, or "". May be nil.
	GetUIDFromMXID func(context.Context, id.UserID) string
}

// TaggedString pairs an *EntityString with the HTML tag name it came from.
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

// Digits counts the digits in num, plus one for a negative sign.
func Digits(num int) int {
	if num == 0 {
		return 1
	} else if num < 0 {
		return Digits(-num) + 1
	}
	return int(math.Floor(math.Log10(float64(num))) + 1)
}

func (parser *HTMLParser) listToString(node *html.Node, ctx Context) *EntityString {
	if node.Data == "ol" {
		return parser.orderedListToString(node, ctx)
	}
	return parser.unorderedListToString(node, ctx)
}

// orderedListToString renders "N. " prefixes with aligned continuation lines and no
// annotations (Google Chat has no ordered-list format).
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
			// Can happen with a negative start=.
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
	// children holds every line of every <li>, already indented.
	return JoinEntityString("\n", children...)
}

// unorderedListToString wraps each <li> in LIST_ITEM and the whole list in LIST, no text prefix.
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

// basicFormatToString handles b/strong, i/em, s/strike/del, u/ins. <tt> is intentionally not monospace.
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

// codeToString handles inline <code> as whitespace-preserving MONOSPACE.
func (parser *HTMLParser) codeToString(node *html.Node, ctx Context) *EntityString {
	return parser.nodeToString(node.FirstChild, ctx.WithWhitespace()).Format(StyleMonospace)
}

// preToString handles <pre> (unwrapping a sole <code> child) as MONOSPACE_BLOCK.
func (parser *HTMLParser) preToString(node *html.Node, ctx Context) *EntityString {
	inner := node
	if node.FirstChild != nil && node.FirstChild.Type == html.ElementNode && node.FirstChild.Data == "code" {
		inner = node.FirstChild
	}
	return parser.nodeToString(inner.FirstChild, ctx.WithWhitespace()).Format(StyleMonospaceBlock)
}

// colorAttribute reads color, then data-mx-color, then a CSS style color.
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

// extractCSSColor finds a "color:" declaration in a style attribute; not a general CSS parser.
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

// colorToFontColor inverts gchatfmt's (rgb+2^31)&0xFFFFFF transform. int64 avoids
// int32 overflow on 1<<31.
func colorToFontColor(color string) (int32, bool) {
	color = strings.TrimLeft(color, "#")
	parsed, err := strconv.ParseInt(color, 16, 64)
	if err != nil {
		return 0, false
	}
	rgbInt := (parsed | 0x7F000000) - (1 << 31)
	return int32(rgbInt), true
}

// spanToString handles <span>/<font>, mapping color to FontColor. Spoilers are ignored.
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

// linkToString handles <a href>: mailto: yields the bare address with no annotation;
// an allowed, resolvable user pill becomes a Mention on "@" + text (the "@" is not
// doubled); everything else becomes a URL annotation, even when text equals href.
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
					// Strip any leading "@" so the sigil appears exactly once.
					return NewEntityString("@" + strings.TrimPrefix(str.TextString(), "@")).Format(Mention{GaiaID: gaiaID})
				}
			}
		}
		// Not allowed or unresolvable: plain text, no annotation.
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
		// Extra trailing newline gives a blank line between paragraphs.
		return parser.nodeToTagAwareString(node.FirstChild, ctx).AppendString("\n")
	case "hr":
		return NewEntityString("---")
	case "pre":
		return parser.preToString(node, ctx)
	default:
		return parser.nodeToTagAwareString(node.FirstChild, ctx)
	}
}

const (
	mxRoomMention = "@room"
	gcRoomMention = "@all"
)

var whitespaceRun = regexp.MustCompile(`\s+`)

func collapseWhitespace(s string) string {
	return whitespaceRun.ReplaceAllString(s, " ")
}

// textToEntityString converts a text node, turning each "@room" into a MentionAll
// "@all" (outside <pre>/<code>, and only if m.mentions allows room) and collapsing whitespace.
func textToEntityString(text string, ctx Context) *EntityString {
	if !ctx.PreserveWhitespace {
		if idx := strings.Index(text, mxRoomMention); idx >= 0 {
			if ctx.AllowedMentions == nil || ctx.AllowedMentions.Room {
				prefix := text[:idx]
				suffix := text[idx+len(mxRoomMention):]
				return JoinEntityString("",
					textToEntityString(prefix, ctx),
					NewEntityString(gcRoomMention).Format(MentionAll{}),
					textToEntityString(suffix, ctx),
				)
			}
		}
		text = collapseWhitespace(text)
	}
	return NewEntityString(text)
}

func (parser *HTMLParser) singleNodeToString(node *html.Node, ctx Context) TaggedString {
	switch node.Type {
	case html.TextNode:
		return TaggedString{textToEntityString(node.Data, ctx), "text"}
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

// BlockTags is the set of block-level HTML tags.
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
	// One-way latch: only the first block child gets a leading "\n", or adjacent <p>s get two blank lines.
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

// nodeToString joins node's children with no separator (no " " is injected into <pre>/<code>).
func (parser *HTMLParser) nodeToString(node *html.Node, ctx Context) *EntityString {
	return JoinEntityString("", parser.nodeToStrings(node, ctx)...)
}

// Parse converts Matrix HTML into an EntityString.
func (parser *HTMLParser) Parse(htmlData string, ctx Context) *EntityString {
	node, _ := html.Parse(strings.NewReader(htmlData))
	return parser.nodeToTagAwareString(node, ctx)
}
