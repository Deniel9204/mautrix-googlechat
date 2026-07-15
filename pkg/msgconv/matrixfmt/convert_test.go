package matrixfmt_test

import (
	"context"
	"sort"
	"testing"

	ggproto "google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv/gchatfmt"
	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv/matrixfmt"
)

// htmlContent builds an HTML-formatted MessageEventContent the way a real
// Matrix client would (body is a plain-text fallback rendition; tests only
// assert on the HTML path's output so the body value itself is arbitrary
// but non-empty, matching real traffic).
func htmlContent(formattedBody string) *event.MessageEventContent {
	return &event.MessageEventContent{
		MsgType:       event.MsgText,
		Body:          "plain-text fallback",
		Format:        event.FormatHTML,
		FormattedBody: formattedBody,
	}
}

// assertAnnotations compares two annotation slices for equal content,
// ignoring order -- BodyRangeList construction order (outermost entity
// first, per EntityString.Format prepending) is an implementation detail
// this package's tests should not be pinned to; Google Chat's wire format
// has no ordering requirement on the annotations array either.
func assertAnnotations(t *testing.T, got, want []*pb.Annotation) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("annotation count = %d, want %d\ngot:  %s\nwant: %s", len(got), len(want), formatAnnotations(got), formatAnnotations(want))
	}
	gotSorted := append([]*pb.Annotation(nil), got...)
	wantSorted := append([]*pb.Annotation(nil), want...)
	byPos := func(anns []*pb.Annotation) func(i, j int) bool {
		return func(i, j int) bool {
			if anns[i].GetStartIndex() != anns[j].GetStartIndex() {
				return anns[i].GetStartIndex() < anns[j].GetStartIndex()
			}
			if anns[i].GetLength() != anns[j].GetLength() {
				return anns[i].GetLength() < anns[j].GetLength()
			}
			return anns[i].GetType() < anns[j].GetType()
		}
	}
	sort.SliceStable(gotSorted, byPos(gotSorted))
	sort.SliceStable(wantSorted, byPos(wantSorted))
	for i := range wantSorted {
		if !ggproto.Equal(gotSorted[i], wantSorted[i]) {
			t.Errorf("annotation[%d] = %s, want %s", i, gotSorted[i], wantSorted[i])
		}
	}
}

func formatAnnotations(anns []*pb.Annotation) string {
	s := "["
	for i, a := range anns {
		if i > 0 {
			s += ", "
		}
		s += a.String()
	}
	return s + "]"
}

// TestParse is the table-driven core of the behavior inventory: one case
// per mandatory M3 Task 2 behavior. See html.go/convert.go's doc comments
// for the Python line references each case ports.
func TestParse(t *testing.T) {
	tests := []struct {
		name        string
		content     *event.MessageEventContent
		mention     matrixfmt.MentionResolver
		wantText    string
		wantAnnotes []*pb.Annotation
	}{
		{
			name: "plain body, no formatted_body -- text only, no annotations",
			content: &event.MessageEventContent{
				MsgType: event.MsgText,
				Body:    "Hello, World!",
			},
			wantText: "Hello, World!",
		},
		{
			name:     "plain body, empty formatted_body -- treated as plain",
			content:  &event.MessageEventContent{MsgType: event.MsgText, Body: "hi", Format: event.FormatHTML, FormattedBody: ""},
			wantText: "hi",
		},
		{
			name:     "bold",
			content:  htmlContent("a <strong>b</strong> c"),
			wantText: "a b c",
			wantAnnotes: []*pb.Annotation{
				gchatfmt.MakeFormatAnnotation(2, 1, pb.FormatMetadata_BOLD),
			},
		},
		{
			name:     "italic",
			content:  htmlContent("a <em>b</em> c"),
			wantText: "a b c",
			wantAnnotes: []*pb.Annotation{
				gchatfmt.MakeFormatAnnotation(2, 1, pb.FormatMetadata_ITALIC),
			},
		},
		{
			name:     "underline (megabridge dropped this entirely -- regression test)",
			content:  htmlContent("a <u>b</u> c"),
			wantText: "a b c",
			wantAnnotes: []*pb.Annotation{
				gchatfmt.MakeFormatAnnotation(2, 1, pb.FormatMetadata_UNDERLINE),
			},
		},
		{
			name:     "strikethrough via <del>",
			content:  htmlContent("a <del>b</del> c"),
			wantText: "a b c",
			wantAnnotes: []*pb.Annotation{
				gchatfmt.MakeFormatAnnotation(2, 1, pb.FormatMetadata_STRIKE),
			},
		},
		{
			name:     "strikethrough via <s> and <strike>",
			content:  htmlContent("<s>a</s> <strike>b</strike>"),
			wantText: "a b",
			wantAnnotes: []*pb.Annotation{
				gchatfmt.MakeFormatAnnotation(0, 1, pb.FormatMetadata_STRIKE),
				gchatfmt.MakeFormatAnnotation(2, 1, pb.FormatMetadata_STRIKE),
			},
		},
		{
			name:     "inline code -- <code> maps to MONOSPACE",
			content:  htmlContent("a <code>b</code> c"),
			wantText: "a b c",
			wantAnnotes: []*pb.Annotation{
				gchatfmt.MakeFormatAnnotation(2, 1, pb.FormatMetadata_MONOSPACE),
			},
		},
		{
			name:     "<tt> is not a monospace trigger in real Python -- no annotation",
			content:  htmlContent("a <tt>b</tt> c"),
			wantText: "a b c",
		},
		{
			name:     "<pre> maps to MONOSPACE_BLOCK, not inline MONOSPACE (megabridge bug)",
			content:  htmlContent("<pre>block</pre>"),
			wantText: "block",
			wantAnnotes: []*pb.Annotation{
				gchatfmt.MakeFormatAnnotation(0, 5, pb.FormatMetadata_MONOSPACE_BLOCK),
			},
		},
		{
			name:     "<pre><code> unwraps the inner code tag -- one MONOSPACE_BLOCK, not nested MONOSPACE",
			content:  htmlContent(`<pre><code class="language-go">block</code></pre>`),
			wantText: "block",
			wantAnnotes: []*pb.Annotation{
				gchatfmt.MakeFormatAnnotation(0, 5, pb.FormatMetadata_MONOSPACE_BLOCK),
			},
		},
		{
			// Nested tags: bold containing italic.
			name:     "nested tags -- bold containing italic",
			content:  htmlContent("<strong>bo<em>ld</em></strong>"),
			wantText: "bold",
			wantAnnotes: []*pb.Annotation{
				gchatfmt.MakeFormatAnnotation(0, 4, pb.FormatMetadata_BOLD),
				gchatfmt.MakeFormatAnnotation(2, 2, pb.FormatMetadata_ITALIC),
			},
		},
		{
			name:     "font color via data-mx-color -- inverse of gchatfmt's (rgb+2^31)&0xFFFFFF",
			content:  htmlContent(`<span data-mx-color="#ff0000">red</span>`),
			wantText: "red",
			wantAnnotes: []*pb.Annotation{
				gchatfmt.MakeFontColorAnnotation(0, 3, -65536),
			},
		},
		{
			name:     "font color via <font color> attribute",
			content:  htmlContent(`<font color="#ff0000">red</font>`),
			wantText: "red",
			wantAnnotes: []*pb.Annotation{
				gchatfmt.MakeFontColorAnnotation(0, 3, -65536),
			},
		},
		{
			name:     "font color via style= (beyond Python, this task's explicit ask)",
			content:  htmlContent(`<span style="color:#ff0000">red</span>`),
			wantText: "red",
			wantAnnotes: []*pb.Annotation{
				gchatfmt.MakeFontColorAnnotation(0, 3, -65536),
			},
		},
		{
			name:     "malformed color is not an error -- unformatted, no panic",
			content:  htmlContent(`<span data-mx-color="not-a-color">red</span>`),
			wantText: "red",
		},
		{
			name:     "hyperlink -- URL annotation must carry the href (the audit's URL-loss bug)",
			content:  htmlContent(`click <a href="https://example.com/path?a=1&amp;b=2">here</a>`),
			wantText: "click here",
			wantAnnotes: []*pb.Annotation{
				gchatfmt.MakeURLAnnotation(6, 4, "https://example.com/path?a=1&b=2", pb.Annotation_DO_NOT_RENDER),
			},
		},
		{
			// The specific case megabridge silently dropped entirely: a
			// "bare" link where the display text equals the href (the most
			// common form -- Matrix clients auto-linkify plain URLs this
			// way). Regression test for the fix.
			name:     "bare URL (text equals href) still gets a URL annotation",
			content:  htmlContent(`<a href="https://example.com">https://example.com</a>`),
			wantText: "https://example.com",
			wantAnnotes: []*pb.Annotation{
				gchatfmt.MakeURLAnnotation(0, 19, "https://example.com", pb.Annotation_DO_NOT_RENDER),
			},
		},
		{
			name:     "mailto link -- bare address, no annotation, original text discarded",
			content:  htmlContent(`<a href="mailto:foo@bar.com">Contact us</a>`),
			wantText: "foo@bar.com",
		},
		{
			name: "mention pill resolves to a MENTION annotation",
			content: htmlContent(
				`Hi <a href="https://matrix.to/#/@alice:example.com">Alice</a>!`,
			),
			mention: func(mxid id.UserID) (string, bool) {
				if mxid == "@alice:example.com" {
					return "123", true
				}
				return "", false
			},
			wantText: "Hi @Alice!",
			wantAnnotes: []*pb.Annotation{
				gchatfmt.MakeMentionAnnotation(3, 6, "123"),
			},
		},
		{
			// The classic UTF-16 trap: an astral-plane emoji (2 UTF-16 code
			// units) sits before the mention. If offsets were computed in
			// runes instead of UTF-16 code units, this would be off by one.
			name:     "mention placeholder-locator offset accounts for an astral char before it",
			content:  htmlContent(`🎆 <a href="https://matrix.to/#/@alice:example.com">Alice</a>`),
			mention:  func(id.UserID) (string, bool) { return "123", true },
			wantText: "🎆 @Alice",
			wantAnnotes: []*pb.Annotation{
				// "🎆"(0,1 rune -> UTF-16 units 0-1) " "(unit 2) "@Alice"(units 3-8, length 6)
				gchatfmt.MakeMentionAnnotation(3, 6, "123"),
			},
		},
		{
			name:     "nil MentionResolver -- pill renders as plain text, no annotation",
			content:  htmlContent(`Hi <a href="https://matrix.to/#/@bob:example.com">Bob</a>!`),
			mention:  nil,
			wantText: "Hi Bob!",
		},
		{
			name:     "MentionResolver returning ok=false -- same as nil, plain text fallback",
			content:  htmlContent(`Hi <a href="https://matrix.to/#/@bob:example.com">Bob</a>!`),
			mention:  func(id.UserID) (string, bool) { return "", false },
			wantText: "Hi Bob!",
		},
		{
			name:     "@room in formatted_body becomes MENTION_ALL",
			content:  htmlContent("hey @room check this out"),
			wantText: "hey @all check this out",
			wantAnnotes: []*pb.Annotation{
				gchatfmt.MakeMentionAllAnnotation(4, 4),
			},
		},
		{
			// from_matrix/__init__.py:30-34's plain-text @room escape hatch:
			// a message with NO formatted_body at all still gets a
			// MENTION_ALL annotation if its plain body contains "@room".
			name: "@room in a PLAIN body (no formatted_body) still becomes MENTION_ALL",
			content: &event.MessageEventContent{
				MsgType: event.MsgText,
				Body:    "hey @room check this out",
			},
			wantText: "hey @all check this out",
			wantAnnotes: []*pb.Annotation{
				gchatfmt.MakeMentionAllAnnotation(4, 4),
			},
		},
		{
			name:     "plain body without @room stays completely untouched",
			content:  &event.MessageEventContent{MsgType: event.MsgText, Body: "no room mention here"},
			wantText: "no room mention here",
		},
		{
			name:     "unordered list -- LIST/LIST_ITEM annotations, no text bullet prefix",
			content:  htmlContent("<ul><li>one</li><li>two</li></ul>"),
			wantText: "one\ntwo",
			wantAnnotes: []*pb.Annotation{
				gchatfmt.MakeFormatAnnotation(0, 3, pb.FormatMetadata_BULLETED_LIST_ITEM),
				gchatfmt.MakeFormatAnnotation(4, 3, pb.FormatMetadata_BULLETED_LIST_ITEM),
				gchatfmt.MakeFormatAnnotation(0, 7, pb.FormatMetadata_BULLETED_LIST),
			},
		},
		{
			name:     "ordered list -- numbered text prefix, no annotations (GC has no ordered-list format)",
			content:  htmlContent("<ol><li>one</li><li>two</li></ol>"),
			wantText: "1. one\n2. two",
		},
		{
			name:     "blockquote -- '> ' prefix per line",
			content:  htmlContent("<blockquote>line one<br/>line two</blockquote>"),
			wantText: "> line one\n> line two",
		},
		{
			// Regression test (gchat-port-auditor finding): JoinEntityString
			// used to leave a dangling trailing separator, and
			// nodeToTagAwareString used to unconditionally prepend a
			// leading "\n" before every block-tag sibling instead of only
			// the first -- both bugs were invisible whenever the affected
			// block was the ONLY/LAST content in the message, because
			// Parse's outer TrimSpace happened to eat the extra newlines
			// off the very end. Trailing content after the list exposes
			// both: without the fixes this produced text
			// "one\ntwo\n\n\nafter" (spurious blank lines) and a
			// BULLETED_LIST annotation of length 8 (one UTF-16 code unit
			// past "one\ntwo", into a stray "\n" that isn't part of any
			// list item).
			name:     "unordered list followed by a paragraph -- no dangling separator, LIST span stays exact",
			content:  htmlContent("<ul><li>one</li><li>two</li></ul><p>after</p>"),
			wantText: "one\ntwo\nafter",
			wantAnnotes: []*pb.Annotation{
				gchatfmt.MakeFormatAnnotation(0, 3, pb.FormatMetadata_BULLETED_LIST_ITEM),
				gchatfmt.MakeFormatAnnotation(4, 3, pb.FormatMetadata_BULLETED_LIST_ITEM),
				gchatfmt.MakeFormatAnnotation(0, 7, pb.FormatMetadata_BULLETED_LIST),
			},
		},
		{
			name:     "ordered list followed by a paragraph -- no dangling separator",
			content:  htmlContent("<ol><li>one</li><li>two</li></ol><p>after</p>"),
			wantText: "1. one\n2. two\nafter",
		},
		{
			// Regression test (gchat-port-auditor re-audit finding): an
			// empty <li></li> must not vanish from JoinEntityString's
			// output -- its blank line must survive between its
			// neighbors, even though (deliberately, see JoinEntityString's
			// doc comment) it contributes no zero-length LIST_ITEM entity
			// of its own. Before the fix, the empty middle item was
			// dropped entirely and "foo"/"bar" were merged onto one line.
			name:     "unordered list with an empty <li> -- blank line survives, item not merged away",
			content:  htmlContent("<ul><li>foo</li><li></li><li>bar</li></ul>"),
			wantText: "foo\n\nbar",
			wantAnnotes: []*pb.Annotation{
				gchatfmt.MakeFormatAnnotation(0, 3, pb.FormatMetadata_BULLETED_LIST_ITEM),
				gchatfmt.MakeFormatAnnotation(5, 3, pb.FormatMetadata_BULLETED_LIST_ITEM),
				gchatfmt.MakeFormatAnnotation(0, 8, pb.FormatMetadata_BULLETED_LIST),
			},
		},
		{
			name:     "blockquote followed by more text -- no dangling separator",
			content:  htmlContent("<blockquote>quoted</blockquote>more text"),
			wantText: "> quoted\nmore text",
		},
		{
			// Regression test (gchat-port-auditor finding): a second <p>
			// sibling must not get its OWN leading blank line on top of
			// the first paragraph's trailing one -- real Python's
			// prev_was_block latch (parser.py:283-289) never resets, so
			// only the very first block-tag child in a run of siblings
			// gets a leading newline prepended. Matrix clients commonly
			// emit exactly this shape (<p>...</p><p>...</p>) for a
			// blank-line-separated multi-paragraph plain-text message.
			name:     "two consecutive paragraphs -- exactly one blank line, not two",
			content:  htmlContent("<p>a</p><p>b</p>"),
			wantText: "a\n\nb",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			text, annotations := matrixfmt.Parse(context.Background(), test.content, test.mention)
			if text != test.wantText {
				t.Errorf("text = %q, want %q", text, test.wantText)
			}
			assertAnnotations(t, annotations, test.wantAnnotes)
		})
	}
}

// TestParse_PlainBodyReturnsNilAnnotationsSlice pins the exact M2-compatible
// contract: a plain message (no formatting) must get a literal nil
// annotations slice, not an empty-but-non-nil one, matching the task
// interface's documented "text only, no annotations" contract and letting
// callers use `if annotations != nil` as a cheap "was this formatted" check.
func TestParse_PlainBodyReturnsNilAnnotationsSlice(t *testing.T) {
	text, annotations := matrixfmt.Parse(context.Background(), &event.MessageEventContent{
		MsgType: event.MsgText,
		Body:    "just text",
	}, nil)
	if text != "just text" {
		t.Errorf("text = %q, want %q", text, "just text")
	}
	if annotations != nil {
		t.Errorf("annotations = %v, want nil", annotations)
	}
}

// TestEntityString_TrimSpace_KeepsEntityOffsetsCorrect is a direct
// regression test for a bug found while self-reviewing this port: the
// initial port of TrimSpace (following megabridge's own TrimSpace exactly)
// shifted an entity's Start by -cutStart BEFORE clamping it into the kept
// [cutStart, cutEnd) window, instead of clamping first (in the pre-trim
// coordinate space) and shifting last -- the order Split's doSplit uses
// correctly. For any entity that actually extends into the whitespace
// being trimmed away, that produces a negative Start and/or an
// out-of-bounds Length instead of a correctly clamped span. In the current
// parser's recursion shape this is hard to trigger end-to-end (every
// Format() call wraps content a nested TrimSpace already cleaned), so it's
// pinned here directly against the primitive rather than relying on an
// HTML fixture to happen to exercise it.
func TestEntityString_TrimSpace_KeepsEntityOffsetsCorrect(t *testing.T) {
	tests := []struct {
		name       string
		text       string
		entity     matrixfmt.BodyRange
		wantText   string
		wantEntity *matrixfmt.BodyRange // nil means the entity must be dropped (fully trimmed away)
	}{
		{
			name:       "entity spans the entire pre-trim string",
			text:       "  bold  ",
			entity:     matrixfmt.BodyRange{Start: 0, Length: 8, Value: matrixfmt.StyleBold},
			wantText:   "bold",
			wantEntity: &matrixfmt.BodyRange{Start: 0, Length: 4, Value: matrixfmt.StyleBold},
		},
		{
			name:       "entity spans into the leading trim only",
			text:       "  bold",
			entity:     matrixfmt.BodyRange{Start: 0, Length: 6, Value: matrixfmt.StyleBold},
			wantText:   "bold",
			wantEntity: &matrixfmt.BodyRange{Start: 0, Length: 4, Value: matrixfmt.StyleBold},
		},
		{
			name:       "entity spans into the trailing trim only",
			text:       "bold  ",
			entity:     matrixfmt.BodyRange{Start: 0, Length: 6, Value: matrixfmt.StyleBold},
			wantText:   "bold",
			wantEntity: &matrixfmt.BodyRange{Start: 0, Length: 4, Value: matrixfmt.StyleBold},
		},
		{
			name:       "entity entirely inside the leading trim is dropped, not corrupted",
			text:       "  bold",
			entity:     matrixfmt.BodyRange{Start: 0, Length: 1, Value: matrixfmt.StyleBold},
			wantText:   "bold",
			wantEntity: nil,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			es := matrixfmt.NewEntityString(test.text)
			es.Entities = matrixfmt.BodyRangeList{test.entity}
			es.TrimSpace()

			if es.Text.String() != test.wantText {
				t.Fatalf("text = %q, want %q", es.Text.String(), test.wantText)
			}
			if test.wantEntity == nil {
				if len(es.Entities) != 0 {
					t.Fatalf("entities = %+v, want none", es.Entities)
				}
				return
			}
			if len(es.Entities) != 1 {
				t.Fatalf("entities = %+v, want exactly 1", es.Entities)
			}
			got := es.Entities[0]
			if got.Start != test.wantEntity.Start || got.Length != test.wantEntity.Length {
				t.Fatalf("entity = %+v, want {Start:%d Length:%d}", got, test.wantEntity.Start, test.wantEntity.Length)
			}
			if got.Start < 0 {
				t.Fatalf("entity.Start = %d, must never be negative", got.Start)
			}
			if got.End() > len(es.Text) {
				t.Fatalf("entity.End() = %d exceeds trimmed text length %d", got.End(), len(es.Text))
			}
		})
	}
}

// --- Round-trip sanity: matrixfmt output fed back through gchatfmt should
// reproduce (semantically equivalent) Matrix HTML. These exercise both M3
// Task 1 and Task 2 together to prove the two directions agree on the wire
// shape of every annotation type they share.

func TestRoundTrip_Bold(t *testing.T) {
	text, annotations := matrixfmt.Parse(context.Background(), htmlContent("<strong>hi</strong>"), nil)
	body, html, _ := gchatfmt.Parse(context.Background(), text, annotations, nil)
	if body != "hi" {
		t.Errorf("body = %q, want %q", body, "hi")
	}
	if html != "<strong>hi</strong>" {
		t.Errorf("html = %q, want %q", html, "<strong>hi</strong>")
	}
}

func TestRoundTrip_NestedBoldItalic(t *testing.T) {
	text, annotations := matrixfmt.Parse(context.Background(), htmlContent("<strong>a<em>b</em></strong>"), nil)
	_, html, _ := gchatfmt.Parse(context.Background(), text, annotations, nil)
	want := "<strong>a<em>b</em></strong>"
	if html != want {
		t.Errorf("html = %q, want %q", html, want)
	}
}

func TestRoundTrip_FontColor(t *testing.T) {
	text, annotations := matrixfmt.Parse(context.Background(), htmlContent(`<span data-mx-color="#ff0000">red</span>`), nil)
	_, html, _ := gchatfmt.Parse(context.Background(), text, annotations, nil)
	want := `<span data-mx-color="#ff0000">red</span>`
	if html != want {
		t.Errorf("html = %q, want %q", html, want)
	}
}

func TestRoundTrip_RoomMention(t *testing.T) {
	text, annotations := matrixfmt.Parse(context.Background(), htmlContent("hey @room x"), nil)
	_, html, _ := gchatfmt.Parse(context.Background(), text, annotations, nil)
	want := "hey @room x"
	if html != want {
		t.Errorf("html = %q, want %q", html, want)
	}
}

func TestRoundTrip_UserMention(t *testing.T) {
	toGaia := func(mxid id.UserID) (string, bool) {
		if mxid == "@alice:example.com" {
			return "123", true
		}
		return "", false
	}
	text, annotations := matrixfmt.Parse(context.Background(), htmlContent(
		`<a href="https://matrix.to/#/@alice:example.com">Alice</a>`,
	), toGaia)

	toMXID := func(gaiaID string) (id.UserID, string, bool) {
		if gaiaID == "123" {
			return "@alice:example.com", "Alice", true
		}
		return "", "", false
	}
	_, html, _ := gchatfmt.Parse(context.Background(), text, annotations, toMXID)
	want := `<a href="https://matrix.to/#/@alice:example.com">Alice</a>`
	if html != want {
		t.Errorf("html = %q, want %q", html, want)
	}
}
