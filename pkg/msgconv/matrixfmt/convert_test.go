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

func htmlContent(formattedBody string) *event.MessageEventContent {
	return &event.MessageEventContent{
		MsgType:       event.MsgText,
		Body:          "plain-text fallback",
		Format:        event.FormatHTML,
		FormattedBody: formattedBody,
	}
}

// assertAnnotations compares annotation slices ignoring order.
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
			name:     "<tt> is not a monospace trigger -- no annotation",
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
			name:     "font color via style=",
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
			// Display text equals href: still needs a URL annotation.
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
			// Anchor text already starting with "@" must not become "@@Bob".
			name: "mention pill whose anchor text already has @ -- single @, not doubled",
			content: htmlContent(
				`Hi <a href="https://matrix.to/#/@bob:example.com">@Bob</a>!`,
			),
			mention: func(mxid id.UserID) (string, bool) {
				if mxid == "@bob:example.com" {
					return "456", true
				}
				return "", false
			},
			wantText: "Hi @Bob!",
			wantAnnotes: []*pb.Annotation{
				gchatfmt.MakeMentionAnnotation(3, 4, "456"),
			},
		},
		{
			// Astral emoji before the mention: offsets must be UTF-16 code units, not runes.
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
			// No formatted_body, but "@room" in the plain body still gets MENTION_ALL.
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
			// Trailing content after a list exposes a dangling separator or extra leading newline.
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
			// An empty <li></li> must keep its blank line.
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
			// Adjacent <p>s must be separated by exactly one blank line.
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

// AllowedMentions (m.mentions) gating.

func TestParse_RoomMention_AllowedByMentions(t *testing.T) {
	content := htmlContent("hey @room check this out")
	content.Mentions = &event.Mentions{Room: true}

	text, annotations := matrixfmt.Parse(context.Background(), content, nil)
	if text != "hey @all check this out" {
		t.Errorf("text = %q, want %q", text, "hey @all check this out")
	}
	assertAnnotations(t, annotations, []*pb.Annotation{
		gchatfmt.MakeMentionAllAnnotation(4, 4),
	})
}

// A Mentions block without Room must leave "@room" literal.
func TestParse_RoomMention_NotAllowedByMentions(t *testing.T) {
	content := htmlContent("hey @room check this out")
	content.Mentions = &event.Mentions{Room: false}

	text, annotations := matrixfmt.Parse(context.Background(), content, nil)
	if text != "hey @room check this out" {
		t.Errorf("text = %q, want the literal, unconverted @room text %q", text, "hey @room check this out")
	}
	if len(annotations) != 0 {
		t.Errorf("annotations = %s, want none -- @room must not bypass AllowedMentions.Room", formatAnnotations(annotations))
	}
}

func TestParse_RoomMention_NotAllowedByMentions_PlainBody(t *testing.T) {
	content := &event.MessageEventContent{
		MsgType:  event.MsgText,
		Body:     "hey @room check this out",
		Mentions: &event.Mentions{Room: false},
	}

	text, annotations := matrixfmt.Parse(context.Background(), content, nil)
	if text != "hey @room check this out" {
		t.Errorf("text = %q, want the literal, unconverted @room text %q", text, "hey @room check this out")
	}
	if len(annotations) != 0 {
		t.Errorf("annotations = %s, want none -- @room must not bypass AllowedMentions.Room", formatAnnotations(annotations))
	}
}

func TestParse_MentionPill_AllowedByMentions(t *testing.T) {
	content := htmlContent(`Hi <a href="https://matrix.to/#/@alice:example.com">Alice</a>!`)
	content.Mentions = &event.Mentions{UserIDs: []id.UserID{"@alice:example.com"}}
	resolve := func(mxid id.UserID) (string, bool) {
		if mxid == "@alice:example.com" {
			return "123", true
		}
		return "", false
	}

	text, annotations := matrixfmt.Parse(context.Background(), content, resolve)
	if text != "Hi @Alice!" {
		t.Errorf("text = %q, want %q", text, "Hi @Alice!")
	}
	assertAnnotations(t, annotations, []*pb.Annotation{
		gchatfmt.MakeMentionAnnotation(3, 6, "123"),
	})
}

func TestParse_MentionPill_NotAllowedByMentions(t *testing.T) {
	content := htmlContent(`Hi <a href="https://matrix.to/#/@alice:example.com">Alice</a>!`)
	content.Mentions = &event.Mentions{} // present but empty -- alice is not in it
	resolve := func(mxid id.UserID) (string, bool) {
		t.Fatal("MentionResolver should not be consulted once AllowedMentions rejects the pill")
		return "", false
	}

	text, annotations := matrixfmt.Parse(context.Background(), content, resolve)
	if text != "Hi Alice!" {
		t.Errorf("text = %q, want %q (plain text, no pill)", text, "Hi Alice!")
	}
	if len(annotations) != 0 {
		t.Errorf("annotations = %s, want none for a pill AllowedMentions rejects", formatAnnotations(annotations))
	}
}

// Plain messages must return a nil (not empty) annotations slice.
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

// Entities extending into trimmed whitespace must be clamped before shifting.
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

// Round-trip: matrixfmt output fed back through gchatfmt.

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

	// gchatfmt renders a GC MENTION over the text "@Bob" into pill HTML.
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

func TestRoundTrip_MentionSigilNotDoubled(t *testing.T) {
	toMXID := func(gaiaID string) (id.UserID, string, bool) {
		if gaiaID == "456" {
			return "@bob:example.com", "@Bob", true
		}
		return "", "", false
	}
	gcAnnotations := []*pb.Annotation{gchatfmt.MakeMentionAnnotation(0, 4, "456")}
	_, pillHTML, _ := gchatfmt.Parse(context.Background(), "@Bob", gcAnnotations, toMXID)
	if pillHTML != `<a href="https://matrix.to/#/@bob:example.com">@Bob</a>` {
		t.Fatalf("gchatfmt produced unexpected pill HTML: %q", pillHTML)
	}

	toGaia := func(mxid id.UserID) (string, bool) {
		if mxid == "@bob:example.com" {
			return "456", true
		}
		return "", false
	}
	text, annotations := matrixfmt.Parse(context.Background(), htmlContent(pillHTML), toGaia)
	if text != "@Bob" {
		t.Errorf("round-tripped text = %q, want %q (single @, not doubled)", text, "@Bob")
	}
	assertAnnotations(t, annotations, []*pb.Annotation{
		gchatfmt.MakeMentionAnnotation(0, 4, "456"),
	})
}
