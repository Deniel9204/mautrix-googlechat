package gchatfmt_test

import (
	"context"
	"strings"
	"testing"

	"maunium.net/go/mautrix/id"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv/gchatfmt"
)

// TestParse is the table-driven core of the behavior inventory: one case
// per mandatory behavior from the M3 Task 1 brief. See convert.go's package
// doc comment for the from_googlechat.py line references each case ports.
func TestParse(t *testing.T) {
	tests := []struct {
		name        string
		text        string
		annotations []*pb.Annotation
		wantBody    string
		wantHTML    string
	}{
		{
			name:     "plain text, no annotations",
			text:     "Hello world!",
			wantBody: "Hello world!",
			wantHTML: "",
		},
		{
			name: "single bold span",
			text: "hello world",
			annotations: []*pb.Annotation{
				gchatfmt.MakeFormatAnnotation(0, 5, pb.FormatMetadata_BOLD),
			},
			wantBody: "hello world",
			wantHTML: "<strong>hello</strong> world",
		},
		{
			name: "two adjacent (touching, non-overlapping) spans",
			text: "ab",
			annotations: []*pb.Annotation{
				gchatfmt.MakeFormatAnnotation(0, 1, pb.FormatMetadata_BOLD),
				gchatfmt.MakeFormatAnnotation(1, 1, pb.FormatMetadata_ITALIC),
			},
			wantBody: "ab",
			wantHTML: "<strong>a</strong><em>b</em>",
		},
		{
			// bold [0,4) and italic [2,6) partially overlap; normalization
			// must split italic into [2,4) (nested inside bold) and [4,6)
			// (after bold), matching the megabridge probe recorded in
			// docs/research/08d-megabridge-msgconv.md §1.3.
			name: "two overlapping spans (bold+italic) get split at the boundary",
			text: "abcdef",
			annotations: []*pb.Annotation{
				gchatfmt.MakeFormatAnnotation(0, 4, pb.FormatMetadata_BOLD),
				gchatfmt.MakeFormatAnnotation(2, 4, pb.FormatMetadata_ITALIC),
			},
			wantBody: "abcdef",
			wantHTML: "<strong>ab<em>cd</em></strong><em>ef</em>",
		},
		{
			// italic [2,4) is fully nested inside bold [0,6) -- no split
			// needed, just nested tags.
			name: "nested spans (italic fully inside bold)",
			text: "abcdef",
			annotations: []*pb.Annotation{
				gchatfmt.MakeFormatAnnotation(0, 6, pb.FormatMetadata_BOLD),
				gchatfmt.MakeFormatAnnotation(2, 2, pb.FormatMetadata_ITALIC),
			},
			wantBody: "abcdef",
			wantHTML: "<strong>ab<em>cd</em>ef</strong>",
		},
		{
			// U+1F386 (🎆) is astral-plane -- 2 UTF-16 code units. The bold
			// span starts at UTF-16 offset 5, which is only correct if the
			// emoji before it was counted as 2 units, not 1 rune: "🎆"(0,1)
			// " "(2) "a"(3) " "(4) "b"(5) " "(6) "z"(7).
			name: "astral char before a span -- offset must be UTF-16 code units",
			text: "🎆 a b z",
			annotations: []*pb.Annotation{
				gchatfmt.MakeFormatAnnotation(5, 1, pb.FormatMetadata_BOLD),
			},
			wantBody: "🎆 a b z",
			wantHTML: "🎆 a <strong>b</strong> z",
		},
		{
			// (rgb + 2^31) & 0xFFFFFF transform: int32(-65536) is
			// 0xFFFF0000 in two's complement; +2^31 wraps the top bit off,
			// leaving 0x7FFF0000, masked to the low 24 bits -> 0xFF0000.
			name: "font color transform",
			text: "red",
			annotations: []*pb.Annotation{
				gchatfmt.MakeFontColorAnnotation(0, 3, -65536),
			},
			wantBody: "red",
			wantHTML: `<span data-mx-color="#ff0000">red</span>`,
		},
		{
			name: "underline and strikethrough and monospace and monospace block",
			text: "u s m p",
			annotations: []*pb.Annotation{
				gchatfmt.MakeFormatAnnotation(0, 1, pb.FormatMetadata_UNDERLINE),
				gchatfmt.MakeFormatAnnotation(2, 1, pb.FormatMetadata_STRIKE),
				gchatfmt.MakeFormatAnnotation(4, 1, pb.FormatMetadata_MONOSPACE),
				gchatfmt.MakeFormatAnnotation(6, 1, pb.FormatMetadata_MONOSPACE_BLOCK),
			},
			wantBody: "u s m p",
			wantHTML: "<u>u</u> <del>s</del> <code>m</code> <pre><code>p</code></pre>",
		},
		{
			name: "hidden format annotation drops its text entirely",
			text: "visible hidden visible",
			annotations: []*pb.Annotation{
				gchatfmt.MakeFormatAnnotation(8, 6, pb.FormatMetadata_HIDDEN),
			},
			wantBody: "visible hidden visible",
			wantHTML: "visible  visible",
		},
		{
			name: "bulleted list and list item",
			text: "one two",
			annotations: []*pb.Annotation{
				gchatfmt.MakeFormatAnnotation(0, 7, pb.FormatMetadata_BULLETED_LIST),
				gchatfmt.MakeFormatAnnotation(0, 3, pb.FormatMetadata_BULLETED_LIST_ITEM),
				gchatfmt.MakeFormatAnnotation(4, 3, pb.FormatMetadata_BULLETED_LIST_ITEM),
			},
			wantBody: "one two",
			wantHTML: "<ul><li>one</li> <li>two</li></ul>",
		},
		{
			name: "@room / MENTION_ALL renders as the literal @room",
			text: "hey @all check this out",
			annotations: []*pb.Annotation{
				gchatfmt.MakeMentionAllAnnotation(4, 4),
			},
			wantBody: "hey @all check this out",
			wantHTML: "hey @room check this out",
		},
		{
			name: "non-DO_NOT_RENDER chip is skipped, text stays plain",
			text: "see http://example.com here",
			annotations: []*pb.Annotation{
				gchatfmt.MakeURLAnnotation(4, 19, "http://example.com", pb.Annotation_RENDER),
			},
			wantBody: "see http://example.com here",
			wantHTML: "see http://example.com here",
		},
		{
			name:        "empty text, no annotations",
			text:        "",
			annotations: nil,
			wantBody:    "",
			wantHTML:    "",
		},
		{
			name:     "whitespace-only body with no annotations is untouched",
			text:     "   ",
			wantBody: "   ",
			wantHTML: "",
		},
		{
			name: "whitespace-only body with a real annotation still formats",
			text: "   ",
			annotations: []*pb.Annotation{
				gchatfmt.MakeFormatAnnotation(0, 1, pb.FormatMetadata_BOLD),
			},
			wantBody: "   ",
			wantHTML: "<strong> </strong>  ",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body, html := gchatfmt.Parse(context.Background(), test.text, test.annotations, nil)
			if body != test.wantBody {
				t.Errorf("body = %q, want %q", body, test.wantBody)
			}
			if html != test.wantHTML {
				t.Errorf("html = %q, want %q", html, test.wantHTML)
			}
		})
	}
}

// TestParse_HyperlinkEscaping is the B1 regression test: a Google
// Chat-controlled URL crafted to break out of the href attribute must never
// produce an unescaped "><script> in the output.
func TestParse_HyperlinkEscaping(t *testing.T) {
	const maliciousURL = `https://x.com/"><script>alert(1)</script>`
	annotations := []*pb.Annotation{
		gchatfmt.MakeURLAnnotation(0, 10, maliciousURL, pb.Annotation_DO_NOT_RENDER),
	}
	_, html := gchatfmt.Parse(context.Background(), "click here", annotations, nil)

	if strings.Contains(html, `"><script>`) {
		t.Fatalf("href was not attribute-escaped, found raw injection payload in output: %q", html)
	}
	if strings.Contains(html, "<script>") {
		t.Fatalf("script tag survived unescaped in output: %q", html)
	}
	if !strings.Contains(html, "&#34;&gt;&lt;script&gt;") {
		t.Errorf("expected the escaped payload in the href attribute, got: %q", html)
	}
	if !strings.HasPrefix(html, `<a href="https://x.com/`) {
		t.Errorf("expected an <a href=...> wrapping the escaped URL, got: %q", html)
	}
	if !strings.Contains(html, ">click here</a>") {
		t.Errorf("expected the link text to survive, got: %q", html)
	}
}

// TestParse_HyperlinkPlain is a control case proving well-formed URLs still
// render normally after the B1 fix.
func TestParse_HyperlinkPlain(t *testing.T) {
	annotations := []*pb.Annotation{
		gchatfmt.MakeURLAnnotation(0, 8, "https://example.com/path?a=1&b=2", pb.Annotation_DO_NOT_RENDER),
	}
	body, html := gchatfmt.Parse(context.Background(), "click me", annotations, nil)

	if body != "click me" {
		t.Errorf("body = %q, want %q", body, "click me")
	}
	want := `<a href="https://example.com/path?a=1&amp;b=2">click me</a>`
	if html != want {
		t.Errorf("html = %q, want %q", html, want)
	}
}

// TestParse_Mention_ResolvedPill covers the MentionResolver seam (fix for
// B2): when the resolver knows the gaiaID, a real user pill is rendered
// with the resolver's display name, not a DM-portal room pill and not
// unescaped displayname interpolation.
func TestParse_Mention_ResolvedPill(t *testing.T) {
	resolve := func(gaiaID string) (id.UserID, string, bool) {
		if gaiaID == "123" {
			return id.UserID("@bob:example.com"), "Bob <3", true
		}
		return "", "", false
	}
	annotations := []*pb.Annotation{
		gchatfmt.MakeMentionAnnotation(0, 4, "123"),
	}
	_, html := gchatfmt.Parse(context.Background(), "@Bob hi", annotations, resolve)

	want := `<a href="https://matrix.to/#/@bob:example.com">Bob &lt;3</a> hi`
	if html != want {
		t.Errorf("html = %q, want %q", html, want)
	}
}

// TestParse_Mention_NilResolver: with no resolver at all (M3 Task 1's
// default -- Task 3 wires the real one), a mention annotation must fall
// back to the original message text, unpilled, with nothing dropped.
func TestParse_Mention_NilResolver(t *testing.T) {
	annotations := []*pb.Annotation{
		gchatfmt.MakeMentionAnnotation(0, 8, "123"),
	}
	body, html := gchatfmt.Parse(context.Background(), "@Charlie hi", annotations, nil)

	if body != "@Charlie hi" {
		t.Errorf("body = %q, want %q", body, "@Charlie hi")
	}
	if html != "@Charlie hi" {
		t.Errorf("html = %q, want %q (no pill, text kept verbatim)", html, "@Charlie hi")
	}
	if strings.Contains(html, "<a ") {
		t.Errorf("expected no pill without a resolver, got: %q", html)
	}
}

// TestParse_Mention_UnresolvedWithName: the resolver knows a display name
// but not an MXID (ok=false, name != "") -- render the name as plain text,
// still no pill.
func TestParse_Mention_UnresolvedWithName(t *testing.T) {
	resolve := func(gaiaID string) (id.UserID, string, bool) {
		return "", "Dana", false
	}
	annotations := []*pb.Annotation{
		gchatfmt.MakeMentionAnnotation(0, 6, "456"),
	}
	_, html := gchatfmt.Parse(context.Background(), "@dana6 hi", annotations, resolve)

	if html != "Dana hi" {
		t.Errorf("html = %q, want %q", html, "Dana hi")
	}
	if strings.Contains(html, "<a ") {
		t.Errorf("expected no pill when the resolver returns ok=false, got: %q", html)
	}
}

// TestParse_MalformedAnnotationFallsBackGracefully: an annotation whose
// span overruns the actual text must not panic (Go must not trust
// server-controlled offsets); Parse should log-and-fall-back to plain text
// for the whole message, matching the graceful-degradation contract in the
// package doc comment.
func TestParse_MalformedAnnotationFallsBackGracefully(t *testing.T) {
	annotations := []*pb.Annotation{
		gchatfmt.MakeFormatAnnotation(1, 5, pb.FormatMetadata_BOLD), // text is only 2 units long
	}
	body, html := gchatfmt.Parse(context.Background(), "hi", annotations, nil)

	if body != "hi" {
		t.Errorf("body = %q, want %q", body, "hi")
	}
	if html != "" {
		t.Errorf("html = %q, want empty on malformed annotation fallback", html)
	}
}

// TestParse_HugeLengthAnnotationFallsBackGracefully is the P0 regression
// test from the port audit: a Length near int32 max, added to a small
// StartIndex, overflows a naive int32 sum (wire-controlled fields, unlike
// Python's arbitrary-precision ints) and can wrap to a value that passes
// an unpromoted ">" bounds check, which then panics as an out-of-range
// slice bound instead of degrading to the plain-text fallback. Must not
// panic; must fall back exactly like any other malformed annotation.
func TestParse_HugeLengthAnnotationFallsBackGracefully(t *testing.T) {
	const text = "hello world this is a test message of some length"
	annotations := []*pb.Annotation{
		gchatfmt.MakeFormatAnnotation(5, 2147483647, pb.FormatMetadata_BOLD),
	}

	body, html := gchatfmt.Parse(context.Background(), text, annotations, nil)

	if body != text {
		t.Errorf("body = %q, want %q", body, text)
	}
	if html != "" {
		t.Errorf("html = %q, want empty on malformed annotation fallback", html)
	}
}
