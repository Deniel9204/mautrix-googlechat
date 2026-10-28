package gchatfmt_test

import (
	"context"
	"strings"
	"testing"

	"maunium.net/go/mautrix/id"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv/gchatfmt"
)

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
			// italic must split into [2,4) nested in bold and [4,6) after it.
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
			// italic is fully nested inside bold: no split needed.
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
			// 🎆 is 2 UTF-16 code units, so "b" is at offset 5.
			name: "astral char before a span -- offset must be UTF-16 code units",
			text: "🎆 a b z",
			annotations: []*pb.Annotation{
				gchatfmt.MakeFormatAnnotation(5, 1, pb.FormatMetadata_BOLD),
			},
			wantBody: "🎆 a b z",
			wantHTML: "🎆 a <strong>b</strong> z",
		},
		{
			// int32(-65536) = 0xFFFF0000; +2^31 then masked to 24 bits -> 0xFF0000.
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
			// Non-url preview chips stay plain text; url_metadata is exempt.
			name: "non-DO_NOT_RENDER chip on a non-URL annotation is skipped, text stays plain",
			text: "see bold here",
			annotations: []*pb.Annotation{
				renderChip(gchatfmt.MakeFormatAnnotation(4, 4, pb.FormatMetadata_BOLD)),
			},
			wantBody: "see bold here",
			wantHTML: "see bold here",
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
			body, html, _ := gchatfmt.Parse(context.Background(), test.text, test.annotations, nil)
			if body != test.wantBody {
				t.Errorf("body = %q, want %q", body, test.wantBody)
			}
			if html != test.wantHTML {
				t.Errorf("html = %q, want %q", html, test.wantHTML)
			}
		})
	}
}

func TestParse_HyperlinkEscaping(t *testing.T) {
	const maliciousURL = `https://x.com/"><script>alert(1)</script>`
	annotations := []*pb.Annotation{
		gchatfmt.MakeURLAnnotation(0, 10, maliciousURL, pb.Annotation_DO_NOT_RENDER),
	}
	_, html, _ := gchatfmt.Parse(context.Background(), "click here", annotations, nil)

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

func TestParse_HyperlinkPlain(t *testing.T) {
	annotations := []*pb.Annotation{
		gchatfmt.MakeURLAnnotation(0, 8, "https://example.com/path?a=1&b=2", pb.Annotation_DO_NOT_RENDER),
	}
	body, html, _ := gchatfmt.Parse(context.Background(), "click me", annotations, nil)

	if body != "click me" {
		t.Errorf("body = %q, want %q", body, "click me")
	}
	want := `<a href="https://example.com/path?a=1&amp;b=2">click me</a>`
	if html != want {
		t.Errorf("html = %q, want %q", html, want)
	}
}

// A javascript: href must become plain text, never an <a> tag.
func TestParse_DangerousURLSchemeNeutralized(t *testing.T) {
	tests := []struct {
		name string
		href string
	}{
		{"javascript scheme", "javascript:alert(1)"},
		{"javascript scheme, mixed case", "JavaScript:alert(1)"},
		{"javascript scheme, leading whitespace/control chars", "\n\t javascript:alert(1)"},
		{"data scheme", "data:text/html,<script>alert(1)</script>"},
		{"data scheme, mixed case", "DATA:text/html,<script>alert(1)</script>"},
		// Browsers strip tab/CR/LF anywhere before parsing the scheme.
		{"javascript scheme, embedded tab", "java\tscript:alert(1)"},
		{"javascript scheme, embedded CR", "java\rscript:alert(1)"},
		{"javascript scheme, embedded LF", "java\nscript:alert(1)"},
		{"javascript scheme, embedded tab + mixed case", "Java\tScript:alert(1)"},
		{"data scheme, embedded newline", "da\nta:text/html,<script>alert(1)</script>"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			annotations := []*pb.Annotation{
				gchatfmt.MakeURLAnnotation(0, 10, test.href, pb.Annotation_DO_NOT_RENDER),
			}
			_, html, _ := gchatfmt.Parse(context.Background(), "click here", annotations, nil)

			if strings.Contains(html, "<a ") || strings.Contains(html, "href=") {
				t.Fatalf("dangerous scheme %q was not neutralized, produced a link: %q", test.href, html)
			}
			if !strings.Contains(html, "click here") {
				t.Errorf("neutralized text was dropped entirely, got: %q", html)
			}
		})
	}
}

func TestParse_SafeURLSchemesStillRenderAsLinks(t *testing.T) {
	tests := []string{
		"https://example.com",
		"http://example.com",
		"mailto:bob@example.com",
	}
	for _, href := range tests {
		t.Run(href, func(t *testing.T) {
			annotations := []*pb.Annotation{
				gchatfmt.MakeURLAnnotation(0, 10, href, pb.Annotation_DO_NOT_RENDER),
			}
			_, html, _ := gchatfmt.Parse(context.Background(), "click here", annotations, nil)
			if !strings.Contains(html, `<a href="`) {
				t.Errorf("safe scheme %q was neutralized, want a normal <a href> link, got: %q", href, html)
			}
		})
	}
}

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
	_, html, _ := gchatfmt.Parse(context.Background(), "@Bob hi", annotations, resolve)

	want := `<a href="https://matrix.to/#/@bob:example.com">Bob &lt;3</a> hi`
	if html != want {
		t.Errorf("html = %q, want %q", html, want)
	}
}

func TestParse_Mention_NilResolver(t *testing.T) {
	annotations := []*pb.Annotation{
		gchatfmt.MakeMentionAnnotation(0, 8, "123"),
	}
	body, html, _ := gchatfmt.Parse(context.Background(), "@Charlie hi", annotations, nil)

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

func TestParse_Mention_UnresolvedWithName(t *testing.T) {
	resolve := func(gaiaID string) (id.UserID, string, bool) {
		return "", "Dana", false
	}
	annotations := []*pb.Annotation{
		gchatfmt.MakeMentionAnnotation(0, 6, "456"),
	}
	_, html, _ := gchatfmt.Parse(context.Background(), "@dana6 hi", annotations, resolve)

	if html != "Dana hi" {
		t.Errorf("html = %q, want %q", html, "Dana hi")
	}
	if strings.Contains(html, "<a ") {
		t.Errorf("expected no pill when the resolver returns ok=false, got: %q", html)
	}
}

func TestParse_MalformedAnnotationFallsBackGracefully(t *testing.T) {
	annotations := []*pb.Annotation{
		gchatfmt.MakeFormatAnnotation(1, 5, pb.FormatMetadata_BOLD), // text is only 2 units long
	}
	body, html, _ := gchatfmt.Parse(context.Background(), "hi", annotations, nil)

	if body != "hi" {
		t.Errorf("body = %q, want %q", body, "hi")
	}
	if html != "" {
		t.Errorf("html = %q, want empty on malformed annotation fallback", html)
	}
}

// Length near int32 max must not overflow the bounds check and panic.
func TestParse_HugeLengthAnnotationFallsBackGracefully(t *testing.T) {
	const text = "hello world this is a test message of some length"
	annotations := []*pb.Annotation{
		gchatfmt.MakeFormatAnnotation(5, 2147483647, pb.FormatMetadata_BOLD),
	}

	body, html, _ := gchatfmt.Parse(context.Background(), text, annotations, nil)

	if body != text {
		t.Errorf("body = %q, want %q", body, text)
	}
	if html != "" {
		t.Errorf("html = %q, want empty on malformed annotation fallback", html)
	}
}

// ParsedMentions must match the pills actually rendered.

func mustResolve(gaiaID string) (id.UserID, string, bool) {
	if gaiaID == "200" {
		return "@200_ghost:example.com", "", true
	}
	return "", "", false
}

func TestParse_MentionsMatchRenderedPills(t *testing.T) {
	annotations := []*pb.Annotation{gchatfmt.MakeMentionAnnotation(0, 4, "200")}
	_, html, mentions := gchatfmt.Parse(context.Background(), "@Bob hi", annotations, mustResolve)

	wantHTML := `<a href="https://matrix.to/#/@200_ghost:example.com">@Bob</a> hi`
	if html != wantHTML {
		t.Errorf("html = %q, want %q", html, wantHTML)
	}
	if len(mentions.UserIDs) != 1 || mentions.UserIDs[0] != "@200_ghost:example.com" {
		t.Errorf("mentions.UserIDs = %v, want exactly [@200_ghost:example.com] (== the rendered pill)", mentions.UserIDs)
	}
	if mentions.Room {
		t.Error("mentions.Room = true, want false")
	}
}

func TestParse_MentionAllSetsRoom(t *testing.T) {
	annotations := []*pb.Annotation{gchatfmt.MakeMentionAllAnnotation(0, 4)}
	_, _, mentions := gchatfmt.Parse(context.Background(), "@all hi", annotations, nil)
	if !mentions.Room {
		t.Error("mentions.Room = false, want true for a valid MENTION_ALL")
	}
	if len(mentions.UserIDs) != 0 {
		t.Errorf("mentions.UserIDs = %v, want empty for a pure @room mention", mentions.UserIDs)
	}
}

func TestParse_UnresolvedMentionNotCollected(t *testing.T) {
	annotations := []*pb.Annotation{gchatfmt.MakeMentionAnnotation(0, 4, "999")}
	_, _, mentions := gchatfmt.Parse(context.Background(), "@Eve hi", annotations, mustResolve)
	if len(mentions.UserIDs) != 0 || mentions.Room {
		t.Errorf("mentions = %+v, want empty for an unresolvable mention", mentions)
	}
}

func TestParse_NilResolverCollectsNoUsers(t *testing.T) {
	annotations := []*pb.Annotation{gchatfmt.MakeMentionAnnotation(0, 4, "200")}
	_, _, mentions := gchatfmt.Parse(context.Background(), "@Bob hi", annotations, nil)
	if len(mentions.UserIDs) != 0 {
		t.Errorf("mentions.UserIDs = %v, want empty with a nil resolver", mentions.UserIDs)
	}
}

// An out-of-bounds mention must not be collected, even if it resolves.
func TestParse_MalformedMentionExcludedFromMentions(t *testing.T) {
	// "hi" is 2 UTF-16 units; the mention claims [0,50).
	annotations := []*pb.Annotation{gchatfmt.MakeMentionAnnotation(0, 50, "200")}
	_, html, mentions := gchatfmt.Parse(context.Background(), "hi", annotations, mustResolve)

	if html != "" {
		t.Errorf("html = %q, want empty (out-of-bounds annotation -> fallback)", html)
	}
	if len(mentions.UserIDs) != 0 || mentions.Room {
		t.Errorf("mentions = %+v, want empty -- a malformed mention must not ping (phantom ping)", mentions)
	}
}

// A valid mention is collected even when another annotation forces the plain fallback.
func TestParse_ValidMentionCollectedDespiteUnrelatedMalformedAnnotation(t *testing.T) {
	annotations := []*pb.Annotation{
		gchatfmt.MakeFormatAnnotation(0, 2147483647, pb.FormatMetadata_BOLD),
		gchatfmt.MakeMentionAnnotation(3, 4, "200"),
	}
	_, html, mentions := gchatfmt.Parse(context.Background(), "hi @Bob", annotations, mustResolve)

	if html != "" {
		t.Errorf("html = %q, want empty (malformed FORMAT annotation -> fallback)", html)
	}
	if len(mentions.UserIDs) != 1 || mentions.UserIDs[0] != "@200_ghost:example.com" {
		t.Errorf("mentions.UserIDs = %v, want [@200_ghost:example.com] -- the valid mention still pings", mentions.UserIDs)
	}
}

func TestParse_MentionChipRenderTypeFilteredOut(t *testing.T) {
	chip := gchatfmt.MakeMentionAnnotation(0, 4, "200")
	chip.ChipRenderType = pb.Annotation_RENDER.Enum()
	_, _, mentions := gchatfmt.Parse(context.Background(), "@Bob hi", []*pb.Annotation{chip}, mustResolve)
	if len(mentions.UserIDs) != 0 {
		t.Errorf("mentions.UserIDs = %v, want empty for a RENDER-chip mention", mentions.UserIDs)
	}
}

func TestParse_MentionsDeduped(t *testing.T) {
	annotations := []*pb.Annotation{
		gchatfmt.MakeMentionAnnotation(0, 4, "200"),
		gchatfmt.MakeMentionAnnotation(5, 4, "200"),
	}
	_, _, mentions := gchatfmt.Parse(context.Background(), "@Bob @Bob", annotations, mustResolve)
	if len(mentions.UserIDs) != 1 {
		t.Errorf("mentions.UserIDs = %v, want exactly one entry (deduped)", mentions.UserIDs)
	}
}

func TestParse_NoAnnotationsEmptyMentions(t *testing.T) {
	_, _, mentions := gchatfmt.Parse(context.Background(), "plain text", nil, mustResolve)
	if len(mentions.UserIDs) != 0 || mentions.Room {
		t.Errorf("mentions = %+v, want empty for a message with no annotations", mentions)
	}
}

// Google Chat sets RENDER_IF_POSSIBLE on pasted links; url_metadata must linkify for any chip type.
func TestParse_InboundURLLinkifiedRegardlessOfChipRenderType(t *testing.T) {
	for _, chip := range []pb.Annotation_ChipRenderType{
		pb.Annotation_UNKNOWN,
		pb.Annotation_RENDER,
		pb.Annotation_RENDER_IF_POSSIBLE,
		pb.Annotation_DO_NOT_RENDER,
	} {
		t.Run(chip.String(), func(t *testing.T) {
			const url = "https://index.hu"
			annotations := []*pb.Annotation{
				gchatfmt.MakeURLAnnotation(0, 16, url, chip),
			}

			body, html, _ := gchatfmt.Parse(context.Background(), url, annotations, nil)

			if body != url {
				t.Errorf("body = %q, want %q (plain body is never rewritten)", body, url)
			}
			want := `<a href="https://index.hu">https://index.hu</a>`
			if html != want {
				t.Errorf("html = %q, want %q", html, want)
			}
		})
	}
}

// renderChip sets an annotation's chip_render_type to RENDER.
func renderChip(a *pb.Annotation) *pb.Annotation {
	a.ChipRenderType = pb.Annotation_RENDER.Enum()
	return a
}

// A zero-length chip must not emit an empty <a href></a>.
func TestParse_ZeroLengthPreviewChipURLNotLinkified(t *testing.T) {
	annotations := []*pb.Annotation{
		gchatfmt.MakeURLAnnotation(0, 0, "https://x.test", pb.Annotation_RENDER_IF_POSSIBLE),
	}

	_, html, _ := gchatfmt.Parse(context.Background(), "hello world", annotations, nil)

	if strings.Contains(html, "<a ") {
		t.Errorf("html = %q, want no anchor for a zero-length preview chip", html)
	}
}

func TestParse_NewlinesInsideCodeBlockArePreserved(t *testing.T) {
	// indices: "intro"=0..4, \n=5, "x"=6, \n=7, "y"=8, \n=9, "end"=10..12
	const text = "intro\nx\ny\nend"
	annotations := []*pb.Annotation{
		gchatfmt.MakeFormatAnnotation(6, 3, pb.FormatMetadata_MONOSPACE_BLOCK), // covers "x\ny"
	}

	_, gotHTML, _ := gchatfmt.Parse(context.Background(), text, annotations, nil)

	const wantHTML = "intro<br/><pre><code>x\ny</code></pre><br/>end"
	if gotHTML != wantHTML {
		t.Errorf("Parse() html =\n  %q\nwant\n  %q", gotHTML, wantHTML)
	}
}

func TestParse_MultipleCodeBlocksEachPreserveNewlines(t *testing.T) {
	// "a\nb" = 0..2, "|" = 3, "c\nd" = 4..6
	const text = "a\nb|c\nd"
	annotations := []*pb.Annotation{
		gchatfmt.MakeFormatAnnotation(0, 3, pb.FormatMetadata_MONOSPACE_BLOCK),
		gchatfmt.MakeFormatAnnotation(4, 3, pb.FormatMetadata_MONOSPACE_BLOCK),
	}

	_, gotHTML, _ := gchatfmt.Parse(context.Background(), text, annotations, nil)

	const wantHTML = "<pre><code>a\nb</code></pre>|<pre><code>c\nd</code></pre>"
	if gotHTML != wantHTML {
		t.Errorf("Parse() html =\n  %q\nwant\n  %q", gotHTML, wantHTML)
	}
}
