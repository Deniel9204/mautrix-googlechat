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

// TestParse_HyperlinkEscaping is the B1 regression test: a Google
// Chat-controlled URL crafted to break out of the href attribute must never
// produce an unescaped "><script> in the output.
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

// TestParse_HyperlinkPlain is a control case proving well-formed URLs still
// render normally after the B1 fix.
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

// TestParse_DangerousURLSchemeNeutralized is the security regression test
// for the javascript:/data: link-scheme fix (M7 Task 3 item 1, mirroring the
// B1 href-escape fix above): a url_metadata annotation whose href is a
// javascript: URI must never become a clickable <a href="javascript:...">
// pill -- escaping alone (B1) makes the attribute syntactically safe but
// does nothing to stop a well-formed javascript:/data: URL from executing
// when clicked. The annotation must be neutralized to plain, unwrapped text
// instead (the same "skip_entity" fallback already used for HIDDEN/
// unrecognized format types), never an <a> tag of any kind.
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
		// The WHATWG URL parser strips ASCII tab/CR/LF from ANYWHERE in the
		// URL (not just the ends) before it recognizes the scheme, so a
		// browser still executes these as javascript: -- the classic
		// embedded-tab/newline filter bypass. See isDangerousURLScheme.
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

// TestParse_SafeURLSchemesStillRenderAsLinks is a control case proving the
// dangerous-scheme filter (above) doesn't over-broadly reject ordinary safe
// schemes (http/https/mailto and a scheme-less relative-looking string all
// still become clickable pills).
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
	_, html, _ := gchatfmt.Parse(context.Background(), "@Bob hi", annotations, resolve)

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
	_, html, _ := gchatfmt.Parse(context.Background(), "@dana6 hi", annotations, resolve)

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
	body, html, _ := gchatfmt.Parse(context.Background(), "hi", annotations, nil)

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

	body, html, _ := gchatfmt.Parse(context.Background(), text, annotations, nil)

	if body != text {
		t.Errorf("body = %q, want %q", body, text)
	}
	if html != "" {
		t.Errorf("html = %q, want empty on malformed annotation fallback", html)
	}
}

// --- ParsedMentions: the 3rd return value drives content.Mentions ----------
// (phantom-ping fix -- the mentions gchatfmt reports it rendered must equal
// the pills it actually emitted, and exclude any malformed/unresolved one.)

func mustResolve(gaiaID string) (id.UserID, string, bool) {
	if gaiaID == "200" {
		return "@200_ghost:example.com", "", true
	}
	return "", "", false
}

// TestParse_MentionsMatchRenderedPills: for a normally-rendering message the
// returned ParsedMentions.UserIDs are exactly the users whose pills appear
// in the HTML.
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

// TestParse_MentionAllSetsRoom: a valid MENTION_ALL sets ParsedMentions.Room.
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

// TestParse_UnresolvedMentionNotCollected: a mention the resolver can't
// resolve renders as plain text and is NOT in ParsedMentions.
func TestParse_UnresolvedMentionNotCollected(t *testing.T) {
	annotations := []*pb.Annotation{gchatfmt.MakeMentionAnnotation(0, 4, "999")}
	_, _, mentions := gchatfmt.Parse(context.Background(), "@Eve hi", annotations, mustResolve)
	if len(mentions.UserIDs) != 0 || mentions.Room {
		t.Errorf("mentions = %+v, want empty for an unresolvable mention", mentions)
	}
}

// TestParse_NilResolverCollectsNoUsers: with no resolver, no user mention can
// resolve, so ParsedMentions carries no UserIDs (a MENTION_ALL would still
// set Room, but there's none here).
func TestParse_NilResolverCollectsNoUsers(t *testing.T) {
	annotations := []*pb.Annotation{gchatfmt.MakeMentionAnnotation(0, 4, "200")}
	_, _, mentions := gchatfmt.Parse(context.Background(), "@Bob hi", annotations, nil)
	if len(mentions.UserIDs) != 0 {
		t.Errorf("mentions.UserIDs = %v, want empty with a nil resolver", mentions.UserIDs)
	}
}

// TestParse_MalformedMentionExcludedFromMentions is the core phantom-ping
// unit test: an out-of-bounds mention annotation (no corresponding body
// text) forces the plain-text fallback AND must NOT appear in the returned
// ParsedMentions -- even though its gaia id resolves fine, its span is
// invalid so no pill is (or could be) rendered for it.
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

// TestParse_ValidMentionCollectedDespiteUnrelatedMalformedAnnotation: a valid
// mention alongside an unrelated malformed FORMAT annotation. The malformed
// annotation forces the whole HTML render to fall back to plain text, but the
// plain body still contains the mention's "@Bob" text -- so the valid mention
// IS collected (keyed on its own validity, not the overall render result).
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

// TestParse_MentionChipRenderTypeFilteredOut: a mention annotation whose chip
// type isn't DO_NOT_RENDER is a preview chip (M5), not an inline mention --
// gchatfmt renders no pill for it, so it must not be collected either.
func TestParse_MentionChipRenderTypeFilteredOut(t *testing.T) {
	chip := gchatfmt.MakeMentionAnnotation(0, 4, "200")
	chip.ChipRenderType = pb.Annotation_RENDER.Enum()
	_, _, mentions := gchatfmt.Parse(context.Background(), "@Bob hi", []*pb.Annotation{chip}, mustResolve)
	if len(mentions.UserIDs) != 0 {
		t.Errorf("mentions.UserIDs = %v, want empty for a RENDER-chip mention", mentions.UserIDs)
	}
}

// TestParse_MentionsDeduped: the same gaia id mentioned twice yields exactly
// one entry in ParsedMentions.UserIDs.
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

// TestParse_NoAnnotationsEmptyMentions: the empty-annotations fast path
// returns an empty ParsedMentions.
func TestParse_NoAnnotationsEmptyMentions(t *testing.T) {
	_, _, mentions := gchatfmt.Parse(context.Background(), "plain text", nil, mustResolve)
	if len(mentions.UserIDs) != 0 || mentions.Room {
		t.Errorf("mentions = %+v, want empty for a message with no annotations", mentions)
	}
}
