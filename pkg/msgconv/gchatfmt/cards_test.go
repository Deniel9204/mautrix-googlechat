package gchatfmt_test

import (
	"strings"
	"testing"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv/gchatfmt"
)

func fmtText(s string) *pb.JAddOnsFormattedText {
	return &pb.JAddOnsFormattedText{OriginalText: &s}
}

func cardAttachment(cards ...*pb.JAddOnsContextualAddOn_Card) *pb.Attachment {
	return &pb.Attachment{
		Type: &pb.Attachment_AddOnData{
			AddOnData: &pb.JAddOnsContextualAddOn{Cards: cards},
		},
	}
}

func textParagraph(s string) *pb.JAddOnsWidget {
	return &pb.JAddOnsWidget{
		Data: &pb.JAddOnsWidget_TextParagraph_{
			TextParagraph: &pb.JAddOnsWidget_TextParagraph{Text: fmtText(s)},
		},
	}
}

func buttonWidget(label, href string) *pb.JAddOnsWidget {
	return &pb.JAddOnsWidget{
		Buttons: []*pb.JAddOnsWidget_Button{{
			Type: &pb.JAddOnsWidget_Button_TextButton{
				TextButton: &pb.JAddOnsWidget_TextButton{
					Text: fmtText(label),
					OnClick: &pb.JAddOnsOnClick{
						DataCase: &pb.JAddOnsOnClick_OpenLink{
							OpenLink: &pb.JAddOnsOpenLink{Url: &href},
						},
					},
				},
			},
		}},
	}
}

// TestRenderCards_HeaderSectionsAndButton is the core case: a bot card whose
// content lives entirely in widgets. Before this, such a message reached
// Matrix completely empty.
func TestRenderCards_HeaderSectionsAndButton(t *testing.T) {
	att := cardAttachment(&pb.JAddOnsContextualAddOn_Card{
		Header: &pb.JAddOnsContextualAddOn_Card_CardHeader{
			Title:    fmtText("Build failed"),
			Subtitle: fmtText("main @ abc1234"),
		},
		Sections: []*pb.JAddOnsContextualAddOn_Card_Section{{
			Header: fmtText("Details"),
			Widgets: []*pb.JAddOnsWidget{
				textParagraph("3 tests failed"),
				buttonWidget("Open build", "https://ci.example.org/build/1"),
			},
		}},
	})

	body, html := gchatfmt.RenderCards([]*pb.Attachment{att})

	for _, want := range []string{"Build failed", "main @ abc1234", "Details", "3 tests failed", "Open build", "https://ci.example.org/build/1"} {
		if !strings.Contains(body, want) {
			t.Errorf("body missing %q\nbody = %q", want, body)
		}
	}
	if !strings.Contains(html, "<strong>Build failed</strong>") {
		t.Errorf("html missing bold card title\nhtml = %q", html)
	}
	if !strings.Contains(html, `<a href="https://ci.example.org/build/1">Open build</a>`) {
		t.Errorf("html missing the button link\nhtml = %q", html)
	}
}

// TestRenderCards_EscapesHostileContent: card text and button hrefs are
// remote, bot-controlled input, so they get the same treatment as annotation
// content -- escaped, and never linkified for a script-bearing scheme.
func TestRenderCards_EscapesHostileContent(t *testing.T) {
	att := cardAttachment(&pb.JAddOnsContextualAddOn_Card{
		Sections: []*pb.JAddOnsContextualAddOn_Card_Section{{
			Widgets: []*pb.JAddOnsWidget{
				textParagraph(`<script>alert(1)</script>`),
				buttonWidget("Click me", "javascript:alert(1)"),
				buttonWidget(`evil"><script>`, "https://ok.example/"),
			},
		}},
	})

	_, html := gchatfmt.RenderCards([]*pb.Attachment{att})

	if strings.Contains(html, "<script>") {
		t.Errorf("card text was not escaped\nhtml = %q", html)
	}
	if strings.Contains(html, "javascript:") {
		t.Errorf("a javascript: button was linkified\nhtml = %q", html)
	}
	if strings.Contains(html, `evil"><script>`) {
		t.Errorf("button label was not escaped\nhtml = %q", html)
	}
}

// TestRenderCards_SkipsUnsupportedWidgets: the widget oneof has ~14 arms and
// only the text-bearing ones are rendered. An unsupported one must cost only
// itself, not the widgets beside it.
func TestRenderCards_SkipsUnsupportedWidgets(t *testing.T) {
	att := cardAttachment(&pb.JAddOnsContextualAddOn_Card{
		Sections: []*pb.JAddOnsContextualAddOn_Card_Section{{
			Widgets: []*pb.JAddOnsWidget{
				textParagraph("before"),
				{Data: &pb.JAddOnsWidget_DateTimePicker_{DateTimePicker: &pb.JAddOnsWidget_DateTimePicker{}}},
				{}, // no arm set at all
				textParagraph("after"),
			},
		}},
	})

	body, _ := gchatfmt.RenderCards([]*pb.Attachment{att})

	if !strings.Contains(body, "before") || !strings.Contains(body, "after") {
		t.Errorf("an unsupported widget swallowed its neighbours\nbody = %q", body)
	}
}

func TestRenderCards_NothingToRender(t *testing.T) {
	if body, html := gchatfmt.RenderCards(nil); body != "" || html != "" {
		t.Errorf("RenderCards(nil) = (%q, %q), want empty", body, html)
	}
	// An attachment carrying no card data at all (the html arm of the oneof).
	att := &pb.Attachment{Type: &pb.Attachment_Html{Html: &pb.HtmlAttachment{}}}
	if body, html := gchatfmt.RenderCards([]*pb.Attachment{att}); body != "" || html != "" {
		t.Errorf("RenderCards(non-card attachment) = (%q, %q), want empty", body, html)
	}
}
