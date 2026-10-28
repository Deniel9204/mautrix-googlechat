package gchatfmt

// Only text-bearing widgets and link buttons are rendered; all strings are bot-controlled.
import (
	"fmt"
	"html"
	"strings"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// RenderCards renders card attachments to plain text and HTML; both are empty if nothing renders.
func RenderCards(attachments []*pb.Attachment) (body, htmlBody string) {
	var out cardOutput
	for _, att := range attachments {
		addOn := att.GetAddOnData()
		if addOn == nil {
			// An HTML attachment carries no card.
			continue
		}
		for _, card := range addOn.GetCards() {
			out.card(card)
		}
	}
	return out.join()
}

// cardOutput accumulates plain and HTML lines in step.
type cardOutput struct {
	plain []string
	html  []string
}

func (o *cardOutput) line(plain, htmlLine string) {
	o.plain = append(o.plain, plain)
	o.html = append(o.html, htmlLine)
}

// text adds an escaped line, skipping empty strings.
func (o *cardOutput) text(s string) {
	if s == "" {
		return
	}
	o.line(s, html.EscapeString(s))
}

func (o *cardOutput) bold(s string) {
	if s == "" {
		return
	}
	o.line(s, "<strong>"+html.EscapeString(s)+"</strong>")
}

func (o *cardOutput) join() (string, string) {
	if len(o.plain) == 0 {
		return "", ""
	}
	return strings.Join(o.plain, "\n"), strings.Join(o.html, "<br/>")
}

func (o *cardOutput) card(card *pb.JAddOnsContextualAddOn_Card) {
	if header := card.GetHeader(); header != nil {
		o.bold(formattedText(header.GetTitle()))
		o.text(formattedText(header.GetSubtitle()))
	}
	for _, section := range card.GetSections() {
		o.bold(formattedText(section.GetHeader()))
		o.text(section.GetDescription())
		for _, widget := range section.GetWidgets() {
			o.widget(widget)
		}
	}
}

// widget renders text-bearing widget arms; buttons are rendered for every widget.
func (o *cardOutput) widget(widget *pb.JAddOnsWidget) {
	switch {
	case widget.GetTextParagraph() != nil:
		o.text(formattedText(widget.GetTextParagraph().GetText()))
	case widget.GetTextWidget() != nil:
		for _, line := range widget.GetTextWidget().GetLine() {
			o.text(line)
		}
	case widget.GetTextKeyValue() != nil:
		kv := widget.GetTextKeyValue()
		o.keyValue(formattedText(kv.GetKey()), formattedText(kv.GetText()))
	case widget.GetKeyValue() != nil:
		kv := widget.GetKeyValue()
		o.keyValue(formattedText(kv.GetTopLabel()), formattedText(kv.GetContent()))
		o.text(formattedText(kv.GetBottomLabel()))
	case widget.GetDivider() != nil:
		o.line("---", "<hr/>")
	}
	for _, button := range widget.GetButtons() {
		o.button(button)
	}
}

func (o *cardOutput) keyValue(key, value string) {
	switch {
	case key != "" && value != "":
		o.line(key+": "+value, "<strong>"+html.EscapeString(key)+":</strong> "+html.EscapeString(value))
	case value != "":
		o.text(value)
	default:
		o.text(key)
	}
}

// button renders a text button with an open-link as a link; otherwise just its label.
func (o *cardOutput) button(button *pb.JAddOnsWidget_Button) {
	textButton := button.GetTextButton()
	if textButton == nil {
		return
	}
	label := formattedText(textButton.GetText())
	if label == "" {
		label = strings.TrimSpace(textButton.GetAltText())
	}
	href := strings.TrimSpace(textButton.GetOnClick().GetOpenLink().GetUrl())

	if href == "" || isDangerousURLScheme(href) {
		// Keep the label so the reader still sees an action existed.
		o.text(label)
		return
	}
	if label == "" {
		label = href
	}
	o.line(fmt.Sprintf("%s: %s", label, href),
		fmt.Sprintf(`<a href="%s">%s</a>`, escapeAttr(href), html.EscapeString(label)))
}

// formattedText returns the unstyled original_text.
func formattedText(t *pb.JAddOnsFormattedText) string {
	return strings.TrimSpace(t.GetOriginalText())
}
