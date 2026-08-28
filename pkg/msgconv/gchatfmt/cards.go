package gchatfmt

// cards.go -- rendering of the card attachments a Google Chat app/bot
// message carries (Message.attachments -> Attachment.add_on_data).
//
// A bot posting a card puts ALL of its content in widgets and commonly
// leaves text_body empty, so before this existed such a message reached
// Matrix with nothing in it at all -- a silent loss for exactly the
// workflow-bot traffic (CI, alerting, ticketing) cards are used for.
//
// # Scope
//
// The widget oneof has fourteen arms, most of them interactive controls
// (menus, text fields, selection controls, date pickers) that Matrix has no
// way to present and that would do nothing if it did -- clicking them posts
// back to the bot through a session this bridge does not model. Only the
// text-bearing widgets are rendered, plus buttons that carry a plain link,
// which is also what purple-googlechat's card handling settles for. An
// unrecognised widget is skipped and costs only itself, never its
// neighbours.
//
// # Untrusted input
//
// Every string here is bot-controlled, so it gets exactly the treatment
// annotation content gets in convert.go: text is HTML-escaped, hrefs are
// attribute-escaped, and a button whose link uses a script-bearing scheme
// (javascript:, data:) is rendered as its plain label rather than linkified
// -- see isDangerousURLScheme.
import (
	"fmt"
	"html"
	"strings"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// RenderCards renders every card attachment into a plain-text body and its
// HTML equivalent. Both are empty when there is nothing renderable, which is
// the caller's signal that the attachments contributed no content.
func RenderCards(attachments []*pb.Attachment) (body, htmlBody string) {
	var out cardOutput
	for _, att := range attachments {
		addOn := att.GetAddOnData()
		if addOn == nil {
			// The other arm of the oneof (an HTML attachment) carries no card.
			continue
		}
		for _, card := range addOn.GetCards() {
			out.card(card)
		}
	}
	return out.join()
}

// cardOutput accumulates the two renditions line by line, so the plain and
// HTML forms cannot drift out of step.
type cardOutput struct {
	plain []string
	html  []string
}

func (o *cardOutput) line(plain, htmlLine string) {
	o.plain = append(o.plain, plain)
	o.html = append(o.html, htmlLine)
}

// text adds one escaped plain line, skipping empties so a card full of unset
// optional fields renders as nothing rather than a run of blank lines.
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

// widget renders the text-bearing arms of the widget oneof. Buttons hang off
// the widget itself rather than being an arm, so they are rendered for every
// widget regardless of which arm (if any) matched.
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

// button renders a text button as a link. An image button carries no text to
// show, and a button whose action posts back to the bot (rather than opening
// a link) cannot be actioned from Matrix, so both degrade to their label.
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
		// No link, or one no client should be handed: keep the label so the
		// reader still sees that an action existed.
		o.text(label)
		return
	}
	if label == "" {
		label = href
	}
	o.line(fmt.Sprintf("%s: %s", label, href),
		fmt.Sprintf(`<a href="%s">%s</a>`, escapeAttr(href), html.EscapeString(label)))
}

// formattedText takes the plain rendition of a card's rich-text value. The
// formatted_text_elements side carries per-run styling this renderer does
// not attempt; original_text is the same content without it.
func formattedText(t *pb.JAddOnsFormattedText) string {
	return strings.TrimSpace(t.GetOriginalText())
}
