package msgconv

import (
	"context"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv/gchatfmt"
)

// ToMatrix converts a Google Chat Message proto into a
// bridgev2.ConvertedMessage: text_body becomes the part's plain
// Content.Body, and msg.Annotations (previously ignored entirely) drive
// gchatfmt.Parse to additionally produce an HTML Content.FormattedBody
// whenever any are present. mention is the gaiaID->Matrix-pill resolver
// gchatfmt.Parse needs to render mention pills (fix B2); callers pass the
// real one built by pkg/connector/mentions.go's newInboundMentionResolver
// via the connector adapter (msgconv_adapter.go) -- msgconv itself stays
// portal/network-ignorant (see msgconv.go's package doc comment), so
// mention is accepted as a plain function value here, not a *bridgev2.Portal.
//
// Plain-text extraction: the message body starts as msg.GetTextBody()
// (proto field 10, TextBody in the generated Go struct), then
// gchatfmt.AppendLinkAnnotations (handling the
// video_call_metadata/drive_metadata/youtube_metadata annotation branches)
// may extend it with a Drive/Meet/YouTube URL before anything else sees it,
// and with a url_metadata URL when that annotation covers no text -- see that
// function's doc comment for why url_metadata splits on length: the covering
// case is rendered inline as <a href> and must NOT be appended (it would
// render twice), while the non-covering case must be, or a message whose only
// content is such an annotation converts to zero parts and is dropped
// entirely. Either way the connector may ALSO download the media it points at
// and add a separate image part (pkg/connector/media.go). Round-tripping text_body through
// UTF-16 surrogate padding and back is a no-op for the plain Body value
// itself, so text_body is taken verbatim as UTF-8: no surrogate
// encode/decode of Body happens in this file (gchatfmt.Parse does its own
// UTF-16 re-encoding internally, only for annotation offset math -- see its
// package doc comment). Astral-plane characters (e.g. emoji outside the
// BMP) are preserved because Go strings are already UTF-8 byte sequences.
//
// text_body presence gate: a text event is only created when text_body is
// non-empty -- an attachment-only message with an empty text_body gets zero
// text parts, not a part with an empty body. The empty string is the only
// excluded case; a whitespace-only body (e.g. "   ") still produces a part,
// verbatim and untrimmed. Crucially, this gate is evaluated on the text
// AFTER gchatfmt.AppendLinkAnnotations has run -- which can
// extend text_body via its video_call_metadata/drive_metadata/
// youtube_metadata branches -- so a message with NO original text but a
// Drive/Meet/YouTube annotation still gets a text part (body = just the
// appended URL), not zero parts.
//
// Format/FormattedBody gate: gchatfmt.Parse returns html == "" whenever
// annotations is empty (see gchatfmt's package doc comment); this
// function mirrors that exactly by leaving Content.Format unset and
// Content.FormattedBody empty in that case, matching Matrix's own contract
// that a plain m.text event carries no format/formatted_body fields at all.
// When html != "", Content.Format is set to event.FormatHTML and
// Content.FormattedBody to html, while Content.Body always stays the plain
// body gchatfmt.Parse returns (which is text_body verbatim -- see its own
// doc comment on why plain-body derivation is left to the caller).
//
// The returned gchatfmt.ParsedMentions is the set of mentions gchatfmt
// ACTUALLY rendered (resolved user MXIDs + a MENTION_ALL/@room flag),
// surfaced to the connector so it can set content.Mentions ("m.mentions")
// from exactly that set -- see the connector adapter (msgconv_adapter.go)
// and the phantom-ping fix. Returning it separately (rather than setting
// content.Mentions here) keeps the per-part content.Mentions cloning
// discipline in the connector, where the rest of the per-part bookkeeping
// (MessageMetadata) already lives; msgconv stays a pure data-in/data-out
// converter. An empty ParsedMentions is returned for the empty-text
// early-return path, since a message with no text part pings no one.
//
// ThreadRoot: the topic id comes from
// msg.id.parent_id.topic_id.topic_id, combined with the head-vs-reply
// distinction inherent to Google Chat's topic model: message_id == topic_id
// for the head/root message of a topic (self-referencing on the wire), and
// != for every reply posted into an existing one (create_message always
// targets an EXISTING topic id, never the new message's own id). Two cases
// set cm.ThreadRoot to msg's topic id (as a networkid.MessageID):
//
//   - message_id != topic_id (a genuine reply into an existing topic):
//     UNCONDITIONALLY, in every room -- not conditioned on threadsOnly at
//     all.
//   - message_id == topic_id (the head/root message of a brand new topic):
//     ONLY when threadsOnly is true. A flat/legacy room's head message gets
//     no ThreadRoot at all.
//
// threadsOnly is accepted as a plain bool (not a *bridgev2.Portal) to keep
// msgconv portal-ignorant (msgconv.go's package doc); the connector adapter
// (msgconv_adapter.go) reads PortalMetadata.ThreadsOnly and passes it
// through, mirroring how the mention resolver is threaded in as a plain
// function value rather than a portal object.
//
// A self-referencing ThreadRoot (equal to msg's own message id) is exactly
// what bridgev2 itself expects for "this message is the head of its own
// thread": mautrix-go bridgev2/portal.go's getRelationMeta checks
// `currentMsg.ThreadRoot != nil && *currentMsg.ThreadRoot != currentMsgID`
// -- it skips the thread-root DB lookup for the self-reference case but
// still stores it on the message's DB row, so a LATER Matrix reply to this
// head message resolves back to it via GetFirstThreadMessage (the
// reply -> thread auto-conversion, bridgev2/portal.go:1259-1268).
//
// ReplyTo: the wire-level source is always
// msg.reply_to.id.message_id (ReplyToMessage.id, MessageId.message_id --
// proto field 37 -> field 1 -> field 2), set whenever present (a nil-safe
// getter chain: an absent reply_to, or one missing its id/message_id,
// yields "" and cm.ReplyTo stays nil). Resolving the DB row and its Matrix
// mxid is a DB lookup that belongs in the connector, not msgconv
// (msgconv.go's package doc, "no portal, no intent"), so this only surfaces
// the network message id itself via cm.ReplyTo
// (*networkid.MessageOptionalPartID, PartID left nil -- "refer to the first
// part", matching every other single-part Google Chat message); resolving
// it to a concrete Matrix event (or dropping it if the target was never
// bridged) is bridgev2's own generic job (mautrix-go
// bridgev2/portal.go:2768-2801, GetFirstOrSpecificPartByID).
//
// Independent of ThreadRoot above: a reply can also live in a thread (both
// message_id != topic_id AND reply_to set, a quote-reply to a specific
// message within that thread) -- the two are read from disjoint proto
// fields (msg.id.parent_id vs msg.reply_to) and set with no interaction
// between them, so both end up populated together in that case.
//
// Computed BEFORE the empty-text_body early return below (not after), so an
// attachment-only message in a thread still carries the right
// ThreadRoot/ReplyTo even when it has zero text parts (an upload_metadata-
// or should_not_render url_metadata-only message still has none today; a
// Drive/Meet/YouTube-only message no longer falls in that bucket -- see
// AppendLinkAnnotations above).
func (mc *MessageConverter) ToMatrix(ctx context.Context, msg *pb.Message, threadsOnly bool, mention gchatfmt.MentionResolver) (*bridgev2.ConvertedMessage, gchatfmt.ParsedMentions) {
	cm := &bridgev2.ConvertedMessage{
		Parts: make([]*bridgev2.ConvertedMessagePart, 0, 1),
	}

	topicID := msg.GetId().GetParentId().GetTopicId().GetTopicId()
	messageID := msg.GetId().GetMessageId()
	if topicID != "" && (topicID != messageID || threadsOnly) {
		threadRoot := networkid.MessageID(topicID)
		cm.ThreadRoot = &threadRoot
	}

	if replyToID := msg.GetReplyTo().GetId().GetMessageId(); replyToID != "" {
		cm.ReplyTo = &networkid.MessageOptionalPartID{MessageID: networkid.MessageID(replyToID)}
	}

	text := gchatfmt.AppendLinkAnnotations(msg.GetTextBody(), msg.GetAnnotations())
	// A Google Chat app posting a card puts all of its content in widgets and
	// routinely leaves text_body empty, so the attachments have to be
	// consulted before deciding this message is empty -- otherwise a
	// card-only bot message (CI, alerting, ticketing) bridges with no parts
	// at all and simply never appears on Matrix.
	cardBody, cardHTML := gchatfmt.RenderCards(msg.GetAttachments())
	if text == "" && cardBody == "" {
		return cm, gchatfmt.ParsedMentions{}
	}

	var body, html string
	var mentions gchatfmt.ParsedMentions
	if text != "" {
		body, html, mentions = gchatfmt.Parse(ctx, text, msg.GetAnnotations(), mention)
	}
	if cardBody != "" {
		if body != "" {
			// The text half may have produced no HTML of its own (no
			// annotations). It still has to be escaped into the combined
			// formatted_body, or appending the card's HTML would drop it.
			if html == "" {
				html = gchatfmt.EscapePlainToHTML(body)
			}
			body += "\n\n"
			html += "<br/><br/>"
		}
		body += cardBody
		html += cardHTML
	}

	content := &event.MessageEventContent{
		MsgType: event.MsgText,
		Body:    body,
	}
	if html != "" {
		content.Format = event.FormatHTML
		content.FormattedBody = html
	}

	// Empty PartID marks this as the first/only part, matching the wider
	// bridgev2 ecosystem convention (e.g. mautrix-meta's
	// metaid.MakeMessagePartID: index 0 -> ""). Further parts (attachments)
	// can be appended with non-empty sequential IDs alongside this one
	// without renumbering it -- appended, never inserted/overwritten, so a
	// future attachment part built elsewhere and merged into the same
	// bridgev2.ConvertedMessage (e.g. via bridgev2.MergeCaption) can never
	// be clobbered by this function building its own text part (see B4,
	// fixed on the outbound side in pkg/connector/handlematrix.go's
	// mergeAnnotations).
	cm.Parts = append(cm.Parts, &bridgev2.ConvertedMessagePart{
		Type:    event.EventMessage,
		Content: content,
	})
	return cm, mentions
}
