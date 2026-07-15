package msgconv

import (
	"context"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv/gchatfmt"
)

// ToMatrix converts a Google Chat Message proto into a bridgev2.
// ConvertedMessage. This is a port of from_googlechat.py's
// googlechat_to_matrix (from_googlechat.py:29-57), now covering its full
// M3 scope: text_body becomes the part's plain Content.Body, and -- unlike
// M2, which ignored them entirely -- msg.Annotations drive
// gchatfmt.Parse (Task 1) to additionally produce an HTML
// Content.FormattedBody whenever any are present. mention is the
// gaiaID->Matrix-pill resolver gchatfmt.Parse needs to render mention pills
// (fix B2); callers pass the real one built by
// pkg/connector/mentions.go's newInboundMentionResolver (Task 3) via the
// connector adapter (msgconv_adapter.go, Task 4) -- msgconv itself stays
// portal/network-ignorant (see msgconv.go's package doc comment), so
// mention is accepted as a plain function value here, not a *bridgev2.Portal.
//
// Plain-text extraction: the message body is exactly msg.GetTextBody()
// (proto field 10, TextBody in the generated Go struct). Python builds
// TextMessageEventContent(body=add_surrogate(evt.text_body)) and later,
// when there is no formatted_body, sets content.body =
// del_surrogate(content.body) -- i.e. round-trips text_body through UTF-16
// surrogate padding and back unchanged, which is a no-op for the plain
// Body value itself. Here text_body is taken verbatim as UTF-8: no
// surrogate encode/decode of Body happens in this file (gchatfmt.Parse
// does its own UTF-16 re-encoding internally, only for annotation offset
// math -- see its package doc comment). Astral-plane characters (e.g.
// emoji outside the BMP) are preserved because Go strings are already
// UTF-8 byte sequences.
//
// text_body presence gate: Python only creates a text event at all when
// evt.text_body is truthy (portal.py:1411, `if evt.text_body:`) -- an
// attachment-only message with an empty text_body gets zero text parts, not
// a part with an empty body. This mirrors Python's exact truthiness rule:
// the empty string is the only falsy case; a whitespace-only body (e.g.
// "   ") is truthy in Python and therefore still produces a part, verbatim
// and untrimmed.
//
// Format/FormattedBody gate: gchatfmt.Parse returns html == "" whenever
// annotations is empty (its own fix for the Python always-truthy-annotations
// bug at from_googlechat.py:45 -- see gchatfmt's package doc comment); this
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
// ThreadRoot (M3 Task 6), ported from handle_googlechat_message's parent_id
// extraction (portal.py:1379, `parent_id = evt.id.parent_id.topic_id.topic_id`)
// plus the head-vs-reply distinction inherent to Google Chat's topic model:
// message_id == topic_id for the head/root message of a topic (self-
// referencing on the wire), and != for every reply posted into an existing
// one (maugclib/client.py's create_message always targets an EXISTING
// topic id, never the new message's own id -- see send_message,
// client.py:441-458). Two cases set cm.ThreadRoot to msg's topic id (as a
// networkid.MessageID):
//
//   - message_id != topic_id (a genuine reply into an existing topic):
//     UNCONDITIONALLY, in every room -- matching Python's unconditional
//     `if parent_id:` gate (portal.py:1380), which is not conditioned on
//     self.threads_only at all.
//   - message_id == topic_id (the head/root message of a brand new topic):
//     ONLY when threadsOnly is true -- matching Python's
//     `if thread_parent or self.threads_only:` self-reference
//     (_append_event_id, portal.py:1406-1409). A flat/legacy room's head
//     message gets no ThreadRoot at all.
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
// reply -> thread auto-conversion the task brief calls out,
// bridgev2/portal.go:1259-1268).
//
// ReplyTo (M3 Task 7), ported from handle_googlechat_message's reply_to
// resolution (portal.py:1390-1396, `if evt.reply_to: reply_to_db =
// DBMessage.get_by_gcid(evt.reply_to.id.message_id, ...)`): the wire-level
// source is always msg.reply_to.id.message_id (ReplyToMessage.id,
// MessageId.message_id -- proto field 37 -> field 1 -> field 2), set
// whenever present (a nil-safe getter chain: an absent reply_to, or one
// missing its id/message_id, yields "" and cm.ReplyTo stays nil). Unlike
// Python, which resolves the DB row and its Matrix mxid right here (a DB
// lookup that belongs in the connector, not msgconv -- msgconv.go's package
// doc, "no portal, no intent"), this only surfaces the network message id
// itself via cm.ReplyTo (*networkid.MessageOptionalPartID, PartID left nil
// -- "refer to the first part", matching every other single-part Google
// Chat message); resolving it to a concrete Matrix event (or dropping it if
// the target was never bridged) is bridgev2's own generic job
// (mautrix-go bridgev2/portal.go:2768-2801, GetFirstOrSpecificPartByID).
//
// Independent of ThreadRoot above: a reply can also live in a thread (both
// message_id != topic_id AND reply_to set, a quote-reply to a specific
// message within that thread) -- the two are read from disjoint proto
// fields (msg.id.parent_id vs msg.reply_to) and set with no interaction
// between them, so both end up populated together in that case, matching
// what portal.py's own thread_parent + non-fallback reply_to combination
// produces (portal.py:891-894).
//
// Computed BEFORE the empty-text_body early return below (not after), so a
// future (M5) attachment-only message in a thread still carries the right
// ThreadRoot/ReplyTo even though it has zero text parts today.
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

	text := msg.GetTextBody()
	if text == "" {
		return cm, gchatfmt.ParsedMentions{}
	}

	body, html, mentions := gchatfmt.Parse(ctx, text, msg.GetAnnotations(), mention)
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
	// metaid.MakeMessagePartID: index 0 -> ""). M5 can append further parts
	// (attachments) with non-empty sequential IDs alongside this one
	// without renumbering it -- appended, never inserted/overwritten, so a
	// future attachment part built elsewhere and merged into the same
	// bridgev2.ConvertedMessage (e.g. via bridgev2.MergeCaption) can never
	// be clobbered by this function building its own text part (see B4,
	// docs/research/08d-megabridge-msgconv.md §2.4, fixed on the outbound
	// side in pkg/connector/handlematrix.go's mergeAnnotations).
	cm.Parts = append(cm.Parts, &bridgev2.ConvertedMessagePart{
		Type:    event.EventMessage,
		Content: content,
	})
	return cm, mentions
}
