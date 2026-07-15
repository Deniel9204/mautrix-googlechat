package connector

// msgconv_adapter.go adapts msgconv.MessageConverter.ToMatrix (pure
// *pb.Message -> *bridgev2.ConvertedMessage conversion, pkg/msgconv/from-gchat.go)
// into the simplevent.Message[*pb.Message].ConvertMessageFunc shape
// events.go's MESSAGE_POSTED handling needs, and stamps MessageMetadata
// (dbmeta.go) onto every resulting part.
//
// This bookkeeping deliberately lives here, in the connector, and not in
// msgconv: msgconv's package doc (msgconv.go) states it holds "no portal, no
// intent, no gchatmeow client" and must stay ignorant of connector-local
// metadata types so it can be unit-tested (Task 3) against plain
// *bridgev2.ConvertedMessage output with no DB/connector dependencies at
// all.
import (
	"context"
	"fmt"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv"
)

// convertMessageToMatrix returns a ConvertMessageFunc (see
// simplevent.Message[T]) that calls conv.ToMatrix and attaches
// MessageMetadata{TimestampMicro: msg.create_time} to every converted part.
// TimestampMicro round-trips Google Chat's microsecond create_time through
// the message's DB row so a later quote-reply (M3, SendReplyTarget) can
// reconstruct the original timestamp without re-fetching the source message.
//
// A fresh *MessageMetadata is allocated per part (rather than one shared
// pointer) so a future per-part mutation (e.g. M4's edit-dedup LastEditTime
// bump on a single part) can never alias into a sibling part's metadata.
//
// It also builds content.Mentions ("m.mentions") from the mentions
// conv.ToMatrix reports it ACTUALLY rendered (gchatfmt.ParsedMentions, fix
// B2/gap G4 -- docs/research/08d §1.7/§6): every converted part (there is
// normally exactly one text part, but this loop covers M5's future
// multi-part messages too) gets its OWN cloneMentions copy of the same
// resolved Mentions block (content.Mentions describes the whole event's
// mentions, not a per-part concept, but each part still gets an independent
// object) -- matching the adjacent *MessageMetadata allocation's own "never
// alias into a sibling part" rule, just above.
//
// Deriving content.Mentions from ParsedMentions (rather than an independent
// second walk of msg.GetAnnotations(), as an earlier version did via the
// now-removed inboundMentions) is the phantom-ping fix: "who gets pinged" is
// now, by construction, exactly "whose mention pill gchatfmt would emit for
// this message" -- both keyed on the same per-annotation validity gate
// (spanWithinParent bounds + chip filter + resolver ok) inside gchatfmt. A
// malformed/out-of-bounds mention annotation that renders no pill (and whose
// "@Name" text is therefore absent from the body) can no longer sneak a
// ping into content.Mentions.
//
// The resolver (built once, below) is still passed INTO conv.ToMatrix, which
// threads it through gchatfmt.Parse to both render pills and resolve the
// ParsedMentions gaia ids -- so a single resolver instance drives both
// within one conversion.
func convertMessageToMatrix(conv *msgconv.MessageConverter) func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, msg *pb.Message) (*bridgev2.ConvertedMessage, error) {
	return func(ctx context.Context, portal *bridgev2.Portal, _ bridgev2.MatrixAPI, msg *pb.Message) (*bridgev2.ConvertedMessage, error) {
		resolve := newInboundMentionResolver(portal)
		threadsOnly := false
		if portal != nil {
			if meta, ok := portal.Metadata.(*PortalMetadata); ok && meta != nil {
				threadsOnly = meta.ThreadsOnly
			}
		}
		cm, parsed := conv.ToMatrix(ctx, msg, threadsOnly, resolve)
		ts := msg.GetCreateTime()
		// topic_id (M3 Task 6): stamped on every part regardless of whether
		// ToMatrix decided to set cm.ThreadRoot for THIS message, so a
		// later Matrix reply into this same topic can look up the topic id
		// it belongs to (handlematrix.go's outbound routing reads it back
		// off the resolved ThreadRoot message's own MessageMetadata).
		// Recomputed independently here (mirroring ts above) rather than
		// threaded through ToMatrix's return values, matching the existing
		// "adapter recomputes its own bookkeeping values from msg" pattern.
		topicID := msg.GetId().GetParentId().GetTopicId().GetTopicId()
		mentions := mentionsFromParsed(parsed)
		for _, part := range cm.Parts {
			part.DBMetadata = &MessageMetadata{TimestampMicro: ts, TopicID: topicID}
			if mentions != nil {
				part.Content.Mentions = cloneMentions(mentions)
			}
		}
		return cm, nil
	}
}

// convertEditToMatrix returns a simplevent.Message[*pb.Message].ConvertEditFunc
// (see simplevent's own doc comment on the shape this must match) for the
// MESSAGE_UPDATED body arm (events.go's queueMessageEdit, M4 Task 1),
// porting handle_googlechat_edit's dedup + re-conversion (portal.py:1228-1260):
//
//	edit_ts = evt.last_edit_time or evt.last_update_time
//	if self._edit_dedup[msg_id] >= edit_ts: return  # dedup, portal.py:1238-1240
//	...
//	elif target.msgtype != "m.text" or not evt.text_body: return  # portal.py:1248-1251
//	content = await fmt.googlechat_to_matrix(source, evt, self)
//	content.set_edit(target.mxid)
//
// Dedup compares editTS (msg.GetLastEditTime(), or msg.GetLastUpdateTime()
// when LastEditTime is unset -- exactly Python's `or` fallback,
// portal.py:1236) against the edit target's OWN stored
// MessageMetadata.LastEditTime (dbmeta.go): the Go equivalent of Python's
// process-local self._edit_dedup dict, except persisted on the message row
// itself rather than an in-memory map, so it survives a bridge restart. A
// duplicate/stale edit (editTS <= stored) is reported via
// bridgev2.ErrIgnoringRemoteEvent -- the exact same signal
// mautrix-meta's own WhatsApp edit dedup uses
// (_reference/meta/pkg/connector/events.go's WAMessageEvent.ConvertEdit) --
// which portal.handleRemoteEdit (mautrix-go bridgev2/portal.go) treats as
// EventHandlingResultIgnored, not a failure.
//
// Only part 0 (existing[0]) is ever read or modified -- matching Python's
// `target = await DBMessage.get_by_gcid(msg_id, ..., index=0)`: this bridge
// has no multi-part text messages to disambiguate between yet (M5's
// attachments), so existing[0] is always the message's one and only part.
//
// Unlike convertMessageToMatrix above, this does NOT stamp a fresh
// *MessageMetadata onto the converted part's DBMetadata: conv.ToMatrix is
// called directly (not through convertMessageToMatrix), so the returned
// part's DBMetadata is nil, and ToEditPart's "cmp.DBMetadata != nil" branch
// is skipped -- existing[0].Metadata is left as-is by ToEditPart, and only
// its LastEditTime field is bumped afterward, in place, below. This
// preserves TimestampMicro/TopicID untouched (an edit never changes a
// message's original create_time or the topic it belongs to) without
// needing a database.MetaMerger implementation on *MessageMetadata --
// mirroring WAMessageEvent.ConvertEdit's identical
// "ToEditPart with no DBMetadata set, mutate existing[0].Metadata directly
// afterward" pattern.
//
// content.Mentions is deliberately left unset on the returned part (and
// ConvertedEditPart.NewMentions is left nil): mautrix-go's own
// sendConvertedEdit (bridgev2/portal.go) always resets Content.Mentions to
// either NewMentions (if set) or an empty *event.Mentions{} before sending,
// regardless of what this function puts there -- so a re-ping only happens
// when NewMentions is explicitly populated. This intentionally diverges from
// portal.py's content.mentions (built the same way as a brand new message,
// which WOULD re-ping every mentioned user on every edit): leaving
// NewMentions nil here instead matches the wider bridgev2 ecosystem's
// deliberate "edits don't re-notify" convention (mautrix-meta's own
// WAMessageEvent.ConvertEdit does not set NewMentions either) rather than
// Python's older behavior -- a documented, intentional UX deviation, not a
// fidelity gap: the mention PILL in the edited HTML body (rendered by
// gchatfmt.Parse via conv.ToMatrix, same as any other message) is
// unaffected either way.
func convertEditToMatrix(conv *msgconv.MessageConverter) func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, existing []*database.Message, msg *pb.Message) (*bridgev2.ConvertedEdit, error) {
	return func(ctx context.Context, portal *bridgev2.Portal, _ bridgev2.MatrixAPI, existing []*database.Message, msg *pb.Message) (*bridgev2.ConvertedEdit, error) {
		if len(existing) == 0 {
			return nil, fmt.Errorf("googlechat: edit target has no parts")
		}
		target := existing[0]

		editTS := msg.GetLastEditTime()
		if editTS == 0 {
			editTS = msg.GetLastUpdateTime()
		}
		if meta, ok := target.Metadata.(*MessageMetadata); ok && meta != nil && meta.LastEditTime >= editTS {
			return nil, fmt.Errorf("%w: duplicate/stale googlechat edit (edit_ts=%d, stored_last_edit_time=%d)", bridgev2.ErrIgnoringRemoteEvent, editTS, meta.LastEditTime)
		}

		resolve := newInboundMentionResolver(portal)
		threadsOnly := false
		if portal != nil {
			if meta, ok := portal.Metadata.(*PortalMetadata); ok && meta != nil {
				threadsOnly = meta.ThreadsOnly
			}
		}
		cm, _ := conv.ToMatrix(ctx, msg, threadsOnly, resolve)
		if len(cm.Parts) == 0 {
			// evt.text_body empty -- matches portal.py's
			// `elif target.msgtype != "m.text" or not evt.text_body:` drop
			// (the msgtype half is covered generically: this bridge stores
			// no non-text message parts to edit yet).
			return nil, fmt.Errorf("%w: googlechat edit has no text body", bridgev2.ErrIgnoringRemoteEvent)
		}

		editPart := cm.Parts[0].ToEditPart(target)

		if meta, ok := target.Metadata.(*MessageMetadata); ok && meta != nil {
			meta.LastEditTime = editTS
		} else {
			target.Metadata = &MessageMetadata{LastEditTime: editTS}
		}

		return &bridgev2.ConvertedEdit{ModifiedParts: []*bridgev2.ConvertedEditPart{editPart}}, nil
	}
}
