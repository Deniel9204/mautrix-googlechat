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
// metadata types so it can be unit-tested against plain
// *bridgev2.ConvertedMessage output with no DB/connector dependencies at
// all.
import (
	"context"
	"fmt"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv"
)

// convertMessageToMatrix returns a ConvertMessageFunc (see
// simplevent.Message[T]) that calls conv.ToMatrix and attaches
// MessageMetadata{TimestampMicro: msg.create_time} to every converted part.
// TimestampMicro round-trips Google Chat's microsecond create_time through
// the message's DB row so a later quote-reply (SendReplyTarget) can
// reconstruct the original timestamp without re-fetching the source message.
//
// A fresh *MessageMetadata is allocated per part (rather than one shared
// pointer) so a future per-part mutation (e.g. an edit-dedup LastEditTime
// bump on a single part) can never alias into a sibling part's metadata.
//
// It also builds content.Mentions ("m.mentions") from the mentions
// conv.ToMatrix reports it ACTUALLY rendered (gchatfmt.ParsedMentions, fix
// B2/gap G4): every converted part (there is normally exactly one text part,
// but this loop covers future multi-part messages too) gets its OWN
// cloneMentions copy of the same
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
//
// media is the seam onto the attachment download+reupload path
// (media.go's mediaFetcher, see its package doc comment for the full
// layering rationale): production wiring (events.go) passes the real
// *GChatClient; every earlier test in msgconv_adapter_test.go now passes
// a bare nil, which convertAttachmentsToMatrix treats as "no attachments to
// bridge" -- safe, since none of those tests' messages carry an
// UPLOAD_METADATA annotation. A nil media is intentionally NOT the same as
// "attachments unsupported": it just means this particular call site (a
// test, or a future non-Matrix-media consumer) has nothing to fetch with.
func convertMessageToMatrix(conv *msgconv.MessageConverter, media mediaFetcher) func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, msg *pb.Message) (*bridgev2.ConvertedMessage, error) {
	return func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, msg *pb.Message) (*bridgev2.ConvertedMessage, error) {
		resolve := newInboundMentionResolver(portal)
		threadsOnly := false
		if portal != nil {
			if meta, ok := portal.Metadata.(*PortalMetadata); ok && meta != nil {
				threadsOnly = meta.ThreadsOnly
			}
		}
		cm, parsed := conv.ToMatrix(ctx, msg, threadsOnly, resolve)
		ts := msg.GetCreateTime()
		// topic_id: stamped on every part regardless of whether
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

		// Attachments: appended AFTER the text-part stamping
		// loop above, not folded into it -- an attachment part shares the
		// same TimestampMicro/TopicID as the text part (both describe the
		// SAME Google Chat message) but must NOT inherit content.Mentions:
		// a media message event never sets .mentions at all, and cloning the
		// text part's ping onto every attachment event would double-notify
		// every mentioned user once per Matrix event instead of once per
		// Google Chat message.
		attachmentParts := convertAttachmentsToMatrix(ctx, portal, intent, msg, media)
		for _, part := range attachmentParts {
			part.DBMetadata = &MessageMetadata{TimestampMicro: ts, TopicID: topicID}
		}
		cm.Parts = append(cm.Parts, attachmentParts...)

		return cm, nil
	}
}

// convertEditToMatrix returns a simplevent.Message[*pb.Message].ConvertEditFunc
// (see simplevent's own doc comment on the shape this must match) for the
// MESSAGE_UPDATED body arm (events.go's queueMessageEdit). It does
// dedup + re-conversion of the edited message:
//
//	edit_ts = last_edit_time or last_update_time
//	if stored_last_edit_time >= edit_ts: return  # dedup
//	if target is not "m.text" or has no text_body: return
//	re-convert the body and mark it as an edit of the target
//
// Dedup compares editTS (msg.GetLastEditTime(), or msg.GetLastUpdateTime()
// when LastEditTime is unset -- an `or` fallback) against the edit target's
// OWN stored MessageMetadata.LastEditTime (dbmeta.go): a per-message dedup
// value persisted on the message row itself rather than an in-memory map, so
// it survives a bridge restart. A duplicate/stale edit (editTS <= stored) is
// reported via bridgev2.ErrIgnoringRemoteEvent -- the exact same signal
// mautrix-meta's own WhatsApp edit dedup uses
// (_reference/meta/pkg/connector/events.go's WAMessageEvent.ConvertEdit) --
// which portal.handleRemoteEdit (mautrix-go bridgev2/portal.go) treats as
// EventHandlingResultIgnored, not a failure.
//
// ONLY the text part (the existing DB part whose PartID == gcid.TextPartID,
// "") is ever read or modified -- found by an explicit scan of `existing`,
// NOT by indexing existing[0]. This is the non-text guard: skip the edit
// unless the target is an "m.text" message with a non-empty text_body --
// figuring out how to map multipart message edits to Matrix is hard, so
// don't even try. A Google Chat message can be multi-part (a text part
// "" plus att_0/att_1... attachment parts), and an ATTACHMENT-ONLY message
// persists ONLY its att_0 part (ToMatrix returns no text part for an empty
// text_body). existing[0] is therefore NO LONGER guaranteed to be the text
// part:
//
//   - an attachment-only message's existing[0] is the att_0 (m.image) row;
//     a MESSAGE_UPDATED that now carries non-empty text (e.g. an added
//     caption, or a Drive/Meet/YouTube link annotation surfaced as text by
//     gchatfmt.AppendLinkAnnotations) would, if applied to existing[0],
//     m.replace the IMAGE event with an m.text body -- silently replacing
//     the image in every client and corrupting the att_0 row;
//   - even a text+attachment message is unsafe to index by position: the DB
//     query bridgev2 uses to load the parts for an edit
//     (GetAllPartsByID/getAllMessagePartsByIDQuery) has NO ORDER BY, and
//     sendConvertedEdit's DB.Message.Update relocates the heap tuple on
//     Postgres, so after the first edit the parts can come back
//     att_0-first -- reading dedup off the wrong part (att_0's zero
//     LastEditTime bypasses the dedup gate) and corrupting the image on the
//     second edit. Insertion order is not a contract; scanning for the ""
//     PartID removes all ordering dependence and works identically on
//     SQLite and Postgres.
//
// If no "" text part exists at all (an attachment-only message, or any
// message with no text part), the whole edit is ignored via
// bridgev2.ErrIgnoringRemoteEvent -- the "don't even try" case.
// (Redaction/reactions/replies are unaffected: they redact every
// part, or use the ORDER BY'd GetFirstPartByID/GetLastPartByID; only this
// edit path used the unordered query.)
//
// Unlike convertMessageToMatrix above, this does NOT stamp a fresh
// *MessageMetadata onto the converted part's DBMetadata: conv.ToMatrix is
// called directly (not through convertMessageToMatrix), so the returned
// part's DBMetadata is nil, and ToEditPart's "cmp.DBMetadata != nil" branch
// is skipped -- the text part's Metadata is left as-is by ToEditPart, and
// only its LastEditTime field is bumped afterward, in place, below. This
// preserves TimestampMicro/TopicID untouched (an edit never changes a
// message's original create_time or the topic it belongs to) without
// needing a database.MetaMerger implementation on *MessageMetadata --
// mirroring WAMessageEvent.ConvertEdit's identical
// "ToEditPart with no DBMetadata set, mutate the target part's Metadata
// directly afterward" pattern.
//
// content.Mentions is deliberately left unset on the returned part (and
// ConvertedEditPart.NewMentions is left nil): mautrix-go's own
// sendConvertedEdit (bridgev2/portal.go) always resets Content.Mentions to
// either NewMentions (if set) or an empty *event.Mentions{} before sending,
// regardless of what this function puts there -- so a re-ping only happens
// when NewMentions is explicitly populated. Leaving NewMentions nil here
// matches the wider bridgev2 ecosystem's deliberate "edits don't re-notify"
// convention (mautrix-meta's own WAMessageEvent.ConvertEdit does not set
// NewMentions either), rather than rebuilding content.mentions the same way
// a brand new message would (which WOULD re-ping every mentioned user on
// every edit) -- a documented, intentional UX deviation, not a fidelity
// gap: the mention PILL in the edited HTML body (rendered by gchatfmt.Parse
// via conv.ToMatrix, same as any other message) is unaffected either way.
func convertEditToMatrix(conv *msgconv.MessageConverter) func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, existing []*database.Message, msg *pb.Message) (*bridgev2.ConvertedEdit, error) {
	return func(ctx context.Context, portal *bridgev2.Portal, _ bridgev2.MatrixAPI, existing []*database.Message, msg *pb.Message) (*bridgev2.ConvertedEdit, error) {
		// Find the text part explicitly by its "" PartID, never by position
		// -- see the doc comment above (the non-text guard + the Postgres
		// ordering hazard M5's multi-part messages introduced). A message
		// with no text part at all (attachment-only, or empty existing) is
		// the non-text-message case: ignore the whole edit.
		var target *database.Message
		for _, part := range existing {
			if part.PartID == gcid.TextPartID {
				target = part
				break
			}
		}
		if target == nil {
			return nil, fmt.Errorf("%w: googlechat edit of non-text message (no text part to edit)", bridgev2.ErrIgnoringRemoteEvent)
		}

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
			// text_body empty (the no-text-body half of the non-text
			// guard): the edit removed all text, which this bridge does not
			// try to map to Matrix -- drop it. conv.ToMatrix only ever emits
			// the text part (attachments are appended by
			// convertMessageToMatrix, not here), so cm.Parts[0] below is
			// always that text part.
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
