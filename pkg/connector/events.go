package connector

// handleGChatEvent is the OnStreamEvent callback installed on every
// gchatmeow.Client by GChatClient.wireAndStart (client.go). It runs
// synchronously and in order on the client's Connect goroutine (see
// pkg/gchatmeow/client.go's "Goroutine model" doc comment), and receives one
// already-flattened *pb.Event per body -- gchatmeow's splitEventBodies has
// already copied the parent frame's group_id/type onto each one (see
// pkg/gchatmeow/client_test.go's TestSplitEventBodies), so evt.GetGroupId()
// and evt.GetType() are always this specific event's own, never the parent
// multi-body frame's.
//
// dispatchGChatEvent (below) is a type-switch skeleton over every event body
// type the proto defines. M1 left every arm as a "not yet handled" debug
// log; M2 Task 4 fills in MessagePosted (new messages only -- see
// handleMessagePosted's doc comment for why edits share this same body arm
// but are routed elsewhere). Later M2+ tasks fill in the rest.
//
// handleGChatEvent advances the revision watermark(s) ONLY AFTER a
// successful dispatch, and only from the dispatch's result -- porting
// user.py:674-682's on_stream_event, which advances the user watermark from
// evt.user_revision on every event, but ORDERED so the message is delivered
// first. Ordering matters: an earlier revision of this handler advanced the
// watermark at the TOP, before dispatch, so if QueueRemoteEvent FAILED the
// message was dropped yet the cursor moved past it -> unrecoverable +
// invisible on the next catch_up (M2-review Important #3, "persist watermark
// only after successful handling"). Now dispatch runs first; the watermark
// advances only when dispatch reports Success (a real queue, or a
// legitimate ignore/no-op), never after a Failed one -- so a dropped event
// is re-fetched on the next reconnect's catch_up_user. user_revision feeds
// the user watermark (advanceUserRevision) and group_revision the per-portal
// one (advancePortalRevision); the two are separate revision spaces and must
// not cross (see those functions' doc comments, M2-review Important #2).
//
// Returns the dispatch result so backfill.go's catchUp drain can observe a
// handling failure and stop before advancing past it; the live-stream
// OnStreamEvent callback (wireAndStart, client.go) discards it.
import (
	"context"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/variationselector"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// typingTimeout is the Matrix typing timeout sent for an ACTIVE typing
// notification, porting handle_googlechat_typing's hardcoded
// `timeout=6000 if status == googlechat.TYPING else 0` (portal.py:1608-1610)
// -- 6000ms verbatim.
const typingTimeout = 6 * time.Second

func (c *GChatClient) handleGChatEvent(ctx context.Context, evt *pb.Event) bridgev2.EventHandlingResult {
	res := c.dispatchGChatEvent(ctx, evt)
	if res.Success {
		c.advanceUserRevision(ctx, evt)
		c.advancePortalRevision(ctx, evt)
	} else {
		zerolog.Ctx(ctx).Warn().Msg("googlechat: event handling failed, not advancing revision watermark (next catch_up will retry it)")
	}
	return res
}

// dispatchGChatEvent routes evt to its body-type handler and returns the
// handling result. Non-message body arms are still unhandled (M2+) and
// report EventHandlingResultIgnored (Success, no-op) -- they carry nothing to
// deliver, so treating them as successfully handled lets the watermark
// advance past them (matching Python advancing on any user_revision event),
// while a genuine QueueRemoteEvent failure on the MessagePosted arm surfaces
// as a non-Success result and blocks the advance.
func (c *GChatClient) dispatchGChatEvent(ctx context.Context, evt *pb.Event) bridgev2.EventHandlingResult {
	log := zerolog.Ctx(ctx)
	switch evt.GetBody().GetType().(type) {
	case *pb.Event_EventBody_GroupViewed:
		return c.queueGroupViewed(ctx, evt)
	case *pb.Event_EventBody_GroupUpdated:
		// Intentionally unhandled, matching Python: room renames and
		// topic/description changes reach this bridge as a SYSTEM_MESSAGE
		// with a ROOM_UPDATED annotation on the MessagePosted body instead
		// (handleMessagePosted -> systemmessage.go's trySystemMessage ->
		// tryRoomUpdated), never via this dedicated body. See
		// systemmessage.go's own top-of-file doc comment for why this
		// standalone GroupUpdatedEvent shape (a bare old/new *Group pair,
		// with no rename-vs-topic split) cannot substitute for that path,
		// and why megabridge's attempt to use it instead is a known,
		// unverified-against-live-traffic defect this bridge does not repeat.
		log.Debug().Msg("googlechat: unhandled GroupUpdated event (handled via SYSTEM_MESSAGE instead, see systemmessage.go)")
	case *pb.Event_EventBody_MessagePosted:
		return c.handleMessagePosted(ctx, evt)
	case *pb.Event_EventBody_WebPushNotification:
		log.Debug().Msg("googlechat: unhandled WebPushNotification event (M2+)")
	case *pb.Event_EventBody_MembershipChanged:
		// Intentionally unhandled, matching Python: membership changes
		// (join/invite/leave/kick, the 9 MembershipChangedMetadata types)
		// reach this bridge as a SYSTEM_MESSAGE with a MEMBERSHIP_CHANGED
		// annotation on the MessagePosted body instead (handleMessagePosted
		// -> systemmessage.go's trySystemMessage -> queueMembershipChanged),
		// never via this dedicated body. See systemmessage.go's own
		// top-of-file doc comment for why this standalone
		// MembershipChangedEvent shape (a single member's new/prior state,
		// no affected-members list or 9-value Type at all) cannot
		// substitute for that path.
		log.Debug().Msg("googlechat: unhandled MembershipChanged event (handled via SYSTEM_MESSAGE instead, see systemmessage.go)")
	case *pb.Event_EventBody_MessageDeleted:
		return c.queueMessageDeleted(ctx, evt)
	case *pb.Event_EventBody_MessageReaction:
		return c.queueMessageReaction(ctx, evt)
	case *pb.Event_EventBody_UserStatusUpdated:
		log.Debug().Msg("googlechat: unhandled UserStatusUpdated event (M2+)")
	case *pb.Event_EventBody_TypingStateChanged:
		return c.queueTypingStateChanged(ctx, evt)
	case *pb.Event_EventBody_ReadReceiptChanged:
		return c.queueReadReceiptChanged(ctx, evt)
	default:
		log.Debug().Msg("googlechat: unhandled event with no/unknown body type (M2+)")
	}
	return bridgev2.EventHandlingResultIgnored
}

// handleMessagePosted handles the MessagePosted event body (EventBody field
// 6, "message_posted"). That one body shape is shared between brand new
// messages (Event.type == MESSAGE_POSTED, plus every other non-edit type
// that can carry this body -- see below) and edits of an existing message
// (Event.type == MESSAGE_UPDATED); the body itself never says which, only
// the OUTER Event.type does. This mirrors portal.py's handle_event exactly
// (portal.py:539-543):
//
//	if evt.body.HasField("message_posted"):
//	    if evt.type == googlechat.Event.MESSAGE_UPDATED:
//	        await self.handle_googlechat_edit(...)
//	    else:
//	        await self.handle_googlechat_message(...)
//
// Python's else is unconditional: ANY Event.type other than the literal
// MESSAGE_UPDATED, with a message_posted body, is treated as a new message
// -- not just the literal MESSAGE_POSTED value. docs/research/02's wire
// protocol notes (§ "event dispatch", rows 21-23) record this as real,
// observed behavior: ON_HOLD_MESSAGE_POSTED/_UPDATED/_PUBLISHED (Workspace
// DLP-held messages) carry this same message_posted body and Python bridges
// them as ordinary messages too, since none of them equal MESSAGE_UPDATED.
// An earlier revision of this function used a three-way switch keyed on the
// literal MESSAGE_POSTED value, which silently dropped every ON_HOLD_*
// event (and any other future type Google adds to this same body arm) --
// message loss relative to Python, caught by the M2 Task 4 gchat-port-auditor
// pass. Matching Python's literal if/else here (instead of enumerating every
// type that isn't MESSAGE_UPDATED) is what keeps this correct as Google adds
// new event types to the enum.
//
// M2 Task 4 built a RemoteMessage for the new-message case (queueMessagePosted,
// below). M4 Task 1 adds the MESSAGE_UPDATED (RemoteEdit) arm
// (queueMessageEdit, below) -- see queueMessageEdit's own doc comment for the
// edit-specific extraction/dedup this shares with, and differs from,
// queueMessagePosted. M4 Task 6 adds a third possibility for the non-edit
// path: trySystemMessage (systemmessage.go) recognizes a SYSTEM_MESSAGE
// (membership change or room rename/topic change) on this same body and
// queues a ChatInfoChange instead, exactly mirroring
// handle_googlechat_message's own ordering (portal.py:1362 checks
// evt.message_type == SYSTEM_MESSAGE only in the non-edit branch;
// handle_googlechat_edit has no equivalent check at all, since Google Chat
// never lets a SYSTEM_MESSAGE be edited) -- trySystemMessage's handled=false
// return (an unrecognized/no-op annotation, or not a SYSTEM_MESSAGE at all)
// falls through to queueMessagePosted exactly like Python's own fallthrough
// to ordinary message bridging. Returns the handling result so
// handleGChatEvent can gate the watermark advance on it.
func (c *GChatClient) handleMessagePosted(ctx context.Context, evt *pb.Event) bridgev2.EventHandlingResult {
	if evt.GetType() == pb.Event_MESSAGE_UPDATED {
		return c.queueMessageEdit(ctx, evt)
	}
	if res, handled := c.trySystemMessage(ctx, evt); handled {
		return res
	}
	return c.queueMessagePosted(ctx, evt)
}

// extractPostedMessage pulls the (msg, group, gcMessageID) triple every
// MessagePosted-bodied event needs, shared between queueMessagePosted (new
// messages) and queueMessageEdit (M4 Task 1, MESSAGE_UPDATED) -- both read
// off the exact same evt.GetBody().GetMessagePosted().GetMessage() /
// evt.GetGroupId() shape (see handleMessagePosted's doc comment on why one
// body arm covers both). ok is false (already logged, matching how
// sync.go's syncChats skips a world item with no usable group id) for any of
// three malformed-payload cases: no message payload, no message id, or a
// group id neither GroupIDToParts oneof arm sets.
func (c *GChatClient) extractPostedMessage(ctx context.Context, evt *pb.Event) (msg *pb.Message, group gcid.GroupID, gcMessageID string, ok bool) {
	log := zerolog.Ctx(ctx)
	msg = evt.GetBody().GetMessagePosted().GetMessage()
	if msg == nil {
		log.Warn().Msg("googlechat: MessagePosted event with no message payload, skipping")
		return nil, gcid.GroupID{}, "", false
	}
	gcMessageID = msg.GetId().GetMessageId()
	if gcMessageID == "" {
		log.Warn().Msg("googlechat: MessagePosted event with no message id, skipping")
		return nil, gcid.GroupID{}, "", false
	}
	id, isDM, groupOK := gchatmeow.GroupIDToParts(evt.GetGroupId())
	if !groupOK {
		log.Warn().Str("gc_message_id", gcMessageID).
			Msg("googlechat: MessagePosted event with no usable group id, skipping")
		return nil, gcid.GroupID{}, "", false
	}
	return msg, gcid.GroupID{ID: id, IsDM: isDM}, gcMessageID, true
}

// queueMessagePosted extracts the sender, group, and timestamp from evt's
// MessagePosted body and queues a bridgev2.RemoteMessage for it, porting
// handle_googlechat_message's extraction (portal.py:1337-1338,1360):
//
//   - sender: evt.creator.user_id.id (the message's own creator gaia id,
//     NOT the outer Event.user_id -- that field is the event's target/actor
//     for other event kinds, e.g. group_viewed's viewer, and is left unused
//     here);
//   - group: the flattened Event's own group_id (already copied from the
//     parent frame by gchatmeow's splitEventBodies -- see this file's
//     top-of-file doc comment), NOT anything on Message/MessageId;
//   - timestamp: evt.create_time, Google Chat's microsecond epoch time --
//     Python divides by 1000 once, at matrix_ts computation
//     (portal.py:1360), to get Matrix's millisecond convention; here the
//     conversion lives in gchatmeow.MicrosToTime (EventMeta.Timestamp) and,
//     separately, the raw microsecond value is preserved verbatim in
//     MessageMetadata.TimestampMicro (msgconv_adapter.go) for M3's
//     quote-reply support, which needs the original Google Chat unit back,
//     not Matrix's derived millisecond one.
//
// A missing/malformed payload is logged and dropped by extractPostedMessage
// rather than queuing a broken RemoteMessage; these skips report Ignored
// (Success): there is nothing deliverable, so the watermark should advance
// past the garbage rather than re-fetch it on every reconnect. Only the
// actual queue's result (which may be a genuine Failed) is returned as-is,
// so a real delivery failure blocks the watermark advance (handleGChatEvent).
func (c *GChatClient) queueMessagePosted(ctx context.Context, evt *pb.Event) bridgev2.EventHandlingResult {
	log := zerolog.Ctx(ctx)
	msg, group, gcMessageID, ok := c.extractPostedMessage(ctx, evt)
	if !ok {
		return bridgev2.EventHandlingResultIgnored
	}
	senderUserID := gcid.MakeUserID(msg.GetCreator().GetUserId().GetId())

	res := c.queueRemoteEvent(&simplevent.Message[*pb.Message]{
		EventMeta: simplevent.EventMeta{
			Type:         bridgev2.RemoteEventMessage,
			PortalKey:    gcid.MakePortalKey(group, c.UserLogin.ID),
			CreatePortal: true,
			Sender: bridgev2.EventSender{
				Sender:   senderUserID,
				IsFromMe: c.IsThisUser(ctx, senderUserID),
			},
			Timestamp: gchatmeow.MicrosToTime(msg.GetCreateTime()),
		},
		ID: gcid.MakeMessageID(gcMessageID),
		// TransactionID is Task 6's own-echo dedup half: msg.GetLocalId()
		// (Message.LocalId, proto field 14) carries back whatever local_id
		// the ORIGINAL send (if any) put on the wire in CreateTopicRequest
		// (handlematrix.go). Empty for any message this login didn't just
		// send itself (proto default when the field is absent -- another
		// user's message, or the same account's own message from a
		// different, non-bridged client/device), which is exactly what
		// bridgev2.Portal.checkPendingMessage (portal.go) needs: it treats
		// an empty transaction id as "not a pending echo" and bridges the
		// message normally, matching portal.py:1341's
		// `if evt.local_id in self._local_dedup` -- an empty/foreign
		// local_id is simply absent from (or never equal to a key in) that
		// set, so Python doesn't drop it either.
		TransactionID:      networkid.TransactionID(msg.GetLocalId()),
		Data:               msg,
		ConvertMessageFunc: convertMessageToMatrix(c.msgConverter(), c),
	})
	log.Debug().
		Str("gc_message_id", gcMessageID).
		Str("gc_group_id", group.ID).
		Bool("is_dm", group.IsDM).
		Any("result", res).
		Msg("googlechat: queued inbound message")
	return res
}

// queueMessageEdit extracts the same (msg, group, gcMessageID) triple
// queueMessagePosted does (via extractPostedMessage) and queues a
// bridgev2.RemoteEdit for it, porting handle_googlechat_edit's own
// extraction (portal.py:1228-1236):
//
//   - sender: evt.creator.user_id.id -- SAME field/derivation as
//     queueMessagePosted (an edit's creator is always the original
//     message's own author; Google Chat itself only allows the original
//     author to edit their own message, so this is never a different user
//     in practice, but nothing here assumes that -- portal.handleRemoteEdit,
//     mautrix-go bridgev2/portal.go:3151-3158, independently verifies the
//     resolved intent's MXID matches the stored original sender before
//     bridging the edit at all, and drops it otherwise).
//   - ID/TargetMessage: BOTH gcid.MakeMessageID(gcMessageID) -- a Google
//     Chat edit reuses the SAME message id as the original (msg_id,
//     portal.py:1232), never a new one, so this event's own ID and the
//     message it targets are identical. TargetMessage is what bridgev2's
//     handleRemoteEdit (mautrix-go bridgev2/portal.go:3121-3139) uses to
//     fetch the target's existing DB rows (portal.py's own
//     `DBMessage.get_by_gcid(msg_id, ..., index=0)`, portal.py:1244).
//   - Timestamp: gchatmeow.MicrosToTime(editTS), the EDIT's own time (NOT
//     msg.create_time, which is the ORIGINAL message's unchanged creation
//     time) -- matching Python's own `edit_ts = evt.last_edit_time or
//     evt.last_update_time` (portal.py:1236) feeding
//     `timestamp=edit_ts // 1000` at the eventual _send_message call
//     (portal.py:1257). The SAME editTS also drives convertEditToMatrix's
//     dedup gate (msgconv_adapter.go) via msg itself (Data, below) --
//     computed once here and left for ConvertEdit to re-derive from msg
//     rather than threaded through as a separate field, since
//     ConvertEditFunc's signature (simplevent.Message[T]) has no room for
//     extra per-call data beyond T.
//
// Unlike queueMessagePosted, CreatePortal is left false (the zero value): an
// edit target that doesn't already have a bridged portal has nothing to
// attach the edit to (mirrors mautrix-meta's FBEditEvent, which likewise
// does not implement RemoteEventThatMayCreatePortal).
func (c *GChatClient) queueMessageEdit(ctx context.Context, evt *pb.Event) bridgev2.EventHandlingResult {
	log := zerolog.Ctx(ctx)
	msg, group, gcMessageID, ok := c.extractPostedMessage(ctx, evt)
	if !ok {
		return bridgev2.EventHandlingResultIgnored
	}
	senderUserID := gcid.MakeUserID(msg.GetCreator().GetUserId().GetId())
	editTS := msg.GetLastEditTime()
	if editTS == 0 {
		editTS = msg.GetLastUpdateTime()
	}

	res := c.queueRemoteEvent(&simplevent.Message[*pb.Message]{
		EventMeta: simplevent.EventMeta{
			Type:      bridgev2.RemoteEventEdit,
			PortalKey: gcid.MakePortalKey(group, c.UserLogin.ID),
			Sender: bridgev2.EventSender{
				Sender:   senderUserID,
				IsFromMe: c.IsThisUser(ctx, senderUserID),
			},
			Timestamp: gchatmeow.MicrosToTime(editTS),
		},
		ID:              gcid.MakeMessageID(gcMessageID),
		TargetMessage:   gcid.MakeMessageID(gcMessageID),
		Data:            msg,
		ConvertEditFunc: convertEditToMatrix(c.msgConverter()),
	})
	log.Debug().
		Str("gc_message_id", gcMessageID).
		Str("gc_group_id", group.ID).
		Bool("is_dm", group.IsDM).
		Any("result", res).
		Msg("googlechat: queued inbound edit")
	return res
}

// queueMessageDeleted handles the MessageDeleted event body (EventBody
// field 18, "message_deleted") and queues a bridgev2.RemoteMessageRemove for
// it, porting handle_googlechat_redaction's own extraction
// (portal.py:1210-1226):
//
//   - target message id: evt.message_id.message_id -- the SAME field
//     Python reads (portal.py:1214's
//     `DBMessage.get_all_by_gcid(evt.message_id.message_id, ...)`). Unlike
//     the message_posted body, MessageDeletedEvent carries no separate
//     Message payload to pull a group id or sender from; group comes from
//     the outer Event's own group_id, exactly like every other body arm
//     (this file's top-of-file doc comment) -- MessageDeletedEvent itself
//     has no group id field at all.
//   - no sender: Python's own redaction always uses self.main_intent
//     (the bridge bot), never a per-user ghost intent (portal.py:1222), so
//     there is no per-event sender to resolve here either -- EventMeta.Sender
//     is left at its zero value, matching mautrix-meta's wrapMessageDelete
//     (pkg/connector/handlemeta.go), which does the same.
//   - timestamp: evt.timestamp (MessageDeletedEvent field 2), Google Chat's
//     microsecond epoch time -- Python divides by 1000 once
//     (`timestamp=evt.timestamp // 1000`, portal.py:1223) to reach Matrix's
//     millisecond convention; here that conversion lives in
//     gchatmeow.MicrosToTime (EventMeta.Timestamp), same as every other
//     inbound event in this file.
//
// Unlike queueMessagePosted, CreatePortal is left false (the zero value): a
// deletion target with no existing portal has nothing to redact (mirrors
// queueMessageEdit's identical reasoning, and mautrix-meta's
// wrapMessageDelete, which likewise never sets CreatePortal).
//
// The framework (bridgev2's handleRemoteMessageRemove,
// mautrix-go bridgev2/portal.go) is responsible for looking up the target's
// existing DB rows by TargetMessage and redacting every Matrix part found
// -- Python's own `target = await DBMessage.get_all_by_gcid(...)` +
// `for msg in target: ... self.main_intent.redact(...)` loop
// (portal.py:1213-1226) is exactly this framework behavior, so this
// function only needs to supply the target id, not enumerate rows itself.
// A target with no matching rows (Python's `if not target: ... return`,
// portal.py:1216-1218 -- e.g. an echo of a deletion this same bridge just
// issued via HandleMatrixMessageRemove, whose DB row bridgev2 already
// deleted after a successful RPC) is likewise a framework-level no-op, not
// something this function needs to check for itself.
func (c *GChatClient) queueMessageDeleted(ctx context.Context, evt *pb.Event) bridgev2.EventHandlingResult {
	log := zerolog.Ctx(ctx)
	deleted := evt.GetBody().GetMessageDeleted()
	gcMessageID := deleted.GetMessageId().GetMessageId()
	if gcMessageID == "" {
		log.Warn().Msg("googlechat: MessageDeleted event with no message id, skipping")
		return bridgev2.EventHandlingResultIgnored
	}
	id, isDM, groupOK := gchatmeow.GroupIDToParts(evt.GetGroupId())
	if !groupOK {
		log.Warn().Str("gc_message_id", gcMessageID).
			Msg("googlechat: MessageDeleted event with no usable group id, skipping")
		return bridgev2.EventHandlingResultIgnored
	}
	group := gcid.GroupID{ID: id, IsDM: isDM}

	res := c.queueRemoteEvent(&simplevent.MessageRemove{
		EventMeta: simplevent.EventMeta{
			Type:      bridgev2.RemoteEventMessageRemove,
			PortalKey: gcid.MakePortalKey(group, c.UserLogin.ID),
			Timestamp: gchatmeow.MicrosToTime(deleted.GetTimestamp()),
		},
		TargetMessage: gcid.MakeMessageID(gcMessageID),
	})
	log.Debug().
		Str("gc_message_id", gcMessageID).
		Str("gc_group_id", group.ID).
		Bool("is_dm", group.IsDM).
		Any("result", res).
		Msg("googlechat: queued inbound deletion")
	return res
}

// queueMessageReaction handles the MessageReaction event body (EventBody
// field 22, "message_reaction") and queues a bridgev2.RemoteReaction or
// bridgev2.RemoteReactionRemove (both are the same simplevent.Reaction type,
// see mautrix-go bridgev2/simplevent/reaction.go's own doc comment) for it,
// porting handle_googlechat_reaction's own extraction (portal.py:1166-1208):
//
//   - target message id: evt.message_id.message_id -- the SAME field
//     Python reads (portal.py:1170-1172's
//     `DBMessage.get_by_gcid(evt.message_id.message_id, self.gcid, self.gc_receiver)`).
//     Like MessageDeletedEvent, MessageReactionEvent carries no separate
//     Message payload to pull a group id from; group comes from the outer
//     Event's own group_id, same as every other body arm (this file's
//     top-of-file doc comment).
//   - sender: evt.user_id.id -- Python's `evt.user_id.id`
//     (portal.py:1169's `p.Puppet.get_by_gcid(evt.user_id.id)`), the
//     REACTOR's gaia id, distinct from MessagePosted's evt.creator (this
//     body arm has no creator field at all, only user_id).
//   - emoji: evt.emoji.unicode, normalized both ways (see handlereaction.go's
//     top-of-file doc comment on variation selectors): EmojiID keeps the
//     bare form GC's own wire protocol already uses (variationselector.Remove
//     is applied defensively in case a future server response ever includes
//     one; GC's own evt.emoji.unicode is not documented to), matching
//     PreHandleMatrixReaction's identical bare-form EmojiID so a
//     Matrix-initiated reaction and its own inbound echo key identically;
//     Emoji gets the selector added back (variationselector.Add) for the
//     value handed toward Matrix, mirroring portal.py:1183's
//     `matrix_reaction = variation_selector.add(evt.emoji.unicode)`.
//   - add vs remove: evt.type selects the RemoteEventType --
//     MessageReactionEvent.ADD -> RemoteEventReaction (portal.py:1179's
//     `if evt.type == googlechat.MessageReactionEvent.ADD:`),
//     MessageReactionEvent.REMOVE -> RemoteEventReactionRemove
//     (portal.py:1197's `elif evt.type == ... REMOVE:`). Any other value
//     (portal.py:1207-1208's `else: self.log.debug(f"Unknown reaction event
//     type {evt.type}")`) is logged and ignored rather than queued -- GC's
//     own proto2 default for an absent type field IS ADD (matching
//     Python's implicit behavior for a field GC always sets on the wire in
//     practice), so this branch only ever fires for a genuinely unexpected
//     future enum value.
//   - timestamp: evt.timestamp (MessageReactionEvent field 4), Google
//     Chat's microsecond epoch time -- Python divides by 1000 once
//     (`timestamp=evt.timestamp // 1000`, portal.py:1185) to reach Matrix's
//     millisecond convention; here that conversion lives in
//     gchatmeow.MicrosToTime (EventMeta.Timestamp), same as every other
//     inbound event in this file.
//
// The uniqueness key bridgev2 dedups/removes reactions by is (message, part,
// sender, EmojiID) -- portal.getTargetReaction (mautrix-go bridgev2/portal.go:3293-3299)
// and portal.handleRemoteReaction's own duplicate check
// (portal.go:3464-3471) both key off exactly this tuple, matching Python's
// own (emoji, sender, message) DBReaction row identity (portal.py:1176-1178).
// Part is left implicit (simplevent.Reaction carries no PartID -- Google
// Chat messages are always single-part, gcid.TextPartID) exactly like
// queueMessageDeleted's TargetMessage above.
//
// Unlike queueMessagePosted, CreatePortal is left false (the zero value): a
// reaction to a message in a portal that doesn't exist yet has nothing to
// attach to, mirroring queueMessageEdit/queueMessageDeleted's identical
// reasoning.
func (c *GChatClient) queueMessageReaction(ctx context.Context, evt *pb.Event) bridgev2.EventHandlingResult {
	log := zerolog.Ctx(ctx)
	reaction := evt.GetBody().GetMessageReaction()
	gcMessageID := reaction.GetMessageId().GetMessageId()
	if gcMessageID == "" {
		log.Warn().Msg("googlechat: MessageReaction event with no message id, skipping")
		return bridgev2.EventHandlingResultIgnored
	}
	id, isDM, groupOK := gchatmeow.GroupIDToParts(evt.GetGroupId())
	if !groupOK {
		log.Warn().Str("gc_message_id", gcMessageID).
			Msg("googlechat: MessageReaction event with no usable group id, skipping")
		return bridgev2.EventHandlingResultIgnored
	}
	group := gcid.GroupID{ID: id, IsDM: isDM}

	var eventType bridgev2.RemoteEventType
	switch reaction.GetType() {
	case pb.MessageReactionEvent_ADD:
		eventType = bridgev2.RemoteEventReaction
	case pb.MessageReactionEvent_REMOVE:
		eventType = bridgev2.RemoteEventReactionRemove
	default:
		log.Debug().
			Str("gc_message_id", gcMessageID).
			Int("gc_reaction_type", int(reaction.GetType())).
			Msg("googlechat: unknown MessageReaction event type, skipping")
		return bridgev2.EventHandlingResultIgnored
	}

	senderUserID := gcid.MakeUserID(reaction.GetUserId().GetId())
	bareEmoji := variationselector.Remove(reaction.GetEmoji().GetUnicode())

	res := c.queueRemoteEvent(&simplevent.Reaction{
		EventMeta: simplevent.EventMeta{
			Type:      eventType,
			PortalKey: gcid.MakePortalKey(group, c.UserLogin.ID),
			Sender: bridgev2.EventSender{
				Sender:   senderUserID,
				IsFromMe: c.IsThisUser(ctx, senderUserID),
			},
			Timestamp: gchatmeow.MicrosToTime(reaction.GetTimestamp()),
		},
		TargetMessage: gcid.MakeMessageID(gcMessageID),
		EmojiID:       networkid.EmojiID(bareEmoji),
		Emoji:         variationselector.Add(bareEmoji),
	})
	log.Debug().
		Str("gc_message_id", gcMessageID).
		Str("gc_group_id", group.ID).
		Bool("is_dm", group.IsDM).
		Str("gc_reaction_type", reaction.GetType().String()).
		Any("result", res).
		Msg("googlechat: queued inbound reaction")
	return res
}

// queueReadReceiptChanged handles the ReadReceiptChanged event body
// (EventBody field 33, "read_receipt_changed") and queues one
// bridgev2.RemoteReadReceipt (simplevent.Receipt) per entry in the event's
// ReadReceiptSet, porting handle_googlechat_read_receipts's own extraction
// (portal.py:1587-1592):
//
//	async def handle_googlechat_read_receipts(self, evt) -> None:
//	    for rr in evt.read_receipt_set.read_receipts:
//	        await self.mark_read(rr.user.user_id.id, rr.read_time_micros)
//
// Python's own Portal.mark_read (portal.py:1594-1598) resolves the closest
// message at-or-before rr.read_time_micros (DBMessage.get_closest_before)
// and marks THAT message read via the reading user's own ghost/double-puppet
// intent. bridgev2's framework does the equivalent lookup itself once this
// event reaches it (handleRemoteReadReceipt, mautrix-go
// bridgev2/portal.go:3690-3769, via GetLastNonFakePartAtOrBeforeTime -- see
// docs/research/07-gap-analysis.md row 68), so this function only needs to
// supply ReadUpTo (rr.read_time_micros converted to a time.Time) and the
// reading user's own Sender -- not look up a message itself.
//
//   - group: the outer Event's own group_id (this file's top-of-file doc
//     comment) -- ReadReceiptChangedEvent DOES carry its own group_id field
//     (proto field 1), but Python's on_stream_event (user.py:674-682) only
//     ever overrides group_id from the body for TYPING_STATE_CHANGED; every
//     other type, including this one, routes on the outer Event.group_id,
//     which gchatmeow's splitEventBodies has already copied onto this
//     per-body Event -- so this function reads evt.GetGroupId(), not the
//     body's own field, for consistency with queueMessageDeleted/
//     queueMessageReaction above (neither of which has a body-level group
//     id at all to be tempted by).
//   - sender: rr.user.user_id.id -- the READER's own gaia id, which may be
//     any member of the conversation, including this login's own account
//     (Google Chat can announce your own read state back to you via this
//     same event on some clients); IsThisUser decides IsFromMe exactly like
//     queueMessagePosted/queueMessageReaction above, so a self-read receipt
//     correctly routes through double puppeting rather than a ghost intent,
//     matching Python's puppet.intent_for(self) (portal.py:1598), which
//     does the identical double-puppet-if-self resolution.
//   - ReadUpTo: rr.read_time_micros converted via gchatmeow.MicrosToTime --
//     Google Chat's own microsecond epoch time, same unit conversion this
//     file already applies to every other inbound event's timestamp.
//
// One bridgev2.RemoteReadReceipt is queued PER read_receipts entry (Python's
// own for loop does the same, one mark_read call per rr) since
// simplevent.Receipt carries a single Sender -- a ReadReceiptSet announcing
// several users' read states at once needs one event per user. If any entry
// fails to queue, the loop stops there and returns that failure (rather than
// continuing and silently losing track of it): handleGChatEvent only
// advances the watermark on a fully-Success result, so a partial failure
// here re-delivers this whole event -- including entries already
// successfully queued -- on the next reconnect's catch-up replay, which is
// safe because read receipts are idempotent (re-marking an already-read
// message read is a no-op).
func (c *GChatClient) queueReadReceiptChanged(ctx context.Context, evt *pb.Event) bridgev2.EventHandlingResult {
	log := zerolog.Ctx(ctx)
	id, isDM, groupOK := gchatmeow.GroupIDToParts(evt.GetGroupId())
	if !groupOK {
		log.Warn().Msg("googlechat: ReadReceiptChanged event with no usable group id, skipping")
		return bridgev2.EventHandlingResultIgnored
	}
	group := gcid.GroupID{ID: id, IsDM: isDM}

	receipts := evt.GetBody().GetReadReceiptChanged().GetReadReceiptSet().GetReadReceipts()
	if len(receipts) == 0 {
		log.Debug().Msg("googlechat: ReadReceiptChanged event with no read receipts, skipping")
		return bridgev2.EventHandlingResultIgnored
	}

	res := bridgev2.EventHandlingResultIgnored
	for _, rr := range receipts {
		gaiaID := rr.GetUser().GetUserId().GetId()
		if gaiaID == "" {
			// An rr entry with no user id would otherwise resolve to
			// EventSender{Sender: ""}, which bridgev2 treats as "no sender"
			// and falls back to the bridge bot intent -- silently marking
			// the room read as the BOT rather than skipping the entry, as
			// queueMembershipChanged's identical empty-gaia-id guard
			// (systemmessage.go) already does for MEMBERSHIP_CHANGED
			// members. Skip it instead of queuing a bogus bot receipt.
			log.Warn().Msg("googlechat: ReadReceiptChanged entry with no user id, skipping")
			continue
		}
		senderUserID := gcid.MakeUserID(gaiaID)
		res = c.queueRemoteEvent(&simplevent.Receipt{
			EventMeta: simplevent.EventMeta{
				Type:      bridgev2.RemoteEventReadReceipt,
				PortalKey: gcid.MakePortalKey(group, c.UserLogin.ID),
				Sender: bridgev2.EventSender{
					Sender:   senderUserID,
					IsFromMe: c.IsThisUser(ctx, senderUserID),
				},
				Timestamp: gchatmeow.MicrosToTime(rr.GetReadTimeMicros()),
			},
			ReadUpTo: gchatmeow.MicrosToTime(rr.GetReadTimeMicros()),
		})
		if !res.Success {
			break
		}
	}
	log.Debug().
		Str("gc_group_id", group.ID).
		Bool("is_dm", group.IsDM).
		Int("receipt_count", len(receipts)).
		Any("result", res).
		Msg("googlechat: queued inbound read receipt(s)")
	return res
}

// queueGroupViewed handles the GroupViewed event body (EventBody field 3,
// "group_viewed") and queues a bridgev2.RemoteReadReceipt (simplevent.Receipt)
// for the login's OWN user, porting portal.py:556-557's
//
//	elif evt.body.HasField("group_viewed"):
//	    await self.mark_read(source.gcid, evt.body.group_viewed.view_time)
//
// where `self` is the Portal and `self.mark_read` is Portal.mark_read
// (portal.py:1594-1598, the SAME helper handle_googlechat_read_receipts uses
// above) -- group_viewed represents the LOGIN'S OWN account viewing this
// conversation from some OTHER Google Chat client (a phone, a different
// browser tab, chat.google.com directly, ...), reported back over the same
// event stream this bridge is subscribed to; `source` at portal.py:556-557
// is the User whose stream this event arrived on, i.e. always this login's
// own account -- there is no other user_id anywhere on GroupViewedEvent to
// read a different sender from. This is Google Chat's own-read-marker sync,
// distinct from read_receipt_changed's per-OTHER-user receipts above (though
// as queueReadReceiptChanged's own doc comment notes, that event CAN also
// carry this login's own gaia id on some clients -- group_viewed is the
// SEPARATE, GC-client-driven channel for the same fact).
//
// EventSender{IsFromMe: true} tells bridgev2 to resolve the intent via this
// login's own double puppet (mautrix-go bridgev2/networkinterface.go's
// EventSender.IsFromMe doc comment: "the UserLogin who the event was
// received through is used as the sender... Double puppeting will be used
// if available"), exactly matching Python's puppet.intent_for(self)
// resolving to the double-puppet intent for the portal's own receiving user
// (portal.py:1596-1598) -- see docs/research/07-gap-analysis.md row 77,
// "Own read marker (group_viewed) | simplevent.Receipt with
// EventSender{IsFromMe: true}".
//
//   - group: evt.GetGroupId() (the outer Event's own group_id), same
//     reasoning as queueReadReceiptChanged above -- GroupViewedEvent also
//     carries its own group_id field (proto field 1) that Python's
//     on_stream_event never reads for this event type either.
//   - ReadUpTo: evt.body.group_viewed.view_time converted via
//     gchatmeow.MicrosToTime, exactly like read_receipt_changed's own
//     read_time_micros above.
func (c *GChatClient) queueGroupViewed(ctx context.Context, evt *pb.Event) bridgev2.EventHandlingResult {
	log := zerolog.Ctx(ctx)
	id, isDM, groupOK := gchatmeow.GroupIDToParts(evt.GetGroupId())
	if !groupOK {
		log.Warn().Msg("googlechat: GroupViewed event with no usable group id, skipping")
		return bridgev2.EventHandlingResultIgnored
	}
	group := gcid.GroupID{ID: id, IsDM: isDM}
	viewTime := evt.GetBody().GetGroupViewed().GetViewTime()

	res := c.queueRemoteEvent(&simplevent.Receipt{
		EventMeta: simplevent.EventMeta{
			Type:      bridgev2.RemoteEventReadReceipt,
			PortalKey: gcid.MakePortalKey(group, c.UserLogin.ID),
			Sender: bridgev2.EventSender{
				Sender:   gcid.MakeUserID(string(c.UserLogin.ID)),
				IsFromMe: true,
			},
			Timestamp: gchatmeow.MicrosToTime(viewTime),
		},
		ReadUpTo: gchatmeow.MicrosToTime(viewTime),
	})
	log.Debug().
		Str("gc_group_id", group.ID).
		Bool("is_dm", group.IsDM).
		Any("result", res).
		Msg("googlechat: queued own-user read marker (group_viewed)")
	return res
}

// typingContextGroupID extracts the *pb.GroupId a TypingStateChangedEvent's
// own TypingContext oneof carries -- its direct group_id arm, or (when
// scoped to a thread) topic_id.group_id, since TopicId embeds the group id
// alongside the topic id (see handletyping.go's typingContext, which builds
// the exact same shape outbound). Mirrors typingContextGroupID's own
// reasoning in reverse: whichever arm is set, the group id inside it is what
// queueTypingStateChanged (below) needs to resolve the portal.
func typingContextGroupID(tc *pb.TypingContext) *pb.GroupId {
	if tc == nil {
		return nil
	}
	if gid := tc.GetGroupId(); gid != nil {
		return gid
	}
	return tc.GetTopicId().GetGroupId()
}

// queueTypingStateChanged handles the TypingStateChanged event body
// (EventBody field 26, "typing_state_changed") and queues a
// bridgev2.RemoteTyping (simplevent.Typing) for it, porting
// handle_googlechat_typing's own extraction (portal.py:1600-1610):
//
//	async def handle_googlechat_typing(self, source: u.User, sender: str, status: int) -> None:
//	    if not self.mxid:
//	        return
//	    puppet = await p.Puppet.get_by_gcid(sender)
//	    ...
//	    await puppet.intent_for(self).set_typing(
//	        self.mxid, timeout=6000 if status == googlechat.TYPING else 0
//	    )
//
// *** THE ROUTING TRAP ***: unlike EVERY OTHER body arm in this file, group
// comes from the BODY's own TypingContext
// (evt.GetBody().GetTypingStateChanged().GetContext()), NOT the outer
// Event's own group_id -- user.py:674-682's on_stream_event:
//
//	async def on_stream_event(self, evt: googlechat.Event) -> None:
//	    group_id = evt.group_id
//	    if evt.type == googlechat.Event.TYPING_STATE_CHANGED:
//	        group_id = evt.body.typing_state_changed.context.group_id
//	    portal = await po.Portal.get_by_group_id(group_id, self.gcid)
//
// TYPING_STATE_CHANGED is the ONLY event type Python overrides group_id for;
// every other body arm in this file (queueMessageDeleted, queueMessageReaction,
// queueReadReceiptChanged, queueGroupViewed -- see each of their own doc
// comments) correctly reads evt.GetGroupId(), the outer Event's own field,
// and must keep doing so. gchatmeow's splitEventBodies (client.go) copies the
// PARENT frame's own group_id onto every flattened per-body Event uniformly,
// with no type-specific exception -- so for a genuine typing_state_changed
// event, evt.GetGroupId() is observed EMPTY on the wire. This is the exact
// defect docs/research/08b records megabridge as having: typing routed to an
// empty/garbage portal key because it (wrongly) reads the outer group_id like
// every other event type instead of the body's own context. This function
// deliberately does NOT call gchatmeow.GroupIDToParts(evt.GetGroupId()) for
// exactly that reason -- typingContextGroupID (above) extracts the id from
// the body's own TypingContext oneof instead, and TestHandleGChatEventTyping
// StateChangedRoutesViaBodyContextNotOuterGroupID (handletyping_test.go)
// pins this by asserting the resolved portal key tracks the BODY's group id
// even when the outer Event.group_id is empty or set to a different value.
//
//   - sender: evt.body.typing_state_changed.user_id.id -- the TYPIST's own
//     gaia id (portal.py:553's `evt.body.typing_state_changed.user_id.id`,
//     passed as handle_googlechat_typing's `sender` parameter). IsFromMe
//     decides the double-puppet-vs-ghost intent exactly like every other
//     inbound event in this file (queueMessagePosted, queueMessageReaction,
//     queueReadReceiptChanged); Python's own self-typing case resolves the
//     SAME puppet.intent_for(self) (portal.py:1608). KNOWN GAP (gchat-port-
//     auditor, M4 Task 5): Python additionally gates that self-typing case
//     on an explicit direct-chat room-membership check (portal.py:1604-1607,
//     `if self.is_direct and puppet.gcid == source.gcid: ... if membership
//     != Membership.JOIN: return`) that this function does NOT replicate --
//     bridgev2's own intent resolution (mautrix-go bridgev2/portal.go's
//     getIntentAndUserMXIDFor/ensureFunctionalMember) does not perform an
//     equivalent room-membership check before typing is sent. Reachable only
//     when the room is a DM, the typist is this login's own gaia (Google
//     Chat echoing your own typing state back to you), AND the resolved
//     ghost/double-puppet intent has not yet joined that DM room (e.g. no
//     double puppeting configured, or a very early DM-creation race) --
//     worst case is intent.MarkTyping erroring (EventHandlingResultFailed +
//     a warning log) where Python would have silently no-op'd instead.
//   - Timeout: typingTimeout (6s, this file's own top-of-file doc comment)
//     when state == TYPING, 0 (immediately stop) for STOPPED or any other/
//     absent value -- porting Python's `timeout=6000 if status ==
//     googlechat.TYPING else 0` (portal.py:1608-1610) verbatim. Unlike
//     queueReadReceiptChanged's idempotent re-delivery, this function is
//     called for BOTH the start AND the explicit stop -- Python calls
//     handle_googlechat_typing for every typing_state_changed event
//     regardless of status, and bridgev2's own framework interprets
//     Timeout==0 as "stop typing now" (handleRemoteTyping, mautrix-go
//     bridgev2/portal.go:3827-3846), the same as Python's timeout=0 argument
//     to Intent.set_typing -- so there is no separate "stop" event kind to
//     emit, just this same RemoteEventTyping with a zero Timeout.
//   - Timestamp: evt.body.typing_state_changed.start_timestamp_usec
//     (TypingStateChangedEvent field 4) converted via gchatmeow.MicrosToTime,
//     same unit conversion this file already applies to every other inbound
//     event's timestamp. Python's handle_googlechat_typing has no equivalent
//     use of this field (it drives no behavior there), but every other
//     handler in this file sets EventMeta.Timestamp from the event's own
//     wire timestamp, so this one does too for consistency/log fidelity.
//
// Unlike queueMessagePosted, CreatePortal is left false (the zero value): a
// typing notification for a portal that doesn't exist yet has nothing to
// attach to, mirroring queueMessageReaction/queueMessageDeleted's identical
// reasoning.
func (c *GChatClient) queueTypingStateChanged(ctx context.Context, evt *pb.Event) bridgev2.EventHandlingResult {
	log := zerolog.Ctx(ctx)
	typing := evt.GetBody().GetTypingStateChanged()
	id, isDM, groupOK := gchatmeow.GroupIDToParts(typingContextGroupID(typing.GetContext()))
	if !groupOK {
		log.Warn().Msg("googlechat: TypingStateChanged event with no usable group id, skipping")
		return bridgev2.EventHandlingResultIgnored
	}
	group := gcid.GroupID{ID: id, IsDM: isDM}

	senderUserID := gcid.MakeUserID(typing.GetUserId().GetId())
	var timeout time.Duration
	if typing.GetState() == pb.TypingState_TYPING {
		timeout = typingTimeout
	}

	res := c.queueRemoteEvent(&simplevent.Typing{
		EventMeta: simplevent.EventMeta{
			Type:      bridgev2.RemoteEventTyping,
			PortalKey: gcid.MakePortalKey(group, c.UserLogin.ID),
			Sender: bridgev2.EventSender{
				Sender:   senderUserID,
				IsFromMe: c.IsThisUser(ctx, senderUserID),
			},
			Timestamp: gchatmeow.MicrosToTime(typing.GetStartTimestampUsec()),
		},
		Timeout: timeout,
	})
	log.Debug().
		Str("gc_group_id", group.ID).
		Bool("is_dm", group.IsDM).
		Str("gc_typing_state", typing.GetState().String()).
		Any("result", res).
		Msg("googlechat: queued inbound typing state")
	return res
}
