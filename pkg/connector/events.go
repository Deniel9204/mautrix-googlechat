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

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

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
		log.Debug().Msg("googlechat: unhandled GroupViewed event (M2+)")
	case *pb.Event_EventBody_GroupUpdated:
		log.Debug().Msg("googlechat: unhandled GroupUpdated event (M2+)")
	case *pb.Event_EventBody_MessagePosted:
		return c.handleMessagePosted(ctx, evt)
	case *pb.Event_EventBody_WebPushNotification:
		log.Debug().Msg("googlechat: unhandled WebPushNotification event (M2+)")
	case *pb.Event_EventBody_MembershipChanged:
		log.Debug().Msg("googlechat: unhandled MembershipChanged event (M2+)")
	case *pb.Event_EventBody_MessageDeleted:
		log.Debug().Msg("googlechat: unhandled MessageDeleted event (M2+)")
	case *pb.Event_EventBody_MessageReaction:
		log.Debug().Msg("googlechat: unhandled MessageReaction event (M2+)")
	case *pb.Event_EventBody_UserStatusUpdated:
		log.Debug().Msg("googlechat: unhandled UserStatusUpdated event (M2+)")
	case *pb.Event_EventBody_TypingStateChanged:
		log.Debug().Msg("googlechat: unhandled TypingStateChanged event (M2+)")
	case *pb.Event_EventBody_ReadReceiptChanged:
		log.Debug().Msg("googlechat: unhandled ReadReceiptChanged event (M2+)")
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
// queueMessagePosted. Returns the handling result so handleGChatEvent can
// gate the watermark advance on it.
func (c *GChatClient) handleMessagePosted(ctx context.Context, evt *pb.Event) bridgev2.EventHandlingResult {
	if evt.GetType() == pb.Event_MESSAGE_UPDATED {
		return c.queueMessageEdit(ctx, evt)
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
		ConvertMessageFunc: convertMessageToMatrix(c.msgConverter()),
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
