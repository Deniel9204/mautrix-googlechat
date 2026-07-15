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
// This is a type-switch skeleton over every event body type the proto
// defines. M1 left every arm as a "not yet handled" debug log; M2 Task 4
// fills in MessagePosted (new messages only -- see handleMessagePosted's
// doc comment for why edits share this same body arm but are routed
// elsewhere). Later M2+ tasks fill in the rest.
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

func (c *GChatClient) handleGChatEvent(ctx context.Context, evt *pb.Event) {
	log := zerolog.Ctx(ctx)
	switch evt.GetBody().GetType().(type) {
	case *pb.Event_EventBody_GroupViewed:
		log.Debug().Msg("googlechat: unhandled GroupViewed event (M2+)")
	case *pb.Event_EventBody_GroupUpdated:
		log.Debug().Msg("googlechat: unhandled GroupUpdated event (M2+)")
	case *pb.Event_EventBody_MessagePosted:
		c.handleMessagePosted(ctx, evt)
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
// M2 Task 4 only builds a RemoteMessage for the new-message case
// (queueMessagePosted). MESSAGE_UPDATED (RemoteEdit) is M4's job.
func (c *GChatClient) handleMessagePosted(ctx context.Context, evt *pb.Event) {
	if evt.GetType() == pb.Event_MESSAGE_UPDATED {
		zerolog.Ctx(ctx).Debug().Msg("googlechat: unhandled MessagePosted/MESSAGE_UPDATED (edit) event (M4)")
		return
	}
	c.queueMessagePosted(ctx, evt)
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
// A missing/malformed payload (nil message, empty message id, or a group id
// neither GroupIDToParts oneof arm sets) is logged and dropped rather than
// queuing a broken RemoteMessage -- matching how sync.go's syncChats skips a
// world item with no usable group id.
func (c *GChatClient) queueMessagePosted(ctx context.Context, evt *pb.Event) {
	log := zerolog.Ctx(ctx)
	msg := evt.GetBody().GetMessagePosted().GetMessage()
	if msg == nil {
		log.Warn().Msg("googlechat: MessagePosted event with no message payload, skipping")
		return
	}
	gcMessageID := msg.GetId().GetMessageId()
	if gcMessageID == "" {
		log.Warn().Msg("googlechat: MessagePosted event with no message id, skipping")
		return
	}
	id, isDM, ok := gchatmeow.GroupIDToParts(evt.GetGroupId())
	if !ok {
		log.Warn().Str("gc_message_id", gcMessageID).
			Msg("googlechat: MessagePosted event with no usable group id, skipping")
		return
	}
	group := gcid.GroupID{ID: id, IsDM: isDM}
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
		Str("gc_group_id", id).
		Bool("is_dm", isDM).
		Any("result", res).
		Msg("googlechat: queued inbound message")
}
