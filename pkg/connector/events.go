package connector

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

// typingTimeout is the Matrix typing timeout for TYPING (STOPPED sends 0).
const typingTimeout = 6 * time.Second

// handleGChatEvent dispatches evt and advances the revision watermarks only on success,
// so a failed event is re-fetched by the next catch_up.
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

// dispatchGChatEvent routes evt by body type; unhandled arms report Ignored (Success).
func (c *GChatClient) dispatchGChatEvent(ctx context.Context, evt *pb.Event) bridgev2.EventHandlingResult {
	log := zerolog.Ctx(ctx)
	switch evt.GetBody().GetType().(type) {
	case *pb.Event_EventBody_GroupViewed:
		return c.queueGroupViewed(ctx, evt)
	case *pb.Event_EventBody_GroupUpdated:
		// Unhandled: renames and topic changes arrive as a SYSTEM_MESSAGE ROOM_UPDATED annotation
		// (systemmessage.go); this body has no rename-vs-topic split.
		log.Debug().Msg("googlechat: unhandled GroupUpdated event (handled via SYSTEM_MESSAGE instead, see systemmessage.go)")
	case *pb.Event_EventBody_MessagePosted:
		return c.handleMessagePosted(ctx, evt)
	case *pb.Event_EventBody_WebPushNotification:
		log.Debug().Msg("googlechat: unhandled WebPushNotification event (M2+)")
	case *pb.Event_EventBody_MembershipChanged:
		// Only the user's own invite state is read here (invites.go); other members' changes
		// come from SYSTEM_MESSAGE MEMBERSHIP_CHANGED annotations.
		return c.handleOwnMembershipChanged(ctx, evt)
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

// handleMessagePosted handles the message_posted body (EventBody field 6). Only
// Event.type == MESSAGE_UPDATED is an edit; every other type (incl. ON_HOLD_*) is a new message.
func (c *GChatClient) handleMessagePosted(ctx context.Context, evt *pb.Event) bridgev2.EventHandlingResult {
	if evt.GetType() == pb.Event_MESSAGE_UPDATED {
		return c.queueMessageEdit(ctx, evt)
	}
	if res, handled := c.trySystemMessage(ctx, evt); handled {
		return res
	}
	return c.queueMessagePosted(ctx, evt)
}

// extractPostedMessage returns msg, group and message id; ok is false (logged) if malformed.
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

// queueMessagePosted queues a RemoteMessage. Sender is evt.creator, not the outer Event.user_id.
// Malformed payloads report Ignored so the watermark advances past them.
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
			// StreamOrder is the create time in µs, as in backfill.
			StreamOrder: msg.GetCreateTime(),
		},
		ID: gcid.MakeMessageID(gcMessageID),
		// TransactionID: Message.local_id (field 14) echoes our own send's local_id, for echo dedup.
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

// queueMessageEdit queues a RemoteEdit. Edits reuse the original message id; the timestamp
// is last_edit_time, falling back to last_update_time.
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
			// StreamOrder: editTS, consistent with Timestamp.
			StreamOrder: editTS,
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

// queueMessageDeleted queues a RemoteMessageRemove (EventBody field 18). Group comes from the
// outer Event; the redaction is sent by the bridge bot, so Sender is left empty.
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

// queueMessageReaction queues a RemoteReaction(Remove) (EventBody field 22). EmojiID is the bare
// emoji (no variation selector) so Matrix-initiated reactions and their echoes key identically.
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

// queueReadReceiptChanged queues one RemoteReadReceipt per entry (EventBody field 33). A failed
// entry stops the loop; re-delivery on catch-up is safe since receipts are idempotent.
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
			// An empty sender would make bridgev2 mark the room read as the bot.
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

// queueGroupViewed queues a read receipt for the login's own account (EventBody field 3):
// the user viewed the chat from another client.
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

// typingContextGroupID returns the group id from the TypingContext oneof (group_id or topic_id.group_id).
func typingContextGroupID(tc *pb.TypingContext) *pb.GroupId {
	if tc == nil {
		return nil
	}
	if gid := tc.GetGroupId(); gid != nil {
		return gid
	}
	return tc.GetTopicId().GetGroupId()
}

// queueTypingStateChanged queues a RemoteTyping (EventBody field 26). Unlike every other arm,
// the group comes from the body's TypingContext: the outer Event.group_id is empty on the wire.
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
