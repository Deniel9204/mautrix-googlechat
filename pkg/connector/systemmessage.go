package connector

import (
	"context"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// trySystemMessage handles a SYSTEM_MESSAGE with exactly one annotation as a
// ChatInfoChange. handled=false means bridge it as an ordinary message.
func (c *GChatClient) trySystemMessage(ctx context.Context, evt *pb.Event) (res bridgev2.EventHandlingResult, handled bool) {
	msg := evt.GetBody().GetMessagePosted().GetMessage()
	if msg.GetMessageType() != pb.Message_SYSTEM_MESSAGE || len(msg.GetAnnotations()) != 1 {
		return bridgev2.EventHandlingResultIgnored, false
	}
	annotation := msg.GetAnnotations()[0]
	switch annotation.GetType() {
	case pb.AnnotationType_ROOM_UPDATED:
		return c.tryRoomUpdated(ctx, evt, msg, annotation.GetRoomUpdated())
	case pb.AnnotationType_MEMBERSHIP_CHANGED:
		return c.queueMembershipChanged(ctx, evt, msg, annotation.GetMembershipChanged()), true
	default:
		return bridgev2.EventHandlingResultIgnored, false
	}
}

// tryRoomUpdated handles ROOM_UPDATED: a non-empty rename wins over a topic
// update. group_details_metadata is a presence check: an empty topic still applies.
func (c *GChatClient) tryRoomUpdated(ctx context.Context, evt *pb.Event, msg *pb.Message, update *pb.RoomUpdatedMetadata) (bridgev2.EventHandlingResult, bool) {
	info := &bridgev2.ChatInfo{}
	switch {
	case update.GetRenameMetadata().GetNewName() != "":
		name := update.GetRenameMetadata().GetNewName()
		info.Name = &name
	case update.GetGroupDetailsMetadata() != nil:
		desc := update.GetGroupDetailsMetadata().GetNewGroupDetails().GetDescription()
		info.Topic = &desc
	default:
		return bridgev2.EventHandlingResultIgnored, false
	}
	return c.queueSystemMessageChatInfoChange(ctx, evt, msg, &bridgev2.ChatInfoChange{ChatInfo: info}, "room_updated"), true
}

// membershipChangeDelta maps a membership-change type to the ChatMember delta;
// ok=false means no Matrix membership action.
func membershipChangeDelta(t pb.MembershipChangedMetadata_Type) (membership, prevMembership event.Membership, ok bool) {
	switch t {
	case pb.MembershipChangedMetadata_INVITED:
		// PrevMembership=leave so an invite never downgrades an existing member.
		return event.MembershipInvite, event.MembershipLeave, true
	case pb.MembershipChangedMetadata_JOINED:
		// PrevMembership unset: the ghost may not be in "invite" state yet.
		return event.MembershipJoin, "", true
	case pb.MembershipChangedMetadata_ADDED, pb.MembershipChangedMetadata_BOT_ADDED:
		// A direct add: same net effect as JOINED.
		return event.MembershipJoin, "", true
	case pb.MembershipChangedMetadata_LEFT:
		// PrevMembership=join guards against stale or duplicate LEFT notices.
		return event.MembershipLeave, event.MembershipJoin, true
	case pb.MembershipChangedMetadata_REMOVED, pb.MembershipChangedMetadata_BOT_REMOVED:
		// A kick: same shape as LEFT.
		return event.MembershipLeave, event.MembershipJoin, true
	case pb.MembershipChangedMetadata_KICKED_DUE_TO_OTR_CONFLICT:
		// A kick with an OTR-conflict reason, which ChatMember can't carry.
		return event.MembershipLeave, event.MembershipJoin, true
	default:
		// ROLE_UPDATED and TYPE_UNSPECIFIED: no membership action.
		return "", "", false
	}
}

// queueMembershipChanged handles MEMBERSHIP_CHANGED. It reads affected_members
// (field 3), not affected_memberships (field 6), which may be unpopulated.
func (c *GChatClient) queueMembershipChanged(ctx context.Context, evt *pb.Event, msg *pb.Message, update *pb.MembershipChangedMetadata) bridgev2.EventHandlingResult {
	log := zerolog.Ctx(ctx)
	membership, prevMembership, ok := membershipChangeDelta(update.GetType())
	change := &bridgev2.ChatInfoChange{}
	if ok {
		memberMap := make(bridgev2.ChatMemberMap)
		ownID := c.ownUserID()
		for _, member := range update.GetAffectedMembers() {
			gcUserID := member.GetUserId().GetId()
			if gcUserID == "" {
				continue
			}
			uid := gcid.MakeUserID(gcUserID)
			if uid == ownID && membership != event.MembershipInvite {
				// Answered from another client: clear the pending invite so the
				// auto-accept echo isn't treated as a Matrix accept (invites.go).
				if id, isDM, ok := gchatmeow.GroupIDToParts(evt.GetGroupId()); ok && !isDM {
					if err := c.clearPendingInvite(ctx, id); err != nil {
						log.Err(err).Str("space_id", id).Msg("googlechat: failed to clear answered space invite")
					}
				}
			}
			memberMap[uid] = bridgev2.ChatMember{
				EventSender:    bridgev2.EventSender{Sender: uid, IsFromMe: uid == ownID},
				Membership:     membership,
				PrevMembership: prevMembership,
			}
		}
		if len(memberMap) > 0 {
			// IsFull stays false: this is a delta, and true would remove everyone else.
			change.MemberChanges = &bridgev2.ChatMemberList{MemberMap: memberMap}
		}
	}
	if change.MemberChanges == nil {
		// Still queued with no changes so a portal-less space gets its room created.
		log.Debug().
			Str("gc_membership_change_type", update.GetType().String()).
			Msg("googlechat: SYSTEM_MESSAGE membership change with no actionable member delta")
	}
	return c.queueSystemMessageChatInfoChange(ctx, evt, msg, change, "membership_changed")
}

// queueSystemMessageChatInfoChange queues the ChatInfoChange for both branches.
// The sender is msg.creator, not MembershipChangedMetadata.initiator.
func (c *GChatClient) queueSystemMessageChatInfoChange(ctx context.Context, evt *pb.Event, msg *pb.Message, change *bridgev2.ChatInfoChange, kind string) bridgev2.EventHandlingResult {
	log := zerolog.Ctx(ctx)
	id, isDM, groupOK := gchatmeow.GroupIDToParts(evt.GetGroupId())
	if !groupOK {
		log.Warn().Str("gc_system_message_kind", kind).
			Msg("googlechat: SYSTEM_MESSAGE event with no usable group id, skipping")
		return bridgev2.EventHandlingResultIgnored
	}
	group := gcid.GroupID{ID: id, IsDM: isDM}
	senderUserID := gcid.MakeUserID(msg.GetCreator().GetUserId().GetId())

	res := c.queueRemoteEvent(&simplevent.ChatInfoChange{
		EventMeta: simplevent.EventMeta{
			Type:         bridgev2.RemoteEventChatInfoChange,
			PortalKey:    gcid.MakePortalKey(group, c.UserLogin.ID),
			CreatePortal: true,
			Sender: bridgev2.EventSender{
				Sender:   senderUserID,
				IsFromMe: c.IsThisUser(ctx, senderUserID),
			},
			Timestamp: gchatmeow.MicrosToTime(msg.GetCreateTime()),
		},
		ChatInfoChange: change,
	})
	log.Debug().
		Str("gc_group_id", group.ID).
		Bool("is_dm", group.IsDM).
		Str("gc_system_message_kind", kind).
		Any("result", res).
		Msg("googlechat: queued inbound SYSTEM_MESSAGE chat info change")
	return res
}
