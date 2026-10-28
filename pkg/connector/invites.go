package connector

// Pending space invites arrive only as a realtime MembershipChangedEvent, never in paginated_world.
import (
	"context"
	"slices"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// handleOwnMembershipChanged records or answers a pending space invite from the user's own membership change.
func (c *GChatClient) handleOwnMembershipChanged(ctx context.Context, evt *pb.Event) bridgev2.EventHandlingResult {
	log := zerolog.Ctx(ctx)
	change := evt.GetBody().GetMembershipChanged()
	m := change.GetNewMembership()
	userID := m.GetId().GetMemberId().GetUserId().GetId()
	groupID := m.GetId().GetGroupId()
	if groupID == nil {
		groupID = evt.GetGroupId()
	}
	id, isDM, ok := gchatmeow.GroupIDToParts(groupID)
	log.Debug().
		Str("gc_user_id", userID).
		Str("gc_group_id", id).
		Bool("is_dm", isDM).
		Str("membership", m.GetMembershipState().String()).
		Str("prior_membership", change.GetPriorMembershipState().String()).
		Str("invite_category", m.GetInviteCategory().String()).
		Msg("googlechat: MembershipChanged event")
	if !ok || isDM || userID == "" || gcid.MakeUserID(userID) != c.ownUserID() {
		return bridgev2.EventHandlingResultIgnored
	}
	if m.GetMembershipState() != pb.MembershipState_MEMBER_INVITED {
		if err := c.clearPendingInvite(ctx, id); err != nil {
			log.Err(err).Str("space_id", id).Msg("googlechat: failed to clear answered space invite")
		}
		return bridgev2.EventHandlingResultIgnored
	}
	if m.GetInviteCategory() == pb.InviteCategory_SPAM_INVITE {
		log.Info().Str("space_id", id).Msg("googlechat: ignoring space invite Google categorised as spam")
		return bridgev2.EventHandlingResultIgnored
	}
	if err := c.addPendingInvite(ctx, id); err != nil {
		log.Err(err).Str("space_id", id).Msg("googlechat: failed to save pending space invite")
		return bridgev2.EventHandlingResultFailed.WithError(err)
	}
	log.Info().Str("space_id", id).Msg("googlechat: invited to a space, creating its portal as an invite")
	return c.queueChatResync(&simplevent.ChatResync{
		EventMeta: simplevent.EventMeta{
			Type:         bridgev2.RemoteEventChatResync,
			PortalKey:    gcid.MakePortalKey(gcid.GroupID{ID: id}, c.UserLogin.ID),
			CreatePortal: true,
		},
		GetChatInfoFunc: c.invitedSpaceChatInfo,
	})
}

// invitedSpaceChatInfo is an invited space's portal info; get_group's member list is ignored
// because it marks self as joined, which would make the bridge accept on the user's behalf.
func (c *GChatClient) invitedSpaceChatInfo(ctx context.Context, portal *bridgev2.Portal) (*bridgev2.ChatInfo, error) {
	group, err := gcid.ParsePortalID(portal.ID)
	if err != nil {
		return nil, err
	}
	roomType := database.RoomTypeDefault
	info := &bridgev2.ChatInfo{Type: &roomType}
	if resp, err := c.getGroup(ctx, group); err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Str("space_id", group.ID).Msg("googlechat: get_group failed for an invited space, creating it unnamed")
	} else {
		info = chatInfoFromGetGroupResponse(group, resp, c.ownUserID())
	}
	// Not a member yet: history is unreadable until the invite is accepted.
	info.CanBackfill = false
	info.Members = invitedSpaceMembers(c.ownUserID())
	return info, nil
}

// invitedSpaceMembers lists just the user as invited; not IsFull, since an invitee can't see the real members.
func invitedSpaceMembers(ownUserID networkid.UserID) *bridgev2.ChatMemberList {
	return &bridgev2.ChatMemberList{MemberMap: bridgev2.ChatMemberMap{
		ownUserID: {
			EventSender:    bridgev2.EventSender{Sender: ownUserID, IsFromMe: true},
			Membership:     event.MembershipInvite,
			PrevMembership: event.MembershipLeave,
		},
	}}
}

// addPendingInvite records a space invite Google Chat reported.
func (c *GChatClient) addPendingInvite(ctx context.Context, spaceID string) error {
	return c.updateMetadata(ctx, func(meta *UserLoginMetadata) bool {
		if slices.Contains(meta.PendingInvites, spaceID) {
			return false
		}
		meta.PendingInvites = append(slices.Clone(meta.PendingInvites), spaceID)
		return true
	})
}

// dropJoinedPendingInvites forgets pending invites to spaces the chat list now shows as joined.
func (c *GChatClient) dropJoinedPendingInvites(ctx context.Context, items []*pb.WorldItemLite) error {
	joined := make(map[string]bool, len(items))
	for _, item := range items {
		if item.GetReadState().GetMembershipState() != pb.MembershipState_MEMBER_JOINED {
			continue
		}
		if id, isDM, ok := gchatmeow.GroupIDToParts(item.GetGroupId()); ok && !isDM {
			joined[id] = true
		}
	}
	return c.updateMetadata(ctx, func(meta *UserLoginMetadata) bool {
		kept := slices.DeleteFunc(slices.Clone(meta.PendingInvites), func(id string) bool { return joined[id] })
		if len(kept) == len(meta.PendingInvites) {
			return false
		}
		meta.PendingInvites = kept
		return true
	})
}

// hasPendingInvite reports whether the space has an unanswered invite.
func (c *GChatClient) hasPendingInvite(spaceID string) bool {
	c.metaMu.Lock()
	defer c.metaMu.Unlock()
	meta, ok := c.UserLogin.Metadata.(*UserLoginMetadata)
	return ok && meta != nil && slices.Contains(meta.PendingInvites, spaceID)
}

// clearPendingInvite drops an answered invite from the pending set.
func (c *GChatClient) clearPendingInvite(ctx context.Context, spaceID string) error {
	return c.updateMetadata(ctx, func(meta *UserLoginMetadata) bool {
		i := slices.Index(meta.PendingInvites, spaceID)
		if i < 0 {
			return false
		}
		meta.PendingInvites = slices.Delete(slices.Clone(meta.PendingInvites), i, i+1)
		return true
	})
}

// resyncAcceptedSpace queues a resync and backfill of a newly joined space. Runs in a goroutine:
// queueing synchronously from the portal's event loop would deadlock with an unbuffered queue.
func (c *GChatClient) resyncAcceptedSpace(group gcid.GroupID) {
	evt := &simplevent.ChatResync{
		EventMeta: simplevent.EventMeta{
			Type:      bridgev2.RemoteEventChatResync,
			PortalKey: gcid.MakePortalKey(group, c.UserLogin.ID),
		},
		GetChatInfoFunc: c.GetChatInfo,
		LatestMessageTS: time.Now(),
	}
	go c.queueChatResync(evt)
}
