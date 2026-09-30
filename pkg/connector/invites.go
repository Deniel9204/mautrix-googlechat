package connector

// invites.go -- spaces this account is INVITED to but has not joined.
//
// Inbound: the invite reaches the invitee as a standalone
// MembershipChangedEvent on the realtime channel -- new_membership for the
// user's own id, state MEMBER_INVITED (verified live 2026-09-29). It is NOT in
// paginated_world: the chat list only carries spaces the user has joined, and
// no world filter or section returns pending invites (also verified live), so
// an invite sent while the bridge is down only surfaces if catch_up replays
// the event. On the event the bridge records the invite as pending and
// creates a portal with the user marked *invited*, not joined
// (invitedSpaceMembers): bridgev2 then sends a plain Matrix invite and, unlike
// for a joined member, never auto-accepts it -- nothing is accepted on Google
// Chat without a user action.
//
// Outbound: the user's Matrix accept/decline arrives as AcceptInvite /
// RejectInvite (handlemembership.go). Those types ALSO arrive for the bridge's
// own auto-accept of every portal it creates, so the RPC is only sent when the
// space is in UserLoginMetadata.PendingInvites. Anything else stays a no-op.
//
// Spam-category invites are ignored, matching the Google Chat web client,
// which files them away. Invited DMs (message requests) are out of scope.
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

// handleOwnMembershipChanged reads the user's own membership off a standalone
// MembershipChangedEvent: an invite to a space records it as pending and
// creates its portal; any other own state answers a pending invite (accepted
// or declined from another Google Chat client). Other users' changes are
// ignored here -- they arrive as SYSTEM_MESSAGEs (systemmessage.go).
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

// invitedSpaceChatInfo is an invited space's portal info: its name, topic and
// threading from get_group when the server gives them to an invitee (best
// effort -- a failure just leaves the room unnamed), and ALWAYS the
// invited-member list below. get_group's own member list is deliberately
// ignored: chatInfoFromGetGroupResponse marks every member joined, self
// included, and a joined self is exactly what makes the bridge accept on the
// user's behalf.
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
	// Not a member yet: history is unreadable until the invite is accepted,
	// and the only members known for sure are the user (invited).
	info.CanBackfill = false
	info.Members = invitedSpaceMembers(c.ownUserID())
	return info, nil
}

// invitedSpaceMembers is the member list for an invited space: just the user,
// as invited. Deliberately not IsFull -- an invitee cannot see the real member
// list, and IsFull would kick everyone else out of a portal another login
// already shares. PrevMembership=leave makes the invite one-shot: a user
// already joined (or still invited) on Matrix is never re-invited.
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

// dropJoinedPendingInvites forgets pending invites to spaces the chat list now
// shows the user as a member of -- accepted from another client while no event
// reached the bridge. The chat list never lists pending invites, so it can
// only ever remove entries, never add them.
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

// hasPendingInvite reports whether Google Chat reported this account as
// invited to the space and the invite has not been answered since.
func (c *GChatClient) hasPendingInvite(spaceID string) bool {
	c.metaMu.Lock()
	defer c.metaMu.Unlock()
	meta, ok := c.UserLogin.Metadata.(*UserLoginMetadata)
	return ok && meta != nil && slices.Contains(meta.PendingInvites, spaceID)
}

// clearPendingInvite drops one space from the pending set once the invite has
// been answered, on either side.
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

// resyncAcceptedSpace queues a full resync of a space the user just joined:
// get_group for the real member list (an invited portal only knew the user)
// plus a forward backfill of its history, which was unreadable while the user
// was only invited.
//
// Queued from a goroutine: this runs inside HandleMatrixMembership, i.e. on
// the portal's own event loop, and with an unbuffered portal queue
// (bridge.portal_event_buffer: 0) queueing into it synchronously from here
// would deadlock.
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
