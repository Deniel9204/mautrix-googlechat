package connector

// invites.go -- spaces this account is INVITED to but has not joined.
//
// Inbound: paginated_world already reports every invited space (its
// read_state.membership_state is MEMBER_INVITED), so chat-list sync creates a
// portal for it (sync.go) with the logged-in user marked as *invited*, not
// joined (invitedSpaceMembers). bridgev2 then sends the user a plain Matrix
// invite and, unlike for a joined member, never auto-accepts it on their
// behalf -- so nothing is accepted on Google Chat without a user action.
//
// Outbound: the user's Matrix accept/decline arrives as AcceptInvite /
// RejectInvite (handlemembership.go). Those types ALSO arrive for the bridge's
// own auto-accept of every portal it creates, so the RPC is only sent when the
// space is in UserLoginMetadata.PendingInvites -- i.e. Google Chat itself
// reported the invite at the last sync. Anything else stays a no-op.
//
// Spam-category invites are skipped entirely (planChatSync), matching the
// Google Chat web client, which files them away rather than listing them.
// Invited DMs (message requests) are out of scope and still skipped.
import (
	"context"
	"slices"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// isInvitedSpace reports whether a world item is a space this account has a
// pending (non-spam) invite to.
func isInvitedSpace(item *pb.WorldItemLite) bool {
	rs := item.GetReadState()
	if rs.GetMembershipState() != pb.MembershipState_MEMBER_INVITED || rs.GetInviteCategory() == pb.InviteCategory_SPAM_INVITE {
		return false
	}
	_, isDM, ok := gchatmeow.GroupIDToParts(item.GetGroupId())
	return ok && !isDM
}

// invitedSpaceMembers is the member list for an invited space: the user
// themself as invited, plus the inviter (when known) as joined so the room
// shows who the invite is from. It is deliberately not IsFull -- an invitee
// cannot see the real member list -- and providing it at all stops room
// creation from calling get_group, which needs the user to be a member.
//
// PrevMembership=leave on the self entry makes the invite one-shot: once the
// user has joined (or is still invited) on Matrix, a later resync of the same
// still-pending invite does not try to re-invite them.
func invitedSpaceMembers(item *pb.WorldItemLite, ownUserID networkid.UserID) *bridgev2.ChatMemberList {
	members := &bridgev2.ChatMemberList{MemberMap: bridgev2.ChatMemberMap{
		ownUserID: {
			EventSender:    bridgev2.EventSender{Sender: ownUserID, IsFromMe: true},
			Membership:     event.MembershipInvite,
			PrevMembership: event.MembershipLeave,
		},
	}}
	if inviter := item.GetReadState().GetInviteState().GetInviterUserId().GetId(); inviter != "" {
		if uid := gcid.MakeUserID(inviter); uid != ownUserID {
			members.MemberMap[uid] = bridgev2.ChatMember{
				EventSender: bridgev2.EventSender{Sender: uid},
				Membership:  event.MembershipJoin,
			}
		}
	}
	return members
}

// setPendingInvites replaces the persisted pending-invite set with the one
// the latest chat-list sync saw. Only saves when the set actually changed.
func (c *GChatClient) setPendingInvites(ctx context.Context, spaceIDs []string) error {
	slices.Sort(spaceIDs)
	return c.updateMetadata(ctx, func(meta *UserLoginMetadata) bool {
		if slices.Equal(meta.PendingInvites, spaceIDs) {
			return false
		}
		meta.PendingInvites = spaceIDs
		return true
	})
}

// hasPendingInvite reports whether Google Chat last reported this account as
// invited to the space.
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
// get_group for the real member list (an invited portal only knew the user
// and the inviter) plus a forward backfill of its history, which was
// unreadable while the user was only invited.
//
// Queued from a goroutine: this runs inside HandleMatrixMembership, i.e. on
// the portal's own event loop, and with an unbuffered portal queue
// (bridgev2.PortalEventBuffer == 0) queueing into it synchronously from here
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
