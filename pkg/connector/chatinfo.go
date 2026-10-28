package connector

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// ownUserID returns the login's gaia ID as a UserID.
func (c *GChatClient) ownUserID() networkid.UserID {
	return gcid.MakeUserID(string(c.UserLogin.ID))
}

// GetChatInfo fetches a portal's live chat info via get_group (uncached).
func (c *GChatClient) GetChatInfo(ctx context.Context, portal *bridgev2.Portal) (*bridgev2.ChatInfo, error) {
	group, err := gcid.ParsePortalID(portal.ID)
	if err != nil {
		return nil, fmt.Errorf("googlechat: invalid portal id %q: %w", portal.ID, err)
	}
	conn := c.getConn()
	if conn == nil {
		return nil, fmt.Errorf("googlechat: not connected")
	}
	resp, err := conn.GetGroup(ctx, &pb.GetGroupRequest{
		GroupId: gchatmeow.PartsToGroupID(group.ID, group.IsDM),
		FetchOptions: []pb.GetGroupRequest_FetchOptions{
			pb.GetGroupRequest_MEMBERS,
			pb.GetGroupRequest_INCLUDE_DYNAMIC_GROUP_NAME,
		},
		// Without this, get_group 404s for DMs.
		IncludeInviteDms: proto.Bool(true),
	})
	if err != nil {
		return nil, fmt.Errorf("googlechat: get_group failed: %w", err)
	}
	return chatInfoFromGetGroupResponse(group, resp, c.ownUserID()), nil
}

// getGroup issues get_group for one conversation; routes through getGroupFn in tests.
func (c *GChatClient) getGroup(ctx context.Context, group gcid.GroupID) (*pb.GetGroupResponse, error) {
	fetch := c.getGroupFn
	if fetch == nil {
		conn := c.getConn()
		if conn == nil {
			return nil, errors.New("googlechat: not connected")
		}
		fetch = conn.GetGroup
	}
	return fetch(ctx, &pb.GetGroupRequest{
		GroupId: gchatmeow.PartsToGroupID(group.ID, group.IsDM),
		// A stripped-down request has been seen to 403.
		FetchOptions:     []pb.GetGroupRequest_FetchOptions{pb.GetGroupRequest_MEMBERS, pb.GetGroupRequest_INCLUDE_DYNAMIC_GROUP_NAME},
		IncludeInviteDms: proto.Bool(true),
	})
}

// chatInfoFromGetGroupResponse converts a get_group response into a ChatInfo.
func chatInfoFromGetGroupResponse(group gcid.GroupID, resp *pb.GetGroupResponse, ownUserID networkid.UserID) *bridgev2.ChatInfo {
	g := resp.GetGroup()

	memberIDs := make([]string, 0, len(resp.GetMemberships()))
	for _, m := range resp.GetMemberships() {
		if id := m.GetId().GetMemberId().GetUserId().GetId(); id != "" {
			memberIDs = append(memberIDs, id)
		}
	}

	// A Google Chat "Space" is an ordinary room: RoomTypeSpace would make an
	// m.space container and disable backfill.
	roomType := database.RoomTypeDefault
	if group.IsDM {
		roomType = database.RoomTypeDM
	}

	info := &bridgev2.ChatInfo{
		Type: &roomType,
		// FetchMessages is supported for every portal.
		CanBackfill: true,
		Members:     chatMemberList(memberIDs, ownUserID, group.IsDM),
	}
	if !group.IsDM {
		applySpaceRoles(info.Members, resp.GetMemberships())
	}
	// Spaces only. Presence-gated (proto2): absent means no update, "" is a real rename.
	if !group.IsDM && g.Name != nil {
		info.Name = g.Name
	}
	// Topic is unconditional; unset yields "".
	desc := g.GetGroupDetails().GetDescription()
	info.Topic = &desc
	threadsOnly := g.GetThreadedGroup() != nil
	threadsEnabled := g.GetFlatThreadsEnabled() || threadsOnly
	info.ExtraUpdates = threadingExtraUpdater(threadsEnabled, threadsOnly)
	return info
}

// chatInfoFromWorldItem builds a ChatInfo from a PaginatedWorld item. Spaces get
// no Members here (WorldItemLite has none); GetChatInfo fills them in later.
func chatInfoFromWorldItem(item *pb.WorldItemLite, ownUserID networkid.UserID) *bridgev2.ChatInfo {
	_, isDM, _ := gchatmeow.GroupIDToParts(item.GetGroupId())

	// RoomTypeDefault, as in chatInfoFromGetGroupResponse.
	roomType := database.RoomTypeDefault
	if isDM {
		roomType = database.RoomTypeDM
	}

	info := &bridgev2.ChatInfo{Type: &roomType, CanBackfill: true}
	// Per-chat mute (handlemute.go).
	info.UserLocal = mutedUntilFromReadState(item.GetReadState())

	if isDM {
		info.Members = dmMemberListFromWorldItem(item, ownUserID)
	} else if item.RoomName != nil {
		// Presence-gated, as in chatInfoFromGetGroupResponse.
		info.Name = item.RoomName
	}

	desc := item.GetGroupLite().GetGroupDetails().GetDescription()
	info.Topic = &desc

	threadsOnly := item.GetThreadedGroup() != nil
	threadsEnabled := item.GetFlatThreadsEnabled() || threadsOnly
	info.ExtraUpdates = threadingExtraUpdater(threadsEnabled, threadsOnly)
	return info
}

// dmMemberListFromWorldItem builds a DM's member list from joined_users (humans
// only count for OtherUserID), falling back to dm_members.
func dmMemberListFromWorldItem(item *pb.WorldItemLite, ownUserID networkid.UserID) *bridgev2.ChatMemberList {
	var humans, bots []string
	if joined := item.GetReadState().GetJoinedUsers(); len(joined) > 0 {
		for _, u := range joined {
			if u.GetType() == pb.UserType_BOT {
				bots = append(bots, u.GetId())
			} else {
				humans = append(humans, u.GetId())
			}
		}
	} else {
		for _, u := range item.GetDmMembers().GetMembers() {
			humans = append(humans, u.GetId())
		}
	}

	members := &bridgev2.ChatMemberList{
		IsFull:      true,
		OtherUserID: deriveOtherUserID(humans, ownUserID),
		MemberMap:   make(bridgev2.ChatMemberMap, len(humans)+len(bots)),
	}
	for _, id := range humans {
		addChatMember(members.MemberMap, id, ownUserID)
	}
	for _, id := range bots {
		addChatMember(members.MemberMap, id, ownUserID)
	}
	return members
}

// Space managers (ROLE_OWNER) get 50, the level Matrix needs for name, topic and kick.
const (
	spaceManagerPowerLevel = 50
	spaceMemberPowerLevel  = 0
)

// applySpaceRoles sets each space member's power level from their role. An
// explicit 0 makes a demotion take effect on resync.
func applySpaceRoles(members *bridgev2.ChatMemberList, memberships []*pb.Membership) {
	for _, m := range memberships {
		uid := gcid.MakeUserID(m.GetId().GetMemberId().GetUserId().GetId())
		member, ok := members.MemberMap[uid]
		if !ok {
			continue
		}
		level := spaceMemberPowerLevel
		if m.GetMembershipRole() == pb.MembershipRole_ROLE_OWNER {
			level = spaceManagerPowerLevel
		}
		member.PowerLevel = &level
		members.MemberMap[uid] = member
	}
}

// chatMemberList builds a ChatMemberList from a flat id list; isDM enables OtherUserID.
func chatMemberList(memberIDs []string, ownUserID networkid.UserID, isDM bool) *bridgev2.ChatMemberList {
	members := &bridgev2.ChatMemberList{
		IsFull:    true,
		MemberMap: make(bridgev2.ChatMemberMap, len(memberIDs)),
	}
	if isDM {
		members.OtherUserID = deriveOtherUserID(memberIDs, ownUserID)
	}
	for _, id := range memberIDs {
		addChatMember(members.MemberMap, id, ownUserID)
	}
	return members
}

func addChatMember(m bridgev2.ChatMemberMap, id string, ownUserID networkid.UserID) {
	uid := gcid.MakeUserID(id)
	m[uid] = bridgev2.ChatMember{
		EventSender: bridgev2.EventSender{Sender: uid, IsFromMe: uid == ownUserID},
		Membership:  event.MembershipJoin,
	}
}

// deriveOtherUserID removes own id only from a 2-entry list and returns the
// remaining id if exactly one is left.
func deriveOtherUserID(memberIDs []string, ownUserID networkid.UserID) networkid.UserID {
	ids := memberIDs
	if len(ids) == 2 {
		ids = removeFirst(ids, string(ownUserID))
	}
	if len(ids) == 1 {
		return gcid.MakeUserID(ids[0])
	}
	return ""
}

// removeFirst returns a copy of ids without the first occurrence of target.
func removeFirst(ids []string, target string) []string {
	out := make([]string, 0, len(ids))
	removed := false
	for _, id := range ids {
		if !removed && id == target {
			removed = true
			continue
		}
		out = append(out, id)
	}
	return out
}

// threadingExtraUpdater stores the threading flags in PortalMetadata.
func threadingExtraUpdater(threadsEnabled, threadsOnly bool) bridgev2.ExtraUpdater[*bridgev2.Portal] {
	return func(_ context.Context, portal *bridgev2.Portal) bool {
		meta, ok := portal.Metadata.(*PortalMetadata)
		if !ok {
			return false
		}
		changed := false
		if meta.ThreadsOnly != threadsOnly {
			meta.ThreadsOnly = threadsOnly
			changed = true
		}
		if meta.ThreadsEnabled != threadsEnabled {
			meta.ThreadsEnabled = threadsEnabled
			changed = true
		}
		return changed
	}
}
