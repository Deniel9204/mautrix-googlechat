package connector

// chatinfo.go -- chat metadata (name/topic/members/threading flags), shared
// between the live GetChatInfo RPC path (get_group -> GetGroupResponse) and
// the chat-list-sync path (sync.go's chatInfoFromWorldItem, built from a
// PaginatedWorld response's lighter WorldItemLite shape). Both response
// shapes feed the same ChatInfo output; this file keeps the two entry points
// separate (one per Go response type) but shares the DM-member/other-user
// derivation logic (deriveOtherUserID) between them, post-processing
// whichever member-id list each branch built.
//
// chatInfoFromWorldItem here takes an ownUserID parameter: Google Chat's
// dm_members/joined_users lists include BOTH DM participants, not just "the
// other one", and self must be removed from that list before deriving
// other_user_id -- there is no way to reproduce that removal without
// knowing which member ID is "self". Two bridgev2
// mechanisms that might look like substitutes don't actually eliminate the
// need for it: (1) updateOtherUser's own generic "2 members, exactly one
// IsFromMe" fallback (portal.go:4563-4587) is real and always active
// (unlike (2) below, it isn't gated on portal.Receiver), but it only
// CONSUMES already-tagged IsFromMe flags on the ChatMember entries this
// file builds -- tagging those correctly in the first place still requires
// knowing which member is "self", so it doesn't remove the need for
// ownUserID, just moves where it would be needed to; (2)
// bridgev2.ChatMemberList.CheckAllLogins (lets the framework infer a
// sender's identity via IsThisUser without SenderLogin pre-filled) is
// gated on portal.Receiver == "" (portal.go's getIntentAndUserMXIDFor), and
// gcid.MakePortalKey scopes every DM portal to a receiver, so that path
// never engages for this bridge regardless. The derivation has to happen
// here instead, and GetChatInfo already has c.UserLogin.ID in scope, so
// only the free function's signature changes.
import (
	"context"
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

// ownUserID returns this login's own Google account, reinterpreted as a
// networkid.UserID -- the login's UserLoginID IS the account's gaia ID
// (gcid.MakeUserLoginID, login.go), so this is the same conversion
// IsThisUser (client.go) already does.
func (c *GChatClient) ownUserID() networkid.UserID {
	return gcid.MakeUserID(string(c.UserLogin.ID))
}

// GetChatInfo fetches a portal's live chat info via the get_group RPC.
// Fidelity note: this bridge does NOT cache the GetGroupResponse per
// conversation (keyed by group revision) the way an account-level cache
// could -- every GetChatInfo call is a fresh RPC. bridgev2 itself only calls
// NetworkAPI.GetChatInfo on resync/backfill-check paths, not on every
// message, so the cache's main purpose (avoiding redundant per-message
// lookups) does not apply the same way here.
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
		// purple-googlechat (the actively-maintained 2026 client) sets this
		// unconditionally on get_group (googlechat_conversation.c:776-777);
		// our own original request did not set it. Live testing showed
		// get_group returning HTTP 404 for a DM under the default
		// (invite-excluded) view -- exactly the symptom of the server not
		// resolving the DM by id without this.
		IncludeInviteDms: proto.Bool(true),
	})
	if err != nil {
		return nil, fmt.Errorf("googlechat: get_group failed: %w", err)
	}
	return chatInfoFromGetGroupResponse(group, resp, c.ownUserID()), nil
}

// chatInfoFromGetGroupResponse wraps a get_group RPC response into
// bridgev2.ChatInfo: description from group.group_details.description (any
// chat type -- not special-cased for DMs even though they rarely have one),
// threads_only/threads_enabled from the threaded_group field
// presence/flat_threads_enabled, member ids from group.memberships, and --
// for DMs -- the same other-user derivation applied to any is_dm chat
// regardless of which response shape produced the member list.
func chatInfoFromGetGroupResponse(group gcid.GroupID, resp *pb.GetGroupResponse, ownUserID networkid.UserID) *bridgev2.ChatInfo {
	g := resp.GetGroup()

	memberIDs := make([]string, 0, len(resp.GetMemberships()))
	for _, m := range resp.GetMemberships() {
		if id := m.GetId().GetMemberId().GetUserId().GetId(); id != "" {
			memberIDs = append(memberIDs, id)
		}
	}

	// A non-DM Google Chat conversation (Google's own term for it is a
	// "Space") is an ordinary group chat and maps to bridgev2's
	// RoomTypeDefault -- NOT RoomTypeSpace. Those two "space"s are different
	// concepts that collided by name: bridgev2's RoomTypeSpace means a Matrix
	// m.space CONTAINER in a parent/child room hierarchy (mautrix-go
	// bridgev2/portal.go:5253-5254, which stamps creation_content
	// type=m.space on any RoomTypeSpace portal), which a Google Chat group
	// conversation is not -- this bridge has no parent/child portal hierarchy
	// and never sets ChatInfo.ParentID. Using RoomTypeSpace here (a
	// naming-collision bug) had two consequences, both wrong: every non-DM
	// room was created as an m.space container instead of a normal chat
	// room, AND mautrix-go's own backfill triggers are ALL gated on
	// `portal.RoomType != database.RoomTypeSpace` (portal.go:3870, 5362,
	// 5400-5404), so a RoomTypeSpace portal is silently skipped by the
	// backfill queue no matter what ChatInfo.CanBackfill says, making the
	// whole flat-room history feature a no-op for group Spaces. The meta
	// blueprint agrees this is an ordinary room (RoomTypeDefault for
	// ordinary group chats, _reference/meta/pkg/connector/chatinfo.go:138,
	// reserving RoomTypeSpace for its literal Community-container feature).
	roomType := database.RoomTypeDefault
	if group.IsDM {
		roomType = database.RoomTypeDM
	}

	info := &bridgev2.ChatInfo{
		Type: &roomType,
		// CanBackfill: tells the framework this portal supports
		// bridgev2.BackfillingNetworkAPI.FetchMessages (backfill.go), so it
		// queues history backfill for it -- unconditionally true for every
		// portal regardless of room type; FetchMessages itself returns an
		// empty response for the strategies not yet implemented
		// (see backfill.go's region doc comment) rather than this flag
		// gating which portals are even offered the chance.
		CanBackfill: true,
		Members:     chatMemberList(memberIDs, ownUserID, group.IsDM),
	}
	// Name (spaces only): DM room names are derived by bridgev2 from the
	// other member's ghost displayname. Presence-gated on the raw *string
	// field (proto2 HasField equivalent), NOT a `!= ""` value check: an
	// absent name field must skip any name update at all (e.g. a space still
	// using Google's dynamically
	// computed name, INCLUDE_DYNAMIC_GROUP_NAME's whole reason for being
	// requested above) -- collapsing "absent" and "explicitly set to empty"
	// into the same `!= ""` branch would also silently swallow a genuine
	// rename-to-empty from the server.
	if !group.IsDM && g.Name != nil {
		info.Name = g.Name
	}
	// Topic: unconditional -- always set to whatever
	// group.group_details.description resolves to, including "" when
	// GroupDetails/Description was never set, since the proto accessor on an
	// unset submessage/field also just yields "" with no HasField gate at
	// all.
	desc := g.GetGroupDetails().GetDescription()
	info.Topic = &desc
	threadsOnly := g.GetThreadedGroup() != nil
	threadsEnabled := g.GetFlatThreadsEnabled() || threadsOnly
	info.ExtraUpdates = threadingExtraUpdater(threadsEnabled, threadsOnly)
	return info
}

// chatInfoFromWorldItem builds the same bridgev2.ChatInfo shape as
// chatInfoFromGetGroupResponse, but from a PaginatedWorld response item
// (sync.go's syncChats) instead of a live get_group RPC -- the "lighter"
// WorldItemLite shape.
//
// Known, intentional simplification: a non-DM WorldItemLite could be
// upgraded to a full get_group RPC response before doing anything else, but
// that upgrade is not done here: emitting one get_group RPC per space in the
// sync loop would multiply the request volume by the chat count for
// comparatively little payoff (only a portal's initial room name/topic/
// threading flags would benefit; membership and everything else is refetched
// on the next GetChatInfo call anyway). Consequence:
// chatInfoFromWorldItem leaves Members nil for spaces (WorldItemLite carries
// no full membership list the way GetGroupResponse.memberships does) -- a
// freshly-created space portal starts with no participants until
// GetChatInfo/backfill refreshes it. DM portals are unaffected: DMs are
// never upgraded, so read_state.joined_users/dm_members already is the
// fidelity-complete source for a DM's member list.
func chatInfoFromWorldItem(item *pb.WorldItemLite, ownUserID networkid.UserID) *bridgev2.ChatInfo {
	_, isDM, _ := gchatmeow.GroupIDToParts(item.GetGroupId())

	// RoomTypeDefault (not RoomTypeSpace) for a non-DM Google Chat "Space" --
	// see chatInfoFromGetGroupResponse's identical roomType block for the full
	// naming-collision rationale (both ChatInfo-building entry points must
	// agree, or the same conversation would resolve to different room types
	// depending on which path -- live GetChatInfo vs chat-list sync -- created
	// it first).
	roomType := database.RoomTypeDefault
	if isDM {
		roomType = database.RoomTypeDM
	}
	// CanBackfill: see chatInfoFromGetGroupResponse's identical field for the
	// full rationale -- both ChatInfo-building entry points (this one feeds
	// sync.go's chat-list-sync/ChatResync path) must set it the same way.
	info := &bridgev2.ChatInfo{Type: &roomType, CanBackfill: true}

	if isDM {
		info.Members = dmMemberListFromWorldItem(item, ownUserID)
	} else if item.RoomName != nil {
		// Presence-gated on room_name (proto2 HasField equivalent) -- see
		// chatInfoFromGetGroupResponse's Name comment for why this must not
		// collapse to a `!= ""` value check.
		info.Name = item.RoomName
	}
	// Topic: WorldItemLite's description lives at group_lite.group_details,
	// unconditional for the same reason as chatInfoFromGetGroupResponse's
	// Topic.
	desc := item.GetGroupLite().GetGroupDetails().GetDescription()
	info.Topic = &desc

	threadsOnly := item.GetThreadedGroup() != nil
	threadsEnabled := item.GetFlatThreadsEnabled() || threadsOnly
	info.ExtraUpdates = threadingExtraUpdater(threadsEnabled, threadsOnly)
	return info
}

// dmMemberListFromWorldItem builds a DM's ChatMemberList from a WorldItemLite:
// prefer read_state.joined_users (splitting HUMAN from BOT members -- only
// the humans count toward other-user derivation); fall back to
// dm_members.members (no human/bot split available there) when joined_users
// is empty.
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

// chatMemberList builds a ChatMemberList from a flat id list (the
// GetGroupResponse.memberships shape, which carries no human/bot split).
// isDM gates whether OtherUserID derivation runs at all.
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

// deriveOtherUserID derives a DM's other_user_id: own id is removed ONLY
// when the list has exactly 2 entries (the common you+other-party DM case);
// a 3+-person DM is left untouched and so never resolves to a single other
// user. The result is used only when exactly 1 id remains after that
// conditional removal.
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

// removeFirst returns ids with the first occurrence of target removed
// (first-match-only semantics). ids is not mutated in place.
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

// threadingExtraUpdater persists the threads_only/threads_enabled flags
// derived from either response shape into PortalMetadata, only marking the
// portal changed when a value actually differs.
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
