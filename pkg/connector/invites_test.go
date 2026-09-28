package connector

// invites_test.go -- invited spaces (#32) and answering the invite from
// Matrix (#36): sync keeps them and records them as pending, their portal
// invites rather than joins the user, and only a pending invite's
// accept/decline reaches Google Chat.

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

func invitedWorldItem(id string, sortTS int64, inviter string) *pb.WorldItemLite {
	item := worldItem(id, sortTS)
	item.RoomName = proto.String("Invited " + id)
	item.ReadState.MembershipState = pb.MembershipState_MEMBER_INVITED.Enum()
	item.ReadState.InviteCategory = pb.InviteCategory_REGULAR_INVITE.Enum()
	if inviter != "" {
		item.ReadState.InviteState = &pb.InviteState{InviterUserId: userIDProto(inviter)}
	}
	return item
}

func pendingInvites(gc *GChatClient) []string {
	return gc.UserLogin.Metadata.(*UserLoginMetadata).PendingInvites
}

// --- planChatSync ----------------------------------------------------------

func TestPlanChatSyncKeepsInvitedSpaceButNotSpamOrDM(t *testing.T) {
	spam := invitedWorldItem("spam", 300, "")
	spam.ReadState.InviteCategory = pb.InviteCategory_SPAM_INVITE.Enum()

	dm := invitedWorldItem("dm", 200, "")
	dm.GroupId = dmGroupID("dm")

	plan := planChatSync([]*pb.WorldItemLite{spam, dm, invitedWorldItem("invited", 100, "")}, 10)

	if len(plan) != 1 {
		t.Fatalf("len(plan) = %d, want 1 (only the regular space invite)", len(plan))
	}
	if id, _, _ := groupIDPlain(plan[0].Item.GetGroupId()); id != "invited" {
		t.Errorf("plan[0] = %q, want \"invited\"", id)
	}
}

// --- chatInfoFromWorldItem -------------------------------------------------

func TestChatInfoFromWorldItemInvitedSpaceInvitesUser(t *testing.T) {
	info := chatInfoFromWorldItem(invitedWorldItem("space1", 100, "777"), ownID)

	if info.CanBackfill {
		t.Error("CanBackfill = true, want false (history is unreadable until the invite is accepted)")
	}
	if info.Name == nil || *info.Name != "Invited space1" {
		t.Errorf("Name = %v, want the room name", info.Name)
	}
	if info.Members == nil {
		t.Fatal("Members = nil; room creation would then call get_group, which needs membership")
	}
	if info.Members.IsFull {
		t.Error("Members.IsFull = true, want false (an invitee cannot see the real member list)")
	}
	self, ok := info.Members.MemberMap[ownID]
	if !ok {
		t.Fatalf("MemberMap missing self: %+v", info.Members.MemberMap)
	}
	if self.Membership != event.MembershipInvite || !self.IsFromMe {
		t.Errorf("self = %+v, want an IsFromMe invite -- a join would make the bridge accept on the user's behalf", self)
	}
	if self.PrevMembership != event.MembershipLeave {
		t.Errorf("self.PrevMembership = %q, want leave (never re-invite a user already joined on Matrix)", self.PrevMembership)
	}
	inviter, ok := info.Members.MemberMap[gcid.MakeUserID("777")]
	if !ok || inviter.Membership != event.MembershipJoin || inviter.IsFromMe {
		t.Errorf("inviter = %+v (present=%v), want a joined non-self member", inviter, ok)
	}
}

func TestChatInfoFromWorldItemInvitedSpaceSkipsSelfAsInviter(t *testing.T) {
	info := chatInfoFromWorldItem(invitedWorldItem("space1", 100, string(ownID)), ownID)
	if len(info.Members.MemberMap) != 1 || info.Members.MemberMap[ownID].Membership != event.MembershipInvite {
		t.Errorf("MemberMap = %+v, want only self as invited", info.Members.MemberMap)
	}
}

func TestChatInfoFromWorldItemJoinedSpaceUnchanged(t *testing.T) {
	info := chatInfoFromWorldItem(worldItem("space1", 100), ownID)
	if info.Members != nil || !info.CanBackfill {
		t.Errorf("joined space: Members = %+v, CanBackfill = %v; want nil, true", info.Members, info.CanBackfill)
	}
}

// --- syncChats -------------------------------------------------------------

func TestSyncChatsRecordsPendingInvitesBeforeQueueing(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{PendingInvites: []string{"stale"}})
	var saves int
	var gc *GChatClient
	var pendingAtFirstQueue []string
	var queued []*simplevent.ChatResync
	gc = &GChatClient{
		UserLogin: login,
		Main:      &GChatConnector{Config: *newTestConfig(t)},
		saveFn:    func(context.Context) error { saves++; return nil },
		paginatedWorldFn: func(context.Context, *pb.PaginatedWorldRequest) (*pb.PaginatedWorldResponse, error) {
			return &pb.PaginatedWorldResponse{WorldItems: []*pb.WorldItemLite{
				worldItem("joined", 300), invitedWorldItem("zeta", 200, ""), invitedWorldItem("alpha", 100, ""),
			}}, nil
		},
		queueChatResyncFn: func(evt *simplevent.ChatResync) bridgev2.EventHandlingResult {
			if queued == nil {
				pendingAtFirstQueue = slices.Clone(pendingInvites(gc))
			}
			queued = append(queued, evt)
			return bridgev2.EventHandlingResultQueued
		},
	}
	gc.setSyncInProgress(true)

	gc.syncChats(context.Background())

	want := []string{"alpha", "zeta"}
	if got := pendingInvites(gc); !slices.Equal(got, want) {
		t.Errorf("PendingInvites = %v, want %v (stale entry replaced, sorted)", got, want)
	}
	if !slices.Equal(pendingAtFirstQueue, want) {
		t.Errorf("PendingInvites at first queue = %v, want %v already recorded", pendingAtFirstQueue, want)
	}
	if saves == 0 {
		t.Error("changed pending set was not saved")
	}
	if len(queued) != 3 {
		t.Fatalf("len(queued) = %d, want 3 (joined + both invites)", len(queued))
	}
}

// --- HandleMatrixMembership: answering a pending invite --------------------

func newInviteTestClient(pending ...string) (*GChatClient, chan *simplevent.ChatResync) {
	resyncs := make(chan *simplevent.ChatResync, 1)
	return &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{PendingInvites: pending}),
		saveFn:    func(context.Context) error { return nil },
		queueChatResyncFn: func(evt *simplevent.ChatResync) bridgev2.EventHandlingResult {
			resyncs <- evt
			return bridgev2.EventHandlingResultQueued
		},
	}, resyncs
}

func TestAcceptPendingInviteJoinsAndResyncs(t *testing.T) {
	gc, resyncs := newInviteTestClient("space1", "space2")
	var gotReq *pb.CreateMembershipRequest
	gc.createMembershipFn = func(_ context.Context, req *pb.CreateMembershipRequest) (*pb.CreateMembershipResponse, error) {
		gotReq = req
		return &pb.CreateMembershipResponse{}, nil
	}

	if _, err := gc.HandleMatrixMembership(context.Background(),
		membershipChange(spacePortal("space1"), gc.UserLogin, bridgev2.AcceptInvite)); err != nil {
		t.Fatalf("HandleMatrixMembership(AcceptInvite) error = %v", err)
	}

	if gotReq == nil {
		t.Fatal("create_membership was not sent for a pending invite")
	}
	if got := gotReq.GetGroupId().GetSpaceId().GetSpaceId(); got != "space1" {
		t.Errorf("group_id space = %q, want space1", got)
	}
	if ids := gotReq.GetMemberIds(); len(ids) != 1 || ids[0].GetUserId().GetId() != "112233" {
		t.Errorf("member_ids = %v, want exactly the own gaia id (purple's join shape)", ids)
	}
	if len(gotReq.GetInviteeMemberInfos()) != 0 {
		t.Error("invitee_member_infos set; a self-join must not use the invite-others shape")
	}
	if got := pendingInvites(gc); !slices.Equal(got, []string{"space2"}) {
		t.Errorf("PendingInvites = %v, want [space2]", got)
	}
	select {
	case evt := <-resyncs:
		if evt.GetChatInfoFunc == nil || evt.LatestMessageTS.IsZero() {
			t.Errorf("resync = %+v, want GetChatInfoFunc + LatestMessageTS (members + backfill)", evt)
		}
		if want := gcid.MakePortalKey(gcid.GroupID{ID: "space1"}, gc.UserLogin.ID); evt.PortalKey != want {
			t.Errorf("resync PortalKey = %+v, want %+v", evt.PortalKey, want)
		}
	case <-time.After(time.Second):
		t.Error("accepting did not queue a resync of the space")
	}
}

func TestRejectPendingInviteRemovesSelf(t *testing.T) {
	gc, resyncs := newInviteTestClient("space1")
	var gotReq *pb.RemoveMembershipsRequest
	gc.removeMembershipsFn = func(_ context.Context, req *pb.RemoveMembershipsRequest) (*pb.RemoveMembershipsResponse, error) {
		gotReq = req
		return &pb.RemoveMembershipsResponse{}, nil
	}

	if _, err := gc.HandleMatrixMembership(context.Background(),
		membershipChange(spacePortal("space1"), gc.UserLogin, bridgev2.RejectInvite)); err != nil {
		t.Fatalf("HandleMatrixMembership(RejectInvite) error = %v", err)
	}

	if gotReq == nil {
		t.Fatal("remove_memberships was not sent for a declined pending invite")
	}
	if ids := gotReq.GetMemberIds(); len(ids) != 1 || ids[0].GetUserId().GetId() != "112233" {
		t.Errorf("member_ids = %v, want exactly the own gaia id", ids)
	}
	if got := pendingInvites(gc); len(got) != 0 {
		t.Errorf("PendingInvites = %v, want empty", got)
	}
	select {
	case evt := <-resyncs:
		t.Errorf("declining queued a resync: %+v", evt)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestAnswerPendingInviteFailureKeepsItPending(t *testing.T) {
	gc, _ := newInviteTestClient("space1")
	gc.createMembershipFn = func(context.Context, *pb.CreateMembershipRequest) (*pb.CreateMembershipResponse, error) {
		return nil, errors.New("boom")
	}

	if _, err := gc.HandleMatrixMembership(context.Background(),
		membershipChange(spacePortal("space1"), gc.UserLogin, bridgev2.AcceptInvite)); err == nil {
		t.Fatal("HandleMatrixMembership(AcceptInvite) = nil error on RPC failure, want the error surfaced")
	}
	if got := pendingInvites(gc); !slices.Equal(got, []string{"space1"}) {
		t.Errorf("PendingInvites = %v, want [space1] kept for a retry", got)
	}
}

// The bridge's own auto-accept of a joined space's portal is an AcceptInvite
// too; with no pending invite for that space it must stay a no-op, even when
// OTHER spaces have invites pending.
func TestAcceptWithoutPendingInviteSendsNothing(t *testing.T) {
	gc, _ := newInviteTestClient("other-space")
	gc.createMembershipFn = func(context.Context, *pb.CreateMembershipRequest) (*pb.CreateMembershipResponse, error) {
		t.Error("create_membership sent for a space with no pending invite")
		return &pb.CreateMembershipResponse{}, nil
	}

	if _, err := gc.HandleMatrixMembership(context.Background(),
		membershipChange(spacePortal("space1"), gc.UserLogin, bridgev2.AcceptInvite)); err != nil {
		t.Fatalf("HandleMatrixMembership(AcceptInvite) error = %v, want nil", err)
	}
}

// --- SYSTEM_MESSAGE: answered in another Google Chat client ---------------

func TestSystemMessageSelfJoinClearsPendingInvite(t *testing.T) {
	gc, _ := newEventTestClient("112233")
	gc.UserLogin.Metadata.(*UserLoginMetadata).PendingInvites = []string{"space-1", "space-2"}
	gc.saveFn = func(context.Context) error { return nil }

	gc.handleGChatEvent(context.Background(), systemMessageEvent(spaceGroupID("space-1"), "sysmsg-1", "112233", 1700000000123456,
		membershipChangedAnnotation(pb.MembershipChangedMetadata_JOINED, "112233")))

	if got := pendingInvites(gc); !slices.Equal(got, []string{"space-2"}) {
		t.Errorf("PendingInvites = %v, want [space-2]", got)
	}
}

func TestSystemMessageOtherUserJoinKeepsPendingInvite(t *testing.T) {
	gc, _ := newEventTestClient("112233")
	gc.UserLogin.Metadata.(*UserLoginMetadata).PendingInvites = []string{"space-1"}

	gc.handleGChatEvent(context.Background(), systemMessageEvent(spaceGroupID("space-1"), "sysmsg-1", "55555", 1700000000123456,
		membershipChangedAnnotation(pb.MembershipChangedMetadata_JOINED, "55555")))

	if got := pendingInvites(gc); !slices.Equal(got, []string{"space-1"}) {
		t.Errorf("PendingInvites = %v, want [space-1] untouched", got)
	}
}
