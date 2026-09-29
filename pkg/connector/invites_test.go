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

// ownMembershipEvent is a standalone MembershipChangedEvent for one member of
// a group, shaped like the one Google pushes to an invitee.
func ownMembershipEvent(groupID *pb.GroupId, gaia string, state pb.MembershipState, category pb.InviteCategory) *pb.Event {
	return &pb.Event{
		Type: pb.Event_MEMBERSHIP_CHANGED.Enum(),
		Body: &pb.Event_EventBody{
			EventType: pb.Event_MEMBERSHIP_CHANGED.Enum(),
			Type: &pb.Event_EventBody_MembershipChanged{MembershipChanged: &pb.MembershipChangedEvent{
				NewMembership: &pb.Membership{
					Id: &pb.MembershipId{
						MemberId: &pb.MemberId{Id: &pb.MemberId_UserId{UserId: userIDProto(gaia)}},
						GroupId:  groupID,
					},
					MembershipState: state.Enum(),
					InviteCategory:  category.Enum(),
				},
			}},
		},
	}
}

func pendingInvites(gc *GChatClient) []string {
	return gc.UserLogin.Metadata.(*UserLoginMetadata).PendingInvites
}

func newEventInviteClient(pending ...string) (*GChatClient, *[]*simplevent.ChatResync) {
	var resyncs []*simplevent.ChatResync
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{PendingInvites: pending}),
		saveFn:    func(context.Context) error { return nil },
		queueChatResyncFn: func(evt *simplevent.ChatResync) bridgev2.EventHandlingResult {
			resyncs = append(resyncs, evt)
			return bridgev2.EventHandlingResultQueued
		},
	}
	return gc, &resyncs
}

// --- MembershipChangedEvent: the user's own invite -------------------------

func TestOwnInviteEventCreatesInvitedPortal(t *testing.T) {
	gc, resyncs := newEventInviteClient()

	res := gc.handleGChatEvent(context.Background(), ownMembershipEvent(spaceGroupID("space1"), "112233",
		pb.MembershipState_MEMBER_INVITED, pb.InviteCategory_REGULAR_INVITE))

	if !res.Success {
		t.Fatalf("handleGChatEvent() = %+v, want Success", res)
	}
	if got := pendingInvites(gc); !slices.Equal(got, []string{"space1"}) {
		t.Errorf("PendingInvites = %v, want [space1]", got)
	}
	if len(*resyncs) != 1 {
		t.Fatalf("queued %d resyncs, want 1", len(*resyncs))
	}
	evt := (*resyncs)[0]
	if !evt.CreatePortal {
		t.Error("CreatePortal = false; the invited space has no portal yet")
	}
	if want := gcid.MakePortalKey(gcid.GroupID{ID: "space1"}, gc.UserLogin.ID); evt.PortalKey != want {
		t.Errorf("PortalKey = %+v, want %+v", evt.PortalKey, want)
	}
	if evt.GetChatInfoFunc == nil {
		t.Fatal("GetChatInfoFunc = nil, want the invited-space info builder")
	}
}

func TestOwnInviteEventIgnoresSpamOtherUsersAndDMs(t *testing.T) {
	cases := map[string]*pb.Event{
		"spam": ownMembershipEvent(spaceGroupID("space1"), "112233",
			pb.MembershipState_MEMBER_INVITED, pb.InviteCategory_SPAM_INVITE),
		"other user": ownMembershipEvent(spaceGroupID("space1"), "55555",
			pb.MembershipState_MEMBER_INVITED, pb.InviteCategory_REGULAR_INVITE),
		"dm": ownMembershipEvent(dmGroupID("dm1"), "112233",
			pb.MembershipState_MEMBER_INVITED, pb.InviteCategory_REGULAR_INVITE),
	}
	for name, evt := range cases {
		t.Run(name, func(t *testing.T) {
			gc, resyncs := newEventInviteClient()
			if res := gc.handleGChatEvent(context.Background(), evt); !res.Success {
				t.Fatalf("handleGChatEvent() = %+v, want Success (ignored)", res)
			}
			if len(*resyncs) != 0 || len(pendingInvites(gc)) != 0 {
				t.Errorf("resyncs = %d, pending = %v; want nothing", len(*resyncs), pendingInvites(gc))
			}
		})
	}
}

func TestOwnMembershipAnsweredElsewhereClearsPending(t *testing.T) {
	for _, state := range []pb.MembershipState{pb.MembershipState_MEMBER_JOINED, pb.MembershipState_MEMBER_NOT_A_MEMBER} {
		t.Run(state.String(), func(t *testing.T) {
			gc, resyncs := newEventInviteClient("space1", "space2")
			gc.handleGChatEvent(context.Background(), ownMembershipEvent(spaceGroupID("space1"), "112233",
				state, pb.InviteCategory_UNKNOWN_INVITE))
			if got := pendingInvites(gc); !slices.Equal(got, []string{"space2"}) {
				t.Errorf("PendingInvites = %v, want [space2]", got)
			}
			if len(*resyncs) != 0 {
				t.Errorf("queued %d resyncs, want 0", len(*resyncs))
			}
		})
	}
}

// --- invitedSpaceChatInfo --------------------------------------------------

func TestInvitedSpaceChatInfoNeverJoinsTheUser(t *testing.T) {
	gc, _ := newEventInviteClient()
	gc.getGroupFn = func(context.Context, *pb.GetGroupRequest) (*pb.GetGroupResponse, error) {
		// get_group may list the invitee among the members; it must not be
		// what decides the user's membership.
		return &pb.GetGroupResponse{
			Group: &pb.Group{Name: proto.String("MyTestSpace3")},
			Memberships: []*pb.Membership{{Id: &pb.MembershipId{
				MemberId: &pb.MemberId{Id: &pb.MemberId_UserId{UserId: userIDProto("112233")}},
			}}},
		}, nil
	}

	info, err := gc.invitedSpaceChatInfo(context.Background(), spacePortal("space1"))
	if err != nil {
		t.Fatalf("invitedSpaceChatInfo() error = %v", err)
	}
	if info.Name == nil || *info.Name != "MyTestSpace3" {
		t.Errorf("Name = %v, want the get_group name", info.Name)
	}
	if info.CanBackfill {
		t.Error("CanBackfill = true, want false (history is unreadable until accepted)")
	}
	own := gc.ownUserID()
	self, ok := info.Members.MemberMap[own]
	if !ok || self.Membership != event.MembershipInvite || !self.IsFromMe || self.PrevMembership != event.MembershipLeave {
		t.Errorf("self = %+v (present=%v), want an IsFromMe invite with PrevMembership=leave", self, ok)
	}
	if info.Members.IsFull || len(info.Members.MemberMap) != 1 {
		t.Errorf("Members = %+v, want only self and not IsFull", info.Members)
	}
}

func TestInvitedSpaceChatInfoSurvivesGetGroupFailure(t *testing.T) {
	gc, _ := newEventInviteClient()
	gc.getGroupFn = func(context.Context, *pb.GetGroupRequest) (*pb.GetGroupResponse, error) {
		return nil, errors.New("403")
	}

	info, err := gc.invitedSpaceChatInfo(context.Background(), spacePortal("space1"))
	if err != nil {
		t.Fatalf("invitedSpaceChatInfo() error = %v, want the invite created unnamed", err)
	}
	if info.Members.MemberMap[gc.ownUserID()].Membership != event.MembershipInvite {
		t.Errorf("Members = %+v, want self invited", info.Members)
	}
}

// --- syncChats: only ever removes pending invites --------------------------

func TestSyncChatsDropsOnlyJoinedPendingInvites(t *testing.T) {
	gc, _ := newEventInviteClient("accepted-elsewhere", "still-pending")
	gc.Main = &GChatConnector{Config: *newTestConfig(t)}
	gc.paginatedWorldFn = func(context.Context, *pb.PaginatedWorldRequest) (*pb.PaginatedWorldResponse, error) {
		return &pb.PaginatedWorldResponse{WorldItems: []*pb.WorldItemLite{worldItem("accepted-elsewhere", 100)}}, nil
	}
	gc.setSyncInProgress(true)

	gc.syncChats(context.Background())

	if got := pendingInvites(gc); !slices.Equal(got, []string{"still-pending"}) {
		t.Errorf("PendingInvites = %v, want [still-pending] (absence from the world means nothing)", got)
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
