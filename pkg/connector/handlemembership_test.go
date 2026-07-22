package connector

// handlemembership_test.go -- HandleMatrixMembership -> create_membership /
// remove_memberships RPCs. Asserts each membership-change type routes to the
// right endpoint with the right target, mirroring the request-construction
// test shape of the other outbound handlers.

import (
	"context"
	"errors"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/event"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

func ghostTarget(gaia string) *bridgev2.Ghost {
	return &bridgev2.Ghost{Ghost: &database.Ghost{ID: gcid.MakeUserID(gaia)}}
}

func membershipChange(portal *bridgev2.Portal, target bridgev2.GhostOrUserLogin, typ bridgev2.MembershipChangeType) *bridgev2.MatrixMembershipChange {
	return &bridgev2.MatrixMembershipChange{
		MatrixRoomMeta: bridgev2.MatrixRoomMeta[*event.MemberEventContent]{
			MatrixEventBase: bridgev2.MatrixEventBase[*event.MemberEventContent]{
				Content: &event.MemberEventContent{},
				Portal:  portal,
			},
		},
		Target: target,
		Type:   typ,
	}
}

func TestHandleMatrixMembershipInviteSendsCreateMembership(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.CreateMembershipRequest
	gc := &GChatClient{
		UserLogin: login,
		createMembershipFn: func(_ context.Context, req *pb.CreateMembershipRequest) (*pb.CreateMembershipResponse, error) {
			gotReq = req
			return &pb.CreateMembershipResponse{}, nil
		},
	}

	_, err := gc.HandleMatrixMembership(context.Background(), membershipChange(spacePortal("space1"), ghostTarget("999888"), bridgev2.Invite))
	if err != nil {
		t.Fatalf("HandleMatrixMembership(Invite) error = %v, want nil", err)
	}
	if gotReq == nil {
		t.Fatal("createMembershipFn was not called")
	}
	if got := gotReq.GetGroupId().GetSpaceId().GetSpaceId(); got != "space1" {
		t.Errorf("group_id space = %q, want %q", got, "space1")
	}
	infos := gotReq.GetInviteeMemberInfos()
	if len(infos) != 1 {
		t.Fatalf("invitee_member_infos len = %d, want 1", len(infos))
	}
	if got := infos[0].GetInviteeInfo().GetUserId().GetId(); got != "999888" {
		t.Errorf("invitee user id = %q, want %q", got, "999888")
	}
}

func TestHandleMatrixMembershipKickSendsRemoveMemberships(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.RemoveMembershipsRequest
	gc := &GChatClient{
		UserLogin: login,
		removeMembershipsFn: func(_ context.Context, req *pb.RemoveMembershipsRequest) (*pb.RemoveMembershipsResponse, error) {
			gotReq = req
			return &pb.RemoveMembershipsResponse{}, nil
		},
	}

	_, err := gc.HandleMatrixMembership(context.Background(), membershipChange(spacePortal("space1"), ghostTarget("999888"), bridgev2.Kick))
	if err != nil {
		t.Fatalf("HandleMatrixMembership(Kick) error = %v, want nil", err)
	}
	if gotReq == nil {
		t.Fatal("removeMembershipsFn was not called")
	}
	if got := gotReq.GetGroupId().GetSpaceId().GetSpaceId(); got != "space1" {
		t.Errorf("group_id space = %q, want %q", got, "space1")
	}
	ids := gotReq.GetMemberIds()
	if len(ids) != 1 || ids[0].GetUserId().GetId() != "999888" {
		t.Errorf("member_ids = %v, want one member with id 999888", ids)
	}
	if gotReq.GetMembershipState() != pb.MembershipState_MEMBER_INVITED {
		t.Errorf("membership_state = %v, want MEMBER_INVITED", gotReq.GetMembershipState())
	}
}

func TestHandleMatrixMembershipLeaveRemovesOwnID(t *testing.T) {
	// newTestUserLogin's own gaia id is 112233.
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.RemoveMembershipsRequest
	gc := &GChatClient{
		UserLogin: login,
		removeMembershipsFn: func(_ context.Context, req *pb.RemoveMembershipsRequest) (*pb.RemoveMembershipsResponse, error) {
			gotReq = req
			return &pb.RemoveMembershipsResponse{}, nil
		},
	}

	// For a self-leave the target is the user themselves; the handler uses its
	// own login id regardless.
	_, err := gc.HandleMatrixMembership(context.Background(), membershipChange(spacePortal("space1"), ghostTarget("112233"), bridgev2.Leave))
	if err != nil {
		t.Fatalf("HandleMatrixMembership(Leave) error = %v, want nil", err)
	}
	if gotReq == nil {
		t.Fatal("removeMembershipsFn was not called")
	}
	ids := gotReq.GetMemberIds()
	if len(ids) != 1 || ids[0].GetUserId().GetId() != "112233" {
		t.Errorf("member_ids = %v, want own id 112233", ids)
	}
}

// A UserLogin target (not a ghost) must also resolve to its gaia id.
func TestHandleMatrixMembershipUserLoginTarget(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.RemoveMembershipsRequest
	gc := &GChatClient{
		UserLogin: login,
		removeMembershipsFn: func(_ context.Context, req *pb.RemoveMembershipsRequest) (*pb.RemoveMembershipsResponse, error) {
			gotReq = req
			return &pb.RemoveMembershipsResponse{}, nil
		},
	}
	target := &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: gcid.MakeUserLoginID("555444")}}

	_, err := gc.HandleMatrixMembership(context.Background(), membershipChange(spacePortal("space1"), target, bridgev2.Kick))
	if err != nil {
		t.Fatalf("HandleMatrixMembership(Kick, UserLogin target) error = %v, want nil", err)
	}
	if gotReq.GetMemberIds()[0].GetUserId().GetId() != "555444" {
		t.Errorf("member id = %q, want 555444 (from UserLogin target)", gotReq.GetMemberIds()[0].GetUserId().GetId())
	}
}

func TestHandleMatrixMembershipDMRejected(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	called := false
	gc := &GChatClient{
		UserLogin: login,
		createMembershipFn: func(context.Context, *pb.CreateMembershipRequest) (*pb.CreateMembershipResponse, error) {
			called = true
			return &pb.CreateMembershipResponse{}, nil
		},
	}

	_, err := gc.HandleMatrixMembership(context.Background(), membershipChange(dmPortal("dm1"), ghostTarget("999888"), bridgev2.Invite))
	if err == nil {
		t.Error("HandleMatrixMembership() on a DM error = nil, want an error")
	}
	if called {
		t.Error("an RPC was sent for a DM membership change, want none")
	}
}

func TestHandleMatrixMembershipUnsupportedTypeErrors(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	gc := &GChatClient{
		UserLogin: login,
		createMembershipFn: func(context.Context, *pb.CreateMembershipRequest) (*pb.CreateMembershipResponse, error) {
			return &pb.CreateMembershipResponse{}, nil
		},
		removeMembershipsFn: func(context.Context, *pb.RemoveMembershipsRequest) (*pb.RemoveMembershipsResponse, error) {
			return &pb.RemoveMembershipsResponse{}, nil
		},
	}

	// BanJoined has no Google Chat equivalent.
	_, err := gc.HandleMatrixMembership(context.Background(), membershipChange(spacePortal("space1"), ghostTarget("999888"), bridgev2.BanJoined))
	if err == nil {
		t.Error("HandleMatrixMembership(BanJoined) error = nil, want unsupported error")
	}
}

func TestHandleMatrixMembershipRPCFailurePropagates(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	gc := &GChatClient{
		UserLogin: login,
		createMembershipFn: func(context.Context, *pb.CreateMembershipRequest) (*pb.CreateMembershipResponse, error) {
			return nil, errors.New("403 permission denied")
		},
	}

	_, err := gc.HandleMatrixMembership(context.Background(), membershipChange(spacePortal("space1"), ghostTarget("999888"), bridgev2.Invite))
	if err == nil {
		t.Error("HandleMatrixMembership() error = nil on RPC failure, want the error propagated")
	}
}
