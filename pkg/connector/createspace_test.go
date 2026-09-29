package connector

// createspace_test.go -- CreateGroup -> create_group (web client shape) +
// create_membership per participant.

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

func createdSpace(id string) *pb.CreateGroupResponse {
	return &pb.CreateGroupResponse{Group: &pb.Group{GroupId: spaceGroupID(id)}}
}

func spaceParams(name string, participants ...networkid.UserID) *bridgev2.GroupCreateParams {
	p := &bridgev2.GroupCreateParams{Type: spaceGroupType, Participants: participants}
	if name != "" {
		p.Name = &event.RoomNameEventContent{Name: name}
	}
	return p
}

func TestCreateGroupSendsWebShapeThenInvites(t *testing.T) {
	var gotCreate *pb.CreateGroupRequest
	var invited []string
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		createGroupChatFn: func(_ context.Context, req *pb.CreateGroupRequest) (*pb.CreateGroupResponse, error) {
			gotCreate = req
			return createdSpace("newspace"), nil
		},
		createMembershipFn: func(_ context.Context, req *pb.CreateMembershipRequest) (*pb.CreateMembershipResponse, error) {
			if got := req.GetGroupId().GetSpaceId().GetSpaceId(); got != "newspace" {
				t.Errorf("invite into %q, want newspace", got)
			}
			invited = append(invited, req.GetInviteeMemberInfos()[0].GetInviteeInfo().GetUserId().GetId())
			return &pb.CreateMembershipResponse{}, nil
		},
	}

	resp, err := gc.CreateGroup(context.Background(), spaceParams("Team", "111", "222"))
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}

	space := gotCreate.GetSpace()
	if space.GetName() != "Team" {
		t.Errorf("name = %q, want Team", space.GetName())
	}
	if space.GetAttributeCheckerGroupType() != pb.SharedAttributeCheckerGroupType_FLAT_ROOM {
		t.Errorf("attribute_checker_group_type = %v, want FLAT_ROOM (the room type the web client sends)", space.GetAttributeCheckerGroupType())
	}
	if len(space.GetInviteeMemberInfos()) != 0 {
		t.Error("invitees inside create_group; the web client creates the space empty")
	}
	if len(gotCreate.GetLocalId()) != 11 {
		t.Errorf("local_id = %q, want an 11-character id like the web client's", gotCreate.GetLocalId())
	}
	if gotCreate.ShouldFindExistingSpace == nil || gotCreate.GetShouldFindExistingSpace() {
		t.Error("should_find_existing_space not explicitly false")
	}
	if len(gotCreate.ProtoReflect().GetUnknown()) != 0 || len(space.ProtoReflect().GetUnknown()) != 0 {
		t.Error("first attempt carries extra wire fields; it must be the minimal shape")
	}
	if len(invited) != 2 || invited[0] != "111" || invited[1] != "222" {
		t.Errorf("invited = %v, want [111 222]", invited)
	}
	if want := gcid.MakePortalKey(gcid.GroupID{ID: "newspace"}, gc.UserLogin.ID); resp.PortalKey != want {
		t.Errorf("PortalKey = %+v, want %+v", resp.PortalKey, want)
	}
	if len(resp.FailedParticipants) != 0 {
		t.Errorf("FailedParticipants = %v, want none", resp.FailedParticipants)
	}
}

func TestCreateGroupNeedsName(t *testing.T) {
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		createGroupChatFn: func(context.Context, *pb.CreateGroupRequest) (*pb.CreateGroupResponse, error) {
			t.Error("create_group sent without a name")
			return createdSpace("x"), nil
		},
	}
	if _, err := gc.CreateGroup(context.Background(), spaceParams("")); err == nil {
		t.Error("CreateGroup(no name) = nil error, want a refusal")
	}
}

// A 400 creates nothing, so the next shape is tried -- with the web client's
// extra wire fields, which must survive marshalling.
func TestCreateGroupRetriesRefusalWithWebExtras(t *testing.T) {
	var attempts []*pb.CreateGroupRequest
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		createGroupChatFn: func(_ context.Context, req *pb.CreateGroupRequest) (*pb.CreateGroupResponse, error) {
			attempts = append(attempts, req)
			if len(attempts) == 1 {
				return nil, &gchatmeow.UnexpectedStatusError{Status: 400}
			}
			return createdSpace("newspace"), nil
		},
	}

	if _, err := gc.CreateGroup(context.Background(), spaceParams("Team")); err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	if len(attempts) != 2 {
		t.Fatalf("attempts = %d, want 2", len(attempts))
	}
	second := attempts[1]
	if second.GetLocalId() != attempts[0].GetLocalId() {
		t.Error("retry changed local_id; it is the same logical create")
	}
	wire, err := proto.Marshal(second)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var round pb.CreateGroupRequest
	if err := proto.Unmarshal(wire, &round); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(round.ProtoReflect().GetUnknown()) == 0 || len(round.GetSpace().ProtoReflect().GetUnknown()) == 0 {
		t.Error("retry lost the web client's extra fields on the wire")
	}
	if round.GetSpace().GetAttributeCheckerGroupType() != pb.SharedAttributeCheckerGroupType_FLAT_ROOM {
		t.Error("retry lost the room type")
	}
}

// After a 5xx or a transport error the space may already exist: no retry.
func TestCreateGroupNeverRetriesAmbiguousFailure(t *testing.T) {
	for name, failure := range map[string]error{
		"5xx":       &gchatmeow.UnexpectedStatusError{Status: 500},
		"transport": errors.New("connection reset"),
	} {
		t.Run(name, func(t *testing.T) {
			attempts := 0
			gc := &GChatClient{
				UserLogin: newTestUserLogin(&UserLoginMetadata{}),
				createGroupChatFn: func(context.Context, *pb.CreateGroupRequest) (*pb.CreateGroupResponse, error) {
					attempts++
					return nil, failure
				},
			}
			if _, err := gc.CreateGroup(context.Background(), spaceParams("Team")); err == nil {
				t.Error("CreateGroup() = nil error, want the failure")
			}
			if attempts != 1 {
				t.Errorf("attempts = %d, want 1 (a retry could create a duplicate space)", attempts)
			}
		})
	}
}

func TestCreateGroupReportsFailedInviteButKeepsSpace(t *testing.T) {
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		createGroupChatFn: func(context.Context, *pb.CreateGroupRequest) (*pb.CreateGroupResponse, error) {
			return createdSpace("newspace"), nil
		},
		createMembershipFn: func(_ context.Context, req *pb.CreateMembershipRequest) (*pb.CreateMembershipResponse, error) {
			if req.GetInviteeMemberInfos()[0].GetInviteeInfo().GetUserId().GetId() == "bad" {
				return nil, errors.New("400")
			}
			return &pb.CreateMembershipResponse{}, nil
		},
	}

	resp, err := gc.CreateGroup(context.Background(), spaceParams("Team", "good", "bad"))
	if err != nil {
		t.Fatalf("CreateGroup() error = %v, want the space kept", err)
	}
	if len(resp.FailedParticipants) != 1 || resp.FailedParticipants["bad"] == nil {
		t.Errorf("FailedParticipants = %v, want only bad", resp.FailedParticipants)
	}
}
