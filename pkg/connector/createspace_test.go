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
	"maunium.net/go/mautrix/id"

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

// The command's own Matrix room must become the portal, bound before anyone is
// invited (provisionutil would otherwise create a second room).
func TestCreateGroupBindsCommandRoomBeforeInviting(t *testing.T) {
	var order []string
	bound := &bridgev2.Portal{}
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		createGroupChatFn: func(context.Context, *pb.CreateGroupRequest) (*pb.CreateGroupResponse, error) {
			return createdSpace("newspace"), nil
		},
		ensureBotPowerFn: func(context.Context, id.RoomID) error { return nil },
		bindCreatedSpaceRoomFn: func(_ context.Context, key networkid.PortalKey, params *bridgev2.GroupCreateParams) (*bridgev2.Portal, error) {
			order = append(order, "bind")
			if key.ID != gcid.MakePortalID(gcid.GroupID{ID: "newspace"}) || params.RoomID != "!room:example.org" {
				t.Errorf("bind(%+v, %q), want the new space and the command's room", key, params.RoomID)
			}
			return bound, nil
		},
		createMembershipFn: func(context.Context, *pb.CreateMembershipRequest) (*pb.CreateMembershipResponse, error) {
			order = append(order, "invite")
			return &pb.CreateMembershipResponse{}, nil
		},
	}
	params := spaceParams("Team", "111")
	params.RoomID = "!room:example.org"

	resp, err := gc.CreateGroup(context.Background(), params)
	if err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	if len(order) != 2 || order[0] != "bind" || order[1] != "invite" {
		t.Errorf("order = %v, want [bind invite]", order)
	}
	if resp.Portal != bound {
		t.Error("response Portal is not the bound portal; provisionutil would create a new room")
	}
}

func TestCreateGroupWithoutRoomDoesNotBind(t *testing.T) {
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		createGroupChatFn: func(context.Context, *pb.CreateGroupRequest) (*pb.CreateGroupResponse, error) {
			return createdSpace("newspace"), nil
		},
		ensureBotPowerFn: func(context.Context, id.RoomID) error { return nil },
		bindCreatedSpaceRoomFn: func(context.Context, networkid.PortalKey, *bridgev2.GroupCreateParams) (*bridgev2.Portal, error) {
			t.Error("bound a room although the request named none")
			return nil, nil
		},
	}
	if _, err := gc.CreateGroup(context.Background(), spaceParams("Team")); err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
}

func TestCreateGroupBindFailureIsReported(t *testing.T) {
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		createGroupChatFn: func(context.Context, *pb.CreateGroupRequest) (*pb.CreateGroupResponse, error) {
			return createdSpace("newspace"), nil
		},
		ensureBotPowerFn: func(context.Context, id.RoomID) error { return nil },
		bindCreatedSpaceRoomFn: func(context.Context, networkid.PortalKey, *bridgev2.GroupCreateParams) (*bridgev2.Portal, error) {
			return nil, errors.New("room is already a portal")
		},
	}
	params := spaceParams("Team")
	params.RoomID = "!room:example.org"
	if _, err := gc.CreateGroup(context.Background(), params); err == nil {
		t.Error("CreateGroup() = nil error after the room could not be bound")
	}
}

// The bot's power is settled before the space exists: if it cannot be, no
// space is created at all.
func TestCreateGroupPromotesBotBeforeCreating(t *testing.T) {
	var order []string
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		ensureBotPowerFn: func(_ context.Context, roomID id.RoomID) error {
			order = append(order, "power:"+string(roomID))
			return nil
		},
		createGroupChatFn: func(context.Context, *pb.CreateGroupRequest) (*pb.CreateGroupResponse, error) {
			order = append(order, "create")
			return createdSpace("newspace"), nil
		},
		bindCreatedSpaceRoomFn: func(context.Context, networkid.PortalKey, *bridgev2.GroupCreateParams) (*bridgev2.Portal, error) {
			return &bridgev2.Portal{}, nil
		},
	}
	params := spaceParams("Team")
	params.RoomID = "!room:example.org"

	if _, err := gc.CreateGroup(context.Background(), params); err != nil {
		t.Fatalf("CreateGroup() error = %v", err)
	}
	if len(order) != 2 || order[0] != "power:!room:example.org" || order[1] != "create" {
		t.Errorf("order = %v, want the bot promoted first", order)
	}
}

func TestCreateGroupPowerFailureCreatesNothing(t *testing.T) {
	gc := &GChatClient{
		UserLogin:        newTestUserLogin(&UserLoginMetadata{}),
		ensureBotPowerFn: func(context.Context, id.RoomID) error { return errors.New("no double puppet") },
		createGroupChatFn: func(context.Context, *pb.CreateGroupRequest) (*pb.CreateGroupResponse, error) {
			t.Error("space created although the bot could not be given power")
			return createdSpace("x"), nil
		},
	}
	params := spaceParams("Team")
	params.RoomID = "!room:example.org"
	if _, err := gc.CreateGroup(context.Background(), params); err == nil {
		t.Error("CreateGroup() = nil error, want the power failure")
	}
}

func TestPromoteBot(t *testing.T) {
	const bot, user = id.UserID("@bot:hs"), id.UserID("@me:hs")
	levels := func(botLevel, userLevel int) *event.PowerLevelsEventContent {
		pl := &event.PowerLevelsEventContent{}
		pl.SetUserLevel(bot, botLevel)
		pl.SetUserLevel(user, userLevel)
		return pl
	}

	if changed, err := promoteBot(levels(100, 100), bot, user, true); changed || err != nil {
		t.Errorf("bot already at 100: changed=%v err=%v, want no change", changed, err)
	}
	if _, err := promoteBot(levels(0, 100), bot, user, false); err == nil {
		t.Error("no double puppet: want an error telling the user to promote the bot")
	}
	if _, err := promoteBot(levels(0, 50), bot, user, true); err == nil {
		t.Error("user at 50: want an error (cannot grant more than own level)")
	}
	pl := levels(0, 100)
	if changed, err := promoteBot(pl, bot, user, true); !changed || err != nil || pl.GetUserLevel(bot) != 100 {
		t.Errorf("promote: changed=%v err=%v bot=%d, want the bot raised to 100", changed, err, pl.GetUserLevel(bot))
	}
	if pl.GetUserLevel(user) != 100 {
		t.Error("promoting the bot changed the user's own level")
	}
}
