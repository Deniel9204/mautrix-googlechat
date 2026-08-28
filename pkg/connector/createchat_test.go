package connector

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

func dmResponse(dmID string, memberGaias ...string) *pb.CreateDmResponse {
	resp := &pb.CreateDmResponse{
		Dm: &pb.Group{GroupId: &pb.GroupId{
			Id: &pb.GroupId_DmId{DmId: &pb.DmId{DmId: proto.String(dmID)}},
		}},
	}
	for _, gaia := range memberGaias {
		resp.Memberships = append(resp.Memberships, &pb.Membership{
			Id: &pb.MembershipId{MemberId: &pb.MemberId{
				Id: &pb.MemberId_UserId{UserId: &pb.UserId{Id: proto.String(gaia)}},
			}},
		})
	}
	return resp
}

// --- ResolveIdentifier ----------------------------------------------------

// TestResolveIdentifierGaiaWithoutCreatingMakesNoRequest: a gaia id is
// self-describing, so resolving one must not cost a round trip -- and must
// certainly not open a conversation the caller did not ask for.
func TestResolveIdentifierGaiaWithoutCreatingMakesNoRequest(t *testing.T) {
	called := false
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		createDmFn: func(context.Context, *pb.CreateDmRequest) (*pb.CreateDmResponse, error) {
			called = true
			return dmResponse("dm1"), nil
		},
	}

	resp, err := gc.ResolveIdentifier(context.Background(), "112233", false)
	if err != nil {
		t.Fatalf("ResolveIdentifier: %v", err)
	}
	if resp.UserID != gcid.MakeUserID("112233") {
		t.Errorf("UserID = %q, want %q", resp.UserID, gcid.MakeUserID("112233"))
	}
	if resp.Chat != nil {
		t.Error("a resolve-only call returned a chat")
	}
	if called {
		t.Error("create_dm was sent for a resolve-only call")
	}
}

func TestResolveIdentifierGaiaWithCreateOpensDM(t *testing.T) {
	var gotReq *pb.CreateDmRequest
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		createDmFn: func(_ context.Context, req *pb.CreateDmRequest) (*pb.CreateDmResponse, error) {
			gotReq = req
			return dmResponse("dm1", "112233"), nil
		},
	}

	resp, err := gc.ResolveIdentifier(context.Background(), "445566", true)
	if err != nil {
		t.Fatalf("ResolveIdentifier: %v", err)
	}
	if len(gotReq.GetMembers()) != 1 || gotReq.GetMembers()[0].GetId() != "445566" {
		t.Errorf("members = %v, want the gaia id in members (not invitees)", gotReq.GetMembers())
	}
	if len(gotReq.GetInvitees()) != 0 {
		t.Errorf("invitees = %v, want none for a known gaia id", gotReq.GetInvitees())
	}
	if resp.Chat == nil {
		t.Fatal("Chat = nil, want the created DM")
	}
	wantKey := gcid.MakePortalKey(gcid.GroupID{ID: "dm1", IsDM: true}, gc.UserLogin.ID)
	if resp.Chat.PortalKey != wantKey {
		t.Errorf("PortalKey = %+v, want %+v", resp.Chat.PortalKey, wantKey)
	}
}

// TestResolveIdentifierEmailWithoutCreatingIsRefused: there is no
// email-to-gaia lookup, so the only way to answer would be to open the DM.
// Doing that silently, for a call that merely asked to resolve, would create
// a conversation the user never requested.
func TestResolveIdentifierEmailWithoutCreatingIsRefused(t *testing.T) {
	called := false
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		createDmFn: func(context.Context, *pb.CreateDmRequest) (*pb.CreateDmResponse, error) {
			called = true
			return dmResponse("dm1"), nil
		},
	}

	_, err := gc.ResolveIdentifier(context.Background(), "someone@example.com", false)
	if !errors.Is(err, ErrCannotResolveEmailWithoutCreating) {
		t.Fatalf("error = %v, want ErrCannotResolveEmailWithoutCreating", err)
	}
	if called {
		t.Error("create_dm was sent for a resolve-only call on an email")
	}
}

// TestResolveIdentifierEmailWithCreateUsesInviteesAndLearnsGaia: the email
// goes in invitees (members only takes gaia ids), and the response's own
// membership list is what finally reveals who the address belonged to.
func TestResolveIdentifierEmailWithCreateUsesInviteesAndLearnsGaia(t *testing.T) {
	const selfGaia = "112233" // matches newTestUserLogin's id
	var gotReq *pb.CreateDmRequest
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		createDmFn: func(_ context.Context, req *pb.CreateDmRequest) (*pb.CreateDmResponse, error) {
			gotReq = req
			return dmResponse("dm7", selfGaia, "998877"), nil
		},
	}

	resp, err := gc.ResolveIdentifier(context.Background(), "mailto:someone@example.com", true)
	if err != nil {
		t.Fatalf("ResolveIdentifier: %v", err)
	}
	if len(gotReq.GetInvitees()) != 1 || gotReq.GetInvitees()[0].GetEmail() != "someone@example.com" {
		t.Errorf("invitees = %v, want the mailto:-stripped address", gotReq.GetInvitees())
	}
	if len(gotReq.GetMembers()) != 0 {
		t.Errorf("members = %v, want none for an email", gotReq.GetMembers())
	}
	if want := gcid.MakeUserID("998877"); resp.UserID != want {
		t.Errorf("UserID = %q, want %q (the member who is not this login)", resp.UserID, want)
	}
}

func TestResolveIdentifierRejectsGarbage(t *testing.T) {
	gc := &GChatClient{UserLogin: newTestUserLogin(&UserLoginMetadata{})}
	for _, id := range []string{"", "   ", "not-an-email-or-id"} {
		if _, err := gc.ResolveIdentifier(context.Background(), id, true); err == nil {
			t.Errorf("ResolveIdentifier(%q) = nil error, want a rejection", id)
		}
	}
}

// --- CreateChatWithGhost --------------------------------------------------

func TestCreateChatWithGhostUsesGhostIDAsGaia(t *testing.T) {
	var gotReq *pb.CreateDmRequest
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		createDmFn: func(_ context.Context, req *pb.CreateDmRequest) (*pb.CreateDmResponse, error) {
			gotReq = req
			return dmResponse("dm2"), nil
		},
	}

	resp, err := gc.CreateChatWithGhost(context.Background(), ghostWithID("778899"))
	if err != nil {
		t.Fatalf("CreateChatWithGhost: %v", err)
	}
	if len(gotReq.GetMembers()) != 1 || gotReq.GetMembers()[0].GetId() != "778899" {
		t.Errorf("members = %v, want the ghost's id used directly as the gaia id", gotReq.GetMembers())
	}
	wantKey := gcid.MakePortalKey(gcid.GroupID{ID: "dm2", IsDM: true}, gc.UserLogin.ID)
	if resp.PortalKey != wantKey {
		t.Errorf("PortalKey = %+v, want %+v", resp.PortalKey, wantKey)
	}
}

func TestCreateChatWithGhostRejectsUnidentified(t *testing.T) {
	gc := &GChatClient{UserLogin: newTestUserLogin(&UserLoginMetadata{})}
	if _, err := gc.CreateChatWithGhost(context.Background(), nil); err == nil {
		t.Error("CreateChatWithGhost(nil) = nil error, want a rejection")
	}
}

// --- CreateGroup ----------------------------------------------------------

func TestCreateGroupSendsNameAndParticipants(t *testing.T) {
	var gotReq *pb.CreateGroupRequest
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		createGroupChatFn: func(_ context.Context, req *pb.CreateGroupRequest) (*pb.CreateGroupResponse, error) {
			gotReq = req
			return &pb.CreateGroupResponse{Group: &pb.Group{GroupId: &pb.GroupId{
				Id: &pb.GroupId_SpaceId{SpaceId: &pb.SpaceId{SpaceId: proto.String("space9")}},
			}}}, nil
		},
	}

	resp, err := gc.CreateGroup(context.Background(), &bridgev2.GroupCreateParams{
		Name:         &event.RoomNameEventContent{Name: "Team"},
		Participants: []networkid.UserID{"111", "222"},
	})
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	info := gotReq.GetSpace()
	if info.GetName() != "Team" {
		t.Errorf("name = %q, want %q", info.GetName(), "Team")
	}
	if len(info.GetInviteeMemberInfos()) != 2 {
		t.Errorf("invitee_member_infos = %d entries, want 2", len(info.GetInviteeMemberInfos()))
	}
	// Deliberately unset: the caller asked for a NEW space, so quietly
	// returning a pre-existing one would be a surprising answer.
	if gotReq.GetShouldFindExistingSpace() {
		t.Error("should_find_existing_space is set, want unset for an explicit create")
	}
	wantKey := gcid.MakePortalKey(gcid.GroupID{ID: "space9", IsDM: false}, gc.UserLogin.ID)
	if resp.PortalKey != wantKey {
		t.Errorf("PortalKey = %+v, want %+v", resp.PortalKey, wantKey)
	}
}

// TestCreateGroupRequiresAName: Google Chat spaces are named, unlike DMs, so
// a nameless create would fail server-side; refuse it before the round trip.
func TestCreateGroupRequiresAName(t *testing.T) {
	called := false
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		createGroupChatFn: func(context.Context, *pb.CreateGroupRequest) (*pb.CreateGroupResponse, error) {
			called = true
			return nil, nil
		},
	}
	for _, params := range []*bridgev2.GroupCreateParams{
		{},
		{Name: &event.RoomNameEventContent{Name: "   "}},
	} {
		if _, err := gc.CreateGroup(context.Background(), params); err == nil {
			t.Errorf("CreateGroup(%+v) = nil error, want a rejection", params)
		}
	}
	if called {
		t.Error("create_group was sent without a name")
	}
}

// ghostWithID builds the minimal ghost CreateChatWithGhost needs: its id,
// which IS the gaia id (gcid.MakeUserID is an identity mapping).
func ghostWithID(gaia string) *bridgev2.Ghost {
	return &bridgev2.Ghost{Ghost: &database.Ghost{ID: gcid.MakeUserID(gaia)}}
}
