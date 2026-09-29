package connector

// leave_test.go -- the leave command and HandleMatrixDeleteChat (#66).

import (
	"context"
	"errors"
	"strings"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/event"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// leaveFixture is a connector whose user has the given logins in the chat,
// recording every Google Chat call and whether the Matrix removal ran.
type leaveFixture struct {
	gc      *GChatConnector
	user    *bridgev2.User
	logins  []*GChatClient
	removed [][]*GChatClient
	calls   []string
}

func newLeaveFixture(t *testing.T, names ...string) *leaveFixture {
	t.Helper()
	f := &leaveFixture{
		gc:   &GChatConnector{},
		user: &bridgev2.User{User: &database.User{MXID: "@me:example.org"}},
	}
	for i, name := range names {
		login := newTestUserLogin(&UserLoginMetadata{})
		login.ID = gcid.MakeUserLoginID(string(rune('1'+i)) + "00")
		login.RemoteName = name
		client := &GChatClient{UserLogin: login, Main: f.gc}
		client.removeMembershipsFn = func(_ context.Context, req *pb.RemoveMembershipsRequest) (*pb.RemoveMembershipsResponse, error) {
			f.calls = append(f.calls, "remove:"+req.GetGroupId().GetSpaceId().GetSpaceId()+":"+req.GetMemberIds()[0].GetUserId().GetId())
			return &pb.RemoveMembershipsResponse{}, nil
		}
		client.hideGroupFn = func(_ context.Context, req *pb.HideGroupRequest) (*pb.HideGroupResponse, error) {
			if !req.GetHide() {
				t.Error("hide_group sent with hide=false")
			}
			f.calls = append(f.calls, "hide:"+req.GetId().GetDmId().GetDmId())
			return &pb.HideGroupResponse{}, nil
		}
		f.logins = append(f.logins, client)
	}
	f.gc.leavingLoginsFn = func(context.Context, *bridgev2.Portal, *bridgev2.User) ([]*GChatClient, error) {
		return f.logins, nil
	}
	f.gc.removeFromPortalFn = func(_ context.Context, _ *bridgev2.Portal, _ *bridgev2.User, logins []*GChatClient) error {
		f.removed = append(f.removed, logins)
		return nil
	}
	return f
}

func namedSpacePortal(id, name string) *bridgev2.Portal {
	p := spacePortal(id)
	p.Name = name
	return p
}

func TestLeaveCommandOnlyDescribesUntilConfirmed(t *testing.T) {
	f := newLeaveFixture(t, "akumul", "Abigél")

	reply := f.gc.runLeaveCommand(context.Background(), namedSpacePortal("space1", "Team"), f.user, nil)

	if len(f.calls) != 0 || len(f.removed) != 0 {
		t.Fatalf("calls = %v, removed = %d; the first leave must not act", f.calls, len(f.removed))
	}
	for _, want := range []string{"**Team**", "akumul", "Abigél", "leave confirm"} {
		if !strings.Contains(reply, want) {
			t.Errorf("preview %q does not mention %q", reply, want)
		}
	}
}

func TestLeaveCommandConfirmLeavesWithEveryLogin(t *testing.T) {
	f := newLeaveFixture(t, "akumul", "Abigél")

	reply := f.gc.runLeaveCommand(context.Background(), namedSpacePortal("space1", "Team"), f.user, []string{"confirm"})

	want := []string{"remove:space1:100", "remove:space1:200"}
	if strings.Join(f.calls, " ") != strings.Join(want, " ") {
		t.Errorf("calls = %v, want %v (each login removes its own id)", f.calls, want)
	}
	if len(f.removed) != 1 || len(f.removed[0]) != 2 {
		t.Errorf("Matrix removal ran %d times, want once with both logins", len(f.removed))
	}
	if !strings.Contains(reply, "Left **Team** on Google Chat as akumul, Abigél") {
		t.Errorf("reply = %q", reply)
	}
}

func TestLeaveCommandSkipConfirmationActsImmediately(t *testing.T) {
	f := newLeaveFixture(t, "akumul")
	f.gc.Config.SkipLeaveConfirmation = true

	f.gc.runLeaveCommand(context.Background(), namedSpacePortal("space1", "Team"), f.user, nil)

	if len(f.calls) != 1 || len(f.removed) != 1 {
		t.Errorf("calls = %v, removed = %d; with skip_leave_confirmation the first leave acts", f.calls, len(f.removed))
	}
}

// One refusing login keeps the user in the room: removing them would make
// the room vanish while a login is still a member, and the next sync would
// bring it back anyway.
func TestLeaveCommandPartialFailureKeepsTheRoom(t *testing.T) {
	f := newLeaveFixture(t, "akumul", "Abigél")
	f.logins[1].removeMembershipsFn = func(context.Context, *pb.RemoveMembershipsRequest) (*pb.RemoveMembershipsResponse, error) {
		return nil, errors.New("400")
	}

	reply := f.gc.runLeaveCommand(context.Background(), namedSpacePortal("space1", "Team"), f.user, []string{"confirm"})

	if len(f.removed) != 0 {
		t.Error("removed from the Matrix room although a login is still a member")
	}
	for _, want := range []string{"Abigél", "Left as akumul", "still in this room", "only manager"} {
		if !strings.Contains(reply, want) {
			t.Errorf("reply %q does not mention %q", reply, want)
		}
	}
}

func TestLeaveCommandHidesADM(t *testing.T) {
	f := newLeaveFixture(t, "akumul")

	reply := f.gc.runLeaveCommand(context.Background(), dmPortal("dm1"), f.user, []string{"confirm"})

	if len(f.calls) != 1 || f.calls[0] != "hide:dm1" {
		t.Errorf("calls = %v, want [hide:dm1] (a DM cannot be left, only hidden)", f.calls)
	}
	if len(f.removed) != 1 {
		t.Error("not removed from the Matrix room after hiding the DM")
	}
	if !strings.Contains(reply, "Hid this DM") {
		t.Errorf("reply = %q", reply)
	}
}

func TestLeaveCommandWithoutLogins(t *testing.T) {
	f := newLeaveFixture(t)

	reply := f.gc.runLeaveCommand(context.Background(), namedSpacePortal("space1", "Team"), f.user, []string{"confirm"})

	if !strings.Contains(reply, "None of your Google Chat logins") {
		t.Errorf("reply = %q", reply)
	}
}

func TestLeaveCommandReportsFailedRoomRemoval(t *testing.T) {
	f := newLeaveFixture(t, "akumul")
	f.gc.removeFromPortalFn = func(context.Context, *bridgev2.Portal, *bridgev2.User, []*GChatClient) error {
		return errors.New("M_FORBIDDEN")
	}

	reply := f.gc.runLeaveCommand(context.Background(), namedSpacePortal("space1", "Team"), f.user, []string{"confirm"})

	if !strings.Contains(reply, "Left **Team**") || !strings.Contains(reply, "removing you from this room failed: M_FORBIDDEN") {
		t.Errorf("reply = %q, want the leave reported and the removal failure named", reply)
	}
}

func matrixDeleteChat(portal *bridgev2.Portal, forEveryone bool) *bridgev2.MatrixDeleteChat {
	return &bridgev2.MatrixDeleteChat{
		Content: &event.BeeperChatDeleteEventContent{DeleteForEveryone: forEveryone},
		Portal:  portal,
	}
}

// delete-chat is an explicit request: it leaves without the confirmation
// step, with every login, like `leave confirm`.
func TestDeleteChatLeavesWithoutConfirmation(t *testing.T) {
	f := newLeaveFixture(t, "akumul", "Abigél")
	f.logins[0].UserLogin.User = f.user

	if err := f.logins[0].HandleMatrixDeleteChat(context.Background(), matrixDeleteChat(namedSpacePortal("space1", "Team"), false)); err != nil {
		t.Fatalf("HandleMatrixDeleteChat() error = %v", err)
	}
	if len(f.calls) != 2 || len(f.removed) != 1 {
		t.Errorf("calls = %v, removed = %d; want both logins to leave and the room removal", f.calls, len(f.removed))
	}
}

func TestDeleteChatForEveryoneIsRefused(t *testing.T) {
	f := newLeaveFixture(t, "akumul")
	f.logins[0].UserLogin.User = f.user

	if err := f.logins[0].HandleMatrixDeleteChat(context.Background(), matrixDeleteChat(namedSpacePortal("space1", "Team"), true)); err == nil {
		t.Error("delete for everyone accepted; there is no RPC for it")
	}
	if len(f.calls) != 0 {
		t.Errorf("calls = %v, want none", f.calls)
	}
}

func TestDeleteChatReportsAFailedLogin(t *testing.T) {
	f := newLeaveFixture(t, "akumul")
	f.logins[0].UserLogin.User = f.user
	f.logins[0].removeMembershipsFn = func(context.Context, *pb.RemoveMembershipsRequest) (*pb.RemoveMembershipsResponse, error) {
		return nil, errors.New("400")
	}

	if err := f.logins[0].HandleMatrixDeleteChat(context.Background(), matrixDeleteChat(namedSpacePortal("space1", "Team"), false)); err == nil {
		t.Error("HandleMatrixDeleteChat() = nil error although the leave failed")
	}
	if len(f.removed) != 0 {
		t.Error("removed from the Matrix room after a failed leave")
	}
}
