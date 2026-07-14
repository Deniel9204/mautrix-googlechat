package connector

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

func worldItem(id string, sortTS int64) *pb.WorldItemLite {
	return &pb.WorldItemLite{
		GroupId:       spaceGroupID(id),
		SortTimestamp: proto.Int64(sortTS),
		ReadState: &pb.GroupReadState{
			MembershipState: pb.MembershipState_MEMBER_JOINED.Enum(),
		},
	}
}

// --- planChatSync: sort + cap --------------------------------------------

func TestPlanChatSyncSortsBySortTimestampDescending(t *testing.T) {
	items := []*pb.WorldItemLite{
		worldItem("oldest", 100),
		worldItem("newest", 300),
		worldItem("middle", 200),
	}

	plan := planChatSync(items, 10)

	if len(plan) != 3 {
		t.Fatalf("len(plan) = %d, want 3", len(plan))
	}
	wantOrder := []string{"newest", "middle", "oldest"}
	for i, want := range wantOrder {
		id, _, _ := groupIDPlain(plan[i].Item.GetGroupId())
		if id != want {
			t.Errorf("plan[%d] = %q, want %q", i, id, want)
		}
	}
}

func TestPlanChatSyncCapsCreatePortalAtInitialChatSync(t *testing.T) {
	items := []*pb.WorldItemLite{
		worldItem("a", 500),
		worldItem("b", 400),
		worldItem("c", 300),
		worldItem("d", 200),
		worldItem("e", 100),
	}

	plan := planChatSync(items, 2)

	if len(plan) != 5 {
		t.Fatalf("len(plan) = %d, want 5 (nothing is dropped, only CreatePortal differs)", len(plan))
	}
	wantCreate := []bool{true, true, false, false, false}
	for i, want := range wantCreate {
		if plan[i].CreatePortal != want {
			t.Errorf("plan[%d].CreatePortal = %v, want %v", i, plan[i].CreatePortal, want)
		}
	}
}

// TestPlanChatSyncSkippedItemConsumesCapSlot pins the fix for the cap
// arithmetic bug the gchat-port-auditor caught: user.py's loop
// (user.py:628-641) enumerates the FULL sorted list and a skipped
// (blocked/hidden/not-joined) item still advances the index for everything
// after it -- `continue` doesn't "collapse" the index. Filtering before
// indexing (an earlier version of planChatSync did this) shifts the cap
// boundary earlier by one slot per skipped item ahead of the boundary.
func TestPlanChatSyncSkippedItemConsumesCapSlot(t *testing.T) {
	blocked := worldItem("blocked", 500) // newest -> absolute sorted position 0
	blocked.ReadState.Blocked = proto.Bool(true)
	items := []*pb.WorldItemLite{
		blocked,
		worldItem("a", 400), // absolute sorted position 1
		worldItem("b", 300), // absolute sorted position 2
	}

	plan := planChatSync(items, 2)

	if len(plan) != 2 {
		t.Fatalf("len(plan) = %d, want 2 (blocked item emits nothing at all)", len(plan))
	}
	idA, _, _ := groupIDPlain(plan[0].Item.GetGroupId())
	if idA != "a" || !plan[0].CreatePortal {
		t.Errorf("plan[0] = {%q, CreatePortal=%v}, want {\"a\", true} (absolute position 1 < cap 2)", idA, plan[0].CreatePortal)
	}
	idB, _, _ := groupIDPlain(plan[1].Item.GetGroupId())
	if idB != "b" || plan[1].CreatePortal {
		t.Errorf("plan[1] = {%q, CreatePortal=%v}, want {\"b\", false} (absolute position 2, NOT < cap 2 -- \"blocked\" at position 0 still consumed a cap slot)", idB, plan[1].CreatePortal)
	}
}

func TestPlanChatSyncZeroCapMarksEverythingNoCreate(t *testing.T) {
	items := []*pb.WorldItemLite{worldItem("a", 100), worldItem("b", 200)}

	plan := planChatSync(items, 0)

	for i, entry := range plan {
		if entry.CreatePortal {
			t.Errorf("plan[%d].CreatePortal = true, want false (initial_chat_sync=0 disables auto-creation)", i)
		}
	}
}

// --- planChatSync: skip conditions (user.py:630-635) ----------------------

func TestPlanChatSyncSkipsBlockedHiddenAndNotJoined(t *testing.T) {
	blocked := worldItem("blocked", 100)
	blocked.ReadState.Blocked = proto.Bool(true)

	hidden := worldItem("hidden", 200)
	hidden.ReadState.HideTimestamp = proto.Int64(12345)

	notJoined := worldItem("not-joined", 300)
	notJoined.ReadState.MembershipState = pb.MembershipState_MEMBER_INVITED.Enum()

	noReadState := &pb.WorldItemLite{GroupId: spaceGroupID("no-read-state"), SortTimestamp: proto.Int64(50)}

	valid := worldItem("valid", 400)

	plan := planChatSync([]*pb.WorldItemLite{blocked, hidden, notJoined, noReadState, valid}, 10)

	if len(plan) != 1 {
		ids := make([]string, len(plan))
		for i, e := range plan {
			ids[i], _, _ = groupIDPlain(e.Item.GetGroupId())
		}
		t.Fatalf("len(plan) = %d %v, want 1 ([\"valid\"])", len(plan), ids)
	}
	id, _, _ := groupIDPlain(plan[0].Item.GetGroupId())
	if id != "valid" {
		t.Errorf("plan[0] = %q, want \"valid\"", id)
	}
}

func TestPlanChatSyncEmptyInput(t *testing.T) {
	plan := planChatSync(nil, 10)
	if len(plan) != 0 {
		t.Errorf("len(plan) = %d, want 0", len(plan))
	}
}

// groupIDPlain is a tiny local wrapper around gchatmeow.GroupIDToParts kept
// private to this test file so the sort/cap/skip tests above can assert on
// which item ended up where without reaching into proto internals directly.
func groupIDPlain(gid *pb.GroupId) (id string, isDM bool, ok bool) {
	if sp := gid.GetSpaceId(); sp != nil {
		return sp.GetSpaceId(), false, true
	}
	if dm := gid.GetDmId(); dm != nil {
		return dm.GetDmId(), true, true
	}
	return "", false, false
}

// --- syncChats: no-conn no-op (RPC path itself covered at Task 13) -------

func TestSyncChatsNoConnIsNoop(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	gc := &GChatClient{UserLogin: login, Main: &GChatConnector{Config: *newTestConfig(t)}}

	// Must not panic despite UserLogin.Bridge being nil (no full bridgev2
	// harness in this test) -- syncChats should return before ever reaching
	// UserLogin.QueueRemoteEvent.
	gc.syncChats(context.Background())
}
