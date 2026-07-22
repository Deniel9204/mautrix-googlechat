package connector

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/simplevent"

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

// --- syncChats: retry + latch reset on paginated_world failure ------------
//
// These pin the M1 whole-branch review fix: shouldSyncOnConnect
// (client.go) latches initialSyncDone=true BEFORE syncChats runs, so if the
// paginated_world RPC fails, the one-time chat-list sync used to never
// retry -- a single transient blip at first connect left the bridge
// CONNECTED with zero portals until a restart. syncChats must now (a) retry
// a bounded number of times before giving up, and (b) reset the latch on
// final failure so the next Connected transition (e.g. a webchannel
// reconnect) tries again.

// TestSyncChatsResetsLatchOnFailure RED-verifies the latch-reset half of the
// fix: without it, shouldSyncOnConnect stays permanently consumed after a
// failed sync and this test fails.
func TestSyncChatsResetsLatchOnFailure(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var calls int
	gc := &GChatClient{
		UserLogin:            login,
		Main:                 &GChatConnector{Config: *newTestConfig(t)},
		syncRetryBackoffBase: time.Millisecond,
		paginatedWorldFn: func(context.Context, *pb.PaginatedWorldRequest) (*pb.PaginatedWorldResponse, error) {
			calls++
			return nil, errors.New("paginated_world: boom")
		},
	}

	if !gc.shouldSyncOnConnect() {
		t.Fatal("shouldSyncOnConnect() = false on first call, want true (test setup)")
	}

	gc.syncChats(context.Background())

	if calls < 2 {
		t.Errorf("paginatedWorldFn called %d times, want a bounded retry (>1)", calls)
	}
	if !gc.shouldSyncOnConnect() {
		t.Error("shouldSyncOnConnect() = false after syncChats exhausted retries, want true (latch must be reset so a later Connected transition retries the sync instead of leaving the bridge permanently unsynced)")
	}
}

// TestSyncChatsRetriesThenSucceeds covers the common case the bug report
// calls out: the webchannel stays up (so no new Connected transition ever
// fires) but a single paginated_world RPC blips. A bounded in-process retry
// must absorb that without needing a whole new conn.
func TestSyncChatsRetriesThenSucceeds(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var queued []*simplevent.ChatResync
	var calls int
	gc := &GChatClient{
		UserLogin:            login,
		Main:                 &GChatConnector{Config: *newTestConfig(t)},
		syncRetryBackoffBase: time.Millisecond,
		paginatedWorldFn: func(context.Context, *pb.PaginatedWorldRequest) (*pb.PaginatedWorldResponse, error) {
			calls++
			if calls < 3 {
				return nil, errors.New("paginated_world: transient")
			}
			return &pb.PaginatedWorldResponse{WorldItems: []*pb.WorldItemLite{worldItem("a", 100)}}, nil
		},
		queueChatResyncFn: func(evt *simplevent.ChatResync) bridgev2.EventHandlingResult {
			queued = append(queued, evt)
			return bridgev2.EventHandlingResultQueued
		},
	}

	if !gc.shouldSyncOnConnect() {
		t.Fatal("shouldSyncOnConnect() = false on first call, want true (test setup)")
	}

	gc.syncChats(context.Background())

	if calls != 3 {
		t.Fatalf("paginatedWorldFn called %d times, want 3 (fail, fail, succeed)", calls)
	}
	if len(queued) != 1 {
		t.Fatalf("len(queued) = %d ChatResync events, want 1", len(queued))
	}
	if gc.shouldSyncOnConnect() {
		t.Error("shouldSyncOnConnect() = true after a retry that eventually succeeded, want false (latch stays consumed on success)")
	}
}

// TestSyncChatsRequestIncludesWorldSection guards the fix for the empty
// world-sync bug: Google's paginated_world returns world_items only when the
// request carries at least one world_section_requests entry with a page_size
// (verified live 2026-07-22 -- without one the server returns a ~2-byte stub
// and syncChats sees zero chats, so new conversations never auto-create
// portals). fetchWorldWithRetry must always send one.
func TestSyncChatsRequestIncludesWorldSection(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.PaginatedWorldRequest
	gc := &GChatClient{
		UserLogin: login,
		Main:      &GChatConnector{Config: *newTestConfig(t)},
		paginatedWorldFn: func(_ context.Context, req *pb.PaginatedWorldRequest) (*pb.PaginatedWorldResponse, error) {
			gotReq = req
			return &pb.PaginatedWorldResponse{}, nil
		},
		queueChatResyncFn: func(*simplevent.ChatResync) bridgev2.EventHandlingResult {
			return bridgev2.EventHandlingResultQueued
		},
	}

	gc.syncChats(context.Background())

	if gotReq == nil {
		t.Fatal("paginatedWorldFn was never called")
	}
	sections := gotReq.GetWorldSectionRequests()
	if len(sections) == 0 {
		t.Fatal("paginated_world request has no world_section_requests -- the server returns an empty world without one (empty world-sync bug)")
	}
	if ps := sections[0].GetPageSize(); ps <= 0 {
		t.Errorf("world_section_requests[0].page_size = %d, want > 0", ps)
	}
}

// TestSyncChatsSuccessKeepsLatch is the control case: an immediately
// successful sync must not retry, must queue every planned entry, and must
// leave the one-time latch consumed (unlike the two failure tests above).
func TestSyncChatsSuccessKeepsLatch(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var queued []*simplevent.ChatResync
	var calls int
	gc := &GChatClient{
		UserLogin: login,
		Main:      &GChatConnector{Config: *newTestConfig(t)},
		paginatedWorldFn: func(context.Context, *pb.PaginatedWorldRequest) (*pb.PaginatedWorldResponse, error) {
			calls++
			return &pb.PaginatedWorldResponse{WorldItems: []*pb.WorldItemLite{worldItem("a", 100), worldItem("b", 50)}}, nil
		},
		queueChatResyncFn: func(evt *simplevent.ChatResync) bridgev2.EventHandlingResult {
			queued = append(queued, evt)
			return bridgev2.EventHandlingResultQueued
		},
	}

	if !gc.shouldSyncOnConnect() {
		t.Fatal("shouldSyncOnConnect() = false on first call, want true (test setup)")
	}

	gc.syncChats(context.Background())

	if calls != 1 {
		t.Errorf("paginatedWorldFn called %d times, want 1 (no retry needed on first-try success)", calls)
	}
	if len(queued) != 2 {
		t.Fatalf("len(queued) = %d ChatResync events, want 2", len(queued))
	}
	if gc.shouldSyncOnConnect() {
		t.Error("shouldSyncOnConnect() = true after a successful sync, want false (latch stays consumed, matching the pre-existing one-sync-per-conn behavior)")
	}
}
