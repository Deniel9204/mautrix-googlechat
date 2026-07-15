package connector

// backfill_test.go -- reconnect gap-recovery via catch_up_user (M2 Task 7).
// Mirrors sync_test.go/events_test.go's seam-capture test pattern: a bare
// *GChatClient (no full bridgev2.Bridge+DB harness, see newTestUserLogin in
// client_test.go) with catchUpUserFn/queueRemoteEventFn overridden instead
// of a live gchatmeow.Client connection or UserLogin.Bridge.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// --- catchUp: request shape carries the stored watermark -------------------

func TestCatchUpCallsCatchUpUserWithStoredRevision(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{Revision: 555})
	var gotReq *pb.CatchUpUserRequest
	gc := &GChatClient{
		UserLogin: login,
		catchUpUserFn: func(_ context.Context, req *pb.CatchUpUserRequest) (*pb.CatchUpResponse, error) {
			gotReq = req
			return &pb.CatchUpResponse{Status: pb.CatchUpResponse_COMPLETED.Enum()}, nil
		},
	}

	gc.catchUp(context.Background())

	if gotReq == nil {
		t.Fatal("catchUpUserFn was not called")
	}
	if got := gotReq.GetRange().GetFromRevisionTimestamp(); got != 555 {
		t.Errorf("Range.FromRevisionTimestamp = %d, want 555 (stored UserLoginMetadata.Revision)", got)
	}
}

func TestCatchUpNoConnNoConnFnIsNoop(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	gc := &GChatClient{UserLogin: login}

	// Must not panic despite UserLogin.Bridge being nil and no live conn --
	// catchUp should return before ever calling QueueRemoteEvent.
	gc.catchUp(context.Background())
}

// TestCatchUpSkippedWhileFirstSyncInProgress pins the gchat-port-auditor P1
// fix: shouldSyncOnConnect's latch is consumed synchronously, before
// syncChats' spawned goroutine has even started, so a reconnect landing in
// that window must not race an unfinished first sync with a meaningless
// (still probably zero) watermark.
func TestCatchUpSkippedWhileFirstSyncInProgress(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var catchUpCalls int
	gc := &GChatClient{
		UserLogin: login,
		catchUpUserFn: func(context.Context, *pb.CatchUpUserRequest) (*pb.CatchUpResponse, error) {
			catchUpCalls++
			return &pb.CatchUpResponse{Status: pb.CatchUpResponse_COMPLETED.Enum()}, nil
		},
	}
	gc.setSyncInProgress(true)

	gc.catchUp(context.Background())

	if catchUpCalls != 0 {
		t.Errorf("catchUpUserFn called %d times, want 0 (must defer while this conn's first sync is still running)", catchUpCalls)
	}
}

// TestCatchUpDrainsAllPagesInOneInvocation pins the reviewer's Important:
// catchUp must LOOP on PAGINATED until COMPLETED, draining the FULL backlog
// in a single invocation rather than one page per reconnect. If it stopped
// after page 1, a concurrent live event (whose revision is higher than the
// whole gap) would advance the watermark past the un-drained pages, which
// would then never be re-requested -> silent message loss. Scripts three
// pages (PAGINATED, PAGINATED, COMPLETED) and asserts every page's event is
// dispatched in ONE catchUp call and the watermark ends at the final page's
// revision. Also asserts each subsequent page's request carries the
// advanced cursor (the de-facto continuation token), so the drain actually
// makes forward progress.
func TestCatchUpDrainsAllPagesInOneInvocation(t *testing.T) {
	meta := &UserLoginMetadata{Revision: 100}
	login := newTestUserLogin(meta)
	login.ID = gcid.MakeUserLoginID("112233")

	page := func(msgID string, rev int64, status pb.CatchUpResponse_ResponseStatus) *pb.CatchUpResponse {
		evt := messagePostedEvent(spaceGroupID("space-1"), msgID, "98765", "gap", 1)
		evt.RevisionType = &pb.Event_UserRevision{UserRevision: &pb.WriteRevision{Timestamp: proto.Int64(rev)}}
		return &pb.CatchUpResponse{Status: status.Enum(), Events: []*pb.Event{evt}}
	}
	pages := []*pb.CatchUpResponse{
		page("msg-1", 200, pb.CatchUpResponse_PAGINATED),
		page("msg-2", 300, pb.CatchUpResponse_PAGINATED),
		page("msg-3", 400, pb.CatchUpResponse_COMPLETED),
	}

	var fromRevisions []int64
	var queued []bridgev2.RemoteEvent
	var calls int
	gc := &GChatClient{
		UserLogin: login,
		catchUpUserFn: func(_ context.Context, req *pb.CatchUpUserRequest) (*pb.CatchUpResponse, error) {
			fromRevisions = append(fromRevisions, req.GetRange().GetFromRevisionTimestamp())
			resp := pages[calls]
			calls++
			return resp, nil
		},
		queueRemoteEventFn: func(e bridgev2.RemoteEvent) bridgev2.EventHandlingResult {
			queued = append(queued, e)
			return bridgev2.EventHandlingResultQueued
		},
		saveFn: func(context.Context) error { return nil },
	}

	gc.catchUp(context.Background())

	if calls != 3 {
		t.Fatalf("catchUpUserFn called %d times, want 3 (drain PAGINATED, PAGINATED, then stop on COMPLETED -- all in ONE catchUp invocation)", calls)
	}
	if len(queued) != 3 {
		t.Fatalf("len(queued) = %d, want 3 (every page's event dispatched, not just the first page)", len(queued))
	}
	// Each page's request must carry the previous page's max revision as its
	// from-cursor: 100 (stored watermark) -> 200 -> 300.
	wantFrom := []int64{100, 200, 300}
	for i, want := range wantFrom {
		if fromRevisions[i] != want {
			t.Errorf("page %d request FromRevisionTimestamp = %d, want %d (drain must continue from the advanced cursor)", i, fromRevisions[i], want)
		}
	}
	if meta.Revision != 400 {
		t.Errorf("Metadata.Revision = %d, want 400 (watermark ends at the FINAL drained page's revision)", meta.Revision)
	}
}

// TestCatchUpDrainStopsIfCursorDoesNotAdvance pins the anti-infinite-loop
// guard for the case where a server keeps saying PAGINATED but returns a
// page whose events do not move the revision cursor forward: re-requesting
// from the identical from-revision would just return the same page forever,
// so catchUp stops instead. (The catchUpMaxPages ceiling is the harder
// backstop for a server that DOES advance the cursor but never says
// COMPLETED; this covers the stuck-cursor case in a bounded, fast test.)
func TestCatchUpDrainStopsIfCursorDoesNotAdvance(t *testing.T) {
	meta := &UserLoginMetadata{Revision: 100}
	login := newTestUserLogin(meta)
	login.ID = gcid.MakeUserLoginID("112233")
	// Every call returns PAGINATED with an event whose revision (150) is
	// fixed -- so after the first page advances the cursor 100 -> 150, the
	// second page (still 150) does not advance it, and the drain must stop.
	evt := messagePostedEvent(spaceGroupID("space-1"), "msg-stuck", "98765", "hi", 1)
	evt.RevisionType = &pb.Event_UserRevision{UserRevision: &pb.WriteRevision{Timestamp: proto.Int64(150)}}
	var calls int
	gc := &GChatClient{
		UserLogin: login,
		catchUpUserFn: func(context.Context, *pb.CatchUpUserRequest) (*pb.CatchUpResponse, error) {
			calls++
			return &pb.CatchUpResponse{Status: pb.CatchUpResponse_PAGINATED.Enum(), Events: []*pb.Event{evt}}, nil
		},
		queueRemoteEventFn: func(bridgev2.RemoteEvent) bridgev2.EventHandlingResult {
			return bridgev2.EventHandlingResultQueued
		},
		saveFn: func(context.Context) error { return nil },
	}

	gc.catchUp(context.Background())

	if calls != 2 {
		t.Errorf("catchUpUserFn called %d times, want 2 (page 1 advances the cursor, page 2 does not -> stop; must not loop to catchUpMaxPages=%d)", calls, catchUpMaxPages)
	}
	if meta.Revision != 150 {
		t.Errorf("Metadata.Revision = %d, want 150 (advanced to the one revision actually seen before the drain stalled)", meta.Revision)
	}
}

// --- catchUp: returned events replay through the normal handleGChatEvent --
// path (b) ------------------------------------------------------------------

func TestCatchUpDispatchesEventsThroughHandleGChatEvent(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{Revision: 100})
	login.ID = gcid.MakeUserLoginID("112233")
	evt := messagePostedEvent(spaceGroupID("space-1"), "msg-1", "98765", "hello", 1700000000000000)
	var queued []bridgev2.RemoteEvent
	gc := &GChatClient{
		UserLogin: login,
		catchUpUserFn: func(context.Context, *pb.CatchUpUserRequest) (*pb.CatchUpResponse, error) {
			return &pb.CatchUpResponse{
				Status: pb.CatchUpResponse_COMPLETED.Enum(),
				Events: []*pb.Event{evt},
			}, nil
		},
		queueRemoteEventFn: func(e bridgev2.RemoteEvent) bridgev2.EventHandlingResult {
			queued = append(queued, e)
			return bridgev2.EventHandlingResultQueued
		},
		saveFn: func(context.Context) error { return nil },
	}

	gc.catchUp(context.Background())

	if len(queued) != 1 {
		t.Fatalf("len(queued) = %d, want 1 (caught-up event dispatched through the normal handleGChatEvent path)", len(queued))
	}
	msg, ok := queued[0].(bridgev2.RemoteMessage)
	if !ok {
		t.Fatalf("queued event does not implement bridgev2.RemoteMessage: %T", queued[0])
	}
	if got, want := msg.GetID(), gcid.MakeMessageID("msg-1"); got != want {
		t.Errorf("GetID() = %q, want %q", got, want)
	}
}

// TestCatchUpSplitsMultiBodyEventsBeforeDispatch pins fidelity with
// portal.py:494-495's `for evt in source.client.split_event_bodies(multi_evt)`:
// a raw catch_up_user event can itself carry more than one body (field 8,
// "bodies") exactly like a live StreamEventsResponse event does, and each
// body must become its own dispatched event.
func TestCatchUpSplitsMultiBodyEventsBeforeDispatch(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	login.ID = gcid.MakeUserLoginID("112233")
	first := messagePostedEvent(spaceGroupID("space-1"), "msg-1", "98765", "one", 1)
	second := messagePostedEvent(spaceGroupID("space-1"), "msg-2", "98765", "two", 2)
	multi := first
	multi.Bodies = []*pb.Event_EventBody{second.GetBody()}

	var queued []bridgev2.RemoteEvent
	gc := &GChatClient{
		UserLogin: login,
		catchUpUserFn: func(context.Context, *pb.CatchUpUserRequest) (*pb.CatchUpResponse, error) {
			return &pb.CatchUpResponse{Status: pb.CatchUpResponse_COMPLETED.Enum(), Events: []*pb.Event{multi}}, nil
		},
		queueRemoteEventFn: func(e bridgev2.RemoteEvent) bridgev2.EventHandlingResult {
			queued = append(queued, e)
			return bridgev2.EventHandlingResultQueued
		},
		saveFn: func(context.Context) error { return nil },
	}

	gc.catchUp(context.Background())

	if len(queued) != 2 {
		t.Fatalf("len(queued) = %d, want 2 (multi-body catch-up event split into two dispatches)", len(queued))
	}
}

// --- catchUp: watermark advances only on success (c) ------------------------

func TestCatchUpAdvancesWatermarkOnSuccess(t *testing.T) {
	meta := &UserLoginMetadata{Revision: 100}
	login := newTestUserLogin(meta)
	login.ID = gcid.MakeUserLoginID("112233")
	evt := messagePostedEvent(spaceGroupID("space-1"), "msg-1", "98765", "hello", 1)
	evt.RevisionType = &pb.Event_UserRevision{UserRevision: &pb.WriteRevision{Timestamp: proto.Int64(999)}}
	var saveCount int
	gc := &GChatClient{
		UserLogin: login,
		catchUpUserFn: func(context.Context, *pb.CatchUpUserRequest) (*pb.CatchUpResponse, error) {
			return &pb.CatchUpResponse{Status: pb.CatchUpResponse_COMPLETED.Enum(), Events: []*pb.Event{evt}}, nil
		},
		queueRemoteEventFn: func(bridgev2.RemoteEvent) bridgev2.EventHandlingResult {
			return bridgev2.EventHandlingResultQueued
		},
		saveFn: func(context.Context) error { saveCount++; return nil },
	}

	gc.catchUp(context.Background())

	if meta.Revision != 999 {
		t.Errorf("Metadata.Revision = %d, want 999 (advanced to the caught-up event's user_revision)", meta.Revision)
	}
	if saveCount != 1 {
		t.Errorf("save called %d times, want 1", saveCount)
	}
}

// TestCatchUpNoNewRevisionSkipsSave covers the "nothing new" case: a
// COMPLETED response with zero events (already caught up) must not persist
// anything at all, not even a no-op write of the same value.
func TestCatchUpNoNewRevisionSkipsSave(t *testing.T) {
	meta := &UserLoginMetadata{Revision: 100}
	login := newTestUserLogin(meta)
	var saveCount int
	gc := &GChatClient{
		UserLogin: login,
		catchUpUserFn: func(context.Context, *pb.CatchUpUserRequest) (*pb.CatchUpResponse, error) {
			return &pb.CatchUpResponse{Status: pb.CatchUpResponse_COMPLETED.Enum()}, nil
		},
		saveFn: func(context.Context) error { saveCount++; return nil },
	}

	gc.catchUp(context.Background())

	if meta.Revision != 100 {
		t.Errorf("Metadata.Revision = %d, want 100 (unchanged)", meta.Revision)
	}
	if saveCount != 0 {
		t.Errorf("save called %d times, want 0 (nothing new to persist)", saveCount)
	}
}

// --- catchUp: failure leaves the watermark unchanged (d) --------------------

func TestCatchUpRPCFailureLeavesWatermarkUnchanged(t *testing.T) {
	meta := &UserLoginMetadata{Revision: 100}
	login := newTestUserLogin(meta)
	var saveCount int
	gc := &GChatClient{
		UserLogin: login,
		catchUpUserFn: func(context.Context, *pb.CatchUpUserRequest) (*pb.CatchUpResponse, error) {
			return nil, errors.New("catch_up_user: boom")
		},
		saveFn: func(context.Context) error { saveCount++; return nil },
	}

	gc.catchUp(context.Background())

	if meta.Revision != 100 {
		t.Errorf("Metadata.Revision = %d, want 100 (unchanged on RPC failure -- retried on next reconnect)", meta.Revision)
	}
	if saveCount != 0 {
		t.Errorf("save called %d times, want 0", saveCount)
	}
}

// TestCatchUpAbortedStatusLeavesWatermarkUnchanged covers the response-level
// failure mode (as opposed to a transport-level RPC error above): an
// ABORTED_* status (portal.py:474-480's equivalent check for catch_up_group)
// means the server refused to honor the requested range at all, so nothing
// it returned is safe to replay or advance the watermark to.
func TestCatchUpAbortedStatusLeavesWatermarkUnchanged(t *testing.T) {
	meta := &UserLoginMetadata{Revision: 100}
	login := newTestUserLogin(meta)
	var saveCount, dispatchCount int
	gc := &GChatClient{
		UserLogin: login,
		catchUpUserFn: func(context.Context, *pb.CatchUpUserRequest) (*pb.CatchUpResponse, error) {
			return &pb.CatchUpResponse{
				Status: pb.CatchUpResponse_ABORTED_FROM_REVISION_TOO_OLD.Enum(),
				Events: []*pb.Event{messagePostedEvent(spaceGroupID("space-1"), "msg-1", "98765", "hi", 1)},
			}, nil
		},
		queueRemoteEventFn: func(bridgev2.RemoteEvent) bridgev2.EventHandlingResult {
			dispatchCount++
			return bridgev2.EventHandlingResultQueued
		},
		saveFn: func(context.Context) error { saveCount++; return nil },
	}

	gc.catchUp(context.Background())

	if meta.Revision != 100 {
		t.Errorf("Metadata.Revision = %d, want 100 (unchanged on ABORTED status)", meta.Revision)
	}
	if saveCount != 0 {
		t.Errorf("save called %d times, want 0", saveCount)
	}
	if dispatchCount != 0 {
		t.Errorf("dispatched %d events, want 0 (an ABORTED response's events are not replayed)", dispatchCount)
	}
}

// --- handleConnState wiring: first Connected syncs, reconnect catches up ---
// (a) + (e) -------------------------------------------------------------

// TestHandleConnStateFirstConnectSyncsThenReconnectCatchesUp drives
// handleConnState (client.go) end to end through two Connected transitions
// on the same *GChatClient, exactly like a real fresh-connect followed by a
// webchannel/SID-expiring reconnect. Uses a channel (not a sleep) to
// observe which RPC each spawned goroutine calls, so this is safe under
// `-race` and never flaky on load.
func TestHandleConnStateFirstConnectSyncsThenReconnectCatchesUp(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{Revision: 42})
	calls := make(chan string, 1)
	var gotReq *pb.CatchUpUserRequest
	gc := &GChatClient{
		UserLogin: login,
		Main:      &GChatConnector{Config: *newTestConfig(t)},
		saveFn:    func(context.Context) error { return nil },
		paginatedWorldFn: func(context.Context, *pb.PaginatedWorldRequest) (*pb.PaginatedWorldResponse, error) {
			calls <- "sync"
			return &pb.PaginatedWorldResponse{}, nil
		},
		catchUpUserFn: func(_ context.Context, req *pb.CatchUpUserRequest) (*pb.CatchUpResponse, error) {
			gotReq = req
			calls <- "catchup"
			return &pb.CatchUpResponse{Status: pb.CatchUpResponse_COMPLETED.Enum()}, nil
		},
	}
	ctx := context.Background()

	gc.handleConnState(ctx, gchatmeow.ConnStateConnected, nil) // 1st Connected: chat-list sync

	select {
	case got := <-calls:
		if got != "sync" {
			t.Fatalf("first Connected called %q, want %q", got, "sync")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the first Connected's sync")
	}
	// The channel receive above only proves paginatedWorldFn was CALLED, not
	// that syncChats (and its deferred setSyncInProgress(false), sync.go)
	// has fully RETURNED yet -- catchUp's syncInProgress guard (backfill.go,
	// the gchat-port-auditor P1 fix) would otherwise make the second
	// Connected below race an unfinished first sync. Poll the mutex-guarded
	// flag directly (not a blind sleep) so this stays deterministic under
	// `-race`.
	waitUntilSyncNotInProgress(t, gc, 2*time.Second)

	gc.handleConnState(ctx, gchatmeow.ConnStateConnected, nil) // 2nd Connected: reconnect -> catch-up

	select {
	case got := <-calls:
		if got != "catchup" {
			t.Fatalf("second Connected called %q, want %q (reconnect must catch up, not resync)", got, "catchup")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the second Connected's catch-up")
	}

	if got := gotReq.GetRange().GetFromRevisionTimestamp(); got != 42 {
		t.Errorf("Range.FromRevisionTimestamp = %d, want 42 (stored UserLoginMetadata.Revision)", got)
	}
}

// --- race: catchUp's watermark write vs. a concurrent metadata writer ------

// TestCatchUpRaceWithConcurrentCookiePersist is the regression test for
// concurrent metadata writers on the same *UserLoginMetadata: catchUp's
// watermark advance (updateMetadata, backfill.go) and persistCookies (a
// live Connected callback re-persisting rotated cookies, client.go) both go
// through metaMu. Run under `-race`, this both proves no data race is
// reported and pins the required outcome: both writes land (Revision
// advanced AND Cookies persisted) no matter which goroutine's critical
// section runs first -- mirrors client_test.go's
// TestMetadataRaceLogoutVsPersist for the same metaMu-guarded pattern.
// Looped so a single lucky ordering can't hide a real bug.
func TestCatchUpRaceWithConcurrentCookiePersist(t *testing.T) {
	for i := 0; i < 50; i++ {
		client, err := gchatmeow.NewClient(gchatmeow.ClientOpts{Cookies: fakeCookies()})
		if err != nil {
			t.Fatalf("NewClient: %v", err)
		}
		meta := &UserLoginMetadata{}
		login := newTestUserLogin(meta)
		evt := messagePostedEvent(spaceGroupID("space-1"), "msg-1", "98765", "hi", 1)
		evt.RevisionType = &pb.Event_UserRevision{UserRevision: &pb.WriteRevision{Timestamp: proto.Int64(int64(i + 1))}}
		gc := &GChatClient{
			UserLogin: login,
			conn:      client,
			saveFn:    func(context.Context) error { return nil },
			catchUpUserFn: func(context.Context, *pb.CatchUpUserRequest) (*pb.CatchUpResponse, error) {
				return &pb.CatchUpResponse{Status: pb.CatchUpResponse_COMPLETED.Enum(), Events: []*pb.Event{evt}}, nil
			},
			queueRemoteEventFn: func(bridgev2.RemoteEvent) bridgev2.EventHandlingResult {
				return bridgev2.EventHandlingResultQueued
			},
		}

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			gc.catchUp(context.Background())
		}()
		go func() {
			defer wg.Done()
			gc.persistCookies(context.Background())
		}()
		wg.Wait()

		if meta.Cookies == nil {
			t.Fatalf("iteration %d: Metadata.Cookies = nil, want persisted cookies", i)
		}
		if meta.Revision != int64(i+1) {
			t.Fatalf("iteration %d: Metadata.Revision = %d, want %d", i, meta.Revision, i+1)
		}
	}
}

// waitUntilSyncNotInProgress polls gc.isSyncInProgress() (a metaMu-guarded
// read, not a raw field access) until it goes false or timeout elapses.
// Used instead of a blind sleep so tests that need to wait for a spawned
// syncChats goroutine to fully finish (not just to have been called) stay
// deterministic under `-race`.
func waitUntilSyncNotInProgress(t *testing.T, gc *GChatClient, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !gc.isSyncInProgress() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for syncInProgress to clear")
}

// --- syncChats: clears syncInProgress when it finishes -------------------

// TestSyncChatsClearsSyncInProgressWhenDone pins syncChats' half of the
// gchat-port-auditor P1 fix contract: the CALLER (handleConnState) sets the
// flag true synchronously before spawning syncChats; syncChats must clear it
// (via defer) once it returns, so catchUp's guard stops deferring after the
// first sync is really done. (The synchronous-SET-before-goroutine half is
// pinned separately by TestHandleConnStateSetsSyncInProgressSynchronously.)
func TestSyncChatsClearsSyncInProgressWhenDone(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var sawInProgress bool
	var gc *GChatClient
	gc = &GChatClient{
		UserLogin: login,
		Main:      &GChatConnector{Config: *newTestConfig(t)},
		paginatedWorldFn: func(context.Context, *pb.PaginatedWorldRequest) (*pb.PaginatedWorldResponse, error) {
			sawInProgress = gc.isSyncInProgress()
			return &pb.PaginatedWorldResponse{}, nil
		},
	}

	gc.setSyncInProgress(true) // mirrors handleConnState setting it before the `go`

	gc.syncChats(context.Background())

	if !sawInProgress {
		t.Error("isSyncInProgress() = false DURING syncChats' own RPC call, want true (caller set it, syncChats must not clear it early)")
	}
	if gc.isSyncInProgress() {
		t.Error("isSyncInProgress() = true after syncChats returned, want false (syncChats must clear it via defer)")
	}
}

// TestHandleConnStateSetsSyncInProgressSynchronously proves the flag is set
// BEFORE the sync goroutine is spawned (not inside it): with the
// paginated_world RPC blocked, the spawned syncChats goroutine cannot
// possibly have run its deferred clear, so if isSyncInProgress() is true the
// instant handleConnState returns, the set must have happened synchronously
// on handleConnState's own call. Deterministic (no sleep): the block is
// released only after the assertion.
func TestHandleConnStateSetsSyncInProgressSynchronously(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	release := make(chan struct{})
	gc := &GChatClient{
		UserLogin: login,
		Main:      &GChatConnector{Config: *newTestConfig(t)},
		saveFn:    func(context.Context) error { return nil },
		paginatedWorldFn: func(context.Context, *pb.PaginatedWorldRequest) (*pb.PaginatedWorldResponse, error) {
			<-release // park the sync goroutine so it cannot reach its deferred clear
			return &pb.PaginatedWorldResponse{}, nil
		},
	}

	gc.handleConnState(context.Background(), gchatmeow.ConnStateConnected, nil)

	if !gc.isSyncInProgress() {
		t.Error("isSyncInProgress() = false right after handleConnState returned (with the sync RPC blocked), want true -- the flag must be set synchronously before the sync goroutine is spawned, not inside it")
	}

	close(release)
	waitUntilSyncNotInProgress(t, gc, 2*time.Second)
}

// --- watermark advancement on the live handleGChatEvent path -------------
// Every successfully handled event (not just catch-up replay) moves the
// watermark, mirroring user.py:674-682's on_stream_event (gchat-port-auditor
// P0 fix: without this, the watermark could only ever move forward from
// inside catchUp's own replay, so it would go stale during ordinary live
// traffic and eventually make catch_up_user permanently fail). The USER
// watermark advances ONLY from user_revision; group_revision goes to the
// per-portal watermark instead (M2-review Important #2); and the advance
// happens only AFTER a successful queue (M2-review Important #3).

func TestHandleGChatEventAdvancesRevisionWatermark(t *testing.T) {
	meta := &UserLoginMetadata{Revision: 10}
	login := newTestUserLogin(meta)
	login.ID = gcid.MakeUserLoginID("112233")
	evt := messagePostedEvent(spaceGroupID("space-1"), "msg-1", "98765", "hi", 1)
	evt.RevisionType = &pb.Event_UserRevision{UserRevision: &pb.WriteRevision{Timestamp: proto.Int64(20)}}
	var saveCount int
	var portalSaved bool
	gc := &GChatClient{
		UserLogin: login,
		queueRemoteEventFn: func(bridgev2.RemoteEvent) bridgev2.EventHandlingResult {
			return bridgev2.EventHandlingResultQueued
		},
		saveFn:               func(context.Context) error { saveCount++; return nil },
		savePortalRevisionFn: func(context.Context, networkid.PortalKey, int64) { portalSaved = true },
	}

	gc.handleGChatEvent(context.Background(), evt)

	if meta.Revision != 20 {
		t.Errorf("Metadata.Revision = %d, want 20 (a live event's own user_revision advances the user watermark, not just catch-up replay)", meta.Revision)
	}
	if saveCount != 1 {
		t.Errorf("save called %d times, want 1", saveCount)
	}
	if portalSaved {
		t.Error("savePortalRevisionFn was called for a user_revision-only event, want not called (group_revision is unset)")
	}
}

// TestHandleGChatEventGroupRevisionDoesNotAdvanceUserWatermark RED-verifies
// the M2-whole-branch Important #2 fix: user_revision and group_revision are
// SEPARATE revision spaces. An event carrying only group_revision must NOT
// advance the persisted user watermark (UserLoginMetadata.Revision, the
// catch_up_user cursor) -- folding it in could over-advance past
// not-yet-delivered user-stream events -> permanent loss. It must instead be
// parked on the per-portal watermark for M6's catch_up_group. Against the old
// eventRevision = max(user,group) fold, this test fails (the user watermark
// would jump to 999).
func TestHandleGChatEventGroupRevisionDoesNotAdvanceUserWatermark(t *testing.T) {
	meta := &UserLoginMetadata{Revision: 10}
	login := newTestUserLogin(meta)
	login.ID = gcid.MakeUserLoginID("112233")
	evt := messagePostedEvent(spaceGroupID("space-1"), "msg-1", "98765", "hi", 1)
	evt.RevisionType = &pb.Event_GroupRevision{GroupRevision: &pb.WriteRevision{Timestamp: proto.Int64(999)}}
	var userSaveCount int
	var gotPortalKey networkid.PortalKey
	var gotPortalRev int64
	gc := &GChatClient{
		UserLogin: login,
		queueRemoteEventFn: func(bridgev2.RemoteEvent) bridgev2.EventHandlingResult {
			return bridgev2.EventHandlingResultQueued
		},
		saveFn: func(context.Context) error { userSaveCount++; return nil },
		savePortalRevisionFn: func(_ context.Context, key networkid.PortalKey, rev int64) {
			gotPortalKey = key
			gotPortalRev = rev
		},
	}

	gc.handleGChatEvent(context.Background(), evt)

	if meta.Revision != 10 {
		t.Errorf("UserLoginMetadata.Revision = %d, want 10 (group_revision must NOT advance the user catch_up_user watermark -- separate revision space)", meta.Revision)
	}
	if userSaveCount != 0 {
		t.Errorf("user metadata saved %d times, want 0 (group_revision routes to the portal, never the user login)", userSaveCount)
	}
	if gotPortalRev != 999 {
		t.Errorf("portal revision saved = %d, want 999 (group_revision must be parked on PortalMetadata.Revision for M6 catch_up_group)", gotPortalRev)
	}
	wantKey := gcid.MakePortalKey(gcid.GroupID{ID: "space-1", IsDM: false}, login.ID)
	if gotPortalKey != wantKey {
		t.Errorf("portal key = %+v, want %+v", gotPortalKey, wantKey)
	}
}

// TestHandleGChatEventFailedQueueLeavesUserWatermarkUnchanged pins the
// M2-whole-branch Important #3 fix: the watermark advances only AFTER a
// successful queue. A QueueRemoteEvent that returns a non-Success result
// (the message was dropped) must leave UserLoginMetadata.Revision untouched
// so the next reconnect's catch_up_user re-fetches it.
func TestHandleGChatEventFailedQueueLeavesUserWatermarkUnchanged(t *testing.T) {
	meta := &UserLoginMetadata{Revision: 10}
	login := newTestUserLogin(meta)
	login.ID = gcid.MakeUserLoginID("112233")
	evt := messagePostedEvent(spaceGroupID("space-1"), "msg-1", "98765", "hi", 1)
	evt.RevisionType = &pb.Event_UserRevision{UserRevision: &pb.WriteRevision{Timestamp: proto.Int64(999)}}
	var saveCount int
	gc := &GChatClient{
		UserLogin: login,
		queueRemoteEventFn: func(bridgev2.RemoteEvent) bridgev2.EventHandlingResult {
			return bridgev2.EventHandlingResultFailed // Success == false
		},
		saveFn: func(context.Context) error { saveCount++; return nil },
	}

	gc.handleGChatEvent(context.Background(), evt)

	if meta.Revision != 10 {
		t.Errorf("UserLoginMetadata.Revision = %d, want 10 (a FAILED queue must not advance the watermark -- the dropped message is retried on the next reconnect's catch_up)", meta.Revision)
	}
	if saveCount != 0 {
		t.Errorf("save called %d times, want 0 (nothing was successfully handled)", saveCount)
	}
}

// TestCatchUpStopsDrainOnQueueFailure proves the catchUp drain stops the
// moment a caught-up event fails to handle, rather than fetching further
// pages and letting a later success advance the persisted watermark past the
// dropped event (M2-review Important #3, replay side).
func TestCatchUpStopsDrainOnQueueFailure(t *testing.T) {
	meta := &UserLoginMetadata{Revision: 100}
	login := newTestUserLogin(meta)
	login.ID = gcid.MakeUserLoginID("112233")
	evt := messagePostedEvent(spaceGroupID("space-1"), "msg-a", "98765", "a", 1)
	evt.RevisionType = &pb.Event_UserRevision{UserRevision: &pb.WriteRevision{Timestamp: proto.Int64(200)}}
	var calls int
	gc := &GChatClient{
		UserLogin: login,
		catchUpUserFn: func(context.Context, *pb.CatchUpUserRequest) (*pb.CatchUpResponse, error) {
			calls++
			return &pb.CatchUpResponse{Status: pb.CatchUpResponse_PAGINATED.Enum(), Events: []*pb.Event{evt}}, nil
		},
		queueRemoteEventFn: func(bridgev2.RemoteEvent) bridgev2.EventHandlingResult {
			return bridgev2.EventHandlingResultFailed
		},
		saveFn: func(context.Context) error { return nil },
	}

	gc.catchUp(context.Background())

	if calls != 1 {
		t.Errorf("catchUpUserFn called %d times, want 1 (drain must stop on the first queue failure, not fetch page 2)", calls)
	}
	if meta.Revision != 100 {
		t.Errorf("UserLoginMetadata.Revision = %d, want 100 (watermark must not advance past an event that failed to queue)", meta.Revision)
	}
}

func TestHandleGChatEventNoRevisionFieldDoesNotTouchMetadata(t *testing.T) {
	meta := &UserLoginMetadata{Revision: 10}
	login := newTestUserLogin(meta)
	login.ID = gcid.MakeUserLoginID("112233")
	evt := messagePostedEvent(spaceGroupID("space-1"), "msg-1", "98765", "hi", 1) // no RevisionType set
	var saveCount int
	gc := &GChatClient{
		UserLogin: login,
		queueRemoteEventFn: func(bridgev2.RemoteEvent) bridgev2.EventHandlingResult {
			return bridgev2.EventHandlingResultQueued
		},
		saveFn: func(context.Context) error { saveCount++; return nil },
	}

	gc.handleGChatEvent(context.Background(), evt)

	if meta.Revision != 10 {
		t.Errorf("Metadata.Revision = %d, want 10 (unchanged, event carried no revision field)", meta.Revision)
	}
	if saveCount != 0 {
		t.Errorf("save called %d times, want 0", saveCount)
	}
}

func TestHandleGChatEventRevisionNeverRegresses(t *testing.T) {
	meta := &UserLoginMetadata{Revision: 500}
	login := newTestUserLogin(meta)
	login.ID = gcid.MakeUserLoginID("112233")
	evt := messagePostedEvent(spaceGroupID("space-1"), "msg-1", "98765", "hi", 1)
	evt.RevisionType = &pb.Event_UserRevision{UserRevision: &pb.WriteRevision{Timestamp: proto.Int64(100)}} // older/out-of-order
	var saveCount int
	gc := &GChatClient{
		UserLogin: login,
		queueRemoteEventFn: func(bridgev2.RemoteEvent) bridgev2.EventHandlingResult {
			return bridgev2.EventHandlingResultQueued
		},
		saveFn: func(context.Context) error { saveCount++; return nil },
	}

	gc.handleGChatEvent(context.Background(), evt)

	if meta.Revision != 500 {
		t.Errorf("Metadata.Revision = %d, want 500 (must never regress on an out-of-order/older event)", meta.Revision)
	}
	if saveCount != 0 {
		t.Errorf("save called %d times, want 0", saveCount)
	}
}
