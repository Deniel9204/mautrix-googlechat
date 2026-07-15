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

// TestCatchUpPaginatedStatusReplaysAndAdvancesPartialWatermark covers the
// PAGINATED response branch (not just COMPLETED): the returned page's
// events are still replayed, and the watermark still advances to whatever
// was actually seen in that one page -- the remaining backlog beyond this
// page is safely picked up by the NEXT reconnect's catch-up (or by live
// traffic advancing the watermark further via advanceRevision), not lost.
func TestCatchUpPaginatedStatusReplaysAndAdvancesPartialWatermark(t *testing.T) {
	meta := &UserLoginMetadata{Revision: 100}
	login := newTestUserLogin(meta)
	login.ID = gcid.MakeUserLoginID("112233")
	evt := messagePostedEvent(spaceGroupID("space-1"), "msg-1", "98765", "hi", 1)
	evt.RevisionType = &pb.Event_UserRevision{UserRevision: &pb.WriteRevision{Timestamp: proto.Int64(150)}}
	var queued []bridgev2.RemoteEvent
	gc := &GChatClient{
		UserLogin: login,
		catchUpUserFn: func(context.Context, *pb.CatchUpUserRequest) (*pb.CatchUpResponse, error) {
			return &pb.CatchUpResponse{Status: pb.CatchUpResponse_PAGINATED.Enum(), Events: []*pb.Event{evt}}, nil
		},
		queueRemoteEventFn: func(e bridgev2.RemoteEvent) bridgev2.EventHandlingResult {
			queued = append(queued, e)
			return bridgev2.EventHandlingResultQueued
		},
		saveFn: func(context.Context) error { return nil },
	}

	gc.catchUp(context.Background())

	if len(queued) != 1 {
		t.Fatalf("len(queued) = %d, want 1 (a PAGINATED response's events are still replayed)", len(queued))
	}
	if meta.Revision != 150 {
		t.Errorf("Metadata.Revision = %d, want 150 (advances to what was actually seen in this page)", meta.Revision)
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

// --- syncChats: syncInProgress reflects its own running/finished state ----

// TestSyncChatsSetsSyncInProgressForItsDuration pins the gchat-port-auditor
// P1 fix's other half: syncChats itself must actually toggle the flag
// catchUp's guard depends on.
func TestSyncChatsSetsSyncInProgressForItsDuration(t *testing.T) {
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

	if gc.isSyncInProgress() {
		t.Fatal("isSyncInProgress() = true before syncChats ever ran")
	}

	gc.syncChats(context.Background())

	if !sawInProgress {
		t.Error("isSyncInProgress() = false DURING syncChats' own RPC call, want true")
	}
	if gc.isSyncInProgress() {
		t.Error("isSyncInProgress() = true after syncChats returned, want false")
	}
}

// --- advanceRevision: every event (not just catch-up replay) moves the ----
// watermark, mirroring user.py:674-682's on_stream_event (gchat-port-auditor
// P0 fix: without this, the watermark could only ever move forward from
// inside catchUp's own replay, so it would go stale during ordinary live
// traffic and eventually make catch_up_user permanently fail).

func TestHandleGChatEventAdvancesRevisionWatermark(t *testing.T) {
	meta := &UserLoginMetadata{Revision: 10}
	login := newTestUserLogin(meta)
	login.ID = gcid.MakeUserLoginID("112233")
	evt := messagePostedEvent(spaceGroupID("space-1"), "msg-1", "98765", "hi", 1)
	evt.RevisionType = &pb.Event_UserRevision{UserRevision: &pb.WriteRevision{Timestamp: proto.Int64(20)}}
	var saveCount int
	gc := &GChatClient{
		UserLogin: login,
		queueRemoteEventFn: func(bridgev2.RemoteEvent) bridgev2.EventHandlingResult {
			return bridgev2.EventHandlingResultQueued
		},
		saveFn: func(context.Context) error { saveCount++; return nil },
	}

	gc.handleGChatEvent(context.Background(), evt)

	if meta.Revision != 20 {
		t.Errorf("Metadata.Revision = %d, want 20 (a live event's own user_revision advances the watermark, not just catch-up replay)", meta.Revision)
	}
	if saveCount != 1 {
		t.Errorf("save called %d times, want 1", saveCount)
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
