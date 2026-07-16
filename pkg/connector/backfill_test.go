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
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv/gchatfmt"
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

// TestCatchUpDrainReplaysMutationEventsThroughHandleGChatEvent pins the M4
// whole-branch-review payoff (Minor #3): this file's own top-of-file doc
// comment claims catchUp dispatches every returned event through
// handleGChatEvent -- the SAME dispatch switch a live stream event takes
// (events.go) -- so M4's edit/reaction/delete handlers apply to
// gap-replayed events automatically, with no separate backfill-specific
// event handling to keep in sync. That held structurally (each mutation
// type's own Success path is pinned per-type in events_test.go), but until
// now no test actually drove catchUp itself over a page containing a
// mutation and checked BOTH that the mutation handler ran (not just "some
// event was queued") AND that the watermark advanced past it. Scripts one
// COMPLETED page carrying a MESSAGE_UPDATED (edit), MESSAGE_DELETED, and a
// MESSAGE_REACTED (add) event together, and asserts all three are
// dispatched as their correct concrete RemoteEvent types in order and the
// persisted watermark ends at the highest (last) event's user_revision.
func TestCatchUpDrainReplaysMutationEventsThroughHandleGChatEvent(t *testing.T) {
	meta := &UserLoginMetadata{Revision: 100}
	login := newTestUserLogin(meta)
	login.ID = gcid.MakeUserLoginID("112233")

	editEvt := messageUpdatedEvent(spaceGroupID("space-1"), "msg-edit", "98765", "edited during gap", 5000)
	editEvt.RevisionType = &pb.Event_UserRevision{UserRevision: &pb.WriteRevision{Timestamp: proto.Int64(200)}}
	deleteEvt := messageDeletedEvent(spaceGroupID("space-1"), "msg-delete", 1)
	deleteEvt.RevisionType = &pb.Event_UserRevision{UserRevision: &pb.WriteRevision{Timestamp: proto.Int64(300)}}
	reactionEvt := messageReactionEvent(spaceGroupID("space-1"), "msg-react", "98765", "❤", pb.MessageReactionEvent_ADD, 1)
	reactionEvt.RevisionType = &pb.Event_UserRevision{UserRevision: &pb.WriteRevision{Timestamp: proto.Int64(400)}}

	var queued []bridgev2.RemoteEvent
	gc := &GChatClient{
		UserLogin: login,
		catchUpUserFn: func(context.Context, *pb.CatchUpUserRequest) (*pb.CatchUpResponse, error) {
			return &pb.CatchUpResponse{
				Status: pb.CatchUpResponse_COMPLETED.Enum(),
				Events: []*pb.Event{editEvt, deleteEvt, reactionEvt},
			}, nil
		},
		queueRemoteEventFn: func(e bridgev2.RemoteEvent) bridgev2.EventHandlingResult {
			queued = append(queued, e)
			return bridgev2.EventHandlingResultQueued
		},
		saveFn: func(context.Context) error { return nil },
	}

	gc.catchUp(context.Background())

	if len(queued) != 3 {
		t.Fatalf("len(queued) = %d, want 3 (edit, delete, and reaction all dispatched through handleGChatEvent during catch-up replay)", len(queued))
	}
	if _, ok := queued[0].(bridgev2.RemoteEdit); !ok {
		t.Errorf("queued[0] does not implement bridgev2.RemoteEdit: %T (M4 Task 1's edit handler must run during catch-up replay, not just live traffic)", queued[0])
	}
	if _, ok := queued[1].(bridgev2.RemoteMessageRemove); !ok {
		t.Errorf("queued[1] does not implement bridgev2.RemoteMessageRemove: %T (M4 Task 2's delete handler must run during catch-up replay)", queued[1])
	}
	if _, ok := queued[2].(bridgev2.RemoteReaction); !ok {
		t.Errorf("queued[2] does not implement bridgev2.RemoteReaction: %T (M4 Task 3's reaction handler must run during catch-up replay)", queued[2])
	}
	if meta.Revision != 400 {
		t.Errorf("Metadata.Revision = %d, want 400 (watermark advances past every replayed mutation event, not just plain messages -- the last event's user_revision)", meta.Revision)
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

// --- M6 Task 1: FetchMessages (flat-room initial/history backfill) --------
//
// See backfill.go's "M6 Task 1" region doc comment for the full rationale
// behind using list_topics (not list_messages) and the single-shot (not
// paged) design these tests pin down.

// flatBackfillPortal builds a *bridgev2.Portal for a flat (non-ThreadsOnly)
// portal with a real, parseable PortalKey.ID (gcid.ParsePortalID must
// succeed -- fetchFlatMessages' first step) and a Bridge whose Matrix
// connector resolves ghost mentions deterministically, so a formatted
// message's mention pill renders the same way it would for a live message
// (TestFetchMessagesFlatConvertMessageReusesLiveConversionPath below).
// Bridge is otherwise unused: GetIntentFor is never called for real in these
// tests (see newBackfillTestClient's getIntentForFn override).
func flatBackfillPortal(group gcid.GroupID) *bridgev2.Portal {
	matrix := &fakeMatrixConnector{
		ghostIntent: func(gaiaID networkid.UserID) id.UserID {
			return id.UserID("@" + string(gaiaID) + "_ghost:example.com")
		},
	}
	return &bridgev2.Portal{
		Portal: &database.Portal{
			PortalKey: gcid.MakePortalKey(group, ""),
			Metadata:  &PortalMetadata{},
		},
		Bridge: &bridgev2.Bridge{Matrix: matrix},
	}
}

// newBackfillTestClient builds a *GChatClient with listTopicsFn wired to
// list and getIntentForFn stubbed to always succeed with a nil intent
// (fine: none of these tests' messages carry attachments, so intent is
// never dereferenced -- msgconv_adapter.go's own doc comment on the "media"
// parameter makes the same "nil is safe when nothing needs it" point).
func newBackfillTestClient(ownGaia string, list func(context.Context, *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error)) *GChatClient {
	login := newTestUserLogin(&UserLoginMetadata{})
	login.ID = gcid.MakeUserLoginID(ownGaia)
	return &GChatClient{
		UserLogin:    login,
		listTopicsFn: list,
		getIntentForFn: func(context.Context, *bridgev2.Portal, bridgev2.EventSender, bridgev2.RemoteEventType) (bridgev2.MatrixAPI, bool) {
			return nil, true
		},
	}
}

// flatTopic builds a *pb.Topic shaped like a non-threaded topic: exactly one
// reply (the flat message itself), matching portal.py's own
// `topic.replies[0]` reading of a non-threaded topic.
func flatTopic(topicID, msgID, creatorGaia, text string, sortTime int64) *pb.Topic {
	return &pb.Topic{
		Id:       &pb.TopicId{TopicId: proto.String(topicID)},
		SortTime: proto.Int64(sortTime),
		Replies: []*pb.Message{{
			Id:         &pb.MessageId{MessageId: proto.String(msgID)},
			Creator:    &pb.User{UserId: &pb.UserId{Id: proto.String(creatorGaia)}},
			CreateTime: proto.Int64(sortTime),
			TextBody:   proto.String(text),
		}},
	}
}

// --- single-shot request shape -------------------------------------------

// TestFetchMessagesFlatCallsListTopicsOnceWithGroupAndCount pins the
// single-shot rework (m6-task-1-review.md's Important finding): fetchFlatMessages
// must issue exactly ONE ListTopics call, with PageSizeForTopics = params.Count
// (the entire requested backfill depth, not a growing cumulative request), and
// the response must report HasMore=false with an empty Cursor -- there is no
// next page to ask for, ever, matching portal.py's _initial_backfill (one
// ListTopicsRequest, no loop).
func TestFetchMessagesFlatCallsListTopicsOnceWithGroupAndCount(t *testing.T) {
	group := gcid.GroupID{ID: "space-1", IsDM: false}
	var gotReq *pb.ListTopicsRequest
	var calls int
	gc := newBackfillTestClient("112233", func(_ context.Context, req *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
		calls++
		gotReq = req
		return &pb.ListTopicsResponse{
			Topics:             []*pb.Topic{flatTopic("t3", "m3", "u1", "three", 30)},
			ContainsFirstTopic: proto.Bool(true),
		}, nil
	})

	resp, err := gc.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
		Portal: flatBackfillPortal(group),
		Count:  3,
	})
	if err != nil {
		t.Fatalf("FetchMessages returned error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("listTopicsFn called %d times, want 1 (single-shot, no page loop)", calls)
	}
	if got := gotReq.GetPageSizeForTopics(); got != 3 {
		t.Errorf("PageSizeForTopics = %d, want 3 (params.Count directly, no delivered-so-far offset)", got)
	}
	id, isDM, ok := gchatmeow.GroupIDToParts(gotReq.GetGroupId())
	if !ok || id != "space-1" || isDM {
		t.Errorf("GroupId = (%q, isDM=%v, ok=%v), want (%q, false, true)", id, isDM, ok, "space-1")
	}
	if len(resp.Messages) != 1 {
		t.Fatalf("len(Messages) = %d, want 1", len(resp.Messages))
	}
	if resp.HasMore {
		t.Error("HasMore = true, want false (single-shot never has more)")
	}
	if resp.Cursor != "" {
		t.Errorf("Cursor = %q, want empty (no pagination)", resp.Cursor)
	}
}

// TestFetchMessagesFlatZeroCountFallsBackToDefault pins the defensive
// non-positive-Count guard: a zero params.Count must not turn into
// PageSizeForTopics=0 (which would ask the server for nothing).
func TestFetchMessagesFlatZeroCountFallsBackToDefault(t *testing.T) {
	group := gcid.GroupID{ID: "space-1", IsDM: false}
	var gotReq *pb.ListTopicsRequest
	gc := newBackfillTestClient("112233", func(_ context.Context, req *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
		gotReq = req
		return &pb.ListTopicsResponse{Topics: nil, ContainsFirstTopic: proto.Bool(true)}, nil
	})

	_, err := gc.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
		Portal: flatBackfillPortal(group),
		Count:  0,
	})
	if err != nil {
		t.Fatalf("FetchMessages returned error: %v", err)
	}
	if got := gotReq.GetPageSizeForTopics(); got != defaultFetchMessagesCount {
		t.Errorf("PageSizeForTopics = %d, want %d (defaultFetchMessagesCount fallback)", got, defaultFetchMessagesCount)
	}
}

// --- chronological ordering + field mapping ----------------------------

// TestFetchMessagesFlatOrdersChronologicallyWithCorrectFields pins the
// brief's core ask: the server returns topics NEWEST-FIRST (matching
// portal.py's own need to reverse them, portal.py:428); the response must
// come back oldest-to-newest, and each BackfillMessage's ID/Sender/
// Timestamp/StreamOrder must be derived from its topic's head reply.
func TestFetchMessagesFlatOrdersChronologicallyWithCorrectFields(t *testing.T) {
	group := gcid.GroupID{ID: "space-1", IsDM: false}
	// Server order: newest (m3) first, oldest (m1) last -- exactly what a
	// real "most recent N" list_topics response looks like.
	serverOrder := []*pb.Topic{
		flatTopic("t3", "m3", "u3", "three", 3_000_000),
		flatTopic("t2", "m2", "u2", "two", 2_000_000),
		flatTopic("t1", "m1", "u1", "one", 1_000_000),
	}
	gc := newBackfillTestClient("owner", func(context.Context, *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
		return &pb.ListTopicsResponse{Topics: serverOrder, ContainsFirstTopic: proto.Bool(true)}, nil
	})

	resp, err := gc.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
		Portal: flatBackfillPortal(group),
		Count:  3,
	})
	if err != nil {
		t.Fatalf("FetchMessages returned error: %v", err)
	}
	if len(resp.Messages) != 3 {
		t.Fatalf("len(Messages) = %d, want 3", len(resp.Messages))
	}
	if resp.HasMore {
		t.Error("HasMore = true, want false (single-shot)")
	}
	if resp.Cursor != "" {
		t.Errorf("Cursor = %q, want empty (single-shot)", resp.Cursor)
	}
	wantIDs := []networkid.MessageID{gcid.MakeMessageID("m1"), gcid.MakeMessageID("m2"), gcid.MakeMessageID("m3")}
	for i, want := range wantIDs {
		if resp.Messages[i].ID != want {
			t.Errorf("Messages[%d].ID = %q, want %q (chronological oldest-first)", i, resp.Messages[i].ID, want)
		}
	}

	m1 := resp.Messages[0]
	wantSender := bridgev2.EventSender{Sender: gcid.MakeUserID("u1"), IsFromMe: false}
	if m1.Sender != wantSender {
		t.Errorf("Messages[0].Sender = %+v, want %+v", m1.Sender, wantSender)
	}
	wantTS := gchatmeow.MicrosToTime(1_000_000)
	if !m1.Timestamp.Equal(wantTS) {
		t.Errorf("Messages[0].Timestamp = %v, want %v", m1.Timestamp, wantTS)
	}
	if m1.StreamOrder != 1_000_000 {
		t.Errorf("Messages[0].StreamOrder = %d, want 1000000 (create_time in µs)", m1.StreamOrder)
	}
}

// TestFetchMessagesFlatSenderIsFromMe pins EventSender.IsFromMe being
// derived the same way the live MESSAGE_POSTED path derives it
// (c.IsThisUser), not left false unconditionally.
func TestFetchMessagesFlatSenderIsFromMe(t *testing.T) {
	group := gcid.GroupID{ID: "space-1", IsDM: false}
	gc := newBackfillTestClient("owner-gaia", func(context.Context, *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
		return &pb.ListTopicsResponse{
			Topics:             []*pb.Topic{flatTopic("t1", "m1", "owner-gaia", "hi", 1000)},
			ContainsFirstTopic: proto.Bool(true),
		}, nil
	})

	resp, err := gc.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
		Portal: flatBackfillPortal(group),
		Count:  1,
	})
	if err != nil {
		t.Fatalf("FetchMessages returned error: %v", err)
	}
	if len(resp.Messages) != 1 {
		t.Fatalf("len(Messages) = %d, want 1", len(resp.Messages))
	}
	if !resp.Messages[0].Sender.IsFromMe {
		t.Error("Sender.IsFromMe = false, want true (message creator is this login's own gaia id)")
	}
}

// --- M6 Task 3: reaction backfill is an intentional, tested omission ------

// TestFetchMessagesFlatOmitsReactionsEvenWhenGCMessageHasThem pins the M6
// Task 3 finding that reaction backfill is factually impossible, not just
// unimplemented: GC's Reaction proto (pkg/gchatmeow/proto/googlechat.pb.go's
// `type Reaction struct`, attached to Message.Reactions field 21) carries
// ONLY Emoji/Count/CurrentUserParticipated/CreateTimestamp -- no reactor user
// id -- so there is no identity to build a bridgev2.BackfillReaction.Sender
// from. Python makes the identical choice: portal.py's _initial_backfill
// (portal.py:406-448) never reads message.reactions at all; reactions are
// bridged exclusively from live MessageReactionEvents, which do carry a real
// reactor id. This test builds a GC Message whose Reactions field IS
// populated (Count>0, CurrentUserParticipated=true -- the only case a naive
// future change might be tempted to attribute to the current user) and
// asserts the resulting BackfillMessage.Reactions is nil/empty regardless.
// See fetchTopicHeadMessages' inline doc comment (backfill.go) for the full
// rationale this test enforces.
func TestFetchMessagesFlatOmitsReactionsEvenWhenGCMessageHasThem(t *testing.T) {
	group := gcid.GroupID{ID: "space-1", IsDM: false}
	topic := flatTopic("t1", "m1", "u1", "hello", 1_000_000)
	topic.Replies[0].Reactions = []*pb.Reaction{
		{
			Emoji:                   &pb.Emoji{Content: &pb.Emoji_Unicode{Unicode: "\U0001F44D"}},
			Count:                   proto.Int32(3),
			CurrentUserParticipated: proto.Bool(true),
			CreateTimestamp:         proto.Int64(1_000_500),
		},
	}
	gc := newBackfillTestClient("owner", func(context.Context, *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
		return &pb.ListTopicsResponse{Topics: []*pb.Topic{topic}, ContainsFirstTopic: proto.Bool(true)}, nil
	})

	resp, err := gc.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
		Portal: flatBackfillPortal(group),
		Count:  1,
	})
	if err != nil {
		t.Fatalf("FetchMessages returned error: %v", err)
	}
	if len(resp.Messages) != 1 {
		t.Fatalf("len(Messages) = %d, want 1", len(resp.Messages))
	}
	if got := resp.Messages[0].Reactions; len(got) != 0 {
		t.Errorf("Messages[0].Reactions = %+v, want nil/empty (GC's Reaction proto has no reactor id -- see fetchTopicHeadMessages' doc comment on why this must stay unset)", got)
	}
}

// --- SortTime ties: deterministic topic-id tiebreaker --------------------

// TestFetchMessagesFlatSortTimeTieBreaksByTopicID pins the m6-task-1-review.md
// Minor fix (comparator) and, more importantly, the tie-hole this whole
// rework exists to close: two distinct topics sharing an identical
// microsecond SortTime must sort in a DETERMINISTIC order (by topic id),
// never in whatever order the server happened to return them in. The server
// lists topic-1 BEFORE topic-2 (newest-first); reversing that (this
// function's first step, oldest-first) puts topic-2 before topic-1, and
// without a tiebreaker slices.SortStableFunc leaves that post-reversal order
// alone since the tie compares equal -- the OPPOSITE of topic-id-ascending
// order. So this test fails under the old
// `int(a.GetSortTime()-b.GetSortTime())` comparator (which returns 0 for the
// tie, so nothing reorders it) and passes once the comparator also breaks
// ties on topic id.
func TestFetchMessagesFlatSortTimeTieBreaksByTopicID(t *testing.T) {
	group := gcid.GroupID{ID: "space-1", IsDM: false}
	// Server order (newest-first, as always): topic-1 before topic-2, both at
	// the identical SortTime 100. Message ids are chosen to NOT correlate
	// alphabetically with topic ids, so a passing test proves the tiebreaker
	// keys on TOPIC id specifically, not message id or server/reversal order.
	gc := newBackfillTestClient("owner", func(context.Context, *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
		return &pb.ListTopicsResponse{
			Topics: []*pb.Topic{
				flatTopic("topic-1", "msg-zzz", "u", "first", 100),
				flatTopic("topic-2", "msg-aaa", "u", "second", 100),
			},
			ContainsFirstTopic: proto.Bool(true),
		}, nil
	})

	resp, err := gc.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
		Portal: flatBackfillPortal(group),
		Count:  2,
	})
	if err != nil {
		t.Fatalf("FetchMessages returned error: %v", err)
	}
	if len(resp.Messages) != 2 {
		t.Fatalf("len(Messages) = %d, want 2", len(resp.Messages))
	}
	// topic-1 < topic-2 lexicographically, so topic-1's reply (msg-zzz) must
	// sort first regardless of the server's tie order.
	want := []networkid.MessageID{gcid.MakeMessageID("msg-zzz"), gcid.MakeMessageID("msg-aaa")}
	for i, w := range want {
		if resp.Messages[i].ID != w {
			t.Errorf("Messages[%d].ID = %q, want %q (deterministic topic-id tiebreak on a SortTime tie)", i, resp.Messages[i].ID, w)
		}
	}
}

// --- HasMore is always false for single-shot ------------------------------

// TestFetchMessagesFlatServerReturnsFewerThanRequestedSetsHasMoreFalse pins
// that HasMore is unconditionally false in the single-shot design,
// regardless of how many topics the server actually returned relative to
// what was requested -- there is no next page to ask for, ever.
func TestFetchMessagesFlatServerReturnsFewerThanRequestedSetsHasMoreFalse(t *testing.T) {
	group := gcid.GroupID{ID: "space-1", IsDM: false}
	// Only 2 topics exist; asked for 5.
	gc := newBackfillTestClient("owner", func(context.Context, *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
		return &pb.ListTopicsResponse{
			Topics: []*pb.Topic{
				flatTopic("t2", "m2", "u", "two", 20),
				flatTopic("t1", "m1", "u", "one", 10),
			},
		}, nil
	})

	resp, err := gc.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
		Portal: flatBackfillPortal(group),
		Count:  5,
	})
	if err != nil {
		t.Fatalf("FetchMessages returned error: %v", err)
	}
	if resp.HasMore {
		t.Error("HasMore = true, want false (single-shot never has more)")
	}
	if len(resp.Messages) != 2 {
		t.Fatalf("len(Messages) = %d, want 2", len(resp.Messages))
	}
}

// --- ConvertMessage reuses the live conversion path ---------------------

// TestFetchMessagesFlatConvertMessageReusesLiveConversionPath pins the
// no-parallel-conversion-path requirement: a formatted backfilled message
// (a BOLD annotation, same mechanism msgconv_adapter_test.go's own
// formatting tests use) must produce EXACTLY the same *bridgev2.ConvertedMessage
// convertMessageToMatrix would produce for the identical *pb.Message live --
// proven by calling convertMessageToMatrix directly on the same message/
// portal and comparing the rendered HTML body.
func TestFetchMessagesFlatConvertMessageReusesLiveConversionPath(t *testing.T) {
	group := gcid.GroupID{ID: "space-1", IsDM: false}
	msg := &pb.Message{
		Id:          &pb.MessageId{MessageId: proto.String("m1")},
		Creator:     &pb.User{UserId: &pb.UserId{Id: proto.String("u1")}},
		CreateTime:  proto.Int64(1000),
		TextBody:    proto.String("bold text"),
		Annotations: []*pb.Annotation{gchatfmt.MakeFormatAnnotation(0, 4, pb.FormatMetadata_BOLD)},
	}
	topic := &pb.Topic{Id: &pb.TopicId{TopicId: proto.String("t1")}, SortTime: proto.Int64(1000), Replies: []*pb.Message{msg}}
	gc := newBackfillTestClient("owner", func(context.Context, *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
		return &pb.ListTopicsResponse{Topics: []*pb.Topic{topic}, ContainsFirstTopic: proto.Bool(true)}, nil
	})
	portal := flatBackfillPortal(group)

	resp, err := gc.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{Portal: portal, Count: 1})
	if err != nil {
		t.Fatalf("FetchMessages returned error: %v", err)
	}
	if len(resp.Messages) != 1 {
		t.Fatalf("len(Messages) = %d, want 1", len(resp.Messages))
	}
	backfillCM := resp.Messages[0].ConvertedMessage
	if backfillCM == nil || len(backfillCM.Parts) != 1 {
		t.Fatalf("backfill ConvertedMessage = %+v, want 1 part", backfillCM)
	}

	convert := convertMessageToMatrix(gc.msgConverter(), gc)
	liveCM, err := convert(context.Background(), portal, nil, msg)
	if err != nil {
		t.Fatalf("live convertMessageToMatrix returned error: %v", err)
	}
	if len(liveCM.Parts) != 1 {
		t.Fatalf("live ConvertedMessage has %d parts, want 1", len(liveCM.Parts))
	}

	if backfillCM.Parts[0].Content.Body != liveCM.Parts[0].Content.Body {
		t.Errorf("backfill body = %q, live body = %q, want identical", backfillCM.Parts[0].Content.Body, liveCM.Parts[0].Content.Body)
	}
	if backfillCM.Parts[0].Content.FormattedBody != liveCM.Parts[0].Content.FormattedBody {
		t.Errorf("backfill formatted body = %q, live formatted body = %q, want identical (BOLD formatting must survive backfill conversion identically)", backfillCM.Parts[0].Content.FormattedBody, liveCM.Parts[0].Content.FormattedBody)
	}
	if backfillCM.Parts[0].Content.FormattedBody == "" {
		t.Error("formatted body is empty, want BOLD annotation to have rendered HTML")
	}
}

// --- anchor filtering: don't re-deliver already-bridged messages --------

// TestFetchMessagesFlatAnchorExcludesAlreadyBridgedMessages mirrors meta's
// own wrapBackfillEvents anchor filter (_reference/meta/pkg/connector/backfill.go):
// a portal that already has SOME bridged history (AnchorMessage != nil) must
// not re-deliver anything at or after the anchor's timestamp.
func TestFetchMessagesFlatAnchorExcludesAlreadyBridgedMessages(t *testing.T) {
	group := gcid.GroupID{ID: "space-1", IsDM: false}
	gc := newBackfillTestClient("owner", func(context.Context, *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
		return &pb.ListTopicsResponse{
			Topics: []*pb.Topic{
				flatTopic("t3", "m3", "u", "three", 3_000_000), // at anchor: excluded
				flatTopic("t2", "m2", "u", "two", 2_000_000),   // older: included
				flatTopic("t1", "m1", "u", "one", 1_000_000),   // older: included
			},
			ContainsFirstTopic: proto.Bool(true),
		}, nil
	})

	resp, err := gc.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
		Portal: flatBackfillPortal(group),
		Count:  3,
		AnchorMessage: &database.Message{
			ID:        gcid.MakeMessageID("m3"),
			Timestamp: gchatmeow.MicrosToTime(3_000_000),
		},
	})
	if err != nil {
		t.Fatalf("FetchMessages returned error: %v", err)
	}
	if len(resp.Messages) != 2 {
		t.Fatalf("len(Messages) = %d, want 2 (m3 excluded, at the anchor)", len(resp.Messages))
	}
	for _, m := range resp.Messages {
		if m.ID == gcid.MakeMessageID("m3") {
			t.Error("anchor message m3 was re-delivered, want excluded")
		}
	}
}

// TestFetchMessagesFlatAnchorKeepsDistinctSiblingAtSameMicrosecond pins
// m6-task-1-review.md's anchor-filter fix: a distinct message sharing the
// anchor's exact microsecond CreateTime must NOT be excluded, only the
// anchor message itself (by id) and anything strictly newer. Under the old
// `msg.GetCreateTime() >= anchorMicros` cutoff this test fails (m-sibling is
// wrongly dropped alongside m-anchor, since both are AT the anchor's
// microsecond); the id-or-strictly-newer check keeps m-sibling.
func TestFetchMessagesFlatAnchorKeepsDistinctSiblingAtSameMicrosecond(t *testing.T) {
	group := gcid.GroupID{ID: "space-1", IsDM: false}
	gc := newBackfillTestClient("owner", func(context.Context, *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
		return &pb.ListTopicsResponse{
			Topics: []*pb.Topic{
				flatTopic("t-anchor", "m-anchor", "u", "anchor msg", 3_000_000),
				flatTopic("t-sibling", "m-sibling", "u", "sibling msg", 3_000_000), // same µs, distinct id
				flatTopic("t-older", "m-older", "u", "older msg", 1_000_000),
			},
			ContainsFirstTopic: proto.Bool(true),
		}, nil
	})

	resp, err := gc.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
		Portal: flatBackfillPortal(group),
		Count:  3,
		AnchorMessage: &database.Message{
			ID:        gcid.MakeMessageID("m-anchor"),
			Timestamp: gchatmeow.MicrosToTime(3_000_000),
		},
	})
	if err != nil {
		t.Fatalf("FetchMessages returned error: %v", err)
	}
	got := map[networkid.MessageID]bool{}
	for _, m := range resp.Messages {
		got[m.ID] = true
	}
	if got[gcid.MakeMessageID("m-anchor")] {
		t.Error("anchor message m-anchor was re-delivered, want excluded")
	}
	if !got[gcid.MakeMessageID("m-sibling")] {
		t.Error("m-sibling (distinct id, same microsecond as anchor) was excluded, want kept")
	}
	if !got[gcid.MakeMessageID("m-older")] {
		t.Error("m-older (strictly older than anchor) was excluded, want kept")
	}
	if len(resp.Messages) != 2 {
		t.Errorf("len(Messages) = %d, want 2 (m-sibling + m-older)", len(resp.Messages))
	}
}

// --- M6 Task 2: flat-with-real-thread gap closure -------------------------
//
// Task 1's region doc comment documented a gap: a topic with a real Google
// Chat thread even inside an otherwise-flat room
// (topic_read_state.thread_created_usec > 0) was bridged as ONLY its head
// reply, never setting ShouldBackfillThread, so the framework would never
// drive a ThreadRoot-scoped fetch for its other replies -- unlike Python's
// `self.threads_only or topic.topic_read_state.thread_created_usec > 0`
// (portal.py:432). These two tests pin the closed gap: a flat topic that DOES
// carry a real thread now gets ShouldBackfillThread=true; a plain flat topic
// (no real thread) still does not.

func TestFetchMessagesFlatTopicWithRealThreadSetsShouldBackfillThread(t *testing.T) {
	group := gcid.GroupID{ID: "space-1", IsDM: false}
	topic := flatTopic("t1", "m1", "u1", "hi", 1000)
	topic.TopicReadState = &pb.TopicReadState{ThreadCreatedUsec: proto.Int64(500)}
	gc := newBackfillTestClient("owner", func(context.Context, *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
		return &pb.ListTopicsResponse{Topics: []*pb.Topic{topic}, ContainsFirstTopic: proto.Bool(true)}, nil
	})

	resp, err := gc.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
		Portal: flatBackfillPortal(group),
		Count:  1,
	})
	if err != nil {
		t.Fatalf("FetchMessages returned error: %v", err)
	}
	if len(resp.Messages) != 1 {
		t.Fatalf("len(Messages) = %d, want 1", len(resp.Messages))
	}
	if !resp.Messages[0].ShouldBackfillThread {
		t.Error("ShouldBackfillThread = false, want true (topic has ThreadCreatedUsec>0, a real thread even in a flat room -- Task 1's documented gap)")
	}
	if resp.Messages[0].LastThreadMessage != gcid.MakeMessageID("m1") {
		t.Errorf("LastThreadMessage = %q, want %q (single-reply topic: the head is also the last known thread message)", resp.Messages[0].LastThreadMessage, "m1")
	}
}

func TestFetchMessagesFlatTopicWithoutRealThreadShouldBackfillThreadFalse(t *testing.T) {
	group := gcid.GroupID{ID: "space-1", IsDM: false}
	gc := newBackfillTestClient("owner", func(context.Context, *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
		return &pb.ListTopicsResponse{
			Topics:             []*pb.Topic{flatTopic("t1", "m1", "u1", "hi", 1000)},
			ContainsFirstTopic: proto.Bool(true),
		}, nil
	})

	resp, err := gc.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
		Portal: flatBackfillPortal(group),
		Count:  1,
	})
	if err != nil {
		t.Fatalf("FetchMessages returned error: %v", err)
	}
	if len(resp.Messages) != 1 {
		t.Fatalf("len(Messages) = %d, want 1", len(resp.Messages))
	}
	if resp.Messages[0].ShouldBackfillThread {
		t.Error("ShouldBackfillThread = true, want false (plain flat topic, no real thread)")
	}
	if resp.Messages[0].LastThreadMessage != "" {
		t.Errorf("LastThreadMessage = %q, want empty (no thread to backfill)", resp.Messages[0].LastThreadMessage)
	}
}

// --- Forward: out of scope (documented GC limitation) ---------------------

func TestFetchMessagesForwardReturnsEmptyResponse(t *testing.T) {
	group := gcid.GroupID{ID: "space-1", IsDM: false}
	var called bool
	gc := newBackfillTestClient("owner", func(context.Context, *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
		called = true
		return &pb.ListTopicsResponse{}, nil
	})

	resp, err := gc.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
		Portal:  flatBackfillPortal(group),
		Forward: true,
		Count:   3,
	})
	if err != nil {
		t.Fatalf("FetchMessages returned error: %v", err)
	}
	if called {
		t.Error("listTopicsFn was called for a Forward request, want not called (M6 focuses on backward/initial)")
	}
	if resp.HasMore || len(resp.Messages) != 0 {
		t.Errorf("resp = %+v, want empty/HasMore=false", resp)
	}
}

// --- M6 Task 2: ThreadsOnly top-level (list_topics, every head threaded) --

// TestFetchMessagesThreadsOnlyTopLevelHeadsHaveShouldBackfillThread pins the
// brief's core ThreadsOnly ask: a ThreadsOnly portal's top-level
// (ThreadRoot=="") FetchMessages call uses the exact same list_topics +
// chronological-head strategy as a flat portal (TestFetchMessagesFlat...
// above), but EVERY topic's head gets ShouldBackfillThread=true
// unconditionally (not gated on ThreadCreatedUsec, since every topic in a
// threaded space is itself a thread), with LastThreadMessage defaulting to
// the head's own id (single-reply topic).
func TestFetchMessagesThreadsOnlyTopLevelHeadsHaveShouldBackfillThread(t *testing.T) {
	group := gcid.GroupID{ID: "space-1", IsDM: false}
	serverOrder := []*pb.Topic{
		flatTopic("t2", "m2", "u2", "two", 2_000_000),
		flatTopic("t1", "m1", "u1", "one", 1_000_000),
	}
	gc := newBackfillTestClient("owner", func(context.Context, *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
		return &pb.ListTopicsResponse{Topics: serverOrder, ContainsFirstTopic: proto.Bool(true)}, nil
	})
	portal := flatBackfillPortal(group)
	portal.Metadata = &PortalMetadata{ThreadsOnly: true}

	resp, err := gc.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{Portal: portal, Count: 2})
	if err != nil {
		t.Fatalf("FetchMessages returned error: %v", err)
	}
	if len(resp.Messages) != 2 {
		t.Fatalf("len(Messages) = %d, want 2", len(resp.Messages))
	}
	if resp.HasMore {
		t.Error("HasMore = true, want false (single-shot)")
	}
	if resp.Cursor != "" {
		t.Errorf("Cursor = %q, want empty (single-shot)", resp.Cursor)
	}
	wantIDs := []networkid.MessageID{gcid.MakeMessageID("m1"), gcid.MakeMessageID("m2")}
	for i, want := range wantIDs {
		if resp.Messages[i].ID != want {
			t.Errorf("Messages[%d].ID = %q, want %q (chronological oldest-first)", i, resp.Messages[i].ID, want)
		}
		if !resp.Messages[i].ShouldBackfillThread {
			t.Errorf("Messages[%d].ShouldBackfillThread = false, want true (every topic head in a ThreadsOnly portal)", i)
		}
		if resp.Messages[i].LastThreadMessage != want {
			t.Errorf("Messages[%d].LastThreadMessage = %q, want %q (single-reply topic: the head is also the last known thread message)", i, resp.Messages[i].LastThreadMessage, want)
		}
	}
}

// --- M6 Task 2: ThreadRoot-scoped (list_messages for one topic) ----------
//
// threadRootAnchor builds the *database.Message a ThreadRoot-scoped
// FetchMessages call receives as params.AnchorMessage: its Metadata carries
// the *MessageMetadata whose TopicID field fetchThreadMessages reads to
// resolve which topic to list_messages against (see backfill.go's "M6 Task
// 2" region doc comment's "MESSAGE -> TOPIC MAPPING" note).
func threadRootAnchor(msgID, topicID string, createMicros int64) *database.Message {
	return &database.Message{
		ID:        gcid.MakeMessageID(msgID),
		Timestamp: gchatmeow.MicrosToTime(createMicros),
		Metadata:  &MessageMetadata{TopicID: topicID},
	}
}

// threadReply builds a *pb.Message shaped like a plain thread reply (no
// parent_id needed here -- fetchThreadMessages never reads it, only the
// anchor's stored metadata), mirroring flatTopic's inner message literal.
func threadReply(msgID, creatorGaia, text string, createTime int64) *pb.Message {
	return &pb.Message{
		Id:         &pb.MessageId{MessageId: proto.String(msgID)},
		Creator:    &pb.User{UserId: &pb.UserId{Id: proto.String(creatorGaia)}},
		CreateTime: proto.Int64(createTime),
		TextBody:   proto.String(text),
	}
}

// newThreadBackfillTestClient mirrors newBackfillTestClient, but wires
// listMessagesFn (the Task 2 seam) instead of listTopicsFn.
func newThreadBackfillTestClient(ownGaia string, list func(context.Context, *pb.ListMessagesRequest) (*pb.ListMessagesResponse, error)) *GChatClient {
	login := newTestUserLogin(&UserLoginMetadata{})
	login.ID = gcid.MakeUserLoginID(ownGaia)
	return &GChatClient{
		UserLogin:      login,
		listMessagesFn: list,
		getIntentForFn: func(context.Context, *bridgev2.Portal, bridgev2.EventSender, bridgev2.RemoteEventType) (bridgev2.MatrixAPI, bool) {
			return nil, true
		},
	}
}

// TestFetchMessagesThreadRootCallsListMessagesWithTopicFromAnchorMetadata
// pins the request-shape half of the brief: the topic id in the
// list_messages request comes from AnchorMessage.Metadata.TopicID, NOT from
// params.ThreadRoot (which is just a message id with no topic encoded in
// it -- task-2-augment.md's key design point). Also pins that
// params.Forward is TRUE on a real ThreadRoot-scoped call (verified against
// portalbackfill.go's fetchThreadBackfill/doThreadBackfill, which always set
// Forward:true even for this still-conceptually-backward/initial fetch) --
// under the naive "check Forward before ThreadRoot" dispatch order this
// would misroute into the Forward-unsupported stub and listMessagesFn would
// never be called at all.
func TestFetchMessagesThreadRootCallsListMessagesWithTopicFromAnchorMetadata(t *testing.T) {
	group := gcid.GroupID{ID: "space-1", IsDM: false}
	var gotReq *pb.ListMessagesRequest
	var calls int
	gc := newThreadBackfillTestClient("owner", func(_ context.Context, req *pb.ListMessagesRequest) (*pb.ListMessagesResponse, error) {
		calls++
		gotReq = req
		return &pb.ListMessagesResponse{}, nil
	})
	anchor := threadRootAnchor("head-1", "topic-1", 1000)

	resp, err := gc.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
		Portal:        flatBackfillPortal(group),
		ThreadRoot:    anchor.ID,
		Forward:       true,
		AnchorMessage: anchor,
		Count:         5,
	})
	if err != nil {
		t.Fatalf("FetchMessages returned error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("listMessagesFn called %d times, want 1 (single-shot, no page loop)", calls)
	}
	gotTopic := gotReq.GetParentId().GetTopicId()
	if gotTopic.GetTopicId() != "topic-1" {
		t.Errorf("ParentId.TopicId.TopicId = %q, want %q (read from AnchorMessage.Metadata.TopicID, not derived from ThreadRoot)", gotTopic.GetTopicId(), "topic-1")
	}
	id, isDM, ok := gchatmeow.GroupIDToParts(gotTopic.GetGroupId())
	if !ok || id != "space-1" || isDM {
		t.Errorf("ParentId.TopicId.GroupId = (%q, isDM=%v, ok=%v), want (%q, false, true)", id, isDM, ok, "space-1")
	}
	if got := gotReq.GetPageSize(); got != 5 {
		t.Errorf("PageSize = %d, want 5 (params.Count)", got)
	}
	if resp.HasMore {
		t.Error("HasMore = true, want false (single-shot)")
	}
	if resp.Cursor != "" {
		t.Errorf("Cursor = %q, want empty (no pagination)", resp.Cursor)
	}
}

// TestFetchMessagesThreadRootExcludesHeadAndOrdersChronologically pins the
// brief's "head message not duplicated" ask plus chronological ordering:
// list_messages may re-include the topic's head reply (Python's own
// handle_googlechat_message defends against exactly this via its DB
// existence check, portal.py:1348-1350) -- this must filter it out via the
// same anchor criterion fetchTopicHeadMessages uses (mirrored for the
// forward direction), and the two genuine replies must come back oldest
// first regardless of the server's delivery order.
func TestFetchMessagesThreadRootExcludesHeadAndOrdersChronologically(t *testing.T) {
	group := gcid.GroupID{ID: "space-1", IsDM: false}
	head := threadReply("head-1", "u1", "head text", 1_000_000)
	reply2 := threadReply("reply-2", "u2", "second", 3_000_000)
	reply1 := threadReply("reply-1", "u3", "first", 2_000_000)
	gc := newThreadBackfillTestClient("owner", func(context.Context, *pb.ListMessagesRequest) (*pb.ListMessagesResponse, error) {
		// Deliberately out of order and including the head, exactly the
		// defensive scenario this test exists to cover.
		return &pb.ListMessagesResponse{Messages: []*pb.Message{reply2, head, reply1}}, nil
	})
	anchor := threadRootAnchor("head-1", "topic-1", 1_000_000)

	resp, err := gc.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
		Portal:        flatBackfillPortal(group),
		ThreadRoot:    anchor.ID,
		Forward:       true,
		AnchorMessage: anchor,
		Count:         10,
	})
	if err != nil {
		t.Fatalf("FetchMessages returned error: %v", err)
	}
	if len(resp.Messages) != 2 {
		t.Fatalf("len(Messages) = %d, want 2 (head excluded, only the two real replies)", len(resp.Messages))
	}
	for _, m := range resp.Messages {
		if m.ID == gcid.MakeMessageID("head-1") {
			t.Error("head-1 was re-delivered by the ThreadRoot-scoped fetch, want excluded (already bridged as the topic's top-level message)")
		}
	}
	wantIDs := []networkid.MessageID{gcid.MakeMessageID("reply-1"), gcid.MakeMessageID("reply-2")}
	for i, want := range wantIDs {
		if resp.Messages[i].ID != want {
			t.Errorf("Messages[%d].ID = %q, want %q (chronological oldest-first)", i, resp.Messages[i].ID, want)
		}
	}
}

// TestFetchMessagesThreadRootKeepsDistinctSiblingAtSameMicrosecond pins the
// ThreadRoot-scoped analog of
// TestFetchMessagesFlatAnchorKeepsDistinctSiblingAtSameMicrosecond: the
// anchor filter's `msg.GetCreateTime() < anchorMicros` cutoff must exclude
// the head only by id match, and anything strictly OLDER than the anchor by
// time, while KEEPING a distinct reply that happens to share the anchor's
// exact microsecond CreateTime. The existing
// TestFetchMessagesThreadRootExcludesHeadAndOrdersChronologically test above
// has no same-microsecond non-head message, so a regression from `<` to
// `<=` would silently drop a genuine same-microsecond reply and still pass
// every other test in this file.
func TestFetchMessagesThreadRootKeepsDistinctSiblingAtSameMicrosecond(t *testing.T) {
	group := gcid.GroupID{ID: "space-1", IsDM: false}
	head := threadReply("head-1", "u1", "head text", 3_000_000)
	sibling := threadReply("reply-sibling", "u2", "sibling text", 3_000_000) // same µs as anchor, distinct id
	older := threadReply("reply-older", "u3", "older text", 1_000_000)       // strictly older: excluded
	newer := threadReply("reply-newer", "u4", "newer text", 5_000_000)       // strictly newer: kept
	gc := newThreadBackfillTestClient("owner", func(context.Context, *pb.ListMessagesRequest) (*pb.ListMessagesResponse, error) {
		// Deliberately out of order, mirroring the sibling test above.
		return &pb.ListMessagesResponse{Messages: []*pb.Message{newer, head, sibling, older}}, nil
	})
	anchor := threadRootAnchor("head-1", "topic-1", 3_000_000)

	resp, err := gc.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
		Portal:        flatBackfillPortal(group),
		ThreadRoot:    anchor.ID,
		Forward:       true,
		AnchorMessage: anchor,
		Count:         10,
	})
	if err != nil {
		t.Fatalf("FetchMessages returned error: %v", err)
	}
	got := map[networkid.MessageID]bool{}
	for _, m := range resp.Messages {
		got[m.ID] = true
	}
	if got[gcid.MakeMessageID("head-1")] {
		t.Error("head-1 was re-delivered, want excluded (id-matches the anchor)")
	}
	if got[gcid.MakeMessageID("reply-older")] {
		t.Error("reply-older was re-delivered, want excluded (strictly older than the anchor)")
	}
	if !got[gcid.MakeMessageID("reply-sibling")] {
		t.Error("reply-sibling (distinct id, same microsecond as anchor) was excluded, want kept")
	}
	if !got[gcid.MakeMessageID("reply-newer")] {
		t.Error("reply-newer (strictly newer than the anchor) was excluded, want kept")
	}
	if len(resp.Messages) != 2 {
		t.Fatalf("len(Messages) = %d, want 2 (reply-sibling + reply-newer)", len(resp.Messages))
	}
	wantIDs := []networkid.MessageID{gcid.MakeMessageID("reply-sibling"), gcid.MakeMessageID("reply-newer")}
	for i, want := range wantIDs {
		if resp.Messages[i].ID != want {
			t.Errorf("Messages[%d].ID = %q, want %q (chronological oldest-first)", i, resp.Messages[i].ID, want)
		}
	}
}

// TestFetchMessagesThreadRootNilAnchorReturnsEmptyResponse covers the
// defensive case the augment file calls out explicitly: with no anchor
// message there is no topic id to resolve, so this must return an empty
// response without ever calling listMessagesFn or panicking (rather than
// guessing at a topic).
func TestFetchMessagesThreadRootNilAnchorReturnsEmptyResponse(t *testing.T) {
	group := gcid.GroupID{ID: "space-1", IsDM: false}
	var calls int
	gc := newThreadBackfillTestClient("owner", func(context.Context, *pb.ListMessagesRequest) (*pb.ListMessagesResponse, error) {
		calls++
		return &pb.ListMessagesResponse{}, nil
	})

	resp, err := gc.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
		Portal:     flatBackfillPortal(group),
		ThreadRoot: gcid.MakeMessageID("head-1"),
		Forward:    true,
		Count:      3,
		// AnchorMessage deliberately left nil.
	})
	if err != nil {
		t.Fatalf("FetchMessages returned error: %v", err)
	}
	if calls != 0 {
		t.Error("listMessagesFn was called with a nil AnchorMessage, want not called (no topic id to resolve)")
	}
	if resp.HasMore || len(resp.Messages) != 0 {
		t.Errorf("resp = %+v, want empty/HasMore=false", resp)
	}
}

// TestFetchMessagesThreadRootAnchorMissingTopicIDReturnsEmptyResponse covers
// an anchor message that exists but carries no usable topic id (nil
// Metadata, e.g. a migrated/legacy row) -- must not guess, must not panic.
func TestFetchMessagesThreadRootAnchorMissingTopicIDReturnsEmptyResponse(t *testing.T) {
	group := gcid.GroupID{ID: "space-1", IsDM: false}
	var calls int
	gc := newThreadBackfillTestClient("owner", func(context.Context, *pb.ListMessagesRequest) (*pb.ListMessagesResponse, error) {
		calls++
		return &pb.ListMessagesResponse{}, nil
	})
	anchor := &database.Message{ID: gcid.MakeMessageID("head-1"), Timestamp: gchatmeow.MicrosToTime(1000)} // no Metadata at all

	resp, err := gc.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
		Portal:        flatBackfillPortal(group),
		ThreadRoot:    anchor.ID,
		Forward:       true,
		AnchorMessage: anchor,
		Count:         3,
	})
	if err != nil {
		t.Fatalf("FetchMessages returned error: %v", err)
	}
	if calls != 0 {
		t.Error("listMessagesFn was called despite the anchor carrying no topic id, want not called")
	}
	if resp.HasMore || len(resp.Messages) != 0 {
		t.Errorf("resp = %+v, want empty/HasMore=false", resp)
	}
}

// TestFetchMessagesThreadRootConvertReusesLiveConversionPath mirrors
// TestFetchMessagesFlatConvertMessageReusesLiveConversionPath for the
// ThreadRoot-scoped path: a formatted thread reply must produce identical
// rendered HTML through fetchThreadMessages as calling convertMessageToMatrix
// directly -- no parallel conversion path for the threaded case either.
func TestFetchMessagesThreadRootConvertReusesLiveConversionPath(t *testing.T) {
	group := gcid.GroupID{ID: "space-1", IsDM: false}
	reply := &pb.Message{
		Id:          &pb.MessageId{MessageId: proto.String("reply-1")},
		Creator:     &pb.User{UserId: &pb.UserId{Id: proto.String("u1")}},
		CreateTime:  proto.Int64(2000),
		TextBody:    proto.String("bold text"),
		Annotations: []*pb.Annotation{gchatfmt.MakeFormatAnnotation(0, 4, pb.FormatMetadata_BOLD)},
	}
	gc := newThreadBackfillTestClient("owner", func(context.Context, *pb.ListMessagesRequest) (*pb.ListMessagesResponse, error) {
		return &pb.ListMessagesResponse{Messages: []*pb.Message{reply}}, nil
	})
	portal := flatBackfillPortal(group)
	anchor := threadRootAnchor("head-1", "topic-1", 1000)

	resp, err := gc.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
		Portal:        portal,
		ThreadRoot:    anchor.ID,
		Forward:       true,
		AnchorMessage: anchor,
		Count:         5,
	})
	if err != nil {
		t.Fatalf("FetchMessages returned error: %v", err)
	}
	if len(resp.Messages) != 1 {
		t.Fatalf("len(Messages) = %d, want 1", len(resp.Messages))
	}
	backfillCM := resp.Messages[0].ConvertedMessage
	if backfillCM == nil || len(backfillCM.Parts) != 1 {
		t.Fatalf("backfill ConvertedMessage = %+v, want 1 part", backfillCM)
	}

	convert := convertMessageToMatrix(gc.msgConverter(), gc)
	liveCM, err := convert(context.Background(), portal, nil, reply)
	if err != nil {
		t.Fatalf("live convertMessageToMatrix returned error: %v", err)
	}
	if backfillCM.Parts[0].Content.FormattedBody != liveCM.Parts[0].Content.FormattedBody {
		t.Errorf("backfill formatted body = %q, live formatted body = %q, want identical", backfillCM.Parts[0].Content.FormattedBody, liveCM.Parts[0].Content.FormattedBody)
	}
	if backfillCM.Parts[0].Content.FormattedBody == "" {
		t.Error("formatted body is empty, want BOLD annotation to have rendered HTML")
	}
}
