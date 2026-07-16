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
// behind using list_topics (not list_messages) and the cumulative-request
// cursor encoding these tests pin down.

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

// --- first page: no cursor, request shape -----------------------------

func TestFetchMessagesFlatFirstPageCallsListTopicsWithGroupAndCount(t *testing.T) {
	group := gcid.GroupID{ID: "space-1", IsDM: false}
	var gotReq *pb.ListTopicsRequest
	gc := newBackfillTestClient("112233", func(_ context.Context, req *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
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
	if gotReq == nil {
		t.Fatal("listTopicsFn was not called")
	}
	if got := gotReq.GetPageSizeForTopics(); got != 3 {
		t.Errorf("PageSizeForTopics = %d, want 3 (Count, no prior delivery)", got)
	}
	id, isDM, ok := gchatmeow.GroupIDToParts(gotReq.GetGroupId())
	if !ok || id != "space-1" || isDM {
		t.Errorf("GroupId = (%q, isDM=%v, ok=%v), want (%q, false, true)", id, isDM, ok, "space-1")
	}
	if len(resp.Messages) != 1 {
		t.Fatalf("len(Messages) = %d, want 1", len(resp.Messages))
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

// --- subsequent page: cursor threading + no re-delivery -----------------

// TestFetchMessagesFlatSubsequentPageGrowsRequestAndSkipsAlreadyDelivered
// pins the cumulative-request cursor design (backfill.go's region doc
// comment): page 2's request must ask for MORE topics than page 1 (delivered
// + Count), and the messages it returns must be exactly the NEXT older
// batch, not a repeat of page 1's.
func TestFetchMessagesFlatSubsequentPageGrowsRequestAndSkipsAlreadyDelivered(t *testing.T) {
	group := gcid.GroupID{ID: "space-1", IsDM: false}
	// 6 topics total, SortTime 10..60 (60 = newest). A real server always
	// returns "the N most recent" for a given page size.
	all := []*pb.Topic{
		flatTopic("t6", "m6", "u", "six", 60),
		flatTopic("t5", "m5", "u", "five", 50),
		flatTopic("t4", "m4", "u", "four", 40),
		flatTopic("t3", "m3", "u", "three", 30),
		flatTopic("t2", "m2", "u", "two", 20),
		flatTopic("t1", "m1", "u", "one", 10),
	}
	var gotSizes []int32
	gc := newBackfillTestClient("owner", func(_ context.Context, req *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
		n := req.GetPageSizeForTopics()
		gotSizes = append(gotSizes, n)
		if int(n) > len(all) {
			n = int32(len(all))
		}
		return &pb.ListTopicsResponse{
			Topics:             all[:n], // newest-first prefix of length n
			ContainsFirstTopic: proto.Bool(int(n) >= len(all)),
		}, nil
	})
	portal := flatBackfillPortal(group)

	page1, err := gc.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{Portal: portal, Count: 3})
	if err != nil {
		t.Fatalf("page 1: FetchMessages returned error: %v", err)
	}
	if len(page1.Messages) != 3 {
		t.Fatalf("page 1: len(Messages) = %d, want 3", len(page1.Messages))
	}
	page1IDs := []networkid.MessageID{page1.Messages[0].ID, page1.Messages[1].ID, page1.Messages[2].ID}
	wantPage1 := []networkid.MessageID{gcid.MakeMessageID("m4"), gcid.MakeMessageID("m5"), gcid.MakeMessageID("m6")}
	for i := range wantPage1 {
		if page1IDs[i] != wantPage1[i] {
			t.Errorf("page 1 Messages[%d] = %q, want %q", i, page1IDs[i], wantPage1[i])
		}
	}
	if !page1.HasMore {
		t.Error("page 1: HasMore = false, want true (3 of 6 topics delivered)")
	}

	page2, err := gc.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
		Portal: portal,
		Count:  3,
		Cursor: page1.Cursor,
	})
	if err != nil {
		t.Fatalf("page 2: FetchMessages returned error: %v", err)
	}
	if len(gotSizes) != 2 || gotSizes[1] <= gotSizes[0] {
		t.Fatalf("request sizes = %v, want page 2's PageSizeForTopics > page 1's (cumulative growth)", gotSizes)
	}
	if len(page2.Messages) != 3 {
		t.Fatalf("page 2: len(Messages) = %d, want 3", len(page2.Messages))
	}
	wantPage2 := []networkid.MessageID{gcid.MakeMessageID("m1"), gcid.MakeMessageID("m2"), gcid.MakeMessageID("m3")}
	for i, want := range wantPage2 {
		if page2.Messages[i].ID != want {
			t.Errorf("page 2 Messages[%d] = %q, want %q (next OLDER batch, not a repeat of page 1)", i, page2.Messages[i].ID, want)
		}
	}
	if page2.HasMore {
		t.Error("page 2: HasMore = true, want false (all 6 topics now delivered, ContainsFirstTopic)")
	}
}

// --- last page: HasMore false ------------------------------------------

func TestFetchMessagesFlatServerReturnsFewerThanRequestedSetsHasMoreFalse(t *testing.T) {
	group := gcid.GroupID{ID: "space-1", IsDM: false}
	// Only 2 topics exist; asked for 5 (Count=5, first page) -- the server
	// can only return 2, which alone (regardless of ContainsFirstTopic) means
	// there is nothing more to page into.
	gc := newBackfillTestClient("owner", func(context.Context, *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
		return &pb.ListTopicsResponse{
			Topics: []*pb.Topic{
				flatTopic("t2", "m2", "u", "two", 20),
				flatTopic("t1", "m1", "u", "one", 10),
			},
			// Deliberately NOT setting ContainsFirstTopic, to prove the
			// "server returned fewer than requested" signal alone is
			// sufficient.
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
		t.Error("HasMore = true, want false (server returned fewer topics than requested)")
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

// --- Forward / ThreadRoot / ThreadsOnly: out of Task 1's scope -----------

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

func TestFetchMessagesThreadRootReturnsEmptyResponse(t *testing.T) {
	group := gcid.GroupID{ID: "space-1", IsDM: false}
	var called bool
	gc := newBackfillTestClient("owner", func(context.Context, *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
		called = true
		return &pb.ListTopicsResponse{}, nil
	})

	resp, err := gc.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
		Portal:     flatBackfillPortal(group),
		ThreadRoot: gcid.MakeMessageID("topic-1"),
		Count:      3,
	})
	if err != nil {
		t.Fatalf("FetchMessages returned error: %v", err)
	}
	if called {
		t.Error("listTopicsFn was called for a ThreadRoot request, want not called (Task 2's scope)")
	}
	if resp.HasMore || len(resp.Messages) != 0 {
		t.Errorf("resp = %+v, want empty/HasMore=false", resp)
	}
}

func TestFetchMessagesThreadsOnlyPortalReturnsEmptyResponse(t *testing.T) {
	group := gcid.GroupID{ID: "space-1", IsDM: false}
	var called bool
	gc := newBackfillTestClient("owner", func(context.Context, *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
		called = true
		return &pb.ListTopicsResponse{}, nil
	})
	portal := flatBackfillPortal(group)
	portal.Metadata = &PortalMetadata{ThreadsOnly: true}

	resp, err := gc.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{Portal: portal, Count: 3})
	if err != nil {
		t.Fatalf("FetchMessages returned error: %v", err)
	}
	if called {
		t.Error("listTopicsFn was called for a ThreadsOnly portal's top-level request, want not called (Task 2's scope)")
	}
	if resp.HasMore || len(resp.Messages) != 0 {
		t.Errorf("resp = %+v, want empty/HasMore=false", resp)
	}
}

// --- cursor codec ---------------------------------------------------------

func TestBackfillCursorRoundTrips(t *testing.T) {
	for _, delivered := range []int{0, 1, 42, 12345} {
		cursor := encodeBackfillCursor(delivered)
		got, err := decodeBackfillCursor(cursor)
		if err != nil {
			t.Fatalf("decodeBackfillCursor(%q) error: %v", cursor, err)
		}
		if got != delivered {
			t.Errorf("round trip %d -> %q -> %d, want %d", delivered, cursor, got, delivered)
		}
	}
}

func TestDecodeBackfillCursorEmptyIsZero(t *testing.T) {
	got, err := decodeBackfillCursor("")
	if err != nil || got != 0 {
		t.Errorf("decodeBackfillCursor(\"\") = (%d, %v), want (0, nil)", got, err)
	}
}

func TestDecodeBackfillCursorRejectsGarbage(t *testing.T) {
	// "9999999999" is > maxBackfillCursor: a corrupted/foreign cursor that,
	// unguarded, would overflow the int32 PageSizeForTopics cast.
	for _, bad := range []networkid.PaginationCursor{"not-a-number", "-1", "9999999999"} {
		if _, err := decodeBackfillCursor(bad); err == nil {
			t.Errorf("decodeBackfillCursor(%q) = nil error, want error", bad)
		}
	}
}

// TestFetchMessagesFlatCursorLargerThanAvailableClampsToNoNewMessages pins
// the clamp at backfill.go's `if delivered > len(topics)` guard: a cursor
// claiming more delivered than the server now returns (e.g. history was
// trimmed/retention-expired between pages) must not panic on a negative slice
// bound, and must yield zero new messages with HasMore=false rather than
// re-delivering the whole page.
func TestFetchMessagesFlatCursorLargerThanAvailableClampsToNoNewMessages(t *testing.T) {
	group := gcid.GroupID{ID: "space-1", IsDM: false}
	gc := newBackfillTestClient("owner", func(context.Context, *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
		return &pb.ListTopicsResponse{
			Topics:             []*pb.Topic{flatTopic("t1", "m1", "u", "one", 10)},
			ContainsFirstTopic: proto.Bool(true),
		}, nil
	})

	resp, err := gc.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
		Portal: flatBackfillPortal(group),
		Count:  3,
		Cursor: encodeBackfillCursor(5), // claims 5 delivered, server has 1
	})
	if err != nil {
		t.Fatalf("FetchMessages returned error: %v", err)
	}
	if len(resp.Messages) != 0 {
		t.Fatalf("len(Messages) = %d, want 0 (cursor claims more delivered than exist)", len(resp.Messages))
	}
	if resp.HasMore {
		t.Error("HasMore = true, want false (nothing left)")
	}
}

// TestFetchMessagesFlatAnchorGrowsBetweenPagesNoDuplicate pins the
// concurrent-topic-creation safety property documented in backfill.go's
// region comment: if a NEW topic is created between page 1 and page 2 (live
// traffic during a backfill run), the positional cursor momentarily
// re-includes an already-delivered topic, but the anchor filter (refreshed by
// the framework from the DB before every call) strips it -- so no message is
// delivered twice. Simulates the framework's own anchor refresh by passing
// page 2's AnchorMessage as the OLDEST message page 1 delivered.
func TestFetchMessagesFlatAnchorGrowsBetweenPagesNoDuplicate(t *testing.T) {
	group := gcid.GroupID{ID: "space-1", IsDM: false}
	// Initial history: 4 topics, SortTime 10..40 (newest = 40).
	base := []*pb.Topic{
		flatTopic("t4", "m4", "u", "four", 40),
		flatTopic("t3", "m3", "u", "three", 30),
		flatTopic("t2", "m2", "u", "two", 20),
		flatTopic("t1", "m1", "u", "one", 10),
	}
	var callNum int
	gc := newBackfillTestClient("owner", func(_ context.Context, req *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
		callNum++
		topics := base
		if callNum >= 2 {
			// A brand-new topic m5 (SortTime 50) arrived after page 1 -- now
			// the "most recent N" ranking includes it at the front.
			topics = append([]*pb.Topic{flatTopic("t5", "m5", "u", "five", 50)}, base...)
		}
		n := req.GetPageSizeForTopics()
		if int(n) > len(topics) {
			n = int32(len(topics))
		}
		return &pb.ListTopicsResponse{Topics: topics[:n], ContainsFirstTopic: proto.Bool(int(n) >= len(topics))}, nil
	})
	portal := flatBackfillPortal(group)

	// Page 1: newest 2 of {10..40} -> m3, m4 (ascending).
	page1, err := gc.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{Portal: portal, Count: 2})
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	if len(page1.Messages) != 2 || page1.Messages[0].ID != gcid.MakeMessageID("m3") || page1.Messages[1].ID != gcid.MakeMessageID("m4") {
		t.Fatalf("page 1 messages = %v, want [m3 m4]", backfillIDs(page1))
	}

	// Page 2: framework refreshes AnchorMessage to the OLDEST message page 1
	// delivered (m3, ts=30). A new topic m5 has since arrived. Without the
	// anchor filter the positional cursor would re-include m4 (or worse); with
	// it, only strictly-older-than-m3 messages come through, no duplicate.
	page2, err := gc.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
		Portal: portal,
		Count:  2,
		Cursor: page1.Cursor,
		AnchorMessage: &database.Message{
			ID:        gcid.MakeMessageID("m3"),
			Timestamp: gchatmeow.MicrosToTime(30),
		},
	})
	if err != nil {
		t.Fatalf("page 2: %v", err)
	}
	// Assert no message from page 1 reappears in page 2, and nothing at/after
	// the anchor leaks through.
	delivered := map[networkid.MessageID]bool{}
	for _, m := range page1.Messages {
		delivered[m.ID] = true
	}
	for _, m := range page2.Messages {
		if delivered[m.ID] {
			t.Errorf("page 2 re-delivered %q, want no duplicates across the concurrent-growth boundary", m.ID)
		}
		if m.Timestamp.UnixMicro() >= 30 {
			t.Errorf("page 2 delivered %q at ts=%d, want strictly older than the anchor (30)", m.ID, m.Timestamp.UnixMicro())
		}
	}
}

// backfillIDs is a small test helper: the message IDs of a response, for
// readable failure messages.
func backfillIDs(resp *bridgev2.FetchMessagesResponse) []networkid.MessageID {
	ids := make([]networkid.MessageID, len(resp.Messages))
	for i, m := range resp.Messages {
		ids[i] = m.ID
	}
	return ids
}

// --- CanBackfill wiring compile-time proof --------------------------------

// TestBackfillingNetworkAPIAssertion is a runtime pin of the compile-time
// assertion already in backfill.go (var _ bridgev2.BackfillingNetworkAPI =
// (*GChatClient)(nil)); kept here too so a future refactor that accidentally
// breaks the interface satisfaction shows up as a normal test failure
// message, not just a build error somewhere else in the package.
func TestBackfillingNetworkAPIAssertion(t *testing.T) {
	var _ bridgev2.BackfillingNetworkAPI = (*GChatClient)(nil)
}
