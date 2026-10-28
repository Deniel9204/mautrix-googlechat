package connector

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

	gc.catchUp(context.Background())
}

// TestCatchUpSkippedWhileFirstSyncInProgress: a reconnect during the first sync must not catch up from a zero watermark.
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

// TestCatchUpDrainsAllPagesInOneInvocation: catchUp must drain every PAGINATED page in one call, advancing the cursor.
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
	// Each page's from-cursor is the previous page's max revision.
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

// TestCatchUpDrainStopsIfCursorDoesNotAdvance: PAGINATED pages that don't advance the cursor must stop the drain.
func TestCatchUpDrainStopsIfCursorDoesNotAdvance(t *testing.T) {
	meta := &UserLoginMetadata{Revision: 100}
	login := newTestUserLogin(meta)
	login.ID = gcid.MakeUserLoginID("112233")
	// Revision fixed at 150: the second page does not advance the cursor.
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

// TestCatchUpSplitsMultiBodyEventsBeforeDispatch: each body (field 8) of a caught-up event is dispatched separately.
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

// TestCatchUpDrainReplaysMutationEventsThroughHandleGChatEvent: edit/delete/reaction replay through the live dispatch.
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

// TestCatchUpNoNewRevisionSkipsSave: an empty COMPLETED response must not write metadata at all.
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
	// Wait for syncChats to fully return, not just be called.
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

// TestCatchUpRaceWithConcurrentCookiePersist: the watermark write and persistCookies both go through metaMu
// and both must land; run under -race.
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

// waitUntilSyncNotInProgress polls gc.isSyncInProgress() until false or timeout.
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

// TestHandleConnStateSetsSyncInProgressSynchronously: the flag is set before the sync goroutine is spawned.
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

// TestHandleGChatEventGroupRevisionDoesNotAdvanceUserWatermark: group_revision goes to the portal watermark, never the user one.
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

// TestHandleGChatEventFailedQueueLeavesUserWatermarkUnchanged: the watermark advances only after a successful queue.
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

// TestCatchUpStopsDrainOnQueueFailure: a failed event stops the drain so later pages can't advance past it.
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

// flatBackfillPortal builds a flat portal whose Bridge resolves ghost mentions deterministically.
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

// newBackfillTestClient wires listTopicsFn and stubs getIntentForFn with a nil intent.
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

// flatTopic builds a non-threaded topic with a single reply.
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

// TestFetchMessagesFlatCallsListTopicsOnceWithGroupAndCount: one ListTopics call, PageSizeForTopics = Count, HasMore=false.
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

// TestFetchMessagesFlatOrdersChronologicallyWithCorrectFields: newest-first server order is returned oldest-first.
func TestFetchMessagesFlatOrdersChronologicallyWithCorrectFields(t *testing.T) {
	group := gcid.GroupID{ID: "space-1", IsDM: false}
	// Server order: newest first.
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

// TestFetchMessagesFlatOmitsReactionsEvenWhenGCMessageHasThem: GC's Reaction proto has no reactor id,
// so backfilled reactions must stay nil even with Count>0 and CurrentUserParticipated.
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

// TestFetchMessagesFlatSortTimeTieBreaksByTopicID: SortTime ties sort by topic id, not server order.
func TestFetchMessagesFlatSortTimeTieBreaksByTopicID(t *testing.T) {
	group := gcid.GroupID{ID: "space-1", IsDM: false}
	// Message ids don't correlate with topic ids, so the tiebreaker must key on topic id.
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
	// topic-1 < topic-2, so msg-zzz sorts first.
	want := []networkid.MessageID{gcid.MakeMessageID("msg-zzz"), gcid.MakeMessageID("msg-aaa")}
	for i, w := range want {
		if resp.Messages[i].ID != w {
			t.Errorf("Messages[%d].ID = %q, want %q (deterministic topic-id tiebreak on a SortTime tie)", i, resp.Messages[i].ID, w)
		}
	}
}

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

// TestFetchMessagesFlatConvertMessageReusesLiveConversionPath: backfill renders the same HTML as convertMessageToMatrix.
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

// TestFetchMessagesFlatAnchorKeepsDistinctSiblingAtSameMicrosecond: only the anchor itself and newer messages are dropped.
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

// A flat topic with thread_created_usec > 0 gets ShouldBackfillThread; a plain flat topic does not.

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

// Forward with a nil anchor is the new-room seed; with an anchor it is catch-up and stays empty.

func TestFetchMessagesForwardNilAnchorSeedsInitialBackfill(t *testing.T) {
	group := gcid.GroupID{ID: "space-1", IsDM: false}
	var calls int
	gc := newBackfillTestClient("owner", func(_ context.Context, req *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
		calls++
		return &pb.ListTopicsResponse{
			Topics:             []*pb.Topic{flatTopic("t1", "m1", "u1", "hi", 1_000_000)},
			ContainsFirstTopic: proto.Bool(true),
		}, nil
	})

	resp, err := gc.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
		Portal:  flatBackfillPortal(group),
		Forward: true,
		// Nil AnchorMessage: the new-room bootstrap seed.
		Count: 3,
	})
	if err != nil {
		t.Fatalf("FetchMessages returned error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("listTopicsFn called %d times, want 1 (forward seed must reach fetchTopicHeadMessages)", calls)
	}
	if len(resp.Messages) != 1 {
		t.Fatalf("len(Messages) = %d, want 1 (topic head)", len(resp.Messages))
	}
	if resp.Messages[0].ID != gcid.MakeMessageID("m1") {
		t.Errorf("Messages[0].ID = %q, want %q", resp.Messages[0].ID, gcid.MakeMessageID("m1"))
	}
	if resp.HasMore {
		t.Error("HasMore = true, want false (single-shot)")
	}
}

func TestFetchMessagesForwardWithAnchorReturnsEmptyResponse(t *testing.T) {
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
		AnchorMessage: &database.Message{
			ID:        gcid.MakeMessageID("m-last"),
			Timestamp: gchatmeow.MicrosToTime(1_000_000),
		},
	})
	if err != nil {
		t.Fatalf("FetchMessages returned error: %v", err)
	}
	if called {
		t.Error("listTopicsFn was called for a forward catch-up (Forward=true, AnchorMessage!=nil) request, want not called (M2's catch_up_user owns reconnect-gap recovery for existing rooms)")
	}
	if resp.HasMore || len(resp.Messages) != 0 {
		t.Errorf("resp = %+v, want empty/HasMore=false", resp)
	}
}

func TestFetchMessagesBackwardUnchanged(t *testing.T) {
	group := gcid.GroupID{ID: "space-1", IsDM: false}
	gc := newBackfillTestClient("owner", func(context.Context, *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
		return &pb.ListTopicsResponse{
			Topics:             []*pb.Topic{flatTopic("t1", "m1", "u1", "hi", 1_000_000)},
			ContainsFirstTopic: proto.Bool(true),
		}, nil
	})

	resp, err := gc.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
		Portal:  flatBackfillPortal(group),
		Forward: false,
		Count:   3,
	})
	if err != nil {
		t.Fatalf("FetchMessages returned error: %v", err)
	}
	if len(resp.Messages) != 1 {
		t.Fatalf("len(Messages) = %d, want 1 (backward path unaffected by the Forward-branch fix)", len(resp.Messages))
	}
}

// TestFetchMessagesThreadsOnlyTopLevelHeadsHaveShouldBackfillThread: every ThreadsOnly topic head gets ShouldBackfillThread.
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

func TestFetchMessagesThreadsOnlyForwardNilAnchorSeedsWithShouldBackfillThread(t *testing.T) {
	group := gcid.GroupID{ID: "space-1", IsDM: false}
	gc := newBackfillTestClient("owner", func(context.Context, *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
		return &pb.ListTopicsResponse{
			Topics:             []*pb.Topic{flatTopic("t1", "m1", "u1", "hi", 1_000_000)},
			ContainsFirstTopic: proto.Bool(true),
		}, nil
	})
	portal := flatBackfillPortal(group)
	portal.Metadata = &PortalMetadata{ThreadsOnly: true}

	resp, err := gc.FetchMessages(context.Background(), bridgev2.FetchMessagesParams{
		Portal:  portal,
		Forward: true,
		Count:   3,
		// Nil AnchorMessage: the new-room bootstrap seed.
	})
	if err != nil {
		t.Fatalf("FetchMessages returned error: %v", err)
	}
	if len(resp.Messages) != 1 {
		t.Fatalf("len(Messages) = %d, want 1", len(resp.Messages))
	}
	if !resp.Messages[0].ShouldBackfillThread {
		t.Error("ShouldBackfillThread = false, want true (ThreadsOnly portal, forward seed)")
	}
	if resp.Messages[0].LastThreadMessage != gcid.MakeMessageID("m1") {
		t.Errorf("LastThreadMessage = %q, want %q", resp.Messages[0].LastThreadMessage, "m1")
	}
}

// threadRootAnchor builds a ThreadRoot anchor whose MessageMetadata.TopicID fetchThreadMessages reads.
func threadRootAnchor(msgID, topicID string, createMicros int64) *database.Message {
	return &database.Message{
		ID:        gcid.MakeMessageID(msgID),
		Timestamp: gchatmeow.MicrosToTime(createMicros),
		Metadata:  &MessageMetadata{TopicID: topicID},
	}
}

func threadReply(msgID, creatorGaia, text string, createTime int64) *pb.Message {
	return &pb.Message{
		Id:         &pb.MessageId{MessageId: proto.String(msgID)},
		Creator:    &pb.User{UserId: &pb.UserId{Id: proto.String(creatorGaia)}},
		CreateTime: proto.Int64(createTime),
		TextBody:   proto.String(text),
	}
}

// newThreadBackfillTestClient is newBackfillTestClient wired to listMessagesFn.
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

// TestFetchMessagesThreadRootCallsListMessagesWithTopicFromAnchorMetadata: the topic comes from anchor metadata,
// and Forward=true must not misroute a ThreadRoot call.
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

// TestFetchMessagesThreadRootExcludesHeadAndOrdersChronologically: a re-sent head is filtered, replies come oldest first.
func TestFetchMessagesThreadRootExcludesHeadAndOrdersChronologically(t *testing.T) {
	group := gcid.GroupID{ID: "space-1", IsDM: false}
	head := threadReply("head-1", "u1", "head text", 1_000_000)
	reply2 := threadReply("reply-2", "u2", "second", 3_000_000)
	reply1 := threadReply("reply-1", "u3", "first", 2_000_000)
	gc := newThreadBackfillTestClient("owner", func(context.Context, *pb.ListMessagesRequest) (*pb.ListMessagesResponse, error) {
		// Out of order and including the head.
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

// TestFetchMessagesThreadRootKeepsDistinctSiblingAtSameMicrosecond: guards against `<` regressing to `<=`.
func TestFetchMessagesThreadRootKeepsDistinctSiblingAtSameMicrosecond(t *testing.T) {
	group := gcid.GroupID{ID: "space-1", IsDM: false}
	head := threadReply("head-1", "u1", "head text", 3_000_000)
	sibling := threadReply("reply-sibling", "u2", "sibling text", 3_000_000) // same µs as anchor, distinct id
	older := threadReply("reply-older", "u3", "older text", 1_000_000)       // strictly older: excluded
	newer := threadReply("reply-newer", "u4", "newer text", 5_000_000)       // strictly newer: kept
	gc := newThreadBackfillTestClient("owner", func(context.Context, *pb.ListMessagesRequest) (*pb.ListMessagesResponse, error) {
		// Deliberately out of order.
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
