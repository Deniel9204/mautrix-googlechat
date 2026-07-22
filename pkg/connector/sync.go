package connector

// sync.go -- chat-list sync: list the "world" (every conversation the
// account is in), then emit one simplevent.ChatResync per chat so bridgev2
// creates/backfill-checks portals for it. Ports mautrix_googlechat/user.py's
// sync() (user.py:609-665).
import (
	"context"
	"sort"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/simplevent"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// syncMaxAttempts bounds syncChats' paginated_world retry loop (see
// fetchWorldWithRetry): up to this many attempts total before giving up and
// resetting the sync latch (GChatClient.resetSyncLatch) so a later
// Connected transition tries again. This absorbs a single transient RPC
// blip -- the M1 whole-branch review bug this retry loop fixes -- without
// turning a persistently-broken connection into a long, silent stall inside
// handleConnState's Connected branch (which runs syncChats in its own
// goroutine specifically so a slow sync never blocks that; see its doc
// comment in client.go).
const syncMaxAttempts = 3

// defaultSyncRetryBackoffBase is syncChats' base retry delay when
// GChatClient.syncRetryBackoffBase is zero (the production default; tests
// override the field to a tiny duration so the retry tests run in
// milliseconds instead of waiting on real wall-clock backoff). Doubles each
// attempt: attempt 1 waits this long, attempt 2 waits 2x, and so on.
const defaultSyncRetryBackoffBase = 500 * time.Millisecond

// worldSectionPageSize is the page_size on the world_section_requests entry
// that fetchWorldWithRetry must send for paginated_world to return any
// world_items at all (see the request builder below). Matches the maintained
// purple-googlechat client's value.
const worldSectionPageSize = 999

// syncChatItem pairs one (post-filter) world item with whether it's within
// this login's initial_chat_sync cap.
type syncChatItem struct {
	Item         *pb.WorldItemLite
	CreatePortal bool
}

// planChatSync filters, sorts, and caps a raw PaginatedWorld item list into
// the per-chat sync plan syncChats emits as ChatResync events. Ports
// user.py's sync() loop (user.py:623-641):
//
//   - sort by sort_timestamp descending (user.py:624) -- sort.SliceStable so
//     ties keep the server's original relative order, matching Python
//     list.sort's stability guarantee;
//   - walk the FULL sorted list with an absolute position counter and skip
//     conditions copied verbatim from user.py:630-635 (blocked / hidden
//     (hide_timestamp > 0) / not MEMBER_JOINED) -- critically, a skipped
//     item still advances the position counter for every item after it,
//     exactly like Python's `for index, item in enumerate(items): ...
//     continue` (the enumerate index is over the unfiltered, sorted list;
//     `continue` does not "collapse" it). An earlier version of this
//     function filtered first and then indexed only the survivors, which
//     silently shifted the cap boundary earlier by one slot for every
//     skipped chat ahead of it -- e.g. with maxSync=2 and the single most
//     recent chat blocked, that version would auto-create portals for the
//     2nd and 3rd most recent chats instead of just the 2nd, diverging from
//     Python whenever a skipped chat ranks within the cap window;
//   - cap at maxSync (bridge.initial_chat_sync, user.py:625): the newest
//     maxSync items BY ABSOLUTE POSITION in the full sorted list are marked
//     CreatePortal true, the rest false.
//
// Deviation from user.py's literal loop: Python additionally keeps (marks
// for update/backfill, not creation) every item beyond the cap that ALREADY
// has a local portal.mxid (user.py:638, "if portal.mxid or index <
// max_sync"), and skips the rest outright rather than emitting anything for
// them. This function has no access to which portals already exist (it's a
// pure, unit-testable transform - see task-12-brief.md's testing note on
// this), so it takes the simpler "emit for every post-filter item" approach
// instead: bridgev2's own event dispatch (portal.go's handleRemoteEvent)
// already drops a CreatePortal:false ChatResync when no portal exists yet
// and processes it normally (update+backfill-check) when one does -- the
// same net effect as user.py's gate, just enforced downstream instead of
// here.
func planChatSync(items []*pb.WorldItemLite, maxSync int) []syncChatItem {
	sorted := make([]*pb.WorldItemLite, len(items))
	copy(sorted, items)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].GetSortTimestamp() > sorted[j].GetSortTimestamp()
	})

	plan := make([]syncChatItem, 0, len(sorted))
	for i, item := range sorted {
		rs := item.GetReadState()
		if rs.GetBlocked() || rs.GetHideTimestamp() > 0 || rs.GetMembershipState() != pb.MembershipState_MEMBER_JOINED {
			continue
		}
		plan = append(plan, syncChatItem{Item: item, CreatePortal: i < maxSync})
	}
	return plan
}

// syncChats lists the world (every conversation) via paginated_world
// (client.py:700, proto_paginated_world) and queues one
// simplevent.ChatResync per (post-filter) chat, matching the request shape
// user.py's sync() builds when called with no limit (user.py:613-619):
// fetch_from_user_spaces=true, fetch_options=[EXCLUDE_GROUP_LITE]. Intended
// to run once Connect reaches CONNECTED (client.go's handleConnState), same
// as Python's on_connect_later calling self.sync() right before pushing
// BridgeStateEvent.CONNECTED (user.py:555-560).
//
// Deviations from user.py's sync(): no `limit` parameter (that's user.py's
// periodic-recheck/reconnect-triggered "small sync", not part of this
// task's scope -- M1 only wires the one-time post-connect sync); no
// prefetch_users batching of DM member GetMembers calls (user.py:627,646 --
// bridgev2 already batches ghost info lookups on its own portal-creation
// path); no update_direct_chats()/m.direct sync at the end (user.py:664 --
// bridgev2 maintains m.direct itself from portal state, this bridge never
// needs to push it directly).
//
// Clears syncInProgress (client.go) when it finishes, via defer -- the
// caller (handleConnState) sets it true synchronously BEFORE spawning this
// goroutine, so backfill.go's catchUp, which can run concurrently if a
// second Connected transition lands before this one finishes, sees the flag
// true the instant the first Connected was handled and defers its
// catch_up_user call rather than racing an unfinished first sync
// (gchat-port-auditor P1 finding, M2 Task 7). Direct callers in tests must
// set the flag themselves first to mirror that contract.
func (c *GChatClient) syncChats(ctx context.Context) {
	log := zerolog.Ctx(ctx)
	defer c.setSyncInProgress(false)

	fetch := c.paginatedWorldFn
	if fetch == nil {
		conn := c.getConn()
		if conn == nil {
			log.Warn().Msg("googlechat: syncChats called with no live connection, skipping")
			c.resetSyncLatch()
			return
		}
		fetch = conn.PaginatedWorld
	}

	resp, err := c.fetchWorldWithRetry(ctx, fetch)
	if err != nil {
		log.Err(err).Msg("googlechat: paginated_world failed after retries, skipping chat-list sync (latch reset so a later Connected transition retries)")
		c.resetSyncLatch()
		return
	}

	// Set the login's own RemoteName (from get_members) BEFORE queuing the
	// chat resyncs below: the first portal creation builds the per-login
	// "personal filtering space", whose name is "Google Chat (<RemoteName>)"
	// -- doing this first stops it coming out as "Google Chat ()".
	c.updateOwnLoginProfile(ctx)

	maxSync := 0
	if c.Main != nil {
		maxSync = c.Main.Config.InitialChatSync
	}
	plan := planChatSync(resp.GetWorldItems(), maxSync)
	own := c.ownUserID()

	for _, entry := range plan {
		id, isDM, ok := gchatmeow.GroupIDToParts(entry.Item.GetGroupId())
		if !ok {
			log.Warn().Msg("googlechat: skipping world item with no usable group id")
			continue
		}
		group := gcid.GroupID{ID: id, IsDM: isDM}
		res := c.queueChatResync(&simplevent.ChatResync{
			EventMeta: simplevent.EventMeta{
				Type:         bridgev2.RemoteEventChatResync,
				PortalKey:    gcid.MakePortalKey(group, c.UserLogin.ID),
				CreatePortal: entry.CreatePortal,
			},
			ChatInfo:        chatInfoFromWorldItem(entry.Item, own),
			LatestMessageTS: gchatmeow.MicrosToTime(entry.Item.GetSortTimestamp()),
		})
		log.Debug().
			Str("gcid", id).
			Bool("is_dm", isDM).
			Bool("create_portal", entry.CreatePortal).
			Any("result", res).
			Msg("googlechat: queued chat-list sync event")
	}
}

// queueChatResync queues one planned chat-list-sync entry, routing through
// queueChatResyncFn when a test has overridden it, and through the real
// c.UserLogin.QueueRemoteEvent otherwise -- mirrors the save/disconnect seam
// pattern documented on GChatClient in client.go.
func (c *GChatClient) queueChatResync(evt *simplevent.ChatResync) bridgev2.EventHandlingResult {
	if c.queueChatResyncFn != nil {
		return c.queueChatResyncFn(evt)
	}
	return c.UserLogin.QueueRemoteEvent(evt)
}

// fetchWorldWithRetry calls fetch (either conn.PaginatedWorld or a test's
// paginatedWorldFn override) up to syncMaxAttempts times, sleeping a growing
// backoff between attempts, and returns the first success or the last error
// once attempts are exhausted. This is the "channel stays up but a single
// RPC blipped" half of the latch-reset fix (see resetSyncLatch's doc
// comment): most transient paginated_world failures never even reach the
// point of consuming the whole retry budget, so they're absorbed here
// without ever needing a fresh Connected transition. ctx-aware: a cancelled
// ctx during the backoff sleep aborts immediately with ctx.Err() rather than
// waiting out the full delay.
func (c *GChatClient) fetchWorldWithRetry(ctx context.Context, fetch func(context.Context, *pb.PaginatedWorldRequest) (*pb.PaginatedWorldResponse, error)) (*pb.PaginatedWorldResponse, error) {
	log := zerolog.Ctx(ctx)
	backoffBase := c.syncRetryBackoffBase
	if backoffBase <= 0 {
		backoffBase = defaultSyncRetryBackoffBase
	}

	// A world_section_requests entry is REQUIRED: as of 2026 Google's
	// paginated_world returns world_items only when the request carries at
	// least one section with a page_size. Without it the server responds with
	// a ~2-byte stub (no world_items), so syncChats sees zero chats and no new
	// conversation ever auto-creates a portal (verified live 2026-07-22; the
	// maintained purple-googlechat client sends page_size=999,
	// googlechat_conversation.c:1173-1178, while the older Python bridge --
	// which omitted it on full sync -- predates this server change). page_size
	// caps the returned world; 999 matches purple and covers any realistic
	// personal account in one page.
	req := &pb.PaginatedWorldRequest{
		FetchFromUserSpaces:  proto.Bool(true),
		FetchOptions:         []pb.PaginatedWorldRequest_FetchOptions{pb.PaginatedWorldRequest_EXCLUDE_GROUP_LITE},
		WorldSectionRequests: []*pb.WorldSectionRequest{{PageSize: proto.Int32(worldSectionPageSize)}},
	}

	var resp *pb.PaginatedWorldResponse
	var err error
	for attempt := 0; attempt < syncMaxAttempts; attempt++ {
		if attempt > 0 {
			if sleepErr := syncSleepOrDone(ctx, backoffBase*time.Duration(uint(1)<<uint(attempt-1))); sleepErr != nil {
				return nil, sleepErr
			}
		}
		resp, err = fetch(ctx, req)
		if err == nil {
			return resp, nil
		}
		log.Err(err).Int("attempt", attempt+1).Int("max_attempts", syncMaxAttempts).
			Msg("googlechat: paginated_world failed, retrying")
	}
	return nil, err
}

// syncSleepOrDone waits for d, or returns ctx.Err() early if ctx is
// cancelled first. A tiny local copy of gchatmeow/session.go's unexported
// sleepOrDone: this package must not reach into gchatmeow's internals just
// for a two-branch select, per the layering rule that keeps pkg/connector
// from depending on gchatmeow beyond its exported API.
func syncSleepOrDone(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
