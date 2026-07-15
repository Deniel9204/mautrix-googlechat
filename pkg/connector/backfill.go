package connector

// backfill.go -- reconnect gap-recovery via catch_up_user (M2 Task 7,
// M1-review Important #2 / M2-entry blocker #2): a webchannel re-register
// after a SID-expiring reconnect resets the channel's AID to 0
// (pkg/gchatmeow/client.go's wireChannel doc comment already flags this),
// so the server never replays whatever happened on the account between
// disconnect and the new channel's registration on its own -- without this
// file, those events are silently lost the moment handleGChatEvent
// (events.go) starts actually bridging them (M2 Task 4+).
//
// Deliberate divergence from user.py/portal.py: the Python bridge never
// calls catch_up_user at all. Its own reconnect-gap handling is a small
// `sync(limit=3)` world re-fetch on a SIDInvalidError (user.py:326-345),
// and its actual catch-up RPC is catch_up_group (portal.py:450-490's
// _catchup_backfill), called PER PORTAL from inside sync() once a fresh
// paginated_world response reveals that portal's server-side revision has
// moved past the locally stored one (portal.py:654-657). That design needs
// a fresh world listing on every reconnect to learn each portal's target
// revision -- exactly the "resync on every reconnect" this bridge's
// shouldSyncOnConnect latch (client.go) was deliberately built to avoid
// (see its doc comment). catch_up_user has no such prerequisite: it is a
// single whole-account RPC keyed on one account-level watermark
// (UserLoginMetadata.Revision), so it can run on every reconnect without a
// world re-listing. M6's full per-portal backfill can still add
// catch_up_group's per-portal-revision path later without this file
// changing; for M2's "just don't lose messages" scope, catch_up_user is the
// minimal correct mechanism, and it is the one M1's review + the M2 plan
// (docs/superpowers/plans/2026-07-14-m2-text-messaging.md, Task 7) both
// name explicitly.
import (
	"context"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// catchUpMaxPages defensively bounds catchUp's PAGINATED drain loop: a
// misbehaving (or buggy) server that keeps returning PAGINATED forever must
// not spin this goroutine indefinitely. 100 pages at the server's default
// page size is far more than any real reconnect gap M2 needs to recover
// (a gap that large is M6 full-backfill territory, not "catch up since the
// last event"); if it is ever hit, catchUp logs and stops rather than
// looping -- the watermark still advanced page by page (via advanceRevision
// as each event was dispatched), so the next reconnect simply resumes from
// wherever the drain got to, no events lost.
const catchUpMaxPages = 100

// catchUp replays whatever happened on this account between the last
// successfully processed revision (UserLoginMetadata.Revision) and now, via
// catch_up_user, and dispatches every returned event through the SAME
// handleGChatEvent path a live stream event takes (events.go) -- so M4's
// later edit/reaction/delete handlers apply to gap-replayed events
// automatically, with no separate backfill-specific event handling to keep
// in sync, AND so the watermark advance itself (advanceRevision, called
// from inside handleGChatEvent for every event, live or replayed) applies
// identically here: each returned event's own revision is persisted right
// after that event is dispatched, not once in bulk at the end -- mirroring
// portal.py:502-503's `_handle_backfill_events`, which calls set_revision
// PER multi_evt inside its loop, not after it, so a crash partway through a
// catch-up page never has to redo work it already committed. Idempotency (a
// caught-up event that also arrives live, or is replayed again by an
// overlapping window on a later reconnect) is left to bridgev2's own
// message-id-keyed dedup on the RemoteMessage path (portal.go's
// checkFakeMessage/message-exists lookup) exactly like a live duplicate
// would be -- this function does no de-duplication of its own.
//
// PAGINATION -- drain ALL pages in this single invocation, do not defer the
// rest to the next reconnect. CatchUpResponse carries no explicit cursor
// field (only events/status/group_data); like portal.py's _catchup_backfill
// (portal.py:455-490), the de-facto cursor IS the request's
// from_revision_timestamp, and each page advances it to the max revision
// seen so far. Python re-reads self.revision (which set_revision moved) for
// the next page; this uses a LOCAL cursor variable instead of re-reading the
// shared UserLoginMetadata.Revision watermark -- and that difference is
// load-bearing here in a way it is not in Python. Python's backfill holds a
// lock and its single asyncio loop means no live event can advance the
// watermark mid-drain; this bridge's catchUp runs on its own goroutine
// while LIVE events keep flowing on the conn's supervision goroutine, each
// advancing the shared watermark (advanceRevision, the P0 fix). A live event
// with a revision higher than the whole gap would push the shared watermark
// PAST the un-drained backlog; if the next page re-read that watermark, the
// remaining pages would never be requested -> silent message loss on large
// gaps. Anchoring pagination to the local cursor (seeded once from the OLD
// watermark before any page) keeps the drain requesting the true backlog
// regardless of concurrent live traffic. Loops while status == PAGINATED,
// stopping on COMPLETED, and is bounded by catchUpMaxPages so a server that
// never says COMPLETED cannot spin forever. A PAGINATED page whose events
// do not advance the cursor also stops the loop (rather than re-requesting
// the identical page forever).
//
// Intended to run on every Connected transition AFTER the first for the
// current conn (client.go's handleConnState, gated by the SAME
// shouldSyncOnConnect latch syncChats uses for the first-connect case --
// see handleConnState's doc comment for why reusing that latch, rather than
// adding a second one, is both correct and required by this task). Also
// bails out (no RPC call at all) while this conn's first-ever syncChats is
// still running (isSyncInProgress, set synchronously by handleConnState
// before spawning syncChats): a second Connected transition landing while
// the first sync is unfinished would otherwise call catch_up_user with a
// meaningless (still probably 0) watermark -- gchat-port-auditor P1 finding.
// Skipping here is safe: it only means this one reconnect's catch-up
// opportunity is deferred, not lost -- once the first sync finishes,
// advanceRevision picks up tracking from the very next live event, same as
// any other reconnect.
//
// If catch_up_user itself fails outright, or a page's response status is
// anything other than COMPLETED/PAGINATED, this returns without dispatching
// that page, so UserLoginMetadata.Revision is left wherever the successfully
// drained pages (if any) already advanced it and the NEXT reconnect retries
// from there instead of silently skipping the window (no gap loss on a
// transient catch-up failure).
func (c *GChatClient) catchUp(ctx context.Context) {
	log := zerolog.Ctx(ctx)

	if c.isSyncInProgress() {
		log.Debug().Msg("googlechat: catchUp skipped, this conn's first chat-list sync is still running")
		return
	}

	fetch := c.catchUpUserFn
	if fetch == nil {
		conn := c.getConn()
		if conn == nil {
			log.Warn().Msg("googlechat: catchUp called with no live connection, skipping")
			return
		}
		fetch = conn.CatchUpUser
	}

	fromRevision := c.getRevision()
	cursor := fromRevision
	totalEvents := 0
	for page := 0; page < catchUpMaxPages; page++ {
		req := &pb.CatchUpUserRequest{
			Range: &pb.CatchUpRange{
				FromRevisionTimestamp: proto.Int64(cursor),
				// ToRevisionTimestamp is deliberately left unset: unlike
				// catch_up_group's portal.py caller (which always knows a
				// freshly-fetched target revision from the paginated_world
				// response that triggered it), a reconnect here has no such
				// upper bound to supply -- an unset optional field asks the
				// server for everything since cursor, i.e. "catch me up to
				// now".
			},
		}

		resp, err := fetch(ctx, req)
		if err != nil {
			log.Err(err).Int("page", page).Msg("googlechat: catch_up_user failed, revision watermark left at last drained page (retried on next reconnect)")
			return
		}
		status := resp.GetStatus()
		if status != pb.CatchUpResponse_COMPLETED && status != pb.CatchUpResponse_PAGINATED {
			// Mirrors portal.py:474-480's status check for catch_up_group:
			// any ABORTED_* status means the server could not honor the
			// requested range at all (cutoff exceeded, cache invalidated, or
			// the requested from-revision has aged out server-side) -- there
			// is nothing safe to replay in this page, and nowhere further
			// along to advance the watermark to.
			log.Warn().Str("status", status.String()).Int("page", page).Msg("googlechat: catch_up_user did not complete, stopping drain")
			return
		}

		pageStartCursor := cursor
		for _, evt := range resp.GetEvents() {
			if r := eventRevision(evt); r > cursor {
				cursor = r
			}
			for _, flat := range gchatmeow.SplitEventBodies(evt) {
				totalEvents++
				c.handleGChatEvent(ctx, flat) // advances the shared watermark itself, per event (see doc comment)
			}
		}

		if status == pb.CatchUpResponse_COMPLETED {
			log.Debug().
				Int("event_count", totalEvents).
				Int("pages", page+1).
				Int64("from_revision", fromRevision).
				Int64("to_revision", cursor).
				Msg("googlechat: catch_up_user drain complete")
			return
		}
		// status == PAGINATED: more pages remain. If this page's events did
		// not move the cursor forward, re-requesting from the same
		// from_revision would just return the identical page -- stop rather
		// than loop uselessly (defensive; the catchUpMaxPages bound below is
		// the harder backstop).
		if cursor <= pageStartCursor {
			log.Warn().Int("page", page).Int64("cursor", cursor).Msg("googlechat: catch_up_user reported more pages but the revision cursor did not advance, stopping drain")
			return
		}
	}
	log.Warn().Int("max_pages", catchUpMaxPages).Int("event_count", totalEvents).Int64("to_revision", cursor).Msg("googlechat: catch_up_user still PAGINATED after max pages, stopping drain (next reconnect resumes from the advanced watermark)")
}

// advanceRevision persists evt's own user_revision/group_revision timestamp
// (eventRevision) as the new catch_up_user watermark
// (UserLoginMetadata.Revision) if it is greater than what is already
// stored. Called from handleGChatEvent (events.go) for EVERY event this
// login processes -- live stream or catchUp replay alike -- mirroring
// user.py:674-682's on_stream_event, which runs its
// `if evt.HasField("user_revision"): await self.set_revision(...)` check
// unconditionally, independent of the event's type-based dispatch. A no-op
// (no lock taken, no I/O) when evt carries no revision at all, which is the
// common case for most event bodies.
func (c *GChatClient) advanceRevision(ctx context.Context, evt *pb.Event) {
	r := eventRevision(evt)
	if r <= 0 {
		return
	}
	if err := c.updateMetadata(ctx, func(meta *UserLoginMetadata) bool {
		if meta.Revision >= r {
			return false
		}
		meta.Revision = r
		return true
	}); err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("googlechat: failed to persist revision watermark")
	}
}

// getRevision snapshots the current catch_up_user watermark
// (UserLoginMetadata.Revision) under metaMu -- mirrors Connect's cookie
// snapshot read (client.go): meta.Revision is also written by
// updateMetadata (advanceRevision above) from potentially a different
// goroutine, so reading the field directly (unlocked) would race it.
func (c *GChatClient) getRevision() int64 {
	c.metaMu.Lock()
	defer c.metaMu.Unlock()
	meta, ok := c.UserLogin.Metadata.(*UserLoginMetadata)
	if !ok || meta == nil {
		return 0
	}
	return meta.Revision
}

// eventRevision returns evt's own user_revision or group_revision timestamp
// (Event.RevisionType, proto fields 6/7) -- whichever is actually set. The
// two are a proto oneof (at most one populated per event), so "the greater
// of both" and "whichever is non-zero" are equivalent; written as a plain
// comparison rather than an explicit oneof switch to match
// GetUserRevision/GetGroupRevision's own nil-safe accessor style (the arm
// that is NOT set simply returns nil, and GetTimestamp() on a nil
// *WriteRevision returns 0). Safe to call on an already-split (flattened)
// event: splitEventBodies (pkg/gchatmeow/client.go) proto.Clones the whole
// parent event -- RevisionType included -- onto every split copy, so
// calling this once per flattened body (as handleGChatEvent does) is
// equivalent to Python's once-per-raw-multi-event read
// (portal.py:502-503's `multi_evt.group_revision`, user.py:681-682's
// `evt.user_revision`): every split copy carries the identical value, and
// advanceRevision's own monotonic guard makes re-applying the same value
// multiple times a harmless no-op.
func eventRevision(evt *pb.Event) int64 {
	r := evt.GetUserRevision().GetTimestamp()
	if g := evt.GetGroupRevision().GetTimestamp(); g > r {
		r = g
	}
	return r
}
