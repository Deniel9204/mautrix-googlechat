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
	"cmp"
	"context"
	"fmt"
	"slices"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// catchUpMaxPages defensively bounds catchUp's PAGINATED drain loop: a
// misbehaving (or buggy) server that keeps returning PAGINATED forever must
// not spin this goroutine indefinitely. 100 pages at the server's default
// page size is far more than any real reconnect gap M2 needs to recover
// (a gap that large is M6 full-backfill territory, not "catch up since the
// last event"); if it is ever hit, catchUp logs and stops rather than
// looping -- the watermark still advanced page by page (via
// advanceUserRevision as each event was handled), so the next reconnect
// simply resumes from wherever the drain got to, no events lost.
const catchUpMaxPages = 100

// catchUp replays whatever happened on this account between the last
// successfully processed revision (UserLoginMetadata.Revision) and now, via
// catch_up_user, and dispatches every returned event through the SAME
// handleGChatEvent path a live stream event takes (events.go) -- so M4's
// later edit/reaction/delete handlers apply to gap-replayed events
// automatically, with no separate backfill-specific event handling to keep
// in sync, AND so the watermark advance itself (advanceUserRevision, run by
// handleGChatEvent after each successfully handled event, live or replayed)
// applies identically here: each returned event's user_revision is persisted
// right after that event is delivered, not once in bulk at the end --
// mirroring portal.py:502-503's `_handle_backfill_events`, which calls
// set_revision PER multi_evt inside its loop, so a crash partway through a
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
// from_revision_timestamp, and each page advances it to the max USER
// revision seen so far (user_revision -- the field that orders the
// catch_up_user stream; see the per-event comment in the loop for why
// group_revision must not seed it). Python re-reads self.revision (which
// set_revision moved) for the next page; this uses a LOCAL cursor variable
// instead of re-reading the shared UserLoginMetadata.Revision watermark --
// and that difference is load-bearing here in a way it is not in Python.
// Python's backfill holds a lock and its single asyncio loop means no live
// event can advance the watermark mid-drain; this bridge's catchUp runs on
// its own goroutine while LIVE events keep flowing on the conn's supervision
// goroutine, each advancing the shared watermark (advanceUserRevision). A
// live event with a revision higher than the whole gap would push the shared
// watermark PAST the un-drained backlog; if the next page re-read that
// watermark, the remaining pages would never be requested -> silent message
// loss on large gaps. Anchoring pagination to the local cursor (seeded once
// from the OLD watermark before any page) keeps the drain requesting the
// true backlog regardless of concurrent live traffic. Loops while status == PAGINATED,
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
// advanceUserRevision picks up tracking from the very next live event, same
// as any other reconnect.
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
			// The drain's continuation cursor is the catch_up_user stream's
			// ordering key: user_revision. NOT group_revision -- that is a
			// separate per-group revision space, and seeding a user-level
			// from_revision_timestamp with a group_revision value could skip
			// user-stream events on the next page. An event carrying only
			// group_revision therefore does not advance the cursor; if a whole
			// page fails to advance it, the "cursor did not advance" guard
			// below stops the drain (safe: under-advancing just means the next
			// reconnect re-fetches the window and bridgev2 dedups by message
			// id -- over-advancing, which would skip events, is the only
			// unsafe direction).
			if r := userRevision(evt); r > cursor {
				cursor = r
			}
			for _, flat := range gchatmeow.SplitEventBodies(evt) {
				totalEvents++
				if res := c.handleGChatEvent(ctx, flat); !res.Success {
					// A caught-up event that FAILED to handle (e.g. the
					// bridgev2 queue rejected it) must not let the drain
					// advance the persisted watermark past it: stop here so
					// the next reconnect re-fetches from the last successfully
					// handled revision (M2-review Important #3). Events already
					// handled earlier in this page advanced the watermark in
					// order; this one did not (handleGChatEvent gates the
					// advance on res.Success), so stopping now keeps the
					// persisted watermark contiguous with what was delivered.
					log.Warn().Int("page", page).Msg("googlechat: caught-up event failed to handle, stopping drain (watermark left at last handled revision, retried on next reconnect)")
					return
				}
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

// advanceUserRevision persists evt's user_revision timestamp as the new
// catch_up_user watermark (UserLoginMetadata.Revision) if it is greater than
// what is already stored. Called from handleGChatEvent (events.go) for every
// SUCCESSFULLY HANDLED event this login processes -- live stream or catchUp
// replay alike -- porting user.py:674-682's on_stream_event, which advances
// the USER-level watermark ONLY from evt.user_revision
// (`if evt.HasField("user_revision"): await self.set_revision(...)`).
//
// Critically, it reads user_revision ONLY, never group_revision. The two are
// separate revision spaces: user_revision orders the whole-account
// user/catch_up_user stream (this watermark seeds catch_up_user's
// from_revision_timestamp), while group_revision is a per-group counter that
// Python routes to a DIFFERENT, per-portal watermark
// (portal.py:502-503/519-531's set_revision on the Portal, feeding
// catch_up_group -- see advancePortalRevision below). Folding group_revision
// into this user watermark could over-advance it past not-yet-delivered
// user-stream events, so a later disconnect would make the next
// catch_up_user skip them -> permanent message loss (the exact failure this
// whole file exists to prevent; M2-review Important #2). A no-op (no lock,
// no I/O) when evt carries no user_revision, which -- since RevisionType is a
// proto oneof -- includes every event that instead carries group_revision.
func (c *GChatClient) advanceUserRevision(ctx context.Context, evt *pb.Event) {
	r := userRevision(evt)
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
		zerolog.Ctx(ctx).Err(err).Msg("googlechat: failed to persist user revision watermark")
	}
}

// advancePortalRevision parks evt's group_revision timestamp on the
// PER-PORTAL watermark (PortalMetadata.Revision) -- the field M6's
// catch_up_group backfill will seed its own from_revision from -- if it is
// greater than what is already stored there. Called from handleGChatEvent
// (events.go) alongside advanceUserRevision, so exactly one of the two
// actually fires per event (RevisionType is a oneof). Ports the OTHER half
// of Python's revision tracking: portal.py:502-503 / user.py:678's
// group-scoped set_revision on the Portal, kept entirely separate from the
// user watermark (user.py:681-682). This does NOT wire catch_up_group itself
// (that is M6) -- it only stores the value so that when M6 lands, the portal
// watermark is already being maintained instead of starting from zero. A
// no-op when evt carries no group_revision, or when its group id is
// unusable, or (in tests without a savePortalRevisionFn seam) when there is
// no live bridgev2.Bridge to look the portal up in.
func (c *GChatClient) advancePortalRevision(ctx context.Context, evt *pb.Event) {
	r := groupRevision(evt)
	if r <= 0 {
		return
	}
	id, isDM, ok := gchatmeow.GroupIDToParts(evt.GetGroupId())
	if !ok {
		return
	}
	key := gcid.MakePortalKey(gcid.GroupID{ID: id, IsDM: isDM}, c.UserLogin.ID)
	c.savePortalRevision(ctx, key, r)
}

// savePortalRevision writes revision onto the portal identified by key
// (PortalMetadata.Revision, monotonic -- never regressed), routing through
// savePortalRevisionFn when a test has overridden it and through the real
// bridgev2.Bridge.GetPortalByKey + Portal.Save otherwise. The seam exists
// because this package's lightweight test UserLogins have a nil Bridge (see
// newTestUserLogin, client_test.go), so the real lookup would panic; it also
// lets the group-revision-routing tests observe the (key, revision) pair
// without a full DB harness. Mirrors megabridge's setPortalRevision
// (_reference/googlechat-megabridge/pkg/connector/portal.go) but with a
// monotonic guard added.
func (c *GChatClient) savePortalRevision(ctx context.Context, key networkid.PortalKey, revision int64) {
	if c.savePortalRevisionFn != nil {
		c.savePortalRevisionFn(ctx, key, revision)
		return
	}
	if c.UserLogin == nil || c.UserLogin.Bridge == nil {
		return
	}
	portal, err := c.UserLogin.Bridge.GetPortalByKey(ctx, key)
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("googlechat: failed to get portal for group-revision watermark")
		return
	}
	meta, ok := portal.Metadata.(*PortalMetadata)
	if !ok || meta == nil || meta.Revision >= revision {
		return
	}
	meta.Revision = revision
	if err := portal.Save(ctx); err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("googlechat: failed to persist portal group-revision watermark")
	}
}

// getRevision snapshots the current catch_up_user watermark
// (UserLoginMetadata.Revision) under metaMu -- mirrors Connect's cookie
// snapshot read (client.go): meta.Revision is also written by
// updateMetadata (advanceUserRevision above) from potentially a different
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

// userRevision returns evt's user_revision timestamp (Event.RevisionType
// oneof arm field 6), or 0 when the event carries group_revision / no
// revision instead. GetUserRevision().GetTimestamp() is nil-safe: the arm
// that is not set returns nil, and GetTimestamp() on a nil *WriteRevision
// returns 0. Safe on an already-split (flattened) event -- splitEventBodies
// (pkg/gchatmeow/client.go) proto.Clones the whole parent, RevisionType
// included, onto every copy, so reading it per flattened body is equivalent
// to Python's once-per-raw-multi-event read (user.py:681-682).
func userRevision(evt *pb.Event) int64 {
	return evt.GetUserRevision().GetTimestamp()
}

// groupRevision returns evt's group_revision timestamp (Event.RevisionType
// oneof arm field 7), or 0 when the event carries user_revision / no
// revision instead. Same nil-safe accessor + split-safety notes as
// userRevision; the Python analog is portal.py:502-503's
// `multi_evt.group_revision`.
func groupRevision(evt *pb.Event) int64 {
	return evt.GetGroupRevision().GetTimestamp()
}

// region M6 Task 1: initial/history backfill for FLAT rooms
//
// This is a DIFFERENT path from catchUp above: catchUp (M2 Task 7) replays a
// reconnect GAP on an already-bridged portal via catch_up_user/group; this
// implements bridgev2.BackfillingNetworkAPI.FetchMessages, which populates a
// portal's Matrix room with HISTORY -- messages that predate the portal ever
// being bridged at all -- porting portal.py's _initial_backfill (portal.py:406-448).
//
// RPC CHOICE -- deliberate divergence from the task brief's literal wording
// ("ListMessages RPC with the group id + a page cursor"): the actual proto
// (pkg/gchatmeow/proto/googlechat.pb.go) makes that impossible as stated.
// ListMessagesRequest.ParentId is a *MessageParentId whose oneof has exactly
// one arm, MessageParentId_TopicId (a concrete *TopicId, itself GroupId +
// topic_id) -- there is no group-only/topic-less arm, so ListMessages can
// only ever list the replies WITHIN one already-known topic, never a group's
// flat message history. This repo's own api.go already documents the two
// RPCs accordingly ("ListTopics pages through topics of a group (backfill)"
// vs "ListMessages pages through messages of a topic (backfill)"), and
// portal.py's _initial_backfill agrees: it drives a FLAT room's entire
// initial fetch off ListTopicsRequest{group_id, page_size_for_topics}, taking
// each topic's FIRST reply (topic.replies[0]) as the flat message, and only
// ever issues ListMessagesRequest for the rare topic that ALSO has a real
// thread (topic_read_state.thread_created_usec > 0, portal.py:432-441) --
// see docs/research/02-wire-protocol.md:217-219,258 for the same conclusion
// from an independent reading of the wire protocol. So: this file's flat
// strategy pages ListTopics, exactly like Python; the per-topic
// ListMessages-for-real-threads sub-case is deliberately NOT ported here (see
// the ShouldBackfillThread scope note on fetchFlatMessages below) since it
// belongs naturally with Task 2's ThreadRoot-scoped ListMessages dispatch.
//
// SINGLE-SHOT, NOT PAGED -- matching portal.py:_initial_backfill exactly
// (portal.py:406-448): that method makes exactly ONE ListTopicsRequest, never
// loops, and never re-fetches. Neither ListTopicsRequest nor
// ListTopicsResponse carries anything resembling a continuation/page token
// (grepped the whole proto file: the only page_token/next_page_token pair in
// existence belongs to the unrelated ListMembersRequest/Response), so there
// is no real cursor to build here even if a multi-page design were wanted.
// ListTopicsRequest's user_not_older_than / group_not_older_than
// (ReferenceRevision{Timestamp}) look tempting but are a consistency FLOOR
// ("don't serve me anything staler than this revision"), not a backward
// filter ("give me topics before this point") -- and portal.py's own
// ListTopicsRequest construction never sets either field, so there is no
// reference behavior to confirm that reading; a silently-wrong guess here
// would corrupt backfilled history with no test able to catch it against a
// real server.
//
// An earlier version of this file built a cumulative count-offset cursor on
// top of PageSizeForTopics (request delivered+Count each call, take the
// front slice that fell outside the previous, smaller request) to page
// beyond one call. That scheme had a silent data-loss hole: two distinct
// topics sharing an identical microsecond SortTime could straddle the
// front/tail slice boundary, and since tie order is page-size-dependent, a
// topic could land in NEITHER page's slice -- a dropped history message that
// nothing downstream would catch (the duplicate half of a tie is caught by
// the anchor filter / framework cutoffMessages; the DROP half is not).
// Matching Python's actual single-shot behavior removes the boundary
// entirely: one ListTopicsRequest with PageSizeForTopics = params.Count (the
// full requested backfill depth -- Task 3 wires this to the config's
// initial_nonthread_limit; beyond-limit history is intentionally not
// fetched, matching Python), reverse the response into oldest-first order,
// stable-sort it ascending by SortTime with a topic-id tiebreaker (so ordering
// is deterministic regardless of the server's tie order -- see the
// tiebreaker note on the sort call below), and return every topic's head
// reply in one response with HasMore=false and an empty Cursor. This also
// deletes the O(n^2) cumulative re-fetch the old paged scheme did across a
// long backfill run.
//
// SCOPE (documented, not silent, gaps -- left for later M6 tasks):
//   - A topic with a real Google Chat thread even inside an otherwise-flat
//     room (topic_read_state.thread_created_usec > 0) now ALSO gets
//     ShouldBackfillThread=true on its head BackfillMessage (M6 Task 2,
//     closing this file's own documented gap): the framework's own
//     fetchThreadBackfill/doThreadBackfill (portalbackfill.go) then drives a
//     ThreadRoot-scoped FetchMessages call for that topic's other replies --
//     see the "M6 Task 2" region below for that dispatch and the
//     ThreadsOnly-portal top-level case (both share this same per-topic
//     ShouldBackfillThread computation).
//   - A topic whose head reply is a SYSTEM_MESSAGE (membership/room-info
//     change) is converted through the ordinary text path
//     (convertMessageToMatrix) here, not systemmessage.go's trySystemMessage
//     (which only runs on the live MESSAGE_POSTED event-dispatch path,
//     events.go's handleMessagePosted) -- flagged for a follow-up milestone
//     task's whole-branch review rather than silently gapped.
//   - Forward=true SPLITS on AnchorMessage (M6 Task 3.5, after this milestone's
//     Task 3 verification surfaced that the blanket Forward==true stub left
//     M6 entirely inert on non-Beeper homeservers -- see FetchMessages' own
//     doc comment below for the full reasoning): AnchorMessage==nil is the
//     NEW-ROOM bootstrap seed and is served by this same list_topics
//     strategy; AnchorMessage!=nil is forward CATCH-UP on an existing room
//     and stays an empty, HasMore=false response, since GC's list_topics only
//     exposes "the N most recent" (inherently BACKWARD-from-now, with no way
//     to ask "the N oldest of what's newer than X") and M2's catch_up_user
//     already owns reconnect-gap recovery for rooms that are already bridged.
//
// CONVERSION REUSE: every backfilled message is converted via the EXACT SAME
// convertMessageToMatrix(c.msgConverter(), c) function events.go's live
// MESSAGE_POSTED dispatch calls (queueMessagePosted, ConvertMessageFunc) --
// not a parallel/duplicated conversion path -- so a backfilled message's
// text, formatting, mentions, thread routing, and attachments are IDENTICAL
// to what the same message would have produced if it had arrived live.
//
// defaultFetchMessagesCount is used only when bridgev2 supplies a
// non-positive params.Count (defensive; the framework's own doc comment
// says it should always be a positive "preferred number of messages",
// but nothing in the interface contract guarantees that).
//
// M6 Task 3 verified (not assumed) where the REAL limit comes from instead:
// there is no connector-specific config for this -- and there must not be
// one, since the framework already drives params.Count end to end and a
// second, connector-owned knob would just shadow or conflict with it. The
// standard top-level `backfill:` section (bridgeconfig.BackfillConfig,
// generated into every mxmain bridge's config, including this one) supplies
// it, but via TWO DIFFERENT keys depending on which call this file receives:
//   - fetchTopicHeadMessages, reached via the Forward==false branch (the
//     backward backfill QUEUE): its params.Count is set by
//     Portal.doBackwardsBackfill (portalbackfill.go:123) from
//     `backfill.queue.batch_size` -- NOT `backfill.max_initial_messages` as
//     an earlier draft of this comment assumed.
//   - fetchTopicHeadMessages, reached via the Forward==true/AnchorMessage==nil
//     branch (the NEW-ROOM bootstrap seed, M6 Task 3.5): its params.Count is
//     `backfill.max_initial_messages` (Portal.doForwardBackfill,
//     portalbackfill.go:46) -- this is the key Task 3's draft of this comment
//     said never reached the connector; Task 3.5 fixed the dispatcher so it
//     now does.
//   - fetchThreadMessages: its params.Count IS `backfill.threads.max_initial_messages`
//     (Portal.fetchThreadBackfill, portalbackfill.go:203) -- this one matches
//     what the augment/brief expected.
//
// Operationally: doBackwardsBackfill only ever runs from the bridge's
// backward backfill QUEUE, which (backfillqueue.go's RunBackfillQueue) only
// starts when BOTH `backfill.queue.enabled` is true AND the connected
// homeserver reports the Beeper-only batch-sending capability
// (mautrix.BeeperFeatureBatchSending) -- a standard self-hosted Synapse/
// Dendrite homeserver never reports that capability, so on a non-Beeper
// deployment this queue (and therefore the Forward==false path through
// fetchTopicHeadMessages) never runs at all. That is NOT the milestone-inert
// gap it once looked like, though: M6 Task 3.5 confirmed the framework fires
// a SECOND, independent trigger on room creation regardless of batch-sending
// support -- Portal.doForwardBackfill(ctx, source, nil, bundle)
// (portal.go:5404), which calls FetchMessages with Forward==true and
// AnchorMessage==nil and sends the result via sendBackfill's non-batch
// branch (sendLegacyBackfill, portalbackfill.go:340) when the homeserver
// lacks BatchSending. That call now also reaches fetchTopicHeadMessages (see
// the Forward split above and FetchMessages' own doc comment below), so a
// non-Beeper deployment like continuwuity still gets its initial backfill
// through this second path even though the queue itself stays dormant.
const defaultFetchMessagesCount = 20

var _ bridgev2.BackfillingNetworkAPI = (*GChatClient)(nil)

// FetchMessages implements bridgev2.BackfillingNetworkAPI for history
// backfill (M6 Task 1 flat rooms; M6 Task 2 threaded spaces). Dispatches:
//   - params.ThreadRoot != "": fetchThreadMessages (Task 2), regardless of
//     params.Forward -- checked FIRST, before the Forward branch below,
//     because the framework's own thread-backfill callers
//     (fetchThreadBackfill/doThreadBackfill, portalbackfill.go) ALWAYS set
//     Forward=true on a ThreadRoot-scoped call (a thread grows forward in
//     time from its head, so cutoffMessages' forward-trim semantics are what
//     apply to it) even though this is conceptually still part of the
//     backward/initial backfill this milestone targets. Checking Forward
//     before ThreadRoot here would misroute every real thread-backfill call
//     into the Forward stub below and silently never fetch any thread
//     replies -- verified by reading portalbackfill.go, not assumed.
//   - params.Forward && params.AnchorMessage != nil (with ThreadRoot == ""):
//     forward CATCH-UP on an EXISTING room ("new messages since the last
//     known one"). Two framework callers can reach this shape:
//     doForwardBackfill's room-creation seed once it is called again with a
//     non-nil lastMessage (portal.go:3882's handleRemoteChatResync path --
//     dormant for this bridge today, since pkg/connector emits no
//     RemoteChatResync event and implements no RemoteChatResyncBackfill/
//     CheckNeedsBackfill, verified by grep), and fetchThreadBackfill's
//     ThreadRoot-scoped call (portalbackfill.go:196-204) -- but that one is
//     already routed to fetchThreadMessages by the ThreadRoot check above,
//     never reaching here. Returns an empty, HasMore=false response --
//     documented GC limitation (see the Forward bullet in this file's region
//     doc comment above); M2's catch_up_user already owns this case for
//     existing rooms.
//   - otherwise (Forward==false, OR Forward==true && AnchorMessage==nil):
//     fetchTopicHeadMessages, for a flat portal's backward/queue path, a
//     ThreadsOnly portal's backward/queue path, AND -- as of M6 Task 3.5 --
//     the Forward==true NEW-ROOM bootstrap seed on EITHER portal shape
//     (doForwardBackfill(ctx, source, nil, bundle) on room creation,
//     portal.go:5404). Task 3's verification found that stubbing every
//     Forward==true call to empty left M6 entirely inert on non-Beeper
//     homeservers like continuwuity: doBackwardsBackfill's queue
//     (backfillqueue.go's RunBackfillQueue) never starts there
//     (BatchSending gate), so the bootstrap seed above was the ONLY
//     room-creation trigger that actually runs on such a deployment, and it
//     was fetching nothing. Routing it here instead fixes that: it reuses
//     the exact same list_topics + per-topic-head strategy as the backward
//     path (fetchTopicHeadMessages already handles AnchorMessage==nil with
//     no filtering, needing no change), the three call shapes differ only in
//     whether ShouldBackfillThread is forced true for every topic
//     (ThreadsOnly) or computed per-topic from ThreadCreatedUsec (flat) --
//     see fetchTopicHeadMessages' own doc comment. A batch-send/Beeper
//     homeserver where BOTH the queue and the room-creation seed could fire
//     for the same room is not this milestone's actual target: continuwuity
//     has no BatchSending, so the queue never runs there at all and the seed
//     is the ONLY trigger (see the region doc comment's "Operationally"
//     paragraph above). Where both CAN fire, overlap is bounded by TIMING,
//     not by any framework per-message dedup -- verified by reading, not
//     assumed, correcting an earlier draft of this comment that wrongly
//     credited id-based dedup here: this connector never sets
//     FetchMessagesResponse.AggressiveDeduplication, so cutoffMessages' id-
//     based GetFirstPartByID pass (portalbackfill.go:286-313) never runs for
//     these responses, and the seed's own call passes AnchorMessage==nil, so
//     cutoffMessages applies NO filtering to it either (its lastMessage==nil
//     early return, portalbackfill.go:248-250). The actual guard is
//     portal.go:5362-5371, which upserts the queue's BackfillTask with
//     NextDispatchMinTS = now + BackfillMinBackoffAfterRoomCreate (1 minute,
//     backfillqueue.go:21) BEFORE the seed call at line 5404 runs
//     synchronously, so the seed's rows are already persisted by the time the
//     queue's first dispatch reads a real anchor (GetFirstPortalMessage) and
//     calls back into this same function with THAT anchor -- whose
//     hasAnchor filter (fetchTopicHeadMessages below) then excludes anything
//     at or newer than it. A topic sharing the anchor's exact microsecond is
//     the one documented, deliberate exception to that filter (see
//     fetchTopicHeadMessages' anchor-filter comment) and could in theory
//     still double-post on such a homeserver; that is a pre-existing
//     tradeoff of the anchor filter itself, not something this task's
//     dispatcher change introduces.
func (c *GChatClient) FetchMessages(ctx context.Context, params bridgev2.FetchMessagesParams) (*bridgev2.FetchMessagesResponse, error) {
	if params.ThreadRoot != "" {
		return c.fetchThreadMessages(ctx, params)
	}
	if params.Forward && params.AnchorMessage != nil {
		// forward CATCH-UP on an EXISTING room ("new messages since the last
		// known one"). GC list_topics exposes only the most-recent-N
		// (backward-from-now); there is no "strictly newer than X" query, and
		// M2 catch_up_user already owns reconnect-gap recovery for existing
		// rooms. Empty.
		return &bridgev2.FetchMessagesResponse{HasMore: false}, nil
	}
	// Everything else = "the most-recent-N topics as this portal's initial
	// history," served by the same single-shot list_topics strategy:
	//   - Forward==false: the backward backfill QUEUE (doBackwardsBackfill) --
	//     runs only on batch-send/Beeper homeservers (RunBackfillQueue gates on
	//     BatchSending, backfillqueue.go:77).
	//   - Forward==true && AnchorMessage==nil: the NEW-ROOM bootstrap seed
	//     (doForwardBackfill on room creation, portal.go:5404) -- the ONLY
	//     initial-backfill path that runs on a NON-batch-send homeserver like
	//     continuwuity, where sendBackfill's non-batch branch
	//     (sendLegacyBackfill, portalbackfill.go:340) inserts the results as
	//     normal timeline events.
	// Both want the same data, so both route here. On continuwuity (no
	// BatchSending) the queue above never runs at all, so this seed is the
	// ONLY trigger and there is nothing to double-post against. On a
	// homeserver that DOES support batch-sending, both COULD fire for the
	// same room; see this function's own doc comment above for why that
	// overlap is bounded by timing (BackfillMinBackoffAfterRoomCreate) plus
	// fetchTopicHeadMessages' own anchor filter below, NOT by the
	// framework's AggressiveDeduplication (which this connector never sets).
	return c.fetchTopicHeadMessages(ctx, params)
}

// fetchTopicHeadMessages fetches a portal's ENTIRE backfilled top-level
// message history in a single list_topics call, taking each topic's head
// reply (topic.replies[0]) as the top-level message -- see this file's region
// doc comment above for the full RPC-choice, single-shot, and scope
// rationale. Serves BOTH a flat portal and a ThreadsOnly portal's top-level
// (ThreadRoot=="") call: the request/response handling is identical either
// way (M6 Task 1 vs Task 2 never needed two separate list_topics strategies),
// they differ only in isThread below, which decides whether each topic's head
// gets ShouldBackfillThread=true so the framework's own
// fetchThreadBackfill/doThreadBackfill later drives a ThreadRoot-scoped
// FetchMessages call (fetchThreadMessages) for that topic's other replies.
// A topic counts as a real thread (isThread) iff the PORTAL is ThreadsOnly
// (every topic in a threaded space is a thread by construction) OR the topic
// itself has topic_read_state.thread_created_usec > 0 (a real thread even
// inside an otherwise-flat room) -- porting portal.py:432's
// `self.threads_only or topic.topic_read_state.thread_created_usec > 0`
// exactly, and closing this file's own previously-documented flat-with-
// real-thread gap (see the SCOPE bullet above).
func (c *GChatClient) fetchTopicHeadMessages(ctx context.Context, params bridgev2.FetchMessagesParams) (*bridgev2.FetchMessagesResponse, error) {
	group, err := gcid.ParsePortalID(params.Portal.ID)
	if err != nil {
		return nil, fmt.Errorf("googlechat: invalid portal id %q: %w", params.Portal.ID, err)
	}

	threadsOnly := false
	if meta, ok := params.Portal.Metadata.(*PortalMetadata); ok && meta != nil {
		threadsOnly = meta.ThreadsOnly
	}

	count := params.Count
	if count <= 0 {
		count = defaultFetchMessagesCount
	}

	list := c.listTopicsFn
	if list == nil {
		conn := c.getConn()
		if conn == nil {
			return nil, fmt.Errorf("googlechat: not connected")
		}
		list = conn.ListTopics
	}
	resp, err := list(ctx, &pb.ListTopicsRequest{
		GroupId:           gchatmeow.PartsToGroupID(group.ID, group.IsDM),
		PageSizeForTopics: proto.Int32(int32(count)),
	})
	if err != nil {
		return nil, fmt.Errorf("googlechat: list_topics failed: %w", err)
	}

	// Server order is newest-first (portal.py:428's own comment: "The
	// reversed list is probably already sorted properly, but re-sort it just
	// in case" -- ported literally: reverse first, then a stable sort by
	// SortTime, exactly like Python's `sorted(reversed(topics), key=...)`).
	// Unlike Python, ties are broken by topic id (cmp.Compare on the string):
	// Python's sort key is SortTime alone, so a tie's order is whatever
	// Python's stable sort leaves it at post-reversal -- i.e. the SERVER's
	// original relative order, which this bridge has no contract guaranteeing
	// is itself deterministic. Adding the topic-id tiebreaker makes this
	// bridge's ordering deterministic regardless of server tie order, at the
	// minor cost of diverging from Python's tie order in the (rare) case of
	// an actual SortTime collision.
	topics := slices.Clone(resp.GetTopics())
	slices.Reverse(topics)
	slices.SortStableFunc(topics, func(a, b *pb.Topic) int {
		if bySortTime := cmp.Compare(a.GetSortTime(), b.GetSortTime()); bySortTime != 0 {
			return bySortTime
		}
		return cmp.Compare(a.GetId().GetTopicId(), b.GetId().GetTopicId())
	})

	getIntent := c.getIntentForFn
	if getIntent == nil {
		getIntent = func(ctx context.Context, portal *bridgev2.Portal, sender bridgev2.EventSender, evtType bridgev2.RemoteEventType) (bridgev2.MatrixAPI, bool) {
			return portal.GetIntentFor(ctx, sender, c.UserLogin, evtType)
		}
	}
	convert := convertMessageToMatrix(c.msgConverter(), c)

	hasAnchor := params.AnchorMessage != nil
	var anchorMicros int64
	if hasAnchor {
		anchorMicros = gchatmeow.TimeToMicros(params.AnchorMessage.Timestamp)
	}

	log := zerolog.Ctx(ctx)
	messages := make([]*bridgev2.BackfillMessage, 0, len(topics))
	for _, topic := range topics {
		replies := topic.GetReplies()
		if len(replies) == 0 {
			continue
		}
		msg := replies[0]
		gcMessageID := msg.GetId().GetMessageId()
		if gcMessageID == "" {
			log.Warn().Msg("googlechat: backfill topic head reply has no message id, skipping")
			continue
		}
		msgID := gcid.MakeMessageID(gcMessageID)
		// Anchor filter: exclude the portal's oldest already-bridged message
		// itself (by ID) and anything strictly NEWER than it. Deliberately
		// NOT a `>= anchorMicros` cutoff (this file's earlier version, and
		// meta's own wrapBackfillEvents timestamp-only DeleteFunc,
		// _reference/meta/pkg/connector/backfill.go): a broad `>=` on
		// microsecond timestamps drops ANY distinct message that happens to
		// share the anchor's exact microsecond, not just the anchor message
		// itself -- a silent, uncaught loss, since that sibling never
		// reaches the framework's own anchor handling
		// (mautrix-go bridgev2/portalbackfill.go's cutoffMessages) to be
		// correctly kept. cutoffMessages itself decides purely by
		// `ID == anchor.ID || Timestamp.After(anchor.Timestamp)`, i.e. exact
		// id match OR strictly newer -- never on tied-but-distinct
		// timestamps -- and runs downstream of this function on every
		// backward-backfill response regardless, so this filter only needs
		// to avoid handing the framework work it would discard anyway; it
		// does not need to (and must not) be the sole line of defense.
		// Matching its id-or-strictly-newer criterion here means a distinct
		// sibling at the anchor's exact microsecond survives both layers.
		if hasAnchor && (msgID == params.AnchorMessage.ID || msg.GetCreateTime() > anchorMicros) {
			continue
		}
		senderID := gcid.MakeUserID(msg.GetCreator().GetUserId().GetId())
		sender := bridgev2.EventSender{
			Sender:   senderID,
			IsFromMe: c.IsThisUser(ctx, senderID),
		}
		intent, ok := getIntent(ctx, params.Portal, sender, bridgev2.RemoteEventBackfill)
		if !ok {
			continue
		}
		cm, err := convert(ctx, params.Portal, intent, msg)
		if err != nil {
			log.Err(err).Str("gc_message_id", gcMessageID).Msg("googlechat: failed to convert backfill message, skipping")
			continue
		}
		// Reactions is deliberately left nil/unset here (M6 Task 3 --
		// verified against the proto and Python, not guessed). msg.Reactions
		// (the GC Message's own `repeated Reaction reactions = 21` field,
		// pkg/gchatmeow/proto/googlechat.pb.go's `type Reaction struct`) only
		// carries Emoji, Count, CurrentUserParticipated, and
		// CreateTimestamp -- there is NO reactor user id anywhere on it, only
		// a per-emoji tally. bridgev2's BackfillReaction (networkinterface.go)
		// REQUIRES a Sender EventSender per reaction, which cannot be
		// synthesized from a count: even the narrowest possible partial
		// fix -- emitting ONE reaction attributed to the current user when
		// CurrentUserParticipated is true, silently dropping the rest of the
		// count -- would still be an incomplete, sender-created-looking
		// reaction the current user never actually left on this exact
		// message in isolation (only "participated in the aggregate"), and
		// there is no way to reconstruct any of the OTHER reactors' identities
		// from this summary at all. Python has the exact same limitation and
		// makes the exact same choice: portal.py's _initial_backfill (portal.py:406-448)
		// calls handle_googlechat_message per topic/reply and never once
		// reads message.reactions -- reactions are bridged EXCLUSIVELY from
		// live MessageReactionEvents (portal.py:1166's
		// handle_googlechat_reaction), which DO carry a real reactor identity
		// on the wire. So this is parity with upstream, not a regression:
		// historical (pre-bridge) reaction counts are silently dropped by
		// both bridges, on purpose. Nothing recoverable is lost either --
		// any reaction that happens while this bridge is connected arrives
		// live (M4's reaction handling, events.go) with its real sender, and
		// a reaction added during a reconnect gap is replayed with identity
		// intact by M2's catchUp (catch_up_user, this file's top region).
		// Only reactions left on a message BEFORE this bridge ever connected
		// are unattributable, and they are unattributable in Python too.
		// TestFetchMessagesFlatOmitsReactionsEvenWhenGCMessageHasThem
		// (backfill_test.go) pins this: a GC Message carrying
		// Count>0/CurrentUserParticipated=true reactions must still produce
		// a BackfillMessage with Reactions==nil, so a future change can't
		// silently start synthesizing misattributed reactions from the
		// count-only summary without tripping that test and reading this
		// comment first.
		bm := &bridgev2.BackfillMessage{
			ConvertedMessage: cm,
			Sender:           sender,
			ID:               msgID,
			Timestamp:        gchatmeow.MicrosToTime(msg.GetCreateTime()),
			StreamOrder:      msg.GetCreateTime(),
		}
		// isThread: see this function's doc comment above for the
		// threadsOnly-OR-thread_created_usec>0 rule. When true, the
		// framework (portalbackfill.go's sendBackfill/compileBatchMessage,
		// gated on msg.ShouldBackfillThread) later drives a
		// ThreadRoot-scoped FetchMessages call for this topic via
		// fetchThreadMessages below. LastThreadMessage is set to the LAST
		// entry in topic.Replies (a preview list that, per this file's
		// proto-fields note, may itself be a subset -- but when it has more
		// than the head, its last entry is the most recent reply known from
		// this same response) or, when Replies has only the head (the
		// common case for a topic that has not been paged before), the
		// head's own id -- exactly what
		// DB.Message.GetLastThreadMessage(portal, threadID=head.ID) will
		// find once the head itself is inserted (its own ThreadRoot
		// self-references its id in a ThreadsOnly portal; see
		// msgconv/from-gchat.go). NOTE (verified by reading
		// portalbackfill.go, not guessed): this field is currently never
		// READ anywhere in the vendored bridgev2 framework -- only
		// GetLastThreadMessage's own DB query drives the real anchor lookup
		// -- so setting it here is forward-compatible bookkeeping, not a
		// behavior this milestone's tests can observe through the
		// framework itself.
		if isThread := threadsOnly || topic.GetTopicReadState().GetThreadCreatedUsec() > 0; isThread {
			bm.ShouldBackfillThread = true
			bm.LastThreadMessage = msgID
			if last := replies[len(replies)-1]; last != msg {
				if lastID := last.GetId().GetMessageId(); lastID != "" {
					bm.LastThreadMessage = gcid.MakeMessageID(lastID)
				}
			}
		}
		messages = append(messages, bm)
	}

	return &bridgev2.FetchMessagesResponse{
		Messages: messages,
		HasMore:  false,
		Cursor:   "",
	}, nil
}

// endregion

// region M6 Task 2: threaded-space backfill (per-topic ListMessages)
//
// This region implements the OTHER half of bridgev2's threaded backfill flow
// that Task 1 above deliberately deferred: FetchMessages calls where
// params.ThreadRoot is set. The framework drives this itself once Task 1/2's
// fetchTopicHeadMessages returns a head BackfillMessage with
// ShouldBackfillThread=true (portalbackfill.go's sendBackfill ->
// doThreadBackfill / compileBatchMessage -> fetchThreadInsideBatch), by
// looking up DB.Message.GetLastThreadMessage(portal, threadID=<head's own
// message id>) as the anchor and calling FetchMessages again with
// ThreadRoot=anchor.ID, AnchorMessage=anchor, Forward=true (see FetchMessages'
// own doc comment above for why Forward is checked AFTER ThreadRoot).
//
// MESSAGE -> TOPIC MAPPING -- the key design point verified against the
// proto and the framework's own message metadata (task-2-augment.md): a
// Google Chat message id string does NOT itself encode which topic it
// belongs to, so the topic id cannot be derived from params.ThreadRoot alone.
// ListMessagesRequest needs a MessageParentId{TopicId{TopicId, GroupId}} to
// scope the fetch to one topic. The reliable source is
// params.AnchorMessage.Metadata.(*MessageMetadata).TopicID -- stamped on
// EVERY bridged message part by the same convertMessageToMatrix path this
// file already reuses (msgconv_adapter.go:91-94), so the anchor row (the
// topic's head, or -- once GetLastThreadMessage's DB query starts returning
// later replies -- any other message already bridged into this thread)
// always carries the topic id it was bridged into. If AnchorMessage is nil,
// or its Metadata isn't a *MessageMetadata, or TopicID is empty, this never
// guesses: it logs and returns an empty, HasMore=false response.
//
// PYTHON PARITY -- portal.py:432-441's thread branch issues exactly ONE
// ListMessagesRequest{parent_id: MessageParentId(topic_id=topic.id),
// page_size: initial_thread_reply_limit} per topic and iterates
// resp.messages directly, with NO reordering (unlike list_topics, which
// portal.py explicitly reverses+re-sorts -- portal.py never does that for
// list_messages). This file additionally sorts the response ascending by
// CreateTime (a real per-message timestamp, unlike Topic.SortTime's
// "most-recently-active" ambiguity) purely as a safety net for
// FetchMessagesResponse's own documented contract ("Messages should always be
// sorted in chronological order") -- CreateTime-ascending IS chronological
// order regardless of whatever order the server actually delivers in, so
// this never diverges from Python's assumption when Python's assumption
// (already-chronological) happens to hold, and only helps when it doesn't.
//
// HEAD DEDUP -- Python's handle_googlechat_message relies on its own
// DBMessage.get_by_gcid existence check to silently drop the head reply if
// list_messages happens to include it again (portal.py:1348-1350); this
// bridge has no equivalent per-call DB read available here (fetchThreadMessages
// is a pure connector function with no DB access -- pkg/connector must stay
// framework-agnostic about persistence), so it applies the SAME anchor filter
// fetchTopicHeadMessages already uses for the flat/backward case, mirrored for
// the forward direction: exclude the anchor message itself (by id) and
// anything STRICTLY OLDER than it (never a distinct sibling at the exact same
// microsecond, matching cutoffMessages' own forward-trim criterion,
// portalbackfill.go's cutoffMessages "forward" branch: `msg.ID ==
// lastMessage.ID || msg.Timestamp.Before(lastMessage.Timestamp)`). Since the
// anchor is (initially) the topic's head reply -- always the OLDEST message
// in its own topic -- this both removes the head if the server re-sent it and
// never wrongly drops a genuine reply (every real reply's CreateTime is >=
// the head's).
//
// SINGLE-SHOT -- exactly like fetchTopicHeadMessages: one ListMessagesRequest
// with PageSize = params.Count, HasMore=false, Cursor="" always. No cursor
// exists to build (ListMessagesRequest/Response carry no page token, same
// grep-verified absence as ListTopicsRequest/Response -- see Task 1's region
// doc comment above), and single-shot is what the M6 plan and Python both
// call for.
func (c *GChatClient) fetchThreadMessages(ctx context.Context, params bridgev2.FetchMessagesParams) (*bridgev2.FetchMessagesResponse, error) {
	log := zerolog.Ctx(ctx)

	if params.AnchorMessage == nil {
		log.Warn().Msg("googlechat: thread backfill requested with no anchor message, cannot resolve topic id")
		return &bridgev2.FetchMessagesResponse{HasMore: false}, nil
	}
	meta, ok := params.AnchorMessage.Metadata.(*MessageMetadata)
	if !ok || meta == nil || meta.TopicID == "" {
		log.Warn().Msg("googlechat: thread backfill anchor message has no topic id in its metadata, cannot resolve topic")
		return &bridgev2.FetchMessagesResponse{HasMore: false}, nil
	}
	topicID := meta.TopicID

	group, err := gcid.ParsePortalID(params.Portal.ID)
	if err != nil {
		return nil, fmt.Errorf("googlechat: invalid portal id %q: %w", params.Portal.ID, err)
	}

	count := params.Count
	if count <= 0 {
		count = defaultFetchMessagesCount
	}

	list := c.listMessagesFn
	if list == nil {
		conn := c.getConn()
		if conn == nil {
			return nil, fmt.Errorf("googlechat: not connected")
		}
		list = conn.ListMessages
	}
	resp, err := list(ctx, &pb.ListMessagesRequest{
		ParentId: &pb.MessageParentId{
			Parent: &pb.MessageParentId_TopicId{
				TopicId: &pb.TopicId{
					TopicId: proto.String(topicID),
					GroupId: gchatmeow.PartsToGroupID(group.ID, group.IsDM),
				},
			},
		},
		PageSize: proto.Int32(int32(count)),
	})
	if err != nil {
		return nil, fmt.Errorf("googlechat: list_messages failed: %w", err)
	}

	// Defensive chronological sort -- see this region's doc comment above for
	// why this is safe regardless of the server's actual delivery order.
	// Ties broken by message id for deterministic output, mirroring
	// fetchTopicHeadMessages' own topic-id tiebreaker.
	msgs := slices.Clone(resp.GetMessages())
	slices.SortStableFunc(msgs, func(a, b *pb.Message) int {
		if byTime := cmp.Compare(a.GetCreateTime(), b.GetCreateTime()); byTime != 0 {
			return byTime
		}
		return cmp.Compare(a.GetId().GetMessageId(), b.GetId().GetMessageId())
	})

	getIntent := c.getIntentForFn
	if getIntent == nil {
		getIntent = func(ctx context.Context, portal *bridgev2.Portal, sender bridgev2.EventSender, evtType bridgev2.RemoteEventType) (bridgev2.MatrixAPI, bool) {
			return portal.GetIntentFor(ctx, sender, c.UserLogin, evtType)
		}
	}
	convert := convertMessageToMatrix(c.msgConverter(), c)

	anchorID := params.AnchorMessage.ID
	anchorMicros := gchatmeow.TimeToMicros(params.AnchorMessage.Timestamp)

	messages := make([]*bridgev2.BackfillMessage, 0, len(msgs))
	for _, msg := range msgs {
		gcMessageID := msg.GetId().GetMessageId()
		if gcMessageID == "" {
			log.Warn().Msg("googlechat: backfill thread message has no message id, skipping")
			continue
		}
		msgID := gcid.MakeMessageID(gcMessageID)
		// Anchor filter (forward direction -- see this region's doc comment's
		// "HEAD DEDUP" note above): exclude the anchor itself (by id) and
		// anything strictly OLDER than it; a distinct sibling AT the anchor's
		// exact microsecond survives, mirroring cutoffMessages' own forward
		// cutoff criterion.
		if msgID == anchorID || msg.GetCreateTime() < anchorMicros {
			continue
		}
		senderID := gcid.MakeUserID(msg.GetCreator().GetUserId().GetId())
		sender := bridgev2.EventSender{
			Sender:   senderID,
			IsFromMe: c.IsThisUser(ctx, senderID),
		}
		intent, ok := getIntent(ctx, params.Portal, sender, bridgev2.RemoteEventBackfill)
		if !ok {
			continue
		}
		cm, err := convert(ctx, params.Portal, intent, msg)
		if err != nil {
			log.Err(err).Str("gc_message_id", gcMessageID).Msg("googlechat: failed to convert backfill thread message, skipping")
			continue
		}
		// Reactions intentionally left nil here too -- see
		// fetchTopicHeadMessages' identical construction above for the full
		// rationale (GC's Reaction proto has no reactor id; Python parity;
		// live/catchUp already cover recoverable reactions).
		messages = append(messages, &bridgev2.BackfillMessage{
			ConvertedMessage: cm,
			Sender:           sender,
			ID:               msgID,
			Timestamp:        gchatmeow.MicrosToTime(msg.GetCreateTime()),
			StreamOrder:      msg.GetCreateTime(),
		})
	}

	return &bridgev2.FetchMessagesResponse{
		Messages: messages,
		HasMore:  false,
		Cursor:   "",
	}, nil
}

// endregion
