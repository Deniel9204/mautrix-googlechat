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
	"fmt"
	"slices"
	"strconv"

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
// CURSOR ENCODING -- also a deliberate design choice forced by the same
// proto: neither ListTopicsRequest nor ListTopicsResponse carries anything
// resembling a continuation/page token (grepped the whole proto file: the
// only page_token/next_page_token pair in existence belongs to the unrelated
// ListMembersRequest/Response). ListTopicsRequest's user_not_older_than /
// group_not_older_than (ReferenceRevision{Timestamp}) look tempting but are a
// consistency FLOOR ("don't serve me anything staler than this revision"),
// not a backward filter ("give me topics before this point") -- and
// portal.py's own ListTopicsRequest construction never sets either field, so
// there is no reference behavior to confirm that reading. Using them for
// pagination would be untested behavior against a private, reverse-engineered
// API; a silently-wrong guess here would corrupt backfilled history (skipped
// or duplicated messages) with no test able to catch it against a real
// server.
//
// Instead, pagination is built entirely on the ONE real, observed knob:
// PageSizeForTopics deterministically returns "the N most recent topics".
// Cursor = the number of (raw, pre-anchor-filter) topics already delivered
// across all pages of this backfill run so far, encoded as a decimal string.
// Each call requests PageSizeForTopics = delivered + Count (asking for
// cumulatively more), reverses the response into oldest-first order, and the
// NEW page is the front slice `topics[:len(topics)-delivered]` -- the portion
// that fell outside every previous, smaller request. Concretely, with
// Count=3 over a 10-topic history: page 1 requests 3 or gets topics
// {8,9,10}; page 2 requests 6, gets {5..10}, and the new slice is
// {5,6,7} == topics[:6-3]; page 3 requests 9, gets {2..10}, new slice
// {2,3,4} == topics[:9-6]; page 4 requests 12, the server can only return all
// 10 (len(topics)=10 < requested 12, the exhaustion signal), new slice is the
// single remaining {1} == topics[:10-9]. This deliberately re-fetches
// already-seen topics on every page (bounded re-fetch, not O(1) continuation)
// -- acceptable ONLY because Task 3's config-driven initial-history limit
// (mirroring Python's initial_nonthread_limit default of 100) keeps the total
// page count, and therefore the cumulative re-fetch, small for a one-time
// per-portal operation; it would not be an acceptable strategy for a hot,
// frequently-repeated pagination path.
//
// HasMore is false once the server returns FEWER topics than requested (nothing
// left to grow into) or GetContainsFirstTopic() is set (the response now
// includes the group's very first topic) -- either one independently means
// there is no more history beyond what has been delivered.
//
// SCOPE (documented, not silent, gaps -- left for later M6 tasks):
//   - A topic with a real Google Chat thread even inside an otherwise-flat
//     room (topic_read_state.thread_created_usec > 0) is bridged as ONLY its
//     head reply here; Python would also fetch and bridge its other replies
//     via ListMessagesRequest (portal.py:432-441). Task 2 owns the
//     ThreadRoot-scoped FetchMessages dispatch (ShouldBackfillThread +
//     LastThreadMessage) that this naturally extends into.
//   - A topic whose head reply is a SYSTEM_MESSAGE (membership/room-info
//     change) is converted through the ordinary text path
//     (convertMessageToMatrix) here, not systemmessage.go's trySystemMessage
//     (which only runs on the live MESSAGE_POSTED event-dispatch path,
//     events.go's handleMessagePosted) -- flagged for a follow-up milestone
//     task's whole-branch review rather than silently gapped.
//   - ThreadsOnly portals (whole "threaded spaces") and any ThreadRoot-scoped
//     call are Task 2's strategy; FetchMessages below returns an empty,
//     HasMore=false response for both rather than guessing at behavior this
//     task does not own.
//   - Forward=true (bridgev2's forward-backfill direction) returns an empty,
//     HasMore=false response: GC's list_topics only exposes "the N most
//     recent", which is inherently a BACKWARD-from-now query with no way to
//     ask "the N oldest of what's newer than X" -- matching the brief's own
//     "return the newer messages or empty; for M6 focus on backward/initial"
//     guidance and mautrix-meta's own early-return for an unsupported forward
//     case (_reference/meta/pkg/connector/backfill.go's FetchMessages).
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
// but nothing in the interface contract guarantees that). Task 3 wires
// the real config-driven initial-history limit; this is not that limit.
const defaultFetchMessagesCount = 20

var _ bridgev2.BackfillingNetworkAPI = (*GChatClient)(nil)

// FetchMessages implements bridgev2.BackfillingNetworkAPI for history
// backfill (M6 Task 1). Dispatches to fetchFlatMessages for a non-threaded
// top-level request; every other combination (Forward, a ThreadRoot-scoped
// call, or a ThreadsOnly portal) is not this task's scope -- see the SCOPE
// note in this file's region comment above -- and returns an empty,
// HasMore=false response rather than an error, so the framework's backfill
// queue treats it as "nothing more to do here" instead of a failure.
func (c *GChatClient) FetchMessages(ctx context.Context, params bridgev2.FetchMessagesParams) (*bridgev2.FetchMessagesResponse, error) {
	if params.Forward {
		return &bridgev2.FetchMessagesResponse{HasMore: false}, nil
	}
	if params.ThreadRoot != "" {
		return &bridgev2.FetchMessagesResponse{HasMore: false}, nil
	}
	if meta, ok := params.Portal.Metadata.(*PortalMetadata); ok && meta != nil && meta.ThreadsOnly {
		return &bridgev2.FetchMessagesResponse{HasMore: false}, nil
	}
	return c.fetchFlatMessages(ctx, params)
}

// fetchFlatMessages pages a flat portal's message history via list_topics,
// taking each topic's head reply (topic.replies[0]) as the flat message --
// see this file's region doc comment above for the full RPC-choice, cursor,
// and scope rationale.
func (c *GChatClient) fetchFlatMessages(ctx context.Context, params bridgev2.FetchMessagesParams) (*bridgev2.FetchMessagesResponse, error) {
	group, err := gcid.ParsePortalID(params.Portal.ID)
	if err != nil {
		return nil, fmt.Errorf("googlechat: invalid portal id %q: %w", params.Portal.ID, err)
	}

	delivered, err := decodeBackfillCursor(params.Cursor)
	if err != nil {
		return nil, err
	}
	count := params.Count
	if count <= 0 {
		count = defaultFetchMessagesCount
	}
	requestSize := delivered + count

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
		PageSizeForTopics: proto.Int32(int32(requestSize)),
	})
	if err != nil {
		return nil, fmt.Errorf("googlechat: list_topics failed: %w", err)
	}

	// Server order is newest-first (portal.py:428's own comment: "The
	// reversed list is probably already sorted properly, but re-sort it just
	// in case" -- ported literally: reverse first, then a stable sort by
	// SortTime, so ties keep the server's original relative order after the
	// reversal, exactly like Python's `sorted(reversed(topics), key=...)`).
	topics := slices.Clone(resp.GetTopics())
	slices.Reverse(topics)
	slices.SortStableFunc(topics, func(a, b *pb.Topic) int {
		return int(a.GetSortTime() - b.GetSortTime())
	})

	if delivered > len(topics) {
		delivered = len(topics)
	}
	newTopics := topics[:len(topics)-delivered]

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
	messages := make([]*bridgev2.BackfillMessage, 0, len(newTopics))
	for _, topic := range newTopics {
		replies := topic.GetReplies()
		if len(replies) == 0 {
			continue
		}
		msg := replies[0]
		// Anchor filter: exclude anything at or after the portal's oldest
		// already-bridged message, mirroring meta's own wrapBackfillEvents
		// slices.DeleteFunc gate for the !forward direction
		// (_reference/meta/pkg/connector/backfill.go). Only ever excludes
		// entries from the newest (first-requested) page in practice, since
		// every later page's newTopics slice is, by construction, entirely
		// older than anything a previous page already delivered.
		if hasAnchor && msg.GetCreateTime() >= anchorMicros {
			continue
		}
		gcMessageID := msg.GetId().GetMessageId()
		if gcMessageID == "" {
			log.Warn().Msg("googlechat: backfill topic head reply has no message id, skipping")
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
		messages = append(messages, &bridgev2.BackfillMessage{
			ConvertedMessage: cm,
			Sender:           sender,
			ID:               gcid.MakeMessageID(gcMessageID),
			Timestamp:        gchatmeow.MicrosToTime(msg.GetCreateTime()),
			StreamOrder:      msg.GetCreateTime(),
		})
	}

	hasMore := len(topics) >= requestSize && !resp.GetContainsFirstTopic()
	return &bridgev2.FetchMessagesResponse{
		Messages: messages,
		Cursor:   encodeBackfillCursor(delivered + len(newTopics)),
		HasMore:  hasMore,
	}, nil
}

// decodeBackfillCursor parses the "topics delivered so far" counter this
// file's Cursor encodes (see the region doc comment above). An empty cursor
// (the first page of a backfill run) decodes to 0. Any other malformed value
// is a genuine error -- a corrupted/foreign cursor must not be silently
// treated as "start over", which would re-deliver already-bridged history.
func decodeBackfillCursor(cursor networkid.PaginationCursor) (int, error) {
	if cursor == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(string(cursor))
	if err != nil || n < 0 {
		return 0, fmt.Errorf("googlechat: invalid backfill cursor %q", cursor)
	}
	return n, nil
}

// encodeBackfillCursor is decodeBackfillCursor's inverse.
func encodeBackfillCursor(delivered int) networkid.PaginationCursor {
	return networkid.PaginationCursor(strconv.Itoa(delivered))
}

// endregion
