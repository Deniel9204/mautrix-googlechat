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

// catchUp replays whatever happened on this account between the last
// successfully processed revision (UserLoginMetadata.Revision) and now, via
// a single catch_up_user RPC, and dispatches every returned event through
// the SAME handleGChatEvent path a live stream event takes (events.go) --
// so M4's later edit/reaction/delete handlers apply to gap-replayed events
// automatically, with no separate backfill-specific event handling to keep
// in sync. Idempotency (a caught-up event that also arrives live, or is
// replayed again by an overlapping window on a later reconnect) is left to
// bridgev2's own message-id-keyed dedup on the RemoteMessage path
// (portal.go's checkFakeMessage/message-exists lookup) exactly like a live
// duplicate would be -- this function does no de-duplication of its own.
//
// Intended to run on every Connected transition AFTER the first for the
// current conn (client.go's handleConnState, gated by the SAME
// shouldSyncOnConnect latch syncChats uses for the first-connect case --
// see handleConnState's doc comment for why reusing that latch, rather than
// adding a second one, is both correct and required by this task).
//
// The revision watermark is persisted ONLY once every returned event has
// been dispatched, and only if this function does not return early -- an
// RPC error, or a non-COMPLETED/PAGINATED response status, leaves
// UserLoginMetadata.Revision untouched so the NEXT reconnect retries the
// exact same window instead of silently skipping it (no gap loss on a
// transient catch-up failure). This mirrors Python's own set_revision guard
// (db/user.py:104-109: never move a stored revision backwards) applied to a
// single account-wide watermark instead of Python's per-object ones.
func (c *GChatClient) catchUp(ctx context.Context) {
	log := zerolog.Ctx(ctx)

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
	req := &pb.CatchUpUserRequest{
		Range: &pb.CatchUpRange{
			FromRevisionTimestamp: proto.Int64(fromRevision),
			// ToRevisionTimestamp is deliberately left unset: unlike
			// catch_up_group's portal.py caller (which always knows a
			// freshly-fetched target revision from the paginated_world
			// response that triggered it), a reconnect here has no such
			// upper bound to supply -- an unset optional field asks the
			// server for everything since fromRevision, i.e. "catch me up
			// to now".
		},
	}

	resp, err := fetch(ctx, req)
	if err != nil {
		log.Err(err).Msg("googlechat: catch_up_user failed, revision watermark left unchanged (retried on next reconnect)")
		return
	}
	if status := resp.GetStatus(); status != pb.CatchUpResponse_COMPLETED && status != pb.CatchUpResponse_PAGINATED {
		// Mirrors portal.py:474-480's status check for catch_up_group: any
		// ABORTED_* status means the server could not honor the requested
		// range at all (cutoff exceeded, cache invalidated, or the
		// requested from-revision has aged out server-side) -- there is
		// nothing safe to replay, and nowhere further along to advance the
		// watermark to.
		log.Warn().Str("status", status.String()).Msg("googlechat: catch_up_user did not complete, revision watermark left unchanged")
		return
	}

	newRevision := fromRevision
	for _, evt := range resp.GetEvents() {
		if r := eventRevision(evt); r > newRevision {
			newRevision = r
		}
		for _, flat := range gchatmeow.SplitEventBodies(evt) {
			c.handleGChatEvent(ctx, flat)
		}
	}
	log.Debug().
		Int("event_count", len(resp.GetEvents())).
		Int64("from_revision", fromRevision).
		Int64("new_revision", newRevision).
		Str("status", resp.GetStatus().String()).
		Msg("googlechat: catch_up_user replay complete")

	if newRevision <= fromRevision {
		// Nothing new: either zero events, or none of them carried a
		// revision field greater than what we already had. Leave the
		// watermark exactly as it was rather than persisting a value that
		// isn't actually further along (and skip the write entirely).
		return
	}
	if err := c.updateMetadata(ctx, func(meta *UserLoginMetadata) bool {
		if meta.Revision >= newRevision {
			return false
		}
		meta.Revision = newRevision
		return true
	}); err != nil {
		log.Err(err).Msg("googlechat: failed to persist catch-up revision watermark")
	}
}

// getRevision snapshots the current catch_up_user watermark
// (UserLoginMetadata.Revision) under metaMu -- mirrors Connect's cookie
// snapshot read (client.go): meta.Revision is also written by
// updateMetadata (catchUp's own watermark advance, above) from potentially
// a different goroutine, so reading the field directly (unlocked) would
// race it.
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
// *WriteRevision returns 0). Computed from the RAW, pre-split multi-body
// event -- catch_up_user's response carries the revision once per multi_evt,
// not once per flattened body -- mirroring portal.py:502-503's
// `multi_evt.group_revision` and user.py:681-682's `evt.user_revision`,
// both read from the event BEFORE split_event_bodies flattens it.
func eventRevision(evt *pb.Event) int64 {
	r := evt.GetUserRevision().GetTimestamp()
	if g := evt.GetGroupRevision().GetTimestamp(); g > r {
		r = g
	}
	return r
}
