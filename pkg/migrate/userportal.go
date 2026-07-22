package migrate

// migrateUserPortals implements the user_portal migrator. The source has no
// dedicated per-user-per-portal membership table at all; user_portal is
// *synthesized*, DM portals only. Same raw-INSERT-through-ctx approach as the
// rest of this package -- see portal.go's package doc comment.
//
// This is a SEPARATE migration step from migrateUsers, run AFTER both
// migratePortals (portal FK) and migrateUsers (user_login FK) -- see
// migrate.go's Run step ordering.

import (
	"context"
	"fmt"

	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// insertMigratedUserPortalQuery writes one Go `user_portal` row per schema
// map §6. in_space/preferred have no Python source at all -- both default
// false (self-heals: bridgev2 recomputes space membership on next sync, and
// "preferred" is a UI nicety, not correctness-critical); last_read has no
// Python source either and IS nullable, so it's hardcoded NULL.
const insertMigratedUserPortalQuery = `
	INSERT INTO user_portal (bridge_id, user_mxid, login_id, portal_id, portal_receiver, in_space, preferred, last_read)
	VALUES ('', $1, $2, $3, $4, false, false, NULL)
`

// userLoginExistsInTarget reports whether the Go `user_login` row (id) has
// already been written to the target -- guards user_portal's FK to
// user_login(bridge_id, id) (schema map §6's own FK note: "the referenced
// user_login must exist in the target"). A DM portal's owning user can
// legitimately have no migrated login at all (e.g. migrateUsers itself
// skipped it for missing/NULL cookies) -- without this check, inserting the
// user_portal row would fail the FK and abort the ENTIRE migration instead
// of warning and skipping just this one row, the same rule
// portalExistsInTarget/ghostExistsInTarget already follow (message.go).
func userLoginExistsInTarget(ctx context.Context, target *dbutil.Database, id string) (bool, error) {
	var exists int
	err := target.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM user_login WHERE id = $1)`, id).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists != 0, nil
}

// migrateUserPortals reads every row of the source Python `portal` table and
// synthesizes one Go `user_portal` row for each DM portal (schema map §6):
// the owning user is found by matching the DM portal's gc_receiver against a
// Python user's own gcid, and login_id/portal_id/portal_receiver are all
// identity copies (schema map §0). Space (group) portals have NO
// reconstructable per-user membership at all -- Python's portal.gc_receiver
// is always "" for a space, and there is no Python table recording which
// local logins are members of which shared space -- so every space portal
// instead contributes to ONE summary warning (a count, not one warning per
// portal): this is a documented, accepted gap, not a bug (schema map §6 /
// Risk #5); affected users simply regain their space memberships on the next
// live sync pass.
func migrateUserPortals(ctx context.Context, deps *Deps, opts Options) (int, []string, error) {
	portals, err := GetPortals(ctx, deps.Source)
	if err != nil {
		return 0, nil, fmt.Errorf("migrate: reading source portals for user_portal synthesis: %w", err)
	}
	users, err := GetUsers(ctx, deps.Source)
	if err != nil {
		return 0, nil, fmt.Errorf("migrate: reading source users for user_portal synthesis: %w", err)
	}

	// gcid -> mxid, for the gc_receiver -> owning-user lookup (schema map
	// §6: "Look up user_mxid via user.gcid == portal.gc_receiver").
	mxidByGCID := make(map[string]string, len(users))
	for _, u := range users {
		if u.GCID.Valid && u.GCID.String != "" {
			mxidByGCID[u.GCID.String] = u.MXID
		}
	}

	var warnings []string
	count := 0
	spaceCount := 0

	for _, p := range portals {
		group, parseErr := gcid.ParsePortalID(networkid.PortalID(p.GCID))
		if parseErr != nil {
			// migratePortals already warned about this exact row (it hits
			// the identical parse failure on the identical gcid) -- there is
			// nothing to synthesize either way, so this is a silent skip
			// here rather than a second warning for the same root cause.
			continue
		}
		if !group.IsDM {
			spaceCount++
			continue
		}

		mxid, ok := mxidByGCID[p.GCReceiver]
		if !ok {
			warnings = append(warnings, fmt.Sprintf("user_portal (portal=%q): no migrated user has gcid=%q (portal.gc_receiver), skipping", p.GCID, p.GCReceiver))
			continue
		}

		loginID := gcid.MakeUserLoginID(p.GCReceiver)
		loginOK, err := userLoginExistsInTarget(ctx, deps.Target, string(loginID))
		if err != nil {
			return count, warnings, fmt.Errorf("migrate: checking target user_login for user_portal (portal=%q): %w", p.GCID, err)
		}
		if !loginOK {
			warnings = append(warnings, fmt.Sprintf("user_portal (portal=%q, user=%q): user_login %q was not migrated (missing/incomplete cookies), skipping", p.GCID, mxid, loginID))
			continue
		}

		portalOK, err := portalExistsInTarget(ctx, deps.Target, p.GCID, p.GCReceiver)
		if err != nil {
			return count, warnings, fmt.Errorf("migrate: checking target portal for user_portal (portal=%q): %w", p.GCID, err)
		}
		if !portalOK {
			warnings = append(warnings, fmt.Sprintf("user_portal (portal=%q): target portal was not migrated, skipping", p.GCID))
			continue
		}

		if _, err := deps.Target.Exec(ctx, insertMigratedUserPortalQuery, mxid, string(loginID), p.GCID, p.GCReceiver); err != nil {
			return count, warnings, fmt.Errorf("migrate: inserting user_portal (portal=%q, user=%q): %w", p.GCID, mxid, err)
		}
		count++
	}

	if spaceCount > 0 {
		warnings = append(warnings, fmt.Sprintf("%d space portal(s) have no reconstructable user_portal membership (self-heals on next sync)", spaceCount))
	}

	return count, warnings, nil
}

// backfillTaskSeedingNote documents M7 Task 7's backfill_task decision
// (schema map Risk #11, resolved by m7-migration-preflight.md's
// controller-level verification): this migration tool writes ZERO
// backfill_task rows. Wired into every Summary via Run (migrate.go).
//
// Why zero is safe: bridgev2.Portal.CreateMatrixRoom (portal.go) only ever
// runs for a portal whose mxid is still "" -- every migrated portal already
// has its mxid set (the Python-created Matrix room itself is what's being
// migrated), so it can never reach finishRoomCreate, the ONLY place that
// calls BackfillTask.Upsert with an explicit is_done/queue_done value
// (bridgev2/portal.go:5363). Forward-resync backfill also returns an empty,
// HasMore=false response for an existing, non-empty portal
// (pkg/connector/backfill.go's FetchMessages doc comment, M6 Task 3.5) --
// M2's catch_up_user already owns filling any gap for an existing room.
//
// A NARROWER, separate auto-create path DOES exist and is recorded here even
// though it does not change the zero-rows decision:
// bridgev2.Portal.UpdateInfo (portal.go:5093-5099) calls
// BackfillTask.EnsureExists whenever `info.CanBackfill && source != nil &&
// portal.MXID != ""` -- info.CanBackfill is `true` on EVERY ChatInfo this
// connector produces (pkg/connector/chatinfo.go), and the `portal.MXID !=
// ""` guard means this fires precisely for a portal that ALREADY has a
// room -- reachable for a migrated portal via
// Portal.handleRemoteChatResync (portal.go:3856,3864). That resync path IS
// live for this bridge: pkg/connector/sync.go's syncChats emits a
// simplevent.ChatResync for every chat on every chat-list sync (M1),
// regardless of whether the portal is new or migrated. EnsureExists's
// INSERT creates a fresh is_done=false, queue_done=false row when none
// exists yet; its ON CONFLICT branch only ever touches
// user_login_id/next_dispatch_min_ts, never is_done/queue_done, so it can't
// un-suppress an already-seeded done row -- it just doesn't write one in the
// first place when nothing seeded it.
//
// This tool still writes zero rows despite that, because the ONLY consumer
// of an is_done=false row (backfillqueue.go's RunBackfillQueue, via
// BackfillTask.GetNext) only starts when BOTH backfill.queue.enabled is true
// AND the connected homeserver reports Beeper's BeeperFeatureBatchSending
// capability -- a standard self-hosted Synapse/Dendrite/Conduit deployment
// (this bridge's primary target) never reports that capability, so the
// queue never runs there and an EnsureExists-created row stays permanently
// dormant (pkg/connector/backfill.go documents this exact gating
// independently, from M6 Task 3.5, confirmed again while resolving this
// task). On a Beeper-capable homeserver WITH backfill.queue.enabled, this
// narrow path is a real, if rare, residual gap this migration tool does not
// close -- flagged here for a follow-up decision (M7 Task 8/9) rather than
// silently accepted: seeding an is_done=true, queue_done=true row per
// migrated DM/space portal here would cheaply close it too, since
// EnsureExists's ON CONFLICT branch would then never touch those two
// columns.
const backfillTaskSeedingNote = "backfill_task: 0 rows seeded by design -- every migrated portal already has its mxid set, so it never reaches finishRoomCreate (the only unconditional is_done/queue_done seeding point); see userportal.go's backfillTaskSeedingNote doc comment for a narrower EnsureExists/ChatResync caveat that applies only to Beeper-capable homeservers with backfill.queue.enabled"
