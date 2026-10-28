package migrate

import (
	"context"
	"fmt"

	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// insertMigratedUserPortalQuery writes one Go `user_portal` row; in_space, preferred and last_read have no Python source.
const insertMigratedUserPortalQuery = `
	INSERT INTO user_portal (bridge_id, user_mxid, login_id, portal_id, portal_receiver, in_space, preferred, last_read)
	VALUES ('', $1, $2, $3, $4, false, false, NULL)
`

// userLoginExistsInTarget guards user_portal's FK to user_login so a missing login is skipped, not fatal.
func userLoginExistsInTarget(ctx context.Context, target *dbutil.Database, id string) (bool, error) {
	var exists int
	err := target.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM user_login WHERE id = $1)`, id).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists != 0, nil
}

// migrateUserPortals synthesizes one Go `user_portal` row per DM portal, owned by the user whose gcid
// equals gc_receiver. Space portals have no recoverable membership and only produce a summary warning.
func migrateUserPortals(ctx context.Context, deps *Deps, opts Options) (int, []string, error) {
	portals, err := GetPortals(ctx, deps.Source)
	if err != nil {
		return 0, nil, fmt.Errorf("migrate: reading source portals for user_portal synthesis: %w", err)
	}
	users, err := GetUsers(ctx, deps.Source)
	if err != nil {
		return 0, nil, fmt.Errorf("migrate: reading source users for user_portal synthesis: %w", err)
	}

	// gcid -> mxid, to find a DM portal's owner by gc_receiver.
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
			// migratePortals already warned about this row.
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

// backfillTaskSeedingNote: the migration seeds no backfill_task rows. Migrated portals already have an mxid,
// so finishRoomCreate never runs; only Beeper-capable homeservers with backfill.queue.enabled are affected.
const backfillTaskSeedingNote = "backfill_task: 0 rows seeded by design -- every migrated portal already has its mxid set, so it never reaches finishRoomCreate (the only unconditional is_done/queue_done seeding point); see userportal.go's backfillTaskSeedingNote doc comment for a narrower EnsureExists/ChatResync caveat that applies only to Beeper-capable homeservers with backfill.queue.enabled"
