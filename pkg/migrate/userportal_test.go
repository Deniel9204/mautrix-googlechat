package migrate

// White-box tests (package migrate) for migrateUserPortals -- M7 Task 7. See
// .superpowers/sdd/m7-migration-schema-map.md §6 (user_portal) for the
// DM-only synthesis rule under test, and the task-7 brief's backfill_task
// decision (documented in userportal.go's backfillTaskSeedingNote).
//
// Like message_test.go, these tests run the real migratePortals/migrateUsers
// migrators first (against the same Deps) rather than hand-seeding the
// target, since migrateUserPortals's FK guards (userLoginExistsInTarget,
// portalExistsInTarget) need those rows to actually exist in the target the
// same way a real migration run would produce them.

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"go.mau.fi/util/dbutil"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

type migratedUserPortalRow struct {
	UserMXID       string
	LoginID        string
	PortalID       string
	PortalReceiver string
	InSpace        bool
	Preferred      bool
	LastRead       sql.NullInt64
}

func readMigratedUserPortal(t *testing.T, target *dbutil.Database, portalID, portalReceiver string) migratedUserPortalRow {
	t.Helper()
	var row migratedUserPortalRow
	err := target.QueryRow(context.Background(), `
		SELECT user_mxid, login_id, portal_id, portal_receiver, in_space, preferred, last_read
		FROM user_portal WHERE portal_id = ? AND portal_receiver = ?
	`, portalID, portalReceiver).Scan(
		&row.UserMXID, &row.LoginID, &row.PortalID, &row.PortalReceiver, &row.InSpace, &row.Preferred, &row.LastRead,
	)
	if err != nil {
		t.Fatalf("reading back migrated user_portal (portal_id=%q, portal_receiver=%q): %v", portalID, portalReceiver, err)
	}
	return row
}

func countUserPortals(t *testing.T, target *dbutil.Database) int {
	t.Helper()
	var count int
	if err := target.QueryRow(context.Background(), `SELECT COUNT(*) FROM user_portal`).Scan(&count); err != nil {
		t.Fatalf("counting user_portal rows: %v", err)
	}
	return count
}

// runFullChainUpToUserPortals runs every migrator migrateUserPortals depends
// on (portals, then users, then logins -- ghosts/messages/reactions are
// irrelevant to user_portal's FK guards) against the same Deps, failing the
// test immediately on any unexpected error, then returns migrateUserPortals's
// own result for the caller to assert on.
func runFullChainUpToUserPortals(t *testing.T, ctx context.Context, deps *Deps) (int, []string) {
	t.Helper()
	if _, _, err := migratePortals(ctx, deps, Options{}); err != nil {
		t.Fatalf("migratePortals: %v", err)
	}
	if _, _, err := migrateUsers(ctx, deps, Options{}); err != nil {
		t.Fatalf("migrateUsers: %v", err)
	}
	if _, _, err := migrateUserLogins(ctx, deps, Options{}); err != nil {
		t.Fatalf("migrateUserLogins: %v", err)
	}
	count, warnings, err := migrateUserPortals(ctx, deps, Options{})
	if err != nil {
		t.Fatalf("migrateUserPortals: %v", err)
	}
	return count, warnings
}

func TestMigrateUserPortals_DMSynthesisAndSpaceGap(t *testing.T) {
	deps := &Deps{
		Source: newFixtureSourceDB(t),
		Target: newFixtureTargetDB(t),
	}
	ctx := context.Background()

	count, warnings := runFullChainUpToUserPortals(t, ctx, deps)

	// Shared fixture has 1 DM portal (dm:gGboEmVbEyE, receiver
	// 103432744896036786916, owned by @user1:example.com who DOES get a
	// migrated login) and 1 space portal (space:AAAAdaMWXXc).
	if count != 1 {
		t.Fatalf("expected 1 user_portal row (the DM portal), got %d", count)
	}

	row := readMigratedUserPortal(t, deps.Target, "dm:gGboEmVbEyE", "103432744896036786916")
	if row.UserMXID != "@user1:example.com" {
		t.Errorf("user_mxid = %q, want \"@user1:example.com\" (found via gc_receiver == user.gcid)", row.UserMXID)
	}
	wantLoginID := string(gcid.MakeUserLoginID("103432744896036786916"))
	if row.LoginID != wantLoginID {
		t.Errorf("login_id = %q, want %q (== portal.receiver)", row.LoginID, wantLoginID)
	}
	if row.PortalReceiver != "103432744896036786916" {
		t.Errorf("portal_receiver = %q", row.PortalReceiver)
	}
	if row.InSpace {
		t.Error("in_space = true, want false (no Python source, default)")
	}
	if row.Preferred {
		t.Error("preferred = true, want false (no Python source, default)")
	}
	if row.LastRead.Valid {
		t.Errorf("last_read = %+v, want NULL (no Python source)", row.LastRead)
	}

	// The space portal must NOT get a user_portal row -- confirm via the
	// total table count (1, not 2).
	if got := countUserPortals(t, deps.Target); got != 1 {
		t.Errorf("total user_portal rows = %d, want 1 (space portal must not synthesize one)", got)
	}

	// Exactly one summary warning, about the one un-reconstructable space
	// portal (schema map §6 / Risk #5).
	if len(warnings) != 1 {
		t.Fatalf("expected exactly 1 warning (the space-portal gap summary), got %d: %v", len(warnings), warnings)
	}
	if !strings.Contains(warnings[0], "1 space portal") {
		t.Errorf("warning = %q, want it to mention \"1 space portal\"", warnings[0])
	}
	if !strings.Contains(warnings[0], "self-heals") {
		t.Errorf("warning = %q, want it to document the gap as self-healing", warnings[0])
	}
}

// newUserPortalEdgeCasesSourceDB covers the two DM-portal failure paths that
// the shared fixture doesn't exercise:
//   - dm:noLoginRoom, receiver=555555555555555555: the owning user
//     (@partial:example.com) exists and has a matching gcid, but NO cookies
//     (so migrateUsers never writes it a user_login) -- "missing user_login"
//     warn+skip.
//   - dm:noUserRoom, receiver=666666666666666666: no Python user has this
//     gcid at all -- "no migrated user" warn+skip.
func newUserPortalEdgeCasesSourceDB(t *testing.T) *dbutil.Database {
	t.Helper()
	path := filepath.Join(t.TempDir(), "userportal_edge_cases_source.db")
	setup, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("opening fixture source for setup: %v", err)
	}
	if _, err := setup.Exec(fixtureSchemaSQL); err != nil {
		t.Fatalf("creating fixture schema: %v", err)
	}

	stmts := []struct {
		query string
		args  []any
	}{
		{
			`INSERT INTO "user" (mxid, gcid, cookies, user_agent, notice_room, revision) VALUES (?, ?, ?, ?, ?, ?)`,
			[]any{"@partial:example.com", "555555555555555555", nil, nil, nil, nil},
		},
		{
			`INSERT INTO portal (gcid, gc_receiver, other_user_id, mxid, name, avatar_mxc, description, name_set, avatar_set, description_set, encrypted, revision, threads_only, threads_enabled) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			[]any{"dm:noLoginRoom", "555555555555555555", nil, "!noLogin:example.com", nil, nil, nil, false, false, false, false, nil, nil, nil},
		},
		{
			`INSERT INTO portal (gcid, gc_receiver, other_user_id, mxid, name, avatar_mxc, description, name_set, avatar_set, description_set, encrypted, revision, threads_only, threads_enabled) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			[]any{"dm:noUserRoom", "666666666666666666", nil, "!noUser:example.com", nil, nil, nil, false, false, false, false, nil, nil, nil},
		},
	}
	for _, s := range stmts {
		if _, err := setup.Exec(s.query, s.args...); err != nil {
			t.Fatalf("seeding fixture row %q: %v", s.query, err)
		}
	}
	if err := setup.Close(); err != nil {
		t.Fatalf("closing fixture setup connection: %v", err)
	}

	db, err := OpenSource(path)
	if err != nil {
		t.Fatalf("OpenSource(%q): %v", path, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestMigrateUserPortals_MissingLoginAndMissingUser_WarnAndSkip(t *testing.T) {
	deps := &Deps{
		Source: newUserPortalEdgeCasesSourceDB(t),
		Target: newFixtureTargetDB(t),
	}
	ctx := context.Background()

	count, warnings := runFullChainUpToUserPortals(t, ctx, deps)

	if count != 0 {
		t.Fatalf("expected 0 user_portal rows (both DM portals should be skipped), got %d", count)
	}
	if got := countUserPortals(t, deps.Target); got != 0 {
		t.Errorf("total user_portal rows = %d, want 0", got)
	}

	if len(warnings) != 2 {
		t.Fatalf("expected exactly 2 warnings, got %d: %v", len(warnings), warnings)
	}

	var sawMissingLogin, sawMissingUser bool
	for _, w := range warnings {
		if strings.Contains(w, "dm:noLoginRoom") && strings.Contains(w, "not migrated") {
			sawMissingLogin = true
		}
		if strings.Contains(w, "dm:noUserRoom") && strings.Contains(w, "no migrated user") {
			sawMissingUser = true
		}
	}
	if !sawMissingLogin {
		t.Errorf("expected a warning about dm:noLoginRoom's missing user_login, got %v", warnings)
	}
	if !sawMissingUser {
		t.Errorf("expected a warning about dm:noUserRoom having no migrated user, got %v", warnings)
	}
}

// TestRun_SeedsNoBackfillTaskRows pins the M7 Task 7 backfill_task decision
// (schema map Risk #11, resolved in m7-migration-preflight.md, documented in
// full in userportal.go's backfillTaskSeedingNote): a complete migration Run
// must write ZERO backfill_task rows, and must document that decision in
// Summary.Warnings so an operator reading the migration report sees it
// explicitly rather than just an empty table.
func TestRun_SeedsNoBackfillTaskRows(t *testing.T) {
	deps := validDeps(t)
	ctx := context.Background()

	summary, err := Run(ctx, deps, Options{SourceDSN: "test-dsn"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	var count int
	if err := deps.Target.QueryRow(ctx, `SELECT COUNT(*) FROM backfill_task`).Scan(&count); err != nil {
		t.Fatalf("counting backfill_task rows: %v", err)
	}
	if count != 0 {
		t.Errorf("backfill_task row count = %d, want 0", count)
	}

	found := false
	for _, w := range summary.Warnings {
		if strings.Contains(w, "backfill_task") && strings.Contains(w, "0 rows seeded by design") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected Summary.Warnings to document the zero-backfill_task decision, got %v", summary.Warnings)
	}
}
