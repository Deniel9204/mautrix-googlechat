package migrate

// White-box tests (package migrate, not migrate_test) for the engine
// skeleton (Run, its guards, and the cookie/timestamp helpers). The fixture
// TARGET database applies the REAL bridgev2 schema (via
// bridgev2/database.New + dbutil.Database.Upgrade) rather than a hand-picked
// subset of tables: that's the exact upgrade path a live bridge runs at
// startup, so it exercises the real portal/user_login/ghost/message/
// reaction/user table shapes Tasks 5-7 will write into, not an
// approximation of them. See m7-task-4-report.md for why this was
// preferred over hand-writing just the two guard tables.

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"go.mau.fi/util/dbutil"
	_ "go.mau.fi/util/dbutil/litestream" // registers the "sqlite3-fk-wal" driver, see initDB's own comment in mxmain.

	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"
)

// newFixtureTargetDB builds a fresh, empty TARGET bridgev2 database by
// running the real upgrade table bridgev2 ships (bridgev2/database/upgrades)
// against a throwaway SQLite file -- the same mechanism mxmain.BridgeMain's
// initDB triggers on the live bridge's own configured database, just
// pointed at a temp file instead of the operator's real one.
func newFixtureTargetDB(t *testing.T) *dbutil.Database {
	t.Helper()
	path := filepath.Join(t.TempDir(), "target.db")
	db, err := dbutil.NewWithDialect(fmt.Sprintf("file:%s?_txlock=immediate", path), "sqlite3-fk-wal")
	if err != nil {
		t.Fatalf("opening fixture target db: %v", err)
	}
	// database.New's return value is a query-helper wrapper we don't need
	// in this test -- its only relevant side effect is setting
	// db.UpgradeTable to the real bridgev2 schema below.
	database.New("", database.MetaTypes{}, db)
	if err := db.Upgrade(context.Background()); err != nil {
		t.Fatalf("upgrading fixture target db to latest bridgev2 schema: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// testFormatGhostMXID stands in for the live bridge's
// matrix.Connector.FormatGhostMXID in tests -- Run/the migrators never call
// it in Task 4 (all per-entity migrators are still stubs), so its exact
// output doesn't matter here, only that Deps.FormatGhostMXID is non-nil and
// has the right signature.
func testFormatGhostMXID(userID networkid.UserID) id.UserID {
	return id.NewUserID("googlechat_"+string(userID), "example.com")
}

// validDeps builds a *Deps with a fresh fixture source and a fresh, empty
// fixture target -- i.e. deps that should let Run proceed all the way
// through without tripping any guard.
func validDeps(t *testing.T) *Deps {
	return &Deps{
		Source:          newFixtureSourceDB(t),
		Target:          newFixtureTargetDB(t),
		FormatGhostMXID: testFormatGhostMXID,
	}
}

// assertTargetEmpty fails the test if any of the six tables Run's migrators
// write into have any rows -- used both to confirm a dry run left the
// target untouched, and (implicitly, by construction) that a fresh fixture
// target really does start empty.
func assertTargetEmpty(t *testing.T, target *dbutil.Database) {
	t.Helper()
	ctx := context.Background()
	for _, table := range []string{"portal", "user_login", "ghost", "message", "reaction", "user"} {
		var count int
		if err := target.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %q", table)).Scan(&count); err != nil {
			t.Fatalf("counting %s: %v", table, err)
		}
		if count != 0 {
			t.Errorf("expected table %q to be empty, got %d row(s)", table, count)
		}
	}
}

// seedOnePortalRow writes directly into the target's portal table,
// bypassing Run entirely -- simulating a target database that has already
// been used by a live bridge (or a previous migration run), which is
// exactly what the non-empty-target guard (targetHasExistingData) exists to
// detect.
func seedOnePortalRow(t *testing.T, target *dbutil.Database) {
	t.Helper()
	_, err := target.Exec(context.Background(), `
		INSERT INTO portal (bridge_id, id, receiver, name, topic, avatar_id, avatar_hash, avatar_mxc, name_set, avatar_set, topic_set, in_space, room_type, metadata)
		VALUES ('', 'space:preexisting', '', 'Pre-existing portal', '', '', '', '', false, false, false, false, 'default', '{}')
	`)
	if err != nil {
		t.Fatalf("seeding pre-existing portal row: %v", err)
	}
}

func TestRun_NilDeps(t *testing.T) {
	_, err := Run(context.Background(), nil, Options{})
	if err == nil {
		t.Fatal("expected an error for nil deps, got nil")
	}
}

func TestRun_MissingSource(t *testing.T) {
	deps := &Deps{
		Target:          newFixtureTargetDB(t),
		FormatGhostMXID: testFormatGhostMXID,
	}
	_, err := Run(context.Background(), deps, Options{})
	if err == nil {
		t.Fatal("expected an error for missing Deps.Source, got nil")
	}
}

func TestRun_MissingTarget(t *testing.T) {
	deps := &Deps{
		Source:          newFixtureSourceDB(t),
		FormatGhostMXID: testFormatGhostMXID,
	}
	_, err := Run(context.Background(), deps, Options{})
	if err == nil {
		t.Fatal("expected an error for missing Deps.Target, got nil")
	}
}

func TestRun_MissingFormatGhostMXID(t *testing.T) {
	deps := &Deps{
		Source: newFixtureSourceDB(t),
		Target: newFixtureTargetDB(t),
	}
	_, err := Run(context.Background(), deps, Options{})
	if err == nil {
		t.Fatal("expected an error for missing Deps.FormatGhostMXID, got nil")
	}
}

// TestRun_DryRun_LeavesTargetUnaffected is the "dry-run-writes-nothing"
// test the task brief asks for. Breakable in two independent, real ways
// (each verified RED->GREEN while writing this test, then reverted -- see
// m7-task-4-report.md):
//   - hardcoding Summary{DryRun: false} in Run makes the summary.DryRun
//     assertion below fail;
//   - removing the errors.Is(txErr, errDryRun) unwrap in Run's final return
//     makes the err == nil assertion below fail (Run would incorrectly
//     surface the internal errDryRun sentinel to the caller).
func TestRun_DryRun_LeavesTargetUnaffected(t *testing.T) {
	deps := validDeps(t)
	ctx := context.Background()

	summary, err := Run(ctx, deps, Options{SourceDSN: "test-dsn", DryRun: true})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if summary == nil {
		t.Fatal("expected a non-nil summary even for a dry run")
	}
	if !summary.DryRun {
		t.Errorf("summary.DryRun = false, want true")
	}
	assertTargetEmpty(t, deps.Target)
}

// TestRun_NonEmptyTargetGuard is the "guard" test the task brief asks for.
// Breakable by disabling the `if !opts.Force` check in Run (verified
// RED->GREEN while writing this test, then reverted -- see
// m7-task-4-report.md): with the guard disabled, the first Run call below
// (Force not set) would incorrectly return a nil error instead of
// ErrTargetNotEmpty.
func TestRun_NonEmptyTargetGuard(t *testing.T) {
	deps := validDeps(t)
	ctx := context.Background()
	seedOnePortalRow(t, deps.Target)

	if _, err := Run(ctx, deps, Options{}); !errors.Is(err, ErrTargetNotEmpty) {
		t.Fatalf("Run without Force against a non-empty target: got err=%v, want ErrTargetNotEmpty", err)
	}

	summary, err := Run(ctx, deps, Options{Force: true})
	if err != nil {
		t.Fatalf("Run with Force against a non-empty target: %v", err)
	}
	if summary == nil {
		t.Fatal("expected a non-nil summary")
	}
}

func TestUppercaseCookieKeys(t *testing.T) {
	t.Run("nil map returns nil", func(t *testing.T) {
		if got := UppercaseCookieKeys(nil); got != nil {
			t.Errorf("UppercaseCookieKeys(nil) = %v, want nil", got)
		}
	})

	t.Run("uppercases all keys, preserves values and unknown keys, does not mutate input", func(t *testing.T) {
		in := map[string]string{
			"compass": "c1",
			"ssid":    "s1",
			"sid":     "si1",
			"osid":    "o1",
			"hsid":    "h1",
			"extra":   "e1",
		}
		got := UppercaseCookieKeys(in)
		want := map[string]string{
			"COMPASS": "c1",
			"SSID":    "s1",
			"SID":     "si1",
			"OSID":    "o1",
			"HSID":    "h1",
			"EXTRA":   "e1",
		}
		if len(got) != len(want) {
			t.Fatalf("UppercaseCookieKeys(%v) = %v (len %d), want len %d", in, got, len(got), len(want))
		}
		for k, v := range want {
			if got[k] != v {
				t.Errorf("got[%q] = %q, want %q", k, got[k], v)
			}
		}
		if _, mutated := in["COMPASS"]; mutated {
			t.Error("UppercaseCookieKeys mutated its input map in place")
		}
	})
}

func TestHasRequiredCookieKeys(t *testing.T) {
	complete := map[string]string{"COMPASS": "a", "SSID": "b", "SID": "c", "OSID": "d", "HSID": "e"}
	if !HasRequiredCookieKeys(complete) {
		t.Error("expected a complete cookie map to satisfy HasRequiredCookieKeys")
	}
	if HasRequiredCookieKeys(nil) {
		t.Error("expected HasRequiredCookieKeys(nil) = false")
	}

	for missing := range complete {
		partial := make(map[string]string, len(complete)-1)
		for k, v := range complete {
			if k != missing {
				partial[k] = v
			}
		}
		if HasRequiredCookieKeys(partial) {
			t.Errorf("expected HasRequiredCookieKeys to be false with %q missing, got true", missing)
		}
	}

	// A key that is PRESENT but has an empty value must be treated the same
	// as missing -- this mirrors pkg/connector/client.go's own
	// hasRequiredCookies (`cookies[key] == ""`), which the migrated
	// UserLogin must satisfy once a live bridge tries to Connect() with it.
	for emptyKey := range complete {
		withEmptyValue := make(map[string]string, len(complete))
		for k, v := range complete {
			if k == emptyKey {
				withEmptyValue[k] = ""
			} else {
				withEmptyValue[k] = v
			}
		}
		if HasRequiredCookieKeys(withEmptyValue) {
			t.Errorf("expected HasRequiredCookieKeys to be false with %q present but empty, got true", emptyKey)
		}
	}
}

func TestTimestampsAreMicroseconds(t *testing.T) {
	cases := []struct {
		name         string
		version      int
		versionKnown bool
		want         bool
	}{
		{"unknown version assumes microseconds", 0, false, true},
		{"pre-v10 known version is milliseconds", 9, true, false},
		{"v10 known version is microseconds", 10, true, true},
		{"post-v10 known version is microseconds", 15, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := TimestampsAreMicroseconds(tc.version, tc.versionKnown); got != tc.want {
				t.Errorf("TimestampsAreMicroseconds(%d, %v) = %v, want %v", tc.version, tc.versionKnown, got, tc.want)
			}
		})
	}
}

func TestNormalizeTimestampMicros(t *testing.T) {
	cases := []struct {
		name         string
		ts           int64
		version      int
		versionKnown bool
		want         int64
	}{
		{"pre-v10 multiplies by 1000", 1700000000, 5, true, 1700000000000},
		{"v10 leaves unchanged", 1700000000, 10, true, 1700000000},
		{"post-v10 leaves unchanged", 1700000000, 15, true, 1700000000},
		{"unknown version leaves unchanged (assumes already microseconds)", 1700000000, 0, false, 1700000000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := NormalizeTimestampMicros(tc.ts, tc.version, tc.versionKnown); got != tc.want {
				t.Errorf("NormalizeTimestampMicros(%d, %d, %v) = %d, want %d", tc.ts, tc.version, tc.versionKnown, got, tc.want)
			}
		})
	}
}
