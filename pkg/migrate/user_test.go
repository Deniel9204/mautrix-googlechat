package migrate

// White-box tests (package migrate) for migrateUsers. Under test: cookie
// key casing and the double-puppet token -> user.access_token copy.
//
// TestMigrateUsers_CookiesAndNoCookies reuses source_test.go's shared
// fixture (@user1:example.com has a gcid+cookies+user_agent+revision;
// @user2:example.com has none) -- exactly the "logged in" vs "never logged
// in" split. That fixture's one puppet
// (custom_mxid=@alice:example.com) does NOT match either migrated user, so
// running migrateUsers against it also exercises the "double-puppet token
// matches no migrated user" warn-and-skip path for free.
//
// newUserEdgeCasesSourceDB is a dedicated second fixture (like ghost_test.go's
// newBadAvatarHashSourceDB) covering cases the shared fixture has no rows
// for: gcid-present-but-cookies-NULL (the v08 cookie-wipe shape), an
// incomplete cookie map that still gets migrated with a warning, and a
// double-puppet token that DOES match a migrated user.

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"go.mau.fi/util/dbutil"

	"github.com/Deniel9204/mautrix-googlechat/pkg/connector"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

type migratedUserRow struct {
	ManagementRoom sql.NullString
	AccessToken    sql.NullString
}

func readMigratedUser(t *testing.T, target *dbutil.Database, mxid string) migratedUserRow {
	t.Helper()
	var row migratedUserRow
	err := target.QueryRow(context.Background(), `
		SELECT management_room, access_token FROM "user" WHERE mxid = ?
	`, mxid).Scan(&row.ManagementRoom, &row.AccessToken)
	if err != nil {
		t.Fatalf("reading back migrated user %q: %v", mxid, err)
	}
	return row
}

func userExists(t *testing.T, target *dbutil.Database, mxid string) bool {
	t.Helper()
	var count int
	err := target.QueryRow(context.Background(), `SELECT COUNT(*) FROM "user" WHERE mxid = ?`, mxid).Scan(&count)
	if err != nil {
		t.Fatalf("counting user %q: %v", mxid, err)
	}
	return count > 0
}

type migratedUserLoginRow struct {
	Metadata string
}

func readMigratedUserLogin(t *testing.T, target *dbutil.Database, id string) migratedUserLoginRow {
	t.Helper()
	var row migratedUserLoginRow
	err := target.QueryRow(context.Background(), `
		SELECT metadata FROM user_login WHERE id = ?
	`, id).Scan(&row.Metadata)
	if err != nil {
		t.Fatalf("reading back migrated user_login %q: %v", id, err)
	}
	return row
}

func userLoginRowExists(t *testing.T, target *dbutil.Database, id string) bool {
	t.Helper()
	var count int
	err := target.QueryRow(context.Background(), `SELECT COUNT(*) FROM user_login WHERE id = ?`, id).Scan(&count)
	if err != nil {
		t.Fatalf("counting user_login %q: %v", id, err)
	}
	return count > 0
}

func TestMigrateUsers_CookiesAndNoCookies(t *testing.T) {
	deps := &Deps{
		Source: newFixtureSourceDB(t),
		Target: newFixtureTargetDB(t),
	}
	ctx := context.Background()

	count, warnings, err := migrateUsers(ctx, deps, Options{})
	if err != nil {
		t.Fatalf("migrateUsers: %v", err)
	}
	// 2 user rows (@user1, @user2); user_login rows are migrateUserLogins's
	// own count now (M7 Task 8's Summary split).
	if count != 2 {
		t.Fatalf("expected count=2 (2 user rows), got %d", count)
	}
	// The fixture's one puppet (custom_mxid=@alice:example.com) matches no
	// migrated user -- exactly 1 warning, about that mismatch.
	if len(warnings) != 1 {
		t.Fatalf("expected exactly 1 warning (unmatched double-puppet), got %d: %v", len(warnings), warnings)
	}
	if !strings.Contains(warnings[0], "@alice:example.com") {
		t.Errorf("warning = %q, want it to mention @alice:example.com", warnings[0])
	}

	loginCount, loginWarnings, err := migrateUserLogins(ctx, deps, Options{})
	if err != nil {
		t.Fatalf("migrateUserLogins: %v", err)
	}
	// 1 user_login row (@user1 only -- @user2 has no gcid/cookies).
	if loginCount != 1 {
		t.Fatalf("expected loginCount=1, got %d", loginCount)
	}
	if len(loginWarnings) != 0 {
		t.Errorf("expected no login warnings, got %v", loginWarnings)
	}

	t.Run("user1 (cookies present) gets a user row and a user_login row", func(t *testing.T) {
		if !userExists(t, deps.Target, "@user1:example.com") {
			t.Fatal("expected a user row for @user1:example.com")
		}
		row := readMigratedUser(t, deps.Target, "@user1:example.com")
		if !row.ManagementRoom.Valid || row.ManagementRoom.String != "!notice:example.com" {
			t.Errorf("management_room = %+v, want valid \"!notice:example.com\"", row.ManagementRoom)
		}
		if row.AccessToken.Valid {
			t.Errorf("access_token = %+v, want NULL (no matching double-puppet)", row.AccessToken)
		}

		loginID := string(gcid.MakeUserLoginID("103432744896036786916"))
		if !userLoginRowExists(t, deps.Target, loginID) {
			t.Fatalf("expected a user_login row with id=%q", loginID)
		}
		login := readMigratedUserLogin(t, deps.Target, loginID)

		var meta connector.UserLoginMetadata
		if err := json.Unmarshal([]byte(login.Metadata), &meta); err != nil {
			t.Fatalf("unmarshaling metadata %q: %v", login.Metadata, err)
		}

		// THE critical assertion: cookie keys must be UPPERCASE, not the
		// lowercase form the Python JSON blob stores (preflight item 1).
		want := map[string]string{"COMPASS": "c1", "SSID": "s1", "SID": "si1", "OSID": "o1", "HSID": "h1"}
		if len(meta.Cookies) != len(want) {
			t.Fatalf("Cookies = %v, want %v", meta.Cookies, want)
		}
		for k, v := range want {
			if meta.Cookies[k] != v {
				t.Errorf("Cookies[%q] = %q, want %q", k, meta.Cookies[k], v)
			}
		}
		if lower, ok := meta.Cookies["compass"]; ok {
			t.Errorf("Cookies contains lowercase key \"compass\"=%q -- must be uppercased", lower)
		}

		if meta.UserAgent != "Mozilla/5.0 (test)" {
			t.Errorf("UserAgent = %q, want \"Mozilla/5.0 (test)\"", meta.UserAgent)
		}
		if meta.Revision != 42 {
			t.Errorf("Revision = %d, want 42", meta.Revision)
		}
	})

	t.Run("user2 (no cookies, no gcid) gets a user row and NO user_login row", func(t *testing.T) {
		if !userExists(t, deps.Target, "@user2:example.com") {
			t.Fatal("expected a user row for @user2:example.com")
		}
		row := readMigratedUser(t, deps.Target, "@user2:example.com")
		if row.ManagementRoom.Valid {
			t.Errorf("management_room = %+v, want NULL (Python notice_room was NULL)", row.ManagementRoom)
		}
		if row.AccessToken.Valid {
			t.Errorf("access_token = %+v, want NULL", row.AccessToken)
		}

		var loginCount int
		if err := deps.Target.QueryRow(ctx, `SELECT COUNT(*) FROM user_login WHERE user_mxid = ?`, "@user2:example.com").Scan(&loginCount); err != nil {
			t.Fatalf("counting user_login rows for @user2: %v", err)
		}
		if loginCount != 0 {
			t.Errorf("expected 0 user_login rows for @user2:example.com, got %d", loginCount)
		}
	})
}

// newUserEdgeCasesSourceDB builds a dedicated fixture covering cases the
// shared fixture doesn't:
//   - @real:example.com: gcid present, cookies NULL (the v08 cookie-wipe
//     shape) -- user row only, no login, even though gcid is valid.
//   - @incomplete:example.com: gcid present, cookies present but only
//     "compass" -- migrated anyway (with a warning), since the brief says
//     "still write it but append a warning".
//   - puppet 1: custom_mxid=@real:example.com, access_token="matched-token"
//     -- DOES match a migrated user (double-puppet applied).
//   - puppet 2: custom_mxid=@nomatch:example.com, access_token="orphan-token"
//     -- matches no migrated user (warn, no crash).
func newUserEdgeCasesSourceDB(t *testing.T) *dbutil.Database {
	t.Helper()
	path := filepath.Join(t.TempDir(), "user_edge_cases_source.db")
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
			[]any{"@real:example.com", "333333333333333333", nil, nil, nil, nil},
		},
		{
			`INSERT INTO "user" (mxid, gcid, cookies, user_agent, notice_room, revision) VALUES (?, ?, ?, ?, ?, ?)`,
			[]any{"@incomplete:example.com", "444444444444444444", `{"compass":"c-only"}`, "Mozilla/5.0", nil, int64(1)},
		},
		{
			`INSERT INTO puppet (gcid, name, photo_id, photo_mxc, photo_hash, name_set, avatar_set, contact_info_set, is_registered, custom_mxid, access_token, next_batch, base_url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			[]any{"777777777777777777", nil, nil, nil, nil, false, false, false, false, "@real:example.com", "matched-token", nil, nil},
		},
		{
			`INSERT INTO puppet (gcid, name, photo_id, photo_mxc, photo_hash, name_set, avatar_set, contact_info_set, is_registered, custom_mxid, access_token, next_batch, base_url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			[]any{"888888888888888888", nil, nil, nil, nil, false, false, false, false, "@nomatch:example.com", "orphan-token", nil, nil},
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

func TestMigrateUsers_EdgeCases(t *testing.T) {
	deps := &Deps{
		Source: newUserEdgeCasesSourceDB(t),
		Target: newFixtureTargetDB(t),
	}
	ctx := context.Background()

	count, warnings, err := migrateUsers(ctx, deps, Options{})
	if err != nil {
		t.Fatalf("migrateUsers: %v", err)
	}
	// 2 user rows (@real, @incomplete).
	if count != 2 {
		t.Fatalf("expected count=2 (2 user rows), got %d", count)
	}
	// Exactly 1 warning from migrateUsers: the unmatched-double-puppet
	// warning for @nomatch:example.com (the incomplete-cookie-map warning is
	// migrateUserLogins's now).
	if len(warnings) != 1 {
		t.Fatalf("expected exactly 1 warning, got %d: %v", len(warnings), warnings)
	}

	loginCount, loginWarnings, err := migrateUserLogins(ctx, deps, Options{})
	if err != nil {
		t.Fatalf("migrateUserLogins: %v", err)
	}
	// 1 user_login row (@incomplete only -- @real has no cookies at all).
	if loginCount != 1 {
		t.Fatalf("expected loginCount=1, got %d", loginCount)
	}
	if len(loginWarnings) != 1 {
		t.Fatalf("expected exactly 1 login warning (incomplete cookie map), got %d: %v", len(loginWarnings), loginWarnings)
	}

	t.Run("gcid present but cookies NULL: user row only, no login", func(t *testing.T) {
		if !userExists(t, deps.Target, "@real:example.com") {
			t.Fatal("expected a user row for @real:example.com")
		}
		loginID := string(gcid.MakeUserLoginID("333333333333333333"))
		if userLoginRowExists(t, deps.Target, loginID) {
			t.Errorf("expected NO user_login row for @real:example.com (cookies were NULL), but one exists")
		}
	})

	t.Run("incomplete cookie map: still migrated, with a warning", func(t *testing.T) {
		loginID := string(gcid.MakeUserLoginID("444444444444444444"))
		if !userLoginRowExists(t, deps.Target, loginID) {
			t.Fatal("expected a user_login row for @incomplete:example.com despite the incomplete cookie map")
		}
		login := readMigratedUserLogin(t, deps.Target, loginID)
		var meta connector.UserLoginMetadata
		if err := json.Unmarshal([]byte(login.Metadata), &meta); err != nil {
			t.Fatalf("unmarshaling metadata: %v", err)
		}
		if meta.Cookies["COMPASS"] != "c-only" {
			t.Errorf("Cookies[COMPASS] = %q, want \"c-only\"", meta.Cookies["COMPASS"])
		}
		if _, ok := meta.Cookies["SSID"]; ok {
			t.Errorf("expected no SSID key in an incomplete cookie map, got %v", meta.Cookies)
		}

		found := false
		for _, w := range loginWarnings {
			if strings.Contains(w, "@incomplete:example.com") && strings.Contains(w, "re-login") {
				found = true
			}
		}
		if !found {
			t.Errorf("expected a warning about @incomplete:example.com's incomplete cookie map, got %v", loginWarnings)
		}
	})

	t.Run("double-puppet: matched token applied, unmatched token warns without crashing", func(t *testing.T) {
		row := readMigratedUser(t, deps.Target, "@real:example.com")
		if !row.AccessToken.Valid || row.AccessToken.String != "matched-token" {
			t.Errorf("access_token = %+v, want valid \"matched-token\"", row.AccessToken)
		}

		found := false
		for _, w := range warnings {
			if strings.Contains(w, "@nomatch:example.com") {
				found = true
			}
		}
		if !found {
			t.Errorf("expected a warning about the unmatched @nomatch:example.com double-puppet token, got %v", warnings)
		}
	})
}
