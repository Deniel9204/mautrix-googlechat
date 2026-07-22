package migrate

// White-box tests (package migrate) for migratePortals.
//
// TestMigratePortals_DMAndSpace reuses source_test.go's shared fixture (one
// DM portal, mostly-NULL optional fields; one space portal, fully
// populated) since it already exercises exactly the cases under test:
// room_type-never-space and description->topic. A dedicated
// second fixture (newInvalidPortalSourceDB) covers the invalid-id
// warn-and-skip path, which the shared fixture has no row for.

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"

	"go.mau.fi/util/dbutil"

	"maunium.net/go/mautrix/bridgev2/database"

	"github.com/Deniel9204/mautrix-googlechat/pkg/connector"
)

// migratedPortalRow is what the test reads back from the target `portal`
// table after migratePortals runs -- one field per column this task writes.
type migratedPortalRow struct {
	MXID       sql.NullString
	Name       string
	Topic      string
	AvatarID   string
	AvatarHash string
	AvatarMXC  string
	NameSet    bool
	AvatarSet  bool
	TopicSet   bool
	InSpace    bool
	RoomType   string
	Metadata   string
}

func readMigratedPortal(t *testing.T, target *dbutil.Database, id, receiver string) migratedPortalRow {
	t.Helper()
	var row migratedPortalRow
	err := target.QueryRow(context.Background(), `
		SELECT mxid, name, topic, avatar_id, avatar_hash, avatar_mxc,
		       name_set, avatar_set, topic_set, in_space, room_type, metadata
		FROM portal WHERE id = ? AND receiver = ?
	`, id, receiver).Scan(
		&row.MXID, &row.Name, &row.Topic, &row.AvatarID, &row.AvatarHash, &row.AvatarMXC,
		&row.NameSet, &row.AvatarSet, &row.TopicSet, &row.InSpace, &row.RoomType, &row.Metadata,
	)
	if err != nil {
		t.Fatalf("reading back migrated portal (%q, %q): %v", id, receiver, err)
	}
	return row
}

func TestMigratePortals_DMAndSpace(t *testing.T) {
	deps := &Deps{
		Source: newFixtureSourceDB(t),
		Target: newFixtureTargetDB(t),
	}
	ctx := context.Background()

	count, warnings, err := migratePortals(ctx, deps, Options{})
	if err != nil {
		t.Fatalf("migratePortals: %v", err)
	}
	if len(warnings) != 0 {
		t.Errorf("expected no warnings, got %v", warnings)
	}
	if count != 2 {
		t.Fatalf("expected 2 portals migrated, got %d", count)
	}

	t.Run("DM portal", func(t *testing.T) {
		row := readMigratedPortal(t, deps.Target, "dm:gGboEmVbEyE", "103432744896036786916")

		if !row.MXID.Valid || row.MXID.String != "!dmroom:example.com" {
			t.Errorf("mxid = %+v, want valid \"!dmroom:example.com\"", row.MXID)
		}
		// NULL Python name/description/avatar_mxc must coalesce to "".
		if row.Name != "" {
			t.Errorf("name = %q, want \"\" (coalesced from NULL)", row.Name)
		}
		if row.Topic != "" {
			t.Errorf("topic = %q, want \"\" (coalesced from NULL description)", row.Topic)
		}
		if row.AvatarMXC != "" {
			t.Errorf("avatar_mxc = %q, want \"\" (coalesced from NULL)", row.AvatarMXC)
		}
		// No Python source for these -- must default, not error.
		if row.AvatarID != "" {
			t.Errorf("avatar_id = %q, want \"\" (no Python source)", row.AvatarID)
		}
		if row.AvatarHash != "" {
			t.Errorf("avatar_hash = %q, want \"\" (no Python source)", row.AvatarHash)
		}
		if row.NameSet || row.AvatarSet || row.TopicSet {
			t.Errorf("expected name_set/avatar_set/topic_set all false (identity copy of false), got %+v", row)
		}
		if row.InSpace {
			t.Errorf("in_space = true, want false (no Python source)")
		}
		// THE critical assertion: a dm: portal must be RoomTypeDM, never RoomTypeSpace.
		if row.RoomType != string(database.RoomTypeDM) {
			t.Errorf("room_type = %q, want %q (RoomTypeDM)", row.RoomType, database.RoomTypeDM)
		}

		var meta connector.PortalMetadata
		if err := json.Unmarshal([]byte(row.Metadata), &meta); err != nil {
			t.Fatalf("unmarshaling metadata %q: %v", row.Metadata, err)
		}
		if meta.Revision != 0 || meta.ThreadsOnly || meta.ThreadsEnabled {
			t.Errorf("metadata = %+v, want all-zero (Python NULL revision/threads_only/threads_enabled)", meta)
		}
	})

	t.Run("space portal", func(t *testing.T) {
		row := readMigratedPortal(t, deps.Target, "space:AAAAdaMWXXc", "")

		if !row.MXID.Valid || row.MXID.String != "!spaceroom:example.com" {
			t.Errorf("mxid = %+v, want valid \"!spaceroom:example.com\"", row.MXID)
		}
		if row.Name != "Team Chat" {
			t.Errorf("name = %q, want \"Team Chat\"", row.Name)
		}
		// THE critical assertion: description -> topic.
		if row.Topic != "A space for team chat" {
			t.Errorf("topic = %q, want \"A space for team chat\" (from Python description)", row.Topic)
		}
		if row.AvatarMXC != "mxc://example.com/abc" {
			t.Errorf("avatar_mxc = %q", row.AvatarMXC)
		}
		if !row.NameSet || !row.AvatarSet || !row.TopicSet {
			t.Errorf("expected name_set/avatar_set/topic_set all true, got %+v", row)
		}
		// THE critical assertion: a space: portal must be RoomTypeDefault, NEVER RoomTypeSpace.
		if row.RoomType != string(database.RoomTypeDefault) {
			t.Errorf("room_type = %q, want %q (RoomTypeDefault, i.e. empty string) -- NEVER %q", row.RoomType, database.RoomTypeDefault, database.RoomTypeSpace)
		}
		if row.RoomType == string(database.RoomTypeSpace) {
			t.Fatalf("room_type = %q -- a Google Chat Space must NEVER become a Matrix RoomTypeSpace", row.RoomType)
		}

		var meta connector.PortalMetadata
		if err := json.Unmarshal([]byte(row.Metadata), &meta); err != nil {
			t.Fatalf("unmarshaling metadata %q: %v", row.Metadata, err)
		}
		if meta.Revision != 5 {
			t.Errorf("meta.Revision = %d, want 5", meta.Revision)
		}
		if !meta.ThreadsOnly {
			t.Errorf("meta.ThreadsOnly = false, want true")
		}
		if !meta.ThreadsEnabled {
			t.Errorf("meta.ThreadsEnabled = false, want true")
		}
	})
}

// newInvalidPortalSourceDB builds a source DB whose only portal row has a
// gcid with neither the dm: nor space: prefix (a corrupt/foreign row) --
// exercising the warn-and-skip path gcid.ParsePortalID's error return
// triggers.
func newInvalidPortalSourceDB(t *testing.T) *dbutil.Database {
	t.Helper()
	path := filepath.Join(t.TempDir(), "invalid_portal_source.db")
	setup, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("opening fixture source for setup: %v", err)
	}
	if _, err := setup.Exec(fixtureSchemaSQL); err != nil {
		t.Fatalf("creating fixture schema: %v", err)
	}
	_, err = setup.Exec(
		`INSERT INTO portal (gcid, gc_receiver, other_user_id, mxid, name, avatar_mxc, description, name_set, avatar_set, description_set, encrypted, revision, threads_only, threads_enabled) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"not-a-valid-gcid", "", nil, "!room:example.com", nil, nil, nil, false, false, false, false, nil, nil, nil,
	)
	if err != nil {
		t.Fatalf("seeding invalid portal row: %v", err)
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

func TestMigratePortals_InvalidPortalID_WarnsAndSkips(t *testing.T) {
	deps := &Deps{
		Source: newInvalidPortalSourceDB(t),
		Target: newFixtureTargetDB(t),
	}
	ctx := context.Background()

	count, warnings, err := migratePortals(ctx, deps, Options{})
	if err != nil {
		t.Fatalf("migratePortals: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 portals migrated (the only row is invalid), got %d", count)
	}
	if len(warnings) != 1 {
		t.Fatalf("expected exactly 1 warning, got %d: %v", len(warnings), warnings)
	}

	var remaining int
	if err := deps.Target.QueryRow(ctx, `SELECT COUNT(*) FROM portal`).Scan(&remaining); err != nil {
		t.Fatalf("counting target portal rows: %v", err)
	}
	if remaining != 0 {
		t.Errorf("expected target portal table to remain empty, got %d row(s)", remaining)
	}
}
