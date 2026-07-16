package migrate

// White-box tests (package migrate) for migrateGhosts -- M7 Task 5. See
// .superpowers/sdd/m7-migration-schema-map.md §4 for the mapping under test.
//
// TestMigrateGhosts_AliceAndBare reuses source_test.go's shared fixture (one
// puppet with a live double-puppet configured and a full profile, one
// all-NULL/never-registered puppet) -- exactly the NULL-coalescing cases
// this task's brief asks for. A dedicated second fixture
// (newBadAvatarHashSourceDB) covers the invalid-avatar_hash warn-and-coalesce
// path, which the shared fixture has no row for (its one populated hash is a
// valid 64-hex-char string).

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"

	"go.mau.fi/util/dbutil"

	"github.com/Deniel9204/mautrix-googlechat/pkg/connector"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

type migratedGhostRow struct {
	Name           string
	AvatarID       string
	AvatarHash     string
	AvatarMXC      string
	NameSet        bool
	AvatarSet      bool
	ContactInfoSet bool
	IsBot          bool
	Identifiers    string
	Metadata       string
}

func readMigratedGhost(t *testing.T, target *dbutil.Database, id string) migratedGhostRow {
	t.Helper()
	var row migratedGhostRow
	err := target.QueryRow(context.Background(), `
		SELECT name, avatar_id, avatar_hash, avatar_mxc,
		       name_set, avatar_set, contact_info_set, is_bot, identifiers, metadata
		FROM ghost WHERE id = ?
	`, id).Scan(
		&row.Name, &row.AvatarID, &row.AvatarHash, &row.AvatarMXC,
		&row.NameSet, &row.AvatarSet, &row.ContactInfoSet, &row.IsBot, &row.Identifiers, &row.Metadata,
	)
	if err != nil {
		t.Fatalf("reading back migrated ghost %q: %v", id, err)
	}
	return row
}

func TestMigrateGhosts_AliceAndBare(t *testing.T) {
	deps := &Deps{
		Source: newFixtureSourceDB(t),
		Target: newFixtureTargetDB(t),
	}
	ctx := context.Background()

	count, warnings, err := migrateGhosts(ctx, deps, Options{})
	if err != nil {
		t.Fatalf("migrateGhosts: %v", err)
	}
	if len(warnings) != 0 {
		t.Errorf("expected no warnings, got %v", warnings)
	}
	if count != 2 {
		t.Fatalf("expected 2 ghosts migrated, got %d", count)
	}

	t.Run("alice (fully populated)", func(t *testing.T) {
		row := readMigratedGhost(t, deps.Target, string(gcid.MakeUserID("103432744896036786916")))

		if row.Name != "Alice" {
			t.Errorf("name = %q, want \"Alice\"", row.Name)
		}
		if row.AvatarID != "https://example.com/alice.jpg" {
			t.Errorf("avatar_id = %q", row.AvatarID)
		}
		// THE critical assertion: photo_hash -> avatar_hash, hex copy.
		wantHash := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
		if row.AvatarHash != wantHash {
			t.Errorf("avatar_hash = %q, want %q", row.AvatarHash, wantHash)
		}
		if row.AvatarMXC != "mxc://example.com/alice" {
			t.Errorf("avatar_mxc = %q", row.AvatarMXC)
		}
		if !row.NameSet || !row.AvatarSet || !row.ContactInfoSet {
			t.Errorf("expected name_set/avatar_set/contact_info_set all true, got %+v", row)
		}
		if row.IsBot {
			t.Errorf("is_bot = true, want false (no Python source, computed live on next sync)")
		}

		var meta connector.GhostMetadata
		if err := json.Unmarshal([]byte(row.Metadata), &meta); err != nil {
			t.Fatalf("unmarshaling metadata %q: %v", row.Metadata, err)
		}
		if meta.Email != "" {
			t.Errorf("meta.Email = %q, want \"\" (no Python source)", meta.Email)
		}
	})

	t.Run("bare (never registered, all-NULL optional fields)", func(t *testing.T) {
		row := readMigratedGhost(t, deps.Target, string(gcid.MakeUserID("999999999999999999")))

		// NULL Python name/photo_id/photo_mxc/photo_hash must coalesce to "".
		if row.Name != "" {
			t.Errorf("name = %q, want \"\" (coalesced from NULL)", row.Name)
		}
		if row.AvatarID != "" {
			t.Errorf("avatar_id = %q, want \"\" (coalesced from NULL photo_id)", row.AvatarID)
		}
		if row.AvatarHash != "" {
			t.Errorf("avatar_hash = %q, want \"\" (coalesced from NULL photo_hash, no warning expected)", row.AvatarHash)
		}
		if row.AvatarMXC != "" {
			t.Errorf("avatar_mxc = %q, want \"\" (coalesced from NULL photo_mxc)", row.AvatarMXC)
		}
		if row.NameSet || row.AvatarSet || row.ContactInfoSet {
			t.Errorf("expected name_set/avatar_set/contact_info_set all false, got %+v", row)
		}
	})
}

// newBadAvatarHashSourceDB builds a source DB whose only puppet row has a
// photo_hash that is NOT 64 hex characters -- exercising the
// warn-and-coalesce path (map §4: "validate it's exactly 64 hex chars ...
// before writing").
func newBadAvatarHashSourceDB(t *testing.T) *dbutil.Database {
	t.Helper()
	path := filepath.Join(t.TempDir(), "bad_avatar_hash_source.db")
	setup, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("opening fixture source for setup: %v", err)
	}
	if _, err := setup.Exec(fixtureSchemaSQL); err != nil {
		t.Fatalf("creating fixture schema: %v", err)
	}
	_, err = setup.Exec(
		`INSERT INTO puppet (gcid, name, photo_id, photo_mxc, photo_hash, name_set, avatar_set, contact_info_set, is_registered, custom_mxid, access_token, next_batch, base_url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"111111111111111111", "Bob", "https://example.com/bob.jpg", "mxc://example.com/bob", "deadbeef", false, true, false, false, nil, nil, nil, nil,
	)
	if err != nil {
		t.Fatalf("seeding bad-avatar-hash puppet row: %v", err)
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

func TestMigrateGhosts_InvalidAvatarHash_WarnsAndCoalesces(t *testing.T) {
	deps := &Deps{
		Source: newBadAvatarHashSourceDB(t),
		Target: newFixtureTargetDB(t),
	}
	ctx := context.Background()

	count, warnings, err := migrateGhosts(ctx, deps, Options{})
	if err != nil {
		t.Fatalf("migrateGhosts: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 ghost migrated (row is kept, just the bad field dropped), got %d", count)
	}
	if len(warnings) != 1 {
		t.Fatalf("expected exactly 1 warning, got %d: %v", len(warnings), warnings)
	}

	row := readMigratedGhost(t, deps.Target, string(gcid.MakeUserID("111111111111111111")))
	if row.AvatarHash != "" {
		t.Errorf("avatar_hash = %q, want \"\" (invalid length, coalesced away)", row.AvatarHash)
	}
	// The rest of the row must still be written -- only the bad field is dropped.
	if row.Name != "Bob" {
		t.Errorf("name = %q, want \"Bob\" (rest of row still migrated)", row.Name)
	}
}
