package migrate

// e2e_test.go is the end-to-end migration test: ONE representative
// fixture Python SQLite database -- a cross-section of a real installation,
// not a narrow per-entity case -- migrated via the FULL Run (against the
// REAL bridgev2 target schema, opened with the FK-enforcing "sqlite3-fk-wal"
// driver, see newFixtureTargetDB in migrate_test.go), asserting every target
// table's rows are correct and that the whole migration committed under FK
// enforcement in one transaction.
//
// This complements, rather than replaces, the white-box per-entity tests in
// portal_test.go/ghost_test.go/message_test.go/reaction_test.go/user_test.go/
// userportal_test.go, which already exercise NULL-coalescing and
// warn-and-skip edge cases per entity in isolation, mostly by calling
// migrateX directly rather than through Run. This file's job is different:
// prove that every migrator, wired together by Run against a single
// consistent fixture, produces a fully FK-consistent target. Two distinct
// failure modes both make this test fail, and both matter: a genuine
// migrator-ordering regression that skips straight past every per-row
// existence guard (portalExistsInTarget/ghostExistsInTarget/
// userLoginExistsInTarget in message.go/userportal.go) would surface as a
// literal SQLite "FOREIGN KEY constraint failed" error from Run's
// transaction (opened against "sqlite3-fk-wal", which enforces FKs, unlike
// the plain "sqlite3" driver the read-only source uses); a regression that
// instead trips one of those guards first would surface as a wrong
// (warned-and-skipped) row count against the assertions below instead of a
// raw FK error. Either way, Run returning a non-nil error, or the Summary
// counts/table contents not matching this file's expectations, is what
// every assertion below depends on NOT happening -- confirmed by
// deliberately breaking the migrateUsers/migrateUserLogins step order while
// writing this test and observing both failure modes, then reverting.
//
// Fixture shape:
//   - one user WITH cookies+user_agent+revision (e2eUser1MXID), one WITHOUT
//     (e2eUser2MXID, never logged in -- the "skip" case);
//   - one puppet giving e2eUser1MXID double-puppet (custom_mxid+access_token),
//     plus two more puppets/ghosts (the DM peer and a space member) that are
//     message senders;
//   - a DM portal (owned by e2eUser1MXID) and a space portal
//     (threads_enabled=true);
//   - a text-only message, a text+2-attachment multi-part message, and a
//     multi-row attachment-only message (the index->part_id rule, all three
//     shapes);
//   - a reaction on the text-part message (resolves to part_id "") and a
//     reaction on the attachment-only message (resolves to part_id "att_0",
//     proving the reaction's message_part_id lookup isn't hardcoded).

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"go.mau.fi/util/dbutil"
	"go.mau.fi/util/variationselector"

	"maunium.net/go/mautrix/bridgev2/database"

	"github.com/Deniel9204/mautrix-googlechat/pkg/connector"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

const (
	e2eUser1MXID = "@e2e-user1:example.com"
	e2eUser2MXID = "@e2e-user2:example.com"

	e2eUser1Gaia = "200000000000000000001" // owns the DM portal, has cookies + double-puppet.
	e2eOtherGaia = "200000000000000000002" // DM peer, sender of the DM text message.
	e2eSpaceGaia = "200000000000000000003" // space member, sender of the space messages.

	e2eDMPortalGCID    = "dm:e2eConvDM1"
	e2eSpacePortalGCID = "space:e2eConvSpace1"
	e2eDMRoomMXID      = "!e2edm:example.com"
	e2eSpaceRoomMXID   = "!e2espace:example.com"

	e2eDoublePuppetToken = "dp_token_e2e_abc123"

	// SHA256 of the empty string -- reused here (same value source_test.go's
	// "Alice" fixture and ghost_test.go both use) purely as a valid,
	// 64-hex-char avatar_hash stand-in.
	e2eValidAvatarHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

// seedE2EFixtureRows inserts one representative cross-section of a real
// Python mautrix-googlechat installation -- see this file's package doc
// comment for the full shape.
func seedE2EFixtureRows(t *testing.T, db *sql.DB) {
	t.Helper()
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := db.Exec(query, args...); err != nil {
			t.Fatalf("seeding e2e fixture row %q: %v", query, err)
		}
	}

	// --- users ---------------------------------------------------------
	exec(`INSERT INTO "user" (mxid, gcid, cookies, user_agent, notice_room, revision) VALUES (?, ?, ?, ?, ?, ?)`,
		e2eUser1MXID, e2eUser1Gaia,
		`{"compass":"c1","ssid":"s1","sid":"si1","osid":"o1","hsid":"h1"}`,
		"Mozilla/5.0 (e2e)", "!e2enotice:example.com", int64(7))
	exec(`INSERT INTO "user" (mxid, gcid, cookies, user_agent, notice_room, revision) VALUES (?, ?, ?, ?, ?, ?)`,
		e2eUser2MXID, nil, nil, nil, nil, nil)

	// --- puppets (ghosts) ------------------------------------------------
	const insertPuppetSQ = `INSERT INTO puppet (gcid, name, photo_id, photo_mxc, photo_hash, name_set, avatar_set, contact_info_set, is_registered, custom_mxid, access_token, next_batch, base_url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	// Self puppet: gives e2eUser1MXID double-puppet.
	exec(insertPuppetSQ,
		e2eUser1Gaia, "E2E User One", "https://example.com/e2euser1.jpg", "mxc://example.com/e2euser1", e2eValidAvatarHash,
		true, true, true, true, e2eUser1MXID, e2eDoublePuppetToken, "s1_2", "https://matrix.example.com")
	// DM peer.
	exec(insertPuppetSQ,
		e2eOtherGaia, "E2E Other Person", nil, nil, nil,
		true, false, false, false, nil, nil, nil, nil)
	// Space member.
	exec(insertPuppetSQ,
		e2eSpaceGaia, "E2E Space Member", nil, nil, nil,
		true, false, false, false, nil, nil, nil, nil)

	// --- portals ---------------------------------------------------------
	const insertPortalSQ = `INSERT INTO portal (gcid, gc_receiver, other_user_id, mxid, name, avatar_mxc, description, name_set, avatar_set, description_set, encrypted, revision, threads_only, threads_enabled) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	exec(insertPortalSQ,
		e2eDMPortalGCID, e2eUser1Gaia, e2eOtherGaia, e2eDMRoomMXID, nil, nil, nil,
		false, false, false, true, nil, nil, nil)
	exec(insertPortalSQ,
		e2eSpacePortalGCID, "", nil, e2eSpaceRoomMXID, "E2E Team Space", "mxc://example.com/e2espace", "Our e2e team's space",
		true, true, true, false, int64(9), false, true)

	// --- messages ----------------------------------------------------------
	// DM: single text-only message -> part_id "".
	exec(msgFixtureInsertMessageSQ,
		"$e2edmtext:example.com", e2eDMRoomMXID, "e2eMsgTextOnly", e2eDMPortalGCID, e2eUser1Gaia, nil, e2eOtherGaia,
		0, int64(1720000000000000), "m.text")

	// Space: text + 2 attachments -> "", att_0, att_1.
	exec(msgFixtureInsertMessageSQ,
		"$e2esp1:example.com", e2eSpaceRoomMXID, "e2eMsgTextPlusAtt", e2eSpacePortalGCID, "", nil, e2eSpaceGaia,
		0, int64(1720000001000000), "m.text")
	exec(msgFixtureInsertMessageSQ,
		"$e2esp2:example.com", e2eSpaceRoomMXID, "e2eMsgTextPlusAtt", e2eSpacePortalGCID, "", nil, e2eSpaceGaia,
		1, int64(1720000001100000), "m.image")
	exec(msgFixtureInsertMessageSQ,
		"$e2esp3:example.com", e2eSpaceRoomMXID, "e2eMsgTextPlusAtt", e2eSpacePortalGCID, "", nil, e2eSpaceGaia,
		2, int64(1720000001200000), "m.video")

	// Space: attachment-only, multi-row (no text row at all) -> att_0, att_1.
	exec(msgFixtureInsertMessageSQ,
		"$e2esp4:example.com", e2eSpaceRoomMXID, "e2eMsgAttOnly", e2eSpacePortalGCID, "", nil, e2eSpaceGaia,
		0, int64(1720000002000000), "m.image")
	exec(msgFixtureInsertMessageSQ,
		"$e2esp5:example.com", e2eSpaceRoomMXID, "e2eMsgAttOnly", e2eSpacePortalGCID, "", nil, e2eSpaceGaia,
		1, int64(1720000002100000), "m.file")

	// --- reactions -----------------------------------------------------
	const insertReactionSQ = `INSERT INTO reaction (mxid, mx_room, emoji, gc_sender, gc_msgid, gc_chat, gc_receiver, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	// Reacts to the DM text-only message -> resolves to part_id "" (single-row group).
	exec(insertReactionSQ,
		"$e2er1:example.com", e2eDMRoomMXID, "\U0001F44D", e2eUser1Gaia, "e2eMsgTextOnly", e2eDMPortalGCID, e2eUser1Gaia,
		int64(1720000000500000))
	// Reacts to the attachment-only message -> resolves to part_id "att_0"
	// (index=0 row of a 2-row, no-text group -- NOT hardcoded "").
	exec(insertReactionSQ,
		"$e2er2:example.com", e2eSpaceRoomMXID, "❤", e2eSpaceGaia, "e2eMsgAttOnly", e2eSpacePortalGCID, "",
		int64(1720000002500000))
}

func newE2EFixtureSourceDB(t *testing.T) *dbutil.Database {
	t.Helper()
	path := filepath.Join(t.TempDir(), "e2e_source.db")

	setup, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("opening e2e fixture source for setup: %v", err)
	}
	if _, err := setup.Exec(fixtureSchemaSQL); err != nil {
		t.Fatalf("creating e2e fixture schema: %v", err)
	}
	seedE2EFixtureRows(t, setup)
	if err := setup.Close(); err != nil {
		t.Fatalf("closing e2e fixture setup connection: %v", err)
	}

	db, err := OpenSource(path)
	if err != nil {
		t.Fatalf("OpenSource(%q): %v", path, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// assertE2ESummaryCounts checks every Summary bucket against the fixture's
// known-correct shape -- shared between the real-run and dry-run tests,
// since they must be IDENTICAL (a dry run must report the same counts it
// would have committed).
func assertE2ESummaryCounts(t *testing.T, summary *Summary) {
	t.Helper()
	cases := []struct {
		name string
		ec   EntityCount
		want int
	}{
		{"Portals", summary.Portals, 2},
		{"Ghosts", summary.Ghosts, 3},
		{"Messages", summary.Messages, 6}, // 1 text-only + 3 multi-part + 2 att-only
		{"Reactions", summary.Reactions, 2},
		{"Users", summary.Users, 2},
		{"Logins", summary.Logins, 1},
		{"UserPortals", summary.UserPortals, 1},
	}
	for _, c := range cases {
		if c.ec.Migrated != c.want {
			t.Errorf("summary.%s.Migrated = %d, want %d", c.name, c.ec.Migrated, c.want)
		}
	}
	// Only UserPortals is expected to warn (the space portal's
	// unreconstructable membership gap) -- every other entity in this
	// fixture is clean, well-formed data.
	for _, c := range []struct {
		name string
		ec   EntityCount
	}{
		{"Portals", summary.Portals}, {"Ghosts", summary.Ghosts}, {"Messages", summary.Messages},
		{"Reactions", summary.Reactions}, {"Users", summary.Users}, {"Logins", summary.Logins},
	} {
		if len(c.ec.Warnings) != 0 {
			t.Errorf("summary.%s.Warnings = %v, want none", c.name, c.ec.Warnings)
		}
	}
	if len(summary.UserPortals.Warnings) != 1 {
		t.Fatalf("summary.UserPortals.Warnings = %v, want exactly 1 (space portal gap)", summary.UserPortals.Warnings)
	}
	if got := summary.UserPortals.Warnings[0]; !strings.Contains(got, "space portal") || !strings.Contains(got, "self-heals") {
		t.Errorf("summary.UserPortals.Warnings[0] = %q, want it to document the space-portal self-heal gap", got)
	}

	// Migration-wide Warnings must document the zero-backfill_task decision
	// (userportal.go's backfillTaskSeedingNote), regardless of dry-run.
	foundBackfillNote := false
	for _, w := range summary.Warnings {
		if strings.Contains(w, "backfill_task") && strings.Contains(w, "0 rows seeded by design") {
			foundBackfillNote = true
		}
	}
	if !foundBackfillNote {
		t.Errorf("summary.Warnings = %v, want the backfill_task zero-rows note", summary.Warnings)
	}
}

// TestEndToEndMigration_FullRun runs the COMPLETE Run pipeline (every
// migrator, real bridgev2 target schema, FK-enforcing "sqlite3-fk-wal")
// against the e2e fixture and asserts every populated target table has the
// expected rows -- the "prove the whole thing end-to-end" deliverable.
func TestEndToEndMigration_FullRun(t *testing.T) {
	deps := &Deps{
		Source:          newE2EFixtureSourceDB(t),
		Target:          newFixtureTargetDB(t),
		FormatGhostMXID: testFormatGhostMXID,
	}
	ctx := context.Background()

	summary, err := Run(ctx, deps, Options{SourceDSN: "e2e-test-dsn"})
	// FK consistency: Run opens its transaction against "sqlite3-fk-wal"
	// (FK-enforcing). If ANY insert above violated a foreign key (wrong
	// migrator ordering, a bad identity copy, ...), this Run call would
	// return a non-nil error and the whole transaction would have rolled
	// back -- so a nil error here IS the FK-consistency proof, not just a
	// happy-path check.
	if err != nil {
		t.Fatalf("Run: %v (a non-nil error here means an FK was violated and the whole migration rolled back)", err)
	}
	if summary == nil {
		t.Fatal("expected a non-nil summary")
	}
	if summary.DryRun {
		t.Error("summary.DryRun = true, want false for a real run")
	}
	assertE2ESummaryCounts(t, summary)

	t.Run("portals: room_type derived from id prefix, never RoomTypeSpace", func(t *testing.T) {
		dm := readMigratedPortal(t, deps.Target, e2eDMPortalGCID, e2eUser1Gaia)
		if dm.RoomType != string(database.RoomTypeDM) {
			t.Errorf("DM portal room_type = %q, want %q", dm.RoomType, database.RoomTypeDM)
		}
		if !dm.MXID.Valid || dm.MXID.String != e2eDMRoomMXID {
			t.Errorf("DM portal mxid = %+v, want valid %q", dm.MXID, e2eDMRoomMXID)
		}

		space := readMigratedPortal(t, deps.Target, e2eSpacePortalGCID, "")
		if space.RoomType != string(database.RoomTypeDefault) {
			t.Errorf("space portal room_type = %q, want %q (never RoomTypeSpace)", space.RoomType, database.RoomTypeDefault)
		}
		if space.RoomType == string(database.RoomTypeSpace) {
			t.Fatalf("space portal room_type = %q -- a Google Chat Space must NEVER become Matrix RoomTypeSpace", space.RoomType)
		}
		if space.Topic != "Our e2e team's space" {
			t.Errorf("space portal topic = %q, want the Python description", space.Topic)
		}
		var meta connector.PortalMetadata
		if err := json.Unmarshal([]byte(space.Metadata), &meta); err != nil {
			t.Fatalf("unmarshaling space portal metadata: %v", err)
		}
		if !meta.ThreadsEnabled {
			t.Errorf("space portal ThreadsEnabled = false, want true")
		}
	})

	t.Run("ghosts: avatar_hash is a direct hex copy, one row per puppet", func(t *testing.T) {
		self := readMigratedGhost(t, deps.Target, string(gcid.MakeUserID(e2eUser1Gaia)))
		if self.AvatarHash != e2eValidAvatarHash {
			t.Errorf("self ghost avatar_hash = %q, want %q", self.AvatarHash, e2eValidAvatarHash)
		}
		if self.Name != "E2E User One" {
			t.Errorf("self ghost name = %q", self.Name)
		}

		other := readMigratedGhost(t, deps.Target, string(gcid.MakeUserID(e2eOtherGaia)))
		if other.Name != "E2E Other Person" {
			t.Errorf("other ghost name = %q", other.Name)
		}

		spaceMember := readMigratedGhost(t, deps.Target, string(gcid.MakeUserID(e2eSpaceGaia)))
		if spaceMember.Name != "E2E Space Member" {
			t.Errorf("space member ghost name = %q", spaceMember.Name)
		}
	})

	t.Run("messages: part_ids per the index rule", func(t *testing.T) {
		textOnly := readMigratedMessage(t, deps.Target, "e2eMsgTextOnly", "")
		if textOnly.MXID != "$e2edmtext:example.com" {
			t.Errorf("text-only message mxid = %q", textOnly.MXID)
		}
		wantOtherSenderID := string(gcid.MakeUserID(e2eOtherGaia))
		if textOnly.SenderID != wantOtherSenderID {
			t.Errorf("text-only message sender_id = %q, want %q", textOnly.SenderID, wantOtherSenderID)
		}

		text := readMigratedMessage(t, deps.Target, "e2eMsgTextPlusAtt", "")
		att0 := readMigratedMessage(t, deps.Target, "e2eMsgTextPlusAtt", "att_0")
		att1 := readMigratedMessage(t, deps.Target, "e2eMsgTextPlusAtt", "att_1")
		if text.MXID != "$e2esp1:example.com" || att0.MXID != "$e2esp2:example.com" || att1.MXID != "$e2esp3:example.com" {
			t.Errorf("multi-part message parts = (%q, %q, %q), want ($e2esp1, $e2esp2, $e2esp3)", text.MXID, att0.MXID, att1.MXID)
		}

		attOnly0 := readMigratedMessage(t, deps.Target, "e2eMsgAttOnly", "att_0")
		attOnly1 := readMigratedMessage(t, deps.Target, "e2eMsgAttOnly", "att_1")
		if attOnly0.MXID != "$e2esp4:example.com" || attOnly1.MXID != "$e2esp5:example.com" {
			t.Errorf("attachment-only message parts = (%q, %q), want ($e2esp4, $e2esp5)", attOnly0.MXID, attOnly1.MXID)
		}

		if n := countMessageRows(t, deps.Target, "e2eMsgTextOnly"); n != 1 {
			t.Errorf("e2eMsgTextOnly row count = %d, want 1", n)
		}
		if n := countMessageRows(t, deps.Target, "e2eMsgTextPlusAtt"); n != 3 {
			t.Errorf("e2eMsgTextPlusAtt row count = %d, want 3", n)
		}
		if n := countMessageRows(t, deps.Target, "e2eMsgAttOnly"); n != 2 {
			t.Errorf("e2eMsgAttOnly row count = %d, want 2", n)
		}
	})

	t.Run("reactions: message_part_id matches its message's own part", func(t *testing.T) {
		onText := readMigratedReaction(t, deps.Target, "e2eMsgTextOnly", "\U0001F44D")
		if onText.MessagePartID != "" {
			t.Errorf("reaction on text-only message: message_part_id = %q, want \"\"", onText.MessagePartID)
		}
		if onText.Emoji != variationselector.Add("\U0001F44D") {
			t.Errorf("reaction emoji (display form) = %q, want variation-selector form", onText.Emoji)
		}

		onAttOnly := readMigratedReaction(t, deps.Target, "e2eMsgAttOnly", "❤")
		if onAttOnly.MessagePartID != "att_0" {
			t.Errorf("reaction on attachment-only message: message_part_id = %q, want \"att_0\" (NOT hardcoded \"\")", onAttOnly.MessagePartID)
		}
		wantSpaceSenderID := string(gcid.MakeUserID(e2eSpaceGaia))
		if onAttOnly.SenderID != wantSpaceSenderID {
			t.Errorf("reaction on attachment-only message sender_id = %q, want %q", onAttOnly.SenderID, wantSpaceSenderID)
		}
	})

	t.Run("users/logins: cookie keys UPPERCASE, double-puppet token on user.access_token", func(t *testing.T) {
		user1 := readMigratedUser(t, deps.Target, e2eUser1MXID)
		if !user1.AccessToken.Valid || user1.AccessToken.String != e2eDoublePuppetToken {
			t.Errorf("user1 access_token = %+v, want valid %q (double-puppet)", user1.AccessToken, e2eDoublePuppetToken)
		}
		if !user1.ManagementRoom.Valid || user1.ManagementRoom.String != "!e2enotice:example.com" {
			t.Errorf("user1 management_room = %+v", user1.ManagementRoom)
		}

		user2 := readMigratedUser(t, deps.Target, e2eUser2MXID)
		if user2.AccessToken.Valid {
			t.Errorf("user2 access_token = %+v, want NULL (no double-puppet)", user2.AccessToken)
		}
		if userLoginRowExists(t, deps.Target, string(gcid.MakeUserLoginID(e2eUser1Gaia))) == false {
			t.Fatal("expected a user_login row for e2eUser1Gaia")
		}
		if userLoginRowExists(t, deps.Target, string(gcid.MakeUserLoginID("no-such-gaia"))) {
			t.Error("did not expect a user_login row for a nonexistent gcid")
		}

		login := readMigratedUserLogin(t, deps.Target, string(gcid.MakeUserLoginID(e2eUser1Gaia)))
		var meta connector.UserLoginMetadata
		if err := json.Unmarshal([]byte(login.Metadata), &meta); err != nil {
			t.Fatalf("unmarshaling user_login metadata: %v", err)
		}
		want := map[string]string{"COMPASS": "c1", "SSID": "s1", "SID": "si1", "OSID": "o1", "HSID": "h1"}
		if len(meta.Cookies) != len(want) {
			t.Fatalf("Cookies = %v, want %v", meta.Cookies, want)
		}
		for k, v := range want {
			if meta.Cookies[k] != v {
				t.Errorf("Cookies[%q] = %q, want %q", k, meta.Cookies[k], v)
			}
		}
		for _, lowerKey := range []string{"compass", "ssid", "sid", "osid", "hsid"} {
			if _, ok := meta.Cookies[lowerKey]; ok {
				t.Errorf("Cookies contains lowercase key %q, want only uppercase keys", lowerKey)
			}
		}
		if meta.UserAgent != "Mozilla/5.0 (e2e)" {
			t.Errorf("UserAgent = %q", meta.UserAgent)
		}
		if meta.Revision != 7 {
			t.Errorf("Revision = %d, want 7", meta.Revision)
		}
	})

	t.Run("user_portals: DM present, space absent", func(t *testing.T) {
		row := readMigratedUserPortal(t, deps.Target, e2eDMPortalGCID, e2eUser1Gaia)
		if row.UserMXID != e2eUser1MXID {
			t.Errorf("user_portal user_mxid = %q, want %q", row.UserMXID, e2eUser1MXID)
		}
		wantLoginID := string(gcid.MakeUserLoginID(e2eUser1Gaia))
		if row.LoginID != wantLoginID {
			t.Errorf("user_portal login_id = %q, want %q", row.LoginID, wantLoginID)
		}

		var spacePortalCount int
		if err := deps.Target.QueryRow(ctx, `SELECT COUNT(*) FROM user_portal WHERE portal_id = ?`, e2eSpacePortalGCID).Scan(&spacePortalCount); err != nil {
			t.Fatalf("counting user_portal rows for the space portal: %v", err)
		}
		if spacePortalCount != 0 {
			t.Errorf("space portal has %d user_portal row(s), want 0 (unreconstructable, self-heals)", spacePortalCount)
		}
		if got := countUserPortals(t, deps.Target); got != 1 {
			t.Errorf("total user_portal rows = %d, want 1", got)
		}
	})

	t.Run("backfill_task: zero rows", func(t *testing.T) {
		var count int
		if err := deps.Target.QueryRow(ctx, `SELECT COUNT(*) FROM backfill_task`).Scan(&count); err != nil {
			t.Fatalf("counting backfill_task rows: %v", err)
		}
		if count != 0 {
			t.Errorf("backfill_task row count = %d, want 0", count)
		}
	})
}

// TestEndToEndMigration_DryRun runs the SAME fixture through Run with
// DryRun: true and asserts it reports the identical, non-zero Summary counts
// TestEndToEndMigration_FullRun does, but writes NOTHING: every table Run
// touches (including user_portal and backfill_task, which
// assertTargetEmpty -- migrate_test.go's helper -- doesn't cover) must be
// empty afterward.
func TestEndToEndMigration_DryRun(t *testing.T) {
	deps := &Deps{
		Source:          newE2EFixtureSourceDB(t),
		Target:          newFixtureTargetDB(t),
		FormatGhostMXID: testFormatGhostMXID,
	}
	ctx := context.Background()

	summary, err := Run(ctx, deps, Options{SourceDSN: "e2e-test-dsn", DryRun: true})
	if err != nil {
		t.Fatalf("Run (dry run): %v", err)
	}
	if summary == nil {
		t.Fatal("expected a non-nil summary even for a dry run")
	}
	if !summary.DryRun {
		t.Error("summary.DryRun = false, want true")
	}
	assertE2ESummaryCounts(t, summary)

	assertTargetEmpty(t, deps.Target) // portal, user_login, ghost, message, reaction, user

	for _, table := range []string{"user_portal", "backfill_task"} {
		var count int
		if err := deps.Target.QueryRow(ctx, `SELECT COUNT(*) FROM `+table).Scan(&count); err != nil {
			t.Fatalf("counting %s rows: %v", table, err)
		}
		if count != 0 {
			t.Errorf("dry run: table %q has %d row(s), want 0 (dry run must write nothing)", table, count)
		}
	}
}
