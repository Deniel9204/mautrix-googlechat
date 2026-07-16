package migrate

// White-box tests (package migrate) for migrateMessages -- M7 Task 6. See
// .superpowers/sdd/m7-migration-schema-map.md §2 (Message -- THE HARD ONE)
// for the index->part_id rule under test.
//
// newMessageFixtureSourceDB/seedMessageFixtureRows build a dedicated fixture
// (rather than reusing source_test.go's shared 2-row fixture) because this
// task's brief needs multi-row message groups the shared fixture doesn't
// have. reaction_test.go reuses seedMessageFixtureRows so both files agree
// on exactly the same message groups.

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

const (
	msgFixtureSenderGaiaID    = "111111111111111111"
	msgFixturePortalGCID      = "space:AAAAdaMWXXc"
	msgFixturePortalReceiver  = ""
	msgFixtureInsertMessageSQ = `INSERT INTO message (mxid, mx_room, gcid, gc_chat, gc_receiver, gc_parent_id, gc_sender, "index", timestamp, msgtype) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
)

// seedMessageFixtureRows inserts one portal + one puppet (the sender) + the
// message rows exercising schema map §2's full index->part_id rule:
//   - msgTextPlusAtt: text + 2 attachments -> "", att_0, att_1
//   - msgAttOnly: attachment-only multi-row (lowest msgtype non-text) -> att_0, att_1
//   - msgSingle: single row (attachment msgtype) -> "" regardless (the plan's
//     resolved single-row decision, map Risk #1 option a)
//   - msgReply: single row with a thread parent -> thread_root_id/TopicID = parent
//   - msgNoSender: single row with a NULL gc_sender -> warn-and-skip
//
// The portal/puppet rows exist so migratePortals/migrateGhosts can seed the
// target rows migrateMessages's FK constraints (message_room_fkey,
// message_sender_fkey) require.
func seedMessageFixtureRows(t *testing.T, db *sql.DB) {
	t.Helper()
	stmts := []struct {
		query string
		args  []any
	}{
		{
			`INSERT INTO portal (gcid, gc_receiver, other_user_id, mxid, name, avatar_mxc, description, name_set, avatar_set, description_set, encrypted, revision, threads_only, threads_enabled) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			[]any{msgFixturePortalGCID, msgFixturePortalReceiver, nil, "!room:example.com", "Team", nil, nil, true, false, false, false, nil, nil, nil},
		},
		{
			`INSERT INTO puppet (gcid, name, photo_id, photo_mxc, photo_hash, name_set, avatar_set, contact_info_set, is_registered, custom_mxid, access_token, next_batch, base_url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			[]any{msgFixtureSenderGaiaID, "Sender", nil, nil, nil, true, false, false, false, nil, nil, nil, nil},
		},
		// group: text + 2 attachments -> "", att_0, att_1
		{msgFixtureInsertMessageSQ, []any{"$e1:example.com", "!room:example.com", "msgTextPlusAtt", msgFixturePortalGCID, msgFixturePortalReceiver, nil, msgFixtureSenderGaiaID, 0, int64(1700000000000000), "m.text"}},
		{msgFixtureInsertMessageSQ, []any{"$e2:example.com", "!room:example.com", "msgTextPlusAtt", msgFixturePortalGCID, msgFixturePortalReceiver, nil, msgFixtureSenderGaiaID, 1, int64(1700000000100000), "m.image"}},
		{msgFixtureInsertMessageSQ, []any{"$e3:example.com", "!room:example.com", "msgTextPlusAtt", msgFixturePortalGCID, msgFixturePortalReceiver, nil, msgFixtureSenderGaiaID, 2, int64(1700000000200000), "m.video"}},
		// group: attachment-only multi-row (lowest-index row's msgtype is NOT text) -> att_0, att_1
		{msgFixtureInsertMessageSQ, []any{"$e4:example.com", "!room:example.com", "msgAttOnly", msgFixturePortalGCID, msgFixturePortalReceiver, nil, msgFixtureSenderGaiaID, 0, int64(1700000001000000), "m.image"}},
		{msgFixtureInsertMessageSQ, []any{"$e5:example.com", "!room:example.com", "msgAttOnly", msgFixturePortalGCID, msgFixturePortalReceiver, nil, msgFixtureSenderGaiaID, 1, int64(1700000001100000), "m.file"}},
		// group: single row (attachment msgtype) -> "" regardless of msgtype
		{msgFixtureInsertMessageSQ, []any{"$e6:example.com", "!room:example.com", "msgSingle", msgFixturePortalGCID, msgFixturePortalReceiver, nil, msgFixtureSenderGaiaID, 0, int64(1700000002000000), "m.image"}},
		// single row with a thread parent -> thread_root_id/TopicID = parent
		{msgFixtureInsertMessageSQ, []any{"$e7:example.com", "!room:example.com", "msgReply", msgFixturePortalGCID, msgFixturePortalReceiver, "msgRootExternal", msgFixtureSenderGaiaID, 0, int64(1700000003000000), "m.text"}},
		// NULL gc_sender -> warn and skip (no ghost to attribute sender to)
		{msgFixtureInsertMessageSQ, []any{"$e8:example.com", "!room:example.com", "msgNoSender", msgFixturePortalGCID, msgFixturePortalReceiver, nil, nil, 0, int64(1700000004000000), "m.text"}},
	}
	for _, s := range stmts {
		if _, err := db.Exec(s.query, s.args...); err != nil {
			t.Fatalf("seeding message fixture row %q: %v", s.query, err)
		}
	}
}

func newMessageFixtureSourceDB(t *testing.T) *dbutil.Database {
	t.Helper()
	path := filepath.Join(t.TempDir(), "message_source.db")

	setup, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("opening fixture source for setup: %v", err)
	}
	if _, err := setup.Exec(fixtureSchemaSQL); err != nil {
		t.Fatalf("creating fixture schema: %v", err)
	}
	seedMessageFixtureRows(t, setup)
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

type migratedMessageRow struct {
	MXID         string
	SenderID     string
	SenderMXID   string
	ThreadRootID sql.NullString
	EditCount    int
	Timestamp    int64 // raw column value (UnixNano, per message.go's doc comment)
	Metadata     string
}

func readMigratedMessage(t *testing.T, target *dbutil.Database, id, partID string) migratedMessageRow {
	t.Helper()
	var row migratedMessageRow
	err := target.QueryRow(context.Background(), `
		SELECT mxid, sender_id, sender_mxid, thread_root_id, edit_count, timestamp, metadata
		FROM message WHERE id = ? AND part_id = ?
	`, id, partID).Scan(
		&row.MXID, &row.SenderID, &row.SenderMXID, &row.ThreadRootID, &row.EditCount, &row.Timestamp, &row.Metadata,
	)
	if err != nil {
		t.Fatalf("reading back migrated message (%q, %q): %v", id, partID, err)
	}
	return row
}

// countMessageRows counts how many Go message rows exist for a given
// Python gcid, across all part_ids -- used to assert a skipped row wrote
// nothing at all.
func countMessageRows(t *testing.T, target *dbutil.Database, id string) int {
	t.Helper()
	var count int
	if err := target.QueryRow(context.Background(), `SELECT COUNT(*) FROM message WHERE id = ?`, id).Scan(&count); err != nil {
		t.Fatalf("counting message rows for id %q: %v", id, err)
	}
	return count
}

func TestMigrateMessages_IndexToPartIDRule(t *testing.T) {
	deps := &Deps{
		Source:          newMessageFixtureSourceDB(t),
		Target:          newFixtureTargetDB(t),
		FormatGhostMXID: testFormatGhostMXID,
	}
	ctx := context.Background()

	// Seed the target's portal/ghost rows the message FK constraints
	// require -- mirrors Run's real step order (portals, ghosts, then
	// messages).
	if _, _, err := migratePortals(ctx, deps, Options{}); err != nil {
		t.Fatalf("migratePortals: %v", err)
	}
	if _, _, err := migrateGhosts(ctx, deps, Options{}); err != nil {
		t.Fatalf("migrateGhosts: %v", err)
	}

	count, warnings, err := migrateMessages(ctx, deps, Options{})
	if err != nil {
		t.Fatalf("migrateMessages: %v", err)
	}
	// 3 (msgTextPlusAtt) + 2 (msgAttOnly) + 1 (msgSingle) + 1 (msgReply) = 7;
	// msgNoSender's single row is skipped.
	if count != 7 {
		t.Fatalf("expected 7 messages migrated, got %d", count)
	}
	if len(warnings) != 1 {
		t.Fatalf("expected exactly 1 warning (msgNoSender), got %d: %v", len(warnings), warnings)
	}
	if !strings.Contains(warnings[0], "msgNoSender") {
		t.Errorf("warning = %q, want it to mention msgNoSender", warnings[0])
	}

	wantSenderID := string(gcid.MakeUserID(msgFixtureSenderGaiaID))
	wantSenderMXID := string(testFormatGhostMXID(gcid.MakeUserID(msgFixtureSenderGaiaID)))

	t.Run("text+2 attachments -> \"\", att_0, att_1", func(t *testing.T) {
		text := readMigratedMessage(t, deps.Target, "msgTextPlusAtt", "")
		att0 := readMigratedMessage(t, deps.Target, "msgTextPlusAtt", "att_0")
		att1 := readMigratedMessage(t, deps.Target, "msgTextPlusAtt", "att_1")

		if text.MXID != "$e1:example.com" {
			t.Errorf("text part mxid = %q, want $e1:example.com", text.MXID)
		}
		if att0.MXID != "$e2:example.com" {
			t.Errorf("att_0 mxid = %q, want $e2:example.com (index 1, first attachment)", att0.MXID)
		}
		if att1.MXID != "$e3:example.com" {
			t.Errorf("att_1 mxid = %q, want $e3:example.com (index 2, second attachment)", att1.MXID)
		}

		// Root message (no gc_parent_id): thread_root_id NULL, TopicID self-references its own gcid.
		if text.ThreadRootID.Valid {
			t.Errorf("text part thread_root_id = %+v, want NULL (no parent)", text.ThreadRootID)
		}
		var meta connector.MessageMetadata
		if err := json.Unmarshal([]byte(text.Metadata), &meta); err != nil {
			t.Fatalf("unmarshaling metadata: %v", err)
		}
		if meta.TopicID != "msgTextPlusAtt" {
			t.Errorf("text part TopicID = %q, want self-reference \"msgTextPlusAtt\"", meta.TopicID)
		}
		if meta.TimestampMicro != 1700000000000000 {
			t.Errorf("text part TimestampMicro = %d, want 1700000000000000", meta.TimestampMicro)
		}
		// Go message.timestamp is UnixNano (bridgev2/database/message.go's
		// sqlVariables/Scan round trip) -- µs*1000.
		if text.Timestamp != 1700000000000000*1000 {
			t.Errorf("text part raw timestamp column = %d, want %d (µs*1000, UnixNano)", text.Timestamp, 1700000000000000*1000)
		}
		if text.EditCount != 0 {
			t.Errorf("text part edit_count = %d, want 0", text.EditCount)
		}
	})

	t.Run("attachment-only multi-row -> att_0, att_1", func(t *testing.T) {
		att0 := readMigratedMessage(t, deps.Target, "msgAttOnly", "att_0")
		att1 := readMigratedMessage(t, deps.Target, "msgAttOnly", "att_1")
		if att0.MXID != "$e4:example.com" {
			t.Errorf("att_0 mxid = %q, want $e4:example.com (index 0, first attachment, no text row)", att0.MXID)
		}
		if att1.MXID != "$e5:example.com" {
			t.Errorf("att_1 mxid = %q, want $e5:example.com (index 1, second attachment)", att1.MXID)
		}
	})

	t.Run("single-row -> \"\" regardless of msgtype", func(t *testing.T) {
		row := readMigratedMessage(t, deps.Target, "msgSingle", "")
		if row.MXID != "$e6:example.com" {
			t.Errorf("single-row part mxid = %q, want $e6:example.com", row.MXID)
		}
	})

	t.Run("threaded reply: thread_root_id + TopicID = parent", func(t *testing.T) {
		row := readMigratedMessage(t, deps.Target, "msgReply", "")
		if !row.ThreadRootID.Valid || row.ThreadRootID.String != "msgRootExternal" {
			t.Errorf("thread_root_id = %+v, want valid \"msgRootExternal\"", row.ThreadRootID)
		}
		var meta connector.MessageMetadata
		if err := json.Unmarshal([]byte(row.Metadata), &meta); err != nil {
			t.Fatalf("unmarshaling metadata: %v", err)
		}
		if meta.TopicID != "msgRootExternal" {
			t.Errorf("TopicID = %q, want \"msgRootExternal\" (gc_parent_id, not self)", meta.TopicID)
		}
	})

	t.Run("sender_id/sender_mxid derived via gcid.MakeUserID + FormatGhostMXID", func(t *testing.T) {
		row := readMigratedMessage(t, deps.Target, "msgSingle", "")
		if row.SenderID != wantSenderID {
			t.Errorf("sender_id = %q, want %q", row.SenderID, wantSenderID)
		}
		if row.SenderMXID != wantSenderMXID {
			t.Errorf("sender_mxid = %q, want %q (FormatGhostMXID output)", row.SenderMXID, wantSenderMXID)
		}
	})

	t.Run("NULL gc_sender row is skipped entirely, not just partially written", func(t *testing.T) {
		if n := countMessageRows(t, deps.Target, "msgNoSender"); n != 0 {
			t.Errorf("expected 0 rows written for msgNoSender, got %d", n)
		}
	})
}

// newUnmigratedPortalMessageSourceDB builds a fixture whose only message row
// references a gc_chat/gc_receiver with NO corresponding portal row in the
// source DB at all -- so migratePortals never creates that portal in the
// target. Exercises the message_room_fkey guard (portalExistsInTarget):
// without it, migrateMessages would fail the FK and abort the ENTIRE
// migration instead of warning and skipping just this row.
func newUnmigratedPortalMessageSourceDB(t *testing.T) *dbutil.Database {
	t.Helper()
	path := filepath.Join(t.TempDir(), "unmigrated_portal_message_source.db")
	setup, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("opening fixture source for setup: %v", err)
	}
	if _, err := setup.Exec(fixtureSchemaSQL); err != nil {
		t.Fatalf("creating fixture schema: %v", err)
	}
	// Deliberately NO portal row for "space:NoPortalHere".
	_, err = setup.Exec(
		`INSERT INTO puppet (gcid, name, photo_id, photo_mxc, photo_hash, name_set, avatar_set, contact_info_set, is_registered, custom_mxid, access_token, next_batch, base_url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		msgFixtureSenderGaiaID, "Sender", nil, nil, nil, true, false, false, false, nil, nil, nil, nil,
	)
	if err != nil {
		t.Fatalf("seeding puppet row: %v", err)
	}
	_, err = setup.Exec(msgFixtureInsertMessageSQ,
		"$eOrphanPortal:example.com", "!room:example.com", "msgOrphanPortal", "space:NoPortalHere", "", nil, msgFixtureSenderGaiaID, 0, int64(1700000006000000), "m.text",
	)
	if err != nil {
		t.Fatalf("seeding orphan-portal message row: %v", err)
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

func TestMigrateMessages_UnmigratedPortal_WarnsAndSkips(t *testing.T) {
	deps := &Deps{
		Source:          newUnmigratedPortalMessageSourceDB(t),
		Target:          newFixtureTargetDB(t),
		FormatGhostMXID: testFormatGhostMXID,
	}
	ctx := context.Background()

	if _, _, err := migratePortals(ctx, deps, Options{}); err != nil {
		t.Fatalf("migratePortals: %v", err)
	}
	if _, _, err := migrateGhosts(ctx, deps, Options{}); err != nil {
		t.Fatalf("migrateGhosts: %v", err)
	}

	count, warnings, err := migrateMessages(ctx, deps, Options{})
	if err != nil {
		t.Fatalf("migrateMessages: %v (must warn-and-skip, not abort the whole migration)", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 messages migrated, got %d", count)
	}
	if len(warnings) != 1 {
		t.Fatalf("expected exactly 1 warning, got %d: %v", len(warnings), warnings)
	}
	if !strings.Contains(warnings[0], "msgOrphanPortal") && !strings.Contains(warnings[0], "space:NoPortalHere") {
		t.Errorf("warning = %q, want it to mention the unmigrated portal/message", warnings[0])
	}
	if n := countMessageRows(t, deps.Target, "msgOrphanPortal"); n != 0 {
		t.Errorf("expected 0 rows written for msgOrphanPortal, got %d", n)
	}
}

// TestMigrateMessages_MNoticeClassifiedAsText exercises the OTHER
// text-classifying msgtype schema map §2 lists alongside m.text -- the
// existing text+2-attachments fixture only covers m.text.
func TestMigrateMessages_MNoticeClassifiedAsText(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mnotice_source.db")
	setup, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("opening fixture source for setup: %v", err)
	}
	if _, err := setup.Exec(fixtureSchemaSQL); err != nil {
		t.Fatalf("creating fixture schema: %v", err)
	}
	_, err = setup.Exec(
		`INSERT INTO portal (gcid, gc_receiver, other_user_id, mxid, name, avatar_mxc, description, name_set, avatar_set, description_set, encrypted, revision, threads_only, threads_enabled) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		msgFixturePortalGCID, msgFixturePortalReceiver, nil, "!room:example.com", "Team", nil, nil, true, false, false, false, nil, nil, nil,
	)
	if err != nil {
		t.Fatalf("seeding portal row: %v", err)
	}
	_, err = setup.Exec(
		`INSERT INTO puppet (gcid, name, photo_id, photo_mxc, photo_hash, name_set, avatar_set, contact_info_set, is_registered, custom_mxid, access_token, next_batch, base_url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		msgFixtureSenderGaiaID, "Sender", nil, nil, nil, true, false, false, false, nil, nil, nil, nil,
	)
	if err != nil {
		t.Fatalf("seeding puppet row: %v", err)
	}
	for _, row := range []struct {
		mxid    string
		index   int
		msgtype string
	}{
		{"$eNotice1:example.com", 0, "m.notice"},
		{"$eNotice2:example.com", 1, "m.image"},
	} {
		_, err = setup.Exec(msgFixtureInsertMessageSQ,
			row.mxid, "!room:example.com", "msgNoticePlusAtt", msgFixturePortalGCID, msgFixturePortalReceiver, nil, msgFixtureSenderGaiaID, row.index, int64(1700000007000000), row.msgtype,
		)
		if err != nil {
			t.Fatalf("seeding m.notice fixture row: %v", err)
		}
	}
	if err := setup.Close(); err != nil {
		t.Fatalf("closing fixture setup connection: %v", err)
	}

	source, err := OpenSource(path)
	if err != nil {
		t.Fatalf("OpenSource(%q): %v", path, err)
	}
	t.Cleanup(func() { _ = source.Close() })

	deps := &Deps{
		Source:          source,
		Target:          newFixtureTargetDB(t),
		FormatGhostMXID: testFormatGhostMXID,
	}
	ctx := context.Background()

	if _, _, err := migratePortals(ctx, deps, Options{}); err != nil {
		t.Fatalf("migratePortals: %v", err)
	}
	if _, _, err := migrateGhosts(ctx, deps, Options{}); err != nil {
		t.Fatalf("migrateGhosts: %v", err)
	}
	count, warnings, err := migrateMessages(ctx, deps, Options{})
	if err != nil {
		t.Fatalf("migrateMessages: %v", err)
	}
	if len(warnings) != 0 {
		t.Errorf("expected no warnings, got %v", warnings)
	}
	if count != 2 {
		t.Fatalf("expected 2 messages migrated, got %d", count)
	}

	text := readMigratedMessage(t, deps.Target, "msgNoticePlusAtt", "")
	att0 := readMigratedMessage(t, deps.Target, "msgNoticePlusAtt", "att_0")
	if text.MXID != "$eNotice1:example.com" {
		t.Errorf("m.notice part mxid = %q, want $eNotice1:example.com (m.notice classified as text, like m.text)", text.MXID)
	}
	if att0.MXID != "$eNotice2:example.com" {
		t.Errorf("att_0 mxid = %q, want $eNotice2:example.com", att0.MXID)
	}
}
