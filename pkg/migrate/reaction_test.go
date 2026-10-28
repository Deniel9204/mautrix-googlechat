package migrate

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"go.mau.fi/util/dbutil"
	"go.mau.fi/util/variationselector"

	"github.com/Deniel9204/mautrix-googlechat/pkg/connector"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

const (
	reactionFixtureBareThumbsUp   = "\U0001F44D" // 👍, no variation selector
	reactionFixtureBareThumbsDown = "\U0001F44E" // 👎, no variation selector
)

// seedReactionFixtureRows adds reactions on seedMessageFixtureRows's messages: one resolving to "",
// one to "att_0", and one orphan.
func seedReactionFixtureRows(t *testing.T, db *sql.DB) {
	t.Helper()
	const insertReactionSQ = `INSERT INTO reaction (mxid, mx_room, emoji, gc_sender, gc_msgid, gc_chat, gc_receiver, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	stmts := []struct {
		query string
		args  []any
	}{
		{insertReactionSQ, []any{"$r1:example.com", "!room:example.com", reactionFixtureBareThumbsUp, msgFixtureSenderGaiaID, "msgTextPlusAtt", msgFixturePortalGCID, msgFixturePortalReceiver, int64(1700000000050000)}},
		{insertReactionSQ, []any{"$r2:example.com", "!room:example.com", reactionFixtureBareThumbsDown, msgFixtureSenderGaiaID, "msgAttOnly", msgFixturePortalGCID, msgFixturePortalReceiver, int64(1700000001050000)}},
		{insertReactionSQ, []any{"$r3:example.com", "!room:example.com", reactionFixtureBareThumbsUp, msgFixtureSenderGaiaID, "msgDoesNotExist", msgFixturePortalGCID, msgFixturePortalReceiver, int64(1700000005000000)}},
	}
	for _, s := range stmts {
		if _, err := db.Exec(s.query, s.args...); err != nil {
			t.Fatalf("seeding reaction fixture row %q: %v", s.query, err)
		}
	}
}

func newReactionFixtureSourceDB(t *testing.T) *dbutil.Database {
	t.Helper()
	path := filepath.Join(t.TempDir(), "reaction_source.db")

	setup, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("opening fixture source for setup: %v", err)
	}
	if _, err := setup.Exec(fixtureSchemaSQL); err != nil {
		t.Fatalf("creating fixture schema: %v", err)
	}
	seedMessageFixtureRows(t, setup)
	seedReactionFixtureRows(t, setup)
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

type migratedReactionRow struct {
	MessagePartID string
	SenderID      string
	SenderMXID    string
	EmojiID       string
	Emoji         string
	Metadata      string
}

func readMigratedReaction(t *testing.T, target *dbutil.Database, messageID, emojiID string) migratedReactionRow {
	t.Helper()
	var row migratedReactionRow
	err := target.QueryRow(context.Background(), `
		SELECT message_part_id, sender_id, sender_mxid, emoji_id, emoji, metadata
		FROM reaction WHERE message_id = ? AND emoji_id = ?
	`, messageID, emojiID).Scan(
		&row.MessagePartID, &row.SenderID, &row.SenderMXID, &row.EmojiID, &row.Emoji, &row.Metadata,
	)
	if err != nil {
		t.Fatalf("reading back migrated reaction (%q, %q): %v", messageID, emojiID, err)
	}
	return row
}

func countReactionRows(t *testing.T, target *dbutil.Database, messageID string) int {
	t.Helper()
	var count int
	if err := target.QueryRow(context.Background(), `SELECT COUNT(*) FROM reaction WHERE message_id = ?`, messageID).Scan(&count); err != nil {
		t.Fatalf("counting reaction rows for message_id %q: %v", messageID, err)
	}
	return count
}

func TestMigrateReactions_MessagePartIDLookup(t *testing.T) {
	deps := &Deps{
		Source:          newReactionFixtureSourceDB(t),
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
	if _, warnings, err := migrateMessages(ctx, deps, Options{}); err != nil {
		t.Fatalf("migrateMessages: %v", err)
	} else if len(warnings) != 1 {
		t.Fatalf("migrateMessages: expected 1 warning (msgNoSender), got %v", warnings)
	}

	count, warnings, err := migrateReactions(ctx, deps, Options{})
	if err != nil {
		t.Fatalf("migrateReactions: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 reactions migrated (orphan skipped), got %d", count)
	}
	if len(warnings) != 1 {
		t.Fatalf("expected exactly 1 warning (orphan reaction), got %d: %v", len(warnings), warnings)
	}

	wantSenderID := string(gcid.MakeUserID(msgFixtureSenderGaiaID))
	wantSenderMXID := string(testFormatGhostMXID(gcid.MakeUserID(msgFixtureSenderGaiaID)))

	t.Run("reaction on text+attachment message resolves part_id \"\"", func(t *testing.T) {
		row := readMigratedReaction(t, deps.Target, "msgTextPlusAtt", reactionFixtureBareThumbsUp)
		if row.MessagePartID != "" {
			t.Errorf("message_part_id = %q, want \"\" (index=0 row is the text part)", row.MessagePartID)
		}
		if row.EmojiID != reactionFixtureBareThumbsUp {
			t.Errorf("emoji_id = %q, want bare %q", row.EmojiID, reactionFixtureBareThumbsUp)
		}
		if row.Emoji != variationselector.Add(reactionFixtureBareThumbsUp) {
			t.Errorf("emoji = %q, want variationselector.Add(bare) = %q", row.Emoji, variationselector.Add(reactionFixtureBareThumbsUp))
		}
		if row.SenderID != wantSenderID {
			t.Errorf("sender_id = %q, want %q", row.SenderID, wantSenderID)
		}
		if row.SenderMXID != wantSenderMXID {
			t.Errorf("sender_mxid = %q, want %q (FormatGhostMXID output)", row.SenderMXID, wantSenderMXID)
		}
		var meta connector.ReactionMetadata
		if err := json.Unmarshal([]byte(row.Metadata), &meta); err != nil {
			t.Fatalf("unmarshaling metadata: %v", err)
		}
		if meta.TopicID != "" {
			t.Errorf("meta.TopicID = %q, want \"\" (left empty on migration, schema map §3)", meta.TopicID)
		}
	})

	t.Run("reaction on attachment-only message resolves part_id \"att_0\"", func(t *testing.T) {
		row := readMigratedReaction(t, deps.Target, "msgAttOnly", reactionFixtureBareThumbsDown)
		if row.MessagePartID != "att_0" {
			t.Errorf("message_part_id = %q, want \"att_0\" (index=0 row, no text part in this group)", row.MessagePartID)
		}
	})

	t.Run("orphan reaction (no matching message) is warned and skipped", func(t *testing.T) {
		if n := countReactionRows(t, deps.Target, "msgDoesNotExist"); n != 0 {
			t.Errorf("expected 0 rows written for the orphan reaction, got %d", n)
		}
	})
}

// The source message exists but migrateMessages skipped it; the reaction must warn-and-skip, not abort.
func TestMigrateReactions_TargetMessageNotMigrated_WarnsAndSkips(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reaction_dangling_message_source.db")
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
	// NULL gc_sender: migrateMessages skips this row.
	_, err = setup.Exec(msgFixtureInsertMessageSQ,
		"$eDangling:example.com", "!room:example.com", "msgNoSenderForReaction", msgFixturePortalGCID, msgFixturePortalReceiver, nil, nil, 0, int64(1700000008000000), "m.text",
	)
	if err != nil {
		t.Fatalf("seeding dangling message row: %v", err)
	}
	_, err = setup.Exec(
		`INSERT INTO reaction (mxid, mx_room, emoji, gc_sender, gc_msgid, gc_chat, gc_receiver, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		"$rDangling:example.com", "!room:example.com", reactionFixtureBareThumbsUp, msgFixtureSenderGaiaID, "msgNoSenderForReaction", msgFixturePortalGCID, msgFixturePortalReceiver, int64(1700000008050000),
	)
	if err != nil {
		t.Fatalf("seeding dangling reaction row: %v", err)
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
	msgCount, msgWarnings, err := migrateMessages(ctx, deps, Options{})
	if err != nil {
		t.Fatalf("migrateMessages: %v", err)
	}
	if msgCount != 0 || len(msgWarnings) != 1 {
		t.Fatalf("expected the message row to be skipped with 1 warning, got count=%d warnings=%v", msgCount, msgWarnings)
	}

	count, warnings, err := migrateReactions(ctx, deps, Options{})
	if err != nil {
		t.Fatalf("migrateReactions: %v (must warn-and-skip an orphaned reaction, not abort the whole migration)", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 reactions migrated, got %d", count)
	}
	if len(warnings) != 1 {
		t.Fatalf("expected exactly 1 warning, got %d: %v", len(warnings), warnings)
	}
	if !strings.Contains(warnings[0], "msgNoSenderForReaction") {
		t.Errorf("warning = %q, want it to mention msgNoSenderForReaction", warnings[0])
	}
	if n := countReactionRows(t, deps.Target, "msgNoSenderForReaction"); n != 0 {
		t.Errorf("expected 0 rows written for the dangling reaction, got %d", n)
	}
}

// The reactor has no puppet row, so no ghost; the message's own sender does.
func TestMigrateReactions_SenderGhostMissing_WarnsAndSkips(t *testing.T) {
	const reactorWithNoGhostGaiaID = "222222222222222222"

	path := filepath.Join(t.TempDir(), "reaction_ghostless_sender_source.db")
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
	_, err = setup.Exec(msgFixtureInsertMessageSQ,
		"$eForGhostlessReactor:example.com", "!room:example.com", "msgForGhostlessReactor", msgFixturePortalGCID, msgFixturePortalReceiver, nil, msgFixtureSenderGaiaID, 0, int64(1700000009000000), "m.text",
	)
	if err != nil {
		t.Fatalf("seeding message row: %v", err)
	}
	_, err = setup.Exec(
		`INSERT INTO reaction (mxid, mx_room, emoji, gc_sender, gc_msgid, gc_chat, gc_receiver, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		"$rGhostlessReactor:example.com", "!room:example.com", reactionFixtureBareThumbsUp, reactorWithNoGhostGaiaID, "msgForGhostlessReactor", msgFixturePortalGCID, msgFixturePortalReceiver, int64(1700000009050000),
	)
	if err != nil {
		t.Fatalf("seeding reaction row: %v", err)
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
	msgCount, _, err := migrateMessages(ctx, deps, Options{})
	if err != nil {
		t.Fatalf("migrateMessages: %v", err)
	}
	if msgCount != 1 {
		t.Fatalf("expected the message to migrate fine (its own sender has a ghost), got count=%d", msgCount)
	}

	count, warnings, err := migrateReactions(ctx, deps, Options{})
	if err != nil {
		t.Fatalf("migrateReactions: %v (must warn-and-skip, not abort)", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 reactions migrated, got %d", count)
	}
	if len(warnings) != 1 {
		t.Fatalf("expected exactly 1 warning, got %d: %v", len(warnings), warnings)
	}
	if !strings.Contains(warnings[0], reactorWithNoGhostGaiaID) {
		t.Errorf("warning = %q, want it to mention the ghostless reactor %q", warnings[0], reactorWithNoGhostGaiaID)
	}
}

// A message group without an index=0 row (corrupt source) must warn-and-skip.
func TestMigrateReactions_NoIndexZeroRow_WarnsAndSkips(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reaction_no_index_zero_source.db")
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

	_, err = setup.Exec(msgFixtureInsertMessageSQ,
		"$eNoIndexZero:example.com", "!room:example.com", "msgNoIndexZero", msgFixturePortalGCID, msgFixturePortalReceiver, nil, msgFixtureSenderGaiaID, 1, int64(1700000010000000), "m.text",
	)
	if err != nil {
		t.Fatalf("seeding no-index-0 message row: %v", err)
	}
	_, err = setup.Exec(
		`INSERT INTO reaction (mxid, mx_room, emoji, gc_sender, gc_msgid, gc_chat, gc_receiver, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		"$rNoIndexZero:example.com", "!room:example.com", reactionFixtureBareThumbsUp, msgFixtureSenderGaiaID, "msgNoIndexZero", msgFixturePortalGCID, msgFixturePortalReceiver, int64(1700000010050000),
	)
	if err != nil {
		t.Fatalf("seeding reaction row: %v", err)
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
	if _, _, err := migrateMessages(ctx, deps, Options{}); err != nil {
		t.Fatalf("migrateMessages: %v", err)
	}

	count, warnings, err := migrateReactions(ctx, deps, Options{})
	if err != nil {
		t.Fatalf("migrateReactions: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 reactions migrated, got %d", count)
	}
	if len(warnings) != 1 {
		t.Fatalf("expected exactly 1 warning, got %d: %v", len(warnings), warnings)
	}
	if !strings.Contains(warnings[0], "msgNoIndexZero") {
		t.Errorf("warning = %q, want it to mention msgNoIndexZero", warnings[0])
	}
}

// The message guard ignores room_id, so a gcid shared by a migrated and an unmigrated chat
// must still skip the reaction via the portal check.
func TestMigrateReactions_PortalNotMigrated_WarnsAndSkips(t *testing.T) {
	const (
		collidingGCID  = "collidingMsgID"
		unmigratedChat = "space:NeverMigrated"
	)

	path := filepath.Join(t.TempDir(), "reaction_dangling_portal_source.db")
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
	// Same gcid in both chats; only the first chat's portal exists.
	_, err = setup.Exec(msgFixtureInsertMessageSQ,
		"$mMigrated:example.com", "!room:example.com", collidingGCID, msgFixturePortalGCID, msgFixturePortalReceiver, nil, msgFixtureSenderGaiaID, 0, int64(1700000009000000), "m.text",
	)
	if err != nil {
		t.Fatalf("seeding migrated message row: %v", err)
	}
	_, err = setup.Exec(msgFixtureInsertMessageSQ,
		"$mOrphan:example.com", "!other:example.com", collidingGCID, unmigratedChat, msgFixturePortalReceiver, nil, msgFixtureSenderGaiaID, 0, int64(1700000009100000), "m.text",
	)
	if err != nil {
		t.Fatalf("seeding orphan message row: %v", err)
	}

	_, err = setup.Exec(
		`INSERT INTO reaction (mxid, mx_room, emoji, gc_sender, gc_msgid, gc_chat, gc_receiver, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		"$rOrphanPortal:example.com", "!other:example.com", reactionFixtureBareThumbsUp, msgFixtureSenderGaiaID, collidingGCID, unmigratedChat, msgFixturePortalReceiver, int64(1700000009150000),
	)
	if err != nil {
		t.Fatalf("seeding orphan reaction row: %v", err)
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
	msgCount, _, err := migrateMessages(ctx, deps, Options{})
	if err != nil {
		t.Fatalf("migrateMessages: %v", err)
	}
	if msgCount != 1 {
		t.Fatalf("expected exactly the migrated chat's message to be written, got count=%d", msgCount)
	}

	count, warnings, err := migrateReactions(ctx, deps, Options{})
	if err != nil {
		t.Fatalf("migrateReactions: %v (an orphan reaction whose PORTAL was not migrated must warn-and-skip, not abort the whole migration)", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 reactions migrated, got %d", count)
	}
	if len(warnings) != 1 {
		t.Fatalf("expected exactly 1 warning, got %d: %v", len(warnings), warnings)
	}
	if !strings.Contains(warnings[0], unmigratedChat) {
		t.Errorf("warning = %q, want it to name the chat that was not migrated (%q)", warnings[0], unmigratedChat)
	}
	if n := countReactionRows(t, deps.Target, collidingGCID); n != 0 {
		t.Errorf("expected 0 reaction rows written, got %d", n)
	}
}
