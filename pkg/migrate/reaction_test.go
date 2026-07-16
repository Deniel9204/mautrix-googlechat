package migrate

// White-box tests (package migrate) for migrateReactions -- M7 Task 6. See
// .superpowers/sdd/m7-migration-schema-map.md §3 (Reaction) for the mapping
// under test, especially the message_part_id lookup (do NOT hardcode "").
//
// Reuses seedMessageFixtureRows (message_test.go) for the message groups a
// reaction can resolve against, so both files agree on exactly the same
// index->part_id assignments.

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

// seedReactionFixtureRows adds reaction rows on top of
// seedMessageFixtureRows's message groups:
//   - thumbsUp reacts to msgTextPlusAtt -> resolves to the text row's part id ("").
//   - thumbsDown reacts to msgAttOnly -> resolves to its index-0 row's part id ("att_0").
//   - orphan reacts to a gc_msgid with no matching message row at all -> warn+skip.
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

	// Full step order (portals, ghosts, messages, then reactions) -- a
	// reaction's FK requires the target message row to already exist.
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

// TestMigrateReactions_TargetMessageNotMigrated_WarnsAndSkips is the
// regression test for the gchat-port-auditor's P1 finding: the SOURCE has a
// matching message row for the reaction's (gc_msgid, gc_chat, gc_receiver,
// index=0), but migrateMessages itself skipped writing that row to the
// TARGET (here: a NULL gc_sender). Without the messagePartExistsInTarget
// guard, inserting the reaction fails reaction_message_fkey and
// migrateReactions returns a non-nil error, which aborts and rolls back the
// ENTIRE migration (portals/ghosts/messages/reactions/users) via Run's
// single transaction -- not a per-row warn+skip. This must instead be
// treated as an orphan reaction.
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
	// NULL gc_sender -> migrateMessages will skip this row (schema map §2:
	// "no ghost to attribute sender to"), leaving nothing in the target for
	// the reaction below to reference.
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

// TestMigrateReactions_SenderGhostMissing_WarnsAndSkips covers a reactor
// whose gaia ID has no corresponding `puppet` row in the source at all (so
// migrateGhosts never creates a ghost for it) -- distinct from the message's
// own sender, which does have a ghost, so the message itself migrates fine.
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
	// Only the message author has a puppet row -- the reactor below does not.
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

// TestMigrateReactions_NoIndexZeroRow_WarnsAndSkips covers the defensive
// "message group has no index=0 row" branch (reaction.go): a corrupt/foreign
// source where the referenced message group exists but its lowest index is
// not 0 (Python's own index counter always starts at 0, so this shouldn't
// happen with well-formed data, but the code must not panic or write a
// bogus row if it does).
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
	// index=1, no index=0 row at all -- a well-formed Python DB never
	// produces this (its counter always starts at 0), but the migrator must
	// still degrade gracefully rather than panic/miswrite.
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
