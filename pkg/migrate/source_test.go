package migrate

// White-box tests (package migrate, not migrate_test): source_test.go builds
// a hand-rolled fixture Python SQLite database matching the FINAL Python
// schema reconstructed in .superpowers/sdd/m7-migration-schema-map.md
// (§1-§5), seeds it with a couple of representative rows per table --
// including deliberate NULLs, since every Python column the schema map
// marks nullable is exercised by real installs -- and asserts the Get*
// readers return the right typed rows with correct NULL handling.
//
// The fixture is built with a normal read-write "sqlite3" connection (the
// same driver source.go blank-imports), then closed and reopened via the
// package's own OpenSource so these tests exercise the exact read-only path
// production code uses, not a shortcut around it.

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"go.mau.fi/util/dbutil"
)

// fixtureSchemaSQL is the Python bridge's final per-entity schema (v10),
// reconstructed per m7-migration-schema-map.md §1-§5. Column names/nullability
// mirror that doc exactly; types are SQLite storage classes (SQLite itself is
// dynamically typed, so BOOLEAN/BIGINT are advisory only).
const fixtureSchemaSQL = `
CREATE TABLE portal (
	gcid             TEXT,
	gc_receiver      TEXT,
	other_user_id    TEXT,
	mxid             TEXT,
	name             TEXT,
	avatar_mxc       TEXT,
	description      TEXT,
	name_set         BOOLEAN NOT NULL DEFAULT 0,
	avatar_set       BOOLEAN NOT NULL DEFAULT 0,
	description_set  BOOLEAN NOT NULL DEFAULT 0,
	encrypted        BOOLEAN NOT NULL DEFAULT 0,
	revision         BIGINT,
	threads_only     BOOLEAN,
	threads_enabled  BOOLEAN,
	PRIMARY KEY (gcid, gc_receiver)
);

CREATE TABLE message (
	mxid         TEXT NOT NULL,
	mx_room      TEXT NOT NULL,
	gcid         TEXT,
	gc_chat      TEXT NOT NULL,
	gc_receiver  TEXT,
	gc_parent_id TEXT,
	gc_sender    TEXT,
	"index"      SMALLINT NOT NULL,
	timestamp    BIGINT NOT NULL,
	msgtype      TEXT
);

CREATE TABLE reaction (
	mxid        TEXT NOT NULL,
	mx_room     TEXT NOT NULL,
	emoji       TEXT,
	gc_sender   TEXT,
	gc_msgid    TEXT,
	gc_chat     TEXT,
	gc_receiver TEXT,
	timestamp   BIGINT NOT NULL
);

CREATE TABLE puppet (
	gcid             TEXT PRIMARY KEY,
	name             TEXT,
	photo_id         TEXT,
	photo_mxc        TEXT,
	photo_hash       TEXT,
	name_set         BOOLEAN NOT NULL DEFAULT 0,
	avatar_set       BOOLEAN NOT NULL DEFAULT 0,
	contact_info_set BOOLEAN NOT NULL DEFAULT 0,
	is_registered    BOOLEAN NOT NULL DEFAULT 0,
	custom_mxid      TEXT,
	access_token     TEXT,
	next_batch       TEXT,
	base_url         TEXT
);

CREATE TABLE "user" (
	mxid        TEXT PRIMARY KEY,
	gcid        TEXT UNIQUE,
	cookies     TEXT,
	user_agent  TEXT,
	notice_room TEXT,
	revision    BIGINT
);
`

// seedFixtureRows inserts two rows per table: one with as many fields
// populated as a real row would have, and one exercising the NULLs that
// real Python installs actually produce (never-logged-in users, space
// portals with no per-portal avatar, attachment-only messages, etc.).
func seedFixtureRows(t *testing.T, db *sql.DB) {
	t.Helper()
	stmts := []struct {
		query string
		args  []any
	}{
		// portal: one DM (mostly NULL optional fields), one space (fully populated).
		{
			`INSERT INTO portal (gcid, gc_receiver, other_user_id, mxid, name, avatar_mxc, description, name_set, avatar_set, description_set, encrypted, revision, threads_only, threads_enabled) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			[]any{"dm:gGboEmVbEyE", "103432744896036786916", "103432744896036786916", "!dmroom:example.com", nil, nil, nil, false, false, false, true, nil, nil, nil},
		},
		{
			`INSERT INTO portal (gcid, gc_receiver, other_user_id, mxid, name, avatar_mxc, description, name_set, avatar_set, description_set, encrypted, revision, threads_only, threads_enabled) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			[]any{"space:AAAAdaMWXXc", "", nil, "!spaceroom:example.com", "Team Chat", "mxc://example.com/abc", "A space for team chat", true, true, true, false, int64(5), true, true},
		},
		// message: one plain text row, one with a thread parent, NULL sender, NULL msgtype.
		{
			`INSERT INTO message (mxid, mx_room, gcid, gc_chat, gc_receiver, gc_parent_id, gc_sender, "index", timestamp, msgtype) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			[]any{"$event1:example.com", "!dmroom:example.com", "abc123", "dm:gGboEmVbEyE", "103432744896036786916", nil, "103432744896036786916", 0, int64(1700000000000000), "m.text"},
		},
		{
			`INSERT INTO message (mxid, mx_room, gcid, gc_chat, gc_receiver, gc_parent_id, gc_sender, "index", timestamp, msgtype) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			[]any{"$event2:example.com", "!spaceroom:example.com", "def456", "space:AAAAdaMWXXc", "", "root789", nil, 1, int64(1700000001000000), nil},
		},
		// reaction: one fully populated, one with NULL sender/receiver.
		{
			`INSERT INTO reaction (mxid, mx_room, emoji, gc_sender, gc_msgid, gc_chat, gc_receiver, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			[]any{"$reactionevent1:example.com", "!dmroom:example.com", "\U0001F44D", "103432744896036786916", "abc123", "dm:gGboEmVbEyE", "103432744896036786916", int64(1700000000500000)},
		},
		{
			`INSERT INTO reaction (mxid, mx_room, emoji, gc_sender, gc_msgid, gc_chat, gc_receiver, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			[]any{"$reactionevent2:example.com", "!spaceroom:example.com", "❤", nil, "def456", "space:AAAAdaMWXXc", nil, int64(1700000002000000)},
		},
		// puppet: one with a live double-puppet configured, one all-NULL/never-registered.
		{
			`INSERT INTO puppet (gcid, name, photo_id, photo_mxc, photo_hash, name_set, avatar_set, contact_info_set, is_registered, custom_mxid, access_token, next_batch, base_url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			[]any{"103432744896036786916", "Alice", "https://example.com/alice.jpg", "mxc://example.com/alice", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", true, true, true, true, "@alice:example.com", "syt_abc_double_puppet_token", "s123_456", "https://matrix.example.com"},
		},
		{
			`INSERT INTO puppet (gcid, name, photo_id, photo_mxc, photo_hash, name_set, avatar_set, contact_info_set, is_registered, custom_mxid, access_token, next_batch, base_url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			[]any{"999999999999999999", nil, nil, nil, nil, false, false, false, false, nil, nil, nil, nil},
		},
		// user: one logged in (lowercase cookie keys, per preflight item 1), one never logged in.
		{
			`INSERT INTO "user" (mxid, gcid, cookies, user_agent, notice_room, revision) VALUES (?, ?, ?, ?, ?, ?)`,
			[]any{"@user1:example.com", "103432744896036786916", `{"compass":"c1","ssid":"s1","sid":"si1","osid":"o1","hsid":"h1"}`, "Mozilla/5.0 (test)", "!notice:example.com", int64(42)},
		},
		{
			`INSERT INTO "user" (mxid, gcid, cookies, user_agent, notice_room, revision) VALUES (?, ?, ?, ?, ?, ?)`,
			[]any{"@user2:example.com", nil, nil, nil, nil, nil},
		},
	}
	for _, s := range stmts {
		if _, err := db.Exec(s.query, s.args...); err != nil {
			t.Fatalf("seeding fixture row %q: %v", s.query, err)
		}
	}
}

// newFixtureSourceDB builds a fresh fixture Python SQLite database (schema +
// seed rows, no version table) and reopens it read-only via OpenSource,
// exactly like production code would for a real source DSN.
func newFixtureSourceDB(t *testing.T) *dbutil.Database {
	t.Helper()
	path := filepath.Join(t.TempDir(), "source.db")

	setup, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("opening fixture source for setup: %v", err)
	}
	if _, err := setup.Exec(fixtureSchemaSQL); err != nil {
		t.Fatalf("creating fixture schema: %v", err)
	}
	seedFixtureRows(t, setup)
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

func TestGetPortals(t *testing.T) {
	db := newFixtureSourceDB(t)
	ctx := context.Background()

	portals, err := GetPortals(ctx, db)
	if err != nil {
		t.Fatalf("GetPortals: %v", err)
	}
	if len(portals) != 2 {
		t.Fatalf("expected 2 portals, got %d", len(portals))
	}

	byGCID := make(map[string]*PythonPortal, len(portals))
	for _, p := range portals {
		byGCID[p.GCID] = p
	}

	dm, ok := byGCID["dm:gGboEmVbEyE"]
	if !ok {
		t.Fatal("missing DM portal row")
	}
	if dm.GCReceiver != "103432744896036786916" {
		t.Errorf("dm.GCReceiver = %q", dm.GCReceiver)
	}
	if dm.Name.Valid {
		t.Errorf("expected dm.Name NULL, got %+v", dm.Name)
	}
	if !dm.Encrypted {
		t.Errorf("expected dm.Encrypted = true")
	}
	if dm.Revision.Valid {
		t.Errorf("expected dm.Revision NULL, got %+v", dm.Revision)
	}
	if dm.ThreadsOnly.Valid || dm.ThreadsEnabled.Valid {
		t.Errorf("expected threads_only/threads_enabled NULL for dm portal, got %+v / %+v", dm.ThreadsOnly, dm.ThreadsEnabled)
	}

	space, ok := byGCID["space:AAAAdaMWXXc"]
	if !ok {
		t.Fatal("missing space portal row")
	}
	if space.GCReceiver != "" {
		t.Errorf("space.GCReceiver = %q, want empty", space.GCReceiver)
	}
	if !space.Name.Valid || space.Name.String != "Team Chat" {
		t.Errorf("space.Name = %+v, want valid \"Team Chat\"", space.Name)
	}
	if !space.Revision.Valid || space.Revision.Int64 != 5 {
		t.Errorf("space.Revision = %+v, want valid 5", space.Revision)
	}
	if !space.ThreadsOnly.Valid || !space.ThreadsOnly.Bool {
		t.Errorf("space.ThreadsOnly = %+v, want valid true", space.ThreadsOnly)
	}
	if !space.ThreadsEnabled.Valid || !space.ThreadsEnabled.Bool {
		t.Errorf("space.ThreadsEnabled = %+v, want valid true", space.ThreadsEnabled)
	}
}

func TestGetMessages(t *testing.T) {
	db := newFixtureSourceDB(t)
	ctx := context.Background()

	messages, err := GetMessages(ctx, db)
	if err != nil {
		t.Fatalf("GetMessages: %v", err)
	}
	if len(messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(messages))
	}

	byGCID := make(map[string]*PythonMessage, len(messages))
	for _, m := range messages {
		if !m.GCID.Valid {
			t.Fatalf("unexpected NULL gcid on message row %+v", m)
		}
		byGCID[m.GCID.String] = m
	}

	text, ok := byGCID["abc123"]
	if !ok {
		t.Fatal("missing text message row")
	}
	if text.Index != 0 {
		t.Errorf("text.Index = %d, want 0", text.Index)
	}
	if text.GCParentID.Valid {
		t.Errorf("expected text.GCParentID NULL, got %+v", text.GCParentID)
	}
	if !text.GCSender.Valid || text.GCSender.String != "103432744896036786916" {
		t.Errorf("text.GCSender = %+v", text.GCSender)
	}
	if !text.MsgType.Valid || text.MsgType.String != "m.text" {
		t.Errorf("text.MsgType = %+v, want valid \"m.text\"", text.MsgType)
	}
	if text.Timestamp != 1700000000000000 {
		t.Errorf("text.Timestamp = %d", text.Timestamp)
	}

	threaded, ok := byGCID["def456"]
	if !ok {
		t.Fatal("missing threaded message row")
	}
	if threaded.Index != 1 {
		t.Errorf("threaded.Index = %d, want 1", threaded.Index)
	}
	if !threaded.GCParentID.Valid || threaded.GCParentID.String != "root789" {
		t.Errorf("threaded.GCParentID = %+v, want valid \"root789\"", threaded.GCParentID)
	}
	if threaded.GCSender.Valid {
		t.Errorf("expected threaded.GCSender NULL, got %+v", threaded.GCSender)
	}
	if threaded.MsgType.Valid {
		t.Errorf("expected threaded.MsgType NULL, got %+v", threaded.MsgType)
	}
	if threaded.GCReceiver.Valid && threaded.GCReceiver.String != "" {
		t.Errorf("threaded.GCReceiver = %+v, want empty (space portal)", threaded.GCReceiver)
	}
}

func TestGetReactions(t *testing.T) {
	db := newFixtureSourceDB(t)
	ctx := context.Background()

	reactions, err := GetReactions(ctx, db)
	if err != nil {
		t.Fatalf("GetReactions: %v", err)
	}
	if len(reactions) != 2 {
		t.Fatalf("expected 2 reactions, got %d", len(reactions))
	}

	byMsgID := make(map[string]*PythonReaction, len(reactions))
	for _, r := range reactions {
		if !r.GCMsgID.Valid {
			t.Fatalf("unexpected NULL gc_msgid on reaction row %+v", r)
		}
		byMsgID[r.GCMsgID.String] = r
	}

	thumbsUp, ok := byMsgID["abc123"]
	if !ok {
		t.Fatal("missing thumbs-up reaction row")
	}
	if !thumbsUp.GCSender.Valid || thumbsUp.GCSender.String != "103432744896036786916" {
		t.Errorf("thumbsUp.GCSender = %+v", thumbsUp.GCSender)
	}
	if !thumbsUp.GCReceiver.Valid || thumbsUp.GCReceiver.String != "103432744896036786916" {
		t.Errorf("thumbsUp.GCReceiver = %+v", thumbsUp.GCReceiver)
	}

	heart, ok := byMsgID["def456"]
	if !ok {
		t.Fatal("missing heart reaction row")
	}
	if heart.GCSender.Valid {
		t.Errorf("expected heart.GCSender NULL, got %+v", heart.GCSender)
	}
	if heart.GCReceiver.Valid {
		t.Errorf("expected heart.GCReceiver NULL, got %+v", heart.GCReceiver)
	}
}

func TestGetPuppets(t *testing.T) {
	db := newFixtureSourceDB(t)
	ctx := context.Background()

	puppets, err := GetPuppets(ctx, db)
	if err != nil {
		t.Fatalf("GetPuppets: %v", err)
	}
	if len(puppets) != 2 {
		t.Fatalf("expected 2 puppets, got %d", len(puppets))
	}

	byGCID := make(map[string]*PythonPuppet, len(puppets))
	for _, p := range puppets {
		byGCID[p.GCID] = p
	}

	alice, ok := byGCID["103432744896036786916"]
	if !ok {
		t.Fatal("missing alice puppet row")
	}
	if !alice.Name.Valid || alice.Name.String != "Alice" {
		t.Errorf("alice.Name = %+v", alice.Name)
	}
	if !alice.PhotoHash.Valid || len(alice.PhotoHash.String) != 64 {
		t.Errorf("alice.PhotoHash = %+v, want 64-char hex", alice.PhotoHash)
	}
	if !alice.CustomMXID.Valid || alice.CustomMXID.String != "@alice:example.com" {
		t.Errorf("alice.CustomMXID = %+v", alice.CustomMXID)
	}
	if !alice.AccessToken.Valid || alice.AccessToken.String != "syt_abc_double_puppet_token" {
		t.Errorf("alice.AccessToken = %+v", alice.AccessToken)
	}

	bare, ok := byGCID["999999999999999999"]
	if !ok {
		t.Fatal("missing never-registered puppet row")
	}
	if bare.Name.Valid || bare.PhotoID.Valid || bare.PhotoHash.Valid {
		t.Errorf("expected bare puppet's optional string fields NULL, got name=%+v photo_id=%+v photo_hash=%+v", bare.Name, bare.PhotoID, bare.PhotoHash)
	}
	if bare.CustomMXID.Valid || bare.AccessToken.Valid {
		t.Errorf("expected bare puppet to have no double-puppet fields, got custom_mxid=%+v access_token=%+v", bare.CustomMXID, bare.AccessToken)
	}
	if bare.IsRegistered {
		t.Errorf("expected bare.IsRegistered = false")
	}
}

func TestGetUsers(t *testing.T) {
	db := newFixtureSourceDB(t)
	ctx := context.Background()

	users, err := GetUsers(ctx, db)
	if err != nil {
		t.Fatalf("GetUsers: %v", err)
	}
	if len(users) != 2 {
		t.Fatalf("expected 2 users, got %d", len(users))
	}

	byMXID := make(map[string]*PythonUser, len(users))
	for _, u := range users {
		byMXID[u.MXID] = u
	}

	loggedIn, ok := byMXID["@user1:example.com"]
	if !ok {
		t.Fatal("missing logged-in user row")
	}
	if !loggedIn.GCID.Valid || loggedIn.GCID.String != "103432744896036786916" {
		t.Errorf("loggedIn.GCID = %+v", loggedIn.GCID)
	}
	if !loggedIn.Cookies.Valid {
		t.Fatal("expected loggedIn.Cookies to be non-NULL")
	}
	if loggedIn.Cookies.String != `{"compass":"c1","ssid":"s1","sid":"si1","osid":"o1","hsid":"h1"}` {
		t.Errorf("loggedIn.Cookies = %q", loggedIn.Cookies.String)
	}
	if !loggedIn.Revision.Valid || loggedIn.Revision.Int64 != 42 {
		t.Errorf("loggedIn.Revision = %+v", loggedIn.Revision)
	}

	neverLoggedIn, ok := byMXID["@user2:example.com"]
	if !ok {
		t.Fatal("missing never-logged-in user row")
	}
	if neverLoggedIn.GCID.Valid {
		t.Errorf("expected neverLoggedIn.GCID NULL, got %+v", neverLoggedIn.GCID)
	}
	if neverLoggedIn.Cookies.Valid {
		t.Errorf("expected neverLoggedIn.Cookies NULL, got %+v", neverLoggedIn.Cookies)
	}
	if neverLoggedIn.Revision.Valid {
		t.Errorf("expected neverLoggedIn.Revision NULL, got %+v", neverLoggedIn.Revision)
	}
}

func TestReadSchemaVersion_NoVersionTable(t *testing.T) {
	db := newFixtureSourceDB(t) // fixture schema deliberately has no version table
	version, ok, err := ReadSchemaVersion(context.Background(), db)
	if err != nil {
		t.Fatalf("ReadSchemaVersion: %v", err)
	}
	if ok {
		t.Errorf("expected ok=false when source has no version table, got version=%d", version)
	}
}

func TestReadSchemaVersion_WithVersionTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source_versioned.db")
	setup, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("opening fixture source for setup: %v", err)
	}
	if _, err := setup.Exec(`CREATE TABLE version (version INTEGER)`); err != nil {
		t.Fatalf("creating version table: %v", err)
	}
	if _, err := setup.Exec(`INSERT INTO version (version) VALUES (10)`); err != nil {
		t.Fatalf("seeding version row: %v", err)
	}
	if err := setup.Close(); err != nil {
		t.Fatalf("closing fixture setup connection: %v", err)
	}

	db, err := OpenSource(path)
	if err != nil {
		t.Fatalf("OpenSource(%q): %v", path, err)
	}
	t.Cleanup(func() { _ = db.Close() })

	version, ok, err := ReadSchemaVersion(context.Background(), db)
	if err != nil {
		t.Fatalf("ReadSchemaVersion: %v", err)
	}
	if !ok || version != 10 {
		t.Errorf("ReadSchemaVersion = (%d, %v), want (10, true)", version, ok)
	}
}
