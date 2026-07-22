// Package migrate implements the Python mautrix-googlechat -> Go bridgev2
// database migration tool. It is a standalone CLI-facing package, not part
// of the bridge's runtime: unlike pkg/gchatmeow (never imports bridgev2) and
// pkg/connector (never does HTTP), pkg/migrate is explicitly allowed to
// import bridgev2/dbutil/gcid, since it is a one-shot operator tool, not the
// live client or connector.
//
// This package implements the source schema's mapping field-by-field. Four
// pre-flight decisions are baked in throughout: cookie key casing,
// sender_mxid, double-puppet token, and ms-vs-µs timestamps.
//
// source.go reads the SOURCE Python bridge's database. It NEVER writes to
// it -- opening it is always read-only (see OpenSource) regardless of
// DryRun/Force, because the source DB is somebody's only copy of their
// message history until the migration is verified.
package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"

	// Registers the "postgres" database/sql driver used by dbutil's
	// Postgres dialect -- the same driver bridgev2/matrix.Connector
	// blank-imports (connector.go), reused here so pkg/migrate can open a
	// source DB standing alone, without the rest of the bridge running.
	_ "github.com/lib/pq"
	// Registers the plain "sqlite3" database/sql driver (NOT the bridge's
	// "sqlite3-fk-wal" variant -- see readOnlyDSN's doc comment for why).
	_ "github.com/mattn/go-sqlite3"

	"go.mau.fi/util/dbutil"
)

// OpenSource opens the SOURCE Python mautrix-googlechat database in
// READ-ONLY mode and returns it wrapped in the same *dbutil.Database type
// bridgev2 itself uses, so the rest of the migration tool (and any test
// fixture) can use the ordinary Query/QueryRow/Exec helpers.
//
// dsn is either:
//   - a SQLite path, optionally prefixed "sqlite:" (matching the Python
//     bridge's own example-config.yaml `database:` field, which documents
//     both "sqlite:filename.db" and a bare path as valid), or
//   - a "postgres://" or "postgresql://" URL.
//
// The connection is opened read-only in both cases; see readOnlyDSN.
func OpenSource(dsn string) (*dbutil.Database, error) {
	driver, uri, err := readOnlyDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := dbutil.NewWithDialect(uri, driver)
	if err != nil {
		return nil, fmt.Errorf("migrate: opening source database: %w", err)
	}
	return db, nil
}

// readOnlyDSN detects the SOURCE DSN's backend (SQLite file vs. Postgres URL)
// and returns the database/sql driver name plus a connection URI that opens
// the connection read-only.
//
// SQLite: uses the plain "sqlite3" driver (github.com/mattn/go-sqlite3's own
// registered name), with the "mode=ro" AND "immutable=1" URI query
// parameters. Deliberately NOT the bridge's own "sqlite3-fk-wal" driver
// (registered by go.mau.fi/util/dbutil/litestream, and what
// mxmain.BridgeMain's initDB uses for the TARGET database): that variant's
// ConnectHook unconditionally runs `PRAGMA journal_mode = WAL`, which --
// unlike a plain read -- can itself write to the database file/create
// -wal/-shm files. `mode=ro` alone is NOT sufficient to prevent this for a
// WAL-mode database (the Python bridge's default journal mode, per
// mautrix.util.async_db's own SQLite pragma setup): opening a cleanly
// checkpointed WAL-mode file with only mode=ro still creates fresh, empty
// `-wal`/`-shm` sidecar files in the source directory on first read --
// reproduced directly against this driver. `immutable=1` tells SQLite the
// file (and its directory) will not change for the duration of the
// connection, which skips that WAL-related bookkeeping entirely and avoids
// creating those files -- verified to still read correctly, and to keep
// working even when the source directory itself is read-only on disk,
// unlike mode=ro alone (which fails outright against a read-only directory
// because it still attempts the WAL setup). The source DB must never be
// mutated (hard rule), so this uses the vanilla driver with both of these
// explicit read-only/immutable parameters instead.
//
// Postgres: appends `default_transaction_read_only=on` to the connection
// string. This is best-effort, not an OS-level guarantee like SQLite's
// mode=ro/immutable=1 -- Postgres has no equivalent of opening a file read-
// only, so this relies on the server honoring the run-time parameter for
// the session.
func readOnlyDSN(dsn string) (driver, uri string, err error) {
	switch {
	case dsn == "":
		return "", "", errors.New("migrate: source DSN must not be empty")
	case strings.HasPrefix(dsn, "postgres://"), strings.HasPrefix(dsn, "postgresql://"):
		return "postgres", appendConnParam(dsn, "default_transaction_read_only", "on"), nil
	case strings.HasPrefix(dsn, "sqlite:"):
		return "sqlite3", sqliteReadOnlyURI(strings.TrimPrefix(dsn, "sqlite:")), nil
	default:
		// Bare filesystem path -- same as the Python bridge's own config.
		return "sqlite3", sqliteReadOnlyURI(dsn), nil
	}
}

func sqliteReadOnlyURI(path string) string {
	uri := path
	if !strings.HasPrefix(uri, "file:") {
		uri = "file:" + uri
	}
	sep := "?"
	if strings.Contains(uri, "?") {
		sep = "&"
	}
	return uri + sep + "mode=ro&immutable=1"
}

// appendConnParam appends a key=value pair to a Postgres connection string,
// handling both the URL form (postgres://...?a=b) and the legacy
// space-separated keyword form (host=... dbname=...).
func appendConnParam(dsn, key, value string) string {
	if strings.Contains(dsn, "://") {
		sep := "?"
		if strings.Contains(dsn, "?") {
			sep = "&"
		}
		return fmt.Sprintf("%s%s%s=%s", dsn, sep, key, url.QueryEscape(value))
	}
	return fmt.Sprintf("%s %s=%s", dsn, key, value)
}

// ReadSchemaVersion reads the SOURCE database's schema-version bookkeeping
// table, mirroring mautrix.util.async_db's convention (a single-row table,
// standard name "version", integer "version" column -- the same convention
// go.mau.fi/util/dbutil's own Go-side Database uses, see
// dbutil.Database.VersionTable's default). This is the table
// m7-migration-preflight.md item 4 says to consult, if present, to decide
// whether message/reaction timestamps need the historical *1000 (ms->µs)
// conversion (only for a source schema version < 10).
//
// Per the preflight note, a missing/differently-shaped version table is NOT
// fatal: ok=false tells the caller to assume the common case (a DB that has
// been started against the current Python bridge codebase at least once,
// which is always already at v10+, i.e. already µs).
func ReadSchemaVersion(ctx context.Context, db *dbutil.Database) (version int, ok bool, err error) {
	row := db.QueryRow(ctx, `SELECT version FROM version LIMIT 1`)
	scanErr := row.Scan(&version)
	if scanErr != nil {
		if errors.Is(scanErr, sql.ErrNoRows) {
			return 0, false, nil
		}
		// Table doesn't exist, wrong shape, etc. -- treat as "unknown",
		// not fatal (this table is optional bookkeeping, not something
		// the migration depends on for correctness beyond the ms/µs
		// decision, which itself defaults safely -- see
		// migrate.TimestampsAreMicroseconds).
		return 0, false, nil
	}
	return version, true, nil
}

// --- Typed rows for each Python entity --------------------------------

// PythonPortal is one row of the Python bridge's `portal` table (final
// schema: v00 baseline + v03 revision/is_threaded + v05
// rename+threads_enabled + v06 description/description_set). See
// m7-migration-schema-map.md §1 for the full column-by-column mapping to Go.
type PythonPortal struct {
	GCID           string // PK part 1. Identity copy -> Go portal.id (see schema map §0).
	GCReceiver     string // PK part 2. Identity copy -> Go portal.receiver.
	OtherUserID    sql.NullString
	MXID           sql.NullString
	Name           sql.NullString
	AvatarMXC      sql.NullString
	Description    sql.NullString // -> Go portal.topic (schema map §1).
	NameSet        bool
	AvatarSet      bool
	DescriptionSet bool // -> Go portal.topic_set.
	Encrypted      bool // Dropped on the Go side -- see schema map §1.
	Revision       sql.NullInt64
	ThreadsOnly    sql.NullBool
	ThreadsEnabled sql.NullBool
}

// GetPortals enumerates every row of the source `portal` table.
func GetPortals(ctx context.Context, db *dbutil.Database) ([]*PythonPortal, error) {
	rows, err := db.Query(ctx, `
		SELECT gcid, gc_receiver, other_user_id, mxid, name, avatar_mxc, description,
		       name_set, avatar_set, description_set, encrypted, revision,
		       threads_only, threads_enabled
		FROM portal
	`)
	if err != nil {
		return nil, fmt.Errorf("migrate: querying source portals: %w", err)
	}
	defer rows.Close()

	var out []*PythonPortal
	for rows.Next() {
		p := &PythonPortal{}
		if err := rows.Scan(
			&p.GCID, &p.GCReceiver, &p.OtherUserID, &p.MXID, &p.Name, &p.AvatarMXC, &p.Description,
			&p.NameSet, &p.AvatarSet, &p.DescriptionSet, &p.Encrypted, &p.Revision,
			&p.ThreadsOnly, &p.ThreadsEnabled,
		); err != nil {
			return nil, fmt.Errorf("migrate: scanning source portal row: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// PythonMessage is one row of the Python bridge's `message` table (final
// schema: v00 baseline + v02 msgtype/gc_sender; v10 only changes timestamp
// *semantics*, not columns). See m7-migration-schema-map.md §2 -- THE HARD
// ONE -- for the index->part_id derivation Task 6 must implement on top of
// this raw row.
type PythonMessage struct {
	MXID       string
	MXRoom     string
	GCID       sql.NullString // -> Go message.id.
	GCChat     string         // -> Go message.room_id (schema map §0).
	GCReceiver sql.NullString // -> Go message.room_receiver.
	GCParentID sql.NullString // -> Go message.thread_root_id.
	GCSender   sql.NullString // -> Go message.sender_id via gcid.MakeUserID.
	Index      int            // PK part -- see schema map §2's index->part_id rule.
	Timestamp  int64          // ms pre-v10, µs from v10 on -- see TimestampsAreMicroseconds.
	MsgType    sql.NullString // Classifier only (text vs. attachment), not copied to Go.
}

// GetMessages enumerates every row of the source `message` table.
func GetMessages(ctx context.Context, db *dbutil.Database) ([]*PythonMessage, error) {
	rows, err := db.Query(ctx, `
		SELECT mxid, mx_room, gcid, gc_chat, gc_receiver, gc_parent_id, gc_sender,
		       "index", timestamp, msgtype
		FROM message
	`)
	if err != nil {
		return nil, fmt.Errorf("migrate: querying source messages: %w", err)
	}
	defer rows.Close()

	var out []*PythonMessage
	for rows.Next() {
		m := &PythonMessage{}
		if err := rows.Scan(
			&m.MXID, &m.MXRoom, &m.GCID, &m.GCChat, &m.GCReceiver, &m.GCParentID, &m.GCSender,
			&m.Index, &m.Timestamp, &m.MsgType,
		); err != nil {
			return nil, fmt.Errorf("migrate: scanning source message row: %w", err)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// PythonReaction is one row of the Python bridge's `reaction` table (v02
// baseline; v10 changes timestamp semantics only). The FK-only `_index`
// column is not read here: it is always the hardcoded literal 0 (schema map
// §3), never part of the dataclass's own columns.
type PythonReaction struct {
	MXID       string
	MXRoom     string
	Emoji      sql.NullString // bare form -> Go emoji_id; variationselector.Add(...) -> Go emoji.
	GCSender   sql.NullString
	GCMsgID    sql.NullString // -> Go reaction.message_id.
	GCChat     sql.NullString // -> Go reaction.room_id.
	GCReceiver sql.NullString // -> Go reaction.room_receiver.
	Timestamp  int64
}

// GetReactions enumerates every row of the source `reaction` table.
func GetReactions(ctx context.Context, db *dbutil.Database) ([]*PythonReaction, error) {
	rows, err := db.Query(ctx, `
		SELECT mxid, mx_room, emoji, gc_sender, gc_msgid, gc_chat, gc_receiver, timestamp
		FROM reaction
	`)
	if err != nil {
		return nil, fmt.Errorf("migrate: querying source reactions: %w", err)
	}
	defer rows.Close()

	var out []*PythonReaction
	for rows.Next() {
		r := &PythonReaction{}
		if err := rows.Scan(
			&r.MXID, &r.MXRoom, &r.Emoji, &r.GCSender, &r.GCMsgID, &r.GCChat, &r.GCReceiver,
			&r.Timestamp,
		); err != nil {
			return nil, fmt.Errorf("migrate: scanning source reaction row: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// PythonPuppet is one row of the Python bridge's `puppet` table (v00
// baseline + v04 photo_hash + v07 contact_info_set). See
// m7-migration-schema-map.md §4 for the mapping to Go `ghost`, and
// m7-migration-preflight.md item 3 for why custom_mxid/access_token are NOT
// simply dropped (they carry the double-puppet token forward to Go's
// user.access_token).
type PythonPuppet struct {
	GCID           string // PK. Identity copy (raw Gaia ID) -> Go ghost.id via gcid.MakeUserID.
	Name           sql.NullString
	PhotoID        sql.NullString // raw avatar URL -> Go ghost.avatar_id.
	PhotoMXC       sql.NullString
	PhotoHash      sql.NullString // sha256 hex -> Go ghost.avatar_hash (direct hex copy).
	NameSet        bool
	AvatarSet      bool
	ContactInfoSet bool
	IsRegistered   bool           // Dropped on the Go side -- see schema map §4.
	CustomMXID     sql.NullString // Double-puppet target Matrix user -- see preflight item 3.
	AccessToken    sql.NullString // Double-puppet token -> Go user.access_token for that mxid.
	NextBatch      sql.NullString // No Go equivalent -- dropped.
	BaseURL        sql.NullString // No Go equivalent -- dropped.
}

// GetPuppets enumerates every row of the source `puppet` table.
func GetPuppets(ctx context.Context, db *dbutil.Database) ([]*PythonPuppet, error) {
	rows, err := db.Query(ctx, `
		SELECT gcid, name, photo_id, photo_mxc, photo_hash, name_set, avatar_set,
		       contact_info_set, is_registered, custom_mxid, access_token, next_batch, base_url
		FROM puppet
	`)
	if err != nil {
		return nil, fmt.Errorf("migrate: querying source puppets: %w", err)
	}
	defer rows.Close()

	var out []*PythonPuppet
	for rows.Next() {
		p := &PythonPuppet{}
		if err := rows.Scan(
			&p.GCID, &p.Name, &p.PhotoID, &p.PhotoMXC, &p.PhotoHash, &p.NameSet, &p.AvatarSet,
			&p.ContactInfoSet, &p.IsRegistered, &p.CustomMXID, &p.AccessToken, &p.NextBatch, &p.BaseURL,
		); err != nil {
			return nil, fmt.Errorf("migrate: scanning source puppet row: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// PythonUser is one row of the Python bridge's `user` table (v00 baseline +
// v03 revision + v08 cookies rename + v09 user_agent). See
// m7-migration-schema-map.md §5: this single row splits into Go's `user` +
// `user_login` (only if Cookies is non-NULL).
type PythonUser struct {
	MXID       string         // PK. -> Go user.mxid AND user_login.user_mxid.
	GCID       sql.NullString // -> Go user_login.id via gcid.MakeUserLoginID. Skip row if NULL (never logged in).
	Cookies    sql.NullString // JSON object, LOWERCASE keys -- see UppercaseCookieKeys. Skip UserLogin if NULL.
	UserAgent  sql.NullString
	NoticeRoom sql.NullString // -> Go user.management_room.
	Revision   sql.NullInt64  // -> Go UserLoginMetadata.Revision.
}

// GetUsers enumerates every row of the source `user` table.
func GetUsers(ctx context.Context, db *dbutil.Database) ([]*PythonUser, error) {
	rows, err := db.Query(ctx, `
		SELECT mxid, gcid, cookies, user_agent, notice_room, revision
		FROM "user"
	`)
	if err != nil {
		return nil, fmt.Errorf("migrate: querying source users: %w", err)
	}
	defer rows.Close()

	var out []*PythonUser
	for rows.Next() {
		u := &PythonUser{}
		if err := rows.Scan(
			&u.MXID, &u.GCID, &u.Cookies, &u.UserAgent, &u.NoticeRoom, &u.Revision,
		); err != nil {
			return nil, fmt.Errorf("migrate: scanning source user row: %w", err)
		}
		out = append(out, u)
	}
	return out, rows.Err()
}
