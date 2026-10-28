// Package migrate imports a Python mautrix-googlechat database into the Go bridge.
package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"

	// Postgres driver for reading the source DB standalone.
	_ "github.com/lib/pq"
	// Plain sqlite3 driver, not sqlite3-fk-wal (see readOnlyDSN).
	_ "github.com/mattn/go-sqlite3"

	"go.mau.fi/util/dbutil"
)

// OpenSource opens the Python bridge database read-only. dsn is a SQLite path
// (optionally prefixed "sqlite:") or a postgres:// / postgresql:// URL.
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

// readOnlyDSN returns the driver and a read-only URI for the source DSN.
// SQLite uses mode=ro&immutable=1 so WAL databases don't get -wal/-shm files created;
// Postgres uses default_transaction_read_only=on (best-effort).
func readOnlyDSN(dsn string) (driver, uri string, err error) {
	switch {
	case dsn == "":
		return "", "", errors.New("migrate: source DSN must not be empty")
	case strings.HasPrefix(dsn, "postgres://"), strings.HasPrefix(dsn, "postgresql://"):
		return "postgres", appendConnParam(dsn, "default_transaction_read_only", "on"), nil
	case strings.HasPrefix(dsn, "sqlite:"):
		return "sqlite3", sqliteReadOnlyURI(strings.TrimPrefix(dsn, "sqlite:")), nil
	default:
		// Bare filesystem path, as in the Python config.
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

// appendConnParam appends key=value to a Postgres URL or keyword/value DSN.
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

// ReadSchemaVersion reads the source `version` table; ok=false if it is missing or unreadable.
func ReadSchemaVersion(ctx context.Context, db *dbutil.Database) (version int, ok bool, err error) {
	row := db.QueryRow(ctx, `SELECT version FROM version LIMIT 1`)
	scanErr := row.Scan(&version)
	if scanErr != nil {
		if errors.Is(scanErr, sql.ErrNoRows) {
			return 0, false, nil
		}
		// Missing or malformed table is not fatal.
		return 0, false, nil
	}
	return version, true, nil
}

// PythonPortal is one row of the Python bridge's `portal` table.
type PythonPortal struct {
	GCID           string // -> Go portal.id.
	GCReceiver     string // PK part 2. Identity copy -> Go portal.receiver.
	OtherUserID    sql.NullString
	MXID           sql.NullString
	Name           sql.NullString
	AvatarMXC      sql.NullString
	Description    sql.NullString // -> Go portal.topic.
	NameSet        bool
	AvatarSet      bool
	DescriptionSet bool // -> Go portal.topic_set.
	Encrypted      bool // Dropped.
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

// PythonMessage is one row of the Python bridge's `message` table.
type PythonMessage struct {
	MXID       string
	MXRoom     string
	GCID       sql.NullString // -> Go message.id.
	GCChat     string         // -> Go message.room_id.
	GCReceiver sql.NullString // -> Go message.room_receiver.
	GCParentID sql.NullString // -> Go message.thread_root_id.
	GCSender   sql.NullString // -> Go message.sender_id via gcid.MakeUserID.
	Index      int            // See assignPartIDs.
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

// PythonReaction is one row of the Python bridge's `reaction` table.
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

// PythonPuppet is one row of the Python bridge's `puppet` table.
type PythonPuppet struct {
	GCID           string // PK. Identity copy (raw Gaia ID) -> Go ghost.id via gcid.MakeUserID.
	Name           sql.NullString
	PhotoID        sql.NullString // raw avatar URL -> Go ghost.avatar_id.
	PhotoMXC       sql.NullString
	PhotoHash      sql.NullString // sha256 hex -> Go ghost.avatar_hash (direct hex copy).
	NameSet        bool
	AvatarSet      bool
	ContactInfoSet bool
	IsRegistered   bool           // Dropped.
	CustomMXID     sql.NullString // Double-puppet target Matrix user.
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

// PythonUser is one row of the Python bridge's `user` table; it splits into Go `user` + `user_login`.
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
