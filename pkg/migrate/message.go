package migrate

// migrateMessages implements the message half of the migration, replicating
// the field-by-field mapping exactly. Same raw-INSERT-through-ctx approach
// as portal.go/ghost.go -- see that file's package doc comment.
//
// The index->part_id rule is implemented once, in assignPartIDs, and shared
// with reaction.go's message_part_id lookup so both entry points agree on
// exactly the same numbering for the same group of Python rows.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"github.com/Deniel9204/mautrix-googlechat/pkg/connector"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// insertMigratedMessageQuery writes one Go `message` row. Columns with no
// Python source (double_puppeted, reply_to_id, reply_to_part_id,
// send_txn_id) are hardcoded NULL; edit_count has no Python source either
// but MUST be NOT NULL, so it's hardcoded 0 (no historical count exists, so
// every migrated row defaults to 0). bridge_id is always ” (see portal.go's
// identical comment).
const insertMigratedMessageQuery = `
	INSERT INTO message (
		bridge_id, id, part_id, mxid,
		room_id, room_receiver, sender_id, sender_mxid,
		timestamp, edit_count, double_puppeted,
		thread_root_id, reply_to_id, reply_to_part_id, send_txn_id,
		metadata
	) VALUES (
		'', $1, $2, $3,
		$4, $5, $6, $7,
		$8, 0, NULL,
		$9, NULL, NULL, NULL,
		$10
	)
`

// messageGroupKey is the composite key the index->part_id rule groups Python
// `message` rows by: "(gcid, gc_chat, gc_receiver)". Rows with a NULL/empty
// gcid have no valid Go message.id to group under at all.
type messageGroupKey struct {
	gcid     string
	chat     string
	receiver string
}

// groupMessagesByID groups rows the way the index->part_id rule requires.
// Rows with a NULL/empty gcid are returned separately in skipped, rather
// than silently dropped, so callers that care (migrateMessages) can warn
// about them; migrateReactions's lookup doesn't need to.
func groupMessagesByID(rows []*PythonMessage) (groups map[messageGroupKey][]*PythonMessage, order []messageGroupKey, skipped []*PythonMessage) {
	groups = make(map[messageGroupKey][]*PythonMessage)
	for _, m := range rows {
		if !m.GCID.Valid || m.GCID.String == "" {
			skipped = append(skipped, m)
			continue
		}
		key := messageGroupKey{gcid: m.GCID.String, chat: m.GCChat, receiver: nullStr(m.GCReceiver)}
		if _, ok := groups[key]; !ok {
			order = append(order, key)
		}
		groups[key] = append(groups[key], m)
	}
	return groups, order, skipped
}

// isTextMsgType reports whether msgtype classifies a Python message row as
// the text/notice part of a message ("m.text"/"m.notice" -> text; anything
// else, including NULL -> attachment).
func isTextMsgType(msgtype sql.NullString) bool {
	return msgtype.Valid && (msgtype.String == "m.text" || msgtype.String == "m.notice")
}

// assignPartIDs implements the full index->part_id rule for one
// (gcid, gc_chat, gc_receiver) group of Python rows, keyed by each row's own
// `index` (unique within the group -- part of Python's composite PK):
//
//  1. Group size 1: part_id = "" (gcid.TextPartID), regardless of msgtype.
//  2. Group size >= 2: the lowest-index row is "first".
//     - If its msgtype is m.text/m.notice: that row -> "", every OTHER row
//     -> att_0, att_1, ... in ascending index order among the remainder
//     (0-based, after removing the text row).
//     - Otherwise (no text body at all): every row is an attachment; the
//     row at rank k (0-based, ascending index) -> att_k.
//
// rows need not be pre-sorted; this function sorts its own copy by Index
// ascending and never mutates the caller's slice.
func assignPartIDs(rows []*PythonMessage) map[int]networkid.PartID {
	sorted := make([]*PythonMessage, len(rows))
	copy(sorted, rows)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Index < sorted[j].Index })

	result := make(map[int]networkid.PartID, len(sorted))
	if len(sorted) == 0 {
		return result
	}
	if len(sorted) == 1 {
		result[sorted[0].Index] = gcid.TextPartID
		return result
	}

	first := sorted[0]
	if isTextMsgType(first.MsgType) {
		result[first.Index] = gcid.TextPartID
		for k, m := range sorted[1:] {
			result[m.Index] = gcid.MakeAttachmentPartID(k)
		}
	} else {
		for k, m := range sorted {
			result[m.Index] = gcid.MakeAttachmentPartID(k)
		}
	}
	return result
}

// portalExistsInTarget reports whether the Go `portal` row (id, receiver)
// has already been written to the target -- used to guard
// message.message_room_fkey. A migrated source can legitimately reference a
// chat whose portal row migratePortals itself skipped (e.g. an unparseable
// gcid, portal.go's own warn-and-skip path); without this check, inserting
// such a message would fail the FK and abort the ENTIRE migration (all
// entities, not just this row) instead of warning and skipping just this
// one -- the hard rule every other per-row problem in this package already
// follows.
func portalExistsInTarget(ctx context.Context, target *dbutil.Database, id, receiver string) (bool, error) {
	var exists int
	err := target.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM portal WHERE id = $1 AND receiver = $2)`, id, receiver).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists != 0, nil
}

// ghostExistsInTarget reports whether the Go `ghost` row (id) has already
// been written to the target -- used to guard message.message_sender_fkey
// and reaction.reaction_sender_fkey. A source row can reference a gc_sender
// gaia ID with no corresponding `puppet` row in the source DB at all (e.g.
// a system/former-member sender never cached as a puppet) -- same
// abort-vs-skip concern as portalExistsInTarget.
func ghostExistsInTarget(ctx context.Context, target *dbutil.Database, id string) (bool, error) {
	var exists int
	err := target.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ghost WHERE id = $1)`, id).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists != 0, nil
}

// messagePartExistsInTarget reports whether the Go `message` row
// (id, part_id, room_receiver) has already been written to the target --
// used by migrateReactions to guard reaction.reaction_message_fkey. A
// reaction's target message row can have been skipped by migrateMessages
// for its own reasons (missing sender, missing portal, ...); without this
// check, inserting the reaction would fail the FK and abort the entire
// migration instead of being treated as an orphan reaction.
func messagePartExistsInTarget(ctx context.Context, target *dbutil.Database, id, partID, receiver string) (bool, error) {
	var exists int
	err := target.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM message WHERE id = $1 AND part_id = $2 AND room_receiver = $3)`, id, partID, receiver).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists != 0, nil
}

// migrateMessages reads every row of the source Python `message` table and
// writes the corresponding Go `message` row(s) -- one Go row per Python row,
// since Go's `message` table is per-PART.
func migrateMessages(ctx context.Context, deps *Deps, opts Options) (int, []string, error) {
	version, versionKnown, err := ReadSchemaVersion(ctx, deps.Source)
	if err != nil {
		return 0, nil, fmt.Errorf("migrate: reading source schema version: %w", err)
	}

	rows, err := GetMessages(ctx, deps.Source)
	if err != nil {
		return 0, nil, fmt.Errorf("migrate: reading source messages: %w", err)
	}

	groups, order, skipped := groupMessagesByID(rows)

	var warnings []string
	for _, m := range skipped {
		warnings = append(warnings, fmt.Sprintf("message (mxid=%q, mx_room=%q): NULL/empty gcid, skipping row", m.MXID, m.MXRoom))
	}

	count := 0
	for _, key := range order {
		groupRows := groups[key]

		// message_room_fkey guard -- see portalExistsInTarget's doc comment.
		portalOK, err := portalExistsInTarget(ctx, deps.Target, key.chat, key.receiver)
		if err != nil {
			return count, warnings, fmt.Errorf("migrate: checking target portal for message group (gcid=%q, chat=%q, receiver=%q): %w", key.gcid, key.chat, key.receiver, err)
		}
		if !portalOK {
			warnings = append(warnings, fmt.Sprintf("message group (gcid=%q, chat=%q, receiver=%q): target portal was not migrated, skipping %d row(s)", key.gcid, key.chat, key.receiver, len(groupRows)))
			continue
		}

		partIDs := assignPartIDs(groupRows)

		for _, m := range groupRows {
			partID := partIDs[m.Index]

			// sender_id/sender_mxid have no fallback: Go's message.sender_id
			// is NOT NULL with an FK to ghost(id), so a row with no known
			// sender can never be written correctly -- warn and skip just
			// this row (the gc_sender column is nullable; this happens for
			// pre-v02 rows that predate the column).
			if !m.GCSender.Valid || m.GCSender.String == "" {
				warnings = append(warnings, fmt.Sprintf("message (gcid=%q, part_id=%q): NULL/empty gc_sender, skipping row (no ghost to attribute sender to)", key.gcid, partID))
				continue
			}

			senderID := gcid.MakeUserID(m.GCSender.String)

			// message_sender_fkey guard -- see ghostExistsInTarget's doc comment.
			ghostOK, err := ghostExistsInTarget(ctx, deps.Target, string(senderID))
			if err != nil {
				return count, warnings, fmt.Errorf("migrate: checking target ghost for message sender (gcid=%q, part_id=%q, sender=%q): %w", key.gcid, partID, m.GCSender.String, err)
			}
			if !ghostOK {
				warnings = append(warnings, fmt.Sprintf("message (gcid=%q, part_id=%q): sender %q has no migrated ghost, skipping orphan row", key.gcid, partID, m.GCSender.String))
				continue
			}

			// sender_mxid: use the bridge's OWN FormatGhostMXID (wired
			// through Deps), never a reimplementation of its username
			// template.
			senderMXID := deps.FormatGhostMXID(senderID)

			tsMicros := NormalizeTimestampMicros(m.Timestamp, version, versionKnown)
			// message.timestamp is stored as time.Time.UnixNano() (see
			// bridgev2/database/message.go's sqlVariables/Scan round trip:
			// `m.Timestamp.UnixNano()` / `time.Unix(0, timestamp)`) -- build
			// the same time.Time from the normalized microsecond value and
			// bind its UnixNano() form, so a migrated row round-trips
			// identically to one a live bridge wrote.
			ts := time.UnixMicro(tsMicros)

			parentID := nullStr(m.GCParentID)
			var threadRoot sql.NullString
			// MessageMetadata.TopicID: gc_parent_id if non-empty, else the
			// message's own gcid (self-reference for a root/standalone
			// message) -- schema map §2's TopicID rule.
			topicID := parentID
			if parentID != "" {
				threadRoot = sql.NullString{String: parentID, Valid: true}
			} else {
				topicID = key.gcid
			}

			meta := &connector.MessageMetadata{
				TimestampMicro: tsMicros,
				TopicID:        topicID,
			}
			metaJSON, err := json.Marshal(meta)
			if err != nil {
				return count, warnings, fmt.Errorf("migrate: marshaling message metadata (gcid=%q, part_id=%q): %w", key.gcid, partID, err)
			}

			_, err = deps.Target.Exec(ctx, insertMigratedMessageQuery,
				key.gcid, string(partID), m.MXID,
				key.chat, key.receiver, string(senderID), string(senderMXID),
				ts.UnixNano(),
				threadRoot,
				string(metaJSON),
			)
			if err != nil {
				return count, warnings, fmt.Errorf("migrate: inserting message (gcid=%q, part_id=%q): %w", key.gcid, partID, err)
			}
			count++
		}
	}
	return count, warnings, nil
}
