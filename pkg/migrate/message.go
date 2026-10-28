package migrate

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

// insertMigratedMessageQuery writes one Go `message` row; edit_count is NOT NULL so it is 0.
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

// messageGroupKey groups Python `message` rows by (gcid, gc_chat, gc_receiver).
type messageGroupKey struct {
	gcid     string
	chat     string
	receiver string
}

// groupMessagesByID groups rows by messageGroupKey; rows with a NULL/empty gcid are returned in skipped.
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

// isTextMsgType reports whether msgtype is m.text/m.notice; anything else, including NULL, is an attachment.
func isTextMsgType(msgtype sql.NullString) bool {
	return msgtype.Valid && (msgtype.String == "m.text" || msgtype.String == "m.notice")
}

// assignPartIDs maps each row's index to a part_id for one message group: a single row is "";
// otherwise a text first row is "" and the rest att_0.., else every row is att_k by index order.
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

// portalExistsInTarget guards message_room_fkey so a missing portal skips the row instead of aborting.
func portalExistsInTarget(ctx context.Context, target *dbutil.Database, id, receiver string) (bool, error) {
	var exists int
	err := target.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM portal WHERE id = $1 AND receiver = $2)`, id, receiver).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists != 0, nil
}

// ghostExistsInTarget guards the sender FKs; a gc_sender may have no puppet row in the source.
func ghostExistsInTarget(ctx context.Context, target *dbutil.Database, id string) (bool, error) {
	var exists int
	err := target.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ghost WHERE id = $1)`, id).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists != 0, nil
}

// messagePartExistsInTarget guards reaction_message_fkey.
func messagePartExistsInTarget(ctx context.Context, target *dbutil.Database, id, partID, receiver string) (bool, error) {
	var exists int
	err := target.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM message WHERE id = $1 AND part_id = $2 AND room_receiver = $3)`, id, partID, receiver).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists != 0, nil
}

// migrateMessages writes one Go `message` row per Python row (Go messages are per-part).
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

			// sender_id is NOT NULL with an FK to ghost; pre-v02 rows have no gc_sender.
			if !m.GCSender.Valid || m.GCSender.String == "" {
				warnings = append(warnings, fmt.Sprintf("message (gcid=%q, part_id=%q): NULL/empty gc_sender, skipping row (no ghost to attribute sender to)", key.gcid, partID))
				continue
			}

			senderID := gcid.MakeUserID(m.GCSender.String)

			ghostOK, err := ghostExistsInTarget(ctx, deps.Target, string(senderID))
			if err != nil {
				return count, warnings, fmt.Errorf("migrate: checking target ghost for message sender (gcid=%q, part_id=%q, sender=%q): %w", key.gcid, partID, m.GCSender.String, err)
			}
			if !ghostOK {
				warnings = append(warnings, fmt.Sprintf("message (gcid=%q, part_id=%q): sender %q has no migrated ghost, skipping orphan row", key.gcid, partID, m.GCSender.String))
				continue
			}

			senderMXID := deps.FormatGhostMXID(senderID)

			tsMicros := NormalizeTimestampMicros(m.Timestamp, version, versionKnown)
			// Stored as UnixNano, matching bridgev2's own round trip.
			ts := time.UnixMicro(tsMicros)

			parentID := nullStr(m.GCParentID)
			var threadRoot sql.NullString
			// TopicID is gc_parent_id, or the message's own gcid for a root message.
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
