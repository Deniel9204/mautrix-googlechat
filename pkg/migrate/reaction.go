package migrate

// migrateReactions implements the reaction half of the migration,
// replicating the source schema's reaction mapping field-by-field. Same
// raw-INSERT-through-ctx approach as message.go/portal.go/ghost.go.

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.mau.fi/util/variationselector"

	"github.com/Deniel9204/mautrix-googlechat/pkg/connector"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// insertMigratedReactionQuery writes one Go `reaction` row per schema map §3.
// Column order matches bridgev2/database/reaction.go's own
// upsertReactionQuery, just as a plain INSERT (a migrated target is always
// fresh -- see migrate.go's non-empty-target guard -- so there's never a
// conflict to upsert over).
const insertMigratedReactionQuery = `
	INSERT INTO reaction (
		bridge_id, message_id, message_part_id, sender_id, sender_mxid, emoji_id, emoji,
		room_id, room_receiver, mxid, timestamp, metadata
	) VALUES (
		'', $1, $2, $3, $4, $5, $6,
		$7, $8, $9, $10, $11
	)
`

// migrateReactions reads every row of the source Python `reaction` table and
// writes the corresponding Go `reaction` row, per schema map §3.
func migrateReactions(ctx context.Context, deps *Deps, opts Options) (int, []string, error) {
	version, versionKnown, err := ReadSchemaVersion(ctx, deps.Source)
	if err != nil {
		return 0, nil, fmt.Errorf("migrate: reading source schema version: %w", err)
	}

	messageRows, err := GetMessages(ctx, deps.Source)
	if err != nil {
		return 0, nil, fmt.Errorf("migrate: reading source messages (for message_part_id resolution): %w", err)
	}
	// Only the groups map is needed here (a lookup by (gcid, gc_chat,
	// gc_receiver)) -- the NULL-gcid warning is migrateMessages's job, not
	// this function's, so `skipped`/`order` are discarded.
	messageGroups, _, _ := groupMessagesByID(messageRows)

	reactions, err := GetReactions(ctx, deps.Source)
	if err != nil {
		return 0, nil, fmt.Errorf("migrate: reading source reactions: %w", err)
	}

	var warnings []string
	count := 0
	for _, r := range reactions {
		msgID := nullStr(r.GCMsgID)
		chat := nullStr(r.GCChat)
		receiver := nullStr(r.GCReceiver)
		senderStr := nullStr(r.GCSender)
		bareEmoji := nullStr(r.Emoji)

		if msgID == "" {
			warnings = append(warnings, fmt.Sprintf("reaction (mxid=%q): NULL/empty gc_msgid, skipping orphan reaction", r.MXID))
			continue
		}
		if senderStr == "" {
			warnings = append(warnings, fmt.Sprintf("reaction (mxid=%q, gc_msgid=%q): NULL/empty gc_sender, skipping row (no ghost to attribute sender to)", r.MXID, msgID))
			continue
		}
		if bareEmoji == "" {
			warnings = append(warnings, fmt.Sprintf("reaction (mxid=%q, gc_msgid=%q): NULL/empty emoji, skipping row", r.MXID, msgID))
			continue
		}

		// message_part_id: schema map §3 -- look up the Python message row
		// with (gcid=gc_msgid, gc_chat, gc_receiver, index=0) and apply §2's
		// index->part_id rule to THAT row's group. Do NOT hardcode "".
		key := messageGroupKey{gcid: msgID, chat: chat, receiver: receiver}
		groupRows, ok := messageGroups[key]
		if !ok {
			warnings = append(warnings, fmt.Sprintf("reaction (mxid=%q, gc_msgid=%q, gc_chat=%q, gc_receiver=%q): no matching message, skipping orphan reaction", r.MXID, msgID, chat, receiver))
			continue
		}
		partIDs := assignPartIDs(groupRows)
		partID, ok := partIDs[0]
		if !ok {
			warnings = append(warnings, fmt.Sprintf("reaction (mxid=%q, gc_msgid=%q): message group has no index=0 row, skipping orphan reaction", r.MXID, msgID))
			continue
		}

		// reaction_message_fkey guard -- see messagePartExistsInTarget's doc
		// comment (message.go). The SOURCE has a matching message row (the
		// lookup above succeeded), but migrateMessages may have skipped
		// writing it to the TARGET for its own reasons (missing sender,
		// missing portal, ...) -- without this check, inserting the
		// reaction would fail the FK and abort the entire migration instead
		// of being treated as an orphan reaction.
		msgExists, err := messagePartExistsInTarget(ctx, deps.Target, msgID, string(partID), receiver)
		if err != nil {
			return count, warnings, fmt.Errorf("migrate: checking target message for reaction (mxid=%q, gc_msgid=%q, part_id=%q): %w", r.MXID, msgID, partID, err)
		}
		if !msgExists {
			warnings = append(warnings, fmt.Sprintf("reaction (mxid=%q, gc_msgid=%q, part_id=%q): target message row was not migrated, skipping orphan reaction", r.MXID, msgID, partID))
			continue
		}

		// reaction_room_fkey guard. The message check above matches on
		// (id, part_id, room_receiver) and deliberately NOT on room_id, so a
		// gcid that appears under two different chats -- one migrated, one not
		// -- satisfies it via the MIGRATED chat's row while this reaction's own
		// portal is absent from the target. The insert below carries room_id,
		// so without this the FK fails and the error return aborts (and rolls
		// back) the ENTIRE migration over one orphan row, instead of skipping
		// it the way every other dangling-reference case here does.
		portalOK, err := portalExistsInTarget(ctx, deps.Target, chat, receiver)
		if err != nil {
			return count, warnings, fmt.Errorf("migrate: checking target portal for reaction (mxid=%q, gc_msgid=%q, gc_chat=%q): %w", r.MXID, msgID, chat, err)
		}
		if !portalOK {
			warnings = append(warnings, fmt.Sprintf("reaction (mxid=%q, gc_msgid=%q, gc_chat=%q, gc_receiver=%q): target portal row was not migrated, skipping orphan reaction", r.MXID, msgID, chat, receiver))
			continue
		}

		senderID := gcid.MakeUserID(senderStr)

		// reaction_sender_fkey guard -- see ghostExistsInTarget's doc comment.
		ghostOK, err := ghostExistsInTarget(ctx, deps.Target, string(senderID))
		if err != nil {
			return count, warnings, fmt.Errorf("migrate: checking target ghost for reaction sender (mxid=%q, gc_msgid=%q, sender=%q): %w", r.MXID, msgID, senderStr, err)
		}
		if !ghostOK {
			warnings = append(warnings, fmt.Sprintf("reaction (mxid=%q, gc_msgid=%q): sender %q has no migrated ghost, skipping orphan reaction", r.MXID, msgID, senderStr))
			continue
		}

		// sender_mxid: same FormatGhostMXID rule as message.sender_mxid
		// (m7-migration-preflight.md item 2; schema map §3 recommends doing
		// this "since it's cheap once the Message-side helper exists").
		senderMXID := deps.FormatGhostMXID(senderID)

		// Emoji split (schema map §3/Transformations #9): Python's stored
		// emoji is always the bare, variation-selector-stripped form ->
		// emoji_id (identity copy); emoji (display form) is computed via
		// variationselector.Add.
		emojiDisplay := variationselector.Add(bareEmoji)

		tsMicros := NormalizeTimestampMicros(r.Timestamp, version, versionKnown)
		ts := time.UnixMicro(tsMicros)

		// ReactionMetadata.TopicID is deliberately left empty (schema map §3:
		// "cheapest-correct choice; the fallback path exists precisely for
		// this case").
		metaJSON, err := json.Marshal(&connector.ReactionMetadata{})
		if err != nil {
			return count, warnings, fmt.Errorf("migrate: marshaling reaction metadata (gc_msgid=%q): %w", msgID, err)
		}

		_, err = deps.Target.Exec(ctx, insertMigratedReactionQuery,
			msgID, string(partID), string(senderID), string(senderMXID), bareEmoji, emojiDisplay,
			chat, receiver, r.MXID, ts.UnixNano(), string(metaJSON),
		)
		if err != nil {
			return count, warnings, fmt.Errorf("migrate: inserting reaction (gc_msgid=%q, part_id=%q, sender=%q): %w", msgID, partID, senderStr, err)
		}
		count++
	}
	return count, warnings, nil
}
