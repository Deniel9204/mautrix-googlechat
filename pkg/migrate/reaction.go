package migrate

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.mau.fi/util/variationselector"

	"github.com/Deniel9204/mautrix-googlechat/pkg/connector"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// insertMigratedReactionQuery writes one Go `reaction` row; plain INSERT since the target is always fresh.
const insertMigratedReactionQuery = `
	INSERT INTO reaction (
		bridge_id, message_id, message_part_id, sender_id, sender_mxid, emoji_id, emoji,
		room_id, room_receiver, mxid, timestamp, metadata
	) VALUES (
		'', $1, $2, $3, $4, $5, $6,
		$7, $8, $9, $10, $11
	)
`

// migrateReactions writes one Go `reaction` row per Python `reaction` row.
func migrateReactions(ctx context.Context, deps *Deps, opts Options) (int, []string, error) {
	version, versionKnown, err := ReadSchemaVersion(ctx, deps.Source)
	if err != nil {
		return 0, nil, fmt.Errorf("migrate: reading source schema version: %w", err)
	}

	messageRows, err := GetMessages(ctx, deps.Source)
	if err != nil {
		return 0, nil, fmt.Errorf("migrate: reading source messages (for message_part_id resolution): %w", err)
	}

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

		// message_part_id comes from the index=0 row's part_id rule; never hardcode "".
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

		// The source message may exist but not have been migrated.
		msgExists, err := messagePartExistsInTarget(ctx, deps.Target, msgID, string(partID), receiver)
		if err != nil {
			return count, warnings, fmt.Errorf("migrate: checking target message for reaction (mxid=%q, gc_msgid=%q, part_id=%q): %w", r.MXID, msgID, partID, err)
		}
		if !msgExists {
			warnings = append(warnings, fmt.Sprintf("reaction (mxid=%q, gc_msgid=%q, part_id=%q): target message row was not migrated, skipping orphan reaction", r.MXID, msgID, partID))
			continue
		}

		// The message check ignores room_id, so the reaction's own portal must be checked too.
		portalOK, err := portalExistsInTarget(ctx, deps.Target, chat, receiver)
		if err != nil {
			return count, warnings, fmt.Errorf("migrate: checking target portal for reaction (mxid=%q, gc_msgid=%q, gc_chat=%q): %w", r.MXID, msgID, chat, err)
		}
		if !portalOK {
			warnings = append(warnings, fmt.Sprintf("reaction (mxid=%q, gc_msgid=%q, gc_chat=%q, gc_receiver=%q): target portal row was not migrated, skipping orphan reaction", r.MXID, msgID, chat, receiver))
			continue
		}

		senderID := gcid.MakeUserID(senderStr)

		ghostOK, err := ghostExistsInTarget(ctx, deps.Target, string(senderID))
		if err != nil {
			return count, warnings, fmt.Errorf("migrate: checking target ghost for reaction sender (mxid=%q, gc_msgid=%q, sender=%q): %w", r.MXID, msgID, senderStr, err)
		}
		if !ghostOK {
			warnings = append(warnings, fmt.Sprintf("reaction (mxid=%q, gc_msgid=%q): sender %q has no migrated ghost, skipping orphan reaction", r.MXID, msgID, senderStr))
			continue
		}

		senderMXID := deps.FormatGhostMXID(senderID)

		// Python stores the bare emoji (emoji_id); emoji gets the variation selector added back.
		emojiDisplay := variationselector.Add(bareEmoji)

		tsMicros := NormalizeTimestampMicros(r.Timestamp, version, versionKnown)
		ts := time.UnixMicro(tsMicros)

		// TopicID is deliberately left empty; the fallback path handles it.
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
