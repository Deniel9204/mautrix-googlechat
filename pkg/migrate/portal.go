package migrate

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"github.com/Deniel9204/mautrix-googlechat/pkg/connector"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// insertMigratedPortalQuery writes one Go `portal` row; columns with no Python source are hardcoded defaults.
const insertMigratedPortalQuery = `
	INSERT INTO portal (
		bridge_id, id, receiver, mxid,
		parent_id, parent_receiver, relay_bridge_id, relay_login_id,
		other_user_id,
		name, topic, avatar_id, avatar_hash, avatar_mxc,
		name_set, avatar_set, topic_set, name_is_custom,
		in_space, message_request, room_type,
		disappear_type, disappear_timer, cap_state,
		metadata
	) VALUES (
		'', $1, $2, $3,
		NULL, '', NULL, NULL,
		$4,
		$5, $6, '', '', $7,
		$8, $9, $10, false,
		false, false, $11,
		NULL, NULL, NULL,
		$12
	)
`

// migratePortals writes one Go `portal` row per Python `portal` row.
func migratePortals(ctx context.Context, deps *Deps, opts Options) (int, []string, error) {
	rows, err := GetPortals(ctx, deps.Source)
	if err != nil {
		return 0, nil, fmt.Errorf("migrate: reading source portals: %w", err)
	}

	var warnings []string
	count := 0
	for _, p := range rows {
		// room_type comes only from the id's dm:/space: prefix; anything else is skipped.
		group, parseErr := gcid.ParsePortalID(networkid.PortalID(p.GCID))
		if parseErr != nil {
			warnings = append(warnings, fmt.Sprintf("portal (gcid=%q, receiver=%q): invalid id, skipping row: %v", p.GCID, p.GCReceiver, parseErr))
			continue
		}

		roomType := database.RoomTypeDefault
		if group.IsDM {
			roomType = database.RoomTypeDM
		}

		meta := &connector.PortalMetadata{
			Revision:       p.Revision.Int64,
			ThreadsOnly:    p.ThreadsOnly.Bool,
			ThreadsEnabled: p.ThreadsEnabled.Bool,
		}
		metaJSON, err := json.Marshal(meta)
		if err != nil {
			return count, warnings, fmt.Errorf("migrate: marshaling portal metadata for (gcid=%q, receiver=%q): %w", p.GCID, p.GCReceiver, err)
		}

		_, err = deps.Target.Exec(ctx, insertMigratedPortalQuery,
			// gcid/gc_receiver are already in Go id/receiver format.
			p.GCID, p.GCReceiver,
			// mxid: nullable.
			p.MXID,
			// other_user_id: nullable.
			p.OtherUserID,
			// Go columns are NOT NULL.
			nullStr(p.Name), nullStr(p.Description), nullStr(p.AvatarMXC),
			p.NameSet, p.AvatarSet, p.DescriptionSet,
			string(roomType),
			string(metaJSON),
		)
		if err != nil {
			return count, warnings, fmt.Errorf("migrate: inserting portal (gcid=%q, receiver=%q): %w", p.GCID, p.GCReceiver, err)
		}
		count++
	}
	return count, warnings, nil
}

// nullStr coalesces a nullable Python TEXT column to "" for a Go NOT NULL column.
func nullStr(s sql.NullString) string {
	if s.Valid {
		return s.String
	}
	return ""
}
