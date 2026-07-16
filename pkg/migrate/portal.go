package migrate

// migratePortals implements M7 Task 5's portal half. See
// .superpowers/sdd/m7-migration-schema-map.md §1 for the full field-by-field
// mapping this replicates exactly, and .superpowers/sdd/task-5-brief.md for
// scope. Every write goes through a raw INSERT against ctx (Run's single
// transaction, per Deps.Target's doc comment) using the EXACT column list
// §1 specifies -- no ORM/QueryHelper involved, since pkg/migrate is a
// standalone tool operating on rows the live bridge's own Portal struct
// never sees.

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

// insertMigratedPortalQuery writes one Go `portal` row per schema map §1.
// Columns with no Python source (parent_id/parent_receiver, relay_*,
// name_is_custom, in_space, message_request, disappear_*, cap_state) are
// hardcoded to their documented defaults (NULL, empty string, or false) rather than
// bound parameters, since every migrated row gets the identical value --
// see §1's "N/A" rows. bridge_id is always the empty string (intro, "every
// Go bridgev2 table is keyed by (bridge_id, ...)" -- this bridge never
// configures multi-instance, so bridge_id is always "").
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

// migratePortals reads every row of the source Python `portal` table and
// writes the corresponding Go `portal` row, per schema map §1.
func migratePortals(ctx context.Context, deps *Deps, opts Options) (int, []string, error) {
	rows, err := GetPortals(ctx, deps.Source)
	if err != nil {
		return 0, nil, fmt.Errorf("migrate: reading source portals: %w", err)
	}

	var warnings []string
	count := 0
	for _, p := range rows {
		// The id's dm:/space: prefix is the ONLY source for room_type --
		// §1: "Computed, not copied... derive from the id's dm:/space:
		// prefix... never RoomTypeSpace". A row whose gcid has neither
		// prefix (corrupt data, or a foreign row from an unrelated table)
		// can't be migrated at all: warn and skip, don't abort the whole run.
		group, parseErr := gcid.ParsePortalID(networkid.PortalID(p.GCID))
		if parseErr != nil {
			warnings = append(warnings, fmt.Sprintf("portal (gcid=%q, receiver=%q): invalid id, skipping row: %v", p.GCID, p.GCReceiver, parseErr))
			continue
		}

		roomType := database.RoomTypeDefault
		if group.IsDM {
			roomType = database.RoomTypeDM
		}

		// sql.NullInt64/NullBool's own zero-value-on-NULL behavior already
		// gives us "NULL -> 0/false" for Revision/ThreadsOnly/ThreadsEnabled
		// without an explicit .Valid check (database/sql leaves .Int64/.Bool
		// at their zero value whenever a NULL is scanned) -- see §1: "NULL
		// bools -> false".
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
			// id, receiver: identity copy (schema map §0 -- gcid/gc_receiver
			// already ARE the Go id/receiver format).
			p.GCID, p.GCReceiver,
			// mxid: identity copy. Go's mxid column is nullable, so a
			// NULL Python mxid stays NULL (sql.NullString implements
			// driver.Valuer, so passing it directly does the right thing).
			p.MXID,
			// other_user_id: identity copy, also nullable on the Go side.
			p.OtherUserID,
			// name/topic/avatar_mxc: identity copy, but Go's columns are
			// NOT NULL -- coalesce a nullable Python NULL to "" (§1).
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

// nullStr coalesces a nullable Python TEXT column to "" for a Go NOT NULL
// column that has no better default -- schema map §1/§4 explicitly calls
// this out for portal.name/topic/avatar_mxc and ghost.name/avatar_id/
// avatar_mxc: "a NULL insert would violate the schema."
func nullStr(s sql.NullString) string {
	if s.Valid {
		return s.String
	}
	return ""
}
