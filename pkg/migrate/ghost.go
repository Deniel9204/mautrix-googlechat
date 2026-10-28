package migrate

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/Deniel9204/mautrix-googlechat/pkg/connector"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// insertMigratedGhostQuery writes one Go `ghost` row; is_bot, identifiers and extra_profile are hardcoded.
const insertMigratedGhostQuery = `
	INSERT INTO ghost (
		bridge_id, id, name, avatar_id, avatar_hash, avatar_mxc,
		name_set, avatar_set, contact_info_set, is_bot,
		identifiers, extra_profile, metadata
	) VALUES (
		'', $1, $2, $3, $4, $5,
		$6, $7, $8, false,
		'[]', NULL, $9
	)
`

// migrateGhosts writes one Go `ghost` row per Python `puppet` row; double-puppet columns go to user.access_token instead.
func migrateGhosts(ctx context.Context, deps *Deps, opts Options) (int, []string, error) {
	rows, err := GetPuppets(ctx, deps.Source)
	if err != nil {
		return 0, nil, fmt.Errorf("migrate: reading source puppets: %w", err)
	}

	// GhostMetadata has no Python source; it repopulates on next sync.
	metaJSON, err := json.Marshal(&connector.GhostMetadata{})
	if err != nil {
		return 0, nil, fmt.Errorf("migrate: marshaling empty ghost metadata: %w", err)
	}

	var warnings []string
	count := 0
	for _, p := range rows {
		id := gcid.MakeUserID(p.GCID)

		avatarHash, hashWarning := validatedAvatarHash(p.PhotoHash)
		if hashWarning != "" {
			warnings = append(warnings, fmt.Sprintf("puppet (gcid=%q): %s", p.GCID, hashWarning))
		}

		_, err := deps.Target.Exec(ctx, insertMigratedGhostQuery,
			string(id),
			// Go columns are NOT NULL.
			nullStr(p.Name), nullStr(p.PhotoID), avatarHash, nullStr(p.PhotoMXC),
			p.NameSet, p.AvatarSet, p.ContactInfoSet,
			string(metaJSON),
		)
		if err != nil {
			return count, warnings, fmt.Errorf("migrate: inserting ghost (gcid=%q): %w", p.GCID, err)
		}
		count++
	}
	return count, warnings, nil
}

// validatedAvatarHash returns photo_hash if it is 64 hex chars; a non-empty invalid value yields a warning.
func validatedAvatarHash(s sql.NullString) (value string, warning string) {
	if !s.Valid || s.String == "" {
		return "", ""
	}
	if decoded, err := hex.DecodeString(s.String); err == nil && len(decoded) == 32 {
		return s.String, ""
	}
	return "", fmt.Sprintf("avatar_hash %q is not 64 hex characters, using \"\" instead", s.String)
}
