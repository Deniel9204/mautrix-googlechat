package migrate

// migrateGhosts implements the ghost half of the migration, replicating the
// field-by-field mapping exactly. Same raw-INSERT approach as portal.go --
// see that file's package doc comment.

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/Deniel9204/mautrix-googlechat/pkg/connector"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// insertMigratedGhostQuery writes one Go `ghost` row. is_bot, identifiers,
// and extra_profile have no Python source at all (computed, not copied, or
// left NULL) so they're hardcoded rather than bound: false, '[]', and NULL
// respectively.
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

// migrateGhosts reads every row of the source Python `puppet` table and
// writes the corresponding Go `ghost` row.
//
// is_registered and the double-puppet columns (custom_mxid/access_token/
// next_batch/base_url) are read by GetPuppets but deliberately NOT used
// here -- is_registered is dropped outright (no Go ghost equivalent), and
// the double-puppet token lands on Go `user.access_token` for the owning
// Matrix user, not on `ghost` at all.
func migrateGhosts(ctx context.Context, deps *Deps, opts Options) (int, []string, error) {
	rows, err := GetPuppets(ctx, deps.Source)
	if err != nil {
		return 0, nil, fmt.Errorf("migrate: reading source puppets: %w", err)
	}

	// GhostMetadata.Email has no Python source at all (the bridge will
	// repopulate it the next time it processes that ghost's user info)
	// -- every migrated ghost gets the identical empty metadata blob.
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
			// name/avatar_id/avatar_mxc: identity copy, but Go's columns
			// are NOT NULL -- coalesce a nullable Python NULL to "".
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

// validatedAvatarHash coalesces a nullable Python photo_hash column to Go
// ghost.avatar_hash (a direct hex-string copy at the SQL/column level, just
// validating it's exactly 64 hex chars (32 bytes) before writing). A
// NULL/empty source value silently becomes the empty-string sentinel --
// that's the documented "no avatar yet" case, not an error. A NON-EMPTY
// value that fails validation is real (if malformed) source data being
// dropped, so that case is reported as a warning.
func validatedAvatarHash(s sql.NullString) (value string, warning string) {
	if !s.Valid || s.String == "" {
		return "", ""
	}
	if decoded, err := hex.DecodeString(s.String); err == nil && len(decoded) == 32 {
		return s.String, ""
	}
	return "", fmt.Sprintf("avatar_hash %q is not 64 hex characters, using \"\" instead", s.String)
}
