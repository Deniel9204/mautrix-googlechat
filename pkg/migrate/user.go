package migrate

// migrateUsers/migrateUserLogins implement the User + UserLogin migrator,
// replicating the source schema's user mapping with two corrections layered
// on top: cookie key casing, and the double-puppet token ->
// user.access_token. Same raw-INSERT-through-ctx approach as
// portal.go/ghost.go/message.go -- see portal.go's package doc comment.
//
// Every Python `user` row always gets a Go `user` row (migrateUsers;
// management_room is a nullable identity copy of notice_room; access_token
// is populated ONLY from a matching double-puppet, never from anything else
// -- see buildDoublePuppetTokens). A Go `user_login` row is added ONLY when
// the Python row has both a non-NULL gcid AND non-NULL cookies (schema map
// §5: "skip rows where Python's gcid is NULL" / "Skip rows with NULL
// cookies") -- migrateUserLogins, a SEPARATE Run step (migrate.go) that runs
// AFTER migrateUsers, since user_login has an FK to user(bridge_id, mxid)
// and every user row must already exist in the target before any of its
// logins can be inserted.
//
// These were originally one combined migrator (M7 Task 7) returning a single
// count that summed user+user_login rows; M7 Task 8's Summary-polish
// deliverable split it into two functions/Summary buckets (Users, Logins) so
// an operator reading the migration report can tell how many accounts vs.
// how many live sessions were migrated.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/Deniel9204/mautrix-googlechat/pkg/connector"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// insertMigratedUserQuery writes one Go `user` row per schema map §5.
// bridge_id is always the empty string `""` (see portal.go's identical
// comment). management_room
// and access_token are both nullable Go columns: management_room is bound
// directly as the (possibly NULL) Python notice_room value (identity copy);
// access_token is left NULL unless a matching double-puppet token was found
// (see buildDoublePuppetTokens) -- never coalesced to "".
const insertMigratedUserQuery = `
	INSERT INTO "user" (bridge_id, mxid, management_room, access_token)
	VALUES ('', $1, $2, $3)
`

// insertMigratedUserLoginQuery writes one Go `user_login` row per schema map
// §5. remote_name has no Python source (schema map: "leave \"\" ...
// non-critical, self-heals on next connect") and Go's column is NOT NULL, so
// it's always the empty string, never NULL. remote_profile/space_room have
// no Python source either and ARE nullable, so both are hardcoded NULL.
const insertMigratedUserLoginQuery = `
	INSERT INTO user_login (bridge_id, user_mxid, id, remote_name, remote_profile, space_room, metadata)
	VALUES ('', $1, $2, '', NULL, NULL, $3)
`

// buildDoublePuppetTokens builds a map from double-puppet Matrix mxid
// (Python puppet.custom_mxid) -> double-puppet access token (Python
// puppet.access_token), per m7-migration-preflight.md item 3 (CORRECTS
// schema map §4/Risk#6: Go user.access_token, not ghost, is where a
// double-puppet token belongs). Only puppets with BOTH a non-empty
// custom_mxid AND a non-empty access_token contribute an entry -- a puppet
// that was never configured for double-puppeting has neither, and one with
// only one of the two has no usable token to carry forward either way.
func buildDoublePuppetTokens(puppets []*PythonPuppet) map[string]string {
	tokens := make(map[string]string)
	for _, p := range puppets {
		if p.CustomMXID.Valid && p.CustomMXID.String != "" && p.AccessToken.Valid && p.AccessToken.String != "" {
			tokens[p.CustomMXID.String] = p.AccessToken.String
		}
	}
	return tokens
}

// migrateUsers reads every row of the source Python `user` table (plus the
// `puppet` table, for double-puppet tokens only -- ghost migration itself is
// Task 5's concern) and writes the corresponding Go `user` row, per schema
// map §5 and preflight item 3. UserLogin rows are migrateUserLogins's job
// (see that function below) -- this keeps the Summary.Users bucket counting
// ONLY `user` rows.
func migrateUsers(ctx context.Context, deps *Deps, opts Options) (int, []string, error) {
	users, err := GetUsers(ctx, deps.Source)
	if err != nil {
		return 0, nil, fmt.Errorf("migrate: reading source users: %w", err)
	}
	puppets, err := GetPuppets(ctx, deps.Source)
	if err != nil {
		return 0, nil, fmt.Errorf("migrate: reading source puppets for double-puppet tokens: %w", err)
	}
	doublePuppetTokens := buildDoublePuppetTokens(puppets)
	matchedDoublePuppetMXIDs := make(map[string]bool, len(doublePuppetTokens))

	var warnings []string
	count := 0

	for _, u := range users {
		accessToken := sql.NullString{}
		if token, ok := doublePuppetTokens[u.MXID]; ok {
			accessToken = sql.NullString{String: token, Valid: true}
			matchedDoublePuppetMXIDs[u.MXID] = true
		}

		if _, err := deps.Target.Exec(ctx, insertMigratedUserQuery, u.MXID, u.NoticeRoom, accessToken); err != nil {
			return count, warnings, fmt.Errorf("migrate: inserting user (mxid=%q): %w", u.MXID, err)
		}
		count++
	}

	// A double-puppet token whose custom_mxid matched no migrated user is a
	// warn-and-skip, not an error (preflight item 3: "If no user row matches
	// the custom_mxid, skip + warn"). Sorted for deterministic output.
	var unmatched []string
	for mxid := range doublePuppetTokens {
		if !matchedDoublePuppetMXIDs[mxid] {
			unmatched = append(unmatched, mxid)
		}
	}
	sort.Strings(unmatched)
	for _, mxid := range unmatched {
		warnings = append(warnings, fmt.Sprintf("puppet custom_mxid=%q has a double-puppet access token, but no migrated user has that mxid: skipping double-puppet token", mxid))
	}

	return count, warnings, nil
}

// migrateUserLogins reads every row of the source Python `user` table and
// writes a Go `user_login` row for the ones that have BOTH a non-NULL gcid
// AND non-NULL cookies (schema map §5). Must run as a Run step AFTER
// migrateUsers: user_login has an FK to user(bridge_id, mxid), so every
// user row this loop's login might reference must already exist in the
// target (migrateUsers always writes one user row per Python user row,
// unconditionally, so that FK is always satisfied by the time this runs --
// no existence guard needed, unlike message.go/userportal.go's
// portalExistsInTarget/ghostExistsInTarget/userLoginExistsInTarget, which
// guard against rows their own upstream migrator may have skipped).
func migrateUserLogins(ctx context.Context, deps *Deps, opts Options) (int, []string, error) {
	users, err := GetUsers(ctx, deps.Source)
	if err != nil {
		return 0, nil, fmt.Errorf("migrate: reading source users: %w", err)
	}

	var warnings []string
	count := 0

	for _, u := range users {
		// Skip: never-logged-in user (no gcid at all) -- schema map §5:
		// "skip rows where Python's gcid is NULL". Not a warning -- this is
		// the documented, expected shape for a user who contacted the
		// bridge bot but never completed login.
		if !u.GCID.Valid || u.GCID.String == "" {
			continue
		}
		// Skip: gcid present but cookies NULL -- schema map §5: "Skip rows
		// with NULL cookies" (v08's one-time cookie wipe, or a user who has
		// since logged out). Also not a warning, for the same reason --
		// those users simply need to re-login, which is already documented
		// bridge-wide behavior, not a migration defect.
		if !u.Cookies.Valid || u.Cookies.String == "" {
			continue
		}

		var rawCookies map[string]string
		if err := json.Unmarshal([]byte(u.Cookies.String), &rawCookies); err != nil {
			warnings = append(warnings, fmt.Sprintf("user (mxid=%q): cookies column is not a valid JSON object, skipping login: %v", u.MXID, err))
			continue
		}

		// preflight item 1: Python's Cookies namedtuple fields (and
		// therefore the JSON keys) are lowercase; Go's
		// gchatmeow.RequiredCookies/hasRequiredCookies require uppercase. A
		// verbatim copy would silently produce a Cookies map every migrated
		// login is rejected for -- see UppercaseCookieKeys's own doc
		// comment.
		cookies := UppercaseCookieKeys(rawCookies)
		if !HasRequiredCookieKeys(cookies) {
			warnings = append(warnings, fmt.Sprintf("user (mxid=%q): cookie map is missing one or more required keys (COMPASS/SSID/SID/OSID/HSID) after uppercasing, migrating anyway -- this user will need to re-login", u.MXID))
		}

		loginID := gcid.MakeUserLoginID(u.GCID.String)
		meta := &connector.UserLoginMetadata{
			Cookies:   cookies,
			UserAgent: nullStr(u.UserAgent),
			Revision:  u.Revision.Int64,
		}
		metaJSON, err := json.Marshal(meta)
		if err != nil {
			return count, warnings, fmt.Errorf("migrate: marshaling user_login metadata (mxid=%q): %w", u.MXID, err)
		}

		if _, err := deps.Target.Exec(ctx, insertMigratedUserLoginQuery, u.MXID, string(loginID), string(metaJSON)); err != nil {
			return count, warnings, fmt.Errorf("migrate: inserting user_login (mxid=%q, id=%q): %w", u.MXID, loginID, err)
		}
		count++
	}

	return count, warnings, nil
}
