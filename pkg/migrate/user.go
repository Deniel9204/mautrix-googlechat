package migrate

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/Deniel9204/mautrix-googlechat/pkg/connector"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// insertMigratedUserQuery writes one Go `user` row; access_token stays NULL unless a double-puppet token matched.
const insertMigratedUserQuery = `
	INSERT INTO "user" (bridge_id, mxid, management_room, access_token)
	VALUES ('', $1, $2, $3)
`

// insertMigratedUserLoginQuery writes one Go `user_login` row; remote_name is NOT NULL so it is "".
const insertMigratedUserLoginQuery = `
	INSERT INTO user_login (bridge_id, user_mxid, id, remote_name, remote_profile, space_room, metadata)
	VALUES ('', $1, $2, '', NULL, NULL, $3)
`

// buildDoublePuppetTokens maps Python puppet.custom_mxid to its access token, which becomes Go user.access_token.
func buildDoublePuppetTokens(puppets []*PythonPuppet) map[string]string {
	tokens := make(map[string]string)
	for _, p := range puppets {
		if p.CustomMXID.Valid && p.CustomMXID.String != "" && p.AccessToken.Valid && p.AccessToken.String != "" {
			tokens[p.CustomMXID.String] = p.AccessToken.String
		}
	}
	return tokens
}

// migrateUsers writes one Go `user` row per Python `user` row, with any matching double-puppet token.
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

	// Unmatched double-puppet tokens are warned about, sorted for deterministic output.
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

// migrateUserLogins writes a Go `user_login` for each Python user with both gcid and cookies.
// Must run after migrateUsers (FK to user).
func migrateUserLogins(ctx context.Context, deps *Deps, opts Options) (int, []string, error) {
	users, err := GetUsers(ctx, deps.Source)
	if err != nil {
		return 0, nil, fmt.Errorf("migrate: reading source users: %w", err)
	}

	var warnings []string
	count := 0

	for _, u := range users {
		if !u.GCID.Valid || u.GCID.String == "" {
			continue
		}
		// Cookies wiped or logged out; the user must re-login.
		if !u.Cookies.Valid || u.Cookies.String == "" {
			continue
		}

		var rawCookies map[string]string
		if err := json.Unmarshal([]byte(u.Cookies.String), &rawCookies); err != nil {
			warnings = append(warnings, fmt.Sprintf("user (mxid=%q): cookies column is not a valid JSON object, skipping login: %v", u.MXID, err))
			continue
		}

		// Python stores cookie keys lowercase; Go requires uppercase.
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
