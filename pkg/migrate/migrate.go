package migrate

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"
)

// Options controls one migration run. See cmd/mautrix-googlechat/main.go for
// how these are wired to CLI flags (--migrate-from-python, --migrate-dry-run,
// --migrate-force).
type Options struct {
	// SourceDSN is the DSN the source database was opened from (see
	// OpenSource). Run doesn't use it to open anything itself -- Deps.Source
	// is already an open connection -- it's carried through for logging and
	// the Summary, so a dry-run report says which DB it read from.
	SourceDSN string
	// DryRun runs every per-entity migrator and produces a full Summary, but
	// always rolls back the target transaction: the target database is left
	// byte-for-byte as it was before Run was called.
	DryRun bool
	// Force allows migrating into a target database that already has data
	// in it (see targetHasExistingData). Without it, Run refuses to touch a
	// non-empty target -- the migration is meant to run exactly once,
	// against a fresh bridge database, and a non-empty target is far more
	// likely to be an operator mistake (wrong config, already-migrated,
	// live bridge database) than an intentional re-run.
	Force bool
}

// Deps are the external collaborators the migration engine needs. All of
// them are supplied by the caller (cmd/mautrix-googlechat/main.go), which
// builds them from an already-initialized mxmain.BridgeMain -- see that
// file's runMigration for how Target/FormatGhostMXID are obtained from the
// live bridge config, not reimplemented here (m7-migration-preflight.md
// item 2).
type Deps struct {
	// Source is the SOURCE Python bridge's database, already open
	// READ-ONLY (see OpenSource). Run and every per-entity migrator must
	// never write to it.
	Source *dbutil.Database
	// Target is the Go bridge's own configured database. Run opens exactly
	// one transaction against it (see Run) and every per-entity migrator
	// must do all its writes through the ctx that transaction produces
	// (dbutil.Database.Exec/Query/QueryRow dispatch to the active
	// transaction automatically based on ctx -- see dbutil.Database.DoTxn).
	Target *dbutil.Database
	// FormatGhostMXID mirrors the bridge's own
	// matrix.Connector.FormatGhostMXID -- the ONLY correct way to compute a
	// ghost's Matrix ID for a given Google Chat user id, per
	// m7-migration-preflight.md item 2. Callers must pass the live bridge's
	// method value, not a reimplementation, so migrated message/reaction
	// sender_mxid columns are byte-identical to what a live bridge would
	// generate for the same ghost.
	FormatGhostMXID func(networkid.UserID) id.UserID
	// Log is optional; if nil, migration proceeds silently (tests rely on
	// this -- Summary carries everything a caller needs).
	Log *zerolog.Logger
}

func (d *Deps) logger() *zerolog.Logger {
	if d.Log != nil {
		return d.Log
	}
	nop := zerolog.Nop()
	return &nop
}

// EntityCount is one entity's migration result: how many rows were written,
// plus any non-fatal, per-row warnings collected along the way (e.g. a
// skipped row, or an unresolvable reference) -- warnings never abort the
// migration, unlike a returned error.
type EntityCount struct {
	Migrated int
	Warnings []string
}

// Summary is the full result of one Run call.
type Summary struct {
	Portals   EntityCount
	Ghosts    EntityCount
	Messages  EntityCount
	Reactions EntityCount
	Users     EntityCount

	// DryRun mirrors Options.DryRun, so a caller holding only the Summary
	// can tell whether these counts were actually committed.
	DryRun bool
	// Warnings are migration-wide notices that aren't specific to one
	// entity (e.g. "source has no version table, assuming microsecond
	// timestamps").
	Warnings []string
}

// MigrateFunc is the signature every per-entity migrator implements --
// Tasks 5-7 slot in by replacing the stub bodies below with real logic. Each
// migrator:
//   - reads whatever source rows it needs via the pkg/migrate/source.go
//     Get* functions against deps.Source,
//   - writes into deps.Target using the ctx passed in (already scoped to
//     Run's single transaction -- see Deps.Target's doc comment),
//   - returns the count of rows it migrated and any non-fatal warnings,
//   - returns a non-nil error only for a genuinely unrecoverable failure,
//     which aborts and rolls back the ENTIRE migration (all entities), not
//     just this one.
type MigrateFunc func(ctx context.Context, deps *Deps, opts Options) (count int, warnings []string, err error)

// ErrTargetNotEmpty is returned by Run when the target database already has
// data and opts.Force was not set.
var ErrTargetNotEmpty = errors.New("migrate: target database is not empty (rerun with --migrate-force to override)")

// errDryRun is an internal sentinel: Run's transaction body returns it after
// a successful dry run to force dbutil.Database.DoTxn to roll back (DoTxn
// commits on nil error, rolls back on any non-nil error). Run itself
// unwraps this back to a nil error before returning to the caller.
var errDryRun = errors.New("migrate: dry run, rolling back")

type entityStep struct {
	name  string
	fn    MigrateFunc
	count *EntityCount
}

// Run executes one migration: it opens exactly one transaction against
// deps.Target, refuses a non-empty target unless opts.Force, dispatches to
// each per-entity migrator in turn (portals and ghosts first, since messages
// and reactions reference them; then messages, then reactions, which
// reference messages; users last), and either commits (opts.DryRun == false
// and no error) or rolls back (opts.DryRun == true, or any migrator
// returned an error).
//
// The returned *Summary is always populated, even on error or dry-run,
// so a caller can report partial progress; only the error return indicates
// whether anything was actually committed.
func Run(ctx context.Context, deps *Deps, opts Options) (*Summary, error) {
	if deps == nil {
		return nil, errors.New("migrate: deps must not be nil")
	}
	if deps.Source == nil {
		return nil, errors.New("migrate: deps.Source must be set (open it with OpenSource)")
	}
	if deps.Target == nil {
		return nil, errors.New("migrate: deps.Target must be set")
	}
	if deps.FormatGhostMXID == nil {
		return nil, errors.New("migrate: deps.FormatGhostMXID must be set")
	}

	log := deps.logger()
	summary := &Summary{DryRun: opts.DryRun}

	steps := []entityStep{
		{"portals", migratePortals, &summary.Portals},
		{"ghosts", migrateGhosts, &summary.Ghosts},
		{"messages", migrateMessages, &summary.Messages},
		{"reactions", migrateReactions, &summary.Reactions},
		{"users", migrateUsers, &summary.Users},
	}

	txErr := deps.Target.DoTxn(ctx, nil, func(ctx context.Context) error {
		if !opts.Force {
			nonEmpty, err := targetHasExistingData(ctx, deps.Target)
			if err != nil {
				return fmt.Errorf("migrate: checking target for existing data: %w", err)
			}
			if nonEmpty {
				return ErrTargetNotEmpty
			}
		}

		for _, step := range steps {
			n, warnings, err := step.fn(ctx, deps, opts)
			if err != nil {
				return fmt.Errorf("migrate: migrating %s: %w", step.name, err)
			}
			step.count.Migrated = n
			step.count.Warnings = warnings
			log.Info().Str("entity", step.name).Int("count", n).Msg("Migrated entity")
		}

		if opts.DryRun {
			return errDryRun
		}
		return nil
	})

	if txErr != nil && !errors.Is(txErr, errDryRun) {
		return summary, txErr
	}
	return summary, nil
}

// targetHasExistingData reports whether the target bridgev2 database
// already has any data this migration would write. It counts EVERY entity
// table the migration populates, not just portals/logins: bridgev2 persists a
// bare `user` row the first time any Matrix user messages the bridge bot
// (loadUser -> DB.User.Insert), before any portal or login exists, so a target
// where someone contacted the bot but never logged in has `user` rows and no
// portals/logins -- counting only portal+user_login would misclassify that as
// empty and let the migration write into a non-fresh DB (Task 7's user writes
// would then PK-conflict or silently merge). An untouched, schema-upgraded
// fresh database has zero rows in all of these; any non-zero count means the
// target is not fresh. (`user` is a reserved word -> quoted; the others are
// quoted too for uniformity, valid in both SQLite and Postgres.)
func targetHasExistingData(ctx context.Context, db *dbutil.Database) (bool, error) {
	for _, table := range []string{`portal`, `ghost`, `message`, `reaction`, `"user"`, `user_login`} {
		var count int
		err := db.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s`, table)).Scan(&count)
		if err != nil {
			return false, fmt.Errorf("counting %s: %w", table, err)
		}
		if count > 0 {
			return true, nil
		}
	}
	return false, nil
}

// --- Per-entity migrators ---------------------------------------------
//
// migratePortals and migrateGhosts are implemented in portal.go/ghost.go
// (M7 Task 5). migrateMessages/migrateReactions/migrateUsers remain stubs
// (Tasks 6-7 replace these) -- every stub below does no reads or writes and
// reports zero rows migrated, so Run's dry-run path and non-empty-target
// guard are fully testable before that per-entity logic exists.

func migrateMessages(ctx context.Context, deps *Deps, opts Options) (int, []string, error) {
	return 0, nil, nil
}

func migrateReactions(ctx context.Context, deps *Deps, opts Options) (int, []string, error) {
	return 0, nil, nil
}

func migrateUsers(ctx context.Context, deps *Deps, opts Options) (int, []string, error) {
	return 0, nil, nil
}

// --- Shared helpers required by the preflight findings ---------------------

// requiredCookieKeys are the uppercase keys gchatmeow.RequiredCookies /
// hasRequiredCookies expect in UserLoginMetadata.Cookies.
var requiredCookieKeys = []string{"COMPASS", "SSID", "SID", "OSID", "HSID"}

// UppercaseCookieKeys remaps a cookies map from Python's on-disk casing to
// Go's expected casing.
//
// m7-migration-preflight.md item 1 (CONFIRMED, corrects the schema map's
// Risk #7): the Python `Cookies` NamedTuple's fields, and therefore
// `user.cookies`'s JSON keys, are lowercase (compass/ssid/sid/osid/hsid).
// Go's gchatmeow.RequiredCookies and hasRequiredCookies require exactly
// COMPASS/SSID/SID/OSID/HSID (uppercase). A verbatim json.Unmarshal copy
// would silently produce a Cookies map hasRequiredCookies rejects as
// incomplete, forcing every migrated user to re-login -- defeating the
// point of migrating auth state at all. This uppercases every key
// (strings.ToUpper is safe/lossless here: all five keys are pure
// ASCII lower<->upper pairs), preserving any extra/unknown keys uppercased
// too rather than dropping them.
func UppercaseCookieKeys(cookies map[string]string) map[string]string {
	if cookies == nil {
		return nil
	}
	out := make(map[string]string, len(cookies))
	for k, v := range cookies {
		out[strings.ToUpper(k)] = v
	}
	return out
}

// HasRequiredCookieKeys reports whether cookies (assumed already
// uppercased via UppercaseCookieKeys) has a NON-EMPTY value for all five
// keys gchatmeow requires -- mirroring pkg/connector/client.go's own
// hasRequiredCookies exactly (key-present-but-empty is treated the same as
// missing there, since an empty cookie value can't authenticate either).
// Useful for the per-user migrator (Task 7) to warn-and-skip a user whose
// cookie blob is incomplete instead of writing a UserLogin that the live
// bridge's hasRequiredCookies would immediately reject as unusable.
func HasRequiredCookieKeys(cookies map[string]string) bool {
	for _, k := range requiredCookieKeys {
		if cookies[k] == "" {
			return false
		}
	}
	return true
}

// minMicrosecondSchemaVersion is the Python bridge's db/upgrade/v10 --
// the one-time `UPDATE message SET timestamp=timestamp*1000` (and same for
// reaction) that converted stored timestamps from milliseconds to
// microseconds. A source schema version >= this is already µs.
const minMicrosecondSchemaVersion = 10

// TimestampsAreMicroseconds decides, from the source DB's schema-version
// bookkeeping (see ReadSchemaVersion), whether message/reaction timestamps
// are already in microseconds.
//
// m7-migration-preflight.md item 4: every DB that has ever been started
// against the current Python bridge codebase has already run the v10
// upgrade (the last registered upgrade, run unconditionally at startup), so
// the common case is "already µs" even when the version table can't be
// read at all (versionKnown == false) -- only a raw, never-upgraded
// snapshot frozen before v10 shipped would still be ms, and that requires
// an explicit, readable version < 10 to detect. Do not hard-fail when the
// version table is missing/unexpected; default to the µs assumption.
func TimestampsAreMicroseconds(version int, versionKnown bool) bool {
	if !versionKnown {
		return true
	}
	return version >= minMicrosecondSchemaVersion
}

// NormalizeTimestampMicros converts a raw Python `timestamp` column value to
// microseconds, applying the *1000 conversion only when the source schema
// predates v10 (see TimestampsAreMicroseconds).
func NormalizeTimestampMicros(ts int64, version int, versionKnown bool) int64 {
	if TimestampsAreMicroseconds(version, versionKnown) {
		return ts
	}
	return ts * 1000
}
