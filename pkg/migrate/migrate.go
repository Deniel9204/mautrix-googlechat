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

// Options controls one migration run.
type Options struct {
	// SourceDSN is carried through for logging and the Summary only.
	SourceDSN string
	// DryRun runs every migrator but always rolls back the target transaction.
	DryRun bool
	// Force allows migrating into a non-empty target database.
	Force bool
}

// Deps are the external collaborators the migration engine needs.
type Deps struct {
	// Source is the Python bridge's database, opened read-only; never write to it.
	Source *dbutil.Database
	// Target is the Go bridge's database; all writes go through Run's transaction ctx.
	Target *dbutil.Database
	// FormatGhostMXID must be the live bridge's method value, not a reimplementation.
	FormatGhostMXID func(networkid.UserID) id.UserID
	// Log is optional; nil means silent.
	Log *zerolog.Logger
}

func (d *Deps) logger() *zerolog.Logger {
	if d.Log != nil {
		return d.Log
	}
	nop := zerolog.Nop()
	return &nop
}

// EntityCount is one entity's migration result; warnings never abort the migration.
type EntityCount struct {
	Migrated int
	Warnings []string
}

// Summary is the full result of one Run call. Users counts accounts, Logins counts valid sessions.
type Summary struct {
	Portals     EntityCount
	Ghosts      EntityCount
	Messages    EntityCount
	Reactions   EntityCount
	Users       EntityCount
	Logins      EntityCount
	UserPortals EntityCount

	// DryRun mirrors Options.DryRun.
	DryRun bool
	// Warnings are migration-wide notices not tied to one entity.
	Warnings []string
}

// MigrateFunc is implemented by every per-entity migrator; a non-nil error rolls back the whole migration.
type MigrateFunc func(ctx context.Context, deps *Deps, opts Options) (count int, warnings []string, err error)

// ErrTargetNotEmpty is returned when the target has data and Force is not set.
var ErrTargetNotEmpty = errors.New("migrate: target database is not empty (rerun with --migrate-force to override)")

// errDryRun forces DoTxn to roll back after a dry run; Run maps it back to nil.
var errDryRun = errors.New("migrate: dry run, rolling back")

type entityStep struct {
	name  string
	fn    MigrateFunc
	count *EntityCount
}

// Run migrates every entity in FK dependency order inside one target transaction,
// committing only if not a dry run and no migrator failed. The Summary is always populated.
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
	// See backfillTaskSeedingNote.
	summary.Warnings = append(summary.Warnings, backfillTaskSeedingNote)

	steps := []entityStep{
		{"portals", migratePortals, &summary.Portals},
		{"ghosts", migrateGhosts, &summary.Ghosts},
		{"messages", migrateMessages, &summary.Messages},
		{"reactions", migrateReactions, &summary.Reactions},
		{"users", migrateUsers, &summary.Users},
		// user_login references user.
		{"logins", migrateUserLogins, &summary.Logins},
		// user_portal references user_login and portal.
		{"user_portals", migrateUserPortals, &summary.UserPortals},
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

// targetHasExistingData reports whether any table the migration writes has rows.
// bridgev2 creates bare `user` rows on first bot contact, so `user` is checked too.
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

// requiredCookieKeys mirror gchatmeow.RequiredCookies.
var requiredCookieKeys = []string{"COMPASS", "SSID", "SID", "OSID", "HSID"}

// UppercaseCookieKeys remaps Python's lowercase cookie keys to the uppercase keys Go expects.
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

// HasRequiredCookieKeys reports whether all required cookies are non-empty, matching hasRequiredCookies.
func HasRequiredCookieKeys(cookies map[string]string) bool {
	for _, k := range requiredCookieKeys {
		if cookies[k] == "" {
			return false
		}
	}
	return true
}

// minMicrosecondSchemaVersion is the Python upgrade (v10) that converted timestamps from ms to µs.
const minMicrosecondSchemaVersion = 10

// TimestampsAreMicroseconds reports whether source timestamps are µs; an unknown version is assumed µs.
func TimestampsAreMicroseconds(version int, versionKnown bool) bool {
	if !versionKnown {
		return true
	}
	return version >= minMicrosecondSchemaVersion
}

// NormalizeTimestampMicros converts a Python timestamp to microseconds.
func NormalizeTimestampMicros(ts int64, version int, versionKnown bool) int64 {
	if TimestampsAreMicroseconds(version, versionKnown) {
		return ts
	}
	return ts * 1000
}
