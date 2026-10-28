package main

import (
	"context"
	"fmt"
	"os"

	flag "maunium.net/go/mauflag"
	"maunium.net/go/mautrix/bridgev2/matrix/mxmain"

	"github.com/Deniel9204/mautrix-googlechat/pkg/connector"
	"github.com/Deniel9204/mautrix-googlechat/pkg/migrate"
)

// Filled at build time with -X linker flags.
var (
	Tag       = "unknown"
	Commit    = "unknown"
	BuildTime = "unknown"
)

var m = mxmain.BridgeMain{
	Name:        "mautrix-googlechat",
	URL:         "https://github.com/Deniel9204/mautrix-googlechat",
	Description: "A Matrix-Google Chat puppeting bridge.",
	Version:     "26.09.2",
	SemCalVer:   true,
	Connector:   &connector.GChatConnector{},
}

// --migrate-from-python runs a one-shot migration from a Python bridge database and exits (pkg/migrate).
var (
	migrateFromPython = flag.Make().LongKey("migrate-from-python").
				Usage("Migrate data from a Python mautrix-googlechat database (SQLite path or postgres:// DSN), then exit").
				Default("").String()
	migrateDryRun = flag.Make().LongKey("migrate-dry-run").
			Usage("With --migrate-from-python, run the migration but roll back all target writes (report only)").
			Default("false").Bool()
	migrateForce = flag.Make().LongKey("migrate-force").
			Usage("With --migrate-from-python, allow migrating into a target database that already has data").
			Default("false").Bool()
)

func main() {
	m.InitVersion(Tag, Commit, BuildTime)
	m.PostInit = runMigrationIfRequested
	m.Run()
}

// runMigrationIfRequested is m.PostInit: it runs the migration after DB/connector init and exits before Start.
func runMigrationIfRequested() {
	if *migrateFromPython == "" {
		return
	}

	log := m.Log.With().Str("component", "migrate").Logger()
	ctx := log.WithContext(context.Background())

	// Start normally runs the DB upgrade, but this mode exits before Start.
	if err := m.DB.Upgrade(ctx); err != nil {
		log.Error().Err(err).Msg("Failed to upgrade target database schema")
		os.Exit(1)
	}

	source, err := migrate.OpenSource(*migrateFromPython)
	if err != nil {
		log.Error().Err(err).Msg("Failed to open source (Python) database")
		os.Exit(1)
	}
	defer source.Close()

	deps := migrate.Deps{
		Source: source,
		Target: m.DB,
		// Same method as a live bridge, so migrated sender_mxid values are identical.
		FormatGhostMXID: m.Matrix.FormatGhostMXID,
		Log:             &log,
	}
	opts := migrate.Options{
		SourceDSN: *migrateFromPython,
		DryRun:    *migrateDryRun,
		Force:     *migrateForce,
	}

	summary, runErr := migrate.Run(ctx, &deps, opts)
	if summary != nil {
		printMigrationSummary(summary)
	}
	if runErr != nil {
		log.Error().Err(runErr).Msg("Migration failed")
		os.Exit(1)
	}
	os.Exit(0)
}

func printMigrationSummary(s *migrate.Summary) {
	mode := "COMMITTED"
	if s.DryRun {
		mode = "DRY RUN -- nothing was written"
	}
	fmt.Printf("Migration summary [%s]\n", mode)
	fmt.Printf("  portals:   %d\n", s.Portals.Migrated)
	fmt.Printf("  ghosts:    %d\n", s.Ghosts.Migrated)
	fmt.Printf("  messages:  %d\n", s.Messages.Migrated)
	fmt.Printf("  reactions: %d\n", s.Reactions.Migrated)
	fmt.Printf("  users:     %d\n", s.Users.Migrated)
	fmt.Printf("  logins:    %d\n", s.Logins.Migrated)
	fmt.Printf("  user_portals: %d\n", s.UserPortals.Migrated)
	for _, entity := range []struct {
		name string
		ec   migrate.EntityCount
	}{
		{"portals", s.Portals}, {"ghosts", s.Ghosts}, {"messages", s.Messages},
		{"reactions", s.Reactions}, {"users", s.Users}, {"logins", s.Logins},
		{"user_portals", s.UserPortals},
	} {
		for _, w := range entity.ec.Warnings {
			fmt.Printf("  warning[%s]: %s\n", entity.name, w)
		}
	}
	for _, w := range s.Warnings {
		fmt.Println("  warning:", w)
	}
}
