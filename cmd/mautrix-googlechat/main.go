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
	Version:     "26.07.3",
	SemCalVer:   true,
	Connector:   &connector.GChatConnector{},
}

// --migrate-from-python switches the binary into one-shot migration mode:
// instead of starting the bridge, it reads a Python mautrix-googlechat
// database and writes its data into this bridge's own configured (target)
// database, then exits without starting the event loop. See pkg/migrate for
// the engine.
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

// runMigrationIfRequested is m.PostInit. mxmain.BridgeMain.Init calls
// PostInit after br.DB, br.Matrix, and br.Bridge are fully initialized from
// the loaded config, but before Run's caller proceeds to Start (which begins
// the live event loop). This is the cleanest available hook to reuse
// mxmain's own config/DB/connector initialization for a one-shot migration:
// if --migrate-from-python was passed, this function runs the migration
// against the already-initialized target DB and connector, prints the
// summary, and exits the process -- Start/the bridge event loop is never
// reached. If the flag is absent, this is a no-op and m.Run() continues
// exactly as it did before migration support existed.
func runMigrationIfRequested() {
	if *migrateFromPython == "" {
		return
	}

	log := m.Log.With().Str("component", "migrate").Logger()
	ctx := log.WithContext(context.Background())

	// PostInit runs inside mxmain.BridgeMain.Init, well before Run's later
	// call to Start (which is what normally triggers br.DB.Upgrade --
	// see bridgev2.Bridge.StartConnectors). Since this migration mode exits
	// before Start is ever reached, the target's bridgev2 schema (portal,
	// user_login, etc.) would not exist yet without this explicit call --
	// migrate.Run's own non-empty-target guard (targetHasExistingData)
	// queries those tables and would fail with "no such table" against a
	// freshly configured target. m.DB.UpgradeTable is already populated at
	// this point (Init's earlier bridgev2.NewBridge call wires it via
	// bridgev2/database.New), so this only needs to run the upgrade itself,
	// matching exactly the DB-upgrade half of what StartConnectors does
	// (the rest of StartConnectors -- split-portal migration, starting the
	// Matrix/network connectors -- only matters for a live event loop,
	// which this mode never starts).
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
		// FormatGhostMXID is the bridge's own, already-configured method,
		// so migrated sender_mxid values are byte-identical to what a live
		// bridge would generate.
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
