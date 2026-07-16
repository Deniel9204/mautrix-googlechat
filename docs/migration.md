# Migrating from the Python bridge

This bridge (the Go rewrite) can migrate an existing
[mautrix/googlechat](https://github.com/mautrix/googlechat) (Python) bridge's
database directly, so you can switch bridges without losing your portals,
message history, reactions, or login sessions.

## What it does

The migration is a **one-shot, point-in-time copy** from the Python bridge's
database into this bridge's own (empty) database:

- **Portals**: every DM and space/group chat, including name, avatar, topic
  (from the Python "space description"), and thread settings.
- **Ghosts**: every puppeted Google Chat user (name, avatar, avatar hash).
- **Messages**: every message, including **multi-part messages** (a text body
  plus one or more attachments in the same Google Chat message) — each part
  becomes its own row, matching how this bridge stores them.
- **Reactions**: every reaction, correctly attached to whichever message part
  the Python bridge attached it to (see the caveats below for one edge case).
- **Users and logins**: every Matrix user's Google account link, **including
  their saved auth cookies** — a successfully migrated user does **not** need
  to log in again.
- **Double-puppeting**: if you had double-puppeting configured in the Python
  bridge, it's carried forward too.
- **DM room memberships** (`user_portal`): so a migrated DM immediately shows
  up in the right place in your Matrix client's bridge space.

## What it doesn't do

- **It does not touch the Python bridge's database.** The source database is
  opened strictly read-only (SQLite: `mode=ro&immutable=1`; Postgres:
  `default_transaction_read_only=on`) — the migration tool can never write to
  it, by construction, not just by convention.
- **It does not re-backfill history.** Migrated portals keep the exact
  message history that was already in the Python database; the migration
  does not trigger the Go bridge's backfill machinery to fetch anything older
  or newer.
- **It does not reconstruct space/group room memberships.** See caveat (c)
  below.
- **It is not incremental or repeatable against a live target.** It's
  designed to run exactly once, against a fresh Go bridge database — see
  Prerequisite 4.

## Prerequisites

**Read all four of these before running anything.**

1. **STOP the Python bridge first.** The migration opens the Python
   database read-only, but that only guarantees *this tool* won't write to
   it — it does nothing to stop the Python bridge process itself from
   continuing to write new messages, reactions, or login state while you're
   migrating. If the Python bridge is still running, the migration reads a
   moving (and potentially inconsistent) snapshot, and anything written after
   the migration reads its data will simply be missing from the Go side.
   Stop the Python bridge completely before you begin, and keep it stopped
   until you've verified the migration on the Go side.

2. **BACK UP both databases first.** Take a copy of the Python bridge's
   database *and* the (empty) Go bridge's database/data directory before
   running the migration, the same way you'd back up before any one-way data
   operation. The migration itself never writes to the source, but you are
   about to point a new piece of software at your only copy of your message
   history — have a restore path ready regardless.

3. **The Go bridge must be configured to use the SAME appservice
   registration and the SAME Matrix rooms the Python bridge used.** The
   migration copies Matrix event IDs and room IDs (`mxid` values) as-is —
   it does not create new Matrix rooms or resend events. This only makes
   sense if the Go bridge is puppeting into the *same* rooms, as the *same*
   appservice, that the Python bridge was already using. Concretely: reuse
   the same `homeserver.domain`, the same `appservice.*` identity (ID,
   tokens, sender localpart/namespace), and the same registration file
   already loaded into your homeserver — do **not** generate a fresh
   registration for this. If the Go bridge runs as a different appservice,
   every migrated room/event reference will be orphaned (the new appservice
   has no relationship to those existing Matrix rooms), and none of the
   migrated data will actually be usable.

4. **The Go bridge's own database must be FRESH (empty).** The migration
   refuses to write into a target database that already has any portals,
   ghosts, messages, reactions, users, or logins in it, unless you pass
   `--migrate-force`. This is deliberate: the tool is meant to run exactly
   once, against a bridge that has never started its normal event loop
   before. Run `-e`/`-g` to generate the config/registration as usual (see
   the main [README](../README.md#quick-start)), but do **not** start the
   bridge normally before migrating — that would populate the database (e.g.
   a bare `user` row the moment someone messages the bridge bot) and trip
   this guard.

## How to run

```sh
mautrix-googlechat --migrate-from-python <python-db-dsn> [--migrate-dry-run] [--migrate-force] -c config.yaml
```

- `--migrate-from-python <dsn>`: **required** to enter migration mode at all.
  Points at the Python bridge's database. Accepts:
  - a bare SQLite file path (e.g. `/path/to/mautrix-googlechat.db`), matching
    whatever the Python bridge's own `database` config value was;
  - a `postgres://` or `postgresql://` connection string.
- `--migrate-dry-run`: run every step and print the full summary, but roll
  back all target writes — the target database is left byte-for-byte as it
  was before the run. **Strongly recommended as your first run**, so you can
  review the counts and warnings before committing anything.
- `--migrate-force`: allow migrating into a target database that already has
  data in it (see Prerequisite 4). Also required, together with
  `--migrate-dry-run`, if you want to preview a migration a *second* time
  after already having committed one — see the note below.
- `-c config.yaml`: the Go bridge's normal config flag, same as running the
  bridge itself. The migration reuses this config's `database` section to
  connect to the **target** (Go) database and runs the same schema-upgrade
  logic a normal bridge startup would, before writing anything.

The target's connection string is the config file's own `database.uri` (see
[`README.md#configuration`](../README.md#configuration)) — SQLite and
Postgres are both supported, matching the two DSN forms above:

```yaml
database:
    type: sqlite3-fk-wal
    uri: "file:mautrix-googlechat.db?_txlock=immediate"
```

```yaml
database:
    type: postgres
    uri: "postgres://user:password@host/database?sslmode=disable"
```

The process exits after the migration completes (or fails) — it never
proceeds to start the bridge's normal event loop. On success, it prints a
summary and exits 0; on failure, it prints whatever partial summary it has
and exits 1 without committing anything (the whole migration runs in a
single transaction — see "What it does" above).

**Recommended sequence:**

```sh
# 1. Preview first — writes nothing.
mautrix-googlechat --migrate-from-python /path/to/python-bridge.db --migrate-dry-run -c config.yaml

# 2. Review the printed counts and warnings, then run for real.
mautrix-googlechat --migrate-from-python /path/to/python-bridge.db -c config.yaml
```

> [!NOTE]
> **A dry run against an already-populated target still needs
> `--migrate-force`.** The empty-target guard (Prerequisite 4) runs *before*
> the dry-run preview logic, not after — `--migrate-dry-run` only controls
> whether the transaction commits at the very end, it does not skip the
> guard at the start. If you've already committed a real migration once and
> want to re-run a dry-run preview against that same (now non-empty) target
> for inspection, you need `--migrate-force` together with
> `--migrate-dry-run`. (This does not make the dry run write anything —
> `--migrate-dry-run` always rolls back regardless of `--migrate-force`; the
> flag combination only gets you past the initial guard.)

## Documented caveats

These are known, accepted gaps — not bugs — carried over from the fact that
the Python bridge's database schema simply doesn't record everything the Go
bridge's schema does. Read the warnings a dry run prints; they'll tell you
exactly which rows (if any) in *your* database are affected.

- **(a) A solo inbound attachment-only message's `part_id` becomes `""`
  instead of `att_0`.** The Python schema has no column recording "was this
  message originally sent from Matrix or received from Google Chat" — an
  inbound, attachment-only message with no text body and an outbound
  (Matrix-originated) message of any type both look identical (a single
  database row). The migration resolves this ambiguity by always using `""`,
  which is correct for outbound sends and *usually* the safer assumption.
  This only affects the message's **internal join key** — the actual Matrix
  event content is completely unaffected. The only way it could ever matter
  is if something later needs to specifically target that exact attachment
  sub-part (e.g. an edit or reaction pointed at that precise part) — the
  Python bridge itself never supported that level of addressing either.

- **(b) Attachment index numbering may not match what a fresh Go bridge
  would have produced for the same original message.** The Python bridge
  compacts attachment indices when an attachment fails to download or
  upload (no gap is left for a skip); the Go bridge does not. A migrated
  `att_N` sequence is internally consistent (unique, stable, resolvable by
  later edits or reactions) but the original count/positions of skipped
  attachments is unrecoverable from the Python database. Rare, and purely
  cosmetic — nothing re-derives "the Nth original attachment" from a part ID
  after the fact.

- **(c) Space/group chat membership (`user_portal`) can't be reconstructed
  for shared spaces.** The Python bridge never recorded which of your Matrix
  accounts corresponds to which login inside a shared space/group
  conversation (only DMs have this recorded, implicitly, via the portal's
  owning user). Migrated space/group portals will not immediately appear in
  your bridge space list — this **self-heals** automatically the next time
  the bridge does a normal chat-list sync after you log in, with no action
  needed on your part.

- **(d) Users whose Python cookies were `NULL` get no login at all and must
  re-login.** This includes anyone who never completed login in the Python
  bridge, anyone who had logged out, and — notably — **every user who was
  logged in before the Python bridge's v08 database upgrade**, which wiped
  every stored cookie once as part of a column rename. If this applies to
  you, the migration still creates your Matrix-side `user` row (management
  room, etc.), just no working Google Chat session; send `login` to the
  bridge bot as usual afterward.

- **(e) Double-puppeting migrates via the Matrix user's `access_token`.** If
  the migration's dry run (or real run) prints a warning that a puppet's
  `custom_mxid` matched no migrated Matrix user, that specific user's
  double-puppet token was **not** carried forward (there was no
  corresponding user row to attach it to) — that user should just run
  `login-matrix` again in the Go bridge to re-establish double-puppeting.
  Nothing else is affected.

- **(f) Messages whose sender has no migrated puppet are skipped, with a
  warning.** This is near-impossible for a normal, well-formed Python
  database (every message sender should have a corresponding `puppet` row),
  but the migration warns and skips rather than failing the whole run if it
  ever happens. **Review your dry run's warnings** for any of these before
  committing — if you see one, it's worth checking why that sender has no
  puppet row in the source database.

- **(g) Migrated portals do not re-backfill their history.** See "What it
  doesn't do" above — this is by design, since the point of the migration is
  that the history is *already there*.

## Post-migration checklist

After the real (non-dry-run) migration completes successfully:

1. **Start the Go bridge** pointed at the same config you migrated into.
2. **Open a portal you migrated** in your Matrix client and confirm recent
   messages (including a multi-part one with an attachment, if you have one)
   render correctly.
3. **Confirm a migrated user's login still works without a re-login
   prompt** — have them send a message and confirm it's delivered via Google
   Chat, or run the `list-logins` command in the bridge bot's management room
   to confirm the migrated login is present. If the bridge instead prompts
   them to log in, check the dry run's warnings for caveats (d) or (e) above.
4. If you use double-puppeting, confirm messages you send from your Matrix
   client show up as sent by your own Google Chat account, not the puppet
   ghost.
5. Keep the Python bridge's database backup until you're satisfied the Go
   bridge is working correctly — the migration is a copy, not a move, so the
   original data is untouched, but there's no reason to delete your only
   other copy early.
