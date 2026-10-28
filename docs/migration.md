# Migrating from the Python bridge

`--migrate-from-python` copies an existing
[mautrix/googlechat](https://github.com/mautrix/googlechat) (Python) database
into this bridge's empty database, once, in a single transaction.

**Migrated:** portals (name, avatar, topic, thread settings), ghosts,
messages (each attachment becomes its own part), reactions, users with their
saved cookies (no re-login needed), double puppeting, and DM room
memberships.

**Not done:** the source is opened read-only and never written; no history
is backfilled; space room memberships are not rebuilt (see caveat c); it
cannot be re-run over an already migrated database.

## Before you start

1. **Stop the Python bridge** and keep it stopped. Anything it writes during
   the migration is lost.
2. **Back up both databases.**
3. **Reuse the Python bridge's appservice**: the same `homeserver.domain`,
   `appservice` id, tokens and namespace, and the registration file already
   loaded in your homeserver. Room and event ids are copied as they are, so a
   new appservice would leave all of them orphaned. Don't generate a new
   registration.
4. **Use a fresh target database.** Generate the config with `-e`, but don't
   start the bridge before migrating: it would write rows (for example when
   someone messages the bot) and the migration refuses a non-empty target.

## Running it

```sh
# Preview: runs everything, then rolls back.
mautrix-googlechat --migrate-from-python /path/to/python-bridge.db --migrate-dry-run -c config.yaml

# Review the counts and warnings, then run it for real.
mautrix-googlechat --migrate-from-python /path/to/python-bridge.db -c config.yaml
```

- `--migrate-from-python <dsn>`: the Python database, as a SQLite file path
  or a `postgres://` / `postgresql://` URL.
- `--migrate-dry-run`: roll back at the end; the target stays untouched.
- `--migrate-force`: allow a target that already has unrelated rows (such as
  a bare `user` row). The empty-target check runs before the dry run, so a
  preview into such a target needs both flags.
- `-c config.yaml`: the target is this config's `database` section.

The process exits when done: 0 with a summary on success, 1 with nothing
committed on failure. Rows are inserted, not upserted, so running it again
over a completed migration fails on the first duplicate and rolls back.

## Caveats

These come from data the Python schema never stored. The dry run's warnings
list the rows affected in your database.

- **(a)** A received message that is a single attachment with no text gets
  part id `""` instead of `att_0`. Only the internal key differs.
- **(b)** Attachment numbering may differ from a fresh sync: the Python
  bridge left no gap for attachments that failed to bridge.
- **(c)** Space room memberships can't be rebuilt. They come back on their
  own at the first chat-list sync after the bridge starts.
- **(d)** Users with no stored cookies get no login and must send `login`
  again. That includes everyone logged in before the Python bridge's v08
  upgrade, which wiped all cookies.
- **(e)** A warning that a puppet's `custom_mxid` matched no user means that
  double-puppet token wasn't carried over; that user runs `login-matrix`.
- **(f)** A message whose sender has no puppet row is skipped with a warning.
  It shouldn't happen in a healthy database; check the source if it does.

## Afterwards

1. Start the bridge with the same config.
2. Open a migrated room and check that recent messages, including one with an
   attachment, look right.
3. Check the logins carried over (`list-logins`) and that sending works
   without a new `login`.
4. With double puppeting, check that your own messages show as you.
5. Keep the Python backup until you're satisfied.
