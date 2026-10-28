# mautrix-googlechat (Go)

A Matrix–Google Chat puppeting bridge, built on
[mautrix-go bridgev2](https://github.com/mautrix/go).

Status: pre-alpha, under active development.

This project is not affiliated with, endorsed by, or supported by Google.
"Google Chat" is a trademark of Google LLC.

If this bridge is useful to you, please consider starring it on GitHub.

[![Star on GitHub](https://img.shields.io/github/stars/Deniel9204/mautrix-googlechat?style=social)](https://github.com/Deniel9204/mautrix-googlechat/stargazers)

## Features

| Feature | Status |
| --- | --- |
| Text, rich formatting, mentions | Supported |
| Threads and replies | Supported |
| Edits, deletes, reactions | Supported |
| Read receipts, typing notifications | Supported |
| Media, both directions | Supported (outbound can be disabled with `network.disable_outbound_media`) |
| Room renames and topic changes | Supported |
| Membership (invites, joins, leaves, kicks) | Supported; leave a space with the `leave` command |
| Per-chat mute | Supported; `mute` / `unmute` in a room |
| History backfill | Supported, opt-in |

The full feature list is in [ROADMAP.md](ROADMAP.md).

## Requirements

- A Matrix homeserver with Application Service support
- PostgreSQL or SQLite (`sqlite3-fk-wal`)
- A Google account with Google Chat (cookie login, see
  [docs/authentication.md](docs/authentication.md))
- Go 1.26+ only if you build from source

## Setup

### Docker

Images are published to `ghcr.io/deniel9204/mautrix-googlechat`
(`:latest`, or a release such as `:v26.09.2`).

```sh
# First run writes /data/config.yaml and exits; edit it.
docker run --rm -v $(pwd)/data:/data ghcr.io/deniel9204/mautrix-googlechat

# Generate the appservice registration.
docker run --rm -v $(pwd)/data:/data --entrypoint /usr/bin/mautrix-googlechat \
    ghcr.io/deniel9204/mautrix-googlechat -c /data/config.yaml -g -r /data/registration.yaml

# Run the bridge.
docker run -v $(pwd)/data:/data ghcr.io/deniel9204/mautrix-googlechat
```

### From source

```sh
./build.sh                                                  # always builds with -tags goolm
./mautrix-googlechat -e -c config.yaml                      # write an example config
./mautrix-googlechat -g -c config.yaml -r registration.yaml # generate the registration
./mautrix-googlechat -c config.yaml                         # run
```

Prebuilt binaries are attached to each
[GitHub release](https://github.com/Deniel9204/mautrix-googlechat/releases).

### Then

1. In `config.yaml`, set at least `homeserver.address`, `homeserver.domain`,
   `appservice.address`, `database.uri`, and give your account `user` access
   in `bridge.permissions`.
2. Register `registration.yaml` with your homeserver and restart it
   ([how](https://docs.mau.fi/bridges/general/registering-appservices.html)).
3. DM the bridge bot and send `login` (elsewhere use the command prefix,
   e.g. `!gc login`). See [docs/authentication.md](docs/authentication.md).

## Configuration

Bridge-specific options live under `network:`; the rest is the standard
bridgev2 config. The ones worth knowing:

- `network.initial_chat_sync`: how many recent chats get rooms after login.
- `network.disable_outbound_media`: refuse Matrix → Google Chat media.
- `network.disable_inline_url_media`: don't download and inline the media a
  link chip (GIF, preview) points at.
- `network.skip_leave_confirmation`: make `leave` act without asking for
  `leave confirm`.
- `backfill.enabled` plus `backfill.max_initial_messages` /
  `backfill.threads.max_initial_messages` above zero: enables history
  backfill.
- `bridge.bridge_matrix_leave` (default off): while off, leaving a room on
  Matrix does not leave the Google Chat space, and the room comes back on the
  next sync. Use the `leave` command instead; it leaves with all your logins.
- `bridge.mute_only_on_create` (default on): Google Chat mutes reach Matrix
  only when the room is created. Set to `false` to follow them on every sync.
  Muting on Matrix needs double puppeting (`double_puppet.secrets`).

## Migrating from the Python bridge

`--migrate-from-python` imports the old `mautrix/googlechat` database:
portals, ghosts, messages, reactions, users, logins and double puppeting.
Read [docs/migration.md](docs/migration.md) first: stop the Python bridge and
back up both databases.

## License

AGPL-3.0, see [LICENSE](LICENSE).
