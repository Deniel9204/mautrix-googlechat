# mautrix-googlechat

A Matrix–Google Chat puppeting bridge in Go, built on mautrix-go bridgev2.
Architecture, protocol notes, and key decisions: `docs/ARCHITECTURE.md` — read it
before making protocol changes; do not re-derive protocol facts from memory.

## Hard rules (violations are always bugs)

- `pkg/gchatmeow` never imports bridgev2. `pkg/connector` never does HTTP.
  `pkg/msgconv` only converts. `pkg/gcid` formats are FROZEN (permanent DB contents).
- The proto is **proto2**. Never convert to proto3; field presence is load-bearing.
- All text offsets for Google Chat annotations are **UTF-16 code units**
  (JS `String.length`), never bytes or runes.
- Never remove the `-tags goolm` build tag path.
- pblite/stream decode errors must log-and-skip, never kill the channel.

## Commits

Plain conventional-commit messages only (`feat:`/`fix:`/`build:`/`docs:`/`test:`).
Never add Co-Authored-By, AI attribution, or session-link trailers.

## Commands

- Build: `./build.sh` · Test: `go test -tags goolm ./...` · Vet: `go vet -tags goolm ./...`
- Proto regen: `pkg/gchatmeow/proto/gen.sh` (needs buf + protoc-gen-go@v1.36.11)
- Live protocol probe: `go test -tags 'goolm live' -run TestLive ./pkg/gchatmeow/`
  (needs real cookies in `GCHAT_LIVE_*`; never runs in CI)

## Protocol references (read-only, at ../_reference/, re-clone if missing)

- `mautrix-go/` — the bridgev2 framework source
- `purple-googlechat/` — actively-maintained client; the protocol-drift reference
  when Google changes the wire format

## Migration

Existing deployments can import their old database via `--migrate-from-python`
(`pkg/migrate`); see `docs/migration.md`. `pkg/gcid` formats are frozen to keep
migrated ids identical to freshly-synced ones.
