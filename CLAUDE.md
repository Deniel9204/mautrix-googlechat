# mautrix-googlechat (Go rewrite)

Go reimplementation of the Python mautrix-googlechat bridge on mautrix-go bridgev2.
Spec: `docs/superpowers/specs/2026-07-14-googlechat-go-bridge-design.md` (approved).
Protocol/framework truth: `docs/research/01`–`08` — READ THE RELEVANT REPORT BEFORE
IMPLEMENTING; do not re-derive protocol facts from memory.

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
- Milestone check: `/verify-milestone`

## Reference code (read-only, at ../_reference/)

- `googlechat-python/` — the Python bridge; maugclib is the protocol spec
- `googlechat-megabridge/` — upstream's unfinished Go rewrite (adopt leaves, not spine;
  see docs/research/08-megabridge-assessment.md before copying anything)
- `mautrix-go/` — bridgev2 framework source · `meta/` — the blueprint bridge
- `purple-googlechat/` — actively-maintained C client; protocol-drift reference

## Workflow

Roadmap M0–M7 in spec §8. One milestone at a time; `/port-module` for porting tasks;
`gchat-port-auditor` agent reviews every ported module before merge.
