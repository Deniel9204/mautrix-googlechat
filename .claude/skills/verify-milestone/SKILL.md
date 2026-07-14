---
name: verify-milestone
description: Run the current milestone's full verification - build, vet, tests, layering rules - then print the manual live-acceptance checklist. Use before declaring any milestone done.
---

# Verify milestone

1. Automated gate (all must pass, in order):
   - `gofmt -l .` -> empty
   - `go build -tags goolm ./...`
   - `go vet -tags goolm ./...`
   - `go test -tags goolm ./...`
   - Layering: `grep -rn "maunium.net/go/mautrix" pkg/gchatmeow/ --include="*.go" | grep -v _test.go`
     -> MUST be empty (gchatmeow never imports bridgev2)
   - `grep -rn "net/http" pkg/connector/ --include="*.go" | grep -v _test.go`
     -> MUST be empty (connector never does HTTP)
2. Find the current milestone's Exit criteria in spec §8
   (`docs/superpowers/specs/2026-07-14-googlechat-go-bridge-design.md`) and the
   matching plan in `docs/superpowers/plans/`.
3. Print a numbered manual checklist of the live-acceptance steps for the owner
   (login, message round-trips, etc. on their continuwuity server), including exact
   commands/configs they need. Do NOT claim the milestone complete — that's the
   owner's call after live testing.
