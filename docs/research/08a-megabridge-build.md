# 08a — Megabridge build health & dependency staleness

**Scope:** empirical build/vet/test health of the upstream Go rewrite
(`megabridge` branch, cloned at
`/Users/danielagoston/utmshared/Projects/git/generalclaude/mautrix/_reference/googlechat-megabridge`,
HEAD `c589550` 2025-04-30), dependency staleness vs the current mautrix-go
checkout (`/Users/danielagoston/utmshared/Projects/git/generalclaude/mautrix/_reference/mautrix-go`,
`v0.28.1-20-gf653177`, 2026-07-12), and the cost of rebasing onto it.
Complements report 06 (ecosystem recon) and report 07 §6 Q1 (fork vs green-field).

**Toolchain used:** `go version go1.26.3 linux/arm64` (installed; network access to
proxy.golang.org confirmed — all pinned modules downloaded on first build).

**Re-verified 2026-07-14:** every experiment re-run from scratch; two experiments
added — a build/test against the **released `v0.28.1` tag** (not just the local
main checkout) and a **compile-time interface-assertion harness** that rules out
silent loss of runtime-asserted optional interfaces (§3.1). All conclusions held.

---

## TL;DR

1. **Megabridge builds, vets, and tests clean today** with `-tags goolm`. The only
   failure without the tag is the libolm cgo header (`olm/olm.h` missing on this
   machine), which is an environment issue, not a code issue — upstream's own
   `Dockerfile:3` installs `olm-dev` for exactly this reason.
2. **The v0.23.3 → v0.28.1 rebase is empirically zero-diff at the compile level.**
   A scratch copy of megabridge was built against both the **released v0.28.1
   tag** and the **current main checkout** (`replace` directive): `go mod tidy`,
   `go build -tags goolm ./...`, `go vet -tags goolm ./...`, and
   `go test -tags goolm ./...` all pass **with no source changes whatsoever**.
   A compile-time assertion harness (§3.1) additionally proves all seven
   runtime-asserted bridgev2 interfaces megabridge implements are still
   satisfied — no silent capability loss. Every breaking bridgev2 interface
   change in the past 15 months (there are two significant ones) lands on
   interfaces megabridge does not implement.
3. **Rebase effort: S** (hours, mechanical) for the compile-level rebase;
   **S–M** overall once behavioral re-verification against the live service is
   included (a handful of framework semantics changed underneath, §5).
4. Risk 10 from report 07 ("framework churn / very stale v0.23.3 pin") is
   **downgraded**: the staleness is real (5 minor versions, ~15 months) but the
   rebase cost is trivially small.

---

## 1. Build health (as pinned, mautrix-go v0.23.3)

All commands run in
`/Users/danielagoston/utmshared/Projects/git/generalclaude/mautrix/_reference/googlechat-megabridge`.

| Command | Result |
|---|---|
| `go build ./...` | **1 failure**, categorized below (cgo/environment, not code) |
| `go build -tags goolm ./...` | **PASS** — zero errors, zero warnings |
| `go vet -tags goolm ./...` | **PASS** — zero findings |
| `go test -tags goolm ./...` | **PASS** — 2 test packages ok, 5 packages `[no test files]` |

### 1.1 The single default-build error (environment, not code)

```
# maunium.net/go/mautrix/crypto/libolm
/home/.../go/pkg/mod/maunium.net/go/mautrix@v0.23.3/crypto/libolm/error.go:4:11:
fatal error: olm/olm.h: No such file or directory
```

(The specific file named varies between runs — any cgo file in
`crypto/libolm` hits the same missing `olm/olm.h` include first.)

Category: **cgo system-dependency**, inside the *dependency* (mautrix-go's
`crypto/libolm`), not in megabridge code. Standard for every bridgev2 bridge:
either install libolm headers (upstream `Dockerfile:3` does
`apk add ... olm-dev`; `Dockerfile:14` ships runtime `olm`) or build with the
pure-Go `goolm` implementation via `-tags goolm`. Note upstream's `build.sh`
does **not** pass `-tags goolm`, so a bare-metal `./build.sh` requires libolm
installed (`build.sh:4`).

No other compile error exists in the entire tree: all 7 packages
(`cmd/mautrix-googlechat`, `pkg/connector`, `pkg/gchatmeow`,
`pkg/gchatmeow/proto`, `pkg/msgconv`, `pkg/msgconv/gchatfmt`,
`pkg/msgconv/matrixfmt`) compile.

### 1.2 Dependency download

Network OK. All 27 pinned modules (mautrix v0.23.3, x/net v0.39.0,
go.mau.fi/util v0.8.6, protobuf v1.36.6, zerolog v1.34.0, etc.) downloaded
without error on first `go build`.

---

## 2. go.mod inventory and staleness

Source: `/Users/danielagoston/utmshared/Projects/git/generalclaude/mautrix/_reference/googlechat-megabridge/go.mod`
(module `go.mau.fi/mautrix-googlechat`, `go 1.23.0`, toolchain `go1.24.2`).

### 2.1 Direct dependencies

| Dependency | Pinned | After rebase (`go mod tidy` vs current mautrix-go) | Staleness |
|---|---|---|---|
| `maunium.net/go/mautrix` | **v0.23.3** (2025-04-16) | v0.28.1-20-gf653177 (2026-07-12) | **5 minor versions / ~15 months** |
| `go.mau.fi/util` | v0.8.6 | v0.9.10 | 1 minor version; tidy bumps it transparently |
| `golang.org/x/net` | v0.39.0 | v0.56.0 | routine |
| `google.golang.org/protobuf` | v1.36.6 | v1.36.11 | patch-level |
| `github.com/rs/zerolog` | v1.34.0 | v1.35.1 | patch-level |
| `github.com/stretchr/testify` | v1.10.0 | v1.11.1 | test-only |

### 2.2 Indirect dependencies worth noting

- `github.com/gorilla/mux v1.8.0` and `github.com/gorilla/websocket v1.5.0`
  (megabridge `go.mod:20-21`) **disappear entirely after the rebase**: mautrix-go
  v0.25.0 replaced gorilla/mux with net/http and (v0.26.x) websockets with
  `github.com/coder/websocket` (CHANGELOG.md:367,
  "Breaking change (appservice,bridgev2,federation) Replaced gorilla/mux").
  Megabridge never imports either directly, so this is transparent — confirmed by
  the tidied scratch go.mod, which lists `github.com/coder/websocket v1.8.15`
  and no gorilla modules.
- Everything else is leaf-level (sqlite3, lib/pq, tidwall/*, goldmark,
  zeroconfig, lumberjack) and bumps cleanly.
- The rebase requires raising the `go` directive from `1.23.0` to `1.25.0`
  (current mautrix-go `go.mod:3`); Go 1.26.3 satisfies it.

---

## 3. Empirical rebase test (the decisive experiment)

Instead of only diffing interfaces on paper, I compiled megabridge against the
new mautrix-go **twice** — once against the local main checkout, once against
the released tag:

```sh
cp -r googlechat-megabridge $SCRATCH/mb-rebase && cd $SCRATCH/mb-rebase

# Experiment A: current main (v0.28.1-20-gf653177, 2026-07-12)
go mod edit -replace maunium.net/go/mautrix=/Users/.../_reference/mautrix-go
go mod tidy          # PASS (auto-bumps go directive to 1.25.0; pulls util
                     #       v0.9.10 + coder/websocket, drops both gorilla mods)
go build -tags goolm ./...   # PASS — zero errors
go vet   -tags goolm ./...   # PASS — zero findings
go test  -tags goolm ./...   # PASS — same 2 packages ok

# Experiment B: released tag (what a real fork would pin)
go mod edit -dropreplace maunium.net/go/mautrix
go mod edit -require maunium.net/go/mautrix@v0.28.1
go mod tidy && go build -tags goolm ./... && go test -tags goolm ./...  # all PASS
```

**Result: zero source-code changes needed** against either the v0.28.1 release
or current main. The scratch copy lives at
`/tmp/claude-501/.../scratchpad/mb-rebase` for inspection (its final `go.mod`
pins `maunium.net/go/mautrix v0.28.1`, `go 1.25.0`).

### 3.1 Guarding against silent optional-interface loss

A clean compile is **not** sufficient on its own: bridgev2 discovers optional
capabilities (edits, reactions, typing, receipts, …) via **runtime type
assertions**, so a changed method signature would not fail the build — the
capability would just silently vanish. To rule this out I added a compile-time
assertion file to the scratch copy:

```go
var (
    _ bridgev2.NetworkConnector              = (*GChatConnector)(nil)
    _ bridgev2.NetworkAPI                    = (*GChatClient)(nil)
    _ bridgev2.EditHandlingNetworkAPI        = (*GChatClient)(nil)
    _ bridgev2.RedactionHandlingNetworkAPI   = (*GChatClient)(nil)
    _ bridgev2.ReactionHandlingNetworkAPI    = (*GChatClient)(nil)
    _ bridgev2.TypingHandlingNetworkAPI      = (*GChatClient)(nil)
    _ bridgev2.ReadReceiptHandlingNetworkAPI = (*GChatClient)(nil)
)
```

This compiles cleanly under **both** v0.23.3 and current main → every interface
megabridge satisfies today it still satisfies after the rebase. (Only
`bridgev2.LoginProcessCookies` is asserted upstream, at
`pkg/connector/login.go:51`; the other seven were unguarded. A fork should
commit these assertions.) Side finding: `bridgev2.MaxFileSizeingNetwork` is
**not** implemented in either version — file-size limits are never propagated
to the connector.

---

## 4. bridgev2 interface drift v0.23.3 → v0.28.1: what megabridge touches

### 4.1 Interfaces megabridge implements (with citations) — all unchanged

Every row below is empirically compile-verified against both versions via the
assertion harness in §3.1, not just paper-diffed.

| Interface (current def in `_reference/mautrix-go/bridgev2/networkinterface.go` unless noted) | Megabridge implementation | Changed since v0.23.3? |
|---|---|---|
| `NetworkConnector` (networkinterface.go:217) | `GChatConnector` — `pkg/connector/connector.go:19-50` (Init/Start/GetName/GetCapabilities/GetBridgeInfoVersion/GetConfig/GetDBMetaTypes) + `pkg/connector/login.go:21-33` (CreateLogin/GetLoginFlows/LoadUserLogin) | **No** (method set identical) |
| `NetworkAPI` (networkinterface.go:381; `Connect(ctx)` still error-less at :386) | `GChatClient` — `pkg/connector/client.go:43-179`, `HandleMatrixMessage` at `pkg/connector/handlematrix.go:22` | **No** |
| `EditHandlingNetworkAPI` (networkinterface.go:616) | `pkg/connector/handlematrix.go:127` | **No** |
| `RedactionHandlingNetworkAPI` (networkinterface.go:649) | `pkg/connector/handlematrix.go:149` | **No** |
| `ReactionHandlingNetworkAPI` (networkinterface.go:631) | `pkg/connector/handlematrix.go:165,174,180` | **No** |
| `TypingHandlingNetworkAPI` (networkinterface.go:676) | `pkg/connector/handlematrix.go:210` | **No** |
| `ReadReceiptHandlingNetworkAPI` (networkinterface.go:656) | `pkg/connector/handlematrix.go:229` | **No** (struct gained additive `Implicit` field, networkinterface.go:1482) |
| `LoginProcessCookies` (login.go:58) | `GChatCookieLogin` — assertion at `pkg/connector/login.go:51`, methods :53-96 | **No** |

Supporting bridgev2 surface megabridge consumes, also compatible: `simplevent`
event structs (`pkg/connector/handlegchat.go:18-132`, `pkg/connector/client.go:268`),
`status.BridgeState` (`pkg/connector/client.go:45-62`), `database.MetaTypes`
(`pkg/connector/connector.go:50`), `event.RoomFeatures` capabilities
(`pkg/connector/client.go:154`), `ConvertedEdit` (`pkg/connector/handlegchat.go:175`).
Note megabridge does **not** implement `BackfillingNetworkAPI`/`FetchMessages`
(no hits in the tree) — it does its own catch-up backfill via `CatchUpGroup` +
`QueueRemoteEvent` (`pkg/connector/portal.go:35-60`).

### 4.2 Breaking changes in networkinterface.go that a rebase would hit — none apply

Full diff of v0.23.3 (module cache) vs current shows exactly **two**
compile-breaking signature changes and one field removal; megabridge implements
none of them:

1. **`MembershipHandlingNetworkAPI.HandleMatrixMembership`**: return type
   `(bool, error)` (v0.23.3 networkinterface.go:747) → `(*MatrixMembershipResult, error)`
   (current networkinterface.go:952). Megabridge has no `HandleMatrixMembership`.
2. **`GroupCreatingNetworkAPI.CreateGroup`**: `(ctx, name string, users ...networkid.UserID)`
   (v0.23.3 networkinterface.go:698) → `(ctx, params *GroupCreateParams)`
   (current networkinterface.go:827). Not implemented.
3. **`MatrixMembershipChange`**: deprecated `TargetGhost`/`TargetUserLogin`
   fields removed (current networkinterface.go:940-944). Not referenced.

Everything else in the diff is **additive**: new optional interfaces
(`ChatViewingNetworkAPI` :666, `DisappearTimerChangingNetworkAPI` :729,
`DeleteChatHandlingNetworkAPI` :739, `MessageRequestAcceptingNetworkAPI` :747,
`StickerImportingNetworkAPI` :1077, `NetworkAPIWithUserID` :422,
`CredentialExportingNetworkAPI` :447, `TransactionIDGeneratingNetwork` :292,
`NetworkResettingNetwork` :324, `PersonalFilteringCustomizingNetworkAPI` :830)
and new struct fields (`ConvertedMessage.ReplyToRoom/-User/-Login`,
`NetworkGeneralCapabilities.ImplicitReadReceipts`/`.Provisioning`,
`FetchMessagesParams.AllowSlowFetch`, `MatrixEventBase.InputTransactionID`,
`MatrixRoomMeta.IsStateRequest`, `EventSender.ForceEditOrigSender`, …).
Additive changes cannot break an implementor.

---

## 5. Behavioral (non-compile) changes a rebase inherits

Compile-clean ≠ runtime-identical. From
`_reference/mautrix-go/CHANGELOG.md`, the bridgev2 semantics that moved under
megabridge between v0.23.3 and v0.28.1, filtered to what it actually uses:

- **v0.24.x (CHANGELOG.md:412)**: `QueueRemoteEvent` now **blocks instead of
  dropping** events when the queue is full — strictly better for megabridge's
  heavy queue usage (`pkg/connector/handlegchat.go`), but changes backpressure
  behavior during large catch-ups.
- **v0.24.x (CHANGELOG.md:410)**: new portal rooms hardcode room v11.
- **v0.26.1 (CHANGELOG.md:227)**: portal creation no longer backfills unless
  `ChatInfo.CanBackfill` is set. Megabridge never sets `CanBackfill` (zero hits
  in `pkg/`) and never used framework backfill, so no regression — but anyone
  finishing the bridge should now set the flag deliberately when adding real
  history backfill.
- **v0.26.1 (CHANGELOG.md:229)**: Matrix reaction handling only deletes the old
  reaction under narrower conditions — touches the `ReactionHandlingNetworkAPI`
  path (`pkg/connector/handlematrix.go:165-208`); re-verify single-reaction
  semantics against `event.RoomFeatures` in `pkg/connector/client.go:154`.
- **v0.25.x (CHANGELOG.md:385)**: Matrix-side "delete for me" events are now
  ignored before reaching `HandleMatrixMessageRemove`.
- **v0.28.0 (CHANGELOG.md:49)**: message sending never sends unencrypted
  messages to encrypted rooms (safety improvement, no action).
- **Join membership type** now carries `IsSelf: true`
  (current networkinterface.go:923) — irrelevant, no membership handling.
- Breaking changes in modules megabridge doesn't import: federation `NewClient`
  (v0.28.0, CHANGELOG.md:25), mediaproxy `GetMediaResponseFile` (v0.26.2,
  CHANGELOG.md:213), client UIA request structs (v0.26.4, CHANGELOG.md:138),
  `id.UserID.ParseAndValidate` split (v0.25.2, CHANGELOG.md:295), gob
  auto-registration removal (v0.27.0, CHANGELOG.md:79-86). All confirmed
  non-issues by the clean scratch build.

---

## 6. Effort estimate for the rebase

| Work item | Effort |
|---|---|
| Bump `go.mod` (mautrix v0.28.1+, `go 1.25.0`), `go mod tidy`, build with `goolm` | **S** (< 1 hour — already proven in §3) |
| Code changes for interface breakage | **S** (zero found) |
| Behavioral re-verification of §5 items (queue backpressure, reaction dedup, room v11) against a live login | **S–M** (~1–2 days, dominated by manual testing since only `msgconv` has tests) |
| **Total rebase cost** | **S, at most S–M** |

The staleness penalty of forking megabridge is therefore **not** a
framework-rebase problem. The real cost driver for the fork decision remains
feature completeness (report 07 gap analysis), not build health.

---

## 7. Other build-health observations

- **Test coverage is thin but green**: exactly 2 test files —
  `pkg/msgconv/gchatfmt/convert_test.go` (`TestParse`: plain,
  bold_italic_strike_underline, emoji) and
  `pkg/msgconv/matrixfmt/convert_test.go` (`TestParse`: Plain, Bold). 5 subtests
  total; both pass on v0.23.3 and on v0.28.1. No tests for `connector`,
  `gchatmeow` (the wire protocol — report 07 risk #2), or `msgconv` top level.
- **No network config**: `GetConfig` returns `("", nil, nil)`
  (`pkg/connector/connector.go:46`) — no example config or upgrader yet; a
  finished bridge needs one.
- **Entry point is standard mxmain** (`cmd/mautrix-googlechat/main.go:22`,
  imports `maunium.net/go/mautrix/bridgev2/matrix/mxmain`), matching the meta
  blueprint (report 05), so the rebase also inherits mxmain improvements for free.
- `Connect` swallows the initial-connect error into a `BadCredentials` bridge
  state rather than returning it (`pkg/connector/client.go:43-66`) — consistent
  with the error-less `Connect(ctx)` signature, unchanged in v0.28.1.
