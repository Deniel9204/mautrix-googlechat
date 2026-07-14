# 08 — Megabridge Assessment: Fork vs Green-field (Executive Summary)

**Answers:** report 07 §6 Q1 ("Megabridge: finish/fork or green-field?").
**Synthesizes:** 08a (build health & rebase), 08b (connector audit), 08c (client-lib
fidelity audit), 08d (msgconv audit). All claims below are grounded in those four
documents; citations are `08x §n` plus the underlying `file:line` where load-bearing.

**Subject:** `mautrix/googlechat@megabridge` branch, HEAD `c589550` (2025-04-30), at
`/Users/danielagoston/utmshared/Projects/git/generalclaude/mautrix/_reference/googlechat-megabridge`.

---

## Bottom line

**Recommendation: Option (b) — green-field repo skeleton on current mautrix-go,
adopting megabridge modules selectively.** Adopt `pkg/msgconv` nearly wholesale
(then a 2–4 day hardening pass), lift `api.go`, the proto schema (regenerated with
explicit presence), `login.go`'s cookie flow, and `handlematrix.go`'s handler
patterns; treat `channel.go`/`session.go` as a wire-choreography *reference* while
rewriting them; rewrite `pkg/connector` outright with Python-compatible IDs pinned
in M0. Do **not** fork the tree as the M0 base, and do **not** green-field from zero.
Reasoning in §4; the decision does not hinge on staleness (§2 kills that concern) —
it hinges on which of megabridge's decisions are load-bearing and wrong.

---

## 1. Overall completeness map

Percentages are the audits' own weighted estimates. "Written" = code exists and is
recognizably the right shape; "usable as-is" = would survive contact with production
without rework.

| Layer | Written | Usable as-is | Blockers (what makes it unusable as-is) |
|---|---|---|---|
| **Build / deps** (08a) | 100% — builds, vets, tests clean with `-tags goolm` at pin *and* at v0.28.1/main | 100% | None. Sole `go build ./...` failure is the libolm cgo header (`olm/olm.h`), an environment issue (08a §1.1). |
| **`pkg/connector`** — bridgev2 glue, 1,115 LoC (08b) | ~50% | **~35–40%** | (1) Portal ID = prototext of `GroupId` — deliberately unstable serialization as a permanent DB key, diverges from Python's `dm:`/`space:` (blocks legacy migration, 07 risk #6), and `Receiver` set for spaces so space portals aren't shared between users (08b §1.7). (2) Chat-list sync nil-derefs on the first DM (`&dmUser.Name`, client.go:255; author's own `NOTE(skip)` comments, 08b §3.2). (3) Cookie rotation never persisted — no `login.Save()` anywhere; restart after rotation = dead login (08b §1.3). (4) No initial history backfill (`FetchMessages` absent; no ListTopics/ListMessages RPCs to build on). (5) `GetConfig` total stub; `IsLoggedIn` hardcoded `true`; `Disconnect`/`LogoutRemote` empty; SYSTEM_MESSAGE parsing (Python's source of membership/rename/topic) explicitly dropped (handlegchat.go:43). |
| **`pkg/gchatmeow`** — client lib (08c) | ~60% | lower — "cannot run unattended" | (1) Channel permanently dies after ~8 min: shared `http.Client` 90s timeout aborts every long poll, `pushTimeout` defined but never used, retries never reset, `Listen` fired-and-forgotten with error discarded (08c §1.4). (2) Chunk parser corrupts split multibyte UTF-8 (U+FFFD baked into buffer → mis-framed chunks, 08c §1.3). (3) No cookie-rotation readback (doc 01 blocker). (4) `ErrSIDInvalid` retries the dead SID instead of re-registering/propagating (08c §1.5). (5) Event fan-out is one-goroutine-per-observer — stream ordering destroyed; AID acked before handling (08c §1.9). (6) 4 of 16 RPCs missing: presence, `catch_up_user`, `list_topics`, `list_messages` — no gap recovery, no backfill substrate (08c §4). (7) pblite (shared go.mau.fi/util lib) lacks maugclib's trailing-sparse-dict + permissive decode → whole `StreamEventsResponse` silently dropped on one bad value (08c §2). (8) `NewSession` args swapped at only call site — UA/proxy support effectively unimplemented (08c §3). |
| **`proto/`** — schema (08c §6) | ~95% structural (identical 173 messages / 21 enums, oneofs preserved) | risky | Converted proto2→proto3 with zero `optional` keywords — every scalar loses presence (unset vs zero indistinguishable, zeros never serialized): a semantic time bomb for a reverse-engineered schema whose ground truth is proto2. `font_color` int32→uint32. Fix is mechanical regeneration, not redesign. |
| **`pkg/msgconv`** — formatting/media (08d) | ~60% (gchatfmt ~75%, matrixfmt ~60%, media ~70%, previews 0%) | **adopt-and-fix: yes** | 4 confirmed one-function bugs (B1 href injection, B2 mention pills target DM *rooms*, B3 no chip_render_type filter → every inline link fetched via bare `http.Get` (SSRF) and re-posted as `m.image`, B4 formatted caption wipes the upload annotation). Missing matrixfmt arms: underline, font color, URL annotations, MONOSPACE_BLOCK, `@room`, list annotations. Crucially, the two problems 07 ranked as the top formatting risk — UTF-16 code-unit indexing and overlapping-span normalization — are **implemented and empirically verified correct**, astral-plane cases included (08d §1.2–1.3). |
| **Tests** (08a §7, 08d §3) | ~15% of the corpus 07 demands | — | 2 test files, 5 subtests total; nothing for connector or the wire protocol; every confirmed bug lives in a path the tests never touch. The table-driven harness itself is good. |

**Weighted overall: ~55% of the parity surface is written; ~40% is usable as a
foundation without rework.** The missing/broken 45–60% is not uniformly distributed —
it is concentrated in exactly what makes a long-lived bridge viable: channel survival,
gap recovery, credential rotation, event ordering, backfill, and the ID scheme.

---

## 2. Rebase cost onto current mautrix-go: empirically ~zero

08a's decisive experiment removes staleness from the decision entirely:

- A scratch copy of megabridge compiled against **both** the released `v0.28.1` tag and
  current mautrix-go main (`f653177`, 2026-07-12) passes `go build`, `go vet`, and
  `go test` (all `-tags goolm`) with **zero source changes** — only `go.mod` edits
  (mautrix bump, `go` directive 1.23.0 → 1.25.0) (08a §3).
- Because bridgev2 discovers optional capabilities via **runtime type assertions**, a
  clean compile alone could hide silent capability loss. A compile-time assertion
  harness proves all 7 runtime-asserted interfaces megabridge implements
  (NetworkConnector, NetworkAPI, Edit/Redaction/Reaction/Typing/ReadReceipt; plus the
  already-asserted LoginProcessCookies) are still satisfied under both pins (08a §3.1).
  Any adopted code should commit these assertions.
- The only two compile-breaking bridgev2 signature changes in 15 months
  (`HandleMatrixMembership`, `CreateGroup`) land on interfaces megabridge does not
  implement (08a §4.2). gorilla/mux+websocket drop out transparently via `go mod tidy`.
- Residual cost is **behavioral**, not compile-level: `QueueRemoteEvent` now blocks
  instead of dropping, portal creation no longer auto-backfills without
  `ChatInfo.CanBackfill` (megabridge never sets it), narrower Matrix-reaction dedup,
  room v11 hardcode (08a §5).

**Effort: S (<1 hour, mechanical, already proven) for the compile-level rebase; S–M
(~1–2 days) including behavioral re-verification.** Report 07's risk #10 ("framework
churn / very stale pin") is downgraded. Consequence for this decision: *every* option
below starts from current mautrix-go at the same near-zero framework cost — the fork
question is decided purely on code quality and which decisions we'd inherit.

---

## 3. The three options

Baseline for effort deltas: 07 §4's milestone plan (M0 skeleton → M1 client-lib core
[the long pole] → M2 text → M3 formatting → M4 mutations → M5 media → M6 backfill →
M7 polish/migration), where 07 rated both formatting directions L-effort and M1 the
riskiest stretch.

### Option (a) — Fork megabridge into this repo as the M0 base

Start from the megabridge tree, rebase (proven trivial), then fix and finish in place.

**Pros**
- Day-one compiling, testing bridge against v0.28.1/main with zero source changes (08a §3);
  inherits mxmain wiring, Dockerfile, build.sh for free.
- All the genuinely strong assets are already in place: msgconv's solved hard core
  (08d §6), `handlematrix.go` at ~80% credible (08b §1.4), the polished
  `LoginProcessCookies` flow (08b §1.3), faithful wire choreography and 12/16 RPC
  wrappers (08c §1.1–1.2, §4), structurally complete proto, faithful resumable upload.
- Same module path/architecture as upstream → cleanest story for upstreaming patches
  (07 risk #8).

**Cons**
- **You inherit wrong load-bearing decisions and must reverse them inside a living
  tree.** The portal-ID format is permanent DB contents (07 risk #6): every day the
  fork runs with prototext IDs is a day of DBs that need re-IDing; the receiver-scoping
  bug means the *portal table shape itself* is wrong for spaces (08b §1.7). Fixing this
  is not additive work — it invalidates `ids.go`, `mapping.go`, `portal.go`,
  `GetCapabilities`, and the catch-up path simultaneously.
- 08c's verdict is that `channel.go`/`session.go` need **rewriting, not patching**
  ("treating gchatmeow as a reference … is more realistic than patching it
  incrementally", 08c §7). A fork psychologically and practically biases toward
  incremental patching of code the audit says to replace.
- The connector — the layer a fork would nominally save — is the *thinnest* layer
  (1,115 LoC) at only ~35–40% usable (08b §4), and 08b's explicit signal is that
  rewriting it costs little relative to untangling it.
- Inherits debug scaffolding (full message payloads logged to stdout, `log.Printf`
  throughout, `NOTE(skip)` markers), a known-crashing connect path, and a proto that
  needs regeneration anyway.
- No `GetConfig`, no gcid tests, no example config — M0's exit criteria are *not*
  actually met by the fork; you'd be retrofitting M0 underneath M2-level code.

**Effort delta:** saves ~1 week of scaffolding vs (b) up front, then pays it back with
interest: reversing the ID scheme + receiver scoping (~3–5 days of surgery through
5 files plus any accumulated test data), rewriting channel/session anyway (~1–2 weeks,
same as (b)), plus ongoing friction from dead/debug code. Net vs (b): **roughly even
on calendar, strictly worse on risk** (wrong IDs existing in history; patching bias).

### Option (b) — Green-field skeleton, selectively adopt proven modules  ← recommended

Fresh repo/module at current mautrix-go (mxmain template per 07 M0), with per-module
adoption based on the audits' per-module verdicts:

| Megabridge module | Disposition | Grounding |
|---|---|---|
| `pkg/msgconv` (all of gchatfmt/matrixfmt/from-gchat) | **Adopt wholesale**, then the 2–4 day hardening pass (fix B1–B4, add missing case-arms, build the corpus on the existing harness) | 08d §6: "adopt-and-fix: yes"; the risky 40% (UTF-16, normalization, EntityString) is done and verified; remaining work is enumerable |
| `proto/googlechat.proto` | **Adopt, regenerate** with explicit presence (proto2 or proto3+`optional` everywhere), fix `font_color` back to int32 | 08c §6: structure identical to Python's; only the conversion policy is wrong |
| `api.go` (wire format, 12 RPC wrappers, resumable upload) | **Adopt**; add the 4 missing RPCs (presence, catch_up_user, list_topics, list_messages), mutex the counter/xsrf | 08c §4–5: wire format exact, upload faithful |
| `login.go` cookie flow + `cookies.go` | **Adopt** (5-cookie fields, domain/pattern hints); fix the post-login client-swap (08b §1.3) | 08b: "most polished file in the package" |
| `handlematrix.go` | **Adopt as pattern/starting point** for the 7 Matrix→GC handlers (thread routing, reply targets, µs timestamps) | 08b §1.4: ~80% credible, broadest solid area |
| `channel.go` / `session.go` | **Reference only — rewrite.** Keep the choreography knowledge (register/SID/ack/ping, UTF-16 counting basis); rewrite lifecycle, chunk buffering (bytes, not string round-trips), error ladder, ordering, rotation readback | 08c §7 verdict; the six §1 blockers are structural |
| `pkg/connector` (rest: ids, mapping, portal, client, handlegchat) | **Rewrite** against current bridgev2, pinning Python's `dm:`/`space:` IDs + receiver rule in M0 (07 §1.3) | 08b §4 fork-vs-greenfield signal |
| pblite | Keep the shared `go.mau.fi/util/pblite`; contribute trailing-sparse-dict + permissive-decode upstream (or wrap locally) | 08c §2: it's the battle-tested gmessages codec, two behaviors short |

**Pros**
- Gets 100% of the audited value (the solved-hard-problems set) while inheriting 0% of
  the load-bearing mistakes. The wrong portal-ID format never exists in our history;
  legacy Python-DB migration (07 §6 Q2) stays possible.
- M0 done properly (config gen, gcid package + tests, ID formats pinned) *before*
  anything persists to a DB — exactly the ordering 07 risk #6 demands.
- Every adopted file crosses the boundary through an already-completed audit — we know
  precisely which lines are wrong (B1–B4 have file:line fixes attached).
- License/provenance clean: both trees are AGPL mautrix-org code; per-file adoption with
  attribution keeps upstreaming viable (07 risk #8 still says: contact
  `#googlechat:maunium.net` before writing code — that advice stands under any option).

**Cons**
- ~2–3 days more scaffolding than (a) up front (mxmain skeleton, gcid, config plumbing).
- Slightly weaker "same tree as upstream" story than (a) for upstreaming — mitigated by
  keeping the same architecture and package layout.
- Adoption requires discipline: import-path rewrites, committing the 08a §3.1 interface
  assertions, and resisting the temptation to also "just copy" channel.go.

**Effort delta:** vs (c), saves the two L-rated formatting items (07 §1.1/1.2 rows) plus
proto conversion, upload protocol, wire-format derivation, and login flow — conservatively
**4–6 weeks of M1/M3/M5 work**. vs (a), costs ~2–3 days of skeleton and saves the ID/
receiver surgery and the patch-vs-rewrite friction; net **even-to-better on calendar,
strictly better on risk**.

### Option (c) — Pure green-field (megabridge as documentation only)

**Pros**
- Maximum freedom; zero inherited defects; uniformly current idioms from line one.

**Cons**
- Re-solves problems that are *empirically solved and verified* in the branch: UTF-16
  code-unit indexing and span normalization (07's #5 risk — 08d proved them correct
  including astral cases), the EntityString architecture, the exact RPC wire format
  (`c/rt=b/alt=proto/key`, xsrf, client_version), the resumable-upload dance, the
  register/SID/ack/ping choreography, and a 173-message proto conversion.
- Both formatting directions were rated L-effort from scratch (07 §1); 08d puts the
  adopt-and-fix alternative at 2–4 days. Discarding verified-correct code to rewrite it
  is pure waste plus fresh-bug risk in the parts megabridge got *right*.
- No offsetting benefit: (b) already delivers the "clean foundation" property, since
  everything structurally unsound is rewritten under (b) anyway.

**Effort delta:** **+4–6 weeks over (b)** with strictly higher regression risk in the
formatting and wire layers. Dominated by (b) on every axis.

---

## 4. Recommendation

**Choose (b): green-field skeleton on current mautrix-go with selective adoption, per
the disposition table above.** Reasoning:

1. **Staleness is a non-factor, so the fork's headline advantage evaporates.** The one
   thing a fork uniquely offers — avoiding a painful rebase — turns out to be worth
   less than a day (08a §3: zero-diff against v0.28.1 *and* main, interface assertions
   green). What remains of option (a) is "inherit the tree", and the audits show the
   tree's spine is the wrong part to inherit.
2. **The value and the defects live in different layers, and the layering is decisive.**
   The proven value is in leaf modules that don't import bridgev2 (msgconv, api.go,
   proto, the choreography knowledge in channel.go) — exactly the modules that copy
   cleanly into a new skeleton. The structural defects are in the spine: the connector's
   ID/receiver scheme (permanent DB contents, wrong — 08b §1.7), the channel/session
   lifecycle (cannot survive 8 minutes — 08c §1.4), and event ordering (08c §1.9).
   A fork keeps the spine and copies nothing; (b) keeps the leaves and rebuilds the
   spine. The audits' own per-layer verdicts say the same thing three times: 08b —
   rewrite the connector, lift handlematrix/login; 08c — reference, don't patch,
   channel/session; 08d — adopt msgconv outright.
3. **The ID decision alone justifies (b) over (a).** 07 risk #6 makes networkid formats
   an M0-pinned, migration-blocking, permanent decision. Megabridge's prototext IDs are
   not merely unfinished — they are actively corrupting-by-design (unstable across
   protobuf releases) and preclude both legacy migration and multi-user spaces. Under
   (a) that scheme is live in the repo until someone excises it from five files; under
   (b) it never exists.
4. **(c) is dominated.** Every clean-slate benefit of (c) is preserved by (b) — the
   spine is new either way — while (b) banks 4–6 weeks of verified work, including the
   two problems the gap analysis ranked as the top formatting risk.

**Immediate next steps if accepted:**
1. Contact `#googlechat:maunium.net` / upstream before writing code (07 risk #8; AGPL
   hygiene, avoid duplicate effort, discuss upstreaming the finished connector).
2. M0 per 07 §4 with IDs pinned to Python's `dm:`/`space:` + receiver rule; commit the
   08a §3.1 compile-time interface assertions from day one.
3. Adopt msgconv + proto (+regeneration) + api.go + login flow per the §3(b) table;
   schedule the 08d hardening pass (B1–B4 + corpus) as part of M3, not M7.
4. Budget M1 as the true long pole: channel/session rewrite with the 08c blocker list
   (§1.3–1.9) as the acceptance checklist, and the live protocol-validation spike
   unchanged from 07.
