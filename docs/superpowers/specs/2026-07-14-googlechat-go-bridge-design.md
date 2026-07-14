# mautrix-googlechat Go Rewrite — Design

**Date:** 2026-07-14
**Status:** Approved by owner (section-by-section review)
**Research base:** `docs/research/01`–`08` in this repo. This spec records the *decisions*;
the research reports hold the protocol/framework detail and are the canonical reference
for implementation agents.

## 1. Goal & context

Reimplement the mautrix-googlechat bridge (Python, `maugclib`/hangups-based) as a Go
bridge on the mautrix-go **bridgev2** framework, at feature parity with the Python bridge,
deployed on the owner's continuwuity homeserver alongside their existing mautrix-meta
(Go) bridge.

Owner decisions from scoping:

| Question | Decision |
|---|---|
| Takeover of existing Python deployment | **Fresh start now, migration later** (legacy Python-DB migration is an optional M7 item, kept possible by pinning Python-compatible IDs) |
| Audience | **Mautrix-standard, maybe upstream** — mirror mautrix/meta conventions (layout, AGPL, Docker, CI) |
| Google account | **Personal Google account**; a separate test account is available for development |
| Execution mode | **Autonomous per milestone** — agents implement each milestone end-to-end; owner reviews/live-tests between milestones |

## 2. Foundation decision: green-field + selective adoption

An unfinished upstream Go rewrite exists (`mautrix/googlechat` branch `megabridge`,
Dec 2024–Apr 2025). Audit (reports 08a–08d, verdict 08): it compiles and tests green even
against current mautrix-go, but its **spine is structurally defective** (prototext-encoded
portal IDs that are unstable across protobuf versions and block legacy migration; channel
dies permanently after ~8 min; no cookie-rotation persistence; unordered event dispatch;
SYSTEM_MESSAGE dropped; no backfill), while its **leaf modules are proven** (msgconv
UTF-16/normalization core with tests; exact RPC wire format; faithful resumable-upload
port; ~95%-complete proto; cookie-login flow).

**Decision:** new skeleton on current mautrix-go; **adopt** the leaf modules (msgconv
+2–4 days hardening, proto regenerated, `api.go` wire format, login flow,
handlematrix patterns); **rewrite** the spine (connector, channel, session/cookies,
pblite, event dispatch); **pin Python-compatible IDs in M0**. Megabridge's channel/session
stay available as a second reference next to the Python original.

Rejected: (a) wholesale fork — inherits the broken ID scheme as live DB contents plus the
spine bugs as constraints; (c) pure green-field — re-derives 4–6 weeks of verified work.

## 3. Architecture

Layering rule (enforced, reviewed in every milestone): **`pkg/gchatmeow` never imports
bridgev2; `pkg/connector` never does HTTP; `pkg/msgconv` only converts.**

```
cmd/mautrix-googlechat/main.go   mxmain.BridgeMain bootstrap (~30 lines)
pkg/gchatmeow/                   Google Chat client library (zero bridgev2 imports)
  client.go                      RPC surface, split_event_bodies, single event callback
  api.go                         ADOPTED: 12 verified RPC wrappers; ADD: list_topics,
                                 list_messages, catch_up_user, catch_up_group
  session.go, cookies.go         REWRITE: custom cookie jar (unquoted values, rotation
                                 readback), XSRF scrape + 24h refresh, *.google.com guard
  channel.go, chunkparser.go     REWRITE: line-by-line port of Python channel.py —
                                 SID/ack/initial-PingEvent choreography, UTF-16 code-unit
                                 chunk framing (split-rune safe), error ladder, 1.5h recycle
  upload.go, download.go         ADOPT upload; FIX download (cookies on redirect hops)
  pblite/                        REWRITE: permissive protoreflect codec — int64-as-string,
                                 trailing sparse-dict, log-and-skip on bad fields
  proto/                         ADOPTED googlechat.proto, kept proto2, regenerated with
                                 option go_package (presence semantics are load-bearing)
  errors.go, ids.go              sentinel error taxonomy; GroupId <-> string codecs
pkg/connector/                   bridgev2 adapter — REWRITTEN spine
  connector.go, client.go        NetworkConnector/NetworkAPI, connection supervision loop
  login.go                       ADOPTED cookie-login (5 cookies, chat.google.com) +
                                 relogin override + persist rotated cookies post-connect
  handlematrix.go                ADOPTED patterns; fix caption-drops-file bug
  handlegchat.go, events.go      ordered dispatch of 6 event body types + SYSTEM_MESSAGE
                                 (membership deltas, renames) -> RemoteEvents
  backfill.go                    NEW: FetchMessages pagination + catch_up revision replay
  chatinfo.go, userinfo.go, capabilities.go, config.go, dbmeta.go
pkg/msgconv/                     ADOPTED + hardened (4 audit bugs incl. URL-escape security fix)
  gchatfmt/, matrixfmt/          annotations <-> HTML, UTF-16 code-unit offsets
pkg/gcid/                        networkid codecs — PINNED IN M0 (see §4)
build.sh, Dockerfile, example-config.yaml, CI   copied from meta conventions
```

Fixed choices: mautrix-go pinned to the commit mautrix/meta currently pins; **goolm**
build tag (pure-Go crypto, no CGO) as default build; binary/config name
`mautrix-googlechat`; module path `github.com/Deniel9204/mautrix-googlechat` (changeable
until first push).

## 4. Identifiers & persistence (permanent — frozen at M0)

- `PortalKey.ID` = `dm:<group_id>` / `space:<group_id>`; `Receiver` = owner's UserLoginID
  for DMs, `""` for spaces (matches Python exactly → keeps legacy migration possible).
- `UserID` = numeric Gaia ID; `UserLoginID` = own Gaia ID; `MessageID` = GC message_id;
  part IDs: `""` for text part, `att_<n>` for attachments.
- Metadata structs (`GetDBMetaTypes`): `PortalMetadata{Revision, ThreadsOnly,
  ThreadsEnabled}`, `UserLoginMetadata{Cookies, UserAgent, Revision}`,
  `MessageMetadata{TimestampMicro, LastEditTime}`, `GhostMetadata{Email}`.
- Every bridged message stores `(message_id, topic_id, µs timestamp)` — quote-reply
  targets require the original microsecond create_time.
- Rotated cookies re-persisted after every successful connect.

## 5. Data flow

**GC → Matrix:** long-poll chunk → chunk parser (UTF-16 framing) → pblite decode →
`StreamEventsResponse` → `split_event_bodies` → **single ordered dispatch** (no
goroutine fan-out) → `RemoteEvent` → `UserLogin.QueueRemoteEvent` → framework per-portal
serialized loop → msgconv `ConvertMessage` (ThreadRoot from topic id). Unknown event
types: log + skip, never fatal.

**Matrix → GC:** framework pre-validates against per-portal `RoomFeatures` and
pre-resolves reply/thread targets → `HandleMatrixMessage` routes `create_topic` (new
topic) vs `create_message` (thread reply; reply→thread auto-conversion in threads-only
rooms) → matrixfmt annotations with `accept_format_annotations=true` → echo dedup via
`local_id` transaction round-trip (`mautrix-googlechat%<rand uint64>`).

## 6. Error handling & connection lifecycle

Ported Python supervision policy, verbatim semantics:

| Condition | Response |
|---|---|
| 1.5 h channel age | scheduled recycle, silent reconnect |
| payload error / `SIDExpiring` | immediate re-register, no backoff |
| HTTP 400 "Unknown SID" (`SIDInvalid`) | resync, limit 3 |
| read timeout / network error | exponential backoff + `TRANSIENT_DISCONNECT` state |
| HTTP 401 / `invalid_grant` / logged-out `/mole/world` | `BAD_CREDENTIALS` state → client prompts re-login |
| hourly periodic sync | forces reconnect if it recovered anything |

Gap recovery: revision watermarks (portal + user-login) trigger `catch_up_group` replay
**through the normal remote-event queue**, so replayed edits/reactions/deletes reuse the
live handlers and dedup. pblite decode failures skip the event, never kill the channel.
Bridge-state error codes get stable constants + human messages in `init()`.

## 7. Testing

1. **msgconv corpus** — table-driven round-trip tests: emoji/astral chars, overlapping
   spans, nested lists, mentions, @room, font colors (extends megabridge's thin tests).
2. **Protocol units** — chunk parser + pblite against captured, sanitized real frames
   committed to `pkg/gchatmeow/testdata/` (captured from the test account via
   `/capture-fixtures`).
3. **ID codecs** — `pkg/gcid` unit tests reviewed against Python formats before first run.
4. **Live acceptance per milestone** — exit-criteria checklist run on the owner's
   continuwuity server with the test Google account. `go vet` + build + tests green is a
   merge precondition for every milestone.

**M1 protocol-validation spike:** before higher layers are built, verify against
2026-current Google Chat that channel choreography, world sync, and send/receive still
match the Python-derived spec; where they differ, diff against purple-googlechat
(actively maintained) and update reports 01/02. Known 2026 drift to re-verify: media
upload HTTP 500 (upstream #114), Matrix→GC formatting stripped (#110), backfill breakage
(#107).

## 8. Roadmap

Dependencies: M0 → M1 → M2 → {M3, M4, M5 in parallel} → M6 → M7.

| # | Milestone | Exit criteria |
|---|---|---|
| M0 | Skeleton: layout, main.go, adopted proto, compiling stubs, pinned IDs, example-config, build/Docker, Claude tooling | Bridge starts + registers on continuwuity; `help` responds; gcid tests green |
| M1 | Client-lib core: session/XSRF, pblite, channel, core RPCs, cookie login, chat-list sync, **live spike** | Cookie login works; portals appear with names/members/ghost avatars; channel survives forced disconnect; logout works |
| M2 | Text both directions: echo dedup, µs timestamps, DM+space scoping | Bidirectional text in DM and space; no dupes; correct attribution with 2 logins |
| M3 | Formatting/threads/replies: msgconv hardening (4 audit bugs incl. security fix), thread routing, quote replies | Round-trip tests green; threads + quote-replies both ways; #110 verified/handled |
| M4 | Edits, deletes, reactions, receipts, typing, SYSTEM_MESSAGE membership/renames | All round-trip in both room types; joins/renames propagate |
| M5 | Media (gated on live #114 re-verification) | GC→Matrix media incl. E2BE rooms; Matrix→GC works **or** ships disabled with clean per-message error |
| M6 | Backfill & gap recovery: FetchMessages, watermarks, catch-up replay, periodic sync | New portals backfill; 10-min outage replays exactly once |
| M7 | Polish & release: docs (manual cookie extraction), CI, Docker, changelog; *optional:* legacy Python-DB migration | Production bridge on owner's server; tagged release |

## 9. Claude tooling (created in M0, lives in `.claude/`)

- **CLAUDE.md** — project constitution: layering rules, frozen ID formats, proto2
  presence, UTF-16 code-unit rule, `docs/research/` as canonical spec, build/test commands.
- **Skill `/port-module`** — port one Python module to Go: research reports as spec,
  megabridge as second reference, fidelity checklist (error ladder, offsets, presence).
- **Skill `/capture-fixtures`** — capture + sanitize wire fixtures from the test account
  into `pkg/gchatmeow/testdata/`.
- **Skill `/verify-milestone`** — build + vet + tests + printed manual live-acceptance
  checklist for the current milestone.
- **Agent `gchat-port-auditor`** — adversarial diff of ported Go vs Python original for
  dropped behaviors; verification stage in every milestone workflow.
- Milestone execution: superpowers subagent-driven development + Workflow fan-outs
  (implement → audit → fix), per the autonomous-per-milestone decision.

## 10. Scope decisions

- Relay mode: available (framework), config default **off**.
- Beeper extras: link previews **yes**; DirectMedia, backward backfill queue, contact
  blobs **no**.
- Presence: **unsupported** (bridgev2 has none; dead code in Python too).
- DM creation from Matrix, Matrix→GC membership/room management: **deferred** past M7.
- Legacy Python-DB migration: optional M7; ID pinning keeps it possible.
- Prometheus metrics parity: **dropped** (not parity-critical).
- Recommendation to owner (non-blocking): mention the effort in `#googlechat:maunium.net`
  — AGPL hygiene, avoids duplicate upstream work, may surface megabridge context.

## 11. Top risks (full table: research 07 §5)

1. **2026 protocol drift** (media 500s, formatting stripped) → M1 live spike; purple-googlechat diffing; `client_version`/`hs` blob configurable, not constant.
2. **BrowserChannel port fidelity** (UTF-16 framing, SID/ack/ping, error ladder) → line-by-line port, fixture tests, forced-expiry integration test.
3. **Cookie-session fragility** → rotation persistence, correct `BAD_CREDENTIALS` mapping, relogin override, documented manual DevTools extraction.
4. **pblite edge cases** → permissive codec + captured-payload tests.
5. **UTF-16 annotation offsets** → adopted verified core + expanded corpus.
6. **ID lock-in** → pinned in M0, code-reviewed against Python before first run.
