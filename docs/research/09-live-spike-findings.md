# 09 — Live Protocol Validation Spike (M1 Task 13)

**Date:** 2026-07-14
**Account:** consumer Google account (test), fresh (empty chat world at connect time)
**Harness:** `pkg/gchatmeow/live_test.go` (`go test -tags 'goolm live'`), cookies via env.

## Headline: the reverse-engineered protocol still works in 2026. All four critical drift risks cleared, and the full inbound path was validated against real traffic.

The M1 client library connected to live Google Chat, authenticated with the
frozen 2023-era constants, synced the world, and received 18 real stream events
(including two operator-sent `MESSAGE_POSTED`s) delivered in order through the
whole pipeline: chunk parser → pblite decode → `split_event_bodies` →
`OnStreamEvent`. This substantially retires research risk **R1 (2026 protocol
drift)** for the auth/channel/sync/receive surface.

## Results per validation

| Check | Result | Detail |
|---|---|---|
| XSRF bootstrap (`/mole/world` scrape, frozen API key + `client_version=2440378181258`) | **PASS** | Session validated, XSRF token obtained. The frozen constants still authenticate. |
| `GetSelfUserStatus` (binary-proto RPC) | **PASS** | Own gaia id decoded. Plain binary-proto round-trip, no base64. |
| `PaginatedWorld` (world sync) | **PASS** | Returned 0 items — the test account has no existing chats (expected for a fresh account). |
| BrowserChannel (register → SID → ack GET → initial ping) | **PASS** | Reached `CONNECTED`; the `$req` double-encoding is accepted by the live server and the post-SID ack GET path works. |
| Inbound event delivery | **PASS** | 18 events over the live window: `SESSION_READY`, `READ_RECEIPT_CHANGED`, `MESSAGE_SMART_REPLIES`, `TOPIC_CREATED`, **`MESSAGE_POSTED`** (×2, operator-sent), `TOPIC_MUTE_CHANGED`, `GROUP_UNREAD_...`, etc. Ordered, single-goroutine, no decode failures. |

## Answers to the research open questions (07 §6, 01, 02)

- **base64 responses** (07 open Q, api.go dual-decode heuristic): NOT observed —
  the RPCs returned plain binary protobuf despite the
  `X-Goog-Encode-Response-If-Executable: base64` header. The dual-decode path is
  harmless dead-weight for these RPCs; keep it (cheap insurance) but it's
  unexercised.
- **`$req` double-encoding** (channel.go, Task 7 concern): ACCEPTED by the live
  server — the channel connected and delivered events. No change needed.
- **post-SID ack GET "unclear why required"** (channel.py:429): the choreography
  including the ack GET connects successfully. (We did not A/B test removing it;
  it works as ported, leave it.)
- **frozen API key / `client_version`**: still valid as of 2026-07-14. No bump
  needed yet; keep them config-updatable per the design.

## Risk #4 (pblite edge cases) validated positively

The permissive codec logged ~20 `pblite: skipping unknown field` debug lines for
fields present in live 2026 responses but absent from our 2023-era proto — and
**never failed a decode**. This is exactly the designed behavior (log + skip,
never fatal) and confirms the permissiveness decision was correct: Google has
added fields since the proto snapshot, and the bridge tolerates them.

## Findings to carry into M2 (message handling)

The "skipped unknown field" log is a precise inventory of what M2 will need to
add to the proto for full fidelity. Observed skipped fields on live traffic:

- `Event` field 9 (`[null, <int>, null, "<µs>", [3,14,10], 1, <int>]` — looks
  like per-event trace/latency metadata) and field 11 (arrays of `[sec, nsec]`
  timing pairs — client-latency telemetry).
- `Event.EventBody` fields 7, 11, 21, 25, 53, 65 — several carry a
  `dm_id`/topic id + a µs timestamp; **field 21/25 are the most important for M2**
  (they accompany `MESSAGE_POSTED`/`MESSAGE_SMART_REPLIES` and likely hold
  message-body/read-state payloads the current proto truncates).
- `Message` fields 24, 25 and `MessageEvent` field 7 (small enums, value 2).
- **Unknown event *types*** delivered as raw numbers: `type=64`, `type=83` —
  event types Google added since the proto snapshot. Handled gracefully (shown
  as the number); M2 should identify and either map or explicitly ignore them.

**Action for M2 entry:** diff our `googlechat.proto` against a current source
(the actively-maintained `purple-googlechat`, per the design) to fill in these
field numbers before implementing message conversion, so `MESSAGE_POSTED`
bodies aren't silently truncated. None of this blocks M1 — the transport,
auth, channel, and event-routing layers are all validated.

## Not yet validated (deferred, out of M1 scope)

- **Send path** (`create_topic`/`create_message`) — M2. The Matrix→GC
  formatting-stripped (#110) and media-upload-500 (#114) drift items attach to
  M3/M5 and were not exercised here.
- **Full bridge on a homeserver** — this spike drove `pkg/gchatmeow` directly
  (no Matrix homeserver). Deploying the binary on continuwuity and logging in
  via the bot command remains the operator's end-to-end acceptance step.
- **Backfill / catch-up** (`list_topics`, `catch_up_*`) — M6.

## Fixtures

18 decoded stream arrays were captured to `.spike-fixtures/` (gitignored). They
contain a real gaia id and DM id and were **not committed**. Sanitizing a subset
into `pkg/gchatmeow/testdata/` (fake gaia/dm ids, lorem text) is a good early-M2
task to lock the real event shapes into the pblite/decode unit tests.
