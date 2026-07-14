# M2 — Text Messaging Both Directions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax. Every task that reimplements Python behavior uses `/port-module` and ends with a `gchat-port-auditor` review.

**Goal:** Plain-text messages bridge both directions — a Google Chat `MESSAGE_POSTED` becomes a Matrix message, and a Matrix message becomes a `create_topic`/`create_message` RPC — with own-echo dedup, microsecond timestamps stored, and reconnect gap-recovery so no messages are silently lost.

**Architecture:** Fill the M1 `handleGChatEvent` stub for the message body types (routing into a new `pkg/msgconv` conversion layer, GC→Matrix direction), and implement `HandleMatrixMessage` on the connector (Matrix→GC), both plain-text only (formatting is M3). Front-load the three M2-entry blockers the M1 reviews flagged.

**Tech Stack:** mautrix-go bridgev2 (`RemoteMessage`/`simplevent.Message`, `HandleMatrixMessage`, `MatrixMessageResponse`, transaction-ID dedup), the M1 `pkg/gchatmeow` client, `pkg/gcid`.

## Global Constraints

- Everything from the M1 plan's Global Constraints still binds (module path `github.com/Deniel9204/mautrix-googlechat`, pins, `-tags goolm`, layering: gchatmeow no bridgev2 / connector no HTTP / msgconv only converts, proto2, frozen `pkg/gcid` IDs, plain commit messages with NO AI/session trailers).
- Protocol truth hierarchy: live spike findings (`docs/research/09-live-spike-findings.md`) > `../_reference/googlechat-python/` > research 01-03 > megabridge.
- Message-part convention (frozen in gcid): text = `TextPartID` (`""`); attachments `att_<n>` (M5). M2 emits only the text part.
- Every bridged message stores its Google Chat `create_time` in **microseconds** in `MessageMetadata.TimestampMicro` (M3 quote-replies need it).
- Echo local_id format: `mautrix-googlechat%<rand uint64>` (matches Python).
- All builds/tests/vets `-tags goolm`; race-test anything touching the connect/goroutine paths.
- Reference clones at `/Users/danielagoston/utmshared/Projects/git/generalclaude/mautrix/_reference/` (`$REF`). Working dir is the repo root; branch `m2-text-messaging`.

---

### Task 1: Fix the lost-cancel window in gchatmeow.Client.Connect (M2-entry blocker #3)

**Files:**
- Modify: `pkg/gchatmeow/client.go` (Connect + the cancel field)
- Modify/Test: `pkg/gchatmeow/client_test.go`

**Problem:** `Client.Connect` currently sets `c.cancel` *inside* the spawned supervision goroutine, so a `Disconnect()` landing before that assignment is lost — the connection then runs unbounded. The connector's `wireAndStart` does `go conn.Connect(ctx)`, so this window is real.

**Interfaces:** No signature change. `Connect(ctx)` must guarantee that a `Disconnect()` called at any point after `Connect` returns/starts cancels the loop.

- [ ] **Step 1: Write the failing test** — `TestDisconnectImmediatelyAfterConnectStops`: build a Client with a fake channel whose `Listen` blocks on ctx; `go c.Connect(ctx)`; immediately (no sync) call `c.Disconnect()` in a tight loop / via a barrier that races the goroutine start; assert `Connect` returns nil within a short timeout. Make it reliably RED by asserting the cancel is installed synchronously (e.g. assert `c.hasCancel()` true immediately after `Connect` is entered). Prefer restructuring so `Connect` installs `ctx, cancel := context.WithCancel(ctx)` and stores `cancel` under `mu` **before** `go c.supervise(ctx)`.
- [ ] **Step 2:** Run → FAIL. **Step 3:** Refactor `Connect` to create the cancellable ctx + store `cancel` under `mu` synchronously, then spawn the loop with the derived ctx. **Step 4:** Run → PASS; `go test -race ./pkg/gchatmeow/`.
- [ ] **Step 5:** Audit (gchat-port-auditor: confirm no behavior change to the supervision ladder, only cancel timing). **Commit** `fix: install Connect cancel synchronously to close lost-cancel window`.

---

### Task 2: Update the proto with live 2026 fields + regenerate (M2-entry blocker + spike finding)

**Files:**
- Modify: `pkg/gchatmeow/proto/googlechat.proto`, regenerate `googlechat.pb.go` via `pkg/gchatmeow/proto/gen.sh`
- Test: a decode test in `pkg/gchatmeow/pblite/` over the fields now present

**Context:** The live spike (`docs/research/09-live-spike-findings.md`) showed Google now sends fields absent from our 2023 proto — most importantly `Event.EventBody` fields 21 and 25 (accompany `MESSAGE_POSTED`), plus `Event` 9/11 (telemetry), `Message` 24/25, `MessageEvent` 7, and event *types* 64/83. Without these, `MESSAGE_POSTED` bodies may be truncated.

- [ ] **Step 1:** Diff our `googlechat.proto` against the actively-maintained `$REF/purple-googlechat` proto (find its `.proto`/`pblite` definitions; grep for the message field numbers). For each live-observed missing field, add the correctly-typed proto2 field at the exact field number. Add the missing `Event.EventType` enum values (64, 83) with the names purple-googlechat uses (or `EVENT_TYPE_64`-style placeholders + a comment if unnamed). **Do NOT** renumber or alter existing fields.
- [ ] **Step 2:** Regenerate: `PATH="$(go env GOPATH)/bin:$PATH" ./pkg/gchatmeow/proto/gen.sh`. Confirm `syntax = "proto2"` preserved, `go build -tags goolm ./...` clean.
- [ ] **Step 3:** Write a decode test asserting a hand-built pblite array carrying the new `EventBody` field 21/25 shape (from the spike log samples in report 09) decodes into the new proto fields (not skipped). RED before the proto change, GREEN after.
- [ ] **Step 4:** vet + gofmt + full suite. **Commit** `feat: add live-2026 proto fields (EventBody 21/25, Event 9/11, event types 64/83)`.

---

### Task 3: pkg/msgconv — GC→Matrix plain text

**Files:**
- Create: `pkg/msgconv/msgconv.go` (MessageConverter holding a portal/intent-agnostic converter), `pkg/msgconv/from-gchat.go`
- Test: `pkg/msgconv/from-gchat_test.go`
- Port of: `$REF/googlechat-python/mautrix_googlechat/formatter/from_googlechat.py` (TEXT ONLY — annotations/formatting are M3; strip to plain text)

**Interfaces:**
```go
package msgconv
type MessageConverter struct { /* config knobs; no bridgev2 portal state */ }
func New() *MessageConverter
// ToMatrix converts a Google Chat Message proto to converted parts (M2: one
// text part; M3 adds formatting + attachments).
func (mc *MessageConverter) ToMatrix(ctx context.Context, msg *pb.Message) *ConvertedMessage
// ConvertedMessage mirrors the subset of bridgev2.ConvertedMessage this layer
// builds, so msgconv stays bridgev2-import-light (connector adapts it). OR, if
// cleaner, build bridgev2.ConvertedMessage directly — decide and document.
```
- [ ] **Step 1:** Tests: plain text body → one text part with `msgtype: m.text`, body = the message's text; empty/whitespace handling; unicode/emoji preserved (UTF-8, no offset math in M2). Astral chars survive.
- [ ] **Step 2-4:** RED → implement (extract plain text from the Message proto — the `text_body`/annotations' plain segments; for M2 take the raw text, ignore annotation formatting) → GREEN.
- [ ] **Step 5:** Audit vs from_googlechat.py (plain-text extraction fidelity; do not replicate the `if annotations:` always-truthy bug noted in research 07). **Commit** `feat: msgconv GC->Matrix plain text`.

---

### Task 4: Inbound MESSAGE_POSTED → RemoteMessage (wire handleGChatEvent)

**Files:**
- Modify: `pkg/connector/events.go` (fill the MESSAGE_POSTED case), `pkg/connector/client.go` if needed
- Create: `pkg/connector/msgconv_adapter.go` (adapts msgconv output to bridgev2, sets MessageMetadata)
- Test: `pkg/connector/events_test.go`

**Interfaces:** Emit `simplevent.Message[*pb.Event]` (or a custom `RemoteMessage`) with `GetID` = `gcid.MakeMessageID(msg id)`, `GetSender`, `ConvertMessage` calling `msgconv.ToMatrix`, `ShouldCreatePortal` true. Store `MessageMetadata{TimestampMicro: msg.create_time µs}` on the part `DBMetadata`.
- [ ] **Step 1:** Test: a `MESSAGE_POSTED` Event (built from a spike-shaped proto) routed through `handleGChatEvent` produces a queued `RemoteMessage` with correct portal key (dm/space), sender ghost id, text content, and `TimestampMicro` set. Use the connector test seam (queueChatResyncFn-style) to capture queued events without a live bridge.
- [ ] **Step 2-4:** RED → implement (dispatch in events.go; build the RemoteMessage; multi-part scaffolding with text = part 0) → GREEN + race.
- [ ] **Step 5:** Audit. **Commit** `feat: bridge inbound MESSAGE_POSTED to Matrix`.

---

### Task 5: Matrix→GC HandleMatrixMessage (create_topic/create_message, plain text)

**Files:**
- Create: `pkg/connector/handlematrix.go`, `pkg/msgconv/from-matrix.go` (plain text → Google Chat message request; formatting M3)
- Add RPCs if missing: `CreateTopic`/`CreateMessage` wrappers in `pkg/gchatmeow/api.go` (check what M1's 16 wrappers already cover — `create_topic`/`create_message` endpoints)
- Test: `handlematrix_test.go`, `from-matrix_test.go`

**Interfaces:** `func (c *GChatClient) HandleMatrixMessage(ctx, *bridgev2.MatrixMessage) (*bridgev2.MatrixMessageResponse, error)` — route `create_topic` (new topic; threaded spaces / top-level) vs `create_message` (reply into a topic; M3 wires thread routing — M2 sends everything as a new topic/top-level message), build the request with the local_id (Task 6), return `MatrixMessageResponse{DB: &database.Message{ID, SenderID, Timestamp, Metadata: &MessageMetadata{TimestampMicro}}}`.
- [ ] **Step 1:** Tests: plain-text Matrix message → correct RPC (create_topic for a top-level send) with the text and a request header; response maps to a DB message with the returned message id + µs timestamp. Mock the conn RPC via an injectable seam.
- [ ] **Step 2-4:** RED → implement → GREEN + race.
- [ ] **Step 5:** Audit vs portal.py send path. **Commit** `feat: send plain-text Matrix messages to Google Chat`.

---

### Task 6: Echo dedup via local_id transaction round-trip

**Files:**
- Modify: `pkg/connector/handlematrix.go` (generate + attach local_id), `pkg/connector/events.go` (match echo), `pkg/connector/client.go` (implement `TransactionIDGeneratingNetwork` if used)
- Test: `echo_dedup_test.go`

**Interfaces:** Generate `mautrix-googlechat%<rand uint64>` local_id, put it in the outgoing request AND as the bridgev2 transaction id (`MatrixMessage.AddPendingToIgnore`/`AddPendingToSave` + `RemoteMessageWithTransactionID.GetTransactionID`). When the `MESSAGE_POSTED` echo returns with that local_id, the framework dedups it against the pending send.
- [ ] **Step 1:** Test: a send generates a local_id; the inbound echo carrying the same local_id is recognized as own-echo (GetTransactionID matches) and does not double-post. Cover: a message from a *different* device (no matching local_id) is NOT deduped.
- [ ] **Step 2-4:** RED → implement (decide AddPendingToIgnore vs TransactionIDGeneratingNetwork — follow how meta does it, `$REF/meta/pkg/connector`) → GREEN + race.
- [ ] **Step 5:** Audit. **Commit** `feat: own-echo dedup via local_id transaction id`.

---

### Task 7: Reconnect gap-recovery via catch_up (M2-entry blocker #2, minimal)

**Files:**
- Create: `pkg/connector/backfill.go` (catch-up-on-reconnect only; full history backfill is M6)
- Modify: `pkg/connector/client.go` (trigger catch-up on the reconnect/Connected-after-first transition), `pkg/gchatmeow/api.go` (CatchUpUser/CatchUpGroup already wrapped in M1 — confirm)
- Test: `backfill_test.go`

**Context:** M1 review Important #2: a SIDExpiring re-register resets channel AID=0, so events during the gap are not replayed. Now that messages flow, a reconnect must catch up or silently lose messages. M2 scope = **catch-up replay through the normal remote-event queue** (as Python does), NOT the full initial-history backfill (M6).

**Interfaces:** On a non-first `ConnStateConnected` (reconnect), call `catch_up_user` (revision watermark from `UserLoginMetadata.Revision`) → for each returned group event, run it through the same `handleGChatEvent` path (so edits/reactions/deletes during the gap reuse M4 handlers later). Persist the new revision watermark only after successful handling.
- [ ] **Step 1:** Tests: on a simulated reconnect, catch_up_user is called with the stored revision; returned events are replayed through handleGChatEvent; the watermark advances only on success; a failure leaves the watermark unchanged (retry next reconnect). Mock the RPC.
- [ ] **Step 2-4:** RED → implement → GREEN + race. Wire it so the first Connected does the chat-list sync (M1) and subsequent Connecteds do catch-up.
- [ ] **Step 5:** Audit vs user.py catch_up. **Commit** `feat: catch-up gap recovery on reconnect`.

---

### Task 8: M2 exit verification + live send/receive spike

**Files:** `docs/research/10-m2-live-findings.md`

- [ ] **Step 1:** Full gate: gofmt, `go build/vet/test -tags goolm ./...`, race, both layering greps, `/verify-milestone`.
- [ ] **Step 2:** Extend `pkg/gchatmeow/live_test.go` (or a connector-level live test) to send a text message via `create_topic` and confirm it appears on the test account, and that an inbound message converts. Requires the operator + test-account cookies (same env-cookie harness). Document in report 10.
- [ ] **Step 3:** Whole-branch review, then finishing-a-development-branch (merge to main).
- **Exit:** Bidirectional plain text works in a DM and a space; no duplicate own-echoes; a 10-minute disconnect replays missed messages exactly once; message DB rows correct both directions.

Dependency order: Task 1, 2 (independent entry blockers) → 3 → 4 (needs 3) ; 5 → 6 (needs 5) ; 7 (needs 4) → 8. Tasks 4 and 5 can run in parallel after 3.
