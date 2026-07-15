# M4 — Mutations & Ephemerals Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Every port task uses `/port-module` and ends with a `gchat-port-auditor` review. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Edits, deletes, reactions, read receipts, and typing bridge both directions, plus Google Chat SYSTEM_MESSAGE membership changes and room name/topic updates propagate GC→Matrix.

**Architecture:** Implement bridgev2's optional handler interfaces on `GChatClient` (Edit/Redaction/Reaction/ReadReceipt/Typing HandlingNetworkAPI) using the M1 RPC wrappers (all already exist), and fill the remaining `handleGChatEvent` stubs (MESSAGE_UPDATED, MessageDeleted, MessageReaction, ReadReceiptChanged, TypingStateChanged, MembershipChanged, GroupUpdated) with the matching `simplevent` types.

**Tech Stack:** bridgev2 optional interfaces + `simplevent.{Message,Reaction,Receipt,Typing,ChatInfoChange}`, `go.mau.fi/util/variationselector`, the M1 client, M2/M3 msgconv.

## Global Constraints

- All M0–M3 Global Constraints bind (module path, pins, `-tags goolm`, layering: gchatmeow no bridgev2 / connector no HTTP / msgconv only converts, proto2, frozen IDs, plain commits, NO AI/session trailers).
- All existing M4 RPCs are already wrapped in `pkg/gchatmeow/api.go`: `EditMessage`, `DeleteMessage`, `UpdateReaction`, `MarkGroupReadstate`, `SetTypingState`. Use them; don't re-wrap.
- The `handleGChatEvent` switch (`pkg/connector/events.go`) already has stub cases for every body type; fill them, keep the log-and-skip for genuinely-unhandled ones.
- Reference: Python `mautrix_googlechat/portal.py` (edit ~840, redaction ~800, reaction ~763, typing ~1133, read receipt in user.py ~684, SYSTEM_MESSAGE parsing). bridgev2 optional interfaces at `$REF/mautrix-go/bridgev2/networkinterface.go`; how meta implements each at `$REF/meta/pkg/connector`. (`$REF` = `/Users/danielagoston/utmshared/Projects/git/generalclaude/mautrix/_reference`.)
- Timestamps: Google Chat µs; Matrix read receipts ms → ×1000 for GC. Edit dedup uses `MessageMetadata.LastEditTime` (already declared in dbmeta.go).
- Emoji reactions: use `go.mau.fi/util/variationselector` (`.Remove` on the way to GC, `.Add` toward Matrix), matching how M1/meta handle it.
- connector no net/http. Working dir = repo root; branch `m4-mutations-ephemerals`. Commit after every task.

---

### Task 1: Edits (both directions)

**Files:** Create `pkg/connector/handleedit.go`; modify `pkg/connector/events.go` (MESSAGE_UPDATED case), `pkg/msgconv/from-gchat.go` (ConvertEdit) or a new msgconv edit path, `pkg/connector/dbmeta.go` (LastEditTime already there). Test: `handleedit_test.go`.

**Interfaces:** `GChatClient` implements `bridgev2.EditHandlingNetworkAPI.HandleMatrixEdit(ctx, *MatrixEdit) error` — call `EditMessage` RPC with the full `MessageId` + new text/annotations (reuse M3 matrixfmt); store the new `last_edit_time` in the target's `MessageMetadata` for dedup. Inbound: a `MESSAGE_POSTED`-body Event with `Event.type == MESSAGE_UPDATED` (per Task 6 M2's dispatch — the inner `if evt.GetType() == MESSAGE_UPDATED` arm) → a `RemoteEdit` (`simplevent.Message` with `RemoteEventEdit` type, or the ConvertEdit path) that re-runs msgconv on the edited message; dedup via `last_edit_time || last_update_time` vs stored metadata (only apply if newer).

- [ ] **Step 1:** Read portal.py edit paths + meta's edit handling + bridgev2 MatrixEdit/RemoteEdit/ConvertEdit. Tests: outbound Matrix edit → EditMessage RPC with the full message id + new formatted text; edit dedup (a duplicate MESSAGE_UPDATED with an older/equal last_edit_time is skipped); inbound edit → edited content (part 0 only). Mock the RPC seam.
- [ ] **Step 2–4:** RED → implement → GREEN + race.
- [ ] **Step 5:** Audit vs portal.py. **Commit** `feat: message edits both directions`.

---

### Task 2: Deletes / redactions (both directions)

**Files:** Create `pkg/connector/handleredact.go`; modify `events.go` (MessageDeleted case). Test: `handleredact_test.go`.

**Interfaces:** `RedactionHandlingNetworkAPI.HandleMatrixMessageRemove(ctx, *MatrixMessageRemove) error` — `DeleteMessage` RPC (with topic-id fallback per Python). Inbound `MessageDeletedEvent` → `simplevent.MessageRemove` (framework redacts all parts).

- [ ] **Steps:** tests (outbound redaction → DeleteMessage RPC with correct message id; inbound MessageDeleted → simplevent.MessageRemove for the right message) → RED → implement → GREEN → audit → **commit** `feat: message deletion both directions`.

---

### Task 3: Reactions (both directions)

**Files:** Create `pkg/connector/handlereaction.go`; modify `events.go` (MessageReaction case). Test: `handlereaction_test.go`.

**Interfaces:** `ReactionHandlingNetworkAPI`: `PreHandleMatrixReaction(ctx, *MatrixReaction) (MatrixReactionPreResponse, error)` (return `{SenderID, EmojiID, Emoji}` with `variationselector.Remove(emoji)`); `HandleMatrixReaction(ctx, *MatrixReaction) (*database.Reaction, error)` (`UpdateReaction` RPC, type ADD); `HandleMatrixReactionRemove(ctx, *MatrixReactionRemove) error` (`UpdateReaction` RPC, type REMOVE). Emoji id = the emoji (per-emoji reactions, not one-per-user). Inbound `MessageReactionEvent` → `simplevent.Reaction` (add/remove; `variationselector.Add` toward Matrix); uniqueness `(message, part, sender, emojiID)`.

- [ ] **Steps:** tests (Matrix reaction → UpdateReaction ADD with the right emoji+message; reaction remove → REMOVE; inbound reaction add/remove → simplevent.Reaction; variation-selector normalization both ways) → RED → implement → GREEN + race → audit vs portal.py:763 → **commit** `feat: reactions both directions`.

---

### Task 4: Read receipts (both directions)

**Files:** Create `pkg/connector/handlereceipt.go`; modify `events.go` (ReadReceiptChanged + GroupViewed cases). Test: `handlereceipt_test.go`.

**Interfaces:** `ReadReceiptHandlingNetworkAPI.HandleMatrixReadReceipt(ctx, *MatrixReadReceipt) error` (must tolerate `ExactMessage == nil`; `MarkGroupReadstate` RPC, ms→µs). Set `ImplicitReadReceipts` in `NetworkGeneralCapabilities` if appropriate (check meta). Inbound `read_receipt_changed` / `group_viewed` → `simplevent.Receipt{ReadUpTo}` (`read_time_micros`→time; `group_viewed` → own-user receipt with `IsFromMe`).

- [ ] **Steps:** tests (Matrix read receipt → MarkGroupReadstate with ms→µs; inbound read receipt → simplevent.Receipt ReadUpTo; group_viewed → own receipt IsFromMe) → RED → implement → GREEN → audit → **commit** `feat: read receipts both directions`.

---

### Task 5: Typing (both directions)

**Files:** Create `pkg/connector/handletyping.go`; modify `events.go` (TypingStateChanged case). Test: `handletyping_test.go`.

**Interfaces:** `TypingHandlingNetworkAPI.HandleMatrixTyping(ctx, *MatrixTyping) error` (`SetTypingState` RPC with `TypingContext` group/topic oneof). Inbound `typing_state_changed` → `simplevent.Typing` (framework runs the remote-typing loop + timeout). **Note the trap:** `Event.group_id` is EMPTY for typing events — route via `body.typing_state_changed.context.group_id`.

- [ ] **Steps:** tests (Matrix typing → SetTypingState with correct context; inbound typing → simplevent.Typing routed via context.group_id not Event.group_id) → RED → implement → GREEN → audit vs portal.py:1133 → **commit** `feat: typing notifications both directions`.

---

### Task 6: SYSTEM_MESSAGE membership + room name/topic (GC→Matrix)

**Files:** Create `pkg/connector/systemmessage.go`; modify `events.go` (MembershipChanged + GroupUpdated cases). Test: `systemmessage_test.go`.

**Interfaces:** Parse SYSTEM_MESSAGE membership (`MEMBERSHIP_CHANGED` annotation, 9 `MembershipChangedMetadata` types) → `simplevent.ChatInfoChange` with `MemberChanges *ChatMemberList` (deltas: `ChatMember{Membership, PrevMembership}`). Parse room rename (`rename_metadata`) / topic (`group_details_metadata`) → `ChatInfoChange` with partial `ChatInfo{Name/Topic}`.

- [ ] **Steps:** tests (a membership-add system message → ChatInfoChange with the member delta; a rename → ChatInfoChange with Name; the 9 membership types mapped correctly) → RED → implement → GREEN → audit vs portal.py SYSTEM_MESSAGE parsing → **commit** `feat: membership and room-info changes from Google Chat`.

---

### Task 7: M4 exit verification + whole-branch review + merge

**Files:** none.

- [ ] **Step 1:** Full gate: gofmt, `go build/vet/test -tags goolm ./...`, race, both layering greps, `/verify-milestone`.
- [ ] **Step 2:** Whole-branch review (cross-cutting: do the new inbound event handlers compose with M2/M3 without breaking the message/format path; does edit dedup interact correctly with the message store; are reactions/receipts/typing all routed to the right portal; do gap-replayed events — M2 catch_up — now correctly replay edits/reactions/deletes through these new handlers, which was the whole point of routing catch-up through handleGChatEvent). Also triage the M2/M3 deferred-minors backlog (in the SDD ledger) — fold in any cheap wins. Fix findings.
- [ ] **Step 3:** finishing-a-development-branch → merge to main.
- **Exit:** Edit/delete/react/read/typing round-trip in both room types; renames and member joins/leaves propagate GC→Matrix; catch-up replay includes edits/reactions/deletes exactly once.

Dependency order: Tasks 1–6 are largely independent (each a distinct event type + handler); do 1→2→3→4→5→6 sequentially for clean reviews. Task 7 last.
