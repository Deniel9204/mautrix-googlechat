# M3 — Formatting, Threads & Replies Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Every port task uses `/port-module` and ends with a `gchat-port-auditor` review. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Rich text both directions (bold/italic/underline/strike/monospace/code/colors/links/mentions/@room), first-class thread mapping, and quote-replies — replacing M2's plain-text passthrough.

**Architecture:** Two conversion packages under `pkg/msgconv` — `gchatfmt` (Google Chat `Annotation` spans → Matrix HTML) and `matrixfmt` (Matrix HTML → GC `Annotation` protos) — adopted-and-hardened from the megabridge branch (which has them ~55–60% complete with documented bugs), wired into `from-gchat.go`/`from-matrix.go`. Plus per-portal thread capabilities and `create_topic`/`create_message` routing, and `SendReplyTarget` quote-replies using the µs timestamp M2 already stores.

**Tech Stack:** mautrix-go `format` (HTML parser) + `event.RoomFeatures`, the M1 client, M2's msgconv.

## Global Constraints

- All M0–M2 Global Constraints still bind (module path `github.com/Deniel9204/mautrix-googlechat`, pins, `-tags goolm`, layering: gchatmeow no bridgev2 / connector no HTTP / msgconv only converts, proto2, frozen `pkg/gcid` IDs, plain commit messages, NO AI/session trailers).
- **All Google Chat annotation offsets are UTF-16 code units** (JS `String.length`), never bytes or runes. This is the #1 correctness rule for this milestone.
- Reference (adopt-and-harden): `$REF/googlechat-megabridge/pkg/msgconv/{gchatfmt,matrixfmt}`. Spec: `$REF/googlechat-python/mautrix_googlechat/formatter/`. Pattern: `$REF/meta/pkg/msgconv/textfmt`. (`$REF` = `/Users/danielagoston/utmshared/Projects/git/generalclaude/mautrix/_reference`.)
- **Known megabridge bugs to FIX during adoption** (from `docs/research/08d`): B1 HTML-attribute injection via unescaped URLs (security); B2 GC-user mentions render as DM pills/plain text and `m.mentions` never set; B3 inline hyperlinks re-downloaded as `m.image` (SSRF) — but that's the attachment path (M5), for M3 just don't let a link annotation become an attachment; B4 formatted captions drop the attached file. Never replicate the Python `if annotations:` always-truthy bug.
- Outgoing formatted messages set `MessageInfo.accept_format_annotations = true`.
- FONT_COLOR transform: `(rgb + 2^31) & 0xFFFFFF` (research 07 §1.2). `chip_render_type`: skip annotations that are not `DO_NOT_RENDER` when rendering text (they're link previews, M5).
- Live-verify of issue #110 (Matrix→GC formatting reportedly stripped server-side) is **deferred** — the test account's realtime channel is throttled (docs/research/10); M3 ships the correct request shape (`accept_format_annotations=true`) and this is verified on real deployment.
- Working dir = repo root; branch `m3-formatting-threads`. Commit after every task.

---

### Task 1: gchatfmt — Google Chat annotations → Matrix HTML

**Files:** Create `pkg/msgconv/gchatfmt/convert.go`, `pkg/msgconv/gchatfmt/utils.go`, `pkg/msgconv/gchatfmt/convert_test.go`. Adopt from `$REF/googlechat-megabridge/pkg/msgconv/gchatfmt/`. Spec: `$REF/googlechat-python/mautrix_googlechat/formatter/from_googlechat.py`.

**Interface:**
```go
package gchatfmt
// Parse renders a Google Chat message's text + annotations into Matrix HTML.
// Returns (plainBody, htmlBody). htmlBody == "" when no formatting applies.
func Parse(ctx context.Context, text string, annotations []*proto.Annotation, mention MentionResolver) (body, html string)
// MentionResolver maps a Google Chat user id to a Matrix mention pill (MXID + display name); nil-safe.
type MentionResolver func(gaiaID string) (mxid id.UserID, name string, ok bool)
```

Mandatory behaviors (each a test): UTF-16 code-unit offset indexing (astral chars count 2); overlapping-span normalization (sort by start, split at boundaries, render nested); FORMAT annotations bold/italic/underline/strike/monospace/monospace-block → `<strong>/<em>/<u>/<del>/<code>/<pre>`; FONT_COLOR via the `(rgb+2^31)&0xFFFFFF` transform → `<span data-mx-color="#rrggbb">`; URL/hyperlink annotations → `<a href>` with the href **HTML-attribute-escaped** (fix B1); mention annotations → ghost pill via MentionResolver (fix B2); `@room`/MENTION_ALL → `@room`; skip annotations whose `chip_render_type != DO_NOT_RENDER`.

- [ ] **Step 1:** Read the megabridge gchatfmt + from_googlechat.py fully; build the behavior inventory. Write table-driven tests FIRST: plain, single span, adjacent spans, **overlapping spans**, nested, emoji/astral offset, mention, @room, font color, hyperlink (incl. a `"><script>`-style URL asserting escaping), a non-DO_NOT_RENDER chip that must be skipped.
- [ ] **Step 2–4:** RED → adopt megabridge convert.go/utils.go, fixing B1 (escape href) and B2 (real ghost pills) → GREEN.
- [ ] **Step 5:** Audit vs from_googlechat.py (offset math, span normalization, no always-truthy-annotations bug). **Commit** `feat: gchatfmt Google Chat annotations to Matrix HTML`.

---

### Task 2: matrixfmt — Matrix HTML → Google Chat annotations

**Files:** Create `pkg/msgconv/matrixfmt/{html.go,tree.go,tags.go,convert.go,convert_test.go}`. Adopt from `$REF/googlechat-megabridge/pkg/msgconv/matrixfmt/`. Spec: `$REF/googlechat-python/mautrix_googlechat/formatter/from_matrix/`.

**Interface:**
```go
package matrixfmt
// Parse converts Matrix message content (HTML formatted_body or plain body) into
// a Google Chat text_body + the annotations describing its formatting.
func Parse(ctx context.Context, content *event.MessageEventContent, mention MentionResolver) (textBody string, annotations []*proto.Annotation)
// MentionResolver maps a Matrix user (MXID) to a Google Chat gaia id for a mention; nil-safe.
type MentionResolver func(mxid id.UserID) (gaiaID string, ok bool)
```

Mandatory behaviors (each a test): parse HTML into a tree (megabridge's html.go/tree.go), walk producing text + annotations with **UTF-16 code-unit** start/length; bold/italic/underline/strike/code/pre → FORMAT annotations; `data-mx-color`/`color:` → FONT_COLOR (inverse transform); `<a href>` → URL annotation (fix: the outgoing hyperlink must carry the href — the audit's silent-URL-loss bug); mentions via the **placeholder-locator** pattern (insert a placeholder, find its UTF-16 offset after building text, replace) → MENTION annotation with the gaia id; `@room` → MENTION_ALL; lists/blockquotes best-effort per Python. Plain-text (no formatted_body) → text only, no annotations.

- [ ] **Step 1:** Read megabridge matrixfmt + Python from_matrix fully. Table-driven tests FIRST: plain, each format tag, nested, **mention placeholder-locator offset with emoji before the mention** (the classic UTF-16 trap), @room, hyperlink (assert the URL survives — the audit bug), color, a list.
- [ ] **Step 2–4:** RED → adopt megabridge (fixing the URL-loss + offset bugs the audit flagged) → GREEN.
- [ ] **Step 5:** Audit vs from_matrix (offset computation, placeholder-locator, no dropped URLs). **Commit** `feat: matrixfmt Matrix HTML to Google Chat annotations`.

---

### Task 3: Mentions resolver wiring (both directions)

**Files:** Create `pkg/connector/mentions.go`; modify the msgconv adapters. Test: `mentions_test.go`.

Wire the `MentionResolver`s: GC→Matrix resolves a gaia id to the ghost MXID (`ghost.Intent`/`MakeUserID` + the portal's member set) and sets `content.Mentions` (`m.mentions`, fix B2); Matrix→GC resolves a Matrix pill MXID to a gaia id (reverse of `MakeUserID`, only for this bridge's ghosts/own user). `@room` maps to MENTION_ALL both ways.
- [ ] **Steps:** tests (GC mention → correct ghost pill + m.mentions populated; Matrix pill → correct gaia MENTION; @room ↔ MENTION_ALL; unknown user → plain text, no crash) → implement → audit → **commit** `feat: mention resolution both directions`.

---

### Task 4: Wire formatting into msgconv (replace M2 plain-text)

**Files:** Modify `pkg/msgconv/from-gchat.go` (use gchatfmt), `pkg/msgconv/from-matrix.go` (use matrixfmt), `pkg/connector/handlematrix.go` (send annotations + `accept_format_annotations=true`), the adapter. Test: extend `from-gchat_test.go`/`from-matrix_test.go`.

- [ ] **Step 1:** Tests: an inbound Message with FORMAT annotations → ConvertedMessage part with `Format: org.matrix.custom.html` + correct `FormattedBody`; an outbound formatted Matrix message → CreateTopicRequest with `TextBody` + `Annotations` + `MessageInfo.accept_format_annotations=true`. Fix B4: a media message with a formatted caption keeps BOTH the file and the caption text (don't overwrite).
- [ ] **Step 2–4:** RED → wire gchatfmt/matrixfmt in, replacing the plain-text extraction (keep plain-text fallback when no formatting) → GREEN + race.
- [ ] **Step 5:** Audit. **Commit** `feat: bridge rich text formatting both directions`.

---

### Task 5: Per-portal thread/reply capabilities (RoomFeatures)

**Files:** Modify `pkg/connector/capabilities.go` (`GetCapabilities`), `pkg/connector/client.go`. Test: `capabilities_test.go`.

Per-portal `event.RoomFeatures`: a threaded space (`PortalMetadata.ThreadsOnly`) advertises `Thread` capability (and reply→thread auto-conversion); a flat room advertises `Reply`. Read the flags M1 Task 12 already stores. Grep `$REF/meta/pkg/connector` and `$REF/mautrix-go/pkg/event` for the exact `RoomFeatures`/capability-level shape.
- [ ] **Steps:** tests (threaded space → Thread level; flat room → Reply level; DM per its flags) → implement → **commit** `feat: per-portal thread and reply capabilities`.

---

### Task 6: Thread routing & mapping (both directions)

**Files:** Modify `pkg/connector/handlematrix.go` (outbound routing), `pkg/connector/events.go` + `pkg/msgconv/from-gchat.go` (inbound ThreadRoot), `pkg/connector/dbmeta.go` if the message metadata needs a topic id. Test: `threads_test.go`.

Outbound: a Matrix message with `ThreadRoot`/`ReplyTo` in a threaded room → `create_message` with `parent_id.topic_id` (the thread's topic); a new top-level message → `create_topic`; reply→thread auto-conversion in threads-only rooms (bridgev2 pre-resolves `ThreadRoot`). Inbound: set `ConvertedMessage.ThreadRoot = <topic id>` when `message_id != topic_id` (and always in threads-only rooms); store `(message_id, topic_id, µs)` per message (extend `MessageMetadata` with the topic id).
- [ ] **Steps:** tests (threaded-space reply → create_message with parent topic; new message → create_topic; inbound threaded message → ThreadRoot set to topic; flat room → no thread) → implement → audit vs portal.py:891-907 → **commit** `feat: thread routing and mapping both directions`.

---

### Task 7: Quote-replies (SendReplyTarget)

**Files:** Modify `pkg/connector/handlematrix.go` (outbound reply), `pkg/msgconv/from-gchat.go` (inbound reply). Test: `replies_test.go`.

Outbound: a Matrix reply (`MatrixMessage.ReplyTo`, pre-resolved to a `*database.Message`) → `create_message`/`create_topic` with `SendReplyTarget{id, create_time}` — **the create_time is the target message's stored µs timestamp** (`MessageMetadata.TimestampMicro`, which M2 stores). Inbound: read `Message.reply_to.id.message_id` → `ConvertedMessage.ReplyTo`.
- [ ] **Steps:** tests (outbound reply builds SendReplyTarget with the stored µs create_time from the target's metadata; inbound reply sets ConvertedMessage.ReplyTo; missing target µs → graceful) → implement → audit vs portal.py:886 → **commit** `feat: quote-reply support both directions`.

---

### Task 8: M3 exit verification + whole-branch review

**Files:** `docs/research/11-m3-notes.md` (optional).

- [ ] **Step 1:** Full gate: gofmt, `go build/vet/test -tags goolm ./...`, race, both layering greps, `/verify-milestone`. The formatting round-trip corpus (Task 1/2 tests) is the core evidence.
- [ ] **Step 2:** Whole-branch review (cross-cutting: does a formatted message survive the full inbound path GC annotations → HTML → Matrix, and outbound HTML → annotations → create_topic; do threads + replies + formatting compose without offset corruption). Fix findings.
- [ ] **Step 3:** finishing-a-development-branch → merge to main. (Live #110 verification deferred to deployment per Global Constraints.)
- **Exit:** Round-trip formatting tests green (emoji/astral, overlapping spans, mentions, colors, links); threads map both ways in a threaded space; quote-replies work both ways; no offset corruption.

Dependency order: Task 1 & 2 (independent) → 3 → 4 ; 5 → 6 → 7 ; 8 last. Formatting (1–4) and threads/replies (5–7) are largely independent tracks.
