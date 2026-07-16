# M7 — Polish & Release Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Every code task ends with a `gchat-port-auditor` (or independent) review. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Take the bridge from feature-complete (M0–M6) to release-ready on the owner's continuwuity server: real docs (incl. manual cookie extraction), CHANGELOG, CI/Docker polish, the accumulated deferred-minors cleanup, AND a Python→Go database migration tool so the owner's existing Python deployment cuts over in place.

**Architecture:** Docs are Markdown under repo root + `docs/`. The migration is a subcommand/mode on the existing `cmd/mautrix-googlechat` binary that reads the old Python bridge DB (SQLite or Postgres) and writes bridgev2 rows into the Go bridge DB, using the FROZEN `gcid` formats (which the mapping analysis proved are identity-compatible with Python's stored ids). Cleanup fixes touch the existing packages.

**Tech Stack:** Go, `go.mau.fi/util/dbutil` (bridgev2's DB layer, both SQLite+Postgres), the M0–M6 connector/gcid/gchatmeow packages.

## Global Constraints

- All M0–M6 constraints bind: layering (gchatmeow does HTTP + no bridgev2; connector no `net/http`; msgconv only converts); proto2; `-tags goolm`; **frozen gcid formats are PERMANENT DB CONTENTS — the migration MUST use them, never invent new ones**; plain conventional-commit messages; **NO AI/Co-Authored-By/session trailers**.
- **The migration's source of truth is `.superpowers/sdd/m7-migration-schema-map.md`** — the complete field-by-field Python→Go mapping (read in full before any migration task). Where it flags a decision, this plan resolves it (below); where it flags a needs-verification read, the task does that read.
- The migration must be SAFE: default to a **dry-run** that reports what it would write; never mutate the source Python DB; refuse to run against a non-empty target unless `--force`; wrap the whole write in one transaction; print a summary (rows per entity, skipped, warnings).
- Migration DECISIONS resolved for this plan (from the map's Risks section):
  - **index→part_id single-row case**: use `""` (TextPartID) for any single-row `(gcid,chat,receiver)` group (map Risk #1 option (a)) — correct for all outbound + all textual inbound; the rare solo-attachment-inbound case gets `""` instead of `att_0`, which is an internal join key only (documented caveat). Multi-row groups follow the map's §2 rule exactly.
  - **sender_mxid**: derive every message/reaction row's `sender_mxid` from the ghost's mxid via the bridge's puppet-mxid template (Task 4 reads the exact template).
  - **user_portal for spaces**: accepted gap — synthesize DM `user_portal` rows only; spaces self-heal on next sync (documented in migration output + docs).
  - **backfill_task**: Task 7 verifies bridgev2's auto-create trigger, then seeds per-portal rows to SUPPRESS a redundant backward backfill of already-migrated history (or documents why not).
  - **cookies / double-puppet / timestamp-version**: Task 4 verifies `maugclib.Cookies` key casing, locates bridgev2 double-puppet storage, and reads the source DB schema-version to decide ms-vs-µs — all before writing.
- Working dir = repo root; branch `m7-polish-release` (already created). Commit after every task.

---

### Task 1: Documentation — README, cookie-extraction guide, CHANGELOG

**Files:** rewrite `README.md`; create `docs/authentication.md`; create `CHANGELOG.md`.

- `README.md`: what the bridge is; feature matrix (text, formatting, threads, replies, edits, deletes, reactions, receipts, typing, media (note #114 outbound caveat), membership/renames, backfill); requirements (Go 1.25+, a Matrix homeserver, `-tags goolm`); quick start (build, `./mautrix-googlechat -g` to generate config, register, run); Docker usage; config pointers (esp. that history backfill needs `backfill.enabled: true` + `backfill.threads.max_initial_messages > 0`); login via `login` command + cookie paste; link to `docs/authentication.md`; link to migration (`docs/migration.md`, created in Task 8); AGPL license note; a "not affiliated with Google" note.
- `docs/authentication.md`: the **manual cookie extraction** guide — the 5 cookies `COMPASS`, `SSID`, `SID`, `OSID`, `HSID` from `chat.google.com` via browser devtools (step-by-step for Chrome/Firefox: DevTools → Application/Storage → Cookies → `https://chat.google.com`), how to paste them into the `login` flow, the security warning (cookies are full-session credentials — never share/commit), and the known caveat that cookie logins can be short-lived vs. a full web-app auth.
- `CHANGELOG.md`: Keep-a-Changelog format; an `## Unreleased` (or first-version) section summarizing the Go rewrite feature set M0–M7 at a high level (not a commit dump).

- [ ] **Steps:** draft each doc → self-review for accuracy against the actual code (verify the config keys, command names, and cookie names exist — grep `login.go`/`connector.go`/`gchatmeow`) → **commit** `docs: README, authentication guide, and changelog`.

---

### Task 2: Release engineering — Dockerfile caching, docker-run exit check, CI review

**Files:** `Dockerfile`; `docker-run.sh`; `.github/workflows/go.yml`.

- `Dockerfile`: add Go dependency-layer caching — `COPY go.mod go.sum ./` + `RUN go mod download` BEFORE `COPY . .`, so dependency layers cache across source changes. Keep the `-tags goolm` build and the goolm path (hard rule: never remove the goolm build-tag path). Keep the final alpine stage + ffmpeg/ca-certificates.
- `docker-run.sh`: add an explicit exit-code check on the config/registration generation step so a failure aborts the container instead of silently continuing (the M0-deferred item).
- `.github/workflows/go.yml`: verify it runs `go build`, `go vet`, and `go test` all with `-tags goolm` on Go 1.25+; add `gofmt` check if absent; ensure the race detector runs on the connector package (or the whole suite). Minimal, correct, no new external actions beyond what's needed.

- [ ] **Steps:** make the Dockerfile/CI/script changes → verify `docker build` args are coherent (don't need to run Docker; verify the Dockerfile is well-formed and the caching COPY order is right) → run `./build.sh` and the full `-tags goolm` gate locally → **commit** `build: docker layer caching, docker-run exit check, CI gate review`.

---

### Task 3: Deferred-minors cleanup batch

**Files:** `pkg/msgconv/gchatfmt/*.go`, `pkg/msgconv/matrixfmt/*.go`, `pkg/connector/mentions.go`, `pkg/gcid/*.go`, `pkg/connector/events.go`, `pkg/connector/userinfo.go`, `pkg/connector/media.go` (+ their tests). Each fix is small and independently testable.

Triage + fix the accumulated backlog (each with a test; skip any that on inspection is a non-issue and say so in the report):
1. **gchatfmt `javascript:` (and `data:`) URL scheme** — filter/neutralize dangerous link schemes in outbound formatting (security; mirror the M3 href-escape fix). Test a `javascript:` link is neutralized.
2. **matrixfmt `AllowedMentions` / `@room`** — cover the untested AllowedMentions branch; ensure `@room` respects `AllowedMentions.Room` (don't bypass). Tests.
3. **mentions.go DB-error paths** — log (don't silently swallow) DB errors. Test or assert via a seam.
4. **`MakeAttachmentPartID` zero-padding** — decide: zero-pad so `att_10` sorts after `att_9`, OR document why the current form is safe (GC realistically never exceeds single-digit attachment counts, and part ordering isn't lexical in the code paths that matter). If changing the FORMAT, remember gcid is FROZEN — a format change is a migration concern; prefer documenting the bound unless there's a real ordering bug. Resolve explicitly in the report.
5. **StreamOrder on live events** — set `StreamOrder` on live inbound messages (events.go) to the create-time µs for consistency with backfill (M6 minor). Test.
6. **strict base64 in upload finalize** — tolerate/handle embedded whitespace like Python, or document why strict is fine. Test.
7. **mime-mismatch test** — add the missing regression test for annotation-vs-downloaded content-type in media.go.
8. **FormatDisplayname template errors** — surface/log template execution errors instead of discarding. Test.

- [ ] **Steps:** for each item, RED test (where behavioral) → fix → GREEN; for doc-only resolutions, state the reasoning in the report → full `-tags goolm` gate + race → **commit** `fix: deferred-minors cleanup (security, logging, consistency)`. (One commit for the batch is fine; keep the message body enumerating the items.)

---

### Task 4: Migration — CLI scaffold, source-DB reader, and pre-flight verifications

**Files:** create `pkg/migrate/migrate.go` (the migration engine, importable + testable), `pkg/migrate/source.go` (Python-DB reader), `cmd/mautrix-googlechat/main.go` (wire a `migrate` subcommand/flag). Test: `pkg/migrate/migrate_test.go`.

- Wire a migration entry point on the existing binary (read how `mxmain.BridgeMain` supports subcommands/`PostInit` or add a top-level flag like `--migrate-from-python <dsn>`; the migration opens the SOURCE Python DB via `dbutil` (SQLite file or Postgres DSN — detect by scheme) read-only, and the TARGET Go DB via the bridge's own configured `dbutil.Database`).
- **Pre-flight verifications (do these reads NOW, they gate correctness — the map flagged them):**
  - `maugclib.Cookies` field names/casing (read `_reference/googlechat-python/mautrix_googlechat/.../maugclib` cookie definition) vs `gchatmeow.RequiredCookies` (`COMPASS/SSID/SID/OSID/HSID`) — confirm the JSON key casing so the cookie blob copies verbatim (or build the key remap).
  - The target bridge's **puppet-mxid username template** (read mxmain/bridge config + how bridgev2 computes a ghost's mxid) so `sender_mxid` is derived to match a live bridge.
  - bridgev2 **double-puppet storage** location (read bridgev2 double-puppet code) — decide migrate-vs-drop for Python `puppet.custom_mxid/access_token/next_batch/base_url`.
  - The source DB **schema version** bookkeeping (mautrix async_db version table) to decide ms-vs-µs timestamps (multiply by 1000 only if < v10).
- Source reader: functions to enumerate Python rows per entity (portals, messages, reactions, puppets, users) with typed structs. Handle NULLs.
- Engine skeleton: `Run(ctx, opts)` with `DryRun bool`, `Force bool`; opens a single target transaction; dispatches to the per-entity migrators (Tasks 5–7); collects a summary/warnings; rolls back on dry-run or error.

- [ ] **Steps:** read the map + do the 4 pre-flight reads (record findings in the report) → write the source reader + engine skeleton with a dry-run that connects to a tiny fixture SQLite Python DB and reports counts → tests against a hand-built fixture SQLite DB (create the Python schema + a couple rows in test setup) → RED→GREEN + race → audit → **commit** `feat: migration CLI scaffold and Python-DB source reader`.

---

### Task 5: Migration — Portal + Ghost

**Files:** `pkg/migrate/portal.go`, `pkg/migrate/ghost.go` (+ tests). Follow map §1 (Portal) and §4 (Ghost) EXACTLY.

- Portal: identity-copy id/receiver/other_user_id/mxid/name/avatar_mxc; `description→topic`, `description_set→topic_set`; coalesce NULLs to `""` for Go NOT-NULL columns; compute `room_type` (`dm:`→DM else Default, NEVER Space); `revision→PortalMetadata.Revision`, `threads_only/threads_enabled→PortalMetadata`; leave the no-source columns at documented defaults; drop `encrypted` (room state preserves it).
- Ghost: `gcid→id` (MakeUserID); name/photo_id→avatar_id/photo_mxc→avatar_mxc; `photo_hash→avatar_hash` (hex-string copy, validate 64 hex chars); `*_set` copies; drop `is_registered`; leave Email/identifiers/is_bot at defaults (self-heal on next sync).

- [ ] **Steps:** tests (a Python portal row → the right Go portal row incl. room_type from prefix, description→topic, revision in metadata; a DM vs space portal; a puppet → ghost incl. avatar_hash hex copy + NULL coalescing) → RED→GREEN → audit vs map §1/§4 → **commit** `feat: migrate portals and ghosts`.

---

### Task 6: Migration — Message + Reaction (the index→part_id rule)

**Files:** `pkg/migrate/message.go`, `pkg/migrate/reaction.go` (+ tests). Follow map §2 (Message) and §3 (Reaction) EXACTLY, with the plan's resolved single-row decision (`""`).

- Message: group Python rows by `(gcid,gc_chat,gc_receiver)`; apply the index→part_id rule (single-row→`""`; multi-row→text row `""` + attachments `att_0..` by ascending index, or all-attachments `att_k` when the lowest-index row is non-text); identity-copy id/room_id/room_receiver/mxid; `gc_parent_id→thread_root_id` + `MessageMetadata.TopicID` (gc_parent_id or self); `gc_sender→sender_id` (MakeUserID) + derive `sender_mxid` (Task 4 template); timestamp→time + `MessageMetadata.TimestampMicro` (µs, per Task 4 version check); `edit_count=0`; nullable no-source columns NULL.
- Reaction: `emoji→emoji_id` (bare) + `emoji`=`variationselector.Add`; `gc_sender→sender_id`; look up the `index=0` message row and apply the index→part_id rule to get `message_part_id` (do NOT hardcode `""`); identity-copy message_id/room_id/room_receiver/mxid/timestamp; leave ReactionMetadata.TopicID empty.

- [ ] **Steps:** tests (text+2 attachments → `""`,`att_0`,`att_1`; attachment-only multi-row → `att_0`,`att_1`; single-row → `""`; a reaction resolves message_part_id via the index-0 row; emoji variationselector.Add; timestamp µs; sender_mxid derived) → RED→GREEN + race → audit vs map §2/§3 (esp. the index rule) → **commit** `feat: migrate messages and reactions`.

---

### Task 7: Migration — User + UserLogin + user_portal + backfill_task seeding

**Files:** `pkg/migrate/user.go`, `pkg/migrate/userportal.go` (+ tests). Follow map §5, §6, and Risk #11.

- User/UserLogin: split Python `user` → Go `user` (mxid, notice_room→management_room) + (only if `cookies IS NOT NULL`) `user_login` keyed by `MakeUserLoginID(gcid)`; `UserLoginMetadata.Cookies` from the JSON blob (Task 4 key casing), `UserAgent`, `Revision`. Skip NULL-cookie users (documented: they re-login).
- user_portal: synthesize DM rows only (`user_mxid` via `user.gcid==portal.gc_receiver`, `login_id=receiver`, portal key); defaults for in_space/preferred/last_read; spaces = accepted gap (log a per-user count of un-reconstructable space memberships).
- backfill_task: per Task 4's verification of the auto-create trigger, seed per migrated portal to SUPPRESS a redundant backward backfill of already-present history (e.g. a done/queue-done row), OR document why no seeding is needed. Whichever, ensure a freshly-migrated portal does NOT re-backfill its whole history nor lose future backfill capability.

- [ ] **Steps:** tests (a user with cookies → user + user_login with the cookie map + revision; a NULL-cookie user → user only, no login; a DM portal → a user_portal row; backfill_task seeding behavior) → RED→GREEN → audit vs map §5/§6 → **commit** `feat: migrate users, logins, user-portals, and backfill state`.

---

### Task 8: Migration — end-to-end dry-run, summary, and migration docs

**Files:** `pkg/migrate/migrate.go` (wire all entities + ordering + FK-safe sequence: users→ghosts→portals→messages→reactions→user_portal→backfill_task), `pkg/migrate/migrate_test.go` (an end-to-end fixture DB → full migrate → assert every table), `docs/migration.md`.

- End-to-end: build a representative fixture Python SQLite DB (a user with cookies, a DM + a space portal, a few multi-part messages with attachments, reactions, puppets), run the full migration into a temp Go DB, assert every target table's rows are correct and FK-consistent. Dry-run prints the summary without writing; real run writes in one transaction; `--force` guards a non-empty target.
- `docs/migration.md`: how to run it (`mautrix-googlechat --migrate-from-python <python-dsn>` against a configured Go DB), prerequisites (stop the Python bridge, back up both DBs, point the Go bridge at the SAME appservice registration / Matrix rooms), the documented caveats (solo-attachment-inbound part_id, attachment index compaction, space user_portal self-heal, NULL-cookie users re-login, double-puppet handling per Task 4's decision), and the post-migration checklist.

- [ ] **Steps:** end-to-end fixture test → RED→GREEN + race → write the doc → audit → **commit** `feat: end-to-end Python-to-Go migration with dry-run and docs`.

---

### Task 9: M7 exit verification + whole-branch review + release prep

**Files:** possibly a version bump (`version.go`/build var if present); no functional code.

- [ ] **Step 1:** Full gate: gofmt, `go build/vet/test -tags goolm ./...`, race, both layering greps, `/verify-milestone`.
- [ ] **Step 2:** Whole-branch review (cross-cutting: does the migration write rows byte-identical to what a live Go bridge would create for the same GChat entities — so a migrated portal/message behaves exactly like a natively-bridged one; are the frozen gcid formats used everywhere in the migration; does the migration compose safely (dry-run default, single transaction, no source mutation, target-non-empty guard); do the cleanup fixes not regress M1–M6; are the docs accurate against the shipped commands/config). Triage any remaining deferred-minors. Fix findings.
- [ ] **Step 3:** Release prep: confirm the version string/tag scheme; ensure CHANGELOG reflects the final state; the OWNER performs the actual `git tag` + deployment (note this in the exit). superpowers:finishing-a-development-branch → merge to main.
- **Exit:** Docs (README + auth + migration + changelog) accurate; CI/Docker polished; deferred-minors cleared or explicitly deferred; the migration tool converts a representative Python DB into a working Go bridge DB (dry-run + real, both tested); everything ready for the owner to tag a release and deploy on continuwuity.

Dependency order: Tasks 1, 2, 3 are independent (docs/build/cleanup) and can be done in any order. Migration Tasks 4→5→6→7→8 are sequential (4 scaffolds; 5/6/7 fill entities; 8 wires end-to-end). Task 9 last.
