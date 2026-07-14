# 08b — Megabridge Connector Coverage Audit (`pkg/connector/`)

Audit of the bridgev2 connector layer in the unfinished upstream Go rewrite.

- **Code audited**: every file in `/Users/danielagoston/utmshared/Projects/git/generalclaude/mautrix/_reference/googlechat-megabridge/pkg/connector/` — 8 files, 1,115 lines total: `client.go` (282), `connector.go` (62), `handlegchat.go` (187), `handlematrix.go` (235), `ids.go` (32), `login.go` (143), `mapping.go` (106), `portal.go` (68). Cross-checked against `pkg/gchatmeow/` (client lib) and `cmd/mautrix-googlechat/main.go` where the connector's correctness depends on them.
- **Grounding**: Python parity baseline from `docs/research/03-python-bridge-features.md`; bridgev2 mechanism mapping from `docs/research/07-gap-analysis.md` §1.
- **Framework pin**: `go.mod:13` — `maunium.net/go/mautrix v0.23.3` (stale; current bridgev2 connectors track much newer commits — interface drift is a real rebase cost).

All `file:line` citations below are relative to `_reference/googlechat-megabridge/` unless prefixed otherwise.

---

## 1. Coverage table: bridgev2 interface/method → status

Legend: **REAL** = plausible working implementation; **PARTIAL** = works for a subset of the Python behavior; **STUB** = empty/hardcoded; **SUSPICIOUS** = present but likely wrong at runtime; **ABSENT** = not implemented at all.

### 1.1 NetworkConnector (`connector.go`, compile-asserted at `pkg/connector/connector.go:16`)

| Method | Status | Where | Quality notes |
|---|---|---|---|
| `Init` | REAL (trivial) | `pkg/connector/connector.go:19` | Stores `*bridgev2.Bridge` only. |
| `Start` | STUB | `pkg/connector/connector.go:23` | `return nil`. No DB upgrade hooks, no error-code registration (`BridgeStateHumanErrors`), nothing. |
| `GetName` | REAL | `pkg/connector/connector.go:27` | Correct BridgeName incl. `BeeperBridgeType: "googlechat"`, port 29320. |
| `GetCapabilities` (network-level) | STUB | `pkg/connector/connector.go:38` | Returns empty `&bridgev2.NetworkGeneralCapabilities{}`. Defensible defaults, but nothing considered (e.g. `AggressiveUpdateInfo`). |
| `GetBridgeInfoVersion` | REAL (trivial) | `pkg/connector/connector.go:42` | `1, 1`. |
| `GetConfig` | **STUB** | `pkg/connector/connector.go:46` | `return "", nil, nil` — **no network config section exists at all**. No example YAML, no upgrader. Python's config surface (03 §6: `displayname_template`, `initial_chat_sync`, backfill limits, proxy, etc.) is entirely unrepresented. |
| `GetDBMetaTypes` | PARTIAL | `pkg/connector/connector.go:50` | Only `Portal` (`PortalMetadata`, `pkg/connector/portal.go:12`) and `UserLogin` (`UserLoginMetadata`, `pkg/connector/login.go:47`). `Ghost`/`Message`/`Reaction` metadata nil — no `LastEditTime` for edit dedup (07 §1.3), no ghost email. |
| `GetLoginFlows` | REAL | `pkg/connector/login.go:25` | Single "cookies" flow. |
| `CreateLogin` | REAL | `pkg/connector/login.go:21` | Returns `GChatCookieLogin`; ignores `flowID` (fine, one flow). |
| `LoadUserLogin` | PARTIAL | `pkg/connector/login.go:33` | Builds `gchatmeow.NewClient(cookies, "", 0, 0)` from metadata. If `Cookies == nil`, `login.Client` wraps a **nil client** — any later call (e.g. `Connect` → `client.OnConnect` at `pkg/connector/client.go:44`) nil-panics instead of sending `StateBadCredentials`. No stored user-agent (Python persists and replays it, 03 §2.2). |

Not implemented on the connector: `MaxFileSizeingNetwork`, `PortalBridgeInfoFillingNetwork` (Python sets `fi.mau.googlechat.threads_only/threads_enabled` in bridge info, 03 §3.1), legacy-DB migration hooks (`mxmain.BridgeMain.CheckLegacyDB` not wired in `cmd/mautrix-googlechat/main.go:31-37`).

### 1.2 NetworkAPI core (`client.go`, compile-asserted at `pkg/connector/client.go:31`)

| Method | Status | Where | Quality notes |
|---|---|---|---|
| `Connect` | PARTIAL/SUSPICIOUS | `pkg/connector/client.go:43` | Registers `OnConnect`/`OnDisconnect`/`OnStreamEvent` observers then calls `client.Connect(ctx, 90*time.Minute)` (non-blocking; spawns `channel.Listen` goroutine, `pkg/gchatmeow/client.go:116`). Sends `StateConnected` / `StateTransientDisconnect` / `StateBadCredentials`. Problems: (1) observers are appended on every `Connect` call — a reconnect would double-register handlers; (2) captures the outer `ctx` into observers that fire much later; (3) no reconnect supervision at the connector level (backoff ladder, SID error taxonomy, hourly periodic sync, `ChannelLifetimeExpired` recycle from Python `user.py:299-345` are absent here and only very roughly approximated inside `gchatmeow/channel.go`). |
| `Disconnect` | **STUB** | `pkg/connector/client.go:67` | Empty body. Long-poll channel is never torn down on logout/shutdown. |
| `IsLoggedIn` | **STUB** | `pkg/connector/client.go:171` | `return true` unconditionally. |
| `LogoutRemote` | **STUB** | `pkg/connector/client.go:179` | Empty body (Python doesn't invalidate cookies remotely either, but it does disconnect + clear state). |
| `IsThisUser` | REAL | `pkg/connector/client.go:175` | Login-ID string compare. |
| `GetChatInfo` | REAL | `pkg/connector/client.go:161` → `pkg/connector/mapping.go:81` | `GetGroup` with `MEMBERS` fetch option; DM vs group-DM room type; member list with `OtherUserID`, own PL 50 (`pkg/connector/mapping.go:65`, matches Python `portal.py:638`). Missing: group **topic/description** (Python sets room topic, 03 §1.2), `CanBackfill`, threads flags capture. On error returns non-nil empty `ChatInfo` alongside the error (`pkg/connector/mapping.go:89`) — smell. |
| `GetUserInfo` | PARTIAL | `pkg/connector/client.go:166` → `pkg/connector/mapping.go:39` | Raw `user.Name` only. Missing Python's displayname template `{full_name} (Google Chat)` + fallback chain name→first+last→email (03 §3.3), `Identifiers: ["mailto:…"]`, `IsBot`. Avatar fetched via plain unauthenticated `http.Get` (`pkg/connector/mapping.go:27`) with no scheme forcing and no status-code check. |
| `GetCapabilities` (per-portal) | SUSPICIOUS | `pkg/connector/client.go:154` | See §4 below — space detection via `strings.Contains(portal.ID, "space")` against a prototext-serialized ID; threads granted to *all* spaces; Python's `threads_only`/`threads_enabled` per-portal model (03 §3.1) not represented. Caps content itself (`pkg/connector/client.go:77-152`) is thoughtful (12,000-char limit, mime maps, `ReactionCount: 1`) but `FmtCodeBlock`/ordered lists/blockquote marked unsupported where Python supports MONOSPACE_BLOCK both directions (03 §4). |

### 1.3 Login flow (`login.go`)

| Item | Status | Where | Quality notes |
|---|---|---|---|
| `LoginProcessCookies` | REAL | asserted `pkg/connector/login.go:51`; `Start` `pkg/connector/login.go:53`, `SubmitCookies` `pkg/connector/login.go:96`, `Cancel` `pkg/connector/login.go:94` | 5 cookie fields (COMPASS/SSID/SID/OSID/HSID from `pkg/gchatmeow/cookies.go:16`), `chat.google.com` domain hint for domain-specific cookies, COMPASS `dynamite-ui=` pattern hint — this is the most polished file in the package. Validates via `RefreshTokens` (`/mole/world` WIZ scrape, `pkg/gchatmeow/client.go:120`) + `GetSelf`, then `User.NewLogin` with cookies in metadata and remote profile (name/email/avatar). |
| Post-login connect | **SUSPICIOUS** | `pkg/connector/login.go:129-132` | `ul.Client.Connect(ctx)` is called on the client built by `LoadUserLogin` (client "M"), **then** `c.client = client` swaps in the warmed login client "W" *after* observers/channel were already started on M. Stream events flow from M; all subsequent RPCs go through W. Two live client instances sharing a `*Cookies` pointer, two separate XSRF tokens/cookie jars. Works by accident at best; `c.userLogin = ul` at line 131 is redundant (already set in `NewClient`). |
| Relogin / override | **ABSENT** | — | No `LoginProcessWithOverride` / `StartWithOverride`; re-login after cookie expiry goes through the generic flow only (07 risk #3 recommends override support for painless relogin). |
| Cookie rotation persistence | **ABSENT** | — | `UserLoginMetadata{Cookies}` (`pkg/connector/login.go:47`) is written once at login. Rotated cookies live only in the `net/http` cookiejar (`pkg/gchatmeow/session.go:51-103`); nothing copies them back into the metadata struct and **no `login.Save(ctx)` call exists anywhere in the connector** (only `portal.Save`, `pkg/connector/portal.go:30`). Python re-persists rotated cookies after every connect (`user.py:279-283`, 03 §2.2) — without this, any restart after Google rotates cookies = dead login. |
| UserLoginMetadata shape | PARTIAL | `pkg/connector/login.go:47` | `{Cookies *gchatmeow.Cookies}` only. Missing vs Python `user` table: `user_agent`, user-level `revision` watermark (03 §3.4), and vs 07 §1.3: `BackfillCompleted`. |

### 1.4 Matrix→GC handling (`handlematrix.go`, assertions at `pkg/connector/handlematrix.go:14-20`)

| Interface / method | Status | Where | Quality notes |
|---|---|---|---|
| `HandleMatrixMessage` | REAL (mostly) | `pkg/connector/handlematrix.go:22` | Thread routing: `ThreadRoot` → `CreateMessage` with `TopicId` parent (line 80-101), else `CreateTopic` (line 103-115) — matches Python. Quote replies via `SendReplyTarget{Id, CreateTime}` using stored µs timestamp (line 57-67, correct because DB timestamps are stored `time.UnixMicro` at line 121). Media: `DownloadMedia` → `UploadFile` → `UPLOAD_METADATA` annotation (line 37-55) — and unlike Python, media keeps a text body/caption. Issues: (1) `LocalId` set only on `CreateMessage` (line 91), **not** on `CreateTopic` — no echo-suppression txn for new topics; (2) echo dedup is `AddPendingToIgnore(responseMsgID)` *after* the RPC returns (line 117) — race window where the stream echo can arrive before the pending entry exists (framework DB-dedup by message ID is the only backstop); Python's `local_id` round-trip (07 §1.1) not implemented; (3) media send doesn't stream (whole file in `[]byte`); (4) no per-message send lock / force-stop-typing (Python `portal.py:1061`). |
| `EditHandlingNetworkAPI.HandleMatrixEdit` | REAL | `pkg/connector/handlematrix.go:127` | `EditMessageRequest` with topic-id fallback (thread root else message id). No `last_edit_time` dedup stored (no Message metadata). |
| `RedactionHandlingNetworkAPI.HandleMatrixMessageRemove` | REAL | `pkg/connector/handlematrix.go:149` | `DeleteMessageRequest` with same topic-id fallback. |
| `ReactionHandlingNetworkAPI.PreHandleMatrixReaction` | PARTIAL | `pkg/connector/handlematrix.go:165` | Returns SenderID/EmojiID/Emoji, but **does not strip emoji variation selectors** (Python: `variation_selector.remove`, `portal.py:766`; 07 §1.1 calls for `variationselector.Remove`). VS16-carrying reactions will likely fail or duplicate on the GC side. Also no `MaxReactions` enforcement interplay noted. |
| `HandleMatrixReaction` | REAL | `pkg/connector/handlematrix.go:174` | `UpdateReaction ADD`; returns nil `*database.Reaction` (framework tolerates, but metadata-less). |
| `HandleMatrixReactionRemove` | REAL | `pkg/connector/handlematrix.go:180` | Looks up thread root from message DB for topic id; `UpdateReaction REMOVE`. |
| `TypingHandlingNetworkAPI.HandleMatrixTyping` | REAL | `pkg/connector/handlematrix.go:210` | `SetTypingState` with group-scoped `TypingContext`. Topic-scoped typing (inside a thread) not attempted — acceptable. |
| `ReadReceiptHandlingNetworkAPI.HandleMatrixReadReceipt` | REAL | `pkg/connector/handlematrix.go:229` | `MarkGroupReadstate` with µs conversion. Matches Python `user.py:684`. |

Not implemented (also absent in Python — deliberate parity): `MembershipHandlingNetworkAPI`, `RoomName/Topic/AvatarHandlingNetworkAPI`, `IdentifierResolvingNetworkAPI`/`GhostDMCreatingNetworkAPI` (no DM creation from Matrix), `TransactionIDGeneratingNetwork`.

### 1.5 GC→Matrix event ingestion (`handlegchat.go`)

Entry: `onStreamEvent` (`pkg/connector/handlegchat.go:33`), fed both by the live stream (multi-body events are split in the client lib before dispatch, `pkg/gchatmeow/client.go:204`) and by catch-up replay (`pkg/connector/portal.go:62-66`).

| GChat event | Python behavior (03 §1.2) | Megabridge status | Where |
|---|---|---|---|
| `MESSAGE_POSTED` (normal) | New message, multi-part | REAL — `simplevent.Message` with `ConvertMessageFunc: msgConv.ToMatrix`, `CreatePortal: true` | `pkg/connector/handlegchat.go:41-50` |
| `MESSAGE_POSTED` with `MessageType == SYSTEM_MESSAGE` | **Parsed**: `MEMBERSHIP_CHANGED` annotation → 9 membership types; `ROOM_UPDATED` → rename; `group_details_metadata` → topic (`portal.py:1281,1262,1272`) | **DROPPED** — explicitly skipped | `pkg/connector/handlegchat.go:43` |
| `MESSAGE_UPDATED` | Edit (text only, dedup by `last_edit_time`) | REAL-ish — `simplevent.Message` with `ConvertEditFunc`; **no edit dedup**; `ConvertEdit` edits the **last** part and copies the whole converted message into it (`pkg/connector/handlegchat.go:175-187`) — diverges from Python's part-0-text-only rule and misbehaves on multipart (text+attachment) messages | `pkg/connector/handlegchat.go:51-60` |
| `MESSAGE_DELETED` | Redact all parts | REAL — `simplevent.Message` with `TargetMessage` only (framework `MessageRemove` semantics via meta type) | `pkg/connector/handlegchat.go:61-67` |
| `MESSAGE_REACTION` | Add/remove, variation selector added toward Matrix | PARTIAL — handled *outside* the type switch on every event via `body.GetMessageReaction()` (`pkg/connector/handlegchat.go:92,95-125`); **no `variationselector.Add`** toward Matrix; good `LogContext` though | `pkg/connector/handlegchat.go:95` |
| `TYPING_STATE_CHANGED` | 6s timeout typing, routed via `body.typing_state_changed.context.group_id` because **`Event.group_id` is empty for typing events** (07 §1.2) | **SUSPICIOUS** — `simplevent.Typing` built with `makePortalKey(evt)` = `evt.GroupId` (`pkg/connector/handlegchat.go:68-72` → `pkg/connector/ids.go:11`); the body's `TypingContext` (field exists: `pkg/gchatmeow/proto/googlechat.proto:1457`) is ignored → typing events likely target an empty/garbage portal key. Also never sets `IsTyping=false`/timeout type. | `pkg/connector/handlegchat.go:68` |
| `READ_RECEIPT_CHANGED` | Map ts → closest earlier message | REAL — one `simplevent.Receipt{ReadUpTo}` per receipt in the set (framework does the closest-message mapping) | `pkg/connector/handlegchat.go:73-83` |
| `GROUP_VIEWED` | Own read marker (`portal.py:556`) | **DROPPED** — no case | — |
| `GROUP_UPDATED` | (Python gets renames via SYSTEM_MESSAGE) | PARTIAL — name+avatar `ChatInfoChange` (`pkg/connector/handlegchat.go:127-141`); **no topic/description**; relies on Google actually emitting this standalone event type on the stream, which the Python bridge never depended on — unverified against live traffic | `pkg/connector/handlegchat.go:84` |
| `MEMBERSHIP_CHANGED` | (Python gets these via SYSTEM_MESSAGE annotation) | PARTIAL — only `MEMBER_JOINED`/`MEMBER_NOT_A_MEMBER`; `MEMBER_INVITED` commented out (`pkg/connector/handlegchat.go:156-157`); vs Python's 9 membership-change types (JOINED/INVITED/ADDED/BOT_ADDED/LEFT/REMOVED/BOT_REMOVED/KICKED…). Same live-delivery caveat as GROUP_UPDATED. | `pkg/connector/handlegchat.go:86,143` |
| Retention/hide/block/DM-creation stream events | Skipped in Python too | ABSENT (parity) | — |

**Net vs the Python-handled body set**: message_posted ✔ (minus SYSTEM_MESSAGE parsing), message_deleted ✔, message_reaction ◐ (no variation selectors), typing_state_changed ✖ (wrong portal key), read_receipt_changed ✔, group_viewed ✖. SYSTEM_MESSAGE-derived membership/rename/topic flows are replaced by a *different, unverified* mechanism (standalone GROUP_UPDATED/MEMBERSHIP_CHANGED events) and are partial even if that mechanism works.

### 1.6 Chat-list sync, backfill, portal metadata (`client.go` `onConnect`, `portal.go`)

| Item | Status | Where | Quality notes |
|---|---|---|---|
| Chat-list sync on connect | **SUSPICIOUS (likely crashes)** | `pkg/connector/client.go:210-282` | `client.Sync` = `paginatedWorld` (`pkg/gchatmeow/client.go:255`, `pkg/gchatmeow/api.go:124`), then per world item a `simplevent.ChatResync` with `CreatePortal: true`. Defects: (1) author's own debug notes `// NOTE(skip): this ends up being empty` (`pkg/connector/client.go:218,234`) — the DM-member map built from `item.DmMembers` comes back empty, so `c.users` stays empty and `dmUser` is nil → **`&dmUser.Name` at `pkg/connector/client.go:255` nil-panics on the first DM** (Python reads DM members from `read_state.joined_users` *or* `dm_members`, 03 §3.2 — the Go code only reads `DmMembers`); (2) **no filtering** of blocked/hidden/non-joined chats (Python `user.py:630-635`); (3) **no sort + `initial_chat_sync` cap** — every world item gets a portal; (4) one blocking `GetGroup` per non-DM item serially inside connect; (5) raw `log.Printf` debugging throughout (also `pkg/connector/handlegchat.go:36`) instead of zerolog. |
| `BackfillingNetworkAPI` / `FetchMessages` | **ABSENT** | — | No compile assertion, no `FetchMessages` anywhere. Initial history backfill (Python `_initial_backfill` via `ListTopics`/`ListMessages`, 03 §5.1) is impossible today: the client lib has **no ListTopics/ListMessages RPCs at all** (`pkg/gchatmeow/api.go:95-196` — the RPC surface stops at 12 wrappers). |
| Catch-up (revision-gap) backfill | PARTIAL | `pkg/connector/portal.go:35-68` | `CatchUpGroup` between stored `PortalMetadata.Revision` and world revision, replayed through `onStreamEvent` via `SplitEventBodies` — architecturally matches Python `_catchup_backfill` (03 §5.2). Missing: `PAGINATED` continuation loop (Python loops while status `PAGINATED`; here a single call), status checking of `res` entirely, and the "no stored revision ⇒ skip catch-up, do initial backfill" rule — a fresh portal has `Revision == 0` and requests the group's entire event history from timestamp 0 in one shot. Runs synchronously inside `onConnect`, serially per chat, bypassing bridgev2's backfill queue. |
| `PortalMetadata` | PARTIAL | `pkg/connector/portal.go:12` | `{Revision int64}` only. Missing `ThreadsOnly`/`ThreadsEnabled` (drives capabilities + reply rerouting, 03 §3.1/07 §1.1) and `OtherUserID`. |
| Revision persistence | SUSPICIOUS | `pkg/connector/portal.go:16-33` | Called on **every** stream event; `GetPortalByKey` will auto-create portal rows for whatever key the event carries (including the empty/garbage keys from typing events); `portal.Save` on every event = DB write per event; and `zerolog ... .Err(err).Msg("Failed to update portal revision in database")` at `pkg/connector/portal.go:31` logs the failure message **unconditionally** (even when `err == nil`). |
| Bridge states | PARTIAL | `pkg/connector/client.go:45-63` | `StateConnected`, `StateTransientDisconnect`, `StateBadCredentials` (with hardcoded error string `"googlechat-invalid-credentials"`). No `StateBackfilling`, no `BridgeStateHumanErrors` catalog, no distinction of SID-invalid vs network vs auth failures (Python's ladder in `user.py:299-345`). |

### 1.7 ID formats (`ids.go`, `mapping.go`) — cross-cutting

**SUSPICIOUS, highest-severity design flaw.** `networkid.PortalID` is set to `evt.GroupId.String()` / `item.GroupId.String()` (`pkg/connector/ids.go:13`, `pkg/connector/client.go:272`, `pkg/connector/portal.go:18,37`) — i.e. **prototext serialization of the `GroupId` proto** — and parsed back with `prototext.Unmarshal` (`pkg/connector/mapping.go:17-21`, `pkg/connector/ids.go:20`). Three problems:

1. `prototext` output is **deliberately unstable** across protobuf-go releases (the library injects randomized whitespace to prevent reliance on stable output). Persisted portal IDs can stop matching newly generated ones after a dependency bump — permanent DB corruption vector.
2. It diverges from Python's `dm:<id>` / `space:<id>` format (03 §3.1), which report 07 (§1.3, risk #6) pins as a hard requirement for legacy-DB migration. Migration from Python deployments is impossible with these IDs.
3. `GetCapabilities`' `strings.Contains(portal.ID, "space")` (`pkg/connector/client.go:155`) works only because the prototext happens to contain `space_id:` — a fragile side effect of #1.

Additionally there is **no receiver-scoping distinction**: `Receiver: c.userLogin.ID` is set for *all* portals including spaces (`pkg/connector/ids.go:14`, `pkg/connector/client.go:274`), whereas Python (and 07 §1.3) uses empty receiver for spaces so multiple bridge users share one space portal. With megabridge's scheme every user gets a private copy of every space, and messages from a second logged-in user in the same space bridge into separate rooms.

---

## 2. (a) Missing entirely vs Python parity

1. **Config** — the whole network config section (`GetConfig` returns nils, `pkg/connector/connector.go:46`): displayname template, initial_chat_sync, backfill limits, proxy, etc. (03 §6).
2. **Initial history backfill** — no `BackfillingNetworkAPI.FetchMessages`, and no `ListTopics`/`ListMessages` RPCs in the client lib to build it on (03 §5.1, 07 §1.2/M6).
3. **SYSTEM_MESSAGE parsing** — membership changes (9 types), room renames, room topic/description changes as Python actually receives them (`pkg/connector/handlegchat.go:43` drops them; 03 §1.2).
4. **`GROUP_VIEWED` own-read-marker** handling (03 §1.2 `portal.py:556`).
5. **Cookie rotation persistence** — no `login.Save` anywhere; rotated cookies lost on restart (03 §2.2 `user.py:279-283`).
6. **Relogin/override flow** (`LoginProcessWithOverride`) and any real `IsLoggedIn`/auth-expiry detection (07 risk #3).
7. **Connection lifecycle supervision** — periodic hourly resync, post-`SIDInvalid` limited resync, backoff/notice escalation ladder, disconnect teardown (Python `user.py:259-345`; connector `Disconnect` is empty).
8. **Threads metadata** — `threads_only`/`threads_enabled` per portal, reply→thread rerouting policy, threads-only room handling (03 §3.1; capabilities are guessed from a substring instead).
9. **Ghost identity polish** — displayname template + fallback chain, `mailto:` identifiers, bot flag, avatar dedup inputs (03 §3.3).
10. **Chat filtering/capping at sync** — blocked/hidden/non-joined skip, sort by `sort_timestamp`, `initial_chat_sync` cap (03 §3.4).
11. **User-level revision watermark** + `user_agent` in `UserLoginMetadata` (03 §3.4, §2.2).
12. **Legacy Python DB migration** — no `CheckLegacyDB` wiring in `cmd/mautrix-googlechat/main.go:31-37`, and the chosen ID format actively precludes it (§1.7).
13. **Message metadata for edit dedup** (`last_edit_time`) — `GetDBMetaTypes` registers no Message meta (`pkg/connector/connector.go:55-57`).
14. **Emoji variation-selector handling** both directions (03 §1.1/1.2).
15. **Bridge error-code catalog / richer bridge states** (`StateBackfilling`, human-readable error map).

(Deliberate non-parity omissions that match Python's own gaps — presence, Matrix→GC membership/metadata, DM creation, relay-specifics — are not counted.)

## 3. (b) Present but wrong / stubby

1. **Portal ID = prototext of `GroupId`** — unstable serialization as a permanent DB key + wrong receiver scoping for spaces (§1.7). Fixing this later means re-IDing every portal.
2. **Chat-list sync nil-deref** — `&dmUser.Name` with `dmUser == nil` (`pkg/connector/client.go:255`), and the author's own `NOTE(skip): this ends up being empty` comments (`client.go:218,234`) show the DM-members path was known-broken when work stopped. First DM in the world list likely crashes `onConnect`.
3. **Typing events use `evt.GroupId`** which is empty for typing (should use `body.typing_state_changed.context.group_id`) — `pkg/connector/handlegchat.go:68-72`; combined with `setPortalRevision`'s get-or-create (`pkg/connector/portal.go:21`), garbage portal rows get created.
4. **`IsLoggedIn` hardcoded `true`**, `Disconnect`/`LogoutRemote` empty (`pkg/connector/client.go:67,171,179`).
5. **Login client-swap race** — observers/channel started on the `LoadUserLogin`-built client, then `c.client` swapped to the warmed login client after `Connect` (`pkg/connector/login.go:129-132`); two live clients per login.
6. **Catch-up backfill from revision 0** for new portals, no `PAGINATED` loop, no status check, run synchronously in the connect path (`pkg/connector/portal.go:47-66`).
7. **`ConvertEdit` targets the last part** and ignores multipart/text-only rules (`pkg/connector/handlegchat.go:175-187`); no edit dedup → edit echo loops possible.
8. **Echo dedup registered after the send RPC returns** and absent entirely for `CreateTopic` (`pkg/connector/handlematrix.go:91,117`).
9. **`setPortalRevision` on every event** with unconditional error-log line (`pkg/connector/portal.go:31` logs "Failed to update portal revision" even on success) and a DB save per event.
10. **`GetCapabilities` substring heuristic** and thread capability granted to all spaces regardless of flat/threaded (`pkg/connector/client.go:154-159`).
11. **Observer accumulation on reconnect** in `Connect` (`pkg/connector/client.go:44-55`).
12. **Debug-grade logging** — stdlib `log.Printf` in the connector and client-lib hot paths (`pkg/connector/client.go:212-236`, `pkg/connector/handlegchat.go:36`, `pkg/gchatmeow/client.go:201,205` logs every stream event at full `%#v`).
13. **`LoadUserLogin` with nil cookies → nil client** wrapped without error (`pkg/connector/login.go:36-39`).
14. **`GetChatInfo` returns non-nil ChatInfo alongside error** (`pkg/connector/mapping.go:89`) and drops group topic.

## 4. (c) Completeness estimate — connector layer

Scoring the connector surface against the Python-parity checklist in 07 §1 (connector-scope items only; msgconv and client lib audited separately):

| Area | Weight (of connector work) | Assessed |
|---|---|---|
| NetworkConnector plumbing (Init/Start/Name/Config/DBMeta) | 10% | ~50% (config 0%, DB meta half) |
| Login flow + credential lifecycle | 15% | ~55% (flow good; rotation/relogin/logout/IsLoggedIn missing) |
| NetworkAPI core (Connect/Disconnect/ChatInfo/UserInfo/Caps) | 15% | ~45% (info fetch real; lifecycle stubs; caps heuristic; crash in connect path) |
| Matrix→GC handlers (7 interfaces) | 20% | ~80% (broadest, most credible area) |
| GC→Matrix ingestion | 20% | ~55% (core message flow real; SYSTEM_MESSAGE/group_viewed/typing routing/selectors missing or wrong) |
| Chat sync + backfill (initial + catch-up) | 15% | ~20% (no initial backfill at all; catch-up partial; sync crashes) |
| IDs / metadata / bridge states | 5% | ~30% (format unusable long-term) |

**Weighted estimate: ~50% of the connector layer's parity surface is written, but only ~35-40% is *usable as-is*** once the crash in `onConnect`, the unstable/unshareable portal-ID format, and the absent cookie persistence are accounted for — those three must be reworked (not merely finished) before anything built on top is trustworthy. The strongest asset is `handlematrix.go` + `login.go` (genuinely close to done); the weakest are sync/backfill and everything lifecycle-related.

**Fork-vs-greenfield signal from this audit**: the connector is the *thinnest* layer of the megabridge branch (1,115 lines) and its two most load-bearing decisions (portal ID format, receiver scoping) are ones we would have to reverse. The value of the branch lies in `pkg/gchatmeow` and `pkg/msgconv`, not here — rewriting `pkg/connector` from scratch against a current mautrix-go (not v0.23.3) while lifting `handlematrix.go`/`login.go` patterns would cost little relative to untangling it.
