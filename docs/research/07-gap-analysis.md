# 07 — Gap Analysis: Python mautrix-googlechat → Go bridgev2

Synthesis of research reports 01–06 in this directory. This document maps every feature of
the Python bridge onto the bridgev2 mechanism that implements it, quantifies the remaining
custom Go work, proposes a milestone plan, and ranks the risks.

Report index referenced throughout:

| Report | Topic |
|---|---|
| 01 | `maugclib` client library (auth, channel, API surface) |
| 02 | Wire protocol / `googlechat.proto` inventory |
| 03 | Python bridge feature surface (`mautrix_googlechat/`) |
| 04 | bridgev2 framework catalog (mautrix-go) |
| 05 | mautrix-meta blueprint (repo layout, patterns) |
| 06 | Ecosystem recon (megabridge branch, 2026 breakage, prior art) |

**Headline finding (report 06):** an unfinished Go bridgev2 rewrite already exists on the
protected `megabridge` branch of `mautrix/googlechat` (started Dec 2024, last commit
2025-04-30, "untested and hasn't been updated in a year"). It contains `pkg/gchatmeow`
(a direct maugclib port incl. channel + pblite), the compiled proto, a connector skeleton,
and `pkg/msgconv` with tests. Whether to finish/fork that branch or green-field is the
single biggest open decision (see §6) — everything below is written so it applies to both
paths (the megabridge branch follows the same architecture this document derives).

---

## 1. Feature-by-feature mapping table

Effort legend (custom Go code only, not framework work): **S** ≤ 1 day, **M** = days,
**L** = a week or more. Interface/struct names are exact, from report 04.

### 1.1 Matrix → Google Chat

| Python bridge feature | bridgev2 mechanism | Custom Go code still needed | Effort |
|---|---|---|---|
| Plain text / notices (`portal.py:1051`) | `NetworkAPI.HandleMatrixMessage(ctx, *MatrixMessage) (*MatrixMessageResponse, error)`; framework pre-resolves `ThreadRoot`/`ReplyTo` to `*database.Message` | msgconv Matrix→GC; route `create_topic` (new topic, `history_v2=true`) vs `create_message` (thread reply); build `RequestHeader`; return `MatrixMessageResponse{DB: &database.Message{ID, SenderID, Timestamp}}` | M |
| Echo dedup (`local_id` round-trip) | `MatrixMessage.AddPendingToIgnore/AddPendingToSave/RemovePending` + `RemoteMessageWithTransactionID`; optionally `TransactionIDGeneratingNetwork.GenerateTransactionID` | Generate `mautrix-googlechat%<rand uint64>` local_id, pass as txn ID, match it in the `MESSAGE_POSTED` echo | S |
| HTML formatting → annotations (`formatter/from_matrix/`) | Framework delivers parsed content; no formatting help beyond `format.HTMLParser` + `PillConverter` (report 05 §3.4) | Full `matrixfmt` package: HTML → `Annotation` protos (BOLD/ITALIC/…/FONT_COLOR/lists/links/mentions/`@room`→MENTION_ALL), UTF-16 offset computation via mention placeholder-locator trick, `chip_render_type=DO_NOT_RENDER`, set `MessageInfo.accept_format_annotations=true` | L |
| Media upload (`portal.py:1081`) | `HandleMatrixMessage` media branch; `portal.Bridge.Bot.DownloadMedia` handles encrypted Matrix files; `RoomFeatures.File` mime maps pre-validate | Resumable-upload client (`x-goog-upload-*` → PUT → base64 `UploadMetadata`), attach as `Annotation{type: UPLOAD_METADATA, chip_render_type: RENDER}`. **Endpoint returns HTTP 500 since ~Feb 2026 (issue #114) — needs live re-verification/fix first** | M (+risk) |
| Edits (text only) (`portal.py:840`) | `EditHandlingNetworkAPI.HandleMatrixEdit(ctx, *MatrixEdit)`; `msg.EditTarget *database.Message` pre-resolved | `edit_message` RPC with full `MessageId`; store `last_edit_time` in `MessageMetadata` for dedup | S |
| Redactions (`portal.py:800`) | `RedactionHandlingNetworkAPI.HandleMatrixMessageRemove` | `delete_message` RPC (topic-id fallback logic) | S |
| Reactions (`portal.py:763`) | `ReactionHandlingNetworkAPI` — `PreHandleMatrixReaction` (return `MatrixReactionPreResponse{SenderID, EmojiID, Emoji}` with `variationselector.Remove`), `HandleMatrixReaction`, `HandleMatrixReactionRemove` | `update_reaction` RPC ADD/REMOVE; emoji = `EmojiID` (per-emoji reactions, not one-per-user) | S |
| Replies (`portal.py:886`) | `MatrixMessage.ReplyTo *database.Message` pre-resolved by framework | Build `SendReplyTarget{id, create_time}` — **requires the target's original µs timestamp**, so `MessageMetadata` must store µs | S |
| Threads (`portal.py:891-907`) | First-class: `RoomFeatures.Thread`, `MatrixMessage.ThreadRoot`; reply→thread auto-conversion when network is threads-only (`portal.go:1255-1268`) | Per-portal `threads_only`/`threads_enabled` flags in `PortalMetadata`; per-portal `GetCapabilities` switching `Thread`/`Reply` levels; route to `create_message` with `parent_id.topic_id` | M |
| Typing (`portal.py:1133`) | `TypingHandlingNetworkAPI.HandleMatrixTyping(ctx, *MatrixTyping)` | `set_typing_state` RPC with `TypingContext` group/topic oneof | S |
| Read receipts (`user.py:684`) | `ReadReceiptHandlingNetworkAPI.HandleMatrixReadReceipt` (must tolerate `ExactMessage == nil`); `ImplicitReadReceipts` in `NetworkGeneralCapabilities` | `mark_group_readstate` RPC; ms→µs (`*1000`) | S |
| Presence | **No bridgev2 presence support** (FEATURES.md unchecked); Python's was dead code too | None — document as unsupported | — |
| Emotes / stickers / location / captions | Python: unsupported. bridgev2 pre-validates against `RoomFeatures` and returns proper rejection MSS | Set `CapLevelRejected/Dropped` in capabilities; optionally *improve on Python* by mapping captions via `MergeCaption` (GC has no captions — would need text+file as 2 messages) | S |
| Membership / room-meta Matrix→GC | `MembershipHandlingNetworkAPI`, `RoomName/Topic/AvatarHandlingNetworkAPI` exist | Python didn't support; defer. GC proto has `UpdateGroupRequest`, `CreateMembershipRequest` etc. but endpoints unverified (report 02 §4) | — (later M) |
| DM creation from Matrix | `IdentifierResolvingNetworkAPI` + `GhostDMCreatingNetworkAPI` | Python didn't support ("DMs need to be explicitly created on Google Chat"); `CreateDmRequest` exists in proto, endpoint unverified — optional stretch goal | — (later M/L) |

### 1.2 Google Chat → Matrix

| Python bridge feature | bridgev2 mechanism | Custom Go code still needed | Effort |
|---|---|---|---|
| New message (`portal.py:1337`) | `RemoteMessage` (`GetID`, `ConvertMessage`) — use `simplevent.Message[T]` or a custom `GCMessageEvent`; `RemoteEventThatMayCreatePortal` + `ShouldCreatePortal()` for auto room creation; per-portal serialized event loop is framework-provided | Channel event pipeline: `split_event_bodies` flattening (`body` field 4 + repeated `bodies` field 8 → one Event per body), dispatch type-switch on the 6 handled body types, `UserLogin.QueueRemoteEvent` | M |
| Multi-part messages (text + N attachments) | `database.Message` keyed `(ID, PartID)`; `ConvertedMessage.Parts []*ConvertedMessagePart` with deterministic `PartID`s | Part ID convention (e.g. `""` for text, `att_<n>` for attachments); edits target part 0 only | S |
| Annotations → HTML (`from_googlechat.py`) | Nothing — pure conversion | Full `gchatfmt` package: UTF-16 code-unit indexing, overlapping-span normalization (sort + split), recursive interval rendering, FONT_COLOR int transform `(rgb+2^31)&0xFFFFFF`, mention pills, `@room`, skip non-`DO_NOT_RENDER` chips. **Do not replicate the Python `if annotations:` always-truthy bug** | L |
| Attachment download | `intent.UploadMediaStream` (streaming reupload, ffmpeg conversion, `bridgev2.ErrMedia*` sentinels) | `get_attachment_url` builder (FIFE_URL for images w/ `sz=w10000-h10000`, DOWNLOAD_URL otherwise), manual redirect following (≤10 hops; cookies only on `*.google.com` hosts), size caps | M |
| Drive / Meet / YouTube annotations | `ConvertedMessagePart.Extra` for Beeper `com.beeper.linkpreviews` | Append URLs to text body; optional Beeper preview generation (thumbnail reupload, oEmbed) | S core / M previews |
| Edits (`portal.py:1228`) | `RemoteEdit.ConvertEdit(ctx, portal, intent, existing) (*ConvertedEdit, error)` (MESSAGE_UPDATED distinguished only by `Event.type`) | Edit dedup by `last_edit_time \|\| last_update_time` vs stored metadata; only modify part 0 | S |
| Deletions | `simplevent.MessageRemove` — framework redacts all parts | Wire `MessageDeletedEvent` → event | S |
| Reactions add/remove | `simplevent.Reaction` (doubles as remove); uniqueness `(message, part, sender, emojiID)` matches Python's `(emoji, gc_sender, gc_msgid, …)` | Wire `MessageReactionEvent`; `variationselector.Add` toward Matrix | S |
| Threads → Matrix | `ConvertedMessage.ThreadRoot *networkid.MessageID`; framework maps via `GetFirstThreadMessage`/`GetLastThreadMessage`; `database.Message.ThreadRoot` column | Set ThreadRoot = topic id when message_id ≠ topic_id (and always in `threads_only` rooms) | S |
| Replies → Matrix | `ConvertedMessage.ReplyTo *networkid.MessageOptionalPartID` | Read `Message.reply_to.id.message_id` | S |
| Typing | `simplevent.Typing` (framework runs the remote-typing loop + timeout) | Route via `body.typing_state_changed.context.group_id` (**`Event.group_id` is empty for typing events**) | S |
| Read receipts | `simplevent.Receipt{ReadUpTo}`; framework's `GetLastMessagePartAtOrBeforeTime` replaces Python's `get_closest_before` | Map `read_time_micros` → `ReadUpTo time.Time`; `group_viewed` → own-user receipt with `IsFromMe` | S |
| Membership changes (SYSTEM_MESSAGE + `MEMBERSHIP_CHANGED` annotation) | `simplevent.ChatInfoChange` with `ChatInfoChange.MemberChanges *ChatMemberList` (deltas) | Parse system messages, map 9 `MembershipChangedMetadata` types → `ChatMember{Membership, PrevMembership}` | M |
| Room name/topic change (SYSTEM_MESSAGE + `ROOM_UPDATED`) | `simplevent.ChatInfoChange` with partial `ChatInfo{Name/Topic}` | Parse `rename_metadata` / `group_details_metadata` | S |
| Room avatar | n/a — GC groups have no avatars | none | — |
| Ghost info (name/avatar/email) | `NetworkAPI.GetUserInfo` → `UserInfo{Name, Avatar, Identifiers: ["mailto:…"], IsBot}`; framework hashes/dedups avatars, publishes Beeper contact info | `get_members` batch fetch + name fallback chain (name → first+last → email); displayname template in config | S |
| Chat info / participants | `NetworkAPI.GetChatInfo` → `ChatInfo{Name, Topic, Members: &ChatMemberList{OtherUserID, MemberMap}, Type, CanBackfill}` | Wrap both shapes (`WorldItemLite` and `GetGroupResponse`); DM member lists from `read_state.joined_users`/`dm_members`; skip blocked/hidden/non-joined | M |
| Chat-list sync on connect (`user.py:610`) | `simplevent.ChatResync` per chat (`WithCreatePortal(true)` for newest N) + `CheckNeedsBackfillFunc` | `paginated_world` call, sort by `sort_timestamp`, initial_chat_sync cap, revision comparison | M |
| Initial backfill (`portal.py:406`) | `BackfillingNetworkAPI.FetchMessages(FetchMessagesParams)` → `FetchMessagesResponse{Messages, HasMore, Cursor}`; `BackfillMessage.ShouldBackfillThread` + `FetchMessagesParams.ThreadRoot` for per-thread pagination; GC's synchronous `list_topics`/`list_messages` needs **no** Meta-style collector | Topic/message pagination, per-room-type strategy (threaded vs flat), cursor encoding | L |
| Catch-up backfill (revision gap replay) | `RemoteChatResync` + `RemoteChatResyncBackfill.CheckNeedsBackfill`; replayed events go through the normal remote-event queue | `catch_up_group` loop (PAGINATED/COMPLETED), `split_event_bodies` on results, revision watermarks in `PortalMetadata` (group) + `UserLoginMetadata` (user) | M |
| Own read marker (`group_viewed`) | `simplevent.Receipt` with `EventSender{IsFromMe: true}` | trivial wiring | S |

### 1.3 Infrastructure / misc

| Python bridge feature | bridgev2 mechanism | Custom Go code still needed | Effort |
|---|---|---|---|
| Cookie login (`login-cookie` cmd + provisioning) | `LoginProcessCookies`: `Start` returns `LoginStep{Type: LoginStepTypeCookies, CookiesParams: &LoginCookiesParams{URL, UserAgent, Fields, WaitForURLPattern}}`; `SubmitCookies`; framework drives commands + provisioning REST | 5-cookie field list (COMPASS/SSID/SID/OSID/HSID, domain `chat.google.com`); validate via `refresh_tokens` (`/mole/world` WIZ scrape); `user.NewLogin` with cookies in `UserLoginMetadata`; reuse warmed client, connect in goroutine | M |
| Session validity / XSRF | none | `/u/0/mole/world` scrape: `qwAQke=="AccountsSignInUi"` ⇒ NotLoggedIn; `SMqcke` ⇒ xsrf token, 24 h refresh, sent as `x-framework-xsrf-token` | S (in client lib) |
| Cookie rotation persistence (`user.py:279`) | `UserLoginMetadata` JSON via `GetDBMetaTypes()`; `login.Save(ctx)` | Read rotated jar values after connect, persist the 5 named cookies | S |
| Connection lifecycle (1.5 h recycle, backoff, SID errors) | `NetworkAPI.Connect` (must not return errors) / `Disconnect`; `login.BridgeState.Send(status.BridgeState{...})`; automatic reconnect on `UNKNOWN_ERROR` (`unknownErrorReconnect`) | Whole channel supervision loop: `ChannelLifetimeExpired` silent recycle, `SIDExpiringError` re-register, `SIDInvalidError` → limit-3 resync, 401/`invalid_grant` → `StateBadCredentials`, exponential backoff, hourly periodic sync | L |
| Bridge state / notices / MSS | Framework: `BridgeStateQueue`, `WrapErrorInStatus`, error catalog (`ErrEditsNotSupported`, …), send checkpoints | Define stable `status.BridgeStateErrorCode` constants + `BridgeStateHumanErrors` in `init()` | S |
| Portal identity `(gcid, gc_receiver)` | `networkid.PortalKey{ID, Receiver}` — Receiver = login ID for `dm:` portals, `""` for `space:` — exact same model | ID codec package (`pkg/gcid`): `MakePortalKey`, `MakeUserID`, `MakeMessageID`, `MakeUserLoginID` + parsers; **formats are permanent DB contents — pin in M0** | S (but decide early) |
| DB schema (portal/message/reaction/user tables) | Framework tables + `GetDBMetaTypes()` `database.MetaTypes{Portal, Ghost, Message, UserLogin}` metadata structs | `PortalMetadata{Revision, ThreadsOnly, ThreadsEnabled, OtherUserID?}`, `UserLoginMetadata{Cookies, UserAgent, Revision, BackfillCompleted}`, `MessageMetadata{TimestampMicro? (or reuse Timestamp), LastEditTime}`, `GhostMetadata{Email?}` | S |
| Legacy Python DB takeover | `BridgeMain.CheckLegacyDB` + `LegacyMigrateSimple(renameTables, copyDataSQL, newVersion)` + `PostMigrate`/`PostMigratePortal` (mxmain/legacymigrate.go) | Hand-written SQL copying Python schema → bridgev2 schema; only works if ID formats match Python's `gcid` strings | M/L |
| Commands (`login-cookie`, `logout`, `ping`, `set-notice-room`) | Framework built-ins: login/logout/list-logins/relogin, help, cancel, version, set-relay, delete-portal… | Nothing (Python's 4 commands are all subsumed); optional extra commands via `commands.Processor.AddHandlers` | — |
| Provisioning API | Framework `/_matrix/provision/v3/` (whoami, login flows, logout, contacts, resolve, backfill…) | Nothing; old v1 API is NOT preserved — Beeper/consumers must move to v3 | — |
| Double puppeting | Framework (`double_puppet` config, auto-login) | Nothing | — |
| E2BE | Framework (matrix connector crypto) | Nothing (decide libolm CGO vs goolm build tag) | — |
| Relay mode | Framework (`bridge.relay` config, `OrigSender`, format templates) — Python had none | Nothing — free feature *gain* | — |
| Spaces (personal filtering space) | Framework (`personal_filtering_spaces`) | Nothing | — |
| Disappearing messages | Framework `DisappearLoop` — GC has retention but Python ignored it | Not needed for parity | — |
| Prometheus metrics | Not part of bridgev2 in the same form | Accept loss or add later; not parity-critical | — |
| Config | `GetConfig()` → embedded `example-config.yaml` + `configupgrade.SimpleUpgrader`; mxmain merges under `network:` | Network section: displayname_template, initial_chat_sync, backfill limits, proxy | S |

---

## 2. What bridgev2 gives for free vs. what Python hand-rolled

### Free from the framework (Python hand-rolled all of this)

- **Portal/ghost lifecycle**: caches, lazy DB loads, Matrix room creation with full initial
  state (`createMatrixRoomInLoop`), participant sync, name/topic diffing, avatar hash
  dedup, portal cleanup. (Python: ~1700 lines of `portal.py` + `puppet.py`.)
- **Per-portal serialized event queue** (64-slot channel + `eventLoop` goroutine) — Python
  built the same thing manually with `asyncio.Queue` per portal.
- **Echo dedup & duplicate suppression** both directions (pending-transaction matching +
  DB message-ID checks) — Python's `_dedup` deque / `_local_dedup` / DB checks.
- **Reply/thread relation resolution**: Matrix events arrive with `ReplyTo`/`ThreadRoot`
  already resolved to DB rows; remote→Matrix `m.thread` bookkeeping
  (`GetFirstThreadMessage`/`GetLastThreadMessage`), reply→thread auto-conversion for
  threads-only rooms — Python implemented all of the rerouting in `portal.py:886-907`.
- **Multi-part message storage** (`(ID, PartID)` keys) — Python's `index` column scheme.
- **Login orchestration**: the whole command + provisioning REST state machine around
  `LoginProcess`; message auto-redaction concerns disappear (cookies go through structured
  steps, not chat messages).
- **Bridge state plumbing**: dedup, retries, TRANSIENT_DISCONNECT notices, auto-reconnect
  on UNKNOWN_ERROR, MSS/checkpoints, standard error → notice conversion.
- **Capabilities publishing & outgoing pre-validation** (`RoomFeatures` as room state;
  rejects unsupported msgtypes before the connector sees them) — Python threw
  `NotImplementedError` by hand.
- **E2BE, double puppeting, relay mode, personal-space hierarchy, disappearing-message
  loop, backfill queue persistence, command framework, config upgrades, registration
  generation, DB init/migrations** — all framework.
- **Legacy-DB migration harness** (`CheckLegacyDB`/`LegacyMigrateSimple`) — purpose-built
  for taking over Python deployments in place.

### Still ours to build (the actual port surface)

1. **The client library** (`maugclib` → Go): cookie session + rotation, XSRF bootstrap,
   BrowserChannel long-poll (register/SID/AID/ack/ping, UTF-16 chunk framing), pblite
   codec, 16 RPC wrappers, resumable upload, redirect-following download, error taxonomy.
   Roughly half the total effort (report 03 §8 concurs).
2. **Formatting both directions** (annotations ↔ HTML with UTF-16 offsets and span
   normalization) — nothing in the framework knows about GC annotations.
3. **Event translation layer**: channel events → `RemoteEvent`s (incl. `split_event_bodies`,
   SYSTEM_MESSAGE annotation parsing) and Matrix handlers → RPCs.
4. **Threading/topic routing policy** per portal (threads_only/threads_enabled).
5. **Backfill logic** (list_topics/list_messages pagination + catch_up_group revision replay).
6. **Connection supervision** policy (channel recycling, SID error ladder, periodic sync).

---

## 3. Client-library port plan (`maugclib` → Go)

Target layout mirrors mautrix-meta (report 05 §1) and matches what the megabridge branch
already started (`pkg/gchatmeow`, report 06 §2a):

```
cmd/mautrix-googlechat/main.go     ~30 lines, mxmain.BridgeMain
pkg/gchatmeow/                     pure client lib — ZERO bridgev2 imports,
│                                  single EventHandler func(ctx, any) callback
│   client.go                      ← maugclib/client.py: Client, connect/disconnect,
│                                    16 proto_* RPC wrappers, xsrf refresh, split_event_bodies
│   session.go / cookies.go        ← maugclib/http_utils.py: cookie jar (unquoted values,
│                                    rotation readback), UA rewriting, *.google.com guard,
│                                    retries, proxy; RequiredCookies list for login UI
│   channel.go                     ← maugclib/channel.py: BrowserChannel register/long-poll,
│                                    SID/AID/ofs state, ack + initial PingEvent, error mapping
│   chunkparser.go                 ← ChunkParser: UTF-16 code-unit length framing
│   upload.go / download.go        ← resumable upload; manual-redirect attachment download
│   errors.go                      ← exceptions.py taxonomy as sentinel errors
│   ids.go                         ← parsers.py: GroupId ↔ "dm:"/"space:", µs time helpers
│   pblite/pblite.go               ← pblite.py: protoreflect-based codec
│   proto/googlechat.proto|.pb.go  ← compiled schema
pkg/connector/                     bridgev2 adapter (connector.go, client.go, login.go,
│                                  handlematrix.go, handlegchat.go, backfill.go,
│                                  chatinfo.go, userinfo.go, capabilities.go, config.go,
│                                  dbmeta.go, events.go)
pkg/msgconv/                       conversion only (from-gchat.go, from-matrix.go,
│                                  gchatfmt/ + matrixfmt/ with table-driven tests)
pkg/gcid/                          networkid codecs + DB metadata structs
```

### 3.1 Module-by-module mapping

| maugclib module (lines) | Go package/file | Porting notes |
|---|---|---|
| `client.py` (814) | `gchatmeow/client.go` | RPC surface is mechanical (`POST /u/0/api/{ep}?c=N&rt=b&alt=proto&key=…`, binary proto both ways, `x-framework-xsrf-token`, hardcoded API key + `client_version=2440378181258`). Keep `split_event_bodies` here. |
| `channel.py` (495) | `gchatmeow/channel.go` + `chunkparser.go` | Highest-fidelity port required. First GET: `SID=null, CVER=22, $req=count=1&ofs=0&req0_data=%5B%5D`; SID from `X-HTTP-Initial-Response` header; then ack GET + initial `PingEvent(ACTIVE/FOREGROUND/INTERACTIVE)` or events never flow. Error ladder: payload error→`SIDExpiring` (re-register, no backoff); HTTP 400 "Unknown SID"→`SIDInvalid` (propagate); clean ~1 h EOF→re-poll, retries reset; 60 s read timeout→NetworkError w/ backoff. |
| `http_utils.py` (273) | `gchatmeow/session.go` | Go's `net/http` cookie jar must emit unquoted values and let us read rotated values back; force `Connection: Keep-Alive`; host allowlist `*.google.com`; do **not** replicate `ssl=False`. |
| `pblite.py` (176) | `gchatmeow/pblite/` | See §3.2. |
| `parsers.py` (49) | `gchatmeow/ids.go` | Trivial. |
| `event.py` (60) | dropped | Replaced by a single `EventHandler` callback (meta pattern); bridgev2's queue supersedes the observer system. |
| `exceptions.py` (85) | `gchatmeow/errors.go` | Sentinel errors + `errors.Is`; keep `UnexpectedStatusError{Status, ErrorCode}` shape for the 401/`invalid_grant` logout path. |
| `googlechat.proto` (~2550) | `gchatmeow/proto/` | See §3.2. |

### 3.2 protoc-gen-go + pblite handling

- **Keep proto2.** The bridge depends on field presence everywhere (`HasField("threaded_group")`,
  `url_metadata` oneof dispatch, `flat_threads_enabled`); proto2 `optional` → pointer
  fields preserves this. Do NOT convert to proto3.
- Copy `googlechat.proto` verbatim, add only `option go_package = ".../pkg/gchatmeow/proto"`,
  compile with `protoc-gen-go` (`google.golang.org/protobuf v1.36.x` — same dep meta pins).
  Keep commented-out fields: they document real field numbers. Don't "fix" odd single-field
  wrappers (`Url.url` is field **3**, `Html.html` field **2**).
- **Two encodings coexist**: all `/u/0/api/*` RPCs are plain binary protobuf both ways;
  only the webchannel uses pblite (inbound `StreamEventsResponse`, outbound
  `StreamEventsRequest` in the `req0_data` form field). The `/uploads` final response is
  base64 of binary `UploadMetadata`.
- **pblite codec** (protoreflect walk: array index i ↔ field number i+1):
  - int64 arrives as JSON **strings** (every µs timestamp) — accept string and number;
  - a trailing JSON **object** is a sparse `{fieldNumber: value}` map for high field
    numbers — `Message.reply_to` is field 37, so this path is hit in practice;
  - `bytes` = base64; nulls skipped; unknown fields skipped silently; decoding must be
    permissive (warn + skip, never fail);
  - oneofs need no special handling (last-set-wins).
  - **Prior art**: mautrix-gmessages `libgm/pblite` (protoreflect-based) and the megabridge
    `gchatmeow` port both exist — verify trailing-dict support before adopting either.

### 3.3 Biggest porting risks in the client lib

1. **UTF-16 chunk framing** (`<length>\n<payload>` where length counts UTF-16 code units,
   i.e. JS `String.length`): the Go parser must incrementally UTF-8-decode (tolerating a
   split multi-byte rune at the buffer boundary) and count UTF-16 units. Off-by-one here
   corrupts the stream silently.
2. **SID/ack/ping choreography**: forgetting the post-SID ack GET or the initial PingEvent
   yields a channel that connects but never delivers events — hard to debug.
3. **Error→reconnect mapping fidelity**: Python distinguishes `ClientPayloadError` /
   HTTP 400 "Unknown SID" / clean EOF / read timeout; Go's `net/http` surfaces these
   differently (e.g. `io.ErrUnexpectedEOF`, `context.DeadlineExceeded`) — needs a careful
   translation table plus live testing.
4. **Cookie jar semantics**: unquoted values, rotation readback, per-request re-persistence;
   Go's `http.CookieJar` interface makes reading values back awkward — likely a custom jar.
5. **2026 protocol drift**: media upload 500s (issue #114) and Matrix→GC formatting
   stripped (#110) mean the Python code is no longer a fully working spec for those two
   paths. Diff against actively-maintained purple-googlechat (daily builds through
   2026-06) to find what changed; the hardcoded `client_version`/`hs` blob may need bumping.
6. **Auth-expiry detection surface**: `Session.fetch` collapses non-200 API responses into
   a status-less NetworkError, so 401 detection effectively only happens on the
   webchannel/register and `/mole/world` paths — the Go port should improve this (keep the
   status code) but must at minimum not regress it.

---

## 4. Milestone breakdown (dependency-ordered)

Each milestone is shippable/verifiable on its own. "Exit" = demoable criteria.

### M0 — Skeleton & decisions (foundation)
- **Entry:** decision on megabridge fork vs green-field (§6 Q1); repo/module name.
- Work: repo layout (§3), `main.go` + `mxmain.BridgeMain`, `go.mod` (mautrix-go @ current
  main, go.mau.fi/util, protobuf), compiled proto, empty-but-compiling
  `NetworkConnector`/`NetworkAPI` stubs, `GetDBMetaTypes` metadata structs,
  **pin all `networkid` formats** (PortalKey `dm:`/`space:` + receiver rule, UserID,
  MessageID, UserLoginID) — matching Python's formats to keep legacy migration possible,
  embedded example-config + upgrader, build.sh/Dockerfile/pre-commit.
- **Exit:** `-e` generates a merged example config; `-g` generates registration; bridge
  starts, registers with the homeserver, bot responds to `help`. Unit tests for `pkg/gcid`.

### M1 — Client lib core: auth + channel + sync (the long pole)
- **Entry:** M0; a test Google account with extracted cookies.
- Work: `gchatmeow` session/cookies/xsrf (`/mole/world` scrape), pblite codec (+tests on
  captured frames), channel (register, SID, chunk parser, ack+ping, error ladder), RPC
  plumbing + `paginated_world`/`get_self_user_status`/`get_group`/`get_members`;
  connector `LoadUserLogin`/`Connect`/`Disconnect`/`IsLoggedIn`; cookie login flow
  (`LoginProcessCookies` → `SubmitCookies` → validate → `NewLogin`); bridge-state error
  codes; cookie-rotation persistence; chat-list sync emitting `simplevent.ChatResync`
  (portals created with names/topics/members, ghosts with names/avatars/emails).
- **Exit:** `login` command with pasted cookies succeeds; bridge state reaches CONNECTED;
  portals for the newest N chats appear on Matrix with correct membership; channel
  survives a forced disconnect (backoff + re-register observed); logout works.
  **Protocol-validation spike completes here**: confirm against live Google that channel,
  world sync, and send/receive still behave as the Python code describes (de-risks R1).

### M2 — Text messaging both directions
- **Entry:** M1 (live channel delivering events).
- Work: `MESSAGE_POSTED` → `RemoteMessage` → plain-text `ConvertedMessage` (multi-part
  scaffolding in place, text = part 0); `HandleMatrixMessage` → `create_topic`/
  `create_message`; local_id echo dedup via pending-transaction API; µs timestamps stored;
  message DB rows correct in both directions; DM vs space receiver scoping verified with
  2 logins.
- **Exit:** Bidirectional plain text in a DM and a space; no duplicate own-echoes; messages
  from a second bridge user in a shared space attribute correctly.

### M3 — Formatting, threads, replies
- **Entry:** M2.
- Work: `gchatfmt` (annotations→HTML) + `matrixfmt` (HTML→annotations) with UTF-16
  offsets, span normalization, mentions/@room, font color transforms —
  **table-driven tests from day one** (emoji/astral chars, overlapping spans, nested lists);
  `RoomFeatures` per portal (Thread/Reply levels from `threads_only`/`threads_enabled`);
  thread routing Matrix↔GC incl. reply-rerouting and quote-replies (`SendReplyTarget`
  with stored µs create_time).
- **Exit:** Round-trip formatting tests green; threads in a threaded space map to Matrix
  threads and back; quote-replies work both ways. (Live-verify #110: if Google now strips
  annotations, investigate `accept_format_annotations` / request shape here.)

### M4 — Mutations & ephemerals: edits, deletes, reactions, receipts, typing
- **Entry:** M2 (M3 parallel-izable).
- Work: `EditHandlingNetworkAPI` + `RemoteEdit` (edit dedup via last_edit_time);
  `RedactionHandlingNetworkAPI` + `simplevent.MessageRemove`;
  `ReactionHandlingNetworkAPI` + `simplevent.Reaction` (variation selectors);
  `ReadReceiptHandlingNetworkAPI` + `simplevent.Receipt` (+ `group_viewed` own-marker);
  `TypingHandlingNetworkAPI` + `simplevent.Typing` (context.group_id routing);
  SYSTEM_MESSAGE parsing → membership + room name/topic `ChatInfoChange`.
- **Exit:** Edit/delete/react/read/typing round-trip in both room types; renames and
  member joins/leaves propagate GC→Matrix.

### M5 — Media
- **Entry:** M2; **blocked on re-verifying the 2026 upload breakage** (issue #114).
- Work: attachment download (`get_attachment_url`, manual redirects, streaming reupload
  via `UploadMediaStream`); GC→Matrix attachments as extra message parts; Drive/Meet/
  YouTube URL appending (+ optional Beeper link previews); Matrix→GC resumable upload +
  `UPLOAD_METADATA` annotation — with live protocol debugging against current Google
  (compare purple-googlechat's current upload code).
- **Exit:** Images and files bridge GC→Matrix (incl. from an E2BE room); Matrix→GC upload
  works **or** is explicitly stubbed with a clean MSS error and a documented upstream
  blocker.

### M6 — Backfill & gap recovery
- **Entry:** M2 (uses message conversion), M4 (replayed events include edits/reactions).
- Work: `BackfillingNetworkAPI.FetchMessages` over `list_topics`/`list_messages`
  (threaded vs flat strategies, cursors, `ShouldBackfillThread`); revision watermarks in
  `PortalMetadata`/`UserLoginMetadata`; `catch_up_group` replay through the remote-event
  queue on `ChatResync`/`SIDInvalid`/periodic sync; 1.5 h channel recycling + hourly
  periodic sync policy; `ChatInfo.CanBackfill`; config knobs (limits mirroring Python's).
- **Exit:** New portal creation backfills history (forward backfill on any homeserver);
  killing the network for 10 min then reconnecting replays missed messages/edits/
  reactions exactly once; backward backfill queue works on hungryserv (if in scope).

### M7 — Parity polish, migration, release
- **Entry:** M3–M6.
- Work: legacy Python DB migration (`CheckLegacyDB` + copy SQL + `PostMigratePortal`) if
  in scope (§6 Q2); capability version bumps; `ResolveIdentifier`(optional), extra
  commands; docs (cookie-extraction instructions given the broken extension —
  manual DevTools flow); CI (build matrix + lint + tests); Docker; changelog; coordinate
  upstream release channel (#googlechat:maunium.net).
- **Exit:** A Python-bridge deployment migrates in place preserving rooms/messages
  (or migration explicitly out of scope); feature table §1 all ✔ or documented-out;
  tagged release.

Dependency graph: M0 → M1 → M2 → {M3, M4, M5} → M6 → M7 (M3/M4/M5 mutually independent;
M6 needs M2+M4; M5 is the only one gated on an external unknown).

---

## 5. Top 10 technical risks, ranked

| # | Risk | Likelihood / impact | Mitigation |
|---|---|---|---|
| 1 | **Protocol drift since 2024**: media upload 500s (#114, ~Feb 2026), Matrix→GC formatting stripped (#110), DM/space backfill broken (#107). The Python code is no longer a verified spec for these paths. | High / High | Run the M1 live protocol-validation spike before building on top; diff maugclib's proto+endpoints against actively-maintained purple-googlechat (daily 2026 builds); treat `client_version`/`hs` blob as config-updatable, not constants; keep M5 media isolated so its breakage doesn't block release. |
| 2 | **BrowserChannel port fidelity** (UTF-16 framing, SID/ack/ping dance, error ladder): subtle bugs = silent event loss. | High / High | Port `channel.py` line-by-line; unit-test the chunk parser against captured real frames (incl. emoji/astral chars and split UTF-8 buffers); integration-test forced SID expiry; reuse megabridge's `channel.go` as a second reference even if green-fielding. |
| 3 | **Cookie-session fragility** (chronic expiry #98/#101; extension broken → manual extraction only). Users get logged out and blame the bridge. | High / Medium | Persist rotated cookies after every connect; correct `StateBadCredentials` mapping (NotLoggedIn from `/mole/world`, 401/`invalid_grant` from register) so clients prompt re-login instead of silent death; document manual DevTools cookie extraction; implement `LoginProcessWithOverride` for painless relogin; consider keeping status-codes on API errors (improve on Python). |
| 4 | **pblite edge cases**: trailing sparse dict (hit via `Message.reply_to` = field 37), int64-as-string, permissiveness. Decode failures = dropped events. | Medium / High | Adopt/port an existing Go pblite (gmessages libgm or megabridge) and verify trailing-dict support; table-driven tests from captured stream payloads; never return an error from stream decode — log and skip. |
| 5 | **UTF-16 annotation offsets + overlapping-span normalization**: wrong indexing garbles formatted messages and mentions (esp. with emoji). | Medium / High | Dedicated `gchatfmt`/`matrixfmt` packages with exhaustive round-trip test corpus (this is where megabridge already has tests); use the meta placeholder-locator pattern for outgoing mention offsets; compute in UTF-16 code units, never bytes/runes. |
| 6 | **ID format lock-in**: `networkid` values are permanent DB contents; a wrong early choice blocks legacy migration and forces painful re-ID. | Medium / High | Pin formats in M0, copying Python exactly (`dm:<id>`/`space:<id>`, numeric Gaia UserID, message_id as MessageID, receiver = owner UserLoginID for DMs, "" for spaces); code-review the `pkg/gcid` package against the migration copy-SQL before first release. |
| 7 | **Threading model edge cases**: threads_only legacy spaces, `message_id == topic_id` head-message fallback, replies rerouted into threads, `SendReplyTarget` needing original µs timestamps. | Medium / Medium | Store `(message_id, topic_id, µs timestamp)` per message from day one; portal metadata flags set on every chat-info sync; test matrix covering {threaded space, flat room, flat+threads DM} × {new msg, thread reply, quote reply, edit, react}. |
| 8 | **Duplicated/competing effort with upstream megabridge** (same repo, AGPL, Beeper authorship): a green-field port could be wasted or unmergeable work. | Medium / Medium | Decide §6 Q1 first; contact upstream (#googlechat:maunium.net) before writing code; if forking, keep the same module path & architecture to allow upstreaming patches. |
| 9 | **Backfill correctness** (revision watermarks, catch-up replay, dedup on overlap): double-bridged or missing history destroys user trust; backward queue is Beeper-only. | Medium / Medium | Forward backfill first (works everywhere); replay catch-up events through the normal remote-event path (as Python does) so all dedup logic is shared; `AggressiveDeduplication` during overlap windows; store revisions only after successful handling. |
| 10 | **Framework churn**: mautrix-go bridgev2 tracks main (meta pins a post-release commit); megabridge is on v0.23.3 (very stale); interfaces may shift mid-project. | Medium / Low-Medium | Pin to the same commit meta uses; budget a rebase in M7; keep connector surface thin (logic in gchatmeow/msgconv which don't import bridgev2). |

Non-risk worth noting: **ban/enforcement risk appears low** (report 06 §5 — no ban reports
in the public record; observed friction is API evolution, not enforcement).

---

## 6. Open questions for the human

1. **Megabridge: finish/fork or green-field?** The `mautrix/googlechat@megabridge` branch
   has the client-lib port, proto, connector skeleton, and msgconv (with tests) already
   written, but is untested, ~15 months stale, and pinned to mautrix-go v0.23.3. Options:
   (a) rebase + finish it upstream, (b) fork it into this repo as the M0 starting point,
   (c) green-field using it only as reference. Also: do we contact
   `#googlechat:maunium.net` / Tulir before starting (recommended for AGPL hygiene and
   avoiding duplicate work)?
2. **Is in-place migration of existing Python-bridge deployments a requirement?** This
   drives (a) matching Python ID formats exactly, (b) the `LegacyMigrateSimple` copy SQL
   in M7, and (c) preserving room state so users keep their portals. If nobody runs the
   Python bridge that we care about, M7 shrinks substantially.
3. **Module/repo identity**: publish as `go.mau.fi/mautrix-googlechat` (upstream
   convention, implies coordination) or an independent module path?
4. **Is Matrix→GC media upload a launch blocker?** Google's upload endpoint has returned
   HTTP 500 since ~Feb 2026 with no known fix. If we can't fix it by diffing
   purple-googlechat, do we ship with upload disabled (clean MSS error) or hold release?
5. **Beeper-specific features in scope?** Backward backfill queue (needs hungryserv batch
   sending), `com.beeper.linkpreviews` URL previews, contact-info blobs, DirectMedia
   proxy. All optional; each adds testing surface. Recommendation: link previews yes
   (cheap), direct media no (skip initially), backward queue only if Beeper deployment is
   a target.
6. **Enable relay mode?** bridgev2 provides it free; the Python bridge never had it.
   Google Chat ToS exposure is on the relaying user's cookie session. Default off?
7. **New-feature ambitions beyond parity**: media captions (via text+file pair or
   `MergeCaption`), emote support (prefix `*`), DM creation from Matrix
   (`CreateDmRequest` exists in the proto but its endpoint is unverified),
   Matrix→GC membership/room-rename. Parity-first, or cherry-pick any of these?
8. **E2EE build**: keep libolm CGO (meta's current setup) or use the goolm build tag for
   a pure-Go build? Affects Docker images and CI.
9. **Test account & fixtures**: we need at least one (ideally two) Google accounts with
   Google Chat access for the M1 validation spike and ongoing integration tests, plus
   consent to capture/store sanitized wire fixtures (chunk frames, pblite arrays) in the
   repo. Workspace or consumer accounts? (Feature availability differs — e.g. spaces.)
10. **Deprecation/positioning**: if this ships, is the goal to replace the Python bridge
    on matrix.org's ecosystem listing / docs.mau.fi (upstream-blessed), or to run as an
    independent alternative? Affects naming, config compat, and how much provisioning-API
    backward compatibility (the old `/v1/login` shape) matters — bridgev2's provisioning
    v3 is not wire-compatible with the Python bridge's API.
