# Architecture

`mautrix-googlechat` is a Matrix ↔ Google Chat puppeting bridge written in Go on
the [mautrix-go **bridgev2**](https://github.com/mautrix/go) framework. It speaks
Google Chat's private web-client API (the same protocol the `chat.google.com` web
app uses) over cookie authentication, and presents each Google Chat conversation
as a Matrix portal room with per-user ghosts.

This document is the single orientation point for a maintainer: the layering
rules, the Google Chat protocol facts you cannot rediscover from the Go standard
library, how each user-visible feature maps onto bridgev2 mechanisms, and the
handful of decisions that are permanently frozen. Module path:
`github.com/Deniel9204/mautrix-googlechat`.

---

## 1. High-level architecture

The codebase is four packages plus a migration engine, arranged as a strict
stack. The dependency direction is one-way and **enforced in review**:

| Package | Responsibility | Hard rule |
|---|---|---|
| `pkg/gchatmeow` | Google Chat client library: cookie session, XSRF, BrowserChannel long-poll, RPC surface, pblite codec, upload/download, generated proto. | **Never imports bridgev2.** It is a standalone Google Chat client that knows nothing about Matrix. |
| `pkg/connector` | The bridgev2 adapter: `NetworkConnector`/`NetworkAPI`, login, event dispatch, chat/user info, backfill, Matrix→GC handlers. | **Never does HTTP.** All network I/O goes through `gchatmeow`. |
| `pkg/msgconv` | Message conversion only: Google Chat annotations ↔ Matrix HTML, both directions, UTF-16 offset handling. | **Only converts.** No network, no DB, no bridgev2 state mutation. |
| `pkg/gcid` | Network-ID codecs (portal/user/message/part IDs). | **Formats are FROZEN** — they are permanent DB contents (see §6). |
| `pkg/migrate` | One-shot importer from a prior bridge's database. | Standalone; only runs under `--migrate-from-python`. |

`cmd/mautrix-googlechat/main.go` is a ~30-line `mxmain.BridgeMain` bootstrap that
registers `&connector.GChatConnector{}` and wires the `--migrate-from-python`
flag into `PostInit`.

The bridge is built exclusively with the pure-Go `goolm` crypto implementation
(build tag `goolm`, no CGO/libolm); `./build.sh` always passes `-tags goolm`, and
the tag path must never be removed.

### Layer interaction at a glance

```
Matrix HS ──▶ bridgev2 framework ──▶ pkg/connector ──▶ pkg/gchatmeow ──▶ chat.google.com
                     │                      │                 │
                     │                      ▼                 │
                     │                 pkg/msgconv            │  (binary proto / pblite / uploads)
                     │              (annotations ↔ HTML)      │
                     └──────────── pkg/gcid (IDs) ────────────┘
```

`pkg/gchatmeow` exposes a single `*gchatmeow.Client` with an `OnStreamEvent`
callback and an `OnConnectionState` callback; everything Matrix-facing lives in
`*connector.GChatClient`, which owns the supervision loop, translates
`*pb.Event`s into bridgev2 `RemoteEvent`s, and drives Matrix→GC sends.

---

## 2. Google Chat protocol essentials

Everything here is the private web-client protocol. There is no official
third-party API; the client mimics the browser closely enough not to stand out.

### 2.1 Authentication: cookies + XSRF

Login is **pure cookie auth** — no OAuth, no refresh tokens. The user extracts
five cookies from a logged-in `chat.google.com` browser session:

```
COMPASS, SSID, SID, OSID, HSID
```

Stored in `UserLoginMetadata.Cookies` (plus the browser `UserAgent`). Two client
requirements a maintainer must respect:

- **Cookie values are sent verbatim, unquoted.** Google's server rejects quoted
  cookie values. `gchatmeow`'s custom `cookieJar` (`cookies.go`) emits raw
  values.
- **Rotated cookies must be persisted.** Google issues fresh `Set-Cookie` values
  (e.g. `SIDCC`, a webchannel-scoped `COMPASS`) over the life of a session. After
  every successful connect, `GChatClient.persistCookies` reads them back out of
  the jar and writes them to the login metadata. Skipping this kills sessions
  early.

Before any RPC, the client bootstraps an **XSRF token**: `GET
/u/0/mole/world` returns HTML embedding `window.WIZ_global_data`. The token is
`WIZ_global_data["SMqcke"]`, sent on every RPC as the `x-framework-xsrf-token`
header and refreshed roughly every 24 h (`Client.FetchXSRFToken` /
`xsrfRefreshLoop`). If `WIZ_global_data["qwAQke"] == "AccountsSignInUi"`, the
cookies are invalid — this is the primary logged-out signal, mapped to
`BAD_CREDENTIALS`.

Frozen web-client constants (config-updatable, not compile-time constants, so
they can be bumped if Google rotates them): API key
`AIzaSyD7InnYR3VKdb4j2rMUEbTCIr2VyEazl6k`, `RequestHeader.client_version`
`2440378181258`. As of the last live validation (2026-07-22) both still
authenticate.

### 2.2 The realtime channel: BrowserChannel long-poll

Realtime events arrive over Google's **BrowserChannel** protocol (closure-library
`goog.net.BrowserChannel`) at `https://chat.google.com/u/0/webchannel/`,
implemented in `channel.go`. The connect choreography (`Channel` + `Client.Connect`):

1. **register** — `GET /webchannel/register?ignore_compass_cookie=1`. The server
   returns a fresh webchannel-scoped `COMPASS` cookie (value prefixed
   `dynamite-ui=`) that must ride along on subsequent webchannel requests.
2. **acquire SID** — the first long-poll `GET /webchannel/events` returns the
   session id in the `X-HTTP-Initial-Response` header (parsed by
   `parseSIDResponse`).
3. **ack GET** — an immediate follow-up GET with the new SID (empirically
   required for the server to proceed).
4. **initial ping** — a `PingEvent` (`state=ACTIVE`, foreground, interactive) is
   sent on the **forward channel** (`Client.SendStreamEvent`, a
   `POST /webchannel/events` with the request pblite-encoded in the `req0_data`
   form field). Without this ping the server never starts streaming.
5. The server then holds the response open (~1 h) and streams event chunks;
   heartbeats arrive every 15–30 s, and the literal array `["noop"]` is a
   keep-alive.

The **forward channel is only used for pings.** All real actions (send, edit,
react, typing, read state) go over the request/response RPC surface (§2.3).

**Chunk framing (`chunkparser.go`) — the critical quirk.** The streamed body is a
sequence of frames:

```
<length><LF><payload…>
```

where `<length>` is the payload length in **UTF-16 code units** (JavaScript
`String.length`), *not* bytes and *not* runes. `ChunkParser` decodes the UTF-8
buffer tolerating a split multibyte char at the boundary (`decodeUTF8Prefix`),
then counts UTF-16 units (`takeUTF16Units`, `utf16RuneLen`) so astral characters
(most emoji) count as 2. Each frame payload is a container array of
`[array_id, data_array]` pairs; `array_id` becomes the `AID` acknowledged in the
next request.

The channel is deliberately recycled every ~1.5 h (silent reconnect) to avoid a
long-lived-session server bug.

### 2.3 Two wire encodings, one proto schema

| Surface | Encoding |
|---|---|
| `/u/0/api/{endpoint}` RPCs (both directions) | **binary protobuf** (`proto.Marshal`/`Unmarshal`) |
| `/webchannel/events` inbound arrays → `StreamEventsResponse` | **pblite** decode |
| `/webchannel/events` outbound `StreamEventsRequest` (`req0_data`) | **pblite** encode |
| `/uploads` final response → `UploadMetadata` | **base64 → binary protobuf** |

RPCs post to `…/api/{endpoint}?c={counter}&rt=b&alt=proto&key={API_KEY}` with
`Content-Type: application/x-protobuf` and the XSRF header. Every request embeds a
`RequestHeader` built by `newRequestHeader`. The `c` counter and a `zx` random
param exist only to mimic the web client. `unmarshalAPIResponse` parses the raw
binary body (a base64 dual-decode path exists as cheap insurance but is
unexercised in practice).

**pblite** (`pkg/gchatmeow/pblite`) is Google's JSON-array proto representation
used only on the channel: a message is a JSON array where **array index `i` holds
field number `i+1`**, with three properties a maintainer must not "simplify" away:

- **int64 arrives as a JSON string** and must be parsed — all µs timestamps and
  revisions are int64.
- **trailing sparse dict**: if the last array element is a JSON object, it is a
  `{"<field_number>": value}` map for high field numbers (e.g. `Message.reply_to`
  is field 37). The decoder must handle it.
- **permissive**: unknown field numbers and wrong-typed values are logged and
  skipped, never fatal. Google adds fields continuously; live traffic routinely
  carries fields absent from the checked-in schema, and the channel must survive
  them. A pblite/stream decode error must **log-and-skip, never kill the
  channel**.

### 2.4 The proto is proto2 — presence is load-bearing

`pkg/gchatmeow/proto/googlechat.proto` is a hand-maintained, reverse-engineered
**proto2** schema (regenerated via `proto/gen.sh` with buf + protoc-gen-go).

**It must stay proto2.** The dispatch logic depends on explicit field presence
that proto3 would erase:

- oneof dispatch on `Event.EventBody` (which body field is set decides the event
  kind);
- `HasField`-style presence checks that distinguish a threaded space from a flat
  room (`threaded_group` vs `flat_group`), an attachment from an inline format
  span, and a set boolean like `flat_threads_enabled` from an unset one.

proto2 `optional` maps to Go pointer fields, preserving presence. Never convert to
proto3. Commented-out field numbers in the `.proto` are kept as documentation of
real wire fields.

### 2.5 `paginated_world` requires a `world_section`

Chat-list sync calls the `paginated_world` RPC (`Client.PaginatedWorld`,
driven from `sync.go`). **As of 2026, the request must carry at least one
`world_section_requests` entry with a `page_size`**, or the server returns a
~2-byte stub with zero `world_items` — no chats sync, no portals auto-create.
`fetchWorldWithRetry` sends `WorldSectionRequests: [{PageSize: 999}]`
(`worldSectionPageSize = 999`, matching the maintained purple-googlechat client).
This is live-verified (2026-07-22) and is easy to regress if the request builder
is "cleaned up".

### 2.6 Annotations & UTF-16 offsets

Google Chat has **no markup language on the wire**. A message is a plain-text
`text_body` plus a flat list of `Annotation` spans that may overlap and nest
arbitrarily. Each annotation carries `start_index` and `length` **in UTF-16 code
units** — the single most important rule in `pkg/msgconv`. All offset math uses
UTF-16 (`matrixfmt` / `gchatfmt` `utf16Encode`/`utf16Decode`), never bytes or
runes.

An annotation's `chip_render_type` decides its meaning:

- `DO_NOT_RENDER` → an inline formatting/entity span processed as text markup
  (bold, italic, link, mention, list, font color…);
- `RENDER` → a separately-rendered chip (notably an `UPLOAD_METADATA` file
  attachment).

Key metadata payloads: `FormatMetadata` (BOLD/ITALIC/UNDERLINE/STRIKE/MONOSPACE/
MONOSPACE_BLOCK/FONT_COLOR/BULLETED_LIST[_ITEM] → HTML), `UserMentionMetadata`
(`MENTION` → pill, `MENTION_ALL` → `@room`), `UrlMetadata` (links + previews),
`UploadMetadata` (file attachments), plus `Drive`/`Youtube`/`VideoCall` metadata
whose URLs are appended to the body (`gchatfmt/linkappend.go`). Outgoing format
annotations set `chip_render_type=DO_NOT_RENDER`, and send/edit requests set
`MessageInfo.accept_format_annotations=true` — **without that flag the server
strips formatting.**

### 2.7 Threading: Group → Topic → Message

Google Chat's data model is always **Group → Topic → Message**; what differs is
how topics behave per room, captured in `PortalMetadata`:

| Room shape | Wire signal | `PortalMetadata` |
|---|---|---|
| Threaded space ("topics") | `Group.threaded_group` set | `ThreadsOnly=true`, `ThreadsEnabled=true` |
| Flat room / DM | `Group.flat_group` set | both false |
| Flat room with inline threads | `Group.flat_threads_enabled=true` | `ThreadsEnabled=true` |

In flat rooms the head message of a "topic" has `message_id == topic_id`; replies
share the topic id. Sending routes accordingly (`handlematrix.go`):
`Client.CreateTopic` starts a new top-level message/topic;
`Client.CreateMessage` posts a reply into an existing topic
(`parent_id.topic_id`). A **quote-reply** additionally sets
`MessageInfo.reply_to = SendReplyTarget{id, create_time}`, where `create_time` is
the target message's **microsecond** timestamp — which is exactly why every
bridged message stores `MessageMetadata.TimestampMicro` and `.TopicID`.

### 2.8 RPC surface (`pkg/gchatmeow/api.go`)

All are binary-proto request/response wrappers on `*gchatmeow.Client`:

| Method | Endpoint | Used for |
|---|---|---|
| `PaginatedWorld` | `paginated_world` | chat-list sync (needs world_section, §2.5) |
| `GetSelfUserStatus` | `get_self_user_status` | own Gaia id |
| `GetMembers` | `get_members` | user/profile info |
| `GetGroup` | `get_group` | portal info + membership |
| `CreateTopic` / `CreateMessage` | `create_topic` / `create_message` | send (new topic / thread reply) |
| `EditMessage` / `DeleteMessage` | `edit_message` / `delete_message` | edit / redact |
| `UpdateReaction` | `update_reaction` | reactions add/remove |
| `SetTypingState` | `set_typing_state` | typing |
| `MarkGroupReadstate` | `mark_group_readstate` | read receipts (µs) |
| `ListTopics` / `ListMessages` | `list_topics` / `list_messages` | initial backfill |
| `CatchUpGroup` / `CatchUpUser` | `catch_up_group` / `catch_up_user` | missed-event replay |

Mutating responses return `WriteRevision{timestamp}`; these revision timestamps
are the resync watermarks (§4).

---

## 3. Inbound & outbound data flow

### 3.1 Google Chat → Matrix

```
long-poll chunk (channel.go, UTF-16 framing)
  → pblite decode → StreamEventsResponse (client.go onReceiveArray)
  → SplitEventBodies  (one Event may carry N bodies; flatten to one Event each)
  → OnStreamEvent → GChatClient.handleGChatEvent (events.go)
  → dispatch by body type → bridgev2 RemoteEvent
  → UserLogin.QueueRemoteEvent → framework per-portal serialized loop
  → msgconv.ToMatrix (annotations → HTML, ThreadRoot from topic id)
```

Dispatch is **single ordered** (no goroutine fan-out) so per-connection event
order is preserved. `SplitEventBodies` (`gchatmeow/client.go`) flattens
multi-body events and is applied to both live stream events and `catch_up`
results. `events.go` routes each `EventBody` to a queue-* handler:
`MESSAGE_POSTED` → message, `MESSAGE_UPDATED` → edit, `MESSAGE_DELETED` →
redaction, `MESSAGE_REACTED` → reaction, `READ_RECEIPT_CHANGED`/`GROUP_VIEWED` →
receipts, `TYPING_STATE_CHANGED` → typing. **Routing quirk:** typing events carry
the group only inside `body.typing_state_changed.context`, not in
`Event.group_id`. Unknown event/body types are logged and skipped, never fatal.

`SYSTEM_MESSAGE`s (a `Message` with membership/room-update annotations) are
handled separately in `systemmessage.go`: `MembershipChangedMetadata` →
join/leave/kick/invite, `RoomUpdatedMetadata` → room name / topic changes, both
emitted as bridgev2 `ChatInfoChange`s.

### 3.2 Matrix → Google Chat

```
framework pre-validates against per-portal RoomFeatures (GetCapabilities)
  and pre-resolves reply/thread targets
  → GChatClient.HandleMatrixMessage (handlematrix.go)
  → msgconv.FromMatrix (HTML → annotations, UTF-16 offsets, accept_format_annotations=true)
  → route: sendNewTopic (CreateTopic) vs sendThreadedMessage (CreateMessage)
  → echo dedup via local_id round-trip (mautrix-googlechat%<rand uint64>)
```

Echo suppression: outgoing sends carry a `local_id` that reappears on the
`MESSAGE_POSTED` stream echo, letting the connector drop its own messages
(`addPendingToIgnore`/`removePending`). In a threads-only room a reply
auto-converts into a thread; without thread support a Matrix thread degrades to a
GC quote-reply.

---

## 4. Connection lifecycle & error handling

`GChatClient.Connect` builds a `*gchatmeow.Client` from login metadata and runs a
supervision loop (`wireAndStart`) that consumes `OnConnectionState` transitions
via `handleConnState`. Policy:

| Condition | Response |
|---|---|
| ~1.5 h channel age | scheduled silent recycle |
| payload error / SID expiring | immediate re-register, no backoff |
| HTTP 400 "Unknown SID" | resync (bounded) then reconnect |
| read timeout / network error | exponential backoff + `TRANSIENT_DISCONNECT` |
| HTTP 401 / `invalid_grant` / logged-out `/mole/world` | `BAD_CREDENTIALS` → user must re-login |

**Gap recovery.** Stream events carry `user_revision` / `group_revision`
watermarks, persisted to `UserLoginMetadata.Revision` and
`PortalMetadata.Revision` (`advanceUserRevision` / `advancePortalRevision`). On
reconnect, `catchUp` replays missed events via `catch_up_group` **through the
normal remote-event queue**, so replayed edits/reactions/deletes reuse the live
handlers and dedup. A message is considered fully handled only after its
`QueueRemoteEvent` succeeds, so a failed queue does not advance the watermark past
an undelivered event.

---

## 5. Feature set → bridgev2 mapping

| Feature | Direction | Where |
|---|---|---|
| Text send / receive | both | `HandleMatrixMessage` (`handlematrix.go`) / `queueMessagePosted` (`events.go`) → `msgconv` |
| Rich formatting | both | `msgconv/matrixfmt` (HTML→annotations) / `msgconv/gchatfmt` (annotations→HTML); advertised per-portal via `GetCapabilities` `RoomFeatures` |
| Threads & replies | both | `ConvertedMessage.ThreadRoot` (from `MessageMetadata.TopicID`); routing in `handlematrix.go`; `RoomFeatures` gate |
| Edits (text) | both | `HandleMatrixEdit` (`handleedit.go`) / `queueMessageEdit`; dedup via `MessageMetadata.LastEditTime` |
| Deletes / redactions | both | `HandleMatrixMessageRemove` (`handleredact.go`) / `queueMessageDeleted` |
| Reactions | both | `PreHandleMatrixReaction`/`HandleMatrixReaction`/`HandleMatrixReactionRemove` (`handlereaction.go`) / `queueMessageReaction` |
| Typing | both | `HandleMatrixTyping` (`handletyping.go`) / `queueTypingStateChanged` |
| Read receipts | both | `HandleMatrixReadReceipt` (`handlereceipt.go`) / `queueReadReceiptChanged` + `queueGroupViewed` |
| Media | both | `media.go` (download via `get_attachment_url` with manual cookie-aware redirects; upload via resumable protocol → `UPLOAD_METADATA` annotation) |
| Membership / renames / topic | GC→Matrix | `systemmessage.go` → `ChatInfoChange` |
| Chat / user metadata | GC→Matrix | `GetChatInfo` (`chatinfo.go`) / `GetUserInfo` (`userinfo.go`); ghost avatars via `avatar.go` |
| Backfill & catch-up | GC→Matrix | `FetchMessages` (`backfill.go`, `list_topics`/`list_messages`) + revision catch-up (§4) |
| Login | — | `CreateLogin` → `GChatLogin.SubmitCookies` (`login.go`), cookie flow |
| Relay mode | — | framework-provided; config default off |
| Multi-user / shared spaces | — | DM portals scoped per login, spaces global (§6) |

Deliberately unsupported: presence (bridgev2 has no model for it), Matrix→GC
membership/room-metadata operations, and DM creation from Matrix (Google Chat
requires DMs to be created server-side). `GetCapabilities` leaves
`ImplicitReadReceipts` false on purpose (Google Chat auto-marks self-sent
messages read).

---

## 6. Frozen / permanent decisions

These are load-bearing DB contents or wire requirements. Changing them after
deployment breaks existing data or the protocol.

- **`pkg/gcid` ID formats are frozen** (they are also chosen to match a prior
  bridge's schema so migration stays possible):
  - `PortalID` = `dm:<group_id>` or `space:<group_id>` (`MakePortalID`).
  - `PortalKey.Receiver` = the owner `UserLoginID` for DMs, `""` for spaces
    (`MakePortalKey`) — so DM portals are per-user and spaces are one shared
    portal for all bridge users.
  - `UserID` / `UserLoginID` = numeric Gaia id; `MessageID` = Google Chat
    `message_id`.
  - `PartID` = `""` for the text part, `att_<n>` for the n-th attachment
    (`MakeAttachmentPartID`). Note the documented lexical-sort gap for 11+
    attachments on a single message (`att_10` sorts before `att_9`); it is
    intentionally left as-is and flagged in the `gcid` source rather than
    silently changed, because the format is frozen.
- **The proto stays proto2** (§2.4); field presence drives dispatch.
- **Every bridged message stores `(message_id, topic_id, µs timestamp)`**
  (`MessageMetadata`) — quote-reply targets need the original microsecond
  `create_time`.
- **Rotated cookies are re-persisted after every connect** (§2.1).

---

## 7. Known quirks & live-verified facts

- **Outbound media upload works.** Some Google Chat web clients hit an HTTP 500 on
  upload ([upstream #114](https://github.com/mautrix/googlechat/issues/114))
  because they append `alt=`/`key=` params to the signed upload URL and omit the
  XSRF header — a **client request-shape bug**. This bridge sends the correct
  shape (matching the [purple-googlechat](https://github.com/EionRobb/purple-googlechat)
  reference client) and a live upload against Google's endpoint succeeded
  (2026-07-22). `network.disable_outbound_media` turns upload attempts into clean
  per-message errors if a future change breaks them for a given account.
- **`paginated_world` needs a `world_section` request** or returns an empty stub
  (§2.5) — the most likely silent-breakage point in chat-list sync.
- **UTF-16 offsets everywhere** for annotations (§2.6) and **UTF-16 units** in the
  channel chunk framing (§2.2). Byte or rune math is always a bug.
- **`accept_format_annotations=true` is mandatory** on send/edit or formatting is
  stripped server-side (§2.6).
- **The forward channel is ping-only**; real actions use RPCs (§2.2).
- **The realtime channel is the fragile surface.** Auth, XSRF, binary-proto RPCs,
  and world sync validate reliably against live Google Chat; the authenticated
  BrowserChannel long-poll can be black-holed by Google's anti-abuse for
  programmatic access from datacenter IPs with repeated connects. A single
  long-lived channel from a normal deployment IP is the validated pattern —
  repeated harness re-connects only reinforce the block.
- **Google adds proto fields continuously.** The pblite decoder logging
  "skipping unknown field" is normal and expected, not an error.

---

## Migration

The bridge can import a prior Python `mautrix/googlechat` database directly
(portals, ghosts, messages incl. multi-part/attachments, reactions, users, saved
logins, double-puppeting) via the one-shot `--migrate-from-python` flag
(`pkg/migrate`, wired in `main.go`); see [`migration.md`](migration.md).
