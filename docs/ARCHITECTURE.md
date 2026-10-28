# Architecture

A Matrix ↔ Google Chat puppeting bridge on
[mautrix-go bridgev2](https://github.com/mautrix/go). It speaks the private
API of the `chat.google.com` web app with cookie auth, and presents each
Google Chat conversation as a portal room with ghosts.

Read this before changing protocol code. Module path:
`github.com/Deniel9204/mautrix-googlechat`.

## 1. Packages

| Package | Responsibility | Rule |
|---|---|---|
| `pkg/gchatmeow` | Google Chat client: cookies, XSRF, BrowserChannel, RPCs, pblite, upload/download, proto | Never imports bridgev2 |
| `pkg/connector` | bridgev2 adapter: login, events, chat/user info, backfill, Matrix → GC handlers | Never does HTTP |
| `pkg/msgconv` | Annotations ↔ Matrix HTML, UTF-16 offsets | Only converts: no network, DB or bridgev2 state |
| `pkg/gcid` | Portal/user/message/part id codecs | Formats are frozen (§6) |
| `pkg/migrate` | One-shot import of the Python bridge database | Runs only under `--migrate-from-python` |

`cmd/mautrix-googlechat/main.go` is a short `mxmain.BridgeMain` bootstrap.
The bridge is built only with the pure-Go `goolm` crypto (`-tags goolm`);
never remove that path.

```
Matrix HS ──▶ bridgev2 ──▶ pkg/connector ──▶ pkg/gchatmeow ──▶ chat.google.com
                                │
                                ├── pkg/msgconv (annotations ↔ HTML)
                                └── pkg/gcid (ids)
```

`*gchatmeow.Client` exposes `OnStreamEvent` and `OnConnectionState`
callbacks. `*connector.GChatClient` owns the supervision loop, turns
`*pb.Event`s into bridgev2 remote events and sends Matrix → GC.

## 2. Protocol

### 2.1 Cookies and XSRF

Login is pure cookie auth: `COMPASS`, `SSID`, `SID`, `OSID`, `HSID` (plus
the browser User-Agent) in `UserLoginMetadata`.

- Cookie values are sent raw. Google rejects quoted values, hence the custom
  `cookieJar` (`cookies.go`).
- Google rotates cookies during a session (`SIDCC`, a webchannel `COMPASS`).
  `persistCookies` saves them after every connect; skipping this kills
  sessions early.

Before any RPC, `GET /u/0/mole/world` returns HTML with
`window.WIZ_global_data`. `SMqcke` is the XSRF token, sent as
`x-framework-xsrf-token` and refreshed about every 24 h. `qwAQke ==
"AccountsSignInUi"` means the cookies are dead (`BAD_CREDENTIALS`).

The web client's API key `AIzaSyD7InnYR3VKdb4j2rMUEbTCIr2VyEazl6k` and
`RequestHeader.client_version` `2440378181258` are config values, so they can
be bumped if Google rotates them.

### 2.2 Realtime: BrowserChannel

`channel.go`, at `https://chat.google.com/u/0/webchannel/`:

1. `GET register?ignore_compass_cookie=1` returns a webchannel `COMPASS`
   (`dynamite-ui=…`) that must be sent on later webchannel requests.
2. The first `GET events` returns the SID in `X-HTTP-Initial-Response`.
3. An immediate ack GET with the SID; required, reason unknown.
4. A `PingEvent` (active, foreground, interactive) on the forward channel
   (`POST events`, pblite in `req0_data`). Without it nothing is streamed.
5. The server holds the response open about an hour and streams chunks;
   `["noop"]` is a keep-alive.

The forward channel only carries pings; every real action is an RPC (§2.3).
The channel is recycled every ~1.5 h to avoid a long-session server bug.

Chunks are framed `<length>\n<payload>`, where length counts **UTF-16 code
units**, not bytes or runes (`chunkparser.go`). Each payload is a list of
`[array_id, data]` pairs; the last `array_id` is acked as `AID`.

### 2.3 Encodings

| Surface | Encoding |
|---|---|
| `/u/0/api/{endpoint}` RPCs | binary protobuf |
| `webchannel/events`, both directions | pblite |
| `/uploads` final response | base64 of binary protobuf |

RPCs go to `…/api/{endpoint}?c={n}&rt=b&alt=proto&key={API_KEY}` with
`Content-Type: application/x-protobuf`, the XSRF header and a
`RequestHeader`.

pblite (`pkg/gchatmeow/pblite`) is a JSON array where index `i` holds field
`i+1`. Do not simplify away:

- int64 values arrive as JSON strings (all µs timestamps and revisions).
- A trailing JSON object holds high field numbers as `{"<field>": value}`
  (e.g. `Message.reply_to` is 37).
- Unknown fields and wrong types are logged and skipped. Google adds fields
  constantly; a decode error must never kill the channel. "skipping unknown
  field" in the log is normal.

### 2.4 The proto is proto2

`pkg/gchatmeow/proto/googlechat.proto` is a reverse-engineered proto2 schema
(regenerate with `proto/gen.sh`). Field presence drives dispatch: the
`Event.EventBody` oneof, `threaded_group` vs `flat_group`, attachments vs
format spans, set vs unset booleans like `flat_threads_enabled`. Never
convert to proto3. Commented-out fields document real wire fields.

### 2.5 Chat list: `paginated_world`

The request must carry a `world_section_requests` entry with a `page_size`,
or the server returns an empty stub and nothing syncs. We send
`[{PageSize: 999}]`. Pending invites are never in this list, whatever the
filter (§3.3).

### 2.6 Annotations

There is no markup on the wire: a message is a plain `text_body` plus a list
of `Annotation` spans that may overlap and nest. `start_index` and `length`
are **UTF-16 code units**; byte or rune math is always a bug.

- `chip_render_type = DO_NOT_RENDER`: an inline span (format, link,
  mention, list, font color). `RENDER` / `RENDER_IF_POSSIBLE`: a separate
  chip such as an `UPLOAD_METADATA` attachment. Exception: Google sets
  `RENDER_IF_POSSIBLE` on ordinary pasted links.
- `FormatMetadata` → HTML formatting, `UserMentionMetadata` → pills
  (`MENTION_ALL` → `@room`), `UploadMetadata` → attachments; Drive, YouTube
  and Meet URLs are appended to the body (`gchatfmt/linkappend.go`).
- `UrlMetadata` with `length > 0` decorates text already in the body;
  `length == 0` means the URL is nowhere in the body, so it is appended,
  otherwise a link-only message converts to zero parts and is dropped.
- When such a link looks like media (a GIF, a preview image), `media.go`
  downloads and inlines it. That is the only fetch whose host a remote party
  chooses, so it goes through `gchatmeow/external.go` (https only, no proxy,
  no cookies, internal addresses refused), never `DownloadAttachment`.
  `disable_inline_url_media` turns it off.
- Outgoing format spans use `DO_NOT_RENDER`, and send/edit must set
  `MessageInfo.accept_format_annotations = true` or the server strips
  formatting.

### 2.7 Threads: Group → Topic → Message

| Room | Wire | `PortalMetadata` |
|---|---|---|
| Threaded space | `threaded_group` set | `ThreadsOnly`, `ThreadsEnabled` |
| Flat room / DM | `flat_group` set | neither |
| Flat room with inline threads | `flat_threads_enabled = true` | `ThreadsEnabled` |

A topic's head message has `message_id == topic_id`. `CreateTopic` starts a
topic, `CreateMessage` replies in one. A quote reply also sets
`MessageInfo.reply_to = {id, create_time}` with the target's µs timestamp,
which is why every message stores `TimestampMicro` and `TopicID`.

### 2.8 RPCs (`pkg/gchatmeow/api.go`)

| Endpoint | Used for |
|---|---|
| `paginated_world` | chat-list sync (§2.5) |
| `get_self_user_status` | own Gaia id |
| `get_members` | user profiles |
| `get_group` | chat info and members |
| `create_topic` / `create_message` | send (new topic / reply in topic) |
| `edit_message` / `delete_message` | edit / redact |
| `update_reaction` | reactions |
| `set_typing_state` | typing |
| `mark_group_readstate` | read receipts (µs) |
| `list_topics` / `list_messages` | backfill |
| `catch_up_group` / `catch_up_user` | missed events |
| `create_membership` / `remove_memberships` | invite, accept, kick, leave (spaces) |
| `create_group` | create a space; needs `attribute_checker_group_type = FLAT_ROOM` or it fails with a bare 400 |
| `update_group` | rename (`NAME`), description (`SPACE_DETAILS`) |
| `update_group_notification_settings` | per-chat mute |
| `hide_group` | hide a DM (a DM can't be left) |

Mutating responses return `WriteRevision{timestamp}`, used as resync
watermarks (§4).

## 3. Data flow

### 3.1 Google Chat → Matrix

```
channel chunk → pblite → StreamEventsResponse
  → SplitEventBodies (one Event per body)
  → GChatClient.handleGChatEvent (events.go) → bridgev2 remote event
  → per-portal queue → msgconv.ToMatrix
```

Dispatch is single and ordered. `SplitEventBodies` is applied to live events
and catch-up results alike. Body types map to message, edit, delete,
reaction, receipt and typing handlers; unknown types are logged and skipped.
Typing events carry their group only in `typing_state_changed.context`, not
in `Event.group_id`.

`SYSTEM_MESSAGE`s (`systemmessage.go`) carry membership changes and room
updates (name, topic) as annotations and become `ChatInfoChange`s.

### 3.2 Matrix → Google Chat

```
bridgev2 (checks RoomFeatures, resolves reply/thread)
  → HandleMatrixMessage (handlematrix.go) → msgconv.FromMatrix
  → CreateTopic or CreateMessage
```

Sends carry a `local_id` (`mautrix-googlechat%<uint64>`, `Message.local_id`
field 14) that comes back on the stream echo, so the bridge drops its own
messages. The pending entry is registered before the RPC so a fast echo is
still caught.

### 3.3 Space invites

The invitee gets a standalone `MembershipChangedEvent` for its own id
(`MEMBER_INVITED`) on the channel. `invites.go` records it in
`UserLoginMetadata.PendingInvites` and creates the room with the user
invited. Accept is `create_membership` with the own id, decline
`remove_memberships`. An invite sent while the bridge is down is only seen
if Google replays it.

## 4. Connection lifecycle

`Connect` builds a `gchatmeow.Client` and supervises it through
`handleConnState`:

| Condition | Response |
|---|---|
| channel ~1.5 h old | silent recycle |
| payload error / SID expiring | re-register at once |
| HTTP 400 "Unknown SID" | bounded resync, then reconnect |
| timeout / network error | backoff, `TRANSIENT_DISCONNECT` |
| 401 / `invalid_grant` / signed-out `mole/world` | `BAD_CREDENTIALS`, user re-logs in |

Events carry `user_revision` / `group_revision`, saved in login and portal
metadata. On reconnect, `catchUp` replays missed events through the normal
remote-event queue. The watermark only advances after an event is queued
successfully.

The realtime channel is the fragile part: Google may black-hole repeated
programmatic connects from datacenter IPs. One long-lived channel from a
normal IP works.

## 5. Features → code

| Feature | Where |
|---|---|
| Messages, formatting | `handlematrix.go`, `events.go`, `msgconv` |
| Threads, replies | `MessageMetadata.TopicID`, `handlematrix.go`, `capabilities.go` |
| Edits | `handleedit.go` (text only); dedup via `LastEditTime` |
| Deletes | `handleredact.go` |
| Reactions | `handlereaction.go` |
| Typing, receipts | `handletyping.go`, `handlereceipt.go` |
| Media | `media.go`: download via `get_attachment_url` with cookie-aware manual redirects; resumable upload → `UPLOAD_METADATA` |
| Inbound membership, renames, topic | `systemmessage.go` |
| Invite, kick (spaces) | `handlemembership.go` |
| Space invites | `invites.go` (§3.3) |
| Rename, topic (spaces) | `handleroomname.go`; `handleroomtopic.go` reads the guidelines with `get_group` and sends them back, since `SPACE_DETAILS` replaces both |
| Space creation | `createspace.go` (`create-group`): raise the bot to PL 100 via the double puppet, `create_group` (empty space), bind the room with `UpdateMatrixRoomID`, then invite |
| Leave | `leave.go` (`leave`, `delete-chat`): leave a space with every login, hide a DM; only then the bot removes the user from the room |
| Mute | `handlemute.go`; inbound via `ChatInfo.UserLocal` (§7) |
| Chat / user info | `chatinfo.go`, `userinfo.go`, `avatar.go` |
| Backfill, catch-up | `backfill.go`, §4 |
| Login | `login.go` |

Not supported: presence (bridgev2 has no model for it) and room avatars (a
space icon is an emoji). `ImplicitReadReceipts` stays false because Google
Chat marks your own messages read.

## 6. Frozen decisions

Changing these breaks stored data or the protocol.

- `pkg/gcid` formats, matching the Python bridge so migration works:
  - `PortalID`: `dm:<group_id>` or `space:<group_id>`.
  - `PortalKey.Receiver`: the login id for DMs, `""` for spaces (DMs are per
    user, spaces are shared).
  - `UserID` / `UserLoginID`: numeric Gaia id. `MessageID`: Google's
    `message_id`.
  - `PartID`: `""` for text, `att_<n>` for attachments. `att_10` sorts before
    `att_9`; known and left as is.
- The proto stays proto2 (§2.4).
- Every message stores `message_id`, `topic_id` and the µs timestamp.
- Rotated cookies are saved after every connect (§2.1).

## 7. Known quirks

- **Upload request shape.** Some clients get HTTP 500 on upload
  ([upstream #114](https://github.com/mautrix/googlechat/issues/114)) because
  they add `alt` / `key` to the signed upload URL and omit the XSRF header.
  This bridge does neither.
- **Matrix leave.** With `bridge.bridge_matrix_leave` off (the default),
  bridgev2 drops your own leave before `HandleMatrixMembership`; the next
  sync joins you back. With it on, the leave goes out through only one login.
  `ASIntent.SendState` adds no double-puppet marker, so a leave mirrored from
  Google Chat can echo back as a redundant `remove_memberships`. The `leave`
  command is the reliable path.
- **Several logins of one Matrix user in a space** works on Google Chat but
  looks muddled on Matrix: the other account shows as a ghost, invites between
  them can't show, and the power level follows whichever login synced last.
- **Mute.** `update_group_notification_settings` takes the group, a type (5
  space, 4 DM, meaning unknown) and a mute state (2 muted, 1 unmuted). The
  chat list returns it as `GroupNotificationSettings` field 3: MUTED when
  muted, absent otherwise. Fields 1 and 2 (`state`, `room_state`) are
  misnamed and say nothing about mute. Never use `SetDndDuration`; it
  silences the whole account. Mutes have no end time. bridgev2 applies an
  inbound mute through the double puppet (`ASIntent.MuteRoom`) and skips it
  without one; `MuteRoom` doesn't re-enable a disabled room push rule, so
  unmute once to fix such a room.

## Migration

`--migrate-from-python` (`pkg/migrate`) imports the Python bridge's
database; see [migration.md](migration.md).
