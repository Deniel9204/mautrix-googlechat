# maugclib: the embedded Google Chat client library (Python reference)

Research report for the Go/bridgev2 reimplementation of mautrix-googlechat.

All paths below are relative to
`/Users/danielagoston/utmshared/Projects/git/generalclaude/mautrix/_reference/googlechat-python/`
unless stated otherwise. Line numbers refer to the checked-in snapshot (read 2026-07-13).

`maugclib` is a stripped-down fork of [tdryer/hangups] rewired for Google Chat
("Dynamite") instead of Hangouts (`maugclib/README.md:2`). It speaks the
*private* Google Chat web-client API: cookie-authenticated HTTPS requests to
`https://chat.google.com/u/0/api/*` carrying binary protobuf, plus a Google
BrowserChannel (closure-library) long-polling channel at
`https://chat.google.com/u/0/webchannel/events` that streams pblite-encoded
protobuf events.

File map:

| File | Lines | Role |
|---|---|---|
| `maugclib/client.py` | 814 | `Client` class: API request methods, connect/disconnect, media up/download, xsrf refresh |
| `maugclib/channel.py` | 495 | `Channel` class: BrowserChannel long-polling, SID/AID handling, chunk parser, reconnect loop |
| `maugclib/http_utils.py` | 273 | `Session` (aiohttp wrapper with cookie jar, retries), `Cookies` and `FetchResponse` NamedTuples |
| `maugclib/pblite.py` | 176 | pblite (protojson) encoder/decoder between JSON arrays and protobuf messages |
| `maugclib/parsers.py` | 49 | Timestamp helpers + `GroupId` <-> `"dm:..."`/`"space:..."` string conversion |
| `maugclib/event.py` | 60 | Minimal observer/event system (`Event.add_observer` / `Event.fire`) |
| `maugclib/exceptions.py` | 85 | Exception taxonomy |
| `maugclib/googlechat.proto` | ~2500 | Hand-reverse-engineered proto schema (source of `googlechat_pb2`) |
| `maugclib/googlechat_pb2.py(i)` | — | Generated protobuf module used everywhere |
| `maugclib/__init__.py` | 21 | Public exports: `Client`, exceptions, `Cookies` |

There is **no** `auth.py` in maugclib anymore. `maugclib/README.md:29-32` still
mentions `maugclib.auth.TokenManager.from_cookies` — that is stale; the current
entry point is `http_utils.Cookies` passed straight into `Client(...)`.

---

## 1. Authentication

### 1.1 What the user provides

Authentication is **pure cookie auth** — no OAuth tokens, no refresh tokens, no
app passwords. The user logs into https://chat.google.com in a normal browser
and extracts five cookies (`maugclib/README.md:5-27`, `http_utils.py:39-44`):

```python
class Cookies(NamedTuple):   # http_utils.py:39
    compass: str   # COMPASS
    ssid: str      # SSID
    sid: str       # SID
    osid: str      # OSID
    hsid: str      # HSID
```

The bridge collects these two ways:

- **`login-cookie` bot command** (`mautrix_googlechat/commands/auth.py:82-104`):
  the user pastes a flat JSON object `{"COMPASS": ..., "SSID": ..., "SID": ...,
  "OSID": ..., "HSID": ...}`; keys are lower-cased and splatted into
  `Cookies(**...)`. The message is redacted afterwards.
- **Provisioning/web API** `POST /v1/login`
  (`mautrix_googlechat/web/auth.py:133-190`): body is
  `{"cookies": {...same 5 keys...}, "user_agent": "<optional browser UA>"}`.
  The user's real browser user-agent is stored per-user and reused for all
  subsequent requests (`mautrix_googlechat/db/user.py:37-38`).

The README's second `COMPASS` cookie under path `/u/0/webchannel/`
(`maugclib/README.md:23-25`) is **no longer required as input**: the channel
`register` endpoint is called with `?ignore_compass_cookie=1` and the server
issues a fresh webchannel-scoped `COMPASS` cookie (value prefix `dynamite-ui=`)
via Set-Cookie, which lands in the shared cookie jar automatically
(`channel.py:260-301`).

### 1.2 Cookie jar behavior

`http_utils.Session.__init__` (`http_utils.py:58-85`):

- Creates `aiohttp.CookieJar(quote_cookie=False)` — **the server does not
  support quoted cookie values** (comment at `http_utils.py:61`, hangups issue
  #498). A Go client must emit cookie values verbatim, unquoted.
- Each of the 5 cookies is installed with `domain=chat.google.com`, `path=/`
  (`http_utils.py:63-67`).
- Google rotates cookies over time (e.g. it sets a new `SIDCC` after login —
  comment `channel.py:267-268`); the jar picks up any `Set-Cookie` responses.
- `Session.get_auth_cookies()` (`http_utils.py:87-92`) reads the *current*
  values of the 5 named cookies back out of the jar; `Client.cookies` property
  exposes it (`client.py:132-134`). After every successful `connect()` the
  bridge compares them to what's stored and persists updated values
  (`mautrix_googlechat/user.py:279-283`). **The Go bridge must round-trip
  rotated cookie values to the DB or sessions die early.** Only the 5 named
  cookies are persisted (`mautrix_googlechat/db/user.py:47-48,81`); everything
  else (SIDCC, webchannel COMPASS) is re-acquired at runtime.

### 1.3 XSRF token bootstrap (`/mole/world`)

Before opening the channel, `Client.connect()` refreshes an XSRF token at most
once per 24h (`client.py:140-149`; `_last_token_refresh` initialized to
`-86400` at `client.py:67` so the first connect always refreshes).

`Client.refresh_tokens()` (`client.py:499-539`):

- `GET https://chat.google.com/u/0/mole/world?origin=https://mail.google.com&shell=9&hl=en&wfi=gtn-roster-iframe-id&hs=<hardcoded JSON blob>`
  with headers `authority: chat.google.com` and `refer: https://mail.google.com/`
  (yes, literally the misspelled header name `refer`, `client.py:515-518`).
  The `hs` value is a hardcoded gmail shell-state string (`client.py:513`).
- The HTML response contains `>window.WIZ_global_data = {...};</script>`
  (regex at `client.py:34`). From that JSON:
  - `wiz_data["qwAQke"] == "AccountsSignInUi"` ⇒ cookies invalid ⇒ raise
    `NotLoggedInError` (`client.py:536-537`). **This is the primary login
    validity check.**
  - `wiz_data["SMqcke"]` ⇒ the XSRF token (`client.py:538`).
- The token is sent on every API call as header `x-framework-xsrf-token`
  (`client.py:596-598`).

`refresh_tokens` doc comment (`client.py:500-504`) notes that this response
also contains everything needed for the `batchexecute` API, should Google ever
force a move to it.

### 1.4 Login sequence end-to-end (bridge side)

`User.connect()` (`mautrix_googlechat/user.py:259-291`):

1. Build `Client(cookies, user_agent=..., max_retries=3, retry_backoff_base=2)`.
2. `await client.refresh_tokens()` — validates cookies, gets xsrf token.
3. On first login, `proto_get_self_user_status` fetches the user's own
   Gaia ID / email (`user.py:441-452`).
4. Persist possibly-rotated cookies.
5. Spawn the long-poll loop: `client.connect(max_age=1.5h)` restarted forever
   by `User._start()` (`user.py:299-387`).

There is no proactive re-login: once cookies are invalidated server-side, the
user must re-extract cookies from a browser.

---

## 2. The realtime channel (`channel.py`)

Implements the client side of Google's **BrowserChannel** protocol
(closure-library `goog.net.BrowserChannel`); protocol docs links in the module
docstring (`channel.py:1-16`). Base URL:
`CHANNEL_URL_BASE = "https://chat.google.com/u/0/webchannel/"` (`channel.py:40`).

Constants: `PUSH_TIMEOUT = 60` seconds (two missed 15–30s heartbeats ⇒ dead,
`channel.py:41-43`), `MAX_READ_BYTES = 1 MiB` per read (`channel.py:44`).

### 2.1 register

`Channel._register()` (`channel.py:260-301`), called at the start of `listen()`
and again whenever the SID expires:

- Resets `_sid_param=None`, `_aid=0`, `_ofs=0`.
- `GET {base}/register?ignore_compass_cookie=1` with header
  `Content-Type: application/x-protobuf`. Non-200 ⇒ `UnexpectedStatusError`.
- On success the response Set-Cookie contains a webchannel `COMPASS` cookie
  whose value starts with `dynamite-ui=`; the suffix is stored as
  `_csessionid_param` (`channel.py:288-301`) — currently **unused** (the
  `csessionid` query param is commented out at `channel.py:316-318`), but the
  cookie itself must be present on subsequent webchannel requests (it lives in
  the jar).

### 2.2 Opening the backward channel (long poll)

`Channel._longpoll_request()` (`channel.py:362-467`). All requests are `GET
{base}/events`. Query params:

- Always: `VER=8` (protocol version), `t=1` ("trial"), `zx=<random base36 of
  64 random bits>` (`_unique_id()`, `channel.py:137-149`), `RID`, `SID`.
- **First request (no SID yet)** (`channel.py:378-387`): `SID=null`, `CVER=22`,
  `RID=<random 10000-99999, incremented>` and
  `$req=count=1&ofs=0&req0_data=%5B%5D` — i.e. an empty forward-channel
  payload folded into the query string of a GET.
- **Subsequent requests (have SID)** (`channel.py:389`): `CI=0`,
  `TYPE=xmlhttp`, `RID=rpc`, `AID=<last array id>`.

Headers: `referer: https://chat.google.com/` (`channel.py:391-393`).

SID acquisition (`channel.py:419-445`): the *first* response carries an
`X-HTTP-Initial-Response` header whose value is a JSON string like
`[0,["c","SID_HERE","",8,12]]]`; the SID is `json[0][1][1]`
(`_parse_sid_response`, `channel.py:124-134`). When a new SID is seen:

1. Reset `_aid=0`, `_ofs=0`.
2. Immediately send an "ack" GET to `{base}/events` with
   `VER=8, RID=rpc, SID=<sid>, AID=0, CI=0, TYPE=xmlhttp, zx=<uid>, t=1`
   (`channel.py:427-442`) — described as required though its purpose is unclear.
3. Send the **initial ping** on the forward channel
   (`_send_initial_ping`, `channel.py:347-360`): a `StreamEventsRequest` with
   `ping_event = PingEvent(state=ACTIVE,
   application_focus_state=FOCUS_STATE_FOREGROUND,
   client_interactive_state=INTERACTIVE, client_notifications_enabled=True)`.
   Without this ping the server doesn't start streaming events.

Then the response body is streamed: read up to 1 MiB at a time with a
60-second `async_timeout` around each read (`channel.py:447-453`); every chunk
goes to `_on_push_data`. The server keeps the HTTP response open ~ an hour
(comment `channel.py:365-367`) and sends heartbeats every 15–30s.

### 2.3 Chunked framing (`ChunkParser`)

`channel.py:67-121`. The body is a sequence of frames:

```
<length><LF><payload...>
```

**Critical quirk:** `<length>` is the payload length in *UTF-16 code units*
(JavaScript `String.length`), not bytes. The Python code emulates this by
decoding the buffer as UTF-8 (incrementally, tolerating a split multi-byte
char at the buffer end — `_best_effort_decode`, `channel.py:61-64`), re-encoding
it as UTF-16 (stripping the 2-byte BOM), and comparing `length*2` bytes
(`channel.py:95-121`). A Go implementation should count UTF-16 code units of
the decoded text (`len(utf16.Encode([]rune(s)))`) rather than bytes; astral
characters (e.g. many emoji) count as 2.

Each complete frame payload is a JSON **container array** of inner arrays
`[array_id, data_array]` (`channel.py:484-495`). For each inner array,
`on_receive_array` is fired with `data_array` and `_aid` is updated to
`array_id` afterwards — `AID` is the "last acknowledged array id" echoed in
subsequent requests.

The channel is considered connected once the first chunk arrives; first time
fires `on_connect`, later times fire `on_reconnect` (`channel.py:474-482`).

### 2.4 Event decoding

`Client._on_receive_array` (`client.py:545-563`):

- `data_array[0] == "noop"` ⇒ keep-alive heartbeat, ignored.
- Otherwise `data_array[0]` is a pblite array which is decoded into
  `googlechat_pb2.StreamEventsResponse` via `pblite.decode` (**no**
  `ignore_first_item` — field 1 is at index 0).
- `StreamEventsResponse` = `{event: Event = 1, sample_id: string = 2,
  clock_sync_response: ClockSyncResponse = 3}` (`googlechat.proto:1630-1634`).
- **Body splitting:** an `Event` (`googlechat.proto:1636-1748`) has
  `type = 3`, a first `body = 4` (an `EventBody`), and `repeated bodies = 8`
  for the second-and-later bodies. `Client.split_event_bodies`
  (`client.py:565-580`) normalizes this: yields the event itself (if `body` is
  set), then a copy per embedded body with `body`/`type` overwritten from
  `EventBody.event_type` (field 12). Each normalized `Event` is fired on
  `Client.on_stream_event`.
- `EventBody` oneof includes (only the uncommented, i.e. actually used, ones):
  `group_viewed=3, group_updated=5, message_posted=6, web_push_notification=10,
  membership_changed=14, message_deleted=18, message_reaction=22,
  user_status_updated=23, typing_state_changed=26, read_receipt_changed=33`
  (`googlechat.proto:1693-1737`). Event types enum has 50 values
  (`googlechat.proto:1639-1691`).

### 2.5 Forward channel (sending on the stream)

`Channel.send_stream_event(StreamEventsRequest)` (`channel.py:303-341`):
`POST {base}/events` with query `VER=8, RID=<incrementing int>, t=1,
SID=<sid>, AID=<last aid>`, `Content-Type:
application/x-www-form-urlencoded`, and form body:

```
count=1
ofs=<incrementing offset, per-SID>
req0_data=<JSON of pblite.encode(StreamEventsRequest)>
```

`_ofs` counts sent maps and resets on re-register/new SID. This path is only
used for pings (`StreamEventsRequest` fields: `ping_event=2`,
`clock_sync_request=3`, `platform=4`, `client_info=5`, ... —
`googlechat.proto:1507-1517`). Exposed via `Client.proto_send_stream_event`
(`client.py:811-814`). Regular messaging does NOT use the forward channel; it
uses the `/api/*` endpoints.

### 2.6 Reconnect / error logic (`Channel.listen`, `channel.py:205-258`)

- `listen(max_age)` registers, then loops while `retries <= max_retries`
  (bridge passes `max_retries=3`, backoff base 2 ⇒ 2^retries seconds sleep,
  `channel.py:222-226`).
- If total lifetime exceeds `max_age` ⇒ raise `ChannelLifetimeExpired`
  (`channel.py:218-219`). The bridge uses `max_age = 1.5h` and treats this as
  a normal, silent reconnect (`mautrix_googlechat/user.py:310,322-325`).
- Error mapping inside `_longpoll_request` (`channel.py:455-467`):
  - HTTP != 200: `UnexpectedStatusError`; specifically **HTTP 400 with
    "Unknown SID"** in reason or body ⇒ `SIDInvalidError` (`channel.py:408-411`).
  - `asyncio.TimeoutError` (no data for 60s) ⇒ `NetworkError`.
  - `aiohttp.ServerDisconnectedError` ⇒ `NetworkError`.
  - **`aiohttp.ClientPayloadError` ⇒ `SIDExpiringError`** (`channel.py:461-463`)
    — a truncated/aborted response body is interpreted as the SID nearing
    expiry.
  - other `aiohttp.ClientError` ⇒ `NetworkError`.
- In `listen`: `SIDExpiringError` ⇒ re-`_register()` and retry immediately
  (no backoff, `channel.py:233-240`); `NetworkError` ⇒ count a retry, fire
  `on_disconnect` if previously connected; clean EOF (server closed after ~1h)
  ⇒ reset `retries=0` and immediately re-poll (`channel.py:243-247`).
  `SIDInvalidError` propagates out of `listen` entirely.
- The bridge layer on `SIDInvalidError` runs a small sync (limit 3) to check
  for dropped messages before reconnecting (`mautrix_googlechat/user.py:326-351`).
- Comment at `channel.py:255-256`: after any errored poll, messages may have
  been dropped — the client must be prepared to resync.

---

## 3. Request API surface (`client.py`)

### 3.1 Wire format

`Client._gc_request(endpoint, request_pb, response_pb)` (`client.py:582-615`):

```
POST https://chat.google.com/u/0/api/{endpoint}?c={reqid}&rt=b&alt=proto&key={API_KEY}
Content-Type: application/x-protobuf
X-Goog-Encode-Response-If-Executable: base64
x-framework-xsrf-token: <token from /mole/world>
<binary serialized request proto>
```

- `c` is an incrementing per-connection request counter (`_api_reqid`,
  `client.py:123-126`; reset to 0 on each `connect`, `client.py:146`); server
  appears to ignore duplicates, kept "to not stand out".
- `rt=b` requests binary response framing; `alt=proto` is added by
  `_base_request` (`client.py:655-666`).
- `key=AIzaSyD7InnYR3VKdb4j2rMUEbTCIr2VyEazl6k` — hardcoded API key inherited
  from the Hangouts web client (`client.py:28-29`); required to avoid
  403 "Daily Limit for Unauthenticated Use Exceeded" (`client.py:662-664`).
- Response body is parsed directly with `response_pb.ParseFromString`
  (`client.py:609-614`), i.e. raw binary protobuf. The
  `X-Goog-Encode-Response-If-Executable: base64` header (set at
  `client.py:650-653`) only causes base64 encoding "if executable"; in
  practice with `rt=b` the body arrives as raw binary and is parsed as such.
- Every request proto carries `request_header = RequestHeader(
  client_type=WEB, client_version=2440378181258,
  client_feature_capabilities=ClientFeatureCapabilities(
  spam_room_invites_level=FULLY_SUPPORTED))` (`client.py:103-109`,
  `googlechat.proto:106-139`).

### 3.2 Low-level proto methods (all `client.py:682-814`)

| Method | Endpoint (`/u/0/api/…`) | Request proto | Response proto | Purpose |
|---|---|---|---|---|
| `proto_get_user_presence` | `get_user_presence` | `GetUserPresenceRequest` | `GetUserPresenceResponse` | Presence for one or more users (`client.py:682`) |
| `proto_get_members` | `get_members` | `GetMembersRequest` | `GetMembersResponse` | Resolve user/member info by ID (`client.py:691`) |
| `proto_paginated_world` | `paginated_world` | `PaginatedWorldRequest` | `PaginatedWorldResponse` | List all conversations ("world view"); used for chat sync (`client.py:700`) |
| `proto_get_self_user_status` | `get_self_user_status` | `GetSelfUserStatusRequest` | `GetSelfUserStatusResponse` | Own user status incl. Gaia ID + user revision (`client.py:710`, proto:101-104) |
| `proto_get_group` | `get_group` | `GetGroupRequest` | `GetGroupResponse` | Fetch one group/DM incl. membership (`client.py:722`) |
| `proto_mark_group_read_state` | `mark_group_readstate` | `MarkGroupReadstateRequest` | `MarkGroupReadstateResponse` | Send read receipt / last-read time (`client.py:730`) |
| `proto_create_topic` | `create_topic` | `CreateTopicRequest` | `CreateTopicResponse` | Send a message that starts a new topic/thread (`client.py:738`) |
| `proto_create_message` | `create_message` | `CreateMessageRequest` | `CreateMessageResponse` | Send a message into an existing thread (`client.py:746`) |
| `proto_update_reaction` | `update_reaction` | `UpdateReactionRequest` | `UpdateReactionResponse` | Add/remove emoji reaction (`client.py:754`) |
| `proto_delete_message` | `delete_message` | `DeleteMessageRequest` | `DeleteMessageResponse` | Delete/redact a message (`client.py:762`) |
| `proto_edit_message` | `edit_message` | `EditMessageRequest` | `EditMessageResponse` | Edit message text (`client.py:769`) |
| `proto_set_typing_state` | `set_typing_state` | `SetTypingStateRequest` | `SetTypingStateResponse` | Typing notifications (group or topic context) (`client.py:776`) |
| `proto_catch_up_user` | `catch_up_user` | `CatchUpUserRequest` | `CatchUpResponse` | Replay missed events for the whole user since a revision (`client.py:783`, proto:2143) |
| `proto_catch_up_group` | `catch_up_group` | `CatchUpGroupRequest` | `CatchUpResponse` | Replay missed events for one group since a revision (`client.py:790`, proto:2135) |
| `proto_list_topics` | `list_topics` | `ListTopicsRequest` | `ListTopicsResponse` | Backfill: page through topics of a group (`client.py:797`) |
| `proto_list_messages` | `list_messages` | `ListMessagesRequest` | `ListMessagesResponse` | Backfill: page through messages of a topic (`client.py:804`) |
| `proto_send_stream_event` | *(forward channel, not `/api`)* | `StreamEventsRequest` | — | Pings/clock sync via webchannel POST (`client.py:811`) |

### 3.3 High-level convenience methods

| Method | Wraps | Notes |
|---|---|---|
| `connect(max_age)` (`client.py:140`) | channel `listen` | Refreshes xsrf token if >24h old; forwards channel events to client events |
| `disconnect()` (`client.py:172`) | — | Cancels the listen task |
| `download_attachment(url, max_size)` (`client.py:182`) | raw GET | See §5 |
| `read_with_max_size(resp, max_size)` (`client.py:238`) | — | Streaming download with size cap ⇒ `FileTooLargeError` |
| `upload_file(data, group_id, filename, mime_type)` (`client.py:275`) | resumable upload | See §5 |
| `update_read_timestamp(conv_id, dt)` (`client.py:323`) | `mark_group_readstate` | µs timestamps |
| `react(conv, thread, msg, emoji, remove)` (`client.py:338`) | `update_reaction` | `Emoji(unicode=emoji)`; ADD/REMOVE enum |
| `delete_message(conv, thread, msg)` (`client.py:367`) | `delete_message` | |
| `edit_message(conv, thread, msg, text, annotations)` (`client.py:385`) | `edit_message` | sets `MessageInfo(accept_format_annotations=True)` |
| `send_message(conv, text, annotations, thread_id, reply_to, reply_to_ts, local_id)` (`client.py:413`) | `create_topic` / `create_message` | `thread_id` set ⇒ `CreateMessageRequest`, else `CreateTopicRequest(history_v2=True)`; `local_id` defaults to `hangups%<random uint64>` for echo dedup; optional `SendReplyTarget` for inline replies |
| `mark_typing(conv, thread_id, typing)` (`client.py:477`) | `set_typing_state` | `TypingContext` with either `group_id` or `topic_id`; returns `start_timestamp_usec` |
| `refresh_tokens()` (`client.py:499`) | `/mole/world` | See §1.3 |

Conversation-ID convention used throughout the bridge:
`"dm:<dm_id>"` / `"space:<space_id>"` strings, converted to/from the `GroupId`
proto by `parsers.group_id_from_id` / `parsers.id_from_group_id`
(`parsers.py:26-49`). Message IDs live in a nested
`MessageId{parent_id: MessageParentId{topic_id: TopicId{group_id, topic_id}},
message_id}` structure; for un-threaded operations, `thread_id or message_id`
is used as the topic id (e.g. `client.py:349-358`).

Timestamps are **microseconds** since epoch (`parsers.py:12-23`).

---

## 4. pblite encoding (`pblite.py`)

pblite ("protojson") represents a protobuf message as a **JSON array where
index i-1 holds field number i** (`pblite.py:1-12`; Google's implementation is
closure-library `goog.proto2`). Used only for the webchannel: decoding
`StreamEventsResponse` from the backward channel and encoding
`StreamEventsRequest` for the forward channel. The `/api/*` endpoints use
binary protobuf instead.

Decoder `decode(message, pblite, ignore_first_item=False)` (`pblite.py:73-125`):

- Non-list input ⇒ log & ignore (permissive throughout: bad values are
  warnings, never exceptions).
- `ignore_first_item` skips index 0 (some legacy responses prefix the array
  with an abbreviated message name); **not used** for the stream-events path.
- **Trailing-dict extension:** if the last array element is a JSON object, it
  is a sparse map of `{"<field_number>": value}` for high field numbers
  (`pblite.py:96-103`). A Go decoder must handle this.
- `null` values are skipped; unknown field numbers are logged at debug level.
- Type handling per field descriptor: nested messages recurse;
  `bytes` fields are base64-decoded; `int64` values arrive as JSON strings and
  are `int()`ed (`pblite.py:34-37`); repeated fields map element-wise, and a
  type error while decoding a repeated field clears the whole field
  (`pblite.py:48-70`).

Encoder `encode(message)` (`pblite.py:140-176`):

- Only set fields are emitted (`ListFields`); the array is padded with `null`s
  so each value lands at index `field_number - 1`.
- Nested messages recurse; `bytes` → base64 strings; repeated fields → arrays.
- The encoder never produces the trailing-dict form.
- int64s are emitted as JSON numbers by Python; for Go, beware that
  JavaScript-side consumers use string-encoded int64s — the decoder must accept
  both string and number for int64.

---

## 5. Media upload / download

### 5.1 Upload (`Client.upload_file`, `client.py:275-321`)

Two-step Google resumable upload against
`UPLOAD_URL = "https://chat.google.com/uploads"` (`client.py:27`):

1. `POST https://chat.google.com/uploads?group_id=<plain group id>&alt=&key=<API_KEY>`
   with headers
   `x-goog-upload-protocol: resumable`, `x-goog-upload-command: start`,
   `x-goog-upload-content-length: <n>`, `x-goog-upload-content-type: <mime>`,
   `x-goog-upload-file-name: <name>`, empty body.
   Note `group_id` here is the **plain** id (no `dm:`/`space:` prefix — the
   bridge passes `self.gcid_plain`, `mautrix_googlechat/portal.py:1099-1100`).
   The response header `x-goog-upload-url` gives the session URL; missing ⇒
   `NetworkError` (`client.py:297-300`).
2. `PUT <upload_url>` with headers `x-goog-upload-command: upload, finalize`,
   `x-goog-upload-protocol: resumable`, `x-goog-upload-offset: 0` and the raw
   file bytes as body.
3. The response body is a **base64-encoded** binary `UploadMetadata` proto
   (`client.py:311-319`; proto at `googlechat.proto:865-879`, key field:
   `attachment_token = 1`, plus `content_name = 3`, `content_type = 4`).

To attach the upload to a message, the bridge wraps it in
`Annotation(type=UPLOAD_METADATA, upload_metadata=<UploadMetadata>,
chip_render_type=Annotation.RENDER)` and passes it in `annotations` of
`send_message` (`mautrix_googlechat/portal.py:1102-1108`).

### 5.2 Download (`Client.download_attachment`, `client.py:182-236`)

Incoming attachments are announced as annotations; the bridge builds the URL
(`mautrix_googlechat/portal.py:1465-1523`):

- `upload_metadata` annotation ⇒
  `https://chat.google.com/api/get_attachment_url?url_type=DOWNLOAD_URL&attachment_token=<token>`;
  for `image/*` content types instead
  `url_type=FIFE_URL&sz=w10000-h10000&content_type=<mime>` (full-size image).
- `url_metadata` annotation ⇒ direct `image_url`/`url`.

`download_attachment` then:

- Follows redirects **manually** (up to 10 hops; typically 4 for files, 1 for
  images — comment `client.py:205-206`), because hops bounce between
  `chat.google.com` (needs cookies) and `googleusercontent.com` (must NOT get
  cookies): `.google.com` hosts are fetched through the authenticated
  `Session`, all other hosts through a fresh cookie-less
  `aiohttp.ClientSession` (`client.py:206-223`).
- Redirect statuses handled: 301, 302, 307, 308.
- Result: `(bytearray, mime from Content-Type, filename from
  Content-Disposition or last path segment)` (`client.py:225-233`).
- `read_with_max_size` (`client.py:238-273`) enforces `max_size` both from
  `Content-Length` up front and while streaming, raising `FileTooLargeError`.

---

## 6. Error / exception taxonomy (`exceptions.py`)

```
HangupsError                       # base (exceptions.py:7)
├── NetworkError                   # transport failures & non-200 in Session.fetch (11)
├── ConversationTypeError          # unused in lib core (15)
├── ChannelLifetimeExpired         # channel outlived max_age; normal recycle (19)
├── SIDError                       # base for channel-session errors (23)
│   ├── SIDExpiringError           # "SID is about to expire" — from ClientPayloadError (27)
│   └── SIDInvalidError            # "SID became invalid" — HTTP 400 "Unknown SID" (32)
├── FileTooLargeError              # media download over cap (37)
├── NotLoggedInError               # cookies invalid (WIZ data says AccountsSignInUi) (41)
└── ResponseError(message, body)   # carries response body (45)
    ├── ResponseNotJSONError       # (53)
    ├── UnexpectedResponseDataError# (58)
    └── UnexpectedStatusError      # non-200 with parsed body; extracts JSON
                                   # "error"/"error_description" into
                                   # .error_code/.error_desc; keeps .status/.reason (62-85)
```

### Auth-expiry detection (bridge policy, `mautrix_googlechat/user.py`)

- `NotLoggedInError` from `refresh_tokens()` ⇒ logout (`user.py:248-250`).
- `UnexpectedStatusError` with `.error_code == "invalid_grant"` **or**
  `.status == 401` escaping the connection loop ⇒ logout + BAD_CREDENTIALS
  bridge state (`user.py:357-364`, `_try_init` at `user.py:245-247`).
  (These arise from the channel register/long-poll paths, which raise
  `UnexpectedStatusError` with status; plain API calls via `Session.fetch`
  collapse non-200 into `NetworkError` without status — `http_utils.py:165-172`.)
- Everything else is treated as transient: bridge-level reconnect loop with
  backoff 4s × 1.5 (reset after 60s of stability), switching from
  TRANSIENT_DISCONNECT to UNKNOWN_ERROR once backoff exceeds 60s
  (`user.py:299-387`).

---

## 7. Miscellany a Go reimplementer must know

- **Base URLs / constants:**
  - API: `GC_BASE_URL = "https://chat.google.com/u/0"` (`client.py:31`)
  - Channel: `https://chat.google.com/u/0/webchannel/` (`channel.py:40`)
  - Uploads: `https://chat.google.com/uploads` (`client.py:27`)
  - API key: `AIzaSyD7InnYR3VKdb4j2rMUEbTCIr2VyEazl6k` (`client.py:29`)
  - Client version claimed in `RequestHeader`: `2440378181258` (`client.py:105`)
- **User-Agent:** per-user browser UA stored at login; `Session` rewrites any
  `Chrome/x.y.z.w` → `Chrome/114.0.0.0` and `Firefox/x.y` → `Firefox/114.0`
  (hardcoded "latest" versions, `http_utils.py:23-30,69-77`). Default UA is a
  Windows 10 Chrome 114 string (`http_utils.py:25-28`). A Go port should bump
  these and keep them configurable.
- **Safety rail:** `Session._fetch_raw` refuses any URL whose host does not
  end in `.google.com` (prevents cookie leakage, `http_utils.py:249-252`).
  Every request gets `Connection: Keep-Alive` (`http_utils.py:255`).
- **`ssl=False`** is passed to aiohttp (`http_utils.py:264`) — certificate
  verification is disabled in the Python client. Do **not** replicate this in
  Go; it appears to be a workaround, not a requirement.
- **Retries:** `Session.fetch` retries transport errors 3× before raising
  `NetworkError` (`http_utils.py:20,136-163`); connect timeout 30s, body-read
  timeout 30s per request (`http_utils.py:18-19`). Long-poll requests use
  `fetch_raw` and bypass these retries/timeouts.
- **Proxy:** honors `HTTP_PROXY` env var (`client.py:93-95`) plus aiohttp
  `trust_env=True`.
- **Echo suppression:** outgoing messages carry `local_id`
  (`hangups%<random-uint64>` by default) that reappears in the
  `MESSAGE_POSTED` stream event, letting the bridge dedupe its own messages
  (`client.py:440`).
- **Revision tracking:** stream `Event`s carry `user_revision`/`group_revision`
  (proto:1743-1746); the bridge persists the user revision
  (`mautrix_googlechat/db/user.py:104-109`) and uses `catch_up_user` /
  `catch_up_group` after gaps. `CatchUpResponse` can signal paging/status
  (proto:2151).
- **Channel lifecycle policy (bridge):** each `Client.connect` runs one channel
  for at most 1.5h then recycles it silently (`user.py:310`); `_skip_on_connect`
  suppresses the "connected" notice/resync on such planned reconnects.
- **Event fan-out:** maugclib's `event.Event` fires observers sequentially and
  awaits coroutine observers one-by-one (`event.py:51-57`); bridgev2's own
  event loop replaces this, but note ordering is preserved per channel array —
  the Go port should preserve per-connection ordering of stream events.
- **The `c` counter and `zx` randomness** exist purely to mimic the web client
  ("keep it around to not stand out", `client.py:123-126`).
- **Read-state, typing, reactions, edits, deletes** are all plain `/api/*`
  calls — only pings go over the forward channel.
- **`get_attachment_url`** (`https://chat.google.com/api/get_attachment_url`,
  no `/u/0`) is used by the bridge for incoming attachments; `url_type` is
  `DOWNLOAD_URL` or `FIFE_URL` (images, with `sz=w10000-h10000`)
  (`mautrix_googlechat/portal.py:1471-1480`).
- **Vestigial fields:** `Client._client_id`, `Client._email`,
  `_last_active_secs`, `_active_client_state` are initialized but never
  populated in this fork (`client.py:111-121`); ignore them.
- The generated `googlechat_pb2` schema is hand-maintained
  (`googlechat.proto` with many commented-out fields); the Go port will need
  its own `.proto` compile of this file — the field numbers in it are the
  ground truth for both binary proto (API) and pblite (channel) paths.
