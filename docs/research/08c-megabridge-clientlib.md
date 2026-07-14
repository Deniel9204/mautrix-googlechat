# 08c — Megabridge `gchatmeow` client-lib fidelity audit vs `maugclib`

Audit of `/Users/danielagoston/utmshared/Projects/git/generalclaude/mautrix/_reference/googlechat-megabridge/pkg/gchatmeow/`
(the unfinished Go rewrite, "megabridge" branch) against
`/Users/danielagoston/utmshared/Projects/git/generalclaude/mautrix/_reference/googlechat-python/maugclib/`,
using `docs/research/01-maugclib-client-library.md` as the checklist of load-bearing behaviors.

Paths below: `Go:` = `_reference/googlechat-megabridge/pkg/gchatmeow/`, `Py:` = `_reference/googlechat-python/maugclib/`,
`conn:` = `_reference/googlechat-megabridge/pkg/connector/`. Line numbers from the checked-in snapshots (read 2026-07-14).

**Verdict up front:** this is a *recognizable but partial port with correctness-critical divergences*, not a faithful
line-by-line port. The module structure and most request choreography mirror maugclib closely (someone clearly worked
from the Python source), but the channel cannot survive more than a few minutes of operation as written, 4 of 16 RPCs
are missing, cookie rotation is never persisted, and the shared pblite codec lacks two of maugclib's decode behaviors.
Estimated client-lib completeness: **~60%** (choreography ported; hardening, lifecycle, and edge behavior absent).

File map (Go side — 7 files + generated proto, vs maugclib's ~1,900 lines of hand-written Python):

| Go file | Lines | Ports |
|---|---|---|
| `channel.go` | 457 | `Py:channel.py` (495 ln) — BrowserChannel, chunk parser, listen loop |
| `client.go` | 277 | `Py:client.py` connect/refresh_tokens/on_receive_array/split_event_bodies/download |
| `api.go` | 235 | `Py:client.py` `_gc_request`/`_base_request` + proto_* wrappers + upload |
| `session.go` | 181 | `Py:http_utils.py` Session |
| `cookies.go` | 32 | `Py:http_utils.py` Cookies NamedTuple |
| `event.go` | 26 | `Py:event.py` observer system |
| `proto/googlechat.proto` | ~2,400 | `Py:googlechat.proto`, converted proto2→proto3 |

---

## 1. Channel (`Go:channel.go` vs `Py:channel.py`)

### 1.1 Register choreography — PRESENT, faithful

- `register()` resets `sidParam=""`, `aid=0`, `ofs=0` and issues
  `GET {base}/register?ignore_compass_cookie=1` with `Content-Type: application/x-protobuf`
  (`Go:channel.go:186-193`), matching `Py:channel.py:260-286`. Non-200 → `ErrUnexpectedStatus`
  (`Go:channel.go:199-202`).
- Webchannel `COMPASS` cookie with `dynamite-ui=` prefix is scraped from Set-Cookie and the suffix returned as
  `csessionid` (`Go:channel.go:204-211`), stored at `Go:channel.go:131` — unused afterwards, same as Python
  (`Py:channel.py:288-301,316-318`). The cookie itself lands in the jar automatically because `FetchRaw` uses the
  jar-backed `http.Client` (`Go:session.go:89-97,180`).

### 1.2 SID acquisition, ack GET, initial ping — PRESENT, faithful

- First poll: `SID=null`, `CVER=22`, `$req=count=1&ofs=0&req0_data=%5B%5D`, incrementing `RID`
  (`Go:channel.go:275-279` vs `Py:channel.py:379-387`). Subsequent polls: `CI=0`, `TYPE=xmlhttp`, `RID=rpc`,
  `AID=<last>` (`Go:channel.go:280-286` vs `Py:channel.py:389`). `referer: https://chat.google.com/` header
  (`Go:channel.go:288-290` vs `Py:channel.py:391-393`).
  - **Wire nit:** Go's `url.Values.Encode()` percent-encodes the whole `$req` value, so the pre-encoded `%5B%5D`
    is double-encoded to `%255B%255D` (`Go:channel.go:277` + `Go:session.go:149-154`); aiohttp/yarl requotes and
    leaves `%5B` intact. Server tolerance unverified.
- SID parsed from `X-HTTP-Initial-Response` as `json[0][1][1]` (`Go:channel.go:309-312,428-449` vs
  `Py:channel.py:124-134,419-425`). On new SID: `aid=0`, `ofs=0`, ack GET with
  `VER=8, RID=rpc, SID, AID=0, CI=0, TYPE=xmlhttp, zx, t=1` (`Go:channel.go:315-333` vs `Py:channel.py:427-442`),
  then initial `PingEvent{ACTIVE, FOCUS_STATE_FOREGROUND, INTERACTIVE, notifications=true}`
  (`Go:channel.go:257-265,335-337` vs `Py:channel.py:347-360`). All four fields serialize despite proto3 implicit
  presence because none is the zero value (enum values 1/1/1 + `true`; `Go:proto/googlechat.proto:1577-1601`).
- Resource leak: neither the ack-GET response (`Go:channel.go:331`) nor the forward-channel POST response
  (`Go:channel.go:249-253`, `_ = res`) is ever closed.

### 1.3 Chunk framing — DIVERGENT: counts UTF-16 code units correctly, but split-multibyte handling is broken

The counting basis is right: `GetChunks` converts the decoded remainder to UTF-16 code units via
`utf16.Encode([]rune(s))` and compares/slices by code-unit count (`Go:channel.go:62-66,93-98`), matching
JavaScript `String.length` semantics as required (`Py:channel.py:87-115`). Astral chars count as 2. Good.

But the Python parser keeps its buffer as **raw bytes** and only best-effort-decodes a *view* of it, leaving an
undecodable split multi-byte UTF-8 tail in the byte buffer for the next read (`Py:channel.py:61-64,95-121`,
incremental decoder + `self._buf = self._buf[drop_length:]` on the raw bytes). The Go parser instead:

1. converts the whole byte buffer to `string` (`Go:channel.go:87`) — Go string conversion replaces an
   incomplete trailing UTF-8 sequence with U+FFFD replacement characters (one per invalid byte), and
2. re-encodes the leftover *decoded* text back into the byte buffer (`Go:channel.go:99`:
   `p.buf = []byte(utf16Str[length:].String())`), permanently baking the U+FFFD bytes (`EF BF BD`) into the buffer.

**Failure mode:** whenever a TCP read boundary splits a multi-byte character (common — emoji and non-ASCII names
appear constantly in the stream), the character is corrupted to U+FFFD; and because U+FFFD counts as 1 UTF-16 unit
while the eventual astral char counts 2, the `len(utf16Str) < length` completeness check (`Go:channel.go:94`) can
fire early, mis-framing the chunk and desynchronizing the JSON parse (`Go:channel.go:390-392` then hard-errors the
poll). Python explicitly documents and handles this case (`Py:channel.py:82-93`).

Also: every received chunk is dumped to stdout including full message content (`Go:channel.go:369`) — debug
scaffolding, not production code.

### 1.4 Long-poll loop — PRESENT but FATALLY DIVERGENT: 90s client timeout kills every poll

- Python reads the streamed body with a 60s per-read watchdog (`PUSH_TIMEOUT`, `Py:channel.py:41-43,447-453`) and
  otherwise lets the response stay open ~1h. The Go port defines `pushTimeout = 60 * time.Second`
  (`Go:channel.go:27`) but **never uses it** (only occurrence in the package), and instead the shared
  `http.Client` has `Timeout: 90 * time.Second` (`Go:session.go:16,96`). Go's `http.Client.Timeout` covers
  *the entire request including body read*, so **every long poll is aborted after 90 seconds**, regardless of
  heartbeats. The mid-body timeout error is not `io.EOF`, so it maps to `ErrNetworkError`
  (`Go:channel.go:349-357`) → counted as a retry with backoff.
- Because `retries` is only reset on a *clean* poll return (`Go:channel.go:180`, reached only via `io.EOF` at
  `Go:channel.go:351-352`), the 90s aborts accumulate: after `maxRetries` (default 5, `Go:client.go:57-59`)
  consecutive aborts `Listen` returns "ran out of retries" (`Go:channel.go:135,183`) — i.e. **the channel
  permanently dies after ~8 minutes**, and since `Connect` launches it as a fire-and-forget goroutine
  (`Go:client.go:116`: `go c.channel.Listen(ctx, maxAge)` with the error discarded), nothing notices, reconnects,
  or updates bridge state. Python's equivalent loop is awaited and its errors drive the bridge reconnect ladder
  (`Py:client.py:140-170`, `mautrix_googlechat/user.py:299-387`).

### 1.5 Error ladder — PARTIAL / DIVERGENT

Typed sentinels exist: `ErrNetworkError`, `ErrUnexpectedStatus`, `ErrSIDInvalid`, `ErrSIDExpiring`,
`ErrChannelLifetime` (`Go:channel.go:33-39`), mirroring `Py:exceptions.py`. But the mapping and handling diverge:

| maugclib behavior | Go status |
|---|---|
| HTTP 400 + "Unknown SID" → `SIDInvalidError`, **propagates out of `listen`** so the bridge can run a gap-sync (`Py:channel.py:408-411`, `user.py:326-351`) | Detected (`Go:channel.go:301-305`) but **not special-cased in `Listen`** — falls into the generic retry branch (`Go:channel.go:169-177`), which re-polls with the *same invalid SID* (no re-register) until retries run out. Never reaches the connector. |
| `aiohttp.ClientPayloadError` (truncated body) → `SIDExpiringError` → immediate re-register, no backoff (`Py:channel.py:233-240,461-463`) | Mapped from read errors containing `"use of closed network connection"` (`Go:channel.go:354-356`) — a string that matches *client-side* connection closes, not a server-truncated chunked body (which yields `unexpected EOF`/`http: ...` errors in Go). Re-register path itself is faithful (`Go:channel.go:155-167`). In practice `ErrSIDExpiring` will almost never fire. |
| 60s read watchdog → `NetworkError` (`Py:channel.py:447-453`) | Absent (see §1.4). |
| `UnexpectedStatusError` carries status/reason/body; bridge maps `invalid_grant`/401 → logout (`Py:exceptions.py:62-85`, `user.py:357-364`) | `fmt.Errorf`-wrapped sentinel with no structured status (`Go:channel.go:201,306`); no auth-expiry ladder anywhere. Connector maps *any* `Connect` error to `BadCredentials` (`conn:client.go:57-64`) — including transient network failures. |
| `NotLoggedInError` typed (`Py:exceptions.py:41`) | Untyped `fmt.Errorf("ErrNotLoggedIn")` — the typed error is commented out (`Go:client.go:152-155`). Callers cannot `errors.Is` it. |

### 1.6 Backoff — PRESENT

`base^retries` seconds, skipped for SID-expiry re-register (`Go:channel.go:140-149` vs `Py:channel.py:222-240`).
Default base 2 (`Go:client.go:60-62`). Faithful.

### 1.7 1.5h recycle — ABSENT (commented out)

`Listen` takes `maxAge` but the lifetime check is **commented out** (`Go:channel.go:133-138`); `ErrChannelLifetime`
is defined (`Go:channel.go:38`) and never returned. The connector passes 90 minutes (`conn:client.go:57`) which is
silently ignored. Python: `Py:channel.py:218-219` + bridge treating it as silent recycle (`user.py:310,322-325`).

### 1.8 Periodic sync / reconnect resync — ABSENT

- `Channel.OnReconnect` and `Channel.OnDisconnect` exist and fire (`Go:channel.go:172-175,376-385`), but
  `Client.Connect` only wires `OnConnect` and `OnReceiveArray` (`Go:client.go:111-114`) — the client-level
  `OnReconnect`/`OnDisconnect` events (`Go:client.go:42-43`) are never fired, so the connector's disconnect
  observer (`conn:client.go:50-54`) is dead code and no resync ever happens after a drop.
- Python resyncs on every reconnect and runs `catch_up_user` after gaps (`user.py:326-351`); megabridge has no
  `catch_up_user` wrapper at all (§4) and syncs exactly once, on first connect (`conn:client.go:44-49,210-282`).

### 1.9 Event ordering — DIVERGENT (ordering lost)

maugclib fires observers **sequentially and awaits each** (`Py:event.py:51-57`); research doc 01 §7 explicitly
flags that a Go port must preserve per-connection stream-event order. The Go `Event.Fire` spawns **one goroutine
per observer per event** (`Go:event.go:20-26`), so consecutive stream events race — message ordering, edits
vs. originals, and read receipts can be processed out of order. `aid` is also updated *after* firing the async
observer (`Go:channel.go:413-416`), so an ack can be sent for an array the handler hasn't processed.

Cosmetic: `zx` is base64-of-decimal-string (`Go:channel.go:423-426`) instead of Python's base36 of 64 random bits
(`Py:channel.py:137-149`) — looks nothing like the web client's `zx`, contrary to the "don't stand out" intent.

---

## 2. pblite codec — SHARED library (go.mau.fi/util/pblite), not its own impl; two behaviors missing

Megabridge imports `go.mau.fi/util/pblite` v0.8.6 (`Go:channel.go:20`, `Go:client.go:17`; `go.mod`:
`go.mau.fi/util v0.8.6`). This is the **same shared package gmessages uses**
(`_reference/gmessages/pkg/libgm/http.go:15`, `longpoll.go:20`, at `go.mau.fi/util v0.9.10`). Note: the task's
reference path `_reference/gmessages/libgm/pblite/` does not exist in the current gmessages checkout — the codec
was upstreamed into `go.mau.fi/util/pblite`; gmessages retains only a 54-line debug helper at
`pkg/libgm/pblitedecode/main.go`. So megabridge's pblite is neither hand-rolled nor forked: it is the
battle-tested gmessages codec. Against maugclib's requirements (`Py:pblite.py`, doc 01 §4):

| Behavior | maugclib | go.mau.fi/util@v0.8.6/pblite |
|---|---|---|
| Field number i at array index i-1 | `Py:pblite.py:73-125` | `deserialize.go:185-198` ✔ |
| int64/int32 accepted as string *or* number | `Py:pblite.py:34-37` | `deserialize.go:101-124` ✔ (also uint32/64: 125-148; bool accepts float: 168-175) |
| bytes ↔ base64 | `Py:pblite.py:110-113,160` | `deserialize.go:85-96`, `serialize.go:98-99` ✔ (plus `pblite_binary` extension for base64-embedded binary protos) |
| Encoder pads with nulls to max field number, emits only set fields | `Py:pblite.py:140-176` | `serialize.go:121-147` ✔ |
| **Trailing sparse dict** (`{"<fieldnum>": value}` as last array element) | `Py:pblite.py:96-103` | **ABSENT** — `deserializeFromSlice` treats every element positionally (`deserialize.go:185-201`); a trailing JSON object either lands out of declared range (silently dropped, `deserialize.go:189`) or on a declared message field → type-mismatch **error** (`deserialize.go:76-79`) |
| **Permissive decode** (bad values logged, never raised) | `Py:pblite.py:75-125` | **ABSENT** — any mismatch aborts the whole `Unmarshal` with an error (`deserialize.go:194-197`) |

Consequence: any stream frame using the trailing-dict form (Google uses it for high field numbers) or containing
one unexpected value makes `pblite.Unmarshal` fail, and `onReceiveArray` then drops the **entire
`StreamEventsResponse`** with only a printed error (`Go:client.go:196-199`) — silent message loss, where Python
would decode everything else in the message. Encoder emits int64 as JSON numbers (`serialize.go:100-101`), same
as Python's encoder — fine for the forward channel.

Also note `StreamEventsResponse` decode has no `ignore_first_item` — correct, matching `Py:client.py:552-556`
(`Go:client.go:183-199`).

---

## 3. Session / cookies (`Go:session.go`, `Go:cookies.go` vs `Py:http_utils.py`)

- **5-cookie handling — PRESENT:** `Cookies{COMPASS,SSID,SID,OSID,HSID}` (`Go:cookies.go:7-13`), installed with
  `domain=chat.google.com, path=/` (`Go:session.go:71-87`), matching `Py:http_utils.py:39-44,63-67`. Login-side
  domain disambiguation hints are a nice addition (`Go:cookies.go:22-24`, `conn:login.go:53-81`).
- **Unquoted values — mostly OK with a caveat:** Python needs `quote_cookie=False` (`Py:http_utils.py:58-62`).
  Go's `net/http` sends jar cookies unquoted *unless* the value contains a space or comma (then it re-quotes in
  `sanitizeCookieValue`). Google's 5 cookie values don't normally contain those characters, so this is latent
  rather than broken — but it is not the verbatim guarantee aiohttp's flag gives.
- **Rotation readback — ABSENT (blocker):** there is no equivalent of `Session.get_auth_cookies()`
  (`Py:http_utils.py:87-92`) anywhere; `Session` never exposes the jar, `Cookies.UpdateValues`
  (`Go:cookies.go:26-32`) is only used to ingest user input at login (`conn:login.go:96-98`), and
  `UserLoginMetadata.Cookies` is persisted once at login (`conn:login.go:120-122`) and never refreshed. Doc 01
  §1.2: "The Go bridge must round-trip rotated cookie values to the DB or sessions die early."
- **XSRF scrape + refresh — PRESENT:** `/mole/world` with the hardcoded `hs` blob, `WIZ_global_data` regex,
  `qwAQke == "AccountsSignInUi"` → not-logged-in, `SMqcke` → token (`Go:client.go:120-163` vs
  `Py:client.py:499-539`); ≤1/24h refresh in `Connect` with `lastTokenRefresh` initialized to `-86400`
  (`Go:client.go:72,94-102` vs `Py:client.py:67,140-149`); token sent as `x-framework-xsrf-token`
  (`Go:api.go:21-23`). Two divergences: the not-logged-in error is untyped (§1.5), and Go sends a correctly
  spelled `referer` header (`Go:client.go:131`) where Python deliberately mimics the web client's misspelled
  `refer` (`Py:client.py:515-518`) — probably harmless, but it breaks the mimicry.
- **Host allowlist — PRESENT:** `FetchRaw` rejects hosts not matching `\.google\.com$`
  (`Go:session.go:145` vs `Py:http_utils.py:249-252`); regex is recompiled per call (perf nit). `Connection:
  Keep-Alive` set (`Go:session.go:166`). UA version rewrite to Chrome/Firefox 114 present
  (`Go:session.go:20-21,64-69` vs `Py:http_utils.py:23-30,69-77`).
- **BUG — `NewSession` arguments swapped at the only call site:** signature is
  `NewSession(cookies, proxyURL, userAgent)` (`Go:session.go:50`) but `NewClient` calls
  `NewSession(cookies, userAgent, os.Getenv("HTTP_PROXY"))` (`Go:client.go:64`). Today the connector always
  passes `userAgent=""` (`conn:login.go:37,100`), so the swap is latent — but any non-empty user agent becomes
  the proxy URL (`url.Parse` accepts a UA string without error — verified; it parses as a path-only URL, breaking
  all requests via `http.ProxyURL`), and setting `HTTP_PROXY` makes the proxy value the User-Agent while no proxy
  is used. Net effect vs maugclib: per-user browser UA (doc 01 §1.1) and proxy support (`Py:client.py:93-95`)
  are both effectively unimplemented.
- **TLS:** Python's `ssl=False` is *not* replicated — `TLSClientConfig: nil` means normal verification despite
  the misleading comment "equivalent to ssl=False" (`Go:session.go:93`). This matches doc 01's recommendation.
- **Retries:** `Fetch` retries 3× (`Go:session.go:16-17,107-137`) like `Py:http_utils.py:20,136-163`, but also
  retries non-200 responses (`Go:session.go:124-127`) where Python raises immediately after a successful read
  (`Py:http_utils.py:165-172`) — a 401 gets hammered 3×, and the final error loses the status code.
- `NewClient` returns `nil` (not an error) if session construction fails (`Go:client.go:64-68`) → guaranteed
  nil-pointer panic at first use.

---

## 4. RPC coverage (`Go:api.go` vs `Py:client.py:682-814`)

Wire format is faithful: `POST {gcBaseURL}/api/{endpoint}?c=<counter>&rt=b&alt=proto&key=<API_KEY>`, binary
protobuf body, `X-Goog-Encode-Response-If-Executable: base64`, xsrf header, and the same `RequestHeader{WEB,
2440378181258, spam_room_invites_level=FULLY_SUPPORTED}` (`Go:api.go:19-93`, `Go:client.go:23-27,82-88` vs
`Py:client.py:28-31,103-109,582-666`). Hardcoded API key identical (`Go:client.go:25`). Nits: `c` counter
increments without the mutex (`Go:api.go:27`) and `xsrfToken` is read unlocked (`Go:api.go:21`) — data races
under concurrent sends; counter reset on connect is present (`Go:client.go:95`).

Coverage of the 16 `proto_*` API wrappers + 1 stream sender:

| maugclib (`Py:client.py`) | Go (`Go:api.go`) | Status |
|---|---|---|
| proto_get_user_presence (:682) | — | **MISSING** |
| proto_get_members (:691) | `GetMembers` (:104) | present (builds MemberIds from plain user IDs) |
| proto_paginated_world (:700) | `paginatedWorld` (:124, private; via `Client.Sync` `Go:client.go:255-257`) | present — but hardcodes `FetchFromUserSpaces` + `EXCLUDE_GROUP_LITE` and supports no world-section paging |
| proto_get_self_user_status (:710) | `getSelfUserStatus` (:95, private; via `GetSelf` `Go:client.go:241-253`) | present |
| proto_get_group (:722) | `GetGroup` (:161) | present |
| proto_mark_group_read_state (:730) | `MarkGroupReadstate` (:185) | present |
| proto_create_topic (:738) | `CreateTopic` (:137) | present |
| proto_create_message (:746) | `CreateMessage` (:143) | present |
| proto_update_reaction (:754) | `UpdateReaction` (:173) | present |
| proto_delete_message (:762) | `DeleteMessage` (:155) | present |
| proto_edit_message (:769) | `EditMessage` (:149) | present |
| proto_set_typing_state (:776) | `SetTypingState` (:179) | present |
| proto_catch_up_user (:783) | — | **MISSING** (no user-level gap recovery possible) |
| proto_catch_up_group (:790) | `CatchUpGroup` (:167) | present (used for backfill, `conn:portal.go:48`) |
| proto_list_topics (:797) | — | **MISSING** (no topic backfill) |
| proto_list_messages (:804) | — | **MISSING** (no message backfill) |
| proto_send_stream_event (:811) | `channel.sendStreamEvent` (`Go:channel.go:216-255`, private) | partial — only reachable as the initial ping; no public wrapper |

**12 of 16 API wrappers present.** The Go wrappers take caller-built request protos and inject the header
(design difference, fine); maugclib's high-level helpers (`send_message` topic-vs-thread logic, `react`,
`update_read_timestamp`, `mark_typing`, local_id echo-suppression generation — `Py:client.py:323-497`) live in
`conn:handlematrix.go` instead and are out of scope here, but note there is no `read_with_max_size` equivalent
anywhere.

---

## 5. Upload / download

- **Resumable upload — PRESENT, faithful:** `UploadFile` (`Go:api.go:191-235`) does the two-step
  start/upload+finalize dance with the exact `x-goog-upload-*` headers, plain `group_id` query param, empty
  `alt=` + `key`, reads `x-goog-upload-url`, and base64-decodes the binary `UploadMetadata` response —
  matching `Py:client.py:275-321` step for step.
- **Redirect-following download — PRESENT but DIVERGENT/BUGGY:** `DownloadAttachment` (`Go:client.go:259-277`)
  splits `.google.com` (cookie-ful, via `FetchRaw` with redirects disabled) from other hosts (cookie-less),
  handling 301/302/307/308, echoing `Py:client.py:182-236`. But:
  - **Bug:** when a `.google.com` URL returns a *non-redirect* (e.g. the final 200 from `chat.google.com`), the
    authenticated response is discarded (and its body leaked) and the code falls through to
    `return http.Get(urlStr)` (`Go:client.go:276`) — re-fetching the same URL **without cookies**, which for
    attachment URLs returns a login redirect instead of the file.
  - No 10-hop redirect cap (unbounded recursion, `Go:client.go:271` vs `Py:client.py:205-206`).
  - No `max_size` enforcement / `FileTooLargeError` (`Py:client.py:238-273` has no Go counterpart).
  - The cookie-less hop uses bare `http.Get` — no User-Agent, no proxy, and no host discipline.
  - Returns the raw `*http.Response`; mime/filename extraction (Content-Disposition) is left to callers
    (`_reference/googlechat-megabridge/pkg/msgconv/from-gchat.go:102`).

---

## 6. Proto

- **Complete in structure, but converted proto2 → proto3:** `Go:proto/googlechat.proto:1` declares
  `syntax = "proto3"` vs `Py:googlechat.proto:2` `proto2`. Message/enum counts are identical (173 messages,
  21 enums in both), all oneofs preserved (20 in each, same lines), and a full `diff` modulo the stripped
  `optional` keywords shows only four substantive deltas:
  - `FormatMetadata.font_color`: `int32` → `uint32` (`Py:googlechat.proto:898` vs `Go:proto/googlechat.proto:899`).
    Wire varints for negative ARGB colors will decode to huge positive values instead of negatives.
  - Three enums gained a `UNSPECIFIED = 0` entry to satisfy proto3 (`Go:proto/googlechat.proto:1180`
    `ReactionUpdateType`, `:1322` `ListTopicsRequest.FetchOptions`, `:1439` `ReactionEventType`); original
    values (ADD=1/REMOVE=2, USER=1…) are preserved, so no renumbering hazard.
  - **The real cost is proto3 implicit presence:** no `optional` keywords are used anywhere (0 occurrences),
    so every scalar loses has-bits. Zero values (`false`, `0`, `""`, enum 0) can neither be distinguished from
    unset on receive nor emitted on send — `pblite`'s serializer skips `!ref.Has(field)`
    (`serialize.go:136-137`) and binary marshaling omits zeros. Works for today's call sites (all set fields are
    non-zero) but is a semantic time bomb for a reverse-engineered schema whose ground truth is proto2
    (doc 01 §7: field numbers + presence are the ground truth).
- Generated with plain `protoc --go_out` (`Go:proto/build.sh:1-2`), single 887KB `googlechat.pb.go`, plus a
  one-line alias `MetadataAssociatedValue` (`Go:proto/extra.go`).
- **`split_event_bodies` — PRESENT and faithful:** `Client.SplitEventBodies` clones the event per embedded body,
  overwriting `Body`/`Type` from `EventBody.event_type` and yielding the primary body first
  (`Go:client.go:211-239` vs `Py:client.py:565-580`). `noop` heartbeats ignored (`Go:client.go:178-180`).
  One latent panic: the type-assertion failure path indexes `array[0]` on a nil slice (`Go:client.go:166-169`),
  and a failed `array[0]` assertion logs but does not return (`Go:client.go:183-186`).

---

## 7. Overall assessment

**Character:** a *close-but-shallow port* — the author demonstrably worked from `maugclib` (identical constants,
choreography, even comments), but stopped before lifecycle hardening and left debug scaffolding (`log.Printf` of
full message payloads at `Go:channel.go:369,410`, `Go:api.go:25,54`; `NOTE(skip)` markers in
`conn:client.go:218,234`) and commented-out load-bearing logic (max-age recycle, typed NotLoggedIn error).

Present and trustworthy (≈): register/SID/ack/ping choreography, UTF-16 code-unit counting basis, API wire
format + 12/16 RPCs, resumable upload, xsrf refresh, proto schema completeness, split_event_bodies, shared pblite
for the happy path.

Broken or absent (the reasons this cannot run unattended): 90s poll death + fire-and-forget Listen (channel
dead in ~8 min), no cookie-rotation persistence, chunk-parser split-multibyte corruption, SIDInvalid never
re-registering or surfacing, unordered event fan-out, no catch_up_user/list_topics/list_messages, download
cookie-less refetch bug, no max-age recycle, pblite trailing-dict/permissiveness gaps, NewSession arg swap.

**Completeness estimate: ~60%** of maugclib's load-bearing client behaviors, with the caveat that the missing
40% is concentrated in exactly the parts that make a long-lived bridge viable (channel survival, gap recovery,
credential rotation, ordering). Fork-vs-greenfield implication: the wire-choreography knowledge embedded here is
real and reusable, but essentially every module needs surgery; treating `gchatmeow` as a reference while
rewriting `channel.go`/`session.go` is more realistic than patching it incrementally.
