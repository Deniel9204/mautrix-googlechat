# Python Bridge (mautrix-googlechat) — Full Feature Surface

Research artifact for the Go/bridgev2 reimplementation.
Source analyzed: `/Users/danielagoston/utmshared/Projects/git/generalclaude/mautrix/_reference/googlechat-python/`
(bridge package `mautrix_googlechat/`, client library `maugclib/`). Version at analysis time: v0.5.2 lineage (CHANGELOG.md).

All file paths below are relative to the repo root unless absolute. Line numbers are from the checked-in reference copy.

---

## 1. Feature matrix (verified against code, not just ROADMAP.md)

Legend: **✔ supported**, **◐ partial**, **✘ absent**.

### Matrix → Google Chat

| Capability | Status | Code anchor | Notes |
|---|---|---|---|
| Plain text | ✔ | `portal.py:1051` `_handle_matrix_text` | `Client.send_message` (`maugclib/client.py:413`) — CreateTopic (new topic) or CreateMessage (thread reply) |
| Notices (`m.notice`) | ✔ | `portal.py:915` | Treated same as text |
| Emotes (`m.emote`) | ✘ | `portal.py:924` | Falls into `NotImplementedError(f"Unsupported msgtype ...")` |
| Formatting (HTML→annotations) | ✔ | `formatter/from_matrix/__init__.py:27` | See §4 |
| Files / images / video / audio | ✔ | `portal.py:1081` `_handle_matrix_media` | Any `msgtype.is_media`; upload via resumable protocol (`maugclib/client.py:275` `upload_file`), sent as `UPLOAD_METADATA` annotation with empty text. **Captions are lost** (only `message.body` used as filename) |
| Stickers | ✘ | — | No `EventType.STICKER` handling anywhere |
| Location | ✘ | `portal.py:924` | NotImplementedError path |
| Edits (text only) | ✔ | `portal.py:840` `handle_matrix_edit` | Non-text edits dropped with "non-text edit" (`portal.py:854`); target looked up by `get_edit()`; per-message edit-dedup timestamp stored (`portal.py:874`) |
| Redactions (messages) | ✔ | `portal.py:800` `handle_matrix_redaction` → `client.delete_message` (`maugclib/client.py:367`) | |
| Redactions (reactions) | ✔ | `portal.py:816` | `client.react(..., remove=True)` |
| Reactions | ✔ | `portal.py:763` `handle_matrix_reaction` | Emoji variation selectors stripped (`variation_selector.remove`, `portal.py:766`); duplicate reactions dropped; fake µs timestamp (`portal.py:781`, TODO in code) |
| Replies | ✔ | `portal.py:886-907` | `reply_to`/`reply_to_ts` in `SendReplyTarget` (`maugclib/client.py:423`); reply fallback relations ignored (`is_falling_back` check `portal.py:887`) |
| Threads | ✔ | `portal.py:891-907` | Matrix native threads → GChat threads when `threads_enabled`; reply-to-a-threaded-message rerouted into the thread; if chat has no threads, thread events downgraded to replies |
| Typing | ✔ | `matrix.py:98` `handle_typing` → `portal.py:1133` `handle_matrix_typing` → `client.mark_typing` (`maugclib/client.py:477`) | Diffs started/stopped typers against `_typing` set; also force-stops typing before sending text (`portal.py:1061`) |
| Read receipts | ✔ | `matrix.py:106` `handle_read_receipt` → `user.py:684` `User.mark_read` (`MarkGroupReadstateRequest`, ms→µs `*1000`) | |
| Presence | ✘ | `matrix.py:90-96` | Handler body commented out; gated on config key `bridge.presence` which is **not in the example config nor copied in config.py** (would KeyError if presence events ever arrived) |
| Membership (invite/kick/leave→GC) | ✘ | — | Only `handle_matrix_leave` (`portal.py:1123`): a leave from a *direct* portal deletes+cleans the portal; group leaves just logged |
| Room metadata (name/topic/avatar→GC) | ✘ | — | No handlers |
| Join by inviting ghost (DM creation) | ✘ | `user.py:181` `get_portal_with` returns `None` with comment "DMs need to be explicitly created on Google Chat" |

### Google Chat → Matrix

| Capability | Status | Code anchor | Notes |
|---|---|---|---|
| Text | ✔ | `portal.py:1337` `handle_googlechat_message` | Timestamp µs→ms (`portal.py:1360`) |
| Formatting (annotations→HTML) | ✔ | `formatter/from_googlechat.py:29` | See §4 |
| Media attachments (upload_metadata) | ✔ | `portal.py:1465` `_preprocess_annotations`, `portal.py:1525` `_process_googlechat_attachment` | Images fetched via `FIFE_URL` w/ `sz=w10000-h10000` for full resolution, other files via `DOWNLOAD_URL`, both through `https://chat.google.com/api/get_attachment_url` (`portal.py:1471-1485`); authed download with manual redirect following (`maugclib/client.py:182`); size-capped by homeserver media config; encrypted in-place when portal is E2BE |
| External image URLs (url_metadata) | ✔ | `portal.py:1486-1495` | Downloaded unauthenticated; `text/html` results discarded (`portal.py:1547`) |
| Google Drive links | ◐ | `portal.py:1504-1511` | Not downloaded — appended to message text as `https://drive.google.com/open?id=...` + Beeper link preview |
| Google Meet links (video_call_metadata) | ◐ | `portal.py:1496-1503` | Meeting URL appended to text body |
| YouTube links | ◐ | `portal.py:1512-1519` | Appended as URL + rich Beeper preview via oEmbed (`formatter/gc_url_preview.py:199`) |
| URL previews | ✔ (Beeper ext) | `formatter/gc_url_preview.py:92` `gc_previews_to_beeper` | Emitted as `com.beeper.linkpreviews` on the text event; thumbnails reuploaded (and encrypted if needed), module-level upload/oembed caches |
| Multi-part messages | ✔ | `portal.py:1439-1451` | One GChat msg → N Matrix events (text at index 0, then attachments); DB rows share `gcid` with `index` column |
| Edits (text only) | ✔ | `portal.py:1228` `handle_googlechat_edit` | Only if stored msgtype is `m.text` and new `text_body` non-empty; comment: "Figuring out how to map multipart message edits to Matrix is hard, so don't even try" (`portal.py:1249`) |
| Deletions | ✔ | `portal.py:1210` `handle_googlechat_redaction` | Redacts *all* index rows of the gcid |
| Reactions add/remove | ✔ | `portal.py:1166` `handle_googlechat_reaction` | `variation_selector.add` on emoji; remove falls back to bridge-bot redaction on MForbidden |
| Threads | ✔ | `portal.py:1378-1389` | Sets `thread_parent` + `last_event_in_thread` (queried via `Message.get_last_in_thread`, `db/message.py:73`); in `threads_only` rooms every topic-starter becomes a thread root (`portal.py:1403-1409`) |
| Replies | ✔ | `portal.py:1390-1396` | `evt.reply_to` → `content.set_reply` |
| Typing | ✔ | `portal.py:1600` `handle_googlechat_typing` | 6 s timeout; own-user typing in DM suppressed unless own puppet joined. (ROADMAP.md says unsupported — **ROADMAP is stale**, code handles `typing_state_changed` at `portal.py:550`) |
| Read receipts | ✔ | `portal.py:1587` `handle_googlechat_read_receipts` + `group_viewed` events (`portal.py:556`) | `mark_read` maps ts → closest earlier message (`db/message.py:106` `get_closest_before`) |
| Presence | ✘ | — | `proto_get_user_presence` exists in client (`maugclib/client.py:682`) but is never called |
| Membership changes | ✔ | `portal.py:1281` `handle_googlechat_membership_change` | JOINED/INVITED/ADDED/BOT_ADDED/LEFT/REMOVED/BOT_REMOVED/KICKED_DUE_TO_OTR_CONFLICT all handled, arriving as `SYSTEM_MESSAGE` with `MEMBERSHIP_CHANGED` annotation (`portal.py:1370`). (ROADMAP says unsupported — stale) |
| Room name change | ✔ | `portal.py:1262` `handle_googlechat_room_update` (`rename_metadata`) — SYSTEM_MESSAGE with `ROOM_UPDATED` annotation |
| Room description/topic change | ✔ | `portal.py:1272` (`group_details_metadata`) → `m.room.topic` |
| Room avatar | ✘ | — | `portal.avatar_mxc`/`avatar_set` exist in DB but are never written (GChat groups have no avatars) |
| History backfill | ✔ | §5 | Initial + catch-up (revision-based) |
| Disappearing messages | ✘ | — | No retention/disappearing handling anywhere |
| User metadata (name/avatar) | ✔ | `puppet.py:145` `update_info` | Name template, sha256-deduplicated avatar reupload (`puppet.py:245`), Beeper contact info on hungryserv (`puppet.py:157`) |

### Misc

| Capability | Status | Notes |
|---|---|---|
| Multi-user | ✔ | Per-user `User`+`Client`; spaces are shared portals |
| Automatic portal creation at startup/login | ✔ | `user.py:610` `sync()`, capped by `bridge.initial_chat_sync` |
| Portal creation on incoming message | ✔ | `portal.py:1351-1355` |
| Double puppeting | ✔ | Auto-enable via `login_shared_secret_map` (`user.py:548-553`); `bridge.invite_own_puppet_to_pm` for echo of own messages from other clients (`portal.py:1151` `_bridge_own_message_pm`) |
| E2BE (end-to-bridge encryption) | ✔ | Full mautrix-python crypto config incl. verification levels & key deletion (example-config.yaml:130-201) |
| Relay mode | ✘ | No relaybot; permission levels only `user`/`admin` (`config.py:76-91`) |
| Prometheus metrics | ✔ | `metrics.enabled`; histograms/gauges in `user.py:46-48`, `portal.py:84`, long-poll counters in `maugclib/channel.py:47-59` |
| Bridge status notices / bridge state | ✔ | notice room (`user.py:193`), `BridgeStateEvent` pushes, `status_endpoint`, message send checkpoints (`portal.py:946-1014`), Beeper `com.beeper.message_send_status` events (`portal.py:1016`) |
| Delivery receipts / error reports | ✔ | `bridge.delivery_receipts`, `bridge.delivery_error_reports` |

---

## 2. Login flow

### 2.1 User-facing commands (`mautrix_googlechat/commands/auth.py`)

Only four bridge-specific commands exist (plus mautrix-python framework built-ins like `help`, `login-matrix`, `logout-matrix`, `ping-matrix`):

| Command | Line | Behavior |
|---|---|---|
| `login-cookie` | `commands/auth.py:74-104` | Management-room only, no prior auth needed. User pastes a **JSON object of raw Google cookies**. Bridge immediately **redacts the user's message** (`auth.py:87`) to limit credential exposure, lowercases keys, builds `Cookies(**data)` and calls `User.connect(cookies)`. Replies "Those cookies don't seem to be valid" on `NotLoggedInError`, else waits on `name_future` and replies `Successfully logged in as {name} <{email}> ({gcid})`. |
| `logout` | `auth.py:29-40` | `User.logout(is_manual=True)` + disables double-puppet (`puppet.switch_mxid(None, None)`) |
| `ping` | `auth.py:43-59` | Calls `get_self()`; replies with name/email/gcid |
| `set-notice-room` | `auth.py:62-71` | Marks current room as notice room |

### 2.2 Cookie set (the credential)

`maugclib/http_utils.py:39`:

```python
class Cookies(NamedTuple):
    compass: str
    ssid: str
    sid: str
    osid: str
    hsid: str
```

i.e. the user extracts `COMPASS`, `SSID`, `SID`, `OSID`, `HSID` cookies for `chat.google.com` from a logged-in browser session (documented at docs.mau.fi, referenced in the command help text). The user's browser **User-Agent is also stored** (`user` table `user_agent` column, upgrade v09) and replayed with the Chrome/Firefox version numbers rewritten to current ones (`http_utils.py:69-75`). Cookies are stored as JSON in `user.cookies` (`db/user.py:81`) and **re-saved after connect if Google rotates them** (`user.py:279-283`).

Session validation happens in `Client.refresh_tokens` (`maugclib/client.py:499`): GET `https://chat.google.com/u/0/mole/world` and scrape `window.WIZ_global_data`; `qwAQke == "AccountsSignInUi"` ⇒ `NotLoggedInError`; otherwise `SMqcke` is the **xsrf token** sent as `x-framework-xsrf-token` on all API calls (`client.py:597`), refreshed every 24 h (`client.py:147`).

### 2.3 Provisioning API (`mautrix_googlechat/web/auth.py`)

Mounted in `__main__.py:56-62`: new API at `{bridge.provisioning.prefix}` (default `/_matrix/provision`), legacy copy under `/login` (`/login/api/...`). Auth: `Authorization: Bearer <bridge.provisioning.shared_secret>` + `?user_id=` query param (`auth.py:81-94`).

| Route | Method | Behavior |
|---|---|---|
| `/v1/whoami` | GET | permissions level, mxid, `{name,email,id,connected}` if client exists (`auth.py:109`) |
| `/v1/login` | POST | Body `{"cookies": {...}, "user_agent": "..."}`; runs `user.connect(cookies, get_self=True)`; returns `{"status":"success","name","email"}` or `{"status":"fail","error"}` (`auth.py:133`) |
| `/v1/logout` | POST | manual logout (`auth.py:103`) |
| `/v1/reconnect` | POST | `user.reconnect()` — force channel restart (`auth.py:127`) |
| legacy `/api/verify`, `/api/authorization`, `/api/logout`, `/api/reconnect`, `/api/whoami` | | same handlers (`auth.py:74-79`) |

There is **no interactive login web page** — the web login UI was removed in v0.3.3 (CHANGELOG.md); a browser extension / manual cookie extraction feeds either the command or this API.

### 2.4 Connection lifecycle (`user.py`)

- `connect()` (`user.py:259`): build `Client` (max_retries=3, backoff base 2), `refresh_tokens()`, fetch own gcid via `GetSelfUserStatus` if unknown (`user.py:441`), persist rotated cookies, spawn `start()` task + hourly `_periodic_sync` task, register client observers.
- `_start()` (`user.py:299`): reconnect loop around `client.connect(max_age=1.5*60*60)` (channel intentionally recycled every 1.5 h; `ChannelLifetimeExpired` restarts silently with `_skip_on_connect`). On `SIDInvalidError` do a small `sync(limit=3)` to catch missed messages (`user.py:326-345`). 401/`invalid_grant` ⇒ automatic logout with `BAD_CREDENTIALS` state. Backoff: reset to 4 s after 60 s of stability, ×1.5 per failure, > 60 s escalates notice to `UNKNOWN_ERROR`.
- `on_connect_later()` (`user.py:526`): get self info (name/email), push `BACKFILLING` state, auto-enable double puppeting, run full `sync()`, push `CONNECTED`.
- `_periodic_sync()` (`user.py:578`): every 60 min `sync(limit=3)`; if anything was backfilled, force channel reconnect (assumes events were missed).
- Transport: BrowserChannel long-polling (`maugclib/channel.py`) — `register` endpoint (`channel.py:259`) obtains `csessionid` from COMPASS `dynamite-ui=` cookie; backward channel chunked arrays are pblite-decoded into `StreamEventsResponse` (`client.py:545`); multi-body events split by `split_event_bodies` (`client.py:566`); forward channel `send_stream_event` exists (`channel.py:303`) but the bridge only uses request/response APIs.

---

## 3. Portal / puppet / user model

### 3.1 IDs

- **Chat ID (`gcid`)**: string-prefixed form of the protobuf `GroupId` — `"dm:<dm_id>"` or `"space:<space_id>"` (`maugclib/parsers.py:26-49`, `id_from_group_id`/`group_id_from_id`).
- **Portal key** = `(gcid, gc_receiver)` (DB PK, `db/upgrade/v00_latest_revision.py:34-50`):
  - **spaces**: `gc_receiver = ""` — one shared portal for all bridge users (`portal.py:1653`: `receiver = "" if gcid.startswith("space:")`).
  - **DMs (incl. group DMs)**: `gc_receiver = <owner user's gcid>` — per-user portals.
- `is_dm` = `gcid.startswith("dm:")` (`portal.py:201`); `is_direct` = DM **and** `other_user_id` set (1:1) (`portal.py:197`); group DMs (`dm:` with >2 members) are DMs but not direct.
- **Threading flags** stored per portal: `threads_only` (protobuf `threaded_group` present — old "threaded space") and `threads_enabled` (`flat_threads_enabled or threads_only`) (`portal.py:261-272`); surfaced in bridge info state event as `fi.mau.googlechat.threads_only` / `threads_enabled` (`portal.py:609-610`).
- **User ID (`gcid` on User/Puppet)**: numeric Google user id string (`UserId.id`).
- **Message key** = `(gcid, gc_chat, gc_receiver, index)`; `gc_parent_id` holds the GChat topic id when the message is inside a thread. Timestamps stored in **microseconds** (upgrade v10).
- **Reaction key** = `(emoji, gc_sender, gc_msgid, gc_chat, gc_receiver)`.

### 3.2 Portal room creation

`_create_matrix_room` (`portal.py:638`): power levels — sender PL 50 in DMs, main intent 100, `notifications.room: 0`; bridge info state events (`m.bridge` + `uk.half-shot.bridge`, state key `net.maunium.googlechat://googlechat/{gcid}`, `portal.py:593`); optional `m.room.encryption`; `m.federate` from `bridge.federate_rooms`; **room_version "11"**; name only when `set_dm_room_metadata` (governed by `bridge.private_chat_portal_meta`: default/always/never, `portal.py:209-214`); topic = group description. Initial backfill runs inside the backfill lock right after creation (`portal.py:685-734`).

- **Main intent**: DM-direct → the other user's ghost `default_mxid_intent`; everything else → bridge bot (`portal.py:1615-1624`).
- **DM room name** = puppet displayname (`portal.py:281-284`).
- Participant sync (`portal.py:331`): DM member list from `WorldItemLite.read_state.joined_users` (split HUMAN/BOT) or `dm_members`; groups from `GetGroupResponse.memberships`; removes own user from 2-member DMs to derive `other_user_id`; kicks ghost members no longer in the group ("User is not in group", `portal.py:365-373`).
- **Per-portal event queue**: incoming stream events for a portal go through `asyncio.Queue` + single dispatcher task that waits for backfill to finish (`portal.py:506-536` `queue_event`/`_event_dispatcher_loop`); portal `revision` is advanced after each event with `group_revision`.

### 3.3 Puppets (ghosts)

- MXID template `bridge.username_template` = `googlechat_{userid}` (`puppet.py:100`); displayname template `{full_name} (Google Chat)` with `{first_name}/{last_name}/{email}` variables and fallback chain full name → first+last → email (`puppet.py:179-201`).
- Avatar: GChat `avatar_url` (scheme forced to https) downloaded, sha256-hashed; re-upload skipped when hash unchanged even if URL changed (`puppet.py:219-266`).
- Beeper/hungryserv contact info blob (`com.beeper.bridge.identifiers = ["mailto:<email>"]` etc.) set once (`puppet.py:157-177`).
- Double puppeting: `intent_for(portal)` returns the ghost intent for messages in one's own DM portal, or during backfill when `bridge.backfill.invite_own_puppet` (`puppet.py:136-141`).

### 3.4 Users

`User` (db: `mxid, gcid, cookies, user_agent, notice_room, revision`) — `revision` is the **user-level** revision watermark updated from `user_revision` on stream events (`user.py:681-682`). In-memory caches of `GetGroupResponse` and `googlechat.User` protos with locks (`user.py:76-79`, `get_users` batch fetch `user.py:466`, `get_group` revision-aware cache `user.py:486`).

`sync()` (`user.py:610`): `PaginatedWorldRequest(fetch_from_user_spaces=True, fetch_options=[EXCLUDE_GROUP_LITE])`; skips chats that are `blocked`, hidden (`hide_timestamp > 0`), or `membership_state != MEMBER_JOINED` (`user.py:630-635`); sorts by `sort_timestamp` desc; creates portals for the newest `bridge.initial_chat_sync` chats plus updates/backfills every chat that already has a room; DM member infos prefetched in one `GetMembers` call; finishes with `update_direct_chats()` (m.direct sync, `user.py:667`).

---

## 4. Formatter

### 4.1 GChat annotations → Matrix HTML (`formatter/from_googlechat.py`)

Entry: `googlechat_to_matrix(source, evt, portal)` (line 29). Key mechanics:

- **UTF-16 offsets**: GChat `start_index`/`length` count UTF-16 code units. Python strings are decomposed with `add_surrogate`/`del_surrogate` (Telethon-derived, `formatter/util.py:6-16`) so indices line up; final HTML converted back. The Go implementation gets this "for free" only if it operates on UTF-16 units explicitly.
- **Normalization** (`_normalize_annotations`, line 84): annotations are sorted (offset asc, length desc; bulleted-list wrappers before items, `_annotation_key` line 69) and overlapping annotations are **split** so nesting is strictly hierarchical.
- Annotations with `chip_render_type != DO_NOT_RENDER` are skipped by the formatter (they are attachments, rendered separately) (line 133).
- Recursive rendering (`_gc_annotations_to_matrix`, line 116) mapping `FormatMetadata.format_type`:
  - `BOLD`→`<strong>`, `ITALIC`→`<em>`, `UNDERLINE`→`<u>`, `STRIKE`→`<del>`, `MONOSPACE`→`<code>`, `MONOSPACE_BLOCK`→`<pre><code>`, `BULLETED_LIST`→`<ul>`, `BULLETED_LIST_ITEM`→`<li>`, `HIDDEN`→text dropped, `FONT_COLOR`→`<font color='#...'>` where `color = (rgb_int + 2**31) & 0xFFFFFF` (line 170-173).
  - `url_metadata` → `<a href='...'>`.
  - `user_mention_metadata`: `MENTION_ALL` → literal `@room`; otherwise resolve gcid → bridge `User` (real Matrix user) or ghost MXID, pill `<a href='https://matrix.to/#/{mxid}'>`; **mention text replaced with the target's current Matrix room displayname** when the target is a real user, so Matrix-side pings work (lines 183-195, added in v0.4.0).
- Newlines become `<br/>` and `body` is regenerated from the HTML via `parse_html` (lines 51-53).
- **Bug**: line 45 `if annotations:` tests the `from __future__ import annotations` feature object (always truthy) instead of `evt.annotations` — the HTML path always runs, even for plain messages.
- **Beeper URL previews**: `com.beeper.linkpreviews` built from `url_metadata` (title/snippet/image), `drive_metadata` (Drive open/thumbnail URLs), `youtube_metadata` (oEmbed lookup + thumbnail) with image reupload/encryption (`formatter/gc_url_preview.py:92-221`).

### 4.2 Matrix HTML → GChat annotations (`formatter/from_matrix/`)

Entry: `matrix_to_googlechat(content)` (`from_matrix/__init__.py:27`) → `(text, list[googlechat.Annotation])`. Plain-text messages bypass parsing entirely unless the body contains `@room` (then body is HTML-escaped and parsed so the room mention converts) (lines 30-34).

`MatrixParser` subclass of mautrix's `BaseMatrixParser` (`from_matrix/parser.py:36`):

- Entities are built directly as protobuf `googlechat.Annotation` objects (`gc_message.py:67-133` `GCEntity`): `FORMAT_DATA` annotations with `chip_render_type=DO_NOT_RENDER` for bold/italic/strike/underline/`MONOSPACE`/`MONOSPACE_BLOCK`/color/lists; `USER_MENTION` annotations (`type=MENTION`, `UserId`); `URL` annotations for links.
- Font color: `rgb_int = (int(hex) | 0x7F000000) - 2**31` (`parser.py:40-47`, inverse of the incoming transform).
- User pills: only ghost MXIDs convert (gcid parsed from the MXID template); mentions of Matrix-native users produce a mention annotation with empty user id — TODO comments at `parser.py:50-51`.
- Room pills dropped (`parser.py:55-57`); spoilers flattened; email entities dropped (`gc_message.py:169-171`).
- `<ul>` → `BULLETED_LIST`/`BULLETED_LIST_ITEM` annotations; `<ol>` falls back to text numbering (`parser.py:62-71`).
- Headers `h1-h6` → `#`-prefixed bold text (`parser.py:73-77`); blockquote → `> ` line prefixes (`parser.py:79-83`).
- `@room` (plain text scan, whitespace-preserving contexts excluded) → literal text `@all` with a `MENTION_ALL` annotation (`parser.py:85-98`).
- Same surrogate-pair add/remove dance as the other direction (`from_matrix/__init__.py:36-37`).
- Send path sets `MessageInfo(accept_format_annotations=True)` (`maugclib/client.py:453,467`).

---

## 5. Backfill

Two modes, both driven per portal from `Portal.backfill` (`portal.py:385`), wrapped in `backfill_lock` + `NotificationDisabler` (double-puppet notification muting, config `bridge.backfill.disable_notifications`); on completion stores latest revision and marks read up to `read_state.last_read_time`.

### 5.1 Initial backfill (`_initial_backfill`, `portal.py:406`)

- `ListTopicsRequest` with `page_size_for_topics` = `bridge.backfill.initial_thread_limit` (threads-only spaces, default 10) or `initial_nonthread_limit` (flat chats/DMs, default 100).
- Topics processed oldest-first (re-sorted by `sort_time`); the first reply of each topic is handled as a normal incoming message; for threads (`threads_only` or `topic_read_state.thread_created_usec > 0`) replies are fetched with `ListMessagesRequest` (`page_size` = `initial_thread_reply_limit`, default 500) and each handled in order.
- Revision watermarks: threads-only spaces store the group revision immediately ("can't continue from the middle anyway", `portal.py:422-424`); flat chats advance per topic and store the group revision at the end.
- Messages are inserted with real timestamps (timestamp massaging — requires ghost/double-puppet senders; `bridge.backfill.invite_own_puppet` default true exists for this).

### 5.2 Catch-up backfill (`_catchup_backfill`, `portal.py:450`)

- Requires a stored portal `revision`; otherwise skipped.
- Loops `CatchUpGroupRequest` with `CatchUpRange(from_revision_timestamp=self.revision, to_revision_timestamp=latest)`, `page_size` = `missed_event_page_size` (100), `cutoff_size` = `missed_event_limit` (5000); continues while status is `PAGINATED`, aborts on any status other than `PAGINATED`/`COMPLETED`.
- Returned events are real stream `Event`s; each is split (`split_event_bodies`) and fed into the normal `handle_event` dispatcher (`portal.py:492-504`), so edits/reactions/deletions/read receipts during downtime replay correctly.
- Triggered from `sync()` whenever `info.group_revision.timestamp > portal.revision` (`user.py:654-659`), from the periodic hourly sync, and after `SIDInvalidError` reconnects.
- Message dedup for backfill overlap: in-memory `_dedup` deque (100 ids), `_local_dedup` for own local ids, and a DB existence check (`portal.py:1340-1350`).

---

## 6. Config surface (`config.py` + `example-config.yaml`)

Beyond standard mautrix-python homeserver/appservice/encryption blocks, the bridge-specific keys (all copied in `config.py:28-74`):

| Key | Default | Meaning |
|---|---|---|
| `homeserver.software` | standard | `hungry` unlocks Beeper contact-info APIs |
| `homeserver.async_media` | false | MSC2246 async uploads (threaded through all upload sites) |
| `metrics.enabled` / `listen_port` | false / 8000 | Prometheus |
| `manhole.*` | disabled | debug REPL |
| `bridge.username_template` | `googlechat_{userid}` | ghost localparts |
| `bridge.displayname_template` | `{full_name} (Google Chat)` | vars: full_name, first_name, last_name, email |
| `bridge.command_prefix` | `!gc` | |
| `bridge.initial_chat_sync` | 10 | portals to auto-create on login/startup (0 disables) |
| `bridge.invite_own_puppet_to_pm` | false | echo own messages sent from other GC clients |
| `bridge.sync_with_custom_puppets` | false | /sync-based ephemeral events via double puppet |
| `bridge.sync_direct_chat_list` | false | maintain m.direct |
| `bridge.double_puppet_server_map`, `double_puppet_allow_discovery`, `login_shared_secret_map` (legacy `login_shared_secret` migrated) | | double puppeting |
| `bridge.update_avatar_initial_sync` | true | (copied but **unused in code** — only read in config) |
| `bridge.encryption.*` | | allow/default/appservice/require, key sharing, deletion policies, verification_levels, rotation |
| `bridge.delivery_receipts` | false | bot read receipt after bridging |
| `bridge.delivery_error_reports` | true | notice in room on failure |
| `bridge.message_status_events` | false | `com.beeper.message_send_status` |
| `bridge.federate_rooms` | true | `m.federate` on created rooms |
| `bridge.backfill.invite_own_puppet` | true | use own ghost during backfill |
| `bridge.backfill.initial_thread_limit` | 10 | topics per threaded space |
| `bridge.backfill.initial_thread_reply_limit` | 500 | replies per thread |
| `bridge.backfill.initial_nonthread_limit` | 100 | messages in flat chats |
| `bridge.backfill.missed_event_limit` | 5000 | catch-up cutoff |
| `bridge.backfill.missed_event_page_size` | 100 | catch-up page |
| `bridge.backfill.disable_notifications` | false | mute during initial backfill |
| `bridge.resend_bridge_info` | false | one-shot re-send of m.bridge events (`__main__.py:64`) |
| `bridge.unimportant_bridge_notices` | false | send non-important notices |
| `bridge.disable_bridge_notices` | false | kill all notices |
| `bridge.private_chat_portal_meta` | default | default/always/never (validated in `config.py:63-64`) |
| `bridge.provisioning.prefix` | `/_matrix/provision` | provisioning mount point |
| `bridge.provisioning.shared_secret` | generate | bearer token (legacy `bridge.web.auth.shared_secret` migrated, `config.py:67-68`) |
| `bridge.permissions` | map | levels: `user`, `admin` only (mxid > domain > `*` precedence, `config.py:82-91`) |

Environment: `HTTP_PROXY` respected for the GChat session (`maugclib/client.py:94`).

---

## 7. Known limitations, bugs, and TODOs found in code

1. **ROADMAP.md is stale** in both directions: GChat→Matrix typing and membership actions ARE implemented (`portal.py:550`, `portal.py:1281`); ROADMAP marks them absent.
2. `formatter/from_googlechat.py:45` — `if annotations:` refers to the `from __future__ import annotations` feature import, always truthy; should be `evt.annotations`. Consequence: every incoming message takes the HTML formatting path.
3. `db/reaction.py:52` `Reaction.get_all_by_gcid` and `db/reaction.py:69` `Reaction.delete_all_by_room` query the **message** table instead of `reaction` (copy-paste bugs; `delete_all_by_room` is invoked from `Portal.delete` — reaction rows are only cleaned up via the FK cascade, and messages get double-deleted harmlessly).
4. `db/message.py:138` `Message.delete()` WHERE clause omits `gc_chat` (`gcid + gc_receiver + index` only) — cross-chat gcid collision would delete the wrong row; PK includes `gc_chat`, so this relies on gcid uniqueness across chats.
5. Matrix reaction timestamps are faked (`portal.py:780-782`, `# TODO real timestamp?`, `# TODO proper locks?`).
6. Media send holds the per-user send lock during the whole download+upload (`portal.py:911-913`, TODO acknowledging it).
7. Edits: non-text edits dropped both directions; multipart GChat edits dropped (`portal.py:853-861`, `1248-1251`).
8. Media captions from Matrix are discarded (body used only as filename, `portal.py:1100`).
9. Unhandled GChat message types produce nothing (`portal.py:1435-1437`, `# TODO send notification`).
10. Oversized attachments silently dropped with a log line (`portal.py:1540-1542`, `# TODO send error message`).
11. Image attachments always use `FIFE_URL` (`portal.py:1476`, `# TODO maybe it should just always use DOWNLOAD_URL?`).
12. Mentions of Matrix-native (non-ghost) users produce empty GChat mentions; displayname suffix not stripped (`formatter/from_matrix/parser.py:50-51` TODOs); room pills unsupported (`parser.py:56`).
13. Presence is dead code and gated on a config key (`bridge.presence`) that doesn't exist in the config schema (`matrix.py:90-96`).
14. `bridge.update_avatar_initial_sync` is copied in config but never read by bridge code.
15. No relay mode, no DM-creation from Matrix (`user.py:181-183` comment: "DMs need to be explicitly created on Google Chat"), no Matrix→GChat membership/metadata operations.
16. Session resume failure without `invalid_grant`/`NotLoggedInError` sends a notice and gives up (`user.py:252-257`, `# TODO retry?`).
17. Client version constant is hard-coded (`maugclib/client.py:105` `client_version=2440378181258`) and the `hs` blob in `refresh_tokens` is hard-coded (`client.py:511-513` TODO); both may need periodic updating.
18. Channel lifetime intentionally capped at 1.5 h (`user.py:310`), with the "disconnecting for no reason after 14 days" bug fixed by that recycling (CHANGELOG v0.5.1).
19. Login is cookie-scrape only; Google can invalidate cookies at any time → `invalid_grant`/401 auto-logout paths exist throughout `user.py` (`_try_init`, `_start`, `_periodic_sync`).
20. `Portal.get_all_by_receiver` filters `gcid LIKE 'dm:%'` (`db/portal.py:73`) — receiver-scoped iteration only ever yields DM portals; spaces are enumerated via `Portal.all()`.

---

## 8. Notes most relevant to the Go/bridgev2 design

- **Portal key model** maps cleanly to bridgev2 `networkid.PortalKey{ID: gcid, Receiver: ...}` with empty receiver for spaces — the Python bridge already implements exactly this split-brain (shared spaces / per-user DMs).
- **Revision watermarks** (user-level and portal-level, microsecond "revision timestamps") are the resync/catch-up primitive; bridgev2's backfill APIs need to carry them (portal metadata + user login metadata).
- **Threads**: portal-level `threads_only`/`threads_enabled` flags decide whether Matrix threads/replies become GChat threads or replies — needs portal metadata + the reply-rerouting logic in the Matrix→remote converter.
- **Multipart messages** (text + N attachments per GChat message id) require bridgev2's multi-part message support (`ConvertedMessage` parts, part IDs = index).
- **UTF-16 annotation offsets** need explicit handling in Go (Python used surrogate padding).
- **Edit dedup** relies on `last_edit_time` comparison; **message dedup** on local_id echo + DB lookup.
- Read receipts map by timestamp → closest earlier message, not by message id.
- The GChat client (login cookies + xsrf refresh + BrowserChannel long-poll + pblite decoding + resumable uploads + authed downloads with manual redirects) is roughly half the porting work; `maugclib` is the spec (channel.py 495 lines, client.py 814 lines, googlechat.proto).
