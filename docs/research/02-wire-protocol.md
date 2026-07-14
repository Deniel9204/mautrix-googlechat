# Research 02 — Google Chat Wire Protocol (`googlechat.proto`) Inventory

Sources (all paths relative to `/Users/danielagoston/utmshared/Projects/git/generalclaude/mautrix/_reference/googlechat-python/` unless absolute):

| File | Role |
|---|---|
| `maugclib/googlechat.proto` (2552 lines) | Hand-written, reverse-engineered proto2 schema for the private Google Chat ("Dynamite") API |
| `maugclib/pblite.py` (177 lines) | pblite (protojson / JS-array) encoder/decoder used **only** for the realtime channel |
| `maugclib/client.py` (814 lines) | RPC wrappers (`proto_*` methods), upload/download, stream event dispatch |
| `maugclib/channel.py` (495 lines) | BrowserChannel long-polling transport for the realtime event stream |
| `maugclib/parsers.py` (49 lines) | GroupId ↔ string conversion + µs timestamp helpers |
| `maugclib/event.py` (60 lines) | Generic observer/event-emitter utility (NOT wire protocol; unrelated to proto `Event`) |
| `mautrix_googlechat/portal.py`, `user.py`, `puppet.py`, `formatter/*` | The consumers that show which proto fields actually matter |

---

## 1. Overall proto structure

- **Syntax:** `proto2` (`googlechat.proto:3`). Almost every field is `optional`; there are no `required` fields, and `repeated` is used normally.
- **No `package` declaration and no `option go_package`** — the file compiles as-is with protoc (the repo ships generated `googlechat_pb2.py`), but protoc-gen-go will need a `go_package` option or an `M` mapping flag added.
- **Counts:** 173 top-level `message`s + 67 nested messages; 21 top-level `enum`s + 65 nested enums.
- **Provenance:** header comment (`googlechat.proto:6-12`) says it was reverse-engineered from the Android/iOS Gmail and Google Chat apps; original message names had a `DYNProto` prefix (stripped). Unknown/unimplemented fields are left as comments throughout — these commented field numbers are load-bearing documentation (they explain gaps in numbering) and should be preserved in the Go port.
- **Real `oneof`s are used** (despite the comment saying they were commented out for protobuf-c; only *some* were commented). Active oneofs that matter: `GroupId.Id` (:971), `MessageParentId.Parent` (:735), `Annotation.Metadata` (:917), `TypingContext.Context` (:946), `Group.ThreadedModel` (:1010), `Emoji.Content` (:61), `Member.Profile` (:148), `MemberId.Id` (:155), `UploadMetadata.Payload` (:866), `Event.RevisionType` (:1743), `Attachment.Type` (:719), `EventBody.Type` (:1694).
- **Informal namespaces (all flat, distinguished by name prefix):**
  - Core chat entities & RPCs: `User*`, `Group*`, `Topic*`, `Message*`, `Annotation`, `Membership*`, `World*`, `CatchUp*`, `StreamEvents*`, `Event`.
  - `JAddOns*` (~25 messages, lines 224–707): Google Workspace add-on *card* UI widgets (buttons, grids, form actions). Reachable via `Attachment.add_on_data`. **The Python bridge never renders these** — safe to carry over unmodified and ignore.
  - `ComGoogleRtcMeetingsV1*` / `MeetingSpace` (lines 1826–1997): Google Meet call metadata. Bridge only reads `MeetingSpace.meeting_url`.
  - SafeHtml/SafeUrl wrappers: `Html` (html = field **2**, :709), `Url` (url = field **3**, :831), `TrustedResourceUrl` (resource_url = field **4**, :837) — note the odd single-field messages with non-1 field numbers; they mirror Google's internal `private_do_not_access_...wrapped_value` types. Do not "fix" the numbers.

### Transport summary (context for everything below)

Two distinct wire encodings are in play:

1. **RPCs** — `POST https://chat.google.com/u/0/api/{endpoint}?c={counter}&rt=b&alt=proto&key={API_KEY}` with `Content-Type: application/x-protobuf`; request body is **standard binary protobuf**, response body is parsed directly with `ParseFromString` (`client.py:582-615`). The header `X-Goog-Encode-Response-If-Executable: base64` is sent (`client.py:650-653`) but the response is treated as raw binary. An XSRF token header `x-framework-xsrf-token` is required, scraped from `WIZ_global_data["SMqcke"]` on `GET /u/0/mole/world` (`client.py:499-539`).
2. **Realtime channel** — BrowserChannel long-poll on `https://chat.google.com/u/0/webchannel/events` (register on `.../webchannel/register`, `channel.py:40,260-301`). Inbound data arrays are **pblite** (JSON arrays positionally indexed by field number) decoded into `StreamEventsResponse` (`client.py:545-553`). Outbound `StreamEventsRequest` is pblite-encoded to JSON and sent as form field `req0_data` (`channel.py:303-341`).
3. **File upload** — resumable upload to `https://chat.google.com/uploads?group_id={id}` using `x-goog-upload-*` headers; the final response body is **base64 of a binary `UploadMetadata` proto** (`client.py:275-321`).
4. **Attachment download** — `GET https://chat.google.com/api/get_attachment_url?url_type=DOWNLOAD_URL|FIFE_URL&attachment_token=...` then follow redirects manually re-attaching cookies on `*.google.com` hosts (`portal.py:1470-1480`, `client.py:182-236`).

---

## 2. Core entities (with the fields the bridge actually uses)

### 2.1 Identifiers

```
UserId    { id(1) string, type(2) UserType HUMAN|BOT }                 // :15-24
DmId      { dm_id(1) string }                                          // :962
SpaceId   { space_id(1) string }                                       // :966
GroupId   { oneof Id { space_id(1) SpaceId; dm_id(3) DmId } }          // :970-975
TopicId   { group_id(3) GroupId, topic_id(2) string }                  // :1110-1113  (note: topic_id is field 2, group_id is 3)
MessageParentId { oneof Parent { topic_id(4) TopicId } }               // :734-738
MessageId { parent_id(1) MessageParentId, message_id(2) string }       // :740-743
MembershipId { member_id(1), space_id(2), group_id(3) }                // :161-165
```

- The Python bridge canonicalizes `GroupId` into a string: `"dm:{dm_id}"` / `"space:{space_id}"` (`parsers.py:26-49`, `id_from_group_id`/`group_id_from_id`). Portal keys, DB rows and the `receiver` scoping (`dm:` portals are per-user, `space:` portals global — `portal.py:1652-1653`) all rely on this format. The Go port should keep an equivalent canonical string for `PortalKey`.
- A message is fully addressed by *(group, topic, message)*. In flat rooms the first message of a "topic" has `message_id == topic_id`; replies-in-thread share the topic_id (see §5 threading).

### 2.2 `User` (:26-42)

Fields consumed by `puppet.py:145-211` (`Puppet.update_info`): `user_id.id`, `name(2)`, `avatar_url(3)`, `email(4)`, `first_name(5)`, `last_name(6)`. Name fallback chain: `name` → `first_name + last_name` → `email`. `UserType` (HUMAN=0/BOT=1) is used to split DM member lists (`portal.py:333-342`). Everything else (`deleted`, `gender`, `block_relationship`) is ignored.

### 2.3 `Group` (:977-1028) and `WorldItemLite` (:2280-2332)

Chat metadata arrives in two shapes; `portal.py` defines `ChatInfo = Union[googlechat.WorldItemLite, googlechat.GetGroupResponse]` (`portal.py:82`).

`Group` fields used (`portal.py:244-291`):
- `name(2)` — room name (only when `HasField("name")`).
- `group_details(37)` → `GroupDetails { description(1), guidelines(2) }` (:1802-1805) — room topic from `description`.
- **`ThreadedModel` oneof: `flat_group(26)` / `threaded_group(27)`** (:1008-1013) — `threads_only = group_meta.HasField("threaded_group")` (`portal.py:261`).
- `flat_threads_enabled(44)` — `threads_enabled = flat_threads_enabled or threads_only` (`portal.py:265`). These two booleans drive the entire threading strategy (§5).
- Not used but present: `group_type(22)` (ROOM/HUMAN_DM/BOT_DM), `avatar_url(28)`, `retention_settings(16)`, `attribute_checker_group_type(33)` (`SharedAttributeCheckerGroupType` :2543-2552 distinguishes 1:1 DM / group DM / flat room / threaded room / post room).

`WorldItemLite` fields used (`user.py:623-641`, `portal.py:247-344`):
- `group_id(1)`, `group_revision(2).timestamp`, `sort_timestamp(3)`.
- `read_state(4)` → `GroupReadState` (:1054-1077): `blocked(15)`, `hide_timestamp(9)`, `membership_state(16)` (must be `MEMBER_JOINED` to sync), `last_read_time(2)`, `joined_users(23)` (list of `UserId`, used to populate DM participants).
- `room_name(5)` (only when `HasField`), `dm_members(6).members` (repeated `UserId`), `group_lite(7).group_details.description`.
- `flat_group(14)` / `threaded_group(15)` — **plain optional fields here, not a oneof** (unlike `Group`); same `HasField("threaded_group")` check (`portal.py:253-261`).
- `flat_threads_enabled(27)`.

### 2.4 `Message` (:745-790)

Fields consumed (`portal.py:1337-1453` `handle_googlechat_message`, `portal.py:1228-1260` `handle_googlechat_edit`, `formatter/from_googlechat.py`):
- `id(1)`: `id.message_id` = message key; `id.parent_id.topic_id.topic_id` = thread/topic key (`portal.py:1379`).
- `creator(2).user_id.id` — sender.
- `create_time(3)` — **microseconds**; Matrix ts = `create_time // 1000` (`portal.py:1360`).
- `last_edit_time(17)`, `last_update_time(4)` — edit dedup timestamp (`portal.py:1236`).
- `text_body(10)` — plain text; formatting layered on via annotations.
- `annotations(11)` — repeated `Annotation` (§2.5, §5).
- `local_id(14)` — client-generated echo-dedup id; the bridge sends `mautrix-googlechat%{random uint64}` and drops own echoes (`portal.py:908-909, 1341`; hangups used `hangups%...`, `client.py:440`).
- `message_type(28)`: `USER_MESSAGE`/`SYSTEM_MESSAGE`. A SYSTEM_MESSAGE with exactly 1 annotation is a room-update or membership-change notice (`portal.py:1362-1374`).
- `reply_to(37)` → `ReplyToMessage` (:792-802): the bridge reads `reply_to.id.message_id` to map GC quote-replies to Matrix replies (`portal.py:1391-1396`).
- Present but unused by bridge: `attachments(15)` (`Attachment` :718-725 — card/add-on payloads, *not* file uploads; files come as `UploadMetadata` annotations), `reactions(21)` (`Reaction` :727-732 — aggregate counts; live reactions come via `MessageReactionEvent`), `message_state(20)`, retention fields, topic-summary fields (`last_reply_time(5)` etc.).

### 2.5 `Annotation` (:901-937) — the single most important message

```
Annotation {
  type(1) AnnotationType            // :1774-1800
  start_index(2) int32              // UTF-16 code-unit offset into text_body
  length(3) int32                   // UTF-16 code-unit length
  local_id(9), unique_id(19)
  chip_render_type(20) ChipRenderType { UNKNOWN|RENDER|RENDER_IF_POSSIBLE|DO_NOT_RENDER }  // :908-914
  server_invalidated(13) bool
  oneof Metadata {
    user_mention_metadata(5)  UserMentionMetadata    // :2028-2042
    format_metadata(8)        FormatMetadata         // :881-899
    slash_command_metadata(15) SlashCommandMetadata  // :2044-2057
    drive_metadata(4)         DriveMetadata          // :809-828
    youtube_metadata(6)       YoutubeMetadata        // :842-846
    url_metadata(7)           UrlMetadata            // :848-863
    upload_metadata(10)       UploadMetadata         // :865-879
    membership_changed(11)    MembershipChangedMetadata // :1999-2026
    video_call_metadata(12)   VideoCallMetadata      // :1993-1997
    room_updated(14)          RoomUpdatedMetadata    // :1807-1824
  }
}
```

**Critical: `start_index`/`length` are in UTF-16 code units.** The Python bridge converts text to a surrogate-pair representation before slicing (`formatter/util.py:6-16` `add_surrogate`/`del_surrogate`, `from_googlechat.py:36`, `from_matrix/__init__.py:36-37`). A Go port must index by UTF-16 units (like mautrix-go's Telegram/Slack entity handling), not bytes or runes.

`chip_render_type` semantics: `DO_NOT_RENDER` = inline formatting entity (bridge processes it as text markup, `from_googlechat.py:133`); `RENDER` = separately-rendered chip (e.g. file upload, sent by bridge with `RENDER`, `portal.py:1102-1108`).

Metadata payloads and their consumption:
- **`FormatMetadata { format_type(1), font_color(2) int32 }`** — `FormatType`: `BOLD(1) ITALIC(2) STRIKE(3) SOURCE_CODE(4) MONOSPACE(5) HIDDEN(6) MONOSPACE_BLOCK(7) UNDERLINE(8) FONT_COLOR(9) BULLETED_LIST(10) BULLETED_LIST_ITEM(11) CLIENT_HIDDEN(12)` (:882-896). Mapping to HTML in `from_googlechat.py:153-179` (see §5).
- **`UserMentionMetadata { id(1) UserId, invitee_info(3), type(2), display_name(4) }`** — `Type`: `INVITE(1) UNINVITE(2) MENTION(3) MENTION_ALL(4) FAILED_TO_ADD(5)`. `MENTION` → Matrix pill; `MENTION_ALL` → `@room` (`from_googlechat.py:182-195`). Outgoing mentions set `type=MENTION`, `id`, `display_name` (`from_matrix/gc_message.py:95-110`).
- **`UrlMetadata`**: `title(1)`, `snippet(2)`, `image_url(3)`, `url(7).url`, `should_not_render(9)`, `int_image_height(10)/int_image_width(11)`, `mime_type(12)`. Used for link formatting (`from_googlechat.py:180-181`), URL previews (`formatter/gc_url_preview.py:92-120`), and image-URL attachments (`portal.py:1486-1495`). Outgoing links: `Annotation(type=URL, url_metadata.url.url=...)` (`gc_message.py:111-120`).
- **`UploadMetadata { attachment_token(1) [oneof Payload], content_name(3), content_type(4), local_id(6), cloned_drive_id(9) }`** — incoming file attachments: bridge builds `get_attachment_url` from `attachment_token` (FIFE_URL for `image/*`, DOWNLOAD_URL otherwise, `portal.py:1470-1485`); outgoing: the whole proto returned by the upload endpoint is attached verbatim as an annotation with `type=UPLOAD_METADATA, chip_render_type=RENDER` (`portal.py:1102-1108`).
- **`DriveMetadata`** — bridge appends `https://drive.google.com/open?id={id}` to text (`portal.py:1504-1511`) and builds previews from `title`/`thumbnail_url` (`gc_url_preview.py:110-113`).
- **`YoutubeMetadata { id(1) }`** — appends `https://www.youtube.com/watch?v={id}` (`portal.py:1512-1519`).
- **`VideoCallMetadata { meeting_space(1) MeetingSpace }`** — appends `meeting_space.meeting_url` to text (`portal.py:1496-1503`).
- **`MembershipChangedMetadata`** — `Type`: `INVITED(1) JOINED(2) ADDED(3) REMOVED(4) LEFT(5) BOT_ADDED(6) BOT_REMOVED(7) KICKED_DUE_TO_OTR_CONFLICT(8) ROLE_UPDATED(9)`; `affected_members(3)` (repeated `MemberId`), `initiator(2)`. Consumed from SYSTEM_MESSAGEs to bridge join/leave/kick/invite (`portal.py:1281-1335`).
- **`RoomUpdatedMetadata`** — `rename_metadata(6).new_name`, `group_details_metadata(7).new_group_details.description`. Consumed from SYSTEM_MESSAGEs for room name/topic changes (`portal.py:1262-1279`).
- `SlashCommandMetadata` — defined, never consumed by the bridge.

### 2.6 Reactions

`Emoji { oneof Content { unicode(1) string } }` (:60-64). `UpdateReactionRequest` (:1174-1183) carries `message_id`, `emoji`, `type ADD(1)|REMOVE(2)`. Incoming: `MessageReactionEvent` (:1430-1440) with `message_id(1)`, `emoji(2)`, `user_id(3)`, `timestamp(4)` (µs), `type(5) ADD|REMOVE` (`portal.py:1166-1208`; note Matrix-side variation-selector normalization via `variation_selector.add/remove`).

---

## 3. The realtime event stream

### 3.1 Envelope

- `StreamEventsResponse { event(1) Event, sample_id(2), clock_sync_response(3) }` (:1630-1634). Decoded from each BrowserChannel data array via pblite (`client.py:552-553`); the literal array `["noop"]` is a keep-alive (`client.py:547`).
- `Event` (:1636-1748):
  ```
  group_id(1) GroupId
  type(3) EventType
  body(4) EventBody              // first body
  user_id(5) UserId
  oneof RevisionType { user_revision(6) WriteRevision; group_revision(7) WriteRevision }
  bodies(8) repeated EventBody   // 2nd..nth bodies
  ```
  **Multi-body flattening:** one `Event` may carry N bodies; `Client.split_event_bodies` (`client.py:565-580`) yields one Event per body, copying the envelope and setting `type = body.event_type`. The Go port must replicate this before dispatch (it is also applied to `CatchUpResponse.events` during backfill, `portal.py:492-504`).
- Revision bookkeeping: `user_revision.timestamp` → per-user revision (`user.py:681-682`); `group_revision.timestamp` → per-portal revision, stored after handling (`portal.py:530-531`), and used as `CatchUpRange.from_revision_timestamp` on reconnect.

### 3.2 `Event.EventType` — all 50 values (:1639-1691)

| Value | Name | Meaning / bridge handling |
|---|---|---|
| 0 | UNKNOWN | — |
| 1/2 | USER_ADDED_TO_GROUP / USER_REMOVED_FROM_GROUP | membership; bridge relies on SYSTEM_MESSAGE annotations instead |
| 3 | GROUP_VIEWED | own read marker; **handled** → `mark_read` (`portal.py:556-557`) |
| 4 | TOPIC_VIEWED | not handled |
| 5 | GROUP_UPDATED | `GroupUpdatedEvent(new, old, update_type CREATED/UPDATED/DELETED/UNDELETED)` (:1473-1484); body decodable but **not handled** by the Python bridge (name changes come via SYSTEM_MESSAGE) |
| 6 | MESSAGE_POSTED | **handled** → new message (`portal.py:539-543`) |
| 7 | MESSAGE_UPDATED | **handled** → edit (same `message_posted` body; distinguished purely by `evt.type`, `portal.py:540-541`) |
| 8 | MESSAGE_DELETED | **handled** → redaction (`MessageDeletedEvent{message_id, timestamp}` :1442-1448) |
| 9-11 | TOPIC_MUTE_CHANGED / USER_SETTINGS_CHANGED / GROUP_STARRED | not handled |
| 12 | WEB_PUSH_NOTIFICATION | `WebPushNotificationEvent` (:1486-1503) decodable, not handled |
| 13/14 | GROUP_UNREAD_SUBSCRIBED_TOPIC_COUNT_UPDATED / INVITE_COUNT_UPDATED | not handled |
| 15 | MEMBERSHIP_CHANGED | `MembershipChangedEvent{new_membership, prior_state, prior_role}` (:1457-1461) decodable, **not handled** (bridge uses SYSTEM_MESSAGE annotation path) |
| 16-19 | GROUP_HIDE_CHANGED / DRIVE_ACL_FIX_PROCESSED / GROUP_NOTIFICATION_SETTINGS_UPDATED / RETENTION_SETTINGS_UPDATED | not handled |
| 20 | TOPIC_CREATED | not handled (new topics arrive as MESSAGE_POSTED too) |
| 21-23 | ON_HOLD_MESSAGE_POSTED / _UPDATED / _PUBLISHED | not handled explicitly; an ON_HOLD body would still be a `message_posted` field and thus bridged as a message |
| 24 | MESSAGE_REACTED | **handled** → `MessageReactionEvent` (`portal.py:544-545`) |
| 25 | USER_STATUS_UPDATED_EVENT | `UserStatusUpdatedEvent{user_status}` (:84-86) decodable, not handled |
| 26-28 | GROUP_RETENTION_SETTINGS_UPDATED / USER_WORKING_HOURS_UPDATED / MESSAGE_SMART_REPLIES | not handled |
| 29 | TYPING_STATE_CHANGED | **handled** → typing notif; NB `Event.group_id` is empty for these — portal routing uses `body.typing_state_changed.context.group_id` (`user.py:676-677`) |
| 30-32 | GROUP_DELETED / BLOCK_STATE_CHANGED / CLEAR_HISTORY | not handled |
| 33 | SESSION_READY | not handled (no body type) |
| 34/35 | GROUP_SORT_TIMESTAMP_CHANGED / GSUITE_INTEGRATION_UPDATED | not handled |
| 36 | READ_RECEIPT_CHANGED | **handled** → per-user read receipts (`ReadReceiptChangedEvent{group_id, read_receipt_set}` :1463-1466; `ReadReceiptSet{enabled, read_receipts[]{read_time_micros(2), user(3)}}` :1366-1374; `portal.py:1587-1598`) |
| 37-50 | MARK_AS_UNREAD, GROUP_NO_OP, INVALIDATE_GROUP_CACHE, USER_NO_OP, INVALIDATE_USER_CACHE, USER_DENORMALIZED_GROUP_UPDATED, USER_PRESENCE_SHARED_UPDATED, NOTIFICATIONS_CARD_UPDATED, USER_HUB_AVAILABILITY_UPDATED, USER_OWNERSHIP_UPDATED, SHARED_DRIVE_CREATE_SCHEDULED, SHARED_DRIVE_UPDATED, MESSAGE_PERSONAL_LABEL_UPDATED, USER_QUOTA_EXCEEDED | not handled |

### 3.3 `Event.EventBody` decodable oneof members (:1693-1737)

Only these ten body fields are uncommented in the schema (the rest exist only as comments): `group_viewed(3)`, `group_updated(5)`, `message_posted(6)` (`MessageEvent{message(1), last_message_in_topic_time(4), prev_revision_time(5), is_head_message(6)}` :1423-1428), `web_push_notification(10)`, `membership_changed(14)`, `message_deleted(18)`, `message_reaction(22)`, `user_status_updated(23)`, `typing_state_changed(26)` (`TypingStateChangedEvent{state, user_id, context, start_timestamp_usec}` :1450-1455), `read_receipt_changed(33)`. Plus scalar `event_type(12)` and `trace_id(20)`.

The bridge dispatches on `body.HasField(...)`, not on `evt.type` (except MESSAGE_UPDATED vs MESSAGE_POSTED) — `portal.py:538-560`.

### 3.4 Client → server stream messages

`StreamEventsRequest` (:1507-1517): `ping_event(2)`, `clock_sync_request(3)`, `group_subscription_event(8)`, plus platform/client-info fields. The only one actually sent is the initial `PingEvent{state=ACTIVE, application_focus_state=FOCUS_STATE_FOREGROUND, client_interactive_state=INTERACTIVE, client_notifications_enabled=true}` right after acquiring a new SID (`channel.py:347-360, 445`). `GroupSubscriptionEvent` and `ClockSyncRequest` are unused. `Client.proto_send_stream_event` (`client.py:811-814`) is exposed but never called by the bridge.

---

## 4. RPC request/response pairs

All via `Client._gc_request(endpoint, req, resp)` (`client.py:582-615`) → `POST /u/0/api/{endpoint}?c=N&rt=b&alt=proto&key=...`, binary protobuf both ways. Every request embeds `RequestHeader` (:122-139) — the bridge sends `client_type=WEB(3)`, `client_version=2440378181258`, `client_feature_capabilities.spam_room_invites_level=FULLY_SUPPORTED` (`client.py:103-109`). `RequestHeader` is field **100** in most requests (field **1** in `PaginatedWorldRequest`, :2356).

| Endpoint | Request → Response (proto lines) | Wrapper (`client.py`) | Bridge usage |
|---|---|---|---|
| `paginated_world` | `PaginatedWorldRequest` (:2355) → `PaginatedWorldResponse` (:2372) | :700 | Chat-list sync; sent with `fetch_from_user_spaces=true`, `fetch_options=[EXCLUDE_GROUP_LITE]`, optional `world_section_requests=[{page_size}]` (`user.py:613-622`). Reads `world_items` (`WorldItemLite`) |
| `get_self_user_status` | `GetSelfUserStatusRequest` (:97) → `GetSelfUserStatusResponse` (:101) | :710 | Own `user_status.user_id.id` = own gcid (`user.py:443-449`) |
| `get_members` | `GetMembersRequest` (:180) → `GetMembersResponse` (:186) | :691 | User profile fetch (batched `MemberId{user_id}`); reads `members[].user` (`user.py:450-485`) |
| `get_group` | `GetGroupRequest` (:2165) → `GetGroupResponse` (:2183) | :722 | Portal info; `fetch_options=[MEMBERS, INCLUDE_DYNAMIC_GROUP_NAME]` (`user.py:513-520`); reads `group`, `memberships[].id.member_id.user_id.id`, `group_revision` |
| `create_topic` | `CreateTopicRequest` (:1140) → `CreateTopicResponse` (:1152) | :738 | Send message to flat room / new thread: `group_id`, `text_body`, `annotations`, `local_id`, `history_v2=true`, `message_info.accept_format_annotations=true`, `message_info.reply_to` (`client.py:460-472`). Response: `topic.id.topic_id` (message id), `topic.create_time_usec` (`portal.py:1047-1048`) |
| `create_message` | `CreateMessageRequest` (:1158) → `CreateMessageResponse` (:1168) | :746 | Send reply inside a thread: `parent_id.topic_id{group_id, topic_id}`, `text_body`, `annotations`, `local_id`, `message_info` (`client.py:441-458`). Response: `message.id.message_id`, `message.create_time` |
| `edit_message` | `EditMessageRequest` (:1201) → `EditMessageResponse` (:1209) | :769 | Edit: full `MessageId`, new `text_body`+`annotations` (`client.py:385-411`); resp `message.last_edit_time` used for dedup (`portal.py:874`) |
| `delete_message` | `DeleteMessageRequest` (:1189) → `DeleteMessageResponse` (:1194) | :762 | Redaction (`client.py:367-383`) |
| `update_reaction` | `UpdateReactionRequest` (:1174) → `UpdateReactionResponse` (:1185) | :754 | Reactions add/remove (`client.py:338-365`) |
| `set_typing_state` | `SetTypingStateRequest` (:952) → `SetTypingStateResponse` (:958) | :776 | Typing; context is `group_id` or `topic_id` oneof (`client.py:477-497`) |
| `mark_group_readstate` | `MarkGroupReadstateRequest` (:2437) → `MarkGroupReadstateResponse` (:2443) | :730 | Read marker; `last_read_time` in µs (`client.py:323-336`, `user.py:684-690`) |
| `list_topics` | `ListTopicsRequest` (:1310) → `ListTopicsResponse` (:1301) | :797 | Initial backfill: `group_id`, `page_size_for_topics`; reads `topics[].replies[0]`, `topics[].topic_read_state.thread_created_usec`, `group_revision` (`portal.py:406-448`) |
| `list_messages` | `ListMessagesRequest` (:1330) → `ListMessagesResponse` (:1339) | :804 | Thread-reply backfill: `parent_id.topic_id`, `page_size` (`portal.py:433-441`) |
| `catch_up_group` | `CatchUpGroupRequest` (:2135) → `CatchUpResponse` (:2151) | :790 | Missed-event backfill per group: `range{from,to revision ts}`, `page_size`, `cutoff_size`; response `events[]` (same `Event` type as stream) + `status` COMPLETED/PAGINATED/ABORTED_* (`portal.py:450-490`) |
| `catch_up_user` | `CatchUpUserRequest` (:2143) → `CatchUpResponse` | :783 | wrapper exists, **unused by bridge** |
| `get_user_presence` | `GetUserPresenceRequest` (:213) → `GetUserPresenceResponse` (:220) | :682 | wrapper exists, **unused by bridge** |

Defined in the proto but with **no client wrapper at all** (available if the Go port wants them): `GetUserStatusRequest/Response` (:88-95), `GetSelfUserStatusRequest` (used), `CreateGroupRequest/Response` (:1256-1277), `CreateDmRequest/Response` (:1279-1299), `ListMembersRequest/Response` (:1346-1364), `RemoveMembershipsRequest/Response` (:2379-2399), `CreateMembershipRequest/Response` (:2421-2435), `HideGroupRequest/Response` (:2402-2411), `UpdateGroupRequest/Response` (:2478-2499), `BlockEntityRequest/Response` (:2501-2514), `SetPresenceSharedRequest/Response` (:2448-2456), `SetDndDurationRequest/Response` (:2458-2476), `SetCustomStatusRequest/Response` (:2516-2528), `MarkTopicReadstate`-style ops, `GetServerTimeRequest/Response` (:2121-2127). Endpoint names for these are unknown/unverified (likely the snake_case of the message name, matching the observed pattern).

Revision plumbing: mutating responses return `WriteRevision{timestamp, prev_timestamp}` (:2530), reads return `ReadRevision{timestamp}` (:2535); requests can pass `ReferenceRevision` (:2539) as `*_not_older_than`. The bridge persists group revision per portal and user revision per user.

---

## 5. Formatting & threading model

### 5.1 Text formatting = flat annotation spans over `text_body`

There is no markup language on the wire. A message is a plain-text `text_body` plus a list of `Annotation` spans (`start_index`, `length` in **UTF-16 code units**) that may overlap and nest arbitrarily.

**GC → Matrix** (`formatter/from_googlechat.py`):
- Annotations with `chip_render_type != DO_NOT_RENDER` are excluded from text formatting (they are attachments/chips) (:133).
- Overlapping spans are normalized by sorting `(start_index, -length)` with a special order for lists (`BULLETED_LIST` before `BULLETED_LIST_ITEM` before others at the same offset) and by splitting spans that cross a boundary (`_normalize_annotations`, :69-113). Rendering is then recursive interval nesting (`_gc_annotations_to_matrix`, :116-201).
- Mapping: BOLD→`<strong>`, ITALIC→`<em>`, UNDERLINE→`<u>`, STRIKE→`<del>`, MONOSPACE→`<code>`, MONOSPACE_BLOCK→`<pre><code>`, FONT_COLOR→`<font color=#...>` (int32 color converted via `(rgb_int + 2**31) & 0xFFFFFF`, :170-173), BULLETED_LIST→`<ul>`, BULLETED_LIST_ITEM→`<li>`, HIDDEN→drop text; `url_metadata`→`<a href>`; `user_mention_metadata` MENTION→matrix.to pill, MENTION_ALL→`@room`. `SOURCE_CODE` and `CLIENT_HIDDEN` are not rendered (skip).
- Newlines become `<br/>` after rendering (:52).

**Matrix → GC** (`formatter/from_matrix/`): mautrix HTML parser produces `GCEntity` objects wrapping real `Annotation` protos (`gc_message.py:67-133`); inline code→MONOSPACE, pre→MONOSPACE_BLOCK, links→URL annotation with `url_metadata.url.url`, mentions→USER_MENTION with `id` + `display_name`, `@room`→MENTION_ALL. All outgoing format annotations set `chip_render_type=DO_NOT_RENDER`, and requests set `message_info.accept_format_annotations=true` — without that flag the server would strip formatting.

### 5.2 Threading: topics vs. messages, threaded vs. flat rooms

Google Chat's data model is **Group → Topic → Message** everywhere; what differs per room is how topics behave:

1. **Threaded ("topics") spaces** — `Group.threaded_group` set (`ThreadedModel` oneof). Every conversation is a topic; replies go inside the topic. `threads_only=true` in the bridge.
2. **Flat rooms & DMs** — `Group.flat_group` set. Each message is its own topic (message_id == topic_id for the head message); no visible threading. `threads_only=false`.
3. **Flat rooms with in-line threads enabled** — `Group.flat_threads_enabled=true` (:1027). Flat room semantics plus optional per-message threads. `threads_enabled=true`.

Sending (`client.py:413-475`, `portal.py:880-943`):
- New top-level message → `create_topic` (`CreateTopicRequest.group_id`).
- Reply in a thread → `create_message` with `parent_id.topic_id.topic_id = thread_id`.
- Quote-reply (non-thread) → either RPC plus `message_info.reply_to = SendReplyTarget{id: full MessageId, create_time: µs ts of target}` (:1130-1138; `client.py:423-438`) — note `create_time` of the *target* message is required, so the bridge stores GC timestamps per message.
- Matrix→GC routing (`portal.py:891-907`): with `threads_enabled`, Matrix thread parent → GC thread (`thread_parent.gc_parent_id or thread_parent.gcid`); a reply to a threaded message reroutes into the thread; without thread support, Matrix threads degrade to GC quote-replies.
- Reaction/edit/delete targets always build the full `MessageId{parent_id.topic_id{group_id, topic_id: thread_id or message_id}, message_id: message_id or thread_id}` (`client.py:346-411`) — i.e. the topic id falls back to the message id for unthreaded messages. The Go DB schema needs to keep `(message_id, parent/topic_id, group, timestamp µs)` per message (Python schema: `DBMessage.gcid/gc_parent_id/gc_chat/gc_receiver/timestamp/index`).

Backfill semantics differ by room type (`portal.py:406-448`): threaded rooms page by topics (`list_topics`) then fetch replies per topic (`list_messages`); flat rooms treat each topic's `replies[0]` as the message and only fetch replies when `topic_read_state.thread_created_usec > 0` (a flat-thread exists).

---

## 6. Recommended strategy for the Go port

### 6.1 Reuse the .proto with protoc-gen-go — yes, with small edits

The file is valid proto2 and protoc already compiles it (Python/pyi outputs are checked in). For Go:

1. Copy `googlechat.proto` verbatim into the Go repo (e.g. `pkg/gchatmeow/proto/googlechat.proto`) and add only:
   ```proto
   option go_package = "go.mau.fi/mautrix-googlechat/pkg/proto/gchatproto";
   ```
   (No `package` statement is needed; adding one would change nothing on the wire — protobuf wire format has no package names — but adding it is safe if desired for symbol hygiene.)
2. proto2 is fully supported by `google.golang.org/protobuf`. Generated getters (`GetTextBody()`, etc.) neatly replace Python's implicit defaults, and presence checks (`msg.TextBody != nil`, or `proto.HasField` via reflection) replace `HasField`. The bridge relies heavily on presence semantics (`HasField("threaded_group")`, `HasField("url_metadata")`, oneof dispatch in `EventBody`) — proto2 `optional` gives Go pointer fields, so presence is preserved. **Do not convert the file to proto3**; proto3 would lose explicit presence for scalars like `flat_threads_enabled` and change default handling.
3. Top-level enum value names collide only within C++ scoping rules, which Go doesn't care about (Go generates `UserType_HUMAN`, `GroupSupportLevel_UNSUPPORTED`, …). The file already avoids the collisions that would break protoc itself.
4. Keep the commented-out fields as comments — they document real field numbers for future reverse engineering.

### 6.2 pblite: needed only for the realtime channel — and field numbers vs array indexes is the whole game

Concretely, per `pblite.py`:
- A message is a JSON array where **array index i (0-based) holds field number i+1**. Missing fields are `null` padding (`pblite.py:172-175`).
- **Trailing-object extension:** if the last element of the array is a JSON *object*, it is a sparse `{ "field_number": value }` map used for high field numbers (`pblite.py:96-104`). `Message.reply_to` is field 37 and `WorldItemLite` uses fields up to 27 — the trailing-dict path *will* be hit in practice; the decoder must support it. (The Python encoder never emits the dict form; padding with nulls is accepted by the server.)
- **int64 arrives as JSON string** and must be parsed to int (`pblite.py:36-37`); all the µs timestamps (`create_time`, `timestamp`, revisions) are int64, so this is mandatory, not an edge case.
- `bytes` fields are base64 strings (`pblite.py:34-35, 161-169`) — only `MeetingSpace.CallInfo.CseInfo.wrapped_key` is `bytes`, so low priority.
- Unknown field numbers must be skipped silently (`pblite.py:109-121`); Google adds fields constantly.
- The decoder must be **permissive**: wrong-typed values are logged and skipped, not fatal (`pblite.py:39-45, 59-70`).
- `decode(..., ignore_first_item=True)` exists for legacy Hangouts responses but the Google Chat stream does **not** use it (`client.py:553` calls plain `decode`).

Where each encoding is used in the Go port:

| Surface | Encoding |
|---|---|
| `/u/0/api/*` RPCs (both directions) | binary protobuf (`proto.Marshal`/`proto.Unmarshal`) — **no pblite** |
| `/u/0/webchannel/events` inbound arrays → `StreamEventsResponse` | pblite decode |
| `/u/0/webchannel/events` outbound `StreamEventsRequest` (`req0_data` form field) | pblite encode |
| `/uploads` final response → `UploadMetadata` | base64 → binary protobuf |

**Prior art to reuse:** mautrix-gmessages' `libgm` contains a Go pblite implementation built on `protoreflect` (`github.com/mautrix/gmessages`, package `libgm/pblite`) that handles exactly this array-index↔field-number mapping including int64-as-string. Port/vendor that rather than writing a new one (verify current import path and whether it supports the trailing-dict sparse form — hangouts-era pblite; if not, add it).

A generic Go implementation is straightforward with `protoreflect`: walk `msg.Descriptor().Fields()`, map `FieldDescriptor.Number()` → array index, recurse for `MessageKind`, `strconv.ParseInt` for 64-bit kinds when the JSON value is a string, base64 for `BytesKind`, and populate oneofs by simply setting whichever member field number appears (pblite has no special oneof encoding; the Python code relies on protobuf's last-set-wins).

### 6.3 Other Go-design consequences found in this inventory

- **UTF-16 indexing** for annotation offsets — reuse the UTF-16 offset handling patterns from mautrix-go (Slack/Telegram formatters do the same); never use byte or rune offsets.
- **Microsecond timestamps everywhere** (`create_time`, `last_read_time`, reaction `timestamp`, revisions). Matrix wants ms; the reply-to path (`SendReplyTarget.create_time`) needs the original µs value, so store µs in the message DB (the Python bridge migrated to µs storage in `db/upgrade/v10_store_microsecond_timestamp.py`).
- **`split_event_bodies` flattening** must run on both live stream events and `catch_up_group` events before dispatch.
- **Event routing quirk:** typing events carry the group only inside `body.typing_state_changed.context`, not in `Event.group_id` (`user.py:674-677`).
- **Echo suppression** relies on `local_id` round-tripping through `Message.local_id`.
- **`MessageInfo.accept_format_annotations=true`** is required on create/edit or formatting is dropped (`client.py:407-409, 453-455, 467-469`).
- **`CreateTopicRequest.history_v2=true`** is always set when sending (`client.py:465`).
- The `X-Goog-Encode-Response-If-Executable: base64` header is set on RPCs but responses are parsed as raw binary (`client.py:650-653, 609-610`) — replicate the header as-is; only the uploads endpoint response is actually base64.

---

## Appendix A: messages safe to deprioritize in the Go port

Everything under `JAddOns*` (cards/widgets, ~480 lines), `MeetingSpace` internals beyond `meeting_url`, `WebPushNotification*`, `MobileLocalNotification`/`AndroidLocalNotification`/`IosLocalNotification`, `WorldSection`/`WorldFilter` (the bridge sends only `page_size`), Dnd/CustomStatus/presence RPCs, `CreateGroup`/`CreateDm` (bridge cannot create chats), `BlockEntity`, `HideGroup`, `UpdateGroup`. They should still compile from the shared .proto (protoc generates them for free), just no Go client code is needed initially.
