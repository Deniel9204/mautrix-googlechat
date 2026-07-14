# bridgev2 Framework Catalog (mautrix-go)

Research artifact for the mautrix-googlechat → Go bridgev2 reimplementation.
All paths are relative to `/Users/danielagoston/utmshared/Projects/git/generalclaude/mautrix/_reference/mautrix-go/` unless absolute. Line numbers refer to the checked-out copy (snapshot 2026-07-13).

bridgev2 ("Megabridge") splits a bridge into three components (`bridgev2/unorganized-docs/README.md`):

1. **Network connector** — what *we* write for Google Chat. Implements `NetworkConnector`, `NetworkAPI`, `LoginProcess`, and produces `RemoteEvent`s.
2. **Central bridge module** — `bridgev2` package. Portals, ghosts, dedup, event loop, backfill, commands, etc.
3. **Matrix connector** — `bridgev2/matrix` (appservice-based, incl. e2ee, provisioning, direct media). We do not touch this; `mxmain` wires it up.

---

## 1. Required interfaces for a network connector

### 1.1 `NetworkConnector` (`bridgev2/networkinterface.go:216-264`)

The single object passed to `mxmain.BridgeMain{Connector: ...}`. Full signature:

```go
type NetworkConnector interface {
    // Called once at bridge init; store the *Bridge for later. No I/O allowed.
    Init(*Bridge)
    // Called at startup; non-user-specific startup work (do NOT connect logins here).
    Start(context.Context) error

    // Bridge name/metadata. First call happens BEFORE config load (used to build the
    // example config / bot localpart), so must return hardcoded defaults.
    GetName() BridgeName
    // Metadata struct constructors for portal/ghost/message/reaction/userlogin rows.
    // Called before Init; must be hardcoded.
    GetDBMetaTypes() database.MetaTypes
    // Connector-level (not per-room) capabilities.
    GetCapabilities() *NetworkGeneralCapabilities
    // example config YAML string, config struct pointer, configupgrade.Upgrader.
    GetConfig() (example string, data any, upgrader configupgrade.Upgrader)

    // Fill login.Client with a NetworkAPI. Called under the bridge's global cache
    // lock — must be fast, no network I/O (connect later in NetworkAPI.Connect).
    LoadUserLogin(ctx context.Context, login *UserLogin) error

    // Login flows for the `login` command / provisioning API.
    GetLoginFlows() []LoginFlow
    // Return a LoginProcess for the given flow; do the real work in Start().
    CreateLogin(ctx context.Context, user *User, flowID string) (LoginProcess, error)

    // Version numbers for bridge info state events and room capabilities.
    // Bump to make the bridge resend m.bridge / room-features state to all portals.
    GetBridgeInfoVersion() (info, capabilities int)
}
```

`BridgeName` (`networkinterface.go:188-205`): `DisplayName`, `NetworkURL`, `NetworkIcon` (mxc URI), `NetworkID` (e.g. `googlechat`), `BeeperBridgeType` (Go import path convention), `DefaultPort`, `DefaultCommandPrefix`.

`database.MetaTypes` (`bridgev2/database/database.go:40-46`): `Portal, Ghost, Message, Reaction, UserLogin` — each a `MetaTypeCreator func() any`. Unset fields get a blank meta. Metadata is stored as JSON in a `metadata` column of the corresponding table; if the metadata type implements `database.MetaMerger` (`CopyFrom(other any)`, database.go:34-36), the framework merges instead of replacing on relogin (`userlogin.go:225-229`).

### 1.2 Optional connector-level interfaces (on the `NetworkConnector` object)

All in `bridgev2/networkinterface.go` unless noted:

| Interface | Line | Methods | Purpose |
|---|---|---|---|
| `StoppableNetwork` | 266 | `Stop()` | called on bridge shutdown after logins disconnected |
| `DirectMediableNetwork` | 278 | `SetUseDirectMedia()`, `Download(ctx, mediaID networkid.MediaID, params map[string]string) (mediaproxy.GetMediaResponse, error)` | on-demand media via `mxc://` proxy |
| `IdentifierValidatingNetwork` | 287 | `ValidateUserID(id networkid.UserID) bool` | shape-check user IDs (no network calls) |
| `TransactionIDGeneratingNetwork` | 292 | `GenerateTransactionID(userID id.UserID, roomID id.RoomID, eventType event.Type) networkid.RawTransactionID` | pre-generate txn IDs for outgoing events |
| `PortalBridgeInfoFillingNetwork` | 297 | `FillPortalBridgeInfo(portal *Portal, content *event.BridgeEventContent)` | add custom fields to `m.bridge` state |
| `ConfigValidatingNetwork` | 309 | `ValidateConfig() error` | reject startup on bad network config (called from mxmain `validateConfig`, `matrix/mxmain/main.go:310-316`) |
| `MaxFileSizeingNetwork` | 319 | `SetMaxFileSize(maxSize int64)` | told homeserver upload limit (default assume 50 MiB) |
| `NetworkResettingNetwork` | 324 | `ResetHTTPTransport()`, `ResetNetworkConnections()` | forced reconnects (used by `Bridge.ResetNetworkConnections`, bridge.go:389) |
| `PushParsingNetwork` | 1059 | `ParsePushNotification(ctx, data json.RawMessage) (networkid.UserLoginID, any, error)` | map native push → login for background wakeup |

### 1.3 `NetworkAPI` (`bridgev2/networkinterface.go:377-417`)

One instance per `UserLogin`; stored in `UserLogin.Client` by `LoadUserLogin`.

```go
type NetworkAPI interface {
    // Connect to remote network. MUST NOT return errors — report failures via
    // login.BridgeState.Send(status.BridgeState{...}).
    Connect(ctx context.Context)
    // Clean disconnect, must not take too long (framework logs at 2s, times out configurable).
    Disconnect()
    // Cached-only check that tokens are valid (no I/O).
    IsLoggedIn() bool
    // Invalidate tokens remotely if possible + disconnect.
    LogoutRemote(ctx context.Context)

    // Is this remote user ID the same account as this login? Used for
    // UserLoginID<->UserID mapping and double-puppet resolution.
    IsThisUser(ctx context.Context, userID networkid.UserID) bool
    // Room metadata. nil fields = don't touch; empty string = clear.
    GetChatInfo(ctx context.Context, portal *Portal) (*ChatInfo, error)
    // Ghost metadata; same nil semantics.
    GetUserInfo(ctx context.Context, ghost *Ghost) (*UserInfo, error)
    // Per-room feature matrix (may be static).
    GetCapabilities(ctx context.Context, portal *Portal) *event.RoomFeatures

    // Outgoing plain (non-edit) message. Convert, send to remote, return DB row info.
    HandleMatrixMessage(ctx context.Context, msg *MatrixMessage) (*MatrixMessageResponse, error)
}
```

### 1.4 Optional `NetworkAPI` interfaces — complete list

All in `bridgev2/networkinterface.go`:

**Messaging features**

| Interface | Line | Methods |
|---|---|---|
| `EditHandlingNetworkAPI` | 616 | `HandleMatrixEdit(ctx, msg *MatrixEdit) error` — mutate `msg.EditTarget` (`*database.Message`); framework saves it afterwards |
| `ReactionHandlingNetworkAPI` | 631 | `PreHandleMatrixReaction(ctx, msg *MatrixReaction) (MatrixReactionPreResponse, error)` (returns `SenderID`, `EmojiID`, `Emoji`, `MaxReactions` for framework dedup/override), then `HandleMatrixReaction(ctx, msg *MatrixReaction) (*database.Reaction, error)` (may return empty struct; framework fills), `HandleMatrixReactionRemove(ctx, msg *MatrixReactionRemove) error` |
| `RedactionHandlingNetworkAPI` | 649 | `HandleMatrixMessageRemove(ctx, msg *MatrixMessageRemove) error` |
| `PollHandlingNetworkAPI` | 624 | `HandleMatrixPollStart(ctx, *MatrixPollStart) (*MatrixMessageResponse, error)`, `HandleMatrixPollVote(ctx, *MatrixPollVote) (*MatrixMessageResponse, error)` |
| `ReadReceiptHandlingNetworkAPI` | 656 | `HandleMatrixReadReceipt(ctx, msg *MatrixReadReceipt) error` — must tolerate `msg.ExactMessage == nil` |
| `TypingHandlingNetworkAPI` | 676 | `HandleMatrixTyping(ctx, msg *MatrixTyping) error` |
| `ChatViewingNetworkAPI` | 666 | `HandleMatrixViewingChat(ctx, msg *MatrixViewingChat) error` (Beeper-only signal) |

**Backfill**

| Interface | Line | Methods |
|---|---|---|
| `BackfillingNetworkAPI` | 598 | `FetchMessages(ctx, fetchParams FetchMessagesParams) (*FetchMessagesResponse, error)` |
| `BackfillingNetworkAPIWithLimits` | 608 | + `GetBackfillMaxBatchCount(ctx, portal *Portal, task *database.BackfillTask) int` (<0 = unlimited) |

`FetchMessagesParams` (line 453): `Portal`, `ThreadRoot networkid.MessageID` (set when paginating inside a thread), `Forward bool`, `AnchorMessage *database.Message`, `Cursor networkid.PaginationCursor`, `Count int`, `AllowSlowFetch bool`, `BundledData any`, `Task *database.BackfillTask`.
`FetchMessagesResponse` (line 556): `Messages []*BackfillMessage` (chronological), `Cursor`, `HasMore` (required), `Forward`, `MarkRead`, `MoreRequiresSlowFetch`, `Pending`, `AggressiveDeduplication`, progress fields (`ApproxProgress float64`, `ApproxRemainingCount`, `ApproxTotalCount`), `CompleteCallback func()`.
`BackfillMessage` (line 505): embeds `*ConvertedMessage` + `Sender EventSender`, `ID networkid.MessageID`, `TxnID`, `Timestamp`, `StreamOrder`, `Reactions []*BackfillReaction`, **`ShouldBackfillThread bool`, `LastThreadMessage networkid.MessageID`** (thread backfill triggers).
`BackfillReaction` (line 489): `TargetPart *networkid.PartID`, `Timestamp`, `Sender`, `EmojiID`, `Emoji`, `ExtraContent`, `DBMetadata`.

**Chat/room management**

| Interface | Line | Methods |
|---|---|---|
| `RoomNameHandlingNetworkAPI` | 700 | `HandleMatrixRoomName(ctx, msg *MatrixRoomName) (bool, error)` — update `portal.Name/NameSet` on success |
| `RoomAvatarHandlingNetworkAPI` | 710 | `HandleMatrixRoomAvatar(ctx, msg *MatrixRoomAvatar) (bool, error)` |
| `RoomTopicHandlingNetworkAPI` | 720 | `HandleMatrixRoomTopic(ctx, msg *MatrixRoomTopic) (bool, error)` |
| `DisappearTimerChangingNetworkAPI` | 729 | `HandleMatrixDisappearingTimer(ctx, msg *MatrixDisappearingTimer) (bool, error)` |
| `MembershipHandlingNetworkAPI` | 950 | `HandleMatrixMembership(ctx, msg *MatrixMembershipChange) (*MatrixMembershipResult, error)`; change types enumerated as `MembershipChangeType` vars (`Invite`, `Join`, `Leave`, `Kick`, `BanJoined`, `AcceptInvite`, …, lines 913-931) |
| `PowerLevelHandlingNetworkAPI` | 979 | `HandleMatrixPowerLevels(ctx, msg *MatrixPowerLevelChange) (bool, error)` |
| `DeleteChatHandlingNetworkAPI` | 739 | `HandleMatrixDeleteChat(ctx, msg *MatrixDeleteChat) error` |
| `MessageRequestAcceptingNetworkAPI` | 747 | `HandleMatrixAcceptMessageRequest(ctx, msg *MatrixAcceptMessageRequest) error` |
| `MarkedUnreadHandlingNetworkAPI` | 684 | `HandleMarkedUnread(ctx, msg *MatrixMarkedUnread) error` |
| `MuteHandlingNetworkAPI` | 689 | `HandleMute(ctx, msg *MatrixMute) error` |
| `TagHandlingNetworkAPI` | 694 | `HandleRoomTag(ctx, msg *MatrixRoomTag) error` |

**Chat/user discovery & creation**

| Interface | Line | Methods |
|---|---|---|
| `IdentifierResolvingNetworkAPI` | 796 | `ResolveIdentifier(ctx, identifier string, createChat bool) (*ResolveIdentifierResponse, error)` — powers `resolve-identifier`/`start-chat` commands + provisioning |
| `GhostDMCreatingNetworkAPI` | 805 | + `CreateChatWithGhost(ctx, ghost *Ghost) (*CreateChatResponse, error)` |
| `ContactListingNetworkAPI` | 815 | `GetContactList(ctx) ([]*ResolveIdentifierResponse, error)` |
| `UserSearchingNetworkAPI` | 820 | `SearchUsers(ctx, query string) ([]*ResolveIdentifierResponse, error)` |
| `GroupCreatingNetworkAPI` | 825 | `CreateGroup(ctx, params *GroupCreateParams) (*CreateChatResponse, error)` |

`ResolveIdentifierResponse` (line 753): `Ghost *Ghost`, `UserID networkid.UserID`, `UserInfo *UserInfo`, `Chat *CreateChatResponse`. `CreateChatResponse` (line 774): `PortalKey` (required), optional `Portal`, `PortalInfo *ChatInfo`, `DMRedirectedTo`, `FailedParticipants`.

**Misc**

| Interface | Line | Methods |
|---|---|---|
| `NetworkAPIWithUserID` | 422 | `GetUserID() networkid.UserID` |
| `BackgroundSyncingNetworkAPI` | 437 | `ConnectBackground(ctx, params *ConnectBackgroundParams) error` (blocking one-shot sync; used by `Bridge.RunOnce`, bridge.go:132) |
| `CredentialExportingNetworkAPI` | 447 | `ExportCredentials(ctx) any` (session transfer) |
| `PushableNetworkAPI` | 1048 | `RegisterPushNotifications(ctx, pushType PushType, token string) error`, `GetPushConfigs() *PushConfig` |
| `StickerImportingNetworkAPI` | 1077 | `DownloadImagePack(ctx, url string) (*ImportedImagePack, error)`, `ListImagePacks(ctx) ([]*event.ImagePackMetadata, error)` |
| `PersonalFilteringCustomizingNetworkAPI` | 830 | `CustomizePersonalFilteringSpace(req *mautrix.ReqCreateRoom)` |

**Presence:** there is no presence interface in bridgev2 — `unorganized-docs/FEATURES.md` lists Presence as unimplemented (`[ ] Presence`). Typing is supported both directions (`TypingHandlingNetworkAPI` outbound, `RemoteTyping` inbound); read receipts both directions; presence neither.

### 1.5 Matrix-event wrapper structs passed to handlers (`networkinterface.go:1392-1502`)

- `MatrixEventBase[ContentType]`: `Event *event.Event`, `Content ContentType`, `Portal *Portal`, `OrigSender *OrigSender` (non-nil only when relaying), `InputTransactionID networkid.RawTransactionID`.
- `MatrixMessage`: base + `ThreadRoot *database.Message`, `ReplyTo *database.Message` (framework already resolved relations per room capabilities). Methods: `AddPendingToIgnore(txnID)`, `AddPendingToSave(message, txnID, handleEcho RemoteEchoHandler)`, `RemovePending(txnID)` (portal.go:1392-1431) for echo dedup — see §4.3.
- `MatrixEdit`: base + `EditTarget *database.Message`.
- `MatrixReaction`: base + `TargetMessage *database.Message`, `PreHandleResp`, `ReactionToOverride *database.Reaction`, `ExistingReactionsToKeep []*database.Reaction`.
- `MatrixReactionRemove`: base + `TargetReaction *database.Reaction`; `MatrixMessageRemove`: base + `TargetMessage`.
- `MatrixRoomMeta[ContentType]`: base + `PrevContent`, `IsStateRequest`; aliases `MatrixRoomName/Avatar/Topic/DisappearingTimer` (lines 1464-1467).
- `MatrixReadReceipt` (1469): `Portal`, `EventID`, `ExactMessage *database.Message` (nullable), `ReadUpTo`, `LastRead time.Time`, `Receipt event.ReadReceipt`, `Implicit bool`.
- `MatrixTyping` (1486): `Portal`, `IsTyping`, `Type TypingType` (`TypingTypeText/UploadingMedia/RecordingMedia`).

### 1.6 `LoginProcess` (`bridgev2/login.go:20-66`)

```go
type LoginProcess interface {
    Start(ctx context.Context) (*LoginStep, error) // called exactly once
    Cancel()                                       // not called after an error (errors are fatal)
}
// Optional step-specific extensions (implement the ones your flow uses):
type LoginProcessDisplayAndWait interface { LoginProcess; Wait(ctx) (*LoginStep, error) }
type LoginProcessUserInput      interface { LoginProcess; SubmitUserInput(ctx, input map[string]string) (*LoginStep, error) }
type LoginProcessCookies        interface { LoginProcess; SubmitCookies(ctx, cookies map[string]string) (*LoginStep, error) }
type LoginProcessWebAuthn       interface { LoginProcess; SubmitWebAuthnResponse(ctx, response json.RawMessage) (*LoginStep, error) }
type LoginProcessWithOverride   interface { LoginProcess; StartWithOverride(ctx, override *UserLogin) (*LoginStep, error) } // relogin
```

`LoginStep` (login.go:93-112): `Type LoginStepType`, `StepID string` (stable namespaced ID, e.g. `fi.mau.gchat.cookies`), `Instructions string`, plus exactly one of `DisplayAndWaitParams | CookiesParams | UserInputParams | WebAuthnParams | CompleteParams`.

Step types (login.go:76-82): `user_input`, `cookies`, `display_and_wait`, `webauthn`, `complete`.

- `LoginCookiesParams` (login.go:170-186): `URL`, `UserAgent`, `Fields []LoginCookieField`, `ExtractJS`, `WaitForURLPattern`. Each `LoginCookieField` (160): `ID`, `Required`, `Sources []LoginCookieFieldSource`, `Pattern`. Source types (135-141): `cookie`, `local_storage`, `request_header`, `request_body`, `special`; source has `Name`, `RequestURLRegex`, `CookieDomain`. **This is the natural fit for Google Chat's cookie-based login** (the Python bridge extracts Google cookies).
- `LoginUserInputParams` (290): `Fields []LoginInputDataField` (+`Attachments`). Field types (190-201): username, password, phone_number, email, 2fa_code, token, url, domain, select, captcha_code. Each field has `Type/ID/Name/Description/DefaultValue/Pattern/Options` and an optional `Validate func(string) (string, error)` (auto-filled by `FillDefaultValidate`, login.go:258).
- `LoginDisplayAndWaitParams` (123): `Type` (`qr`/`emoji`/`code`/`nothing`), `Data`, `ImageURL`.
- `LoginCompleteParams` (312): `UserLoginID networkid.UserLoginID`, `UserLogin *UserLogin` — **the process must itself create the login via `user.NewLogin(ctx, &database.UserLogin{ID:..., RemoteName:..., Metadata:...}, &bridgev2.NewLoginParams{...})`** (userlogin.go:185-262) before returning the `complete` step. `NewLoginParams`: `LoadUserLogin` override, `DeleteOnConflict`, `DontReuseExisting`. After `NewLogin`, connectors conventionally call `login.Client.Connect(ctx)` themselves (the framework doesn't auto-connect fresh logins).

Login flow orchestration (commands + provisioning) is entirely framework-side: `commands/login.go:59-593` and `matrix/provisioning.go:103-106` drive the state machine.

---

## 2. RemoteEvent model

### 2.1 Queuing

Remote events enter through `UserLogin.QueueRemoteEvent(evt RemoteEvent)` → `Bridge.QueueRemoteEvent(login, evt)` (`bridgev2/queue.go:230-275`). The bridge:

1. Resolves the portal from `evt.GetPortalKey()` (handling `RemoteEventWithUncertainPortalReceiver` / `...Fetcher` for uncertain receivers when `SplitPortals` is off).
2. Calls `login.MarkInPortal(ctx, portal)` (space.go:23 — ensures user_portal row, double-puppet join, personal-space add).
3. Pushes onto the portal's serial event channel (`portal.queueEvent`, portal.go:354; buffer size `PortalEventBuffer = 64`, portal.go:112). Each portal runs one `eventLoop()` goroutine (portal.go:395) → strict per-portal ordering; `Config.AsyncEvents` opts out. Handlers that run >30 s log warnings; after `EventHandlingTimeoutTicks` (10) ticks they're backgrounded (portal.go:418-470).

Return type everywhere is `EventHandlingResult` (queue.go:169-228) with sentinels `EventHandlingResultSuccess/Ignored/Queued/Failed`.

### 2.2 Core interface + capability sub-interfaces (`networkinterface.go:1144-1377`)

```go
type RemoteEvent interface {
    GetType() RemoteEventType
    GetPortalKey() networkid.PortalKey
    AddLogContext(c zerolog.Context) zerolog.Context
    GetSender() EventSender
}
```

`RemoteEventType` enum (1125-1142): `Unknown, Message, MessageUpsert, Edit, Reaction, ReactionRemove, ReactionSync, MessageRemove, ReadReceipt, DeliveryReceipt, MarkUnread, Typing, ChatInfoChange, ChatResync, ChatDelete, Backfill`. Dispatch in `portal.handleRemoteEvent` (portal.go:2562-2643) switches on the type and type-asserts to the matching interface.

Per-type interfaces (line numbers in networkinterface.go):

- `RemoteMessage` (1253): `GetID() networkid.MessageID`, `ConvertMessage(ctx, portal, intent MatrixAPI) (*ConvertedMessage, error)`.
- `RemoteMessageUpsert` (1265): + `HandleExisting(ctx, portal, intent, existing []*database.Message) (UpsertResult, error)`.
- `RemoteMessageWithTransactionID` (1270): + `GetTransactionID()` — enables echo dedup against pending outgoing messages.
- `RemoteEdit` (1275): `GetTargetMessage()`, `ConvertEdit(ctx, portal, intent, existing []*database.Message) (*ConvertedEdit, error)`.
- `RemoteReaction` (1280): `GetReactionEmoji() (string, networkid.EmojiID)`; extensions `RemoteReactionWithExtraContent` (1313), `RemoteReactionWithMeta` (1318).
- `RemoteReactionRemove` (1323): `GetRemovedEmojiID()`.
- `RemoteReactionSync` (1308): `GetReactions() *ReactionSyncData` (`Users map[networkid.UserID]*ReactionSyncUser`, `HasAllUsers`; per-user `Reactions []*BackfillReaction`, `HasAllReactions`, `MaxCount`, lines 1285-1298).
- `RemoteMessageRemove` (1328); `RemoteMessageRemoveWithoutPlaceholder` (1332): `DontRenderPlaceholder()`.
- `RemoteReadReceipt` (1340): `GetLastReceiptTarget()`, `GetReceiptTargets()`, `GetReadUpTo() time.Time`; `RemoteReadReceiptWithStreamOrder` (1347).
- `RemoteDeliveryReceipt` (1352): `GetReceiptTargets()`.
- `RemoteMarkUnread` (1357): `GetUnread() bool`.
- `RemoteTyping` (1362): `GetTimeout() time.Duration`; `RemoteTypingWithType` (1375).
- `RemoteChatInfoChange` (1180): `GetChatInfoChange(ctx) (*ChatInfoChange, error)`.
- `RemoteChatResync` (1185, marker) with `RemoteChatResyncWithInfo` (1189): `GetChatInfo(ctx, portal) (*ChatInfo, error)`; `RemoteChatResyncBackfill` (1194): `CheckNeedsBackfill(ctx, latestMessage *database.Message) (bool, error)`; `RemoteChatResyncBackfillBundle` (1199): `GetBundledBackfillData() any`.
- `RemoteChatDelete` (1214) = `RemoteDeleteOnlyForMe` (`DeleteOnlyForMe() bool`); `RemoteChatDeleteWithChildren` (1218).
- `RemoteBackfill` (1204): `GetBackfillData(ctx, portal) (*FetchMessagesResponse, error)`.

Cross-cutting mixins: `RemoteEventThatMayCreatePortal` (1223, `ShouldCreatePortal() bool` — without this, events for unknown portals are dropped, portal.go:2564-2569), `RemoteEventWithTimestamp` (1243), `RemoteEventWithStreamOrder` (1248), `RemoteEventWithTargetMessage` (1228), `RemoteEventWithTargetPart` (1238), `RemoteEventWithBundledParts` (1233), `RemotePreHandler`/`RemotePostHandler` (1170/1175), `RemoteEventWithContextMutation` (1155), `RemoteEventWithUncertainPortalReceiver(Fetcher)` (1160/1165).

`EventSender` (networkinterface.go:60-83): `IsFromMe bool` (use the source login's double puppet), `SenderLogin networkid.UserLoginID`, `Sender networkid.UserID`, `ForceDMUser bool`, `ForceEditOrigSender bool`. Intent resolution: `portal.GetIntentFor` / `getIntentAndUserMXIDFor` (portal.go:2685-2758) — double puppet if from-me/sender-login matches, else the ghost's intent; also triggers `ghost.UpdateInfoIfNecessary`.

### 2.3 `simplevent` helpers (`bridgev2/simplevent/`)

Preferred over the deprecated `bridgev2.SimpleRemoteEvent[T]` (simpleremoteevent.go:19-25). All embed `simplevent.EventMeta` (meta.go:20-35) which implements the base + uncertain-receiver + create-portal + timestamp + stream-order + pre/post-handle + context-mutation interfaces, with builder-style `WithType/WithPortalKey/WithSender/WithCreatePortal/WithTimestamp/...` helpers (meta.go:113-163):

```go
type EventMeta struct {
    Type bridgev2.RemoteEventType
    LogContext func(zerolog.Context) zerolog.Context
    PortalKey networkid.PortalKey
    UncertainReceiver bool
    Sender bridgev2.EventSender
    CreatePortal bool
    Timestamp time.Time
    StreamOrder int64
    PreHandleFunc, PostHandleFunc func(context.Context, *bridgev2.Portal)
    MutateContextFunc func(context.Context) context.Context
    FetchCertainPortalKeyFunc func(context.Context) networkid.PortalKey
}
```

- `simplevent.Message[T]` (message.go:18-29): generic message/edit/upsert; fields `Data T`, `ID`, `TransactionID`, `TargetMessage`, plus `ConvertMessageFunc`, `ConvertEditFunc`, `HandleExistingFunc` callbacks.
- `simplevent.PreConvertedMessage` (message.go:63): carries a ready `*bridgev2.ConvertedMessage`.
- `simplevent.MessageRemove` (message.go:97): `TargetMessage`, `OnlyForMe`, `HidePlaceholder`.
- `simplevent.Reaction` (reaction.go:15): `TargetMessage`, `EmojiID`, `Emoji`, `ExtraContent`, `ReactionDBMeta`; doubles as remove.
- `simplevent.ReactionSync` (reaction.go:51).
- `simplevent.Receipt` (receipt.go:16): `LastTarget`, `Targets`, `ReadUpTo`, `ReadUpToStreamOrder`; also delivery receipt.
- `simplevent.MarkUnread` (receipt.go:47), `simplevent.Typing` (receipt.go:60).
- `simplevent.ChatResync` (chat.go:25): `ChatInfo` or `GetChatInfoFunc`, `LatestMessageTS` or `CheckNeedsBackfillFunc`, `BundledBackfillData`. Default backfill check compares `LatestMessageTS` to the newest bridged message (chat.go:43-51).
- `simplevent.ChatDelete` (chat.go:65), `simplevent.ChatInfoChange` (chat.go:82), `simplevent.Backfill` (chat.go:95).

### 2.4 Conversion output structs

`ConvertedMessage` (networkinterface.go:98-110):

```go
type ConvertedMessage struct {
    ReplyTo    *networkid.MessageOptionalPartID
    ReplyToRoom networkid.PortalKey; ReplyToUser networkid.UserID; ReplyToLogin networkid.UserLoginID // Beeper backfill only
    ThreadRoot *networkid.MessageID
    Parts      []*ConvertedMessagePart
    Disappear  database.DisappearingSetting
}
type ConvertedMessagePart struct { // line 29
    ID networkid.PartID; Type event.Type; Content *event.MessageEventContent
    Extra map[string]any; DBMetadata any; DontBridge bool
}
```

Helpers: `MergeCaption` (112) and `(*ConvertedMessage).MergeCaption()` (142) to collapse text+media into one part.

`ConvertedEdit` (178): `ModifiedParts []*ConvertedEditPart`, `DeletedParts []*database.Message`, `AddedParts *ConvertedMessage`. `ConvertedEditPart` (161): `Part *database.Message`, `Type`, `Content` (framework wraps in `m.new_content` — do NOT call SetEdit), `Extra`, `TopLevelExtra`, `NewMentions`, `DontBridge`.

### 2.5 `ChatInfo` / `UserInfo`

`ChatInfo` (`bridgev2/portal.go:4189-4208`):

```go
type ChatInfo struct {
    Name, Topic *string           // nil = leave, ptr-to-"" = clear (DefaultChatName)
    Avatar *Avatar
    Members *ChatMemberList
    JoinRule *event.JoinRulesEventContent
    Type *database.RoomType       // "", "dm", "group_dm", "space" (database/portal.go:23-30)
    Disappear *database.DisappearingSetting
    ParentID *networkid.PortalID  // space parent portal
    UserLocal *UserLocalPortalInfo // MutedUntil *time.Time, Tag *event.RoomTag (4232)
    MessageRequest *bool
    CanBackfill bool               // gates forward backfill + queue task creation on room create
    ExcludeChangesFromTimeline bool
    ExtraUpdates ExtraUpdater[*Portal] // func(ctx, *Portal) bool for custom metadata updates
}
```

`ChatMemberList` (portal.go:4078-4101): `IsFull` (removes extra members), `CheckAllLogins`, `ExcludeChangesFromTimeline`, `TotalMemberCount`, `OtherUserID` (DM peer; auto-derived from a 2-entry map), `MemberMap ChatMemberMap` (`Members []ChatMember` deprecated), `PowerLevels *PowerLevelOverrides` (4116: per-event-type levels, defaults, `Custom func(*event.PowerLevelsEventContent) bool`).
`ChatMember` (4036): embeds `EventSender` + `Membership event.Membership`, `Nickname *string`, `PowerLevel *int`, `UserInfo *UserInfo`, `MemberSender`, `MemberEventExtra map[string]any`, `PrevMembership`.
`ChatInfoChange` (4008): `ChatInfo *ChatInfo` (only changed fields) + `MemberChanges *ChatMemberList` (deltas only).

`UserInfo` (`bridgev2/ghost.go:141-149`): `Identifiers []string` (URI-style, e.g. `mailto:`), `Name *string`, `Avatar *Avatar`, `IsBot *bool`, `ExtraProfile database.ExtraProfile`, `ExtraUpdates ExtraUpdater[*Ghost]`.
`Avatar` (ghost.go:108-116): `ID networkid.AvatarID`, `Get func(ctx) ([]byte, error)`, `Remove bool`, or pre-uploaded `MXC`+`Hash`. Framework hashes & reuploads only on change (`Avatar.Reupload`, ghost.go:118).

### 2.6 `MatrixMessageResponse` + database rows

`MatrixMessageResponse` (networkinterface.go:336-348):

```go
type MatrixMessageResponse struct {
    DB *database.Message       // must have ID, SenderID (framework fills MXID/Room/Timestamp/relations via fillDBMessage, portal.go:1433)
    StreamOrder int64
    Pending bool               // true => framework does NOT save; requires prior AddPendingToSave
    RemovePending networkid.TransactionID // clear AddPendingToIgnore entry after save
    PostSave func(context.Context, *database.Message)
}
```

`database.Message` (`bridgev2/database/message.go:33-53`): `RowID`, `BridgeID`, `ID networkid.MessageID`, `PartID networkid.PartID`, `MXID id.EventID`, `Room networkid.PortalKey`, `SenderID networkid.UserID`, `SenderMXID id.UserID`, `Timestamp`, `EditCount`, `IsDoublePuppeted`, `ThreadRoot networkid.MessageID`, `ReplyTo networkid.MessageOptionalPartID`, `SendTxnID networkid.RawTransactionID`, `Metadata any` (connector-defined via `GetDBMetaTypes().Message`). Key queries: `GetAllPartsByID`, `GetPartByID`, `GetPartByMXID`, `GetFirstThreadMessage`, `GetLastThreadMessage`, `GetLastMessagePartAtOrBeforeTime` (message.go:62-77).

`database.Reaction` (`bridgev2/database/reaction.go:25-38`): `Room`, `MessageID`, `MessagePartID`, `SenderID`, `SenderMXID`, `EmojiID`, `MXID`, `Timestamp`, `Emoji`, `Metadata any`. Uniqueness: `(bridge_id, room_receiver, message_id, message_part_id, sender_id, emoji_id)` (upsert at reaction.go:50-55) — networks with one-reaction-per-message use `EmojiID = ""` (`networkid/bridgeid.go:134-139`).

### 2.7 networkid types (`bridgev2/networkid/bridgeid.go`)

Opaque string types generated only by the connector: `PortalID`/`PortalKey{ID, Receiver}` (Receiver segregates DMs per login; MUST be set everywhere if `SplitPortals`), `UserID` (globally unique in-bridge), `UserLoginID`, `MessageID` (globally unique), `TransactionID`, `RawTransactionID`, `PartID`, `MessagePartID`, `MessageOptionalPartID`, `PaginationCursor`, `AvatarID`, `EmojiID`, `MediaID []byte`. **Backwards compatibility of formats matters — they're persisted.** For Google Chat this is where "space ID / topic ID / message ID" formats must be pinned down once.

---

## 3. Capabilities

### 3.1 Connector-level: `NetworkGeneralCapabilities` (networkinterface.go:358-375)

```go
type NetworkGeneralCapabilities struct {
    DisappearingMessages bool  // enables the DisappearLoop
    AggressiveUpdateInfo bool  // re-fetch ghost info on every message, not just when missing
    ImplicitReadReceipts bool  // synthesize HandleMatrixReadReceipt when user sends a message
    OutgoingMessageTimeouts *OutgoingTimeoutConfig // {CheckInterval, NoEchoTimeout, NoEchoMessage, NoAckTimeout, NoAckMessage} (line 350)
    Provisioning ProvisioningCapabilities // resolve_identifier caps, group_creation map, image_pack_import (line 835)
}
```

### 3.2 Room-level: `event.RoomFeatures` (`event/capabilities.go:26-68`)

Returned by `NetworkAPI.GetCapabilities(ctx, portal)`; published as `StateBeeperRoomFeatures` state and used by the framework to pre-validate outgoing Matrix events (`portal.checkMessageContentCaps`, portal.go:1108). Fields:

- `ID string` (stable capability-set ID; defaults to hash),
- `Formatting FormattingFeatureMap` (per-HTML-feature `CapabilitySupportLevel`; ~30 features enumerated at capabilities.go:224-254),
- `File FileFeatureMap` (`map[CapabilityMsgType]*FileFeatures` with `MimeTypes`, `Caption`, `MaxCaptionLength`, `MaxSize`, `MaxWidth/Height/Duration`, `ViewOnce`; capabilities.go:256-270),
- `State StateFeatureMap`, `MemberActions MemberFeatureMap`,
- `MaxTextLength int`, `LocationMessage`, `Poll`, **`Thread`**, `Reply` (all `CapabilitySupportLevel`),
- `Edit`, `EditMaxCount`, `EditMaxAge`, `Delete`, `DeleteForMe`, `DeleteMaxAge`, `DeleteHide`,
- `DisappearingTimer *DisappearingTimerCapability`,
- `Reaction`, `ReactionCount`, `AllowedReactions`, `CustomEmojiReactions`,
- `ReadReceipts`, `TypingNotifications`, `Archive`, `MarkAsUnread`, `DeleteChat`, `DeleteChatForEveryone`, `MessageRequest`, `PerMessageProfileRelay` (relay via per-message profiles instead of text prefixes).

`CapabilitySupportLevel` (capabilities.go:200-220): `CapLevelRejected(-2)`, `Dropped(-1)`, `Unsupported(0)`, `PartialSupport(1)`, `FullySupported(2)`; helpers `.Partial()`, `.Full()`, `.Reject()`.

The framework re-publishes capabilities when `GetBridgeInfoVersion()`'s capability version changes (`Bridge.PostStart` → `ResendBridgeInfo`, bridge.go:218-298) and rate-limits implicit updates (`portal.UpdateCapabilities`, portal.go:4364).

### 3.3 Matrix-connector capabilities (`bridgev2/matrixinterface.go:27-33`)

`MatrixCapabilities{AutoJoinInvites, BatchSending, ArbitraryMemberChange, ExtraProfileMeta, ReplaceEntireProfile}` — informational for connector code; e.g. backward backfill queue requires `BatchSending` (Beeper/hungryserv only; backfillqueue.go:77-80).

---

## 4. What the framework does for you

### 4.1 Portal lifecycle & room creation
- Portal cache + lazy load + auto insert of DB rows (`Bridge.GetPortalByKey`/`loadPortal`, portal.go:116-352).
- Per-portal serialized event loop (§2.1).
- **Room creation**: `portal.CreateMatrixRoom(ctx, source, info)` (portal.go:5134) or automatically when a remote event implements `RemoteEventThatMayCreatePortal` and `ShouldCreatePortal()` (portal.go:2564). `createMatrixRoomInLoop` (5180-5407) fetches `GetChatInfo` if needed, applies name/topic/avatar/members/power levels, sets initial state (functional members, `m.bridge` + `uk.half-shot.bridge`, room features, disappearing timer, space parent, join rules), creates the room, registers backfill queue task if `info.CanBackfill`, joins members / syncs participants, adds to spaces, and runs forward backfill.
- Portal info sync: `portal.UpdateInfo(ctx, info, source, sender, ts)` (5023) handles name/topic/avatar diffs, member sync (`syncParticipants`, 4622), parent changes, DM "other user".
- Matrix→remote state changes are reverted if the connector rejects them (`RevertFailedStateChanges` config; `portal.revertRoomMeta`, 4466).
- Portal deletion/cleanup incl. child spaces (`portal.Delete`, 5442; `DeleteManyPortals`, userlogin.go:356).

### 4.2 Ghost management
- Ghost cache/lazy insert (`Bridge.GetGhostByID`, ghost.go:90).
- `ghost.UpdateInfo(ctx, *UserInfo)` (ghost.go:333) — name/avatar/contact-info diffing, avatar hash dedup, Beeper extra-profile publishing, DM portal meta propagation (`updateDMPortals`, ghost.go:319, gated by `private_chat_portal_meta`).
- `UpdateInfoIfNecessary` auto-fetches `GetUserInfo` on incoming events when the ghost has no name/avatar (ghost.go:294), or always with `AggressiveUpdateInfo`.

### 4.3 Deduplication / echo suppression
- **Remote→Matrix**: `portal.handleRemoteMessage` first checks pending outgoing messages by transaction ID (`checkPendingMessage`, portal.go:2950 — pairs with `MatrixMessage.AddPendingToIgnore`/`AddPendingToSave`), then checks the DB for the message ID (`GetAllPartsByID`, portal.go:3058) and drops duplicates.
- **Matrix→remote**: optional `deduplicate_matrix_messages` config checks event ID/txn ID against the DB (portal.go:1297-1307). `OutgoingTimeoutConfig` powers a pending-message timeout loop (portal.go:1465).
- Backfill dedup: timestamps cutoffs by default; `AggressiveDeduplication` for per-message DB checks (networkinterface.go:579-582).

### 4.4 Encryption
Entirely inside the Matrix connector (`bridgev2/matrix/crypto.go`, `cryptostore.go`, `cryptoerror.go`); config `encryption:` block (`bridgeconfig/encryption.go:13-51`: allow/default/require/appservice, MSC4190/MSC4392, key deletion, verification levels, rotation). Network connectors never see encryption.

### 4.5 Relay mode
`bridge.relay` config (`bridgeconfig/relay.go:21-31`: enabled, admin_only, default_relays, message_formats templates, displayname_format). Framework picks the relay login when the Matrix sender has no own login (`portal.FindPreferredLogin(ctx, user, allowRelay)`, portal.go:656), wraps the sender into `OrigSender` (networkinterface.go:1380-1390) and pre-formats the message via templates unless `RoomFeatures.PerMessageProfileRelay` (portal.go:1194-1210). `portal.SetRelay` (5548), `!bridge set-relay` command (commands/relay.go).

### 4.6 Provisioning API
`matrix/provisioning.go` — full REST API under `/_matrix/provision/v3/`: whoami, capabilities, login flows/start/step/cancel, logout, logins, contacts, search_users, resolve_identifier, create_dm, create_group, backfill (pagination), image packs, session transfers (provisioning.go:101-122). Auth via shared secret or Matrix auth. Driven by the same connector interfaces (LoginProcess, IdentifierResolving, etc.). Config: `provisioning:` (bridgeconfig/config.go:113-119).

### 4.7 Commands
`bridgev2/commands` package: processor (`NewProcessor`, processor.go:34) with built-ins — help, cancel, version (added by mxmain), login/relogin/list-logins/logout/set-preferred-login (login.go), start-chat/resolve-identifier/search (startchat.go), delete-portal/cleanup (cleanup.go), relay (relay.go), sudo (sudo.go), imagepack, managechat, debug. Add custom commands via `proc.AddHandler(&commands.FullHandler{Func, Name, Help, RequiresLogin/RequiresAdmin, NetworkAPIImplements gating...})` (handler.go:53). Commands are messages prefixed with `command_prefix` or anything in the management room (queue.go:104-122).

### 4.8 Backfill
- **Forward backfill** on room creation and on `RemoteChatResync` with `CheckNeedsBackfill` → `doForwardBackfill` (portalbackfill.go:27); driven by `BackfillingNetworkAPI.FetchMessages` with `Forward: true`.
- **Backward backfill queue**: `Bridge.RunBackfillQueue` (backfillqueue.go:72) — DB-persisted `backfill_task` rows (`database.BackfillTask`, database/backfillqueue.go:24-39: BatchCount, IsDone, Cursor, OldestMessageID, NextDispatchMinTS...), batch delay/size/max-batches from `backfill.queue` config (bridgeconfig/backfill.go:23-31). Requires Matrix `BatchSending` (Beeper). On standard homeservers only forward backfill applies (`sendLegacyBackfill` path, portalbackfill.go:340).
- `RemoteBackfill` events feed `ManualBackfill` tasks (portal.go:3994; backfillqueue.go:49).
- Config: `backfill.enabled`, `max_initial_messages`, `max_catchup_messages`, `unread_hours_threshold`, `threads.max_initial_messages` (bridgeconfig/backfill.go:9-21).

### 4.9 Disappearing messages
`DisappearLoop` (disappear.go:22-153): DB-backed timer queue (`disappearing_message` table), redacts via bot; enabled when `GetCapabilities().DisappearingMessages`. Connector only sets `ConvertedMessage.Disappear` / `ChatInfo.Disappear` (`database.DisappearingSetting{Type, Timer, DisappearAt}`, database/disappear.go:34).

### 4.10 Spaces
- **Personal filtering space** per login (`bridge.personal_filtering_spaces`): auto-created space room, portals auto-added (`UserLogin.GetSpaceRoom`, `MarkInPortal`, `AddPortalToSpace` — space.go:23-211).
- **Network-provided space hierarchy**: set `ChatInfo.ParentID` / `RoomType = space`; framework creates parent portals as space rooms, sends `m.space.child`/`m.space.parent` (portal.go:5253-5255, 5323-5332, space.go:83-132).

### 4.11 Bridge state & message status
- Per-login `BridgeStateQueue` (bridgestate.go:27) with dedup, retry, transient-disconnect notices, and **automatic reconnect on UNKNOWN_ERROR** (`unknownErrorReconnect`, bridgestate.go:180, config `unknown_error_auto_reconnect`). Connector sends states via `login.BridgeState.Send(status.BridgeState{StateEvent: status.StateConnected, ...})`.
- `status.BridgeStateEvent` values (`bridgev2/status/bridgestate.go:42-55`): STARTING, UNCONFIGURED, RUNNING, BRIDGE_UNREACHABLE, CONNECTING, BACKFILLING, CONNECTED, TRANSIENT_DISCONNECT, BAD_CREDENTIALS, UNKNOWN_ERROR, LOGGED_OUT. `UserLogin`/`NetworkAPI` may implement `status.BridgeStateFiller` to enrich states (userlogin.go:509-527).
- Message send status (MSS) events + error notices: `WrapErrorInStatus` and a large catalog of standard errors (`errors.go:47+`, e.g. `ErrEditsNotSupported`, `ErrReactionsNotSupported`); `MessageStatus` (messagestatus.go:58). Return wrapped errors from `HandleMatrix*` and the framework reports them.

### 4.12 Other
- Double puppeting (user.go:102-160) incl. auto-login via `double_puppet` config.
- Typing loop remote→Matrix incl. periodic refresh (portal.go:1002-1106).
- Read receipts both ways, implicit receipts, `user_portal.last_read` tracking.
- Matrix invites to ghosts → DM creation (matrixinvite.go), bot invites/management rooms (queue.go:124-137, user.go:204).
- `Bridge.RunOnce` background-sync mode for push-triggered iOS-style wakeups (bridge.go:132-176).
- Portal re-ID (portalreid.go) for networks that change chat IDs.
- Split portals mode + migration (bridge.go:300-355).

---

## 5. mxmain bootstrap

A bridge main.go is tiny (real example: `_reference/meta/cmd/mautrix-meta/main.go`):

```go
var m = mxmain.BridgeMain{
    Name: "mautrix-googlechat", URL: "https://github.com/...",
    Description: "A Matrix-Google Chat puppeting bridge.",
    Version: "0.1.0",
    Connector: &connector.GChatConnector{},
}
func main() {
    m.InitVersion(Tag, Commit, BuildTime) // ldflags-filled vars
    m.Run()
}
```

`mxmain.BridgeMain` (`bridgev2/matrix/mxmain/main.go:53-99`): fields `Name/Description/URL/Version/SemCalVer`, hooks `PostInit func()`, `PostStart func()`, `PostMigratePortal func(ctx, *bridgev2.Portal) error`, `Connector bridgev2.NetworkConnector`; auto-filled `Log`, `DB`, `Config`, `Matrix *matrix.Connector`, `Bridge *bridgev2.Bridge`.

`Run()` = `PreInit` → `Init` → `Start` → `WaitForInterrupt` → `Stop` (main.go:114-121):

- **CLI flags** (main.go:40-50): `-c/--config`, `-e/--generate-example-config`, `-n/--no-update`, `-r/--registration`, `-g/--generate-registration`, `-v/--version`, `--version-json`, `--ignore-unsupported-database`, `--ignore-foreign-tables`, `--ignore-unsupported-server`.
- **Example config generation** merges the base example (mxmain/example-config.yaml) with the connector's `GetConfig()` example under the `network:` key (`makeFullExampleConfig`, mxmain/config.go). Config upgrades run on every start via `configupgrade` with the connector's upgrader proxied to `network.*` (main.go:321-330).
- **Registration generation** writes registration.yaml and injects as_token/hs_token back into the config (main.go:181-209).
- **Init** (main.go:213-256): logging (zeroconfig), config validation (fails on placeholder values; calls connector `ValidateConfig`), DB init (rejects `sqlite3`, wants `sqlite3-fk-wal` with `_txlock=immediate`), `matrix.NewConnector(cfg)`, `bridgev2.NewBridge("", db, log, &cfg.Bridge, matrixConnector, networkConnector, commands.NewProcessor)`, adds `version` command, runs `PostInit`.
- **Start** (main.go:370-396): `Bridge.StartConnectors` → `PostMigrate` (legacy migration hooks) → `Bridge.StartLogins` (connects every stored login) → `Bridge.PostStart` (bridge-info resend check) → user `PostStart`.
- **Env config**: `env_config_prefix` allows overriding config values from environment (mxmain/envconfig.go, main.go:358-364).

**Legacy DB migration** (critical for taking over a Python mautrix-googlechat deployment): `BridgeMain.CheckLegacyDB(expectedVersion, minBridgeVersion, firstMegaVersion, migrator, transaction)` (mxmain/legacymigrate.go:116-172) detects a legacy DB by the `database_owner` table matching `br.Name`, checks the legacy schema version, and runs a migrator — typically `br.LegacyMigrateSimple(renameTablesQuery, copyDataQuery, newDBVersion)` (legacymigrate.go:112) which renames old tables, creates the bridgev2 schema, and runs a hand-written SQL copy query. Call it from `PostInit`. `PostMigrate` (legacymigrate.go:220) then fixes up rooms (functional members, DM power levels) after first start; the default DM handling can be overridden with `PostMigratePortal`. The Python googlechat schema → bridgev2 copy SQL would be written for this hook.

`bridgeconfig.Config` top-level (bridgeconfig/config.go:23-42): `network` (raw YAML for connector), `bridge` (BridgeConfig, config.go:67-95: command_prefix, personal_filtering_spaces, private_chat_portal_meta, async_events, split_portals, relay, permissions, cleanup_on_logout, portal_create_filter, backfill...), `database`, `homeserver`, `appservice`, `matrix`, `analytics`, `provisioning`, `public_media`, `direct_media`, `backfill`, `double_puppet`, `encryption`, `logging`. Permissions levels (bridgeconfig/permissions.go:20-28): SendEvents/Commands/Login/DoublePuppet/Admin/ManageRelay/MaxLogins.

---

## 6. unorganized-docs summary

- `README.md`: architecture (three components) and the connector checklist: implement `NetworkConnector`, `LoginProcess`, `NetworkAPI`, `RemoteEvent`. Login flow walkthrough: `GetLoginFlows` → `CreateLogin` → `Start` → repeat `Wait`/`SubmitUserInput`/`SubmitCookies` per step type → login process creates the `UserLogin` and returns a `complete` step.
- `FEATURES.md`: framework feature matrix. Done: text/attachments/replies/**threads**/edits/reactions (+mass-sync)/deletions/MSS/backfill, login/logout/relogin, disappearing messages, read receipts, typing, spaces, relay mode, chat & user & group metadata incl. membership and power levels, create group/DM, contact list, identifier resolve, user search. NOT done: polls (interface exists but unchecked), **presence**, invites/accept-message-request (partial), delete chat (partial), report spam, custom emojis.
- `incoming-remote-message.uml`: Network lib → connector → `Bridge.QueueRemoteEvent` → portal event channel → (create room if needed) → `ConvertMessage` → Matrix send → `Message.Insert`.
- `incoming-matrix-message.uml`: Matrix → `QueueMatrixEvent` → portal channel → check edit/reply/thread → `HandleMatrixMessage` → connector converts + sends → returns `database.Message` → insert + success checkpoint.
- `login-steps.uml` + `login-step.schema.json`: client/bridge sequence for the three interactive step types incl. QR refresh loop (each `Wait` may return a fresh `display_and_wait` step).

---

## 7. Threading model (Matrix threads ↔ remote threads)

This is the most Google-Chat-relevant subsystem, since Google Chat spaces are threaded.

**Opt-in**: set `RoomFeatures.Thread` to `CapLevelPartialSupport`/`FullySupported` in `GetCapabilities` (event/capabilities.go:40). No separate interface.

**Remote → Matrix**: set `ConvertedMessage.ThreadRoot *networkid.MessageID` (networkinterface.go:107). The framework resolves it via `portal.getRelationMeta` (portal.go:2760-2824): looks up the **first** message with that thread ID (`Message.GetFirstThreadMessage`, database/message.go:71) as `m.thread` root and the **latest** (`GetLastThreadMessage`, :72) as the reply-fallback event, then `applyRelationMeta` (2826) calls `content.GetRelatesTo().SetThread(threadRoot.MXID, prevThreadEvent.MXID)`. The saved `database.Message.ThreadRoot` column always stores the remote thread-root message ID (flattened: if the root itself is in a thread, the outer root is used — portal.go:1450-1455).

**Matrix → remote** (`portal.handleMatrixMessage`, portal.go:1234-1273): if `caps.Thread.Partial()`, the framework reads `m.relates_to` thread parent, loads it from the DB and passes it as `MatrixMessage.ThreadRoot *database.Message`; a non-fallback reply within the thread is passed as `ReplyTo`. **Reply→thread fallback**: replies from non-threaded clients to a message inside a thread are converted into thread messages; if the network is threads-only (`Thread` set, `Reply` unset), a plain reply starts/continues a thread rooted at the replied-to message (portal.go:1255-1268). The connector reads `msg.ThreadRoot.ID`/`.ThreadRoot` to address the remote thread.

**Backfill**: `FetchMessagesParams.ThreadRoot` requests pagination inside one thread (networkinterface.go:457). A `BackfillMessage` with `ShouldBackfillThread: true` (+ optional `LastThreadMessage`) makes the framework recursively backfill that thread (`portal.doThreadBackfill`, portalbackfill.go:226; in-batch variant at :507), limited by `backfill.threads.max_initial_messages` (bridgeconfig/backfill.go:19-21).

**DB support**: `message.thread_root_id` column, `GetFirstThreadMessage`/`GetLastThreadMessage` queries treat `id=$4 OR thread_root_id=$4` (message.go:71-72).

---

## 8. Miscellaneous facts that affect the Go design

- `LoadUserLogin` runs under the global cache lock — construct the client from `login.Metadata` only; do network I/O in `Connect`.
- `Connect` returning is not "connected" — send `status.StateConnected` from the connector when the websocket/channel is actually up.
- Message IDs, portal IDs, user IDs are permanent DB contents; the Python bridge's `gcid` formats (space/thread/message) should be mapped 1:1 where possible to allow DB migration (`LegacyMigrateSimple` copy SQL).
- `ConvertedMessagePart.ID` (PartID) is "" by convention for single-part messages; multi-part (e.g. text + N attachments) needs deterministic part IDs.
- `EventSender{IsFromMe: true}` is how own-message echo from other devices gets double-puppeted.
- The framework already auto-handles: duplicate remote messages, message parts, reply/thread resolution, ghost provisioning, room creation, avatar dedup, MSS, notices, spaces, receipt bookkeeping. The connector is mostly: client lifecycle + event translation in both directions + login.
- For non-Beeper deployments, backward backfill queue is inert (no batch sending); forward backfill on room creation still works via normal message sends (portalbackfill.go:338-341).
- `bridgev2.PortalEventBuffer`, `EventHandlingTimeoutTicks` are package vars that can be tuned (portal.go:112-114).
