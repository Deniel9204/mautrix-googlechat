# mautrix-meta as the blueprint for a bridgev2 Go bridge

Research report for the mautrix-googlechat → Go (bridgev2) reimplementation.
Source studied: `/Users/danielagoston/utmshared/Projects/git/generalclaude/mautrix/_reference/meta`
(module `go.mau.fi/mautrix-meta`, HEAD `e976d15`, version "26.06", pinned to mautrix-go
`v0.28.2-0.20260708122614-d9c352f407dd`).

mautrix-meta is the canonical, actively maintained example of a **cookie-login** bridgev2
bridge — the same login model mautrix-googlechat needs — which makes it the best single
reference for our port. All file paths below are relative to the meta repo root unless
absolute.

---

## 1. Repo layout

```
meta/
├── cmd/                              # binaries only — thin entrypoints, no logic
│   ├── mautrix-meta/main.go          # THE bridge binary (30 lines, mxmain.BridgeMain)
│   ├── mautrix-instagram/main.go     # 2nd binary reusing the repo w/ different connector
│   ├── lscli/lscli.go                # debug CLI: decode Lightspeed payloads (dev tool)
│   └── dgwparse/main.go              # debug CLI: parse DGW frames (dev tool)
├── pkg/
│   ├── connector/                    # bridgev2 NetworkConnector implementation (glue)
│   │   ├── connector.go              #   MetaConnector (NetworkConnector)
│   │   ├── client.go                 #   MetaClient (NetworkAPI), connect/reconnect logic
│   │   ├── login.go                  #   cookie + user-input login flows
│   │   ├── handlematrix.go           #   Matrix → remote (messages/reactions/edits/...)
│   │   ├── handlemeta.go             #   remote event pipeline → bridgev2 RemoteEvents
│   │   ├── handlewhatsapp.go         #   E2EE (whatsmeow) event handler [meta-specific]
│   │   ├── events.go                 #   RemoteEvent wrapper types (RemoteMessage etc.)
│   │   ├── backfill.go               #   BackfillingNetworkAPI (FetchMessages)
│   │   ├── threadbackfill.go         #   full-history thread pagination after connect
│   │   ├── chatinfo.go               #   ChatInfo/ChatMember wrapping
│   │   ├── userinfo.go               #   UserInfo/Avatar wrapping
│   │   ├── startchat.go              #   ResolveIdentifier / SearchUsers / CreateGroup
│   │   ├── capabilities.go           #   NetworkGeneralCapabilities + RoomFeatures
│   │   ├── config.go                 #   network config struct + configupgrade
│   │   ├── example-config.yaml       #   embedded network-section example config
│   │   ├── commands.go               #   extra `!meta` bot commands
│   │   ├── directmedia.go            #   DirectMediableNetwork (media proxy) [optional]
│   │   ├── push.go                   #   push notifications + background sync [optional]
│   │   ├── ids.go                    #   PortalKey/EventSender helpers (login-scoped)
│   │   └── dbmeta.go                 #   GetDBMetaTypes (custom metadata structs)
│   ├── msgconv/                      # message conversion, isolated from connector
│   │   ├── msgconv.go                #   MessageConverter struct + New()
│   │   ├── from-meta.go              #   remote → Matrix (ToMatrix)
│   │   ├── from-matrix.go            #   Matrix → remote (ToMeta → []socket.Task)
│   │   ├── from-whatsapp.go          #   [meta-specific E2EE path]
│   │   ├── to-whatsapp.go            #   [meta-specific E2EE path]
│   │   ├── igconv/                   #   [instagram-specific]
│   │   ├── mediadl/                  #   media transfer helpers
│   │   │   ├── download.go           #     HTTP download from CDN w/ browser-like headers
│   │   │   ├── reupload.go           #     remote URL → Matrix (streaming) or direct-media
│   │   │   ├── upload.go             #     Matrix → remote upload (Mercury endpoint)
│   │   │   └── contextkey.go         #     ctx keys for passing client/intent/portal down
│   │   └── textfmt/                  #   formatting pipeline
│   │       ├── from-matrix.go        #     MatrixHTMLParser (HTML → network markdown)
│   │       ├── from-meta.go          #     network text+mentions → Matrix HTML
│   │       ├── markdown.go           #     network markdown-ish → HTML renderer (+test)
│   │       └── mentions.go           #     mention placeholder machinery
│   ├── messagix/                     # PURE network client library (no bridgev2 imports)
│   │   ├── client.go                 #   Client, NewClient(cookies, logger, cfg)
│   │   ├── events.go                 #   Event_Ready/_PublishResponse/_SocketError/...
│   │   ├── socket.go, packets/, ...  #   MQTT-over-websocket transport
│   │   ├── table/                    #   Lightspeed "LSTable" data model
│   │   ├── socket/                   #   task payload types (SendMessageTask etc.)
│   │   ├── httpclient/               #   HTTP layer, config fetching, uploads
│   │   ├── cookies/cookies.go        #   Cookies type + required-cookie lists
│   │   ├── types/                    #   platform enum, config/user-info types
│   │   └── ...                       #   crypto, graphql, bloks, e2ee glue, endpoints
│   ├── metaid/                       # ID codec + DB metadata structs (tiny, dependency-light)
│   │   ├── ids.go                    #   networkid.UserID/PortalID/MessageID make/parse
│   │   ├── dbmeta.go                 #   UserLoginMetadata/PortalMetadata/... structs
│   │   └── mediaid.go                #   direct-media ID encoding [optional]
│   ├── metadb/                       # network-specific DB tables + embedded migrations
│   │   ├── database.go               #   MetaDB wrapping dbutil.Database.Child(...)
│   │   └── NN-*.sql                  #   embedded SQL upgrade files (00-latest-schema…)
│   ├── instameow/, igconnector/      # [instagram-specific, ignore]
├── go.mod / go.sum
├── build.sh                          # one-liner: go tool maubuild
├── Dockerfile / Dockerfile.ci / docker-run.sh
├── .github/workflows/go.yml          # lint-only CI (pre-commit)
├── .gitlab-ci.yml                    # real build/release CI (mau.dev)
├── .pre-commit-config.yaml           # goimports/vet/staticcheck/zerolog rules
├── ROADMAP.md                        # feature checklist (also linked from README)
└── CHANGELOG.md
```

**The layering rule** (worth copying exactly):

- `pkg/messagix` = standalone protocol client. Emits its own event structs through a
  single `EventHandler func(ctx context.Context, evt any)` callback
  (`pkg/messagix/client.go:28`). It never imports bridgev2.
- `pkg/connector` = adapter that implements bridgev2 interfaces and translates
  client-lib events into `bridgev2.RemoteEvent`s.
- `pkg/msgconv` = content conversion only; receives clients/intents via arguments or
  context values, so it's testable without a live connection.
- `pkg/metaid` = pure functions for `networkid.*` encoding plus the DB `Metadata`
  structs; imported by both connector and msgconv without cycles.
- `pkg/metadb` = extra SQL tables for network-specific state.
- `cmd/<name>/main.go` = ~30 lines.

For googlechat this maps to: `pkg/gchatmeow` (or similar client lib), `pkg/connector`,
`pkg/msgconv`, `pkg/gcid`, optional `pkg/gcdb`, `cmd/mautrix-googlechat`.

---

## 2. Connector wiring

### 2.1 Interface → file map

Every implemented interface is declared with a compile-time assertion right in the file
that implements it (`var _ bridgev2.X = (*T)(nil)`); this is the discovery index for the
whole connector. Full inventory:

**On `*MetaConnector` (the network singleton):**

| Interface | File:Line |
|---|---|
| `bridgev2.NetworkConnector` | `pkg/connector/connector.go:25` |
| `bridgev2.MaxFileSizeingNetwork` | `pkg/connector/connector.go:26` |
| `bridgev2.NetworkResettingNetwork` | `pkg/connector/connector.go:27` |
| `bridgev2.DirectMediableNetwork` | `pkg/connector/directmedia.go:27` |
| `bridgev2.IdentifierValidatingNetwork` | `pkg/connector/startchat.go:24` |
| `bridgev2.TransactionIDGeneratingNetwork` | `pkg/connector/handlematrix.go:45` |
| (implicitly `ConfigValidatingNetwork` via `ValidateConfig`) | `pkg/connector/config.go:116` |
| (implicitly `GetDBMetaTypes`) | `pkg/connector/dbmeta.go:9` |

**On `*MetaClient` (per-UserLogin):**

| Interface | File:Line |
|---|---|
| `bridgev2.NetworkAPI` | `pkg/connector/client.go:103` |
| `bridgev2.CredentialExportingNetworkAPI` | `pkg/connector/client.go:104` |
| `status.BridgeStateFiller` | `pkg/connector/client.go:105` |
| `bridgev2.EditHandlingNetworkAPI` | `pkg/connector/handlematrix.go:33` |
| `bridgev2.ReactionHandlingNetworkAPI` | `pkg/connector/handlematrix.go:34` |
| `bridgev2.RedactionHandlingNetworkAPI` | `pkg/connector/handlematrix.go:35` |
| `bridgev2.ReadReceiptHandlingNetworkAPI` | `pkg/connector/handlematrix.go:36` |
| `bridgev2.ChatViewingNetworkAPI` | `pkg/connector/handlematrix.go:37` |
| `bridgev2.TypingHandlingNetworkAPI` | `pkg/connector/handlematrix.go:38` |
| `bridgev2.MessageRequestAcceptingNetworkAPI` | `pkg/connector/handlematrix.go:39` |
| `bridgev2.DeleteChatHandlingNetworkAPI` | `pkg/connector/handlematrix.go:40` |
| `bridgev2.RoomNameHandlingNetworkAPI` | `pkg/connector/handlematrix.go:41` |
| `bridgev2.RoomAvatarHandlingNetworkAPI` | `pkg/connector/handlematrix.go:42` |
| `bridgev2.BackfillingNetworkAPI` | `pkg/connector/backfill.go:22` |
| `bridgev2.IdentifierResolvingNetworkAPI` | `pkg/connector/startchat.go:21` |
| `bridgev2.UserSearchingNetworkAPI` | `pkg/connector/startchat.go:22` |
| `bridgev2.GroupCreatingNetworkAPI` | `pkg/connector/startchat.go:23` |
| `bridgev2.PushableNetworkAPI` | `pkg/connector/push.go:42` |
| `bridgev2.BackgroundSyncingNetworkAPI` | `pkg/connector/push.go:43` |

**On login-process types:**

| Interface | File:Line |
|---|---|
| `bridgev2.LoginProcessCookies` on `*MetaCookieLogin` | `pkg/connector/login.go:168` |
| `bridgev2.LoginProcessUserInput` on `*MetaNativeLogin` | `pkg/connector/login.go:407` |
| `bridgev2.LoginProcessDisplayAndWait` on `*MetaNativeLogin` | `pkg/connector/login.go:408` |

**On RemoteEvent wrapper types (pkg/connector/events.go):**

| Type | Interfaces | Lines |
|---|---|---|
| `VerifyThreadExistsEvent` | `RemoteChatResyncWithInfo`, `RemoteEventThatMayCreatePortal`, `RemoteEventWithUncertainPortalReceiver` | 62–64 |
| `FBMessageEvent` | `RemoteMessage`, `RemoteEventWithUncertainPortalReceiver`, `RemoteEventWithTimestamp`, `RemoteEventWithStreamOrder` | 128–131 |
| `FBEditEvent` | `RemoteEdit` | 191 |
| `EnsureWAChatStateEvent` | `RemoteChatResyncWithInfo`, `RemoteEventThatMayCreatePortal` | 238–239 |
| `WAMessageEvent` | `RemoteMessage`, `RemoteEdit`, `RemoteEventWithTimestamp`, `RemoteReaction`, `RemoteReactionRemove`, `RemoteMessageRemove`, `RemoteEventWithStreamOrder` | 296–302 |
| `FBChatResync` | `RemoteChatResyncWithInfo`, `RemoteEventThatMayCreatePortal`, `RemoteChatResyncBackfillBundle`, `RemoteEventWithUncertainPortalReceiver` | 565–568 |
| `FBFolderResync` | `RemoteChatResyncWithInfo`, `RemoteEventThatMayCreatePortal` | 734–735 |

Simpler remote events (receipts, typing, reactions, deletes, chat-info changes) don't get
custom types — they use `maunium.net/go/mautrix/bridgev2/simplevent` structs
(`simplevent.Receipt`, `simplevent.Typing`, `simplevent.Reaction`,
`simplevent.MessageRemove`, `simplevent.ChatDelete`, `simplevent.ChatInfoChange`), built
in `pkg/connector/handlemeta.go` (e.g. `handleMarkThreadRead` at line 477,
`handleTypingIndicator` at 513, `wrapMessageDelete` at 534, `wrapReaction` at 638).

### 2.2 Connector lifecycle

`MetaConnector` (`pkg/connector/connector.go:16-22`) holds: `Bridge *bridgev2.Bridge`,
`Config Config`, `MsgConv *msgconv.MessageConverter`, network DB (`*metadb.MetaDB`), and
the whatsmeow device store (meta-specific).

- `Init(bridge)` (connector.go:30): stores the bridge, registers extra bot commands via
  `m.Bridge.Commands.(*commands.Processor).AddHandlers(cmdToggleEncryption)`
  (connector.go:37), constructs `metadb.New(bridge.ID, bridge.DB.Database, log)` and
  `msgconv.New(bridge, db)`.
- `Start(ctx)` (connector.go:43): runs DB migrations; upgrade failures are wrapped in
  `bridgev2.DBUpgradeError{Err: err, Section: "meta"}`.
- `GetName()` (connector.go:60): returns `bridgev2.BridgeName{DisplayName, NetworkURL,
  NetworkIcon (mxc://), NetworkID, BeeperBridgeType, DefaultPort}` — meta switches on
  config mode; googlechat needs just one.
- `LoadUserLogin(ctx, login)` (client.go:82): constructs a `*MetaClient` from
  `login.Metadata.(*metaid.UserLoginMetadata)` and assigns `login.Client = c`. **No I/O
  here** — connection happens in `Connect`.
- `SetMaxFileSize(int64)` (connector.go:56) pipes bridge-computed homeserver limit into
  the MessageConverter.

### 2.3 The per-login client and connection management

`MetaClient` (`pkg/connector/client.go:32-72`) owns the messagix client and a pile of
connection-state machinery worth studying:

- `Connect(ctx)` (client.go:163) guards with `connectLock.TryLock()`, sends
  `status.StateConnecting`, then `connectWithRetry` (client.go:187) with exponential
  backoff (`1<<attempts` seconds, `MaxConnectRetries = 10` at client.go:185).
- Error classification → bridge states: `connectWithRetry` maps client-lib sentinel
  errors (`httpclient.ErrTokenInvalidated`, `ErrChallengeRequired`, ...) to
  `status.StateBadCredentials` / `StateTransientDisconnect` / `StateUnknownError` with
  stable `status.BridgeStateErrorCode` constants defined in
  `pkg/connector/handlemeta.go:27-49`, and human-readable strings registered via
  `status.BridgeStateHumanErrors.Update(...)` in an `init()` (handlemeta.go:51-71).
  On invalid cookies it *clears the stored cookies* and saves the login
  (client.go:253-257).
- `IsLoggedIn()` (client.go:568) is a cheap local check.
- `Disconnect()` (client.go:524) tears down socket + background loops via
  `atomic.Pointer[context.CancelFunc]` swap idiom (used for connect attempt, table loop,
  and periodic reconnect — see client.go:39-54).
- `LogoutRemote(ctx)` (client.go:576) clears credentials.
- `FullReconnect()` (client.go:603) + `periodicReconnect()` (client.go:389) driven by
  `force_refresh_interval_seconds` config; `canReconnect()` rate-limits by
  `min_full_reconnect_interval_seconds` (client.go:591).
- `FillBridgeState` (client.go:624) merges two sub-connection states — meta-specific,
  but the pattern of a `status.BridgeStateFiller` adding `state.Info["mode"]` extras is
  reusable.
- `ExportCredentials` (client.go:156) returns cookies for bridge-to-bridge migration;
  paired with `CreateUserLoginFromCredentials` (login.go:72) which feeds them back
  through `SubmitCookies`.

### 2.4 Cookie login flow (the part googlechat should copy nearly verbatim)

`pkg/connector/login.go`:

1. `GetLoginFlows()` (login.go:120) returns `[]bridgev2.LoginFlow{{Name, Description,
   ID}}`; `CreateLogin(ctx, user, flowID)` (login.go:40) returns a
   `bridgev2.LoginProcess`.
2. `MetaCookieLogin.Start(ctx)` (login.go:188) returns a single
   `*bridgev2.LoginStep{Type: bridgev2.LoginStepTypeCookies, StepID:
   "fi.mau.meta.cookies", Instructions: "...", CookiesParams:
   &bridgev2.LoginCookiesParams{...}}` where `CookiesParams` carries:
   - `UserAgent` (the UA the embedded webview should use),
   - `URL` — page to open (e.g. `https://www.messenger.com/?no_redirect=true`),
   - `Fields []bridgev2.LoginCookieField` — built by `cookieListToFields`
     (login.go:170): each field has `ID`, `Required: true`, and `Sources:
     []bridgev2.LoginCookieFieldSource{{Type: bridgev2.LoginCookieTypeCookie, Name,
     CookieDomain}}`,
   - `WaitForURLPattern` — regex telling clients when login inside the webview finished.
   The required-cookie lists live in the client lib:
   `cookies.FBRequiredCookies = []MetaCookieName{FBCookieXS, FBCookieCUser, MetaCookieDatr}`
   (`pkg/messagix/cookies/cookies.go:46`).
3. `SubmitCookies(ctx, map[string]string)` (login.go:317) builds a `cookies.Cookies`,
   verifies `GetMissingCookieNames()`, constructs a client, and calls the shared
   `loginWithCookies` (login.go:238) which:
   - performs the first authenticated fetch (`client.LoadMessagesPage`) to validate the
     session and learn the user's remote ID/name,
   - maps client-lib errors to typed `bridgev2.RespError`s with custom `ErrCode`s
     (login.go:218-225, e.g. `FI.MAU.META_MISSING_COOKIES`),
   - creates the login: `bridgeUser.NewLogin(ctx, &database.UserLogin{ID: loginID,
     RemoteName, RemoteProfile, Metadata: &metaid.UserLoginMetadata{Platform, Cookies,
     LoginUA}}, nil)` (login.go:280-291),
   - captures the provisioning request's User-Agent from
     `ctx.Value("fi.mau.provision.request").(*http.Request)` (login.go:276),
   - **reuses the already-warmed client**: assigns it onto the newly created
     `ul.Client.(*MetaClient)` and kicks off `go metaClient.connectWithTable(...)` so the
     first page load isn't repeated (login.go:296-305),
   - returns `LoginStepTypeComplete` with `CompleteParams{UserLoginID, UserLogin}`.

The second flow (`MetaNativeLogin`, login.go:338-408) shows the
`LoginProcessUserInput` + `LoginProcessDisplayAndWait` combination for interactive
username/password/2FA logins — useful reference if googlechat ever adds an OAuth-style
flow, otherwise skippable.

### 2.5 Remote events: client lib → bridgev2

Flow (all in `pkg/connector`):

1. The client lib invokes the registered handler:
   `m.Client.SetEventHandler(m.handleMetaEvent)` (client.go:152).
2. `handleMetaEvent` (handlemeta.go:73) type-switches on client-lib event structs:
   `*messagix.Event_PublishResponse` (data), `*messagix.Event_Ready` (connected — sends
   `StateConnected`, flips an `exsync.Event` "connectWaiter", kicks off thread backfill),
   `*messagix.Event_SocketError` (→ `StateTransientDisconnect`),
   `*messagix.Event_Reconnected`, `*messagix.Event_PermanentError`.
3. Data events are parsed synchronously (`parseTable`, handlemeta.go:320) into a slice of
   `bridgev2.RemoteEvent`, then queued on a **buffered channel**
   (`parsedTables chan *parsedTable`, cap 16, client.go:89) via `parseAndQueueTable`
   (handlemeta.go:170), and drained by a single goroutine `handleTableLoop`
   (handlemeta.go:198).
4. The loop calls `m.UserLogin.QueueRemoteEvent(evt)` per event (handlemeta.go:292) and
   aborts the batch if `res.Success` is false. Ghost profile syncs happen inline before
   queueing (`syncGhost`, handlemeta.go:309, which calls
   `ghost.UpdateInfo(ctx, m.wrapUserInfo(info))`).
5. Ordering matters and is hand-managed in `parseTable`: chat deletes first, then chat
   resyncs, then message inserts, then receipts/typing/reactions (handlemeta.go:416-456).
   Events that arrive in the same batch as a chat resync are *merged into* the resync
   (`FBChatResync.Members`, `.Info`, `.Backfill`) instead of being emitted separately —
   see the `tk.Sync != nil` branches in e.g. `handleAddParticipant` (handlemeta.go:709)
   and `handleUpsertMessages` (backfill.go:87-91).

Key event-implementation details:

- `FBMessageEvent.ConvertMessage` (events.go:176) delegates entirely to
  `MsgConv.ToMatrix(...)` — the RemoteEvent is a thin shim.
- `GetStreamOrder` returns the remote millisecond timestamp so bridgev2 can order/dedup
  (events.go:162).
- Portal keys: `makeFBPortalKey` (ids.go:42) sets `Receiver = m.UserLogin.ID` only for
  DMs/unknown types or when `Bridge.Config.SplitPortals` is set; group threads are shared
  portals with empty receiver. `RemoteEventWithUncertainPortalReceiver` covers the case
  where the thread type isn't known yet.
- Event senders: `makeEventSender(id int64)` (ids.go:20) fills
  `{IsFromMe, Sender, SenderLogin}`; `selfEventSender()` (ids.go:12) for own actions.
- "Note to self" chats need the two-member trick `makeNoteToSelfMembers`
  (chatinfo.go:186-193).
- Edits keyed only by message ID (no thread ID) are resolved via
  `Bridge.DB.Message.GetFirstPartByID` before queueing (handlemeta.go:791-815).

### 2.6 Matrix events: bridgev2 → client lib

All in `pkg/connector/handlematrix.go`:

- `HandleMatrixMessage(ctx, msg *bridgev2.MatrixMessage)` (line 68):
  1. cheap logged-in check → `bridgev2.ErrNotLoggedIn`,
  2. wait for connection readiness with `connectWaiter.WaitTimeout(ConnectWaitTimeout)`
     (1 min; line 56) → `ErrNotConnected` (a
     `bridgev2.WrapErrorInStatus(...).WithErrorAsMessage().WithSendNotice(true)` error,
     lines 52-54),
  3. convert with `MsgConv.ToMeta(...)` → network payload(s),
  4. send with retry loop (5 attempts, line 149),
  5. parse the network's echo/ack to extract the remote message ID, and return
     `&bridgev2.MatrixMessageResponse{DB: &database.Message{ID, SenderID, Timestamp},
     StreamOrder: parsedTS}` (lines 212-219). Echo dedup is handled by bridgev2 via the
     returned DB message ID plus `GenerateTransactionID` (line 47), which pre-generates
     the network transaction ID (`otid`) so the send and the incoming echo match up
     (`getOTID`, line 58).
- `PreHandleMatrixReaction` (line 223) returns
  `bridgev2.MatrixReactionPreResponse{SenderID, Emoji: variationselector.Remove(...),
  MaxReactions: 1}` — bridgev2 uses this to enforce the one-reaction-per-user rule before
  calling `HandleMatrixReaction` (line 274).
- `HandleMatrixEdit` (line 361), `HandleMatrixMessageRemove` (line 476),
  `HandleMatrixReadReceipt` (line 508 — converts Matrix receipt range into per-message
  network receipts using `DB.Message.GetMessagesBetweenTimeQuery`),
  `HandleMatrixTyping` (line 610), `HandleMatrixViewingChat` (line 582),
  `HandleMatrixRoomName`/`RoomAvatar` (lines 728, 754), `HandleMatrixMembership`
  (line 813), `HandleMatrixDeleteChat` (line 657), `HandleMatrixAcceptMessageRequest`
  (line 694).

### 2.7 Capabilities

`pkg/connector/capabilities.go`:

- `GetCapabilities()` on the connector (line 53) → `bridgev2.NetworkGeneralCapabilities`
  (`DisappearingMessages`, `ImplicitReadReceipts`, provisioning caps incl. `CreateDM`,
  `Search`, `GroupCreation` — lines 35-51).
- `GetBridgeInfoVersion()` (line 57) returns `(info, caps)` version ints — bump caps
  when room features change.
- Per-portal `GetCapabilities(ctx, portal)` on the client (line 231) returns
  `*event.RoomFeatures`; meta builds one base `metaCaps` (line 81: formatting map, file
  type/mime/size matrix, `Reply`, `Edit` + `EditMaxCount`/`EditMaxAge`, `Delete`,
  `Reaction`+`ReactionCount`, `TypingNotifications`, ...) and derives variants in
  `init()` (line 181) via `.Clone()` with distinct `ID` strings (e.g.
  `"fi.mau.meta.capabilities.2026_07_07+e2e"`). The `ID` includes a date and a
  `+ffmpeg` suffix depending on `ffmpeg.Supported()` (`capID()`, line 73).

### 2.8 DB metadata

`GetDBMetaTypes` (`pkg/connector/dbmeta.go:9`) registers constructor funcs for
`Portal`, `Ghost`, `Message`, `UserLogin` metadata (Reaction nil). The structs live in
`pkg/metaid/dbmeta.go`: `UserLoginMetadata` (line 30 — **cookies are stored here as
JSON**, plus `WADeviceID`, push keys, `BackfillCompleted` flag), `PortalMetadata`
(line 51 — thread type etc.), `MessageMetadata` (line 20 — `EditTimestamp`,
`DirectMediaMeta`), `GhostMetadata` (line 25 — username).

Additional relational state goes into a **child database**: `metadb.New` does
`db.Child("meta_version", table, dbutil.ZeroLogger(log))` with `//go:embed *.sql`
migrations (`pkg/metadb/database.go:49-68`); tables are keyed by `bridge_id` where
needed. Meta uses it for subthread mapping, IG↔FB ID mapping, reconnection-state cache,
IG reaction mapping. Googlechat probably needs little or none of this initially, but the
pattern (child DB + embedded migrations) is the right one when it does.

---

## 3. Message conversion architecture (`pkg/msgconv`)

`MessageConverter` (`pkg/msgconv/msgconv.go:27-46`) is a plain struct holding
`Bridge`, `MaxFileSize`, `HTMLParser`, feature flags, and the network DB. Constructed
once in `MetaConnector.Init`.

### 3.1 Remote → Matrix (`from-meta.go`)

`ToMatrix(ctx, portal, client, intent, messageID, msg, disableXMA) *bridgev2.ConvertedMessage`
(from-meta.go:89):

- Stuffs the client, intent, portal, message ID, and part ID into the context via typed
  keys (`pkg/msgconv/mediadl/contextkey.go`) so deep helpers can upload media without
  threading 6 params everywhere.
- Builds `ConvertedMessage.Parts` as an ordered list: attachments first (each with a
  stable `networkid.PartID` like `"blob_attachment_0"`), stickers, then one text part
  (mentions parsed → HTML), URL previews as `content.BeeperLinkPreviews`.
- Reply/thread linkage: sets `cm.ReplyTo = &networkid.MessageOptionalPartID{MessageID:
  ...}` and `ThreadRoot` (visible later in the file), disappearing-message settings, etc.

### 3.2 Media transfer: remote → Matrix is a **streamed reupload**

`mediadl.ReuploadFileToMatrix(ctx, params ReuploadParams)`
(`pkg/msgconv/mediadl/reupload.go:107`):

- Default mode: download from the network CDN (`DownloadMedia`,
  `mediadl/download.go` — dedicated `*http.Client` configured from
  `Bridge.GetHTTPClientSettings()`, browser-mimicking headers, 5-minute global timeout)
  and stream it straight into
  `intent.UploadMediaStream(ctx, portal.MXID, size, requireFile, func(dest io.Writer) (*bridgev2.FileStreamResult, error))`
  (reupload.go:235). **No full in-memory buffering**; the callback copies the body, and
  only seeks back when it needs mime sniffing (`mimetype.DetectReader`), image
  dimensions (`image.DecodeConfig`), or ffmpeg voice-note conversion to ogg/opus
  (`ffmpeg.ConvertPath`, reupload.go:265 — returns `ReplacementFile`).
- Alternate mode (`params.DirectMedia`): skip download entirely; mint an
  `mxc://` via `portal.Bridge.Matrix.GenerateContentURI(ctx, mediaID)` and stash the
  remote URL + refresh info as `MessageMetadata.DirectMediaMeta`
  (reupload.go:182-216). Served later by `MetaConnector.Download` in
  `pkg/connector/directmedia.go:33`, including URL-expiry refresh. This is the
  Beeper-style media proxy — **optional**, only if `DirectMediableNetwork` is wanted.
- `bridgev2.ErrMediaDownloadFailed` / `ErrMediaConvertFailed` wrapping gives users
  proper per-message status notices.

### 3.3 Matrix → remote (`from-matrix.go`)

`ToMeta(...) ([]socket.Task, error)` (from-matrix.go:36) returns *network request
payloads*, not side effects — the connector executes them. Media path:
`mediadl.ReuploadFileToMeta` (`mediadl/upload.go:38`) downloads the Matrix media with
`portal.Bridge.Bot.DownloadMedia(ctx, content.URL, content.File)` (handles encrypted
files transparently), sniffs mime if missing, converts MSC1767 waveforms, and uploads to
the network, returning the network attachment ID. Unsupported msgtypes return
`fmt.Errorf("%w %s", bridgev2.ErrUnsupportedMessageType, content.MsgType)`.

### 3.4 Formatting pipeline (`pkg/msgconv/textfmt`)

- **Matrix → network**: `MatrixHTMLParser` (from-matrix.go:36-68) wraps
  `maunium.net/go/mautrix/format.HTMLParser` with per-construct converters
  (bold→`*`, italic→`_`, strike→`~`, inline code→backticks, fenced blocks) and a
  `PillConverter` that resolves ghost/user MXIDs to network user IDs. Mentions use a
  random **placeholder locator** inserted during parse, then post-processed to compute
  exact byte offsets/lengths for the network mention format (from-matrix.go:70-101).
  Google Chat's annotation-based mention format (offset+length) is directly analogous.
- **Network → Matrix**: `MetaToMatrixText(ctx, text, mentions, getBasicUserInfo)`
  (from-meta.go:35-74 in textfmt) renders network markdown to HTML
  (`markdown.go`, 457 lines, has a unit test `markdown_test.go` — one of only two test
  files in the repo), replaces mention spans with `<a href="matrix.to...">` pills, sets
  `event.Mentions`, and *omits* `FormattedBody` when the HTML is equivalent to the plain
  body (from-meta.go:60).

---

## 4. Backfill implementation

Two cooperating layers:

### 4.1 `FetchMessages` (bridgev2.BackfillingNetworkAPI) — `pkg/connector/backfill.go`

Meta's history API is asynchronous (request over socket, results arrive as regular
table events), so the connector bridges async→sync with a **collector**:

- `FetchMessages(ctx, params bridgev2.FetchMessagesParams)` (backfill.go:183):
  - `params.Forward` + `params.BundledData` + `params.AnchorMessage` + `params.Count`
    drive the logic. Forward backfill *without* bundled data is skipped
    (backfill.go:194-197).
  - Registers a `BackfillCollector{UpsertMessages, MaxMessages, Forward, Anchor, Done}`
    (backfill.go:30-36) keyed by thread, sends the history request
    (`requestMoreHistory`, backfill.go:137), and waits on a done-channel with timeout
    (30s normal, 15s forward, 8s in background-connect mode; lines 25-28) and re-request
    ticker.
  - Incoming pages land in `handleUpsertMessages` (backfill.go:40), which either feeds
    the active collector (requesting more pages until count/anchor/end reached), attaches
    the upsert to an in-flight `FBChatResync` as bundled backfill, or emits a standalone
    resync event.
- `wrapBackfillEvents` (backfill.go:300) normalizes: reverse + stable sort by
  `PrimarySortKey/SecondarySortKey`, dedupe by message ID (`slices.CompactFunc`), trim
  at the anchor, then map each message to `&bridgev2.BackfillMessage{ConvertedMessage
  (via MsgConv.ToMatrix), Sender, ID, Timestamp, StreamOrder, Reactions:
  []*bridgev2.BackfillReaction{...}}`. Per-message intent comes from
  `portal.GetIntentFor(ctx, sender, m.UserLogin, bridgev2.RemoteEventBackfill)`
  (backfill.go:338). Returns `&bridgev2.FetchMessagesResponse{Messages, HasMore,
  MarkRead}`.

### 4.2 Bundled backfill on chat resync

`FBChatResync` implements `bridgev2.RemoteChatResyncBackfillBundle`
(events.go:696-698: `GetBundledBackfillData() any` returns the `*table.UpsertMessages`)
and `CheckNeedsBackfill(ctx, lastMessage)` (events.go:666-694) which compares the
bundle's max message ID/timestamp and the thread's last-activity timestamp against the
last bridged message to decide whether bridgev2 should call `FetchMessages` forward.
This is the mechanism that turns "initial sync gave me the last N messages of each
thread" into proper backfill.

### 4.3 Thread (chat-list) backfill — `pkg/connector/threadbackfill.go`

After the first `Event_Ready`, `StartThreadBackfill` (threadbackfill.go:12, launched from
handlemeta.go:96-101) pages through the server-side chat list
(`m.Client.FetchMoreThreads`), pushing each page through the normal
`parseAndQueueTable` flow (so portals + bundled backfill happen organically), with
config knobs `thread_backfill.batch_count` / `batch_delay` and a per-login completion
flag persisted in `UserLoginMetadata.BackfillCompleted` (metaid/dbmeta.go:39,
threadbackfill.go:99-104). Termination is defensive: no-more flag, unchanged min key, or
batch limit.

For googlechat: the paginated `list_topics`/`list_messages` API maps cleanly onto
`FetchMessages` (it's synchronous request/response, so **no collector needed** — that
whole async dance is a Meta-ism), and world sync maps onto the thread-backfill loop.

---

## 5. go.mod: key dependencies

```
module go.mau.fi/mautrix-meta
go 1.25.0        (toolchain go1.26.4)
tool go.mau.fi/util/cmd/maubuild          ← Go 1.24+ tool directive for the build script
```

Direct deps that matter for any bridge (versions as pinned at HEAD):

| Dep | Version | Role |
|---|---|---|
| `maunium.net/go/mautrix` | `v0.28.2-0.20260708122614-d9c352f407dd` | bridgev2 framework (pinned to a commit past the release tag — they track main) |
| `go.mau.fi/util` | `v0.9.11-0.20260625130032-7f1066352431` | dbutil, configupgrade, exsync, ffmpeg, ptr, jsontime, variationselector, exhttp, maubuild |
| `github.com/rs/zerolog` | v1.35.1 | logging (the only logger used) |
| `gopkg.in/yaml.v3` | v3.0.1 | config |
| `google.golang.org/protobuf` | v1.36.11 | protobuf runtime (googlechat's API is protobuf — same dep) |
| `github.com/gabriel-vasile/mimetype` | v1.4.13 | mime sniffing for media |
| `golang.org/x/image` | v0.42.0 | webp decode for image dimensions |
| `golang.org/x/net`, `x/crypto`, `x/exp` | — | transport/utility |
| `github.com/google/uuid` | v1.6.0 | ids |
| indirect: `github.com/mattn/go-sqlite3` v1.14.45, `github.com/lib/pq` | — | pulled by dbutil (SQLite + Postgres) |
| indirect: `github.com/yuin/goldmark` | v1.8.2 | markdown (via mautrix/format) |

Meta-specific (do NOT carry over): `go.mau.fi/whatsmeow`, `go.mau.fi/libsignal`,
`github.com/apache/thrift`, `github.com/refraction-networking/utls`,
`github.com/imroc/req/v3` (replaced by beeper fork via `replace` directive, go.mod:65),
`github.com/coder/websocket` (meta's MQTT-over-WS; googlechat uses long-polling
`channel` HTTP instead), `beeper/poly1305`, `tidwall/gjson`.

Note: mautrix-go still links libolm (CGO) in this configuration — the Dockerfile installs
`olm-dev`/`olm` and CI installs `libolm-dev`. Check whether the goolm build tag is
acceptable for googlechat to avoid CGO.

---

## 6. Build / packaging / CI

### main.go (cmd/mautrix-meta/main.go — copy this file verbatim, adjust strings)

```go
var (
    Tag       = "unknown"   // filled by -X main.Tag
    Commit    = "unknown"
    BuildTime = "unknown"
)

var m = mxmain.BridgeMain{
    Name:        "mautrix-meta",
    URL:         "https://github.com/mautrix/meta",
    Description: "A Matrix-Meta puppeting bridge.",
    Version:     "26.06",          // CalVer YY.MM
    SemCalVer:   true,
    Connector:   &connector.MetaConnector{},
}

func main() {
    m.InitVersion(Tag, Commit, BuildTime)
    m.Run()
}
```

`mxmain.BridgeMain` (from `maunium.net/go/mautrix/bridgev2/matrix/mxmain`) provides the
whole CLI: `-c config`, `-e` example-config generation, `-g`/`-r` registration
generation, `-ec` (used by CI to render the merged example config).

### build.sh

One line: `BINARY_NAME=mautrix-meta go tool maubuild "$@"`. `maubuild` (declared via the
`tool` directive in go.mod) computes the ldflags (`-X main.Tag/Commit/BuildTime` and
`-X 'maunium.net/go/mautrix.GoModVersion=...'`) automatically. The GitLab CI does the
same by hand:

```
GO_LDFLAGS="-s -w -linkmode external -extldflags -static
  -X main.Tag=$CI_COMMIT_TAG -X main.Commit=$CI_COMMIT_SHA
  -X 'main.BuildTime=`date -Iseconds`'
  -X 'maunium.net/go/mautrix.GoModVersion=$MAUTRIX_VERSION'"
```
where `MAUTRIX_VERSION` is grepped out of go.mod (.gitlab-ci.yml `.build` anchor).

### Dockerfile

Two-stage: `golang:1-alpine3.23` builder (`apk add git ca-certificates build-base
su-exec olm-dev`, `RUN ./build.sh`) → `alpine:3.23` runtime with
`ffmpeg su-exec ca-certificates olm bash jq yq-go curl`, binary + `docker-run.sh`,
`VOLUME /data`. `docker-run.sh` implements the standard mautrix first-run dance:
generate config if missing → exit; generate registration if missing → exit; else
`fixperms` + `exec su-exec $UID:$GID /usr/bin/mautrix-meta`. `Dockerfile.ci` is
runtime-only (copies a binary built in CI).

### CI

- `.github/workflows/go.yml`: **lint only** — matrix Go 1.25/1.26, installs libolm,
  goimports + staticcheck, runs `pre-commit/action`. No test job (there are only 2 test
  files in the repo).
- `.pre-commit-config.yaml`: trailing-whitespace/eof fixers, `go-imports -local
  go.mau.fi/mautrix-meta`, `go-vet-mod`, `go-staticcheck-repo-mod`, and beeper's zerolog
  hooks (`zerolog-ban-msgf`, `zerolog-use-stringer`).
- `.gitlab-ci.yml` (mau.dev): real builds for linux amd64/arm64/arm + macOS arm64,
  static binaries, docker multi-arch manifests, and a "deploy config" job that runs the
  binary with `-ec` to publish the rendered example config to docs.mau.fi. For our
  project GitHub Actions with a build+lint matrix is enough; the GitLab file is only
  useful as a checklist of ldflags and the `-ec` config-rendering trick.

---

## 7. Config: example-config.yaml structure

Network-specific config is **only the `network:` section**; bridgev2/mxmain supplies
everything else (homeserver, appservice, database, logging, encryption, permissions...).
The file `pkg/connector/example-config.yaml` is the *contents* of that section, embedded
via `//go:embed example-config.yaml` (config.go:16-17) and returned from:

```go
func (m *MetaConnector) GetConfig() (string, any, up.Upgrader) {
    return ExampleConfig, &m.Config, up.SimpleUpgrader(upgradeConfig)
}
```
(config.go:112). The `up.Upgrader` (`go.mau.fi/util/configupgrade`) — `upgradeConfig`
(config.go:87-110) — explicitly `helper.Copy(...)`s every key so user configs are
migrated across upgrades; **every new config key needs a Copy line**. Types:
`up.Str`, `up.Bool`, `up.Int`, `up.List`, `up.Str|up.Null`, nested via variadic path
(`helper.Copy(up.Int, "thread_backfill", "batch_count")`).

Patterns in the meta config worth reusing for googlechat:

- `displayname_template` — Go `text/template` compiled in `PostProcess()`
  (config.go:70-85), executed by `FormatDisplayname(params)` (config.go:129) — used by
  `wrapUserInfo` in userinfo.go:57.
- `UnmarshalYAML` wrapper type (`type umConfig Config`, config.go:60-68) to run
  post-processing/validation on decode.
- `ValidateConfig() error` (config.go:116) for startup-time validation.
- Backfill knobs under a nested key (`thread_backfill: {batch_count, batch_delay}`).
- Proxy support (`proxy`, `get_proxy_from` returning `{"proxy_url": ...}` JSON —
  client.go:108-142) — optional but cheap to keep.

---

## 8. What to copy verbatim vs. what NOT to copy

### Copy nearly verbatim for googlechat

1. **The five-package layout** (`cmd/`, `pkg/connector`, `pkg/msgconv`,
   `pkg/<netid>` for ID/metadata, client lib package) and the "client lib never imports
   bridgev2" rule.
2. **`cmd/.../main.go`, `build.sh` (`go tool maubuild`), Dockerfile, docker-run.sh,
   .pre-commit-config.yaml** — mechanical, just rename. Keep CalVer + `SemCalVer: true`.
3. **The cookie login flow skeleton** (`login.go`): `GetLoginFlows` → `CreateLogin` →
   `Start` returning `LoginStepTypeCookies` with
   `LoginCookiesParams{URL, UserAgent, Fields (cookie sources w/ domain),
   WaitForURLPattern}` → `SubmitCookies` → validate by making the first authenticated
   API call → `bridgeUser.NewLogin(...)` with cookies in `UserLoginMetadata` →
   `LoginStepTypeComplete`, reusing the warmed client and connecting in a goroutine.
   For googlechat the cookie list is the Google auth cookies used by the Python bridge
   (`dynamite` cookies on `chat.google.com` / `.google.com` domain — define the
   equivalent of `FBRequiredCookies` in the client lib's `cookies` package).
   Also implement `CredentialExportingNetworkAPI` + `CreateUserLoginFromCredentials`
   for migration tooling (that pair is how old-bridge → new-bridge credential import
   works).
4. **Interface-assertion style** (`var _ bridgev2.X = (*T)(nil)` at top of the file that
   implements it) — makes the codebase greppable and compile-checked.
5. **Event pipeline shape**: client-lib events → type-switch handler → parse into
   `[]bridgev2.RemoteEvent` → buffered channel → single dispatcher goroutine calling
   `UserLogin.QueueRemoteEvent`, with `simplevent.*` for receipts/typing/reactions and
   custom structs only for messages/edits/resyncs.
6. **Connection-state conventions**: `status.BridgeStateErrorCode` constants + human
   map in `init()`, `StateConnecting`→`StateConnected` on ready,
   `StateTransientDisconnect` on socket error, `StateBadCredentials` + clearing stored
   cookies on auth failure, `exsync.Event` connect-waiters gating Matrix→remote
   handlers with a 1-minute timeout, `atomic.Pointer[context.CancelFunc]` for
   cancelable background loops.
7. **msgconv structure**: converter struct constructed in `Init`; `ToMatrix` returning
   `*bridgev2.ConvertedMessage` with stable PartIDs; conversion returning payloads for
   the connector to send (not sending itself); context keys for intent/portal/client.
8. **Streaming media reupload** via `intent.UploadMediaStream` incl. ffmpeg voice
   conversion and `bridgev2.ErrMedia*` sentinel wrapping; Matrix media download via
   `portal.Bridge.Bot.DownloadMedia`.
9. **Formatting**: `format.HTMLParser` with `PillConverter` + placeholder-locator
   mention offset computation (Google Chat annotations need exactly this); the
   "omit FormattedBody when identical to plain body" check.
10. **Capabilities**: base `event.RoomFeatures` + `Clone()` variants, dated capability
    `ID`, `supportedIfFFmpeg()` gate, `GetBridgeInfoVersion`.
11. **Config plumbing**: `//go:embed example-config.yaml`, `GetConfig` +
    `configupgrade.SimpleUpgrader`, `displayname_template`, `ValidateConfig`.
12. **Backfill surface**: `BackfillingNetworkAPI.FetchMessages` +
    `RemoteChatResyncBackfillBundle`/`CheckNeedsBackfill` on the chat-resync event +
    post-connect chat-list pagination loop with `BackfillCompleted` flag in login
    metadata and batch_count/batch_delay config.
13. **`GetDBMetaTypes`** + metadata structs in the small `<netid>` package; child DB
    with embedded `.sql` migrations *if* extra tables are needed.

### Do NOT copy (Meta-specific complexity)

1. **Everything whatsmeow/E2EE**: `DeviceStore`, `connectE2EE`, `handlewhatsapp.go`,
   `to/from-whatsapp.go`, dual `metaState`/`waState` + `FillBridgeState` worst-case
   merging, WA JID portal keys, `toggle-encryption` command. Google Chat has one
   transport; a single bridge state suffices (a trivial `BridgeStateFiller` may still be
   useful for extra `Info` fields).
2. **The LSTable/collector machinery**: `parseTable`'s 150-line ordering dance,
   `BackfillCollector`, `handleUpsertMessages`, `UpdateExistingMessageRange` handling —
   all consequences of Meta's async database-sync protocol. Google Chat's request/
   response API means `FetchMessages` can just call the API and convert.
3. **Dual-platform mode switching** (`mode`, `allowed_modes`, `types.Platform`
   everywhere, per-mode `GetName`, the whole `igconnector`/`instameow` second stack,
   second binary in cmd/).
4. **XMA attachment handling** and `disable_xma_*` config, story/reel refresh logic in
   directmedia.go.
5. **Messenger-Lite native login** (`MetaNativeLogin`, bloks package) — Meta-proprietary.
6. **The `editChannels` "cursed extra edits" hack** (handlematrix.go:388-444) and push
   notification decryption (`push.go`, `pushcrypto`) — server-quirk workarounds; only
   port push support if Beeper-style background sync is a goal.
7. **`meta_instagram_*` DB tables**, reconnection-state cache
   (`cache_connection_state`) — driven by Meta's expensive page-load handshake; Google
   Chat login/registration is cheap enough not to need it (evaluate after measuring).
8. **utls/req fork, thrift, DGW, MQTT packet layer** — transport specifics.
9. **GitLab CI file** — reference only, unless publishing to mau.dev infra.

### Gaps in meta to be aware of (don't imitate)

- Almost no unit tests (only `textfmt/markdown_test.go` and a bloks test). For a
  reimplementation project, msgconv and ID codecs deserve tests from day one.
- `events.go` `WAMessageEvent.GetType` logs through `zerolog.Ctx(context.TODO())`
  (events.go:406, marked FIXME) — RemoteEvent interfaces don't get a ctx in `GetType`;
  design event types so type decisions don't need logging.
