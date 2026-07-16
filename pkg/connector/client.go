package connector

// GChatClient is the per-UserLogin bridgev2.NetworkAPI implementation. It
// owns exactly one gchatmeow.Client at a time (c.conn) and translates its
// connection-state callbacks into bridgev2 BridgeState updates (see
// bridgestate.go's connStateToBridgeState and docs/research/04 §4.11 / 07
// §1.3 "Connection lifecycle").
//
// The conn field is deliberately NOT named "Client" (as it was through Task
// 10): *GChatClient itself has a Connect method (the bridgev2.NetworkAPI
// entry point below), so "gc.Client.Connect(ctx)" (the real gchatmeow
// supervision loop) and "gc.Connect(ctx)" (this type's own method) are two
// entirely different things that both compile -- an earlier revision of
// login.go's attachAndConnect called out exactly this confusion as a P0 risk.
// Renaming the field to conn removes the naming collision that made the two
// easy to conflate.
import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv"
)

type GChatClient struct {
	// Main is this login's owning connector, giving access to Config
	// (e.g. Config.InitialChatSync in sync.go, Config.FormatDisplayname in
	// userinfo.go) without reaching through UserLogin.Bridge.Network's
	// interface type on every call. Populated by GChatConnector.LoadUserLogin
	// (connector.go); left nil in tests that construct a bare *GChatClient
	// directly (matching UserLogin's own "often nil in tests" pattern in this
	// file -- only the methods that need it (sync.go, userinfo.go) require a
	// real one).
	Main      *GChatConnector
	UserLogin *bridgev2.UserLogin

	// mu guards conn, lastState, and initialSyncDone, which the
	// OnConnectionState callback (running on conn's own Connect goroutine)
	// and bridgev2's calling goroutine (Connect/Disconnect/IsLoggedIn) may
	// touch concurrently.
	mu        sync.Mutex
	conn      *gchatmeow.Client
	lastState status.BridgeStateEvent
	// initialSyncDone latches true the first time handleConnState's
	// Connected branch runs syncChats for the currently-installed conn, and
	// is reset to false by wireAndStart whenever a new conn is installed
	// (i.e. a fresh session bootstrap). See shouldSyncOnConnect's doc
	// comment for why this gate exists.
	initialSyncDone bool
	// syncInProgress is true for the duration of a still-running syncChats
	// call. handleConnState sets it true SYNCHRONOUSLY, before spawning the
	// syncChats goroutine, and syncChats clears it (via defer) when done --
	// set before the `go`, not inside the goroutine, because
	// shouldSyncOnConnect consumes the "may sync" latch synchronously and a
	// second Connected transition landing in the gap before the goroutine
	// starts would otherwise observe the flag still false and take
	// backfill.go's catchUp branch concurrently with an unfinished first
	// sync -- calling catch_up_user with a meaningless (still probably zero)
	// UserLoginMetadata.Revision watermark (gchat-port-auditor P1 finding,
	// M2 Task 7). catchUp checks this flag and skips its RPC call entirely
	// while it is true, deferring (not losing) that reconnect's catch-up
	// opportunity: once the first sync finishes, advanceUserRevision
	// (backfill.go) picks up tracking from the very next live event, same as
	// any other reconnect.
	syncInProgress bool

	// metaMu guards all UserLoginMetadata mutations + the paired Save, and
	// the loggedOut flag; held across Save (I/O). Most metadata writes are
	// infrequent (cookies on connect/logout), but advanceUserRevision
	// (backfill.go) also updates the user revision watermark here once per
	// successfully handled inbound event that carries a user_revision -- so
	// on a busy account this lock IS taken on the live event path, at Google
	// Chat's own event-revision cadence (matching Python's set_revision,
	// called from on_stream_event for every user-revisioned event,
	// user.py:674-682; no regression, just no longer connect/logout-only).
	// Without this lock, the now-multiple independent, unsynchronized writers
	// -- persistCookies (conn's OnConnectionState goroutine), LogoutRemote (a
	// bridgev2 goroutine), Connect's pre-flight read of meta.Cookies (a third
	// goroutine), and advanceUserRevision (both the live-event and catchUp
	// goroutines) -- can race on the same *UserLoginMetadata, and worse: a
	// Connected callback
	// still in flight when LogoutRemote runs can write live cookies back over
	// the just-cleared nil, resurrecting a session the user explicitly logged
	// out of. All metadata access goes through updateMetadata (mutate+save
	// under one lock) or Connect's guarded snapshot read, never directly.
	//
	// Lock ordering: mu and metaMu are never held at once by this type's own
	// code (metadata ops only ever take metaMu; conn ops only ever take mu).
	// If a future change ever needs both, acquire mu first, then metaMu.
	metaMu sync.Mutex
	// loggedOut latches true once LogoutRemote has run, so a Connected
	// callback that was already in flight (raced against the logout) skips
	// persisting cookies instead of resurrecting the just-cleared session. A
	// fresh Connect clears it: a new Connect means this login's session is
	// being (re)activated. Guarded by metaMu.
	loggedOut bool

	// disconnectFn tears down a superseded/replaced conn. Defaults to
	// (*gchatmeow.Client).Disconnect; overridden in tests so old-client
	// teardown (Task 10 review carry-over: the cookie-resubmit goroutine
	// leak) can be observed via a counter without a live network client.
	disconnectFn func(*gchatmeow.Client)
	// saveFn persists UserLogin.Metadata changes. Defaults to
	// UserLogin.Save; overridden in tests that don't have a full
	// bridgev2.Bridge+DB harness (mirrors gchatmeow.Client's sleepFn test
	// seam, pkg/gchatmeow/client.go).
	saveFn func(ctx context.Context) error

	// paginatedWorldFn issues the paginated_world RPC that sync.go's
	// syncChats needs. Defaults to conn.PaginatedWorld; overridden in tests
	// so syncChats' retry/latch-reset behavior (below, and see
	// resetSyncLatch's doc comment) can be exercised without a live
	// gchatmeow.Client connection -- mirrors saveFn/disconnectFn above.
	paginatedWorldFn func(ctx context.Context, req *pb.PaginatedWorldRequest) (*pb.PaginatedWorldResponse, error)

	// catchUpUserFn issues the catch_up_user RPC that backfill.go's catchUp
	// needs. Defaults to conn.CatchUpUser; overridden in tests so catchUp's
	// watermark/dispatch/failure behavior can be exercised without a live
	// gchatmeow.Client connection -- mirrors paginatedWorldFn above.
	catchUpUserFn func(ctx context.Context, req *pb.CatchUpUserRequest) (*pb.CatchUpResponse, error)

	// queueChatResyncFn queues one planned chat-list-sync entry (sync.go's
	// syncChats). Defaults to c.UserLogin.QueueRemoteEvent; overridden in
	// tests that construct a UserLogin without a full bridgev2.Bridge+DB
	// harness, since the real UserLogin.QueueRemoteEvent dereferences
	// UserLogin.Bridge, which is nil for this package's lightweight test
	// UserLogins (see newTestUserLogin, client_test.go).
	queueChatResyncFn func(evt *simplevent.ChatResync) bridgev2.EventHandlingResult

	// syncRetryBackoffBase is syncChats' base retry delay (doubled on each
	// subsequent attempt); a zero value means "use
	// defaultSyncRetryBackoffBase" (sync.go). Overridden in tests to a tiny
	// duration so the retry-then-succeed / retry-then-give-up tests run
	// fast instead of waiting on real wall-clock backoff.
	syncRetryBackoffBase time.Duration

	// createTopicFn issues the create_topic RPC that handlematrix.go's
	// HandleMatrixMessage needs to send a brand-new top-level message.
	// Defaults to conn.CreateTopic; overridden in tests so the outbound send
	// path (request construction, response->DB mapping) can be exercised
	// without a live gchatmeow.Client connection -- mirrors
	// paginatedWorldFn above (sync.go).
	createTopicFn func(ctx context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error)

	// createMessageFn issues the create_message RPC that handlematrix.go's
	// HandleMatrixMessage needs to send a reply into an EXISTING topic (M3
	// Task 6, taken when msg.ThreadRoot != nil -- see send_message's
	// `if thread_id:` branch, client.py:441-458). Defaults to
	// conn.CreateMessage; overridden in tests for the same reason
	// createTopicFn is (no live gchatmeow.Client connection in this
	// package's tests).
	createMessageFn func(ctx context.Context, req *pb.CreateMessageRequest) (*pb.CreateMessageResponse, error)

	// editMessageFn issues the edit_message RPC that handleedit.go's
	// HandleMatrixEdit needs to push a Matrix edit of a previously bridged
	// message to Google Chat (M4 Task 1). Defaults to conn.EditMessage;
	// overridden in tests so the outbound edit path (request construction,
	// response -> MessageMetadata.LastEditTime mapping) can be exercised
	// without a live gchatmeow.Client connection -- mirrors
	// createTopicFn/createMessageFn above.
	editMessageFn func(ctx context.Context, req *pb.EditMessageRequest) (*pb.EditMessageResponse, error)

	// deleteMessageFn issues the delete_message RPC that handleredact.go's
	// HandleMatrixMessageRemove needs to push a Matrix redaction of a
	// previously bridged message to Google Chat (M4 Task 2). Defaults to
	// conn.DeleteMessage; overridden in tests so the outbound redaction
	// path (request construction) can be exercised without a live
	// gchatmeow.Client connection -- mirrors editMessageFn above.
	deleteMessageFn func(ctx context.Context, req *pb.DeleteMessageRequest) (*pb.DeleteMessageResponse, error)

	// updateReactionFn issues the update_reaction RPC that handlereaction.go's
	// HandleMatrixReaction/HandleMatrixReactionRemove need to push a Matrix
	// reaction add/remove of a previously bridged message to Google Chat (M4
	// Task 3). Defaults to conn.UpdateReaction; overridden in tests so the
	// outbound reaction path (request construction: ADD vs REMOVE type, the
	// emoji, the target MessageId) can be exercised without a live
	// gchatmeow.Client connection -- mirrors editMessageFn/deleteMessageFn
	// above.
	updateReactionFn func(ctx context.Context, req *pb.UpdateReactionRequest) (*pb.UpdateReactionResponse, error)

	// markGroupReadstateFn issues the mark_group_readstate RPC that
	// handlereceipt.go's HandleMatrixReadReceipt needs to push a Matrix read
	// receipt in a portal room to Google Chat (M4 Task 4). Defaults to
	// conn.MarkGroupReadstate; overridden in tests so the outbound read
	// receipt path (request construction: group id, ExactMessage vs receipt
	// timestamp selection) can be exercised without a live gchatmeow.Client
	// connection -- mirrors editMessageFn/deleteMessageFn/updateReactionFn
	// above.
	markGroupReadstateFn func(ctx context.Context, req *pb.MarkGroupReadstateRequest) (*pb.MarkGroupReadstateResponse, error)

	// setTypingStateFn issues the set_typing_state RPC that handletyping.go's
	// HandleMatrixTyping needs to push a Matrix typing start/stop in a
	// portal room to Google Chat (M4 Task 5). Defaults to
	// conn.SetTypingState; overridden in tests so the outbound typing path
	// (request construction: group/topic context oneof, TYPING vs STOPPED
	// state) can be exercised without a live gchatmeow.Client connection --
	// mirrors markGroupReadstateFn/updateReactionFn above.
	setTypingStateFn func(ctx context.Context, req *pb.SetTypingStateRequest) (*pb.SetTypingStateResponse, error)

	// uploadFileFn issues the resumable /uploads RPC that handlematrix.go's
	// HandleMatrixMessage media branch needs to attach a Matrix media
	// message's file to Google Chat as an UPLOAD_METADATA annotation (M5
	// Task 5, gchatmeow.Client.UploadFile, Task 2, pkg/gchatmeow/upload.go).
	// Defaults to conn.UploadFile; overridden in tests so both the
	// successful-upload -> annotation composition AND the #114
	// upload-failure -> clean-error path (media.go's buildUploadAnnotation)
	// can be exercised without a live gchatmeow.Client connection -- mirrors
	// createTopicFn/createMessageFn above.
	uploadFileFn func(ctx context.Context, groupID string, data []byte, filename, mimeType string) (*pb.UploadMetadata, error)

	// downloadMediaFn downloads (and, for an encrypted room, decrypts) the
	// Matrix media an outbound media message references (media.go's
	// buildUploadAnnotation, M5 Task 5), matching portal.py's
	// `self.main_intent.download_media` (portal.py:1090,1095) -- decryption
	// itself is handled internally by bridgev2's own DownloadMedia
	// (mautrix-go bridgev2/matrix/intent.go's ASIntent.DownloadMedia)
	// whenever the file argument is non-nil, so this port needs no separate
	// decrypt_attachment call of its own the way portal.py does
	// (portal.py:1091-1093). Defaults to msg.Portal.Bridge.Bot.DownloadMedia
	// (downloadMatrixMedia, below); overridden in tests because
	// bridgev2.Portal.Bridge is nil for this package's lightweight test
	// fixtures (spacePortal/dmPortal, handlematrix_test.go) -- mirrors
	// addPendingToIgnoreFn's own reasoning above.
	downloadMediaFn func(ctx context.Context, uri id.ContentURIString, file *event.EncryptedFileInfo) ([]byte, error)

	// getMessageFn resolves a previously-bridged message row by its network
	// message id, used by reactionTopicID (handlereaction.go) as a fallback
	// when a reaction carries no cached *ReactionMetadata.TopicID -- notably
	// every reaction added from the Google Chat side (queueMessageReaction,
	// events.go, has no per-message payload on MessageReactionEvent to read
	// a topic id off directly, unlike a Matrix-initiated HandleMatrixReaction,
	// which already has msg.TargetMessage -- and therefore its stored
	// MessageMetadata.TopicID -- in hand with no lookup at all). Defaults to
	// c.UserLogin.Bridge.DB.Message.GetFirstPartByID; overridden in tests
	// that construct a UserLogin without a full bridgev2.Bridge+DB harness,
	// for the same reason savePortalRevisionFn (backfill.go) exists.
	getMessageFn func(ctx context.Context, receiver networkid.UserLoginID, id networkid.MessageID) (*database.Message, error)

	// addPendingToIgnoreFn registers a send's local_id as a pending-to-ignore
	// transaction on msg.Portal (handlematrix.go's HandleMatrixMessage, Task
	// 6's echo-dedup mechanism), BEFORE the create_topic RPC is issued --
	// matching portal.py's `self._local_dedup.add(local_id)` happening before
	// dispatch (portal.py:908-909), not after the RPC returns (the
	// megabridge defect this ports around, docs/research/08b row 61: its
	// AddPendingToIgnore call only fires once the RPC response is in hand,
	// leaving a race window where a fast echo on the event stream arrives
	// before the pending entry exists). Defaults to msg.AddPendingToIgnore;
	// overridden in tests because bridgev2.Portal.outgoingMessages is a
	// private field only initialized by a real bridgev2.Bridge's loadPortal
	// (portal.go) -- the lightweight spacePortal/dmPortal test fixtures
	// (handlematrix_test.go) construct a bare *bridgev2.Portal with that map
	// left nil, so calling the real method against one would panic
	// (assignment to entry in nil map). Mirrors every other *Fn seam on this
	// type (createTopicFn above, etc.).
	addPendingToIgnoreFn func(msg *bridgev2.MatrixMessage, txnID networkid.TransactionID)

	// removePendingFn undoes an addPendingToIgnoreFn registration when the
	// create_topic RPC that followed it fails (handlematrix.go's
	// HandleMatrixMessage). Defaults to msg.RemovePending; overridden in
	// tests for the same reason addPendingToIgnoreFn is (the real method
	// also reaches into bridgev2.Portal's private fields) and so tests can
	// observe that a failed send's pending registration gets cleaned up
	// (echo_dedup_test.go) rather than leaking for the life of the process.
	removePendingFn func(msg *bridgev2.MatrixMessage, txnID networkid.TransactionID)

	// queueRemoteEventFn queues one inbound bridgev2.RemoteEvent built from a
	// live gchatmeow stream event (events.go's handleGChatEvent, starting
	// with MESSAGE_POSTED in Task 4; later M2+ event kinds -- edits,
	// reactions, deletes -- route through the same seam). Defaults to
	// c.UserLogin.QueueRemoteEvent; overridden in tests that construct a
	// UserLogin without a full bridgev2.Bridge+DB harness, for the same
	// reason queueChatResyncFn (sync.go) exists: the real
	// UserLogin.QueueRemoteEvent dereferences UserLogin.Bridge, which is nil
	// for this package's lightweight test UserLogins (see newTestUserLogin,
	// client_test.go). Unlike queueChatResyncFn, this seam is typed on the
	// generic bridgev2.RemoteEvent interface rather than one concrete
	// simplevent type, since MESSAGE_POSTED's RemoteMessage is the first of
	// several distinct event shapes that will end up queued from
	// handleGChatEvent.
	queueRemoteEventFn func(evt bridgev2.RemoteEvent) bridgev2.EventHandlingResult

	// savePortalRevisionFn parks a group_revision watermark on one portal's
	// PortalMetadata.Revision (backfill.go's advancePortalRevision, the
	// group-revision half of M2 Task 7's split between the user watermark and
	// the per-portal one). Defaults to a real
	// bridgev2.Bridge.GetPortalByKey + Portal.Save; overridden in tests
	// because this package's lightweight test UserLogins have a nil Bridge
	// (newTestUserLogin, client_test.go) that the real lookup would
	// dereference -- and so the group-revision-routing tests can observe the
	// (portalKey, revision) pair without a full DB harness. Mirrors every
	// other *Fn seam on this type.
	savePortalRevisionFn func(ctx context.Context, portalKey networkid.PortalKey, revision int64)

	// listTopicsFn issues the single list_topics RPC that backfill.go's
	// FetchMessages uses to fetch a flat portal's message history (M6 Task 1;
	// single-shot, matching portal.py's _initial_backfill -- see backfill.go).
	// Defaults to conn.ListTopics; overridden in tests so FetchMessages'
	// request construction (group id, page size) and single-shot response
	// handling can be exercised without a live gchatmeow.Client connection --
	// mirrors catchUpUserFn/paginatedWorldFn above.
	listTopicsFn func(ctx context.Context, req *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error)

	// getIntentForFn resolves the Matrix intent to bridge a backfilled
	// message as (backfill.go's FetchMessages, M6 Task 1). Defaults to
	// params.Portal.GetIntentFor(ctx, sender, c.UserLogin,
	// bridgev2.RemoteEventBackfill); overridden in tests because the real
	// method dereferences portal.Bridge (bridgev2.Portal.GetIntentFor ->
	// getIntentAndUserMXIDFor -> portal.Bridge.GetGhostByID), which is nil
	// for this package's lightweight test portals (no full bridgev2.Bridge+DB
	// harness) -- mirrors getMessageFn/addPendingToIgnoreFn's own reasoning
	// above.
	getIntentForFn func(ctx context.Context, portal *bridgev2.Portal, sender bridgev2.EventSender, evtType bridgev2.RemoteEventType) (bridgev2.MatrixAPI, bool)
}

var _ bridgev2.NetworkAPI = (*GChatClient)(nil)

// Connect builds a gchatmeow.Client from this login's persisted
// UserLoginMetadata (cookies + user agent) -- LoadUserLogin (connector.go)
// only allocates the *GChatClient shell, per docs/research/04 §8: "LoadUserLogin
// runs under the global cache lock -- construct the client from
// login.Metadata only; do network I/O in Connect" -- wires its callbacks, and
// starts its supervision loop in the background. Never returns an error
// (bridgev2.NetworkAPI.Connect's contract); failures are surfaced via
// BridgeState.Send, matching a missing/invalid cookie set to BAD_CREDENTIALS.
//
// Any previously-attached client is torn down first (see wireAndStart), so
// calling Connect again on the same *GChatClient -- which bridgev2 itself
// does not do in normal operation, but which a defensive caller might -- can
// never orphan a running client goroutine.
//
// Connect also clears the loggedOut latch (metaMu-guarded, see its doc
// comment) before doing anything else: a new Connect call means this login's
// session is being (re)activated, so a stale latch from an earlier
// LogoutRemote must not suppress cookie persistence once this connection
// reaches CONNECTED (persistCookies below).
func (c *GChatClient) Connect(ctx context.Context) {
	c.metaMu.Lock()
	c.loggedOut = false
	meta, _ := c.UserLogin.Metadata.(*UserLoginMetadata)
	var cookies map[string]string
	var userAgent string
	if meta != nil {
		// Snapshot under metaMu: meta.Cookies/UserAgent are also written by
		// persistCookies and LogoutRemote from other goroutines, so reading
		// the fields directly here (unlocked) would race them.
		cookies = meta.Cookies
		userAgent = meta.UserAgent
	}
	c.metaMu.Unlock()

	if meta == nil || !hasRequiredCookies(cookies) {
		zerolog.Ctx(ctx).Warn().Msg("googlechat: Connect called with no usable stored cookies")
		c.reportState(gchatmeow.ConnStateBadCredentials, errNoStoredCookies)
		return
	}

	conn, err := gchatmeow.NewClient(gchatmeow.ClientOpts{
		Cookies:   cookies,
		UserAgent: userAgent,
	})
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("googlechat: failed to build gchatmeow client from stored metadata")
		c.reportState(gchatmeow.ConnStateBadCredentials, err)
		return
	}

	c.wireAndStart(ctx, conn)
}

// errNoStoredCookies is reported (as GChatBadCredentials) when Connect finds
// no usable cookie set in UserLoginMetadata -- e.g. after LogoutRemote
// cleared it, or a corrupted/incomplete DB row.
var errNoStoredCookies = fmt.Errorf("googlechat: no stored cookies")

// hasRequiredCookies reports whether cookies has a non-empty value for every
// entry in gchatmeow.RequiredCookies (COMPASS/SSID/SID/OSID/HSID). A partial
// cookie set can't authenticate, so it is treated the same as no cookies at
// all.
func hasRequiredCookies(cookies map[string]string) bool {
	for _, key := range gchatmeow.RequiredCookies {
		if cookies[key] == "" {
			return false
		}
	}
	return true
}

// wireAndStart installs conn as this login's active gchatmeow client --
// disconnecting any previously-attached client first via replaceConn, so a
// login resubmit (SubmitCookies reusing an existing UserLogin row) or a
// defensive double-Connect never leaves an orphaned client goroutine (and
// live webchannel session) running -- wires conn's callbacks to this
// GChatClient's bridge-state mapping (handleConnState) and event stub
// (handleGChatEvent, Task 12's territory), and starts conn's supervision loop
// in the background.
//
// Also resets initialSyncDone: installing a new conn represents a fresh
// session bootstrap (a brand new gchatmeow.Client, not one of its own
// internal silent reconnects), so the next Connected transition should run
// syncChats again, matching Python's on_connect_later running once per
// User.connect() call (user.py:259-292 -> 526-560).
//
// ctx is retained for the connection's lifetime: it is the context
// conn.Connect runs under, and the OnConnectionState closure captures it for
// every later BridgeState.Send / cookie-persistence call. Callers choose
// ctx's lifetime -- Connect above uses the ctx bridgev2 gave it (already the
// bridge's long-lived background context by the time it reaches a
// NetworkAPI.Connect call, per $REF/mautrix-go bridgev2/bridge.go's
// StartLogins/StartConnectors); login.go's attachAndConnect explicitly uses
// the bridge's BackgroundCtx rather than a short HTTP-request-scoped ctx.
func (c *GChatClient) wireAndStart(ctx context.Context, conn *gchatmeow.Client) {
	c.mu.Lock()
	c.initialSyncDone = false
	c.mu.Unlock()
	conn.OnStreamEvent = func(ctx context.Context, ev *pb.Event) {
		// handleGChatEvent returns an EventHandlingResult (so catchUp's drain
		// can observe a handling failure, backfill.go); the live-stream
		// callback has no use for it and discards it.
		c.handleGChatEvent(ctx, ev)
	}
	conn.OnConnectionState = func(state gchatmeow.ConnState, err error) {
		c.handleConnState(ctx, state, err)
	}
	c.replaceConn(conn)
	go conn.Connect(ctx)
}

// shouldSyncOnConnect reports whether the current Connected transition is
// the first one since this GChatClient's active conn was (re)installed
// (wireAndStart), latching true so every later call for the same conn
// returns false.
//
// Without this gate, handleConnState would re-run the (potentially large,
// uncapped-emission) chat-list sync on EVERY Connected transition, not just
// the first -- and gchatmeow.Client's own internal webchannel reconnects
// (channel.go's SetOnReconnect) emit ConnStateConnected too, including after
// the routine ~1.5h channel-lifetime recycle (client.go's
// ErrChannelLifetimeExpired branch, which starts a brand new channel that
// re-registers and re-fires OnConnect). Python's equivalent path is
// explicitly silent there (user.py:322-325's _skip_on_connect skips
// on_connect_later entirely) and its bare on_reconnect (user.py:562-565)
// never calls sync() either -- the only recurring resync Python performs is
// an hourly, throttled sync(limit=3) (user.py:578-591), which is out of
// scope for this task (sync.go's syncChats doc comment) and not
// reintroduced by this gate; this just stops the one-time sync from
// silently becoming a "resync on every reconnect" one.
func (c *GChatClient) shouldSyncOnConnect() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.initialSyncDone {
		return false
	}
	c.initialSyncDone = true
	return true
}

// resetSyncLatch clears initialSyncDone so a later Connected transition for
// the current conn (e.g. a webchannel reconnect) gets another chance to run
// syncChats. shouldSyncOnConnect latches the "may sync" slot BEFORE
// syncChats has actually run, so on its own that latch would permanently
// consume the one-time sync opportunity even if the paginated_world RPC
// never succeeds -- a single transient blip at first connect would leave
// the bridge CONNECTED with zero portals until a process restart. syncChats
// (sync.go) calls this from every path where it gives up without queuing
// any chats: no live conn, and paginated_world failing after its bounded
// retry loop is exhausted.
func (c *GChatClient) resetSyncLatch() {
	c.mu.Lock()
	c.initialSyncDone = false
	c.mu.Unlock()
}

// setSyncInProgress sets/clears the syncInProgress flag (see its doc
// comment above); syncChats (sync.go) calls this with true right before its
// body runs and defers a false to cover every return path.
func (c *GChatClient) setSyncInProgress(v bool) {
	c.mu.Lock()
	c.syncInProgress = v
	c.mu.Unlock()
}

// isSyncInProgress reports whether this conn's syncChats call is still
// running -- backfill.go's catchUp checks this before ever issuing a
// catch_up_user RPC (see syncInProgress's doc comment for why).
func (c *GChatClient) isSyncInProgress() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.syncInProgress
}

// replaceConn installs newConn as the active client, tearing down (via
// c.disconnect) whatever client was previously installed. Safe to call with
// newConn as the very first client (old is nil, no teardown happens).
func (c *GChatClient) replaceConn(newConn *gchatmeow.Client) {
	c.mu.Lock()
	old := c.conn
	c.conn = newConn
	c.mu.Unlock()
	if old != nil {
		c.disconnect(old)
	}
}

func (c *GChatClient) getConn() *gchatmeow.Client {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn
}

// disconnect tears down conn, routing through disconnectFn when a test has
// overridden it, and through the real (idempotent, safe-when-never-connected)
// gchatmeow.Client.Disconnect otherwise.
func (c *GChatClient) disconnect(conn *gchatmeow.Client) {
	if c.disconnectFn != nil {
		c.disconnectFn(conn)
		return
	}
	conn.Disconnect()
}

func (c *GChatClient) setLastState(evt status.BridgeStateEvent) {
	c.mu.Lock()
	c.lastState = evt
	c.mu.Unlock()
}

func (c *GChatClient) getLastState() status.BridgeStateEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastState
}

// save persists UserLogin.Metadata, routing through saveFn when a test has
// overridden it (no full bridgev2.Bridge+DB harness needed to unit-test
// cookie persistence), and through the real UserLogin.Save otherwise.
func (c *GChatClient) save(ctx context.Context) error {
	if c.saveFn != nil {
		return c.saveFn(ctx)
	}
	return c.UserLogin.Save(ctx)
}

// updateMetadata is the single choke point for mutating UserLoginMetadata:
// it locks metaMu, type-asserts the metadata, calls mutate (which applies
// field changes and reports whether the result needs persisting), and --
// still holding metaMu -- calls c.save when persist is true. Holding metaMu
// across the save (I/O) serializes the whole mutate+marshal+write sequence
// against any other goroutine's updateMetadata call, so persistCookies and
// LogoutRemote can never interleave (see metaMu's doc comment on GChatClient
// for why that matters). A no-op if there is no UserLoginMetadata attached.
func (c *GChatClient) updateMetadata(ctx context.Context, mutate func(*UserLoginMetadata) (persist bool)) error {
	c.metaMu.Lock()
	defer c.metaMu.Unlock()
	meta, ok := c.UserLogin.Metadata.(*UserLoginMetadata)
	if !ok || meta == nil {
		return nil
	}
	if !mutate(meta) {
		return nil
	}
	return c.save(ctx)
}

// reportState maps state/err to a BridgeState (bridgestate.go) and both sends
// it and updates the cached last-seen state IsLoggedIn reads. Used both by
// Connect's pre-flight missing-cookie check (no live conn yet) and by
// handleConnState (conn's real OnConnectionState callback).
func (c *GChatClient) reportState(state gchatmeow.ConnState, err error) {
	bs := connStateToBridgeState(state, err)
	c.setLastState(bs.StateEvent)
	c.UserLogin.BridgeState.Send(bs)
}

// handleConnState is conn's OnConnectionState callback (installed by
// wireAndStart): it reports the mapped BridgeState, and -- once the
// connection actually reaches CONNECTED -- re-persists conn's current
// (possibly rotated) cookies into UserLoginMetadata so a later restart resumes
// with the freshest session (Task 10 review carry-over (c); see also
// login.go's SubmitCookies, which persists the initial post-validation
// snapshot once at login).
//
// It also branches on shouldSyncOnConnect's one-time-per-conn latch to pick
// between two mutually exclusive actions, both run in their own goroutine
// (see below for why): the FIRST time this conn reaches Connected, it kicks
// off the chat-list sync (sync.go's syncChats, Task 12), matching Python's
// on_connect_later calling self.sync() once per connect() call right before
// pushing BridgeStateEvent.CONNECTED (user.py:555-560). EVERY SUBSEQUENT
// Connected transition for the same conn -- i.e. every reconnect, including
// gchatmeow's own internal webchannel reconnects (see shouldSyncOnConnect's
// doc comment) -- instead runs backfill.go's catchUp, replaying whatever
// happened on the account during the gap through the normal event-queue
// path (M2 Task 7, M1 review Important #2: a SID-expiring re-register
// resets the channel's AID to 0, so the server never replays that gap on
// its own -- pkg/gchatmeow/client.go's wireChannel doc comment flags this
// exact risk). Reusing shouldSyncOnConnect's existing latch instead of a
// second one keeps the "first connect vs. reconnect" distinction in exactly
// one place, and automatically inherits its failure/retry behavior:
// syncChats resets the latch on a failed first sync (resetSyncLatch, its
// own doc comment) specifically so the NEXT Connected retries the first
// sync rather than incorrectly running catchUp against portals that were
// never created.
//
// Both syncChats and catchUp run in their own goroutine rather than inline:
// handleConnState is conn's OnConnectionState callback and runs on conn's
// own supervision goroutine (see wireAndStart's doc comment), so blocking it
// here on a full paginated_world/catch_up_user RPC round trip would stall
// that client's ability to notice a subsequent reconnect/disconnect for as
// long as the call takes.
func (c *GChatClient) handleConnState(ctx context.Context, state gchatmeow.ConnState, err error) {
	c.reportState(state, err)
	if state == gchatmeow.ConnStateConnected {
		c.persistCookies(ctx)
		if c.shouldSyncOnConnect() {
			// Set syncInProgress SYNCHRONOUSLY, before spawning the sync
			// goroutine (which clears it on completion, via its own defer):
			// if it were set inside the goroutine instead, a reconnect's
			// Connected transition landing in the gap between this `go` and
			// the goroutine actually starting would observe it still false
			// and race an unfinished first sync (same timing class as the
			// Connect-cancel window). syncChats' catchUp guard depends on
			// this flag being true the instant this branch is taken.
			c.setSyncInProgress(true)
			go c.syncChats(ctx)
		} else {
			go c.catchUp(ctx)
		}
	}
}

// persistCookies snapshots conn's current auth cookies + user agent into
// UserLoginMetadata and saves it, through updateMetadata so the mutate+save
// is serialized against a concurrent LogoutRemote. A no-op if there is no
// live conn (shouldn't happen when called from handleConnState) or no
// UserLoginMetadata attached, and -- critically -- a no-op if loggedOut is
// already set: a Connected callback that raced a LogoutRemote must not write
// live cookies back over the cookies LogoutRemote just cleared (that would
// resurrect a session the user explicitly logged out of).
func (c *GChatClient) persistCookies(ctx context.Context) {
	conn := c.getConn()
	if conn == nil {
		return
	}
	err := c.updateMetadata(ctx, func(meta *UserLoginMetadata) bool {
		if c.loggedOut {
			return false
		}
		meta.Cookies = conn.Cookies()
		meta.UserAgent = conn.UserAgent()
		return true
	})
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("googlechat: failed to persist rotated cookies")
	}
}

// Disconnect stops this login's active client, if any. Safe to call multiple
// times (including when no client was ever attached): gchatmeow.Client.Disconnect
// is itself documented as a safe no-op when not connected, and a nil conn is
// simply skipped here.
func (c *GChatClient) Disconnect() {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn != nil {
		c.disconnect(conn)
	}
}

// IsLoggedIn is a cached-only check (no I/O, per bridgev2.NetworkAPI's
// contract): true only once the last connection-state transition we saw was
// CONNECTED.
func (c *GChatClient) IsLoggedIn() bool {
	return c.getLastState() == status.StateConnected
}

// LogoutRemote disconnects and best-effort clears the stored cookies so a
// later Connect (e.g. after a restart) reports BAD_CREDENTIALS instead of
// replaying a session the user explicitly logged out of. Google Chat's
// cookie-based sessions have no known remote "revoke" endpoint (docs/research
// 01/03 don't document one), so there is no remote invalidation call to make
// -- "best-effort" here means "local cleanup, tolerate a failed Save".
//
// Also sets the loggedOut latch (under the same metaMu-guarded update as the
// cookie clear) so a persistCookies call from a Connected callback already in
// flight when this runs -- e.g. conn was mid-handshake and reached CONNECTED
// just as the user hit "log out" -- skips instead of resurrecting the
// just-cleared cookies. Connect clears the latch again on the next real
// (re)activation.
func (c *GChatClient) LogoutRemote(ctx context.Context) {
	c.Disconnect()
	err := c.updateMetadata(ctx, func(meta *UserLoginMetadata) bool {
		c.loggedOut = true
		meta.Cookies = nil
		return true
	})
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("googlechat: failed to clear cookies on logout")
	}
}

// queueRemoteEvent queues evt, routing through queueRemoteEventFn when a
// test has overridden it, and through the real c.UserLogin.QueueRemoteEvent
// otherwise -- mirrors sync.go's queueChatResync / the save/disconnect seam
// pattern documented on this type above.
func (c *GChatClient) queueRemoteEvent(evt bridgev2.RemoteEvent) bridgev2.EventHandlingResult {
	if c.queueRemoteEventFn != nil {
		return c.queueRemoteEventFn(evt)
	}
	return c.UserLogin.QueueRemoteEvent(evt)
}

// addPendingToIgnore routes through addPendingToIgnoreFn when a test has
// overridden it, and through the real msg.AddPendingToIgnore otherwise --
// mirrors queueRemoteEvent/save/disconnect above. See addPendingToIgnoreFn's
// doc comment for why HandleMatrixMessage (handlematrix.go) must call this
// wrapper rather than msg.AddPendingToIgnore directly.
func (c *GChatClient) addPendingToIgnore(msg *bridgev2.MatrixMessage, txnID networkid.TransactionID) {
	if c.addPendingToIgnoreFn != nil {
		c.addPendingToIgnoreFn(msg, txnID)
		return
	}
	msg.AddPendingToIgnore(txnID)
}

// removePending routes through removePendingFn when a test has overridden
// it, and through the real msg.RemovePending otherwise -- mirrors
// addPendingToIgnore above. See removePendingFn's doc comment for why
// HandleMatrixMessage calls this wrapper (rather than msg.RemovePending
// directly) on the create_topic RPC-failure path.
func (c *GChatClient) removePending(msg *bridgev2.MatrixMessage, txnID networkid.TransactionID) {
	if c.removePendingFn != nil {
		c.removePendingFn(msg, txnID)
		return
	}
	msg.RemovePending(txnID)
}

// downloadMatrixMedia routes through downloadMediaFn when a test has
// overridden it, and through the real msg.Portal.Bridge.Bot.DownloadMedia
// otherwise -- mirrors addPendingToIgnore/removePending above. See
// downloadMediaFn's doc comment for why media.go's buildUploadAnnotation
// must call this wrapper rather than reaching into msg.Portal.Bridge.Bot
// directly. msg.Content.File is passed through as-is (nil for an
// unencrypted room, a populated *event.EncryptedFileInfo for an encrypted
// one) -- DownloadMedia's own contract is to decrypt in that case, matching
// portal.py's `if message.file and decrypt_attachment:` branch
// (portal.py:1089) without this port needing its own decrypt_attachment
// call (see downloadMediaFn's doc comment).
func (c *GChatClient) downloadMatrixMedia(ctx context.Context, msg *bridgev2.MatrixMessage) ([]byte, error) {
	if c.downloadMediaFn != nil {
		return c.downloadMediaFn(ctx, msg.Content.URL, msg.Content.File)
	}
	if msg.Portal == nil || msg.Portal.Bridge == nil || msg.Portal.Bridge.Bot == nil {
		return nil, fmt.Errorf("googlechat: no matrix intent available to download media")
	}
	return msg.Portal.Bridge.Bot.DownloadMedia(ctx, msg.Content.URL, msg.Content.File)
}

// msgConverter returns this login's msgconv.MessageConverter, falling back
// to a fresh msgconv.New() when Main is nil (bare *GChatClient test
// construction -- Main's doc comment above already documents it as "often
// nil in tests") or Main.MsgConv was never populated. msgconv.MessageConverter
// holds no per-login state (msgconv.go: "conversion configuration only"), so
// this fallback is always behaviorally identical to the real
// GChatConnector.Init-populated one, just without requiring every test to
// wire Main.MsgConv by hand.
func (c *GChatClient) msgConverter() *msgconv.MessageConverter {
	if c.Main != nil && c.Main.MsgConv != nil {
		return c.Main.MsgConv
	}
	return msgconv.New()
}

// IsThisUser reports whether userID names the same Google account as this
// login: the login's UserLoginID IS the account's gaia ID (gcid.MakeUserLoginID
// in login.go), and UserID is the same gaia ID reinterpreted
// (gcid.MakeUserID) -- so the comparison is just a type conversion, no I/O.
func (c *GChatClient) IsThisUser(_ context.Context, userID networkid.UserID) bool {
	return userID == gcid.MakeUserID(string(c.UserLogin.ID))
}

// GetChatInfo and GetUserInfo are implemented in chatinfo.go and userinfo.go
// (Task 12) respectively.

// GetCapabilities is implemented in capabilities.go (Task 5).

// HandleMatrixMessage is implemented in handlematrix.go.
