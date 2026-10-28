package connector

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

// GChatClient is the per-UserLogin bridgev2.NetworkAPI implementation.
type GChatClient struct {
	// Main is the owning connector; nil in tests that build a bare GChatClient.
	Main      *GChatConnector
	UserLogin *bridgev2.UserLogin

	// mu guards conn, lastState, lastError, initialSyncDone and syncInProgress.
	mu        sync.Mutex
	conn      *gchatmeow.Client
	lastState status.BridgeStateEvent
	// lastError distinguishes failures that both map to StateBadCredentials.
	lastError status.BridgeStateErrorCode
	// initialSyncDone latches once the first Connected for the current conn ran syncChats.
	initialSyncDone bool
	// syncInProgress is set before the syncChats goroutine starts; catchUp skips while it is true.
	syncInProgress bool

	// metaMu guards UserLoginMetadata mutations (held across Save) and loggedOut.
	// Lock ordering: if both are ever needed, take mu before metaMu.
	metaMu sync.Mutex
	// loggedOut stops an in-flight Connected callback from re-persisting cookies after logout.
	loggedOut bool

	// The *Fn fields below are test seams; nil means use the real implementation.
	disconnectFn func(*gchatmeow.Client)

	saveFn func(ctx context.Context) error

	paginatedWorldFn func(ctx context.Context, req *pb.PaginatedWorldRequest) (*pb.PaginatedWorldResponse, error)

	catchUpUserFn func(ctx context.Context, req *pb.CatchUpUserRequest) (*pb.CatchUpResponse, error)

	getMembersFn func(ctx context.Context, req *pb.GetMembersRequest) (*pb.GetMembersResponse, error)

	queueChatResyncFn func(evt *simplevent.ChatResync) bridgev2.EventHandlingResult

	// syncRetryBackoffBase is syncChats' base retry delay; zero means defaultSyncRetryBackoffBase.
	syncRetryBackoffBase time.Duration

	createTopicFn func(ctx context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error)

	createDmFn        func(ctx context.Context, req *pb.CreateDmRequest) (*pb.CreateDmResponse, error)
	createGroupChatFn func(ctx context.Context, req *pb.CreateGroupRequest) (*pb.CreateGroupResponse, error)

	createMessageFn func(ctx context.Context, req *pb.CreateMessageRequest) (*pb.CreateMessageResponse, error)

	editMessageFn func(ctx context.Context, req *pb.EditMessageRequest) (*pb.EditMessageResponse, error)

	deleteMessageFn func(ctx context.Context, req *pb.DeleteMessageRequest) (*pb.DeleteMessageResponse, error)

	updateReactionFn func(ctx context.Context, req *pb.UpdateReactionRequest) (*pb.UpdateReactionResponse, error)

	markGroupReadstateFn func(ctx context.Context, req *pb.MarkGroupReadstateRequest) (*pb.MarkGroupReadstateResponse, error)

	setTypingStateFn func(ctx context.Context, req *pb.SetTypingStateRequest) (*pb.SetTypingStateResponse, error)

	uploadFileFn func(ctx context.Context, groupID string, data []byte, filename, mimeType string) (*pb.UploadMetadata, error)

	// downloadMediaFn also decrypts when file is non-nil.
	downloadMediaFn func(ctx context.Context, uri id.ContentURIString, file *event.EncryptedFileInfo) ([]byte, error)

	// getMessageFn is the fallback for reactions with no cached topic id.
	getMessageFn func(ctx context.Context, receiver networkid.UserLoginID, id networkid.MessageID) (*database.Message, error)

	// addPendingToIgnoreFn must run before create_topic is sent, or a fast echo is not deduped.
	addPendingToIgnoreFn func(msg *bridgev2.MatrixMessage, txnID networkid.TransactionID)

	removePendingFn func(msg *bridgev2.MatrixMessage, txnID networkid.TransactionID)

	queueRemoteEventFn func(evt bridgev2.RemoteEvent) bridgev2.EventHandlingResult

	savePortalRevisionFn func(ctx context.Context, portalKey networkid.PortalKey, revision int64)

	listTopicsFn func(ctx context.Context, req *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error)

	listMessagesFn func(ctx context.Context, req *pb.ListMessagesRequest) (*pb.ListMessagesResponse, error)

	getIntentForFn func(ctx context.Context, portal *bridgev2.Portal, sender bridgev2.EventSender, evtType bridgev2.RemoteEventType) (bridgev2.MatrixAPI, bool)

	createMembershipFn  func(ctx context.Context, req *pb.CreateMembershipRequest) (*pb.CreateMembershipResponse, error)
	removeMembershipsFn func(ctx context.Context, req *pb.RemoveMembershipsRequest) (*pb.RemoveMembershipsResponse, error)
	updateGroupFn       func(ctx context.Context, req *pb.UpdateGroupRequest) (*pb.UpdateGroupResponse, error)

	getGroupFn func(ctx context.Context, req *pb.GetGroupRequest) (*pb.GetGroupResponse, error)

	bindCreatedSpaceRoomFn func(ctx context.Context, key networkid.PortalKey, params *bridgev2.GroupCreateParams) (*bridgev2.Portal, error)

	ensureBotPowerFn func(ctx context.Context, roomID id.RoomID) error

	hideGroupFn func(ctx context.Context, req *pb.HideGroupRequest) (*pb.HideGroupResponse, error)

	updateNotificationSettingsFn func(ctx context.Context, req *pb.UpdateGroupNotificationSettingsRequest) (*pb.UpdateGroupNotificationSettingsResponse, error)
}

var _ bridgev2.NetworkAPI = (*GChatClient)(nil)

// Connect builds a gchatmeow.Client from the stored cookies and starts it in the background.
// Errors are reported via BridgeState, never returned.
func (c *GChatClient) Connect(ctx context.Context) {
	c.metaMu.Lock()
	c.loggedOut = false
	meta, _ := c.UserLogin.Metadata.(*UserLoginMetadata)
	var cookies map[string]string
	var userAgent string
	if meta != nil {
		// Snapshot under metaMu: persistCookies and LogoutRemote write these concurrently.
		cookies = meta.Cookies
		userAgent = meta.UserAgent
	}
	c.metaMu.Unlock()

	if meta == nil || !hasRequiredCookies(cookies) {
		zerolog.Ctx(ctx).Warn().Err(errNoStoredCookies).Msg("googlechat: Connect called with no usable stored cookies")
		// Distinct code: nothing was rejected, there were simply no cookies.
		c.reportBridgeState(missingCookiesBridgeState())
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

// errNoStoredCookies is reported when Connect finds no usable cookie set.
var errNoStoredCookies = fmt.Errorf("googlechat: no stored cookies")

// hasRequiredCookies reports whether every gchatmeow.RequiredCookies entry is non-empty.
func hasRequiredCookies(cookies map[string]string) bool {
	for _, key := range gchatmeow.RequiredCookies {
		if cookies[key] == "" {
			return false
		}
	}
	return true
}

// wireAndStart installs conn (tearing down the old one) and starts it in the background.
// ctx is kept for the connection's lifetime, so it must be long-lived.
func (c *GChatClient) wireAndStart(ctx context.Context, conn *gchatmeow.Client) {
	c.mu.Lock()
	c.initialSyncDone = false
	c.mu.Unlock()
	conn.OnStreamEvent = func(ctx context.Context, ev *pb.Event) {
		// The result only matters to catchUp's drain.
		c.handleGChatEvent(ctx, ev)
	}
	conn.OnConnectionState = c.connStateCallback(ctx, conn)
	c.replaceConn(conn)
	go conn.Connect(ctx)
}

// connStateCallback ignores transitions from a superseded conn: Disconnect does not wait
// for the old goroutine, and a late Connected would consume the new conn's sync latch.
func (c *GChatClient) connStateCallback(ctx context.Context, conn *gchatmeow.Client) func(gchatmeow.ConnState, error) {
	return func(state gchatmeow.ConnState, err error) {
		if c.getConn() != conn {
			zerolog.Ctx(ctx).Debug().
				Str("state", state.String()).
				Msg("googlechat: ignoring connection-state callback from a superseded connection")
			return
		}
		c.handleConnState(ctx, state, err)
	}
}

// shouldSyncOnConnect latches true on the first Connected per conn; reconnects must not resync.
func (c *GChatClient) shouldSyncOnConnect() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.initialSyncDone {
		return false
	}
	c.initialSyncDone = true
	return true
}

// resetSyncLatch lets a later Connected retry syncChats after a failed first sync.
func (c *GChatClient) resetSyncLatch() {
	c.mu.Lock()
	c.initialSyncDone = false
	c.mu.Unlock()
}

// setSyncInProgress sets or clears syncInProgress.
func (c *GChatClient) setSyncInProgress(v bool) {
	c.mu.Lock()
	c.syncInProgress = v
	c.mu.Unlock()
}

// isSyncInProgress reports whether syncChats is still running.
func (c *GChatClient) isSyncInProgress() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.syncInProgress
}

// replaceConn installs newConn and disconnects the previous client, if any.
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

// disconnect tears down conn via disconnectFn if set.
func (c *GChatClient) disconnect(conn *gchatmeow.Client) {
	if c.disconnectFn != nil {
		c.disconnectFn(conn)
		return
	}
	conn.Disconnect()
}

func (c *GChatClient) setLastState(evt status.BridgeStateEvent, code status.BridgeStateErrorCode) {
	c.mu.Lock()
	c.lastState, c.lastError = evt, code
	c.mu.Unlock()
}

func (c *GChatClient) getLastState() status.BridgeStateEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastState
}

func (c *GChatClient) getLastErrorCode() status.BridgeStateErrorCode {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastError
}

// save persists UserLogin.Metadata via saveFn if set.
func (c *GChatClient) save(ctx context.Context) error {
	if c.saveFn != nil {
		return c.saveFn(ctx)
	}
	return c.UserLogin.Save(ctx)
}

// updateMetadata mutates UserLoginMetadata and saves it under metaMu.
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

// updateProfile is updateMetadata for RemoteName/RemoteProfile; it takes metaMu because
// Save marshals the profile and metadata together.
func (c *GChatClient) updateProfile(ctx context.Context, mutate func() (persist bool)) error {
	c.metaMu.Lock()
	defer c.metaMu.Unlock()
	if !mutate() {
		return nil
	}
	return c.save(ctx)
}

// ownEmail returns the login's Google address, or "" if unknown (never treat "" as "no match").
func (c *GChatClient) ownEmail() string {
	c.metaMu.Lock()
	defer c.metaMu.Unlock()
	if c.UserLogin == nil {
		return ""
	}
	return c.UserLogin.RemoteProfile.Email
}

// reportState maps state/err to a BridgeState, sends it and caches it for IsLoggedIn.
func (c *GChatClient) reportState(state gchatmeow.ConnState, err error) {
	c.reportBridgeState(connStateToBridgeState(state, err))
}

// reportBridgeState sends bs and caches what was sent.
func (c *GChatClient) reportBridgeState(bs status.BridgeState) {
	c.setLastState(bs.StateEvent, bs.Error)
	c.UserLogin.BridgeState.Send(bs)
}

// handleConnState reports the state and, on Connected, persists cookies and runs syncChats
// (first connect per conn) or catchUp (reconnects) in a goroutine.
func (c *GChatClient) handleConnState(ctx context.Context, state gchatmeow.ConnState, err error) {
	c.reportState(state, err)
	if state == gchatmeow.ConnStateConnected {
		c.persistCookies(ctx)
		if c.shouldSyncOnConnect() {
			// Set before spawning, so a quick reconnect can't race an unfinished first sync.
			c.setSyncInProgress(true)
			go c.syncChats(ctx)
		} else {
			go c.catchUp(ctx)
		}
	}
}

// persistCookies saves conn's current cookies, unless LogoutRemote already ran.
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

// Disconnect stops the active client, if any. Safe to call multiple times.
func (c *GChatClient) Disconnect() {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn != nil {
		c.disconnect(conn)
	}
}

// IsLoggedIn reports whether the last connection state was CONNECTED (cached, no I/O).
func (c *GChatClient) IsLoggedIn() bool {
	return c.getLastState() == status.StateConnected
}

// LogoutRemote disconnects and clears the stored cookies; there is no remote revoke endpoint.
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

func (c *GChatClient) queueRemoteEvent(evt bridgev2.RemoteEvent) bridgev2.EventHandlingResult {
	if c.queueRemoteEventFn != nil {
		return c.queueRemoteEventFn(evt)
	}
	return c.UserLogin.QueueRemoteEvent(evt)
}

func (c *GChatClient) addPendingToIgnore(msg *bridgev2.MatrixMessage, txnID networkid.TransactionID) {
	if c.addPendingToIgnoreFn != nil {
		c.addPendingToIgnoreFn(msg, txnID)
		return
	}
	msg.AddPendingToIgnore(txnID)
}

func (c *GChatClient) removePending(msg *bridgev2.MatrixMessage, txnID networkid.TransactionID) {
	if c.removePendingFn != nil {
		c.removePendingFn(msg, txnID)
		return
	}
	msg.RemovePending(txnID)
}

func (c *GChatClient) downloadMatrixMedia(ctx context.Context, msg *bridgev2.MatrixMessage) ([]byte, error) {
	if c.downloadMediaFn != nil {
		return c.downloadMediaFn(ctx, msg.Content.URL, msg.Content.File)
	}
	if msg.Portal == nil || msg.Portal.Bridge == nil || msg.Portal.Bridge.Bot == nil {
		return nil, fmt.Errorf("googlechat: no matrix intent available to download media")
	}
	return msg.Portal.Bridge.Bot.DownloadMedia(ctx, msg.Content.URL, msg.Content.File)
}

func (c *GChatClient) msgConverter() *msgconv.MessageConverter {
	if c.Main != nil && c.Main.MsgConv != nil {
		return c.Main.MsgConv
	}
	return msgconv.New()
}

// IsThisUser reports whether userID is this login's own gaia ID.
func (c *GChatClient) IsThisUser(_ context.Context, userID networkid.UserID) bool {
	return userID == gcid.MakeUserID(string(c.UserLogin.ID))
}
