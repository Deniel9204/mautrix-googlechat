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

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
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

	// metaMu guards all UserLoginMetadata mutations + the paired Save, and
	// the loggedOut flag; held across Save (I/O) because metadata writes are
	// infrequent (connect/logout only), not a hot path. Without this, three
	// independent, unsynchronized writers -- persistCookies (conn's
	// OnConnectionState goroutine), LogoutRemote (a bridgev2 goroutine), and
	// Connect's pre-flight read of meta.Cookies (a third goroutine) -- can
	// race on the same *UserLoginMetadata, and worse: a Connected callback
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
	conn.OnStreamEvent = c.handleGChatEvent
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
// snapshot once at login). Also kicks off the chat-list sync (sync.go's
// syncChats, Task 12) the FIRST time this conn reaches Connected (gated by
// shouldSyncOnConnect, see its doc comment for why) -- matching Python's
// on_connect_later, which calls self.sync() once per connect() call, right
// before pushing BridgeStateEvent.CONNECTED (user.py:555-560). syncChats
// runs in its own goroutine rather than inline: handleConnState is conn's
// OnConnectionState callback and runs on conn's own supervision goroutine
// (see wireAndStart's doc comment), so blocking it here on a full
// paginated_world RPC round trip would stall that client's ability to
// notice a subsequent reconnect/disconnect for as long as the sync takes.
func (c *GChatClient) handleConnState(ctx context.Context, state gchatmeow.ConnState, err error) {
	c.reportState(state, err)
	if state == gchatmeow.ConnStateConnected {
		c.persistCookies(ctx)
		if c.shouldSyncOnConnect() {
			go c.syncChats(ctx)
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

// IsThisUser reports whether userID names the same Google account as this
// login: the login's UserLoginID IS the account's gaia ID (gcid.MakeUserLoginID
// in login.go), and UserID is the same gaia ID reinterpreted
// (gcid.MakeUserID) -- so the comparison is just a type conversion, no I/O.
func (c *GChatClient) IsThisUser(_ context.Context, userID networkid.UserID) bool {
	return userID == gcid.MakeUserID(string(c.UserLogin.ID))
}

// GetChatInfo and GetUserInfo are implemented in chatinfo.go and userinfo.go
// (Task 12) respectively.

func (c *GChatClient) GetCapabilities(_ context.Context, _ *bridgev2.Portal) *event.RoomFeatures {
	return &event.RoomFeatures{MaxTextLength: 4096}
}

func (c *GChatClient) HandleMatrixMessage(_ context.Context, _ *bridgev2.MatrixMessage) (*bridgev2.MatrixMessageResponse, error) {
	return nil, fmt.Errorf("sending messages is not implemented yet")
}
