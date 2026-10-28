package connector

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"

	"maunium.net/go/mautrix/bridgev2"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

func fakeCookies() map[string]string {
	return map[string]string{
		"COMPASS": "dynamite-ui=abc",
		"SSID":    "s",
		"SID":     "i",
		"OSID":    "o",
		"HSID":    "h",
	}
}

func newTestUserLogin(meta *UserLoginMetadata) *bridgev2.UserLogin {
	return &bridgev2.UserLogin{
		UserLogin: &database.UserLogin{
			ID:       gcid.MakeUserLoginID("112233"),
			Metadata: meta,
		},
		// BridgeState is left nil: BridgeStateQueue.Send tolerates a nil receiver.
	}
}

func TestHasRequiredCookies(t *testing.T) {
	if !hasRequiredCookies(fakeCookies()) {
		t.Error("hasRequiredCookies(full set) = false, want true")
	}
	if hasRequiredCookies(nil) {
		t.Error("hasRequiredCookies(nil) = true, want false")
	}
	partial := fakeCookies()
	delete(partial, "SID")
	if hasRequiredCookies(partial) {
		t.Error("hasRequiredCookies(missing SID) = true, want false")
	}
	empty := fakeCookies()
	empty["SID"] = ""
	if hasRequiredCookies(empty) {
		t.Error("hasRequiredCookies(empty SID value) = true, want false")
	}
}

func TestConnectMissingCookiesSendsBadCredentials(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	gc := &GChatClient{UserLogin: login}

	gc.Connect(context.Background())

	if got := gc.getLastState(); got != status.StateBadCredentials {
		t.Errorf("lastState = %v, want StateBadCredentials", got)
	}
	// Distinct code from a Google-rejected cookie set.
	if got := gc.getLastErrorCode(); got != GChatCookiesMissing {
		t.Errorf("lastError = %v, want %v", got, GChatCookiesMissing)
	}
	if gc.getConn() != nil {
		t.Error("Connect() built a gchatmeow client despite missing cookies")
	}
}

func TestConnectPartialCookiesSendsBadCredentials(t *testing.T) {
	partial := fakeCookies()
	delete(partial, "HSID")
	login := newTestUserLogin(&UserLoginMetadata{Cookies: partial})
	gc := &GChatClient{UserLogin: login}

	gc.Connect(context.Background())

	if got := gc.getLastState(); got != status.StateBadCredentials {
		t.Errorf("lastState = %v, want StateBadCredentials", got)
	}
}

func TestConnectBuildsAndInstallsClient(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{Cookies: fakeCookies()})
	gc := &GChatClient{UserLogin: login}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancelled: the spawned supervision goroutine returns immediately

	gc.Connect(ctx)

	conn := gc.getConn()
	if conn == nil {
		t.Fatal("Connect() did not install a gchatmeow client")
	}
	if conn.OnStreamEvent == nil {
		t.Error("Connect() did not wire OnStreamEvent")
	}
	if conn.OnConnectionState == nil {
		t.Error("Connect() did not wire OnConnectionState")
	}
	time.Sleep(20 * time.Millisecond) // let the background goroutine return
}

func TestReplaceConnDisconnectsOldClient(t *testing.T) {
	client1, err := gchatmeow.NewClient(gchatmeow.ClientOpts{})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	client2, err := gchatmeow.NewClient(gchatmeow.ClientOpts{})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	var disconnected *gchatmeow.Client
	gc := &GChatClient{
		conn: client1,
		disconnectFn: func(c *gchatmeow.Client) {
			disconnected = c
		},
	}

	gc.replaceConn(client2)

	if disconnected != client1 {
		t.Errorf("disconnected client = %p, want old client %p", disconnected, client1)
	}
	if gc.getConn() != client2 {
		t.Error("replaceConn() did not install the new client")
	}
}

func TestReplaceConnNoPriorClientDoesNotDisconnect(t *testing.T) {
	client, err := gchatmeow.NewClient(gchatmeow.ClientOpts{})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	called := false
	gc := &GChatClient{disconnectFn: func(*gchatmeow.Client) { called = true }}

	gc.replaceConn(client)

	if called {
		t.Error("replaceConn() called disconnectFn with no prior client")
	}
	if gc.getConn() != client {
		t.Error("replaceConn() did not install the client")
	}
}

func TestConnectTearsDownPriorClientOnReconnect(t *testing.T) {
	oldClient, err := gchatmeow.NewClient(gchatmeow.ClientOpts{})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	var disconnectedOld bool
	login := newTestUserLogin(&UserLoginMetadata{Cookies: fakeCookies()})
	gc := &GChatClient{
		UserLogin: login,
		conn:      oldClient,
		disconnectFn: func(c *gchatmeow.Client) {
			if c == oldClient {
				disconnectedOld = true
			}
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	gc.Connect(ctx)
	time.Sleep(20 * time.Millisecond)

	if !disconnectedOld {
		t.Error("Connect() did not tear down the prior client")
	}
	if conn := gc.getConn(); conn == nil || conn == oldClient {
		t.Error("Connect() did not install a new client in place of the old one")
	}
}

func TestGChatClientDisconnectNilConnIsNoop(t *testing.T) {
	gc := &GChatClient{}
	gc.Disconnect()
	gc.Disconnect() // must not panic
}

func TestGChatClientDisconnectIdempotentWithRealClient(t *testing.T) {
	client, err := gchatmeow.NewClient(gchatmeow.ClientOpts{})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	gc := &GChatClient{conn: client}

	gc.Disconnect()
	gc.Disconnect()
}

func TestGChatClientDisconnectRoutesThroughDisconnectFn(t *testing.T) {
	client, err := gchatmeow.NewClient(gchatmeow.ClientOpts{})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	count := 0
	gc := &GChatClient{conn: client, disconnectFn: func(*gchatmeow.Client) { count++ }}
	gc.Disconnect()
	gc.Disconnect()
	if count != 2 {
		t.Errorf("disconnectFn called %d times, want 2 (each Disconnect() call forwards)", count)
	}
}

func TestHandleConnStatePersistsCookiesOnConnected(t *testing.T) {
	client, err := gchatmeow.NewClient(gchatmeow.ClientOpts{Cookies: fakeCookies()})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	meta := &UserLoginMetadata{Cookies: map[string]string{"SID": "stale"}}
	login := newTestUserLogin(meta)
	var saveCount int
	gc := &GChatClient{
		UserLogin: login,
		conn:      client,
		saveFn:    func(context.Context) error { saveCount++; return nil },
	}

	// Pre-cancelled so the spawned syncChats returns immediately.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	gc.handleConnState(ctx, gchatmeow.ConnStateConnected, nil)

	if saveCount != 1 {
		t.Fatalf("save called %d times, want 1", saveCount)
	}
	want := client.Cookies()
	if !reflect.DeepEqual(meta.Cookies, want) {
		t.Errorf("Metadata.Cookies = %v, want %v (client.Cookies())", meta.Cookies, want)
	}
	if meta.UserAgent != client.UserAgent() {
		t.Errorf("Metadata.UserAgent = %q, want %q", meta.UserAgent, client.UserAgent())
	}
	if got := gc.getLastState(); got != status.StateConnected {
		t.Errorf("lastState = %v, want StateConnected", got)
	}
	if !gc.IsLoggedIn() {
		t.Error("IsLoggedIn() = false after a Connected transition, want true")
	}
	time.Sleep(20 * time.Millisecond) // let the spawned syncChats goroutine return
}

func TestShouldSyncOnConnectTrueOnceThenFalse(t *testing.T) {
	gc := &GChatClient{}

	if !gc.shouldSyncOnConnect() {
		t.Error("shouldSyncOnConnect() = false on first call, want true")
	}
	if gc.shouldSyncOnConnect() {
		t.Error("shouldSyncOnConnect() = true on second call, want false (gchatmeow.Client's own internal reconnects -- e.g. the ~1.5h channel-lifetime recycle -- also emit ConnStateConnected, and must not re-trigger a full chat-list sync)")
	}
	if gc.shouldSyncOnConnect() {
		t.Error("shouldSyncOnConnect() = true on third call, want false")
	}
}

func TestWireAndStartResetsInitialSyncDone(t *testing.T) {
	gc := &GChatClient{initialSyncDone: true}
	client, err := gchatmeow.NewClient(gchatmeow.ClientOpts{})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancelled: the spawned goroutine returns immediately, no real I/O

	gc.wireAndStart(ctx, client)

	if !gc.shouldSyncOnConnect() {
		t.Error("shouldSyncOnConnect() = false right after wireAndStart, want true (a freshly installed conn is a new session bootstrap and must sync once)")
	}
	time.Sleep(20 * time.Millisecond)
}

// TestHandleConnStateDoesNotResyncOnSecondConnected: a reconnect on the same conn must not resync.
func TestHandleConnStateDoesNotResyncOnSecondConnected(t *testing.T) {
	client, err := gchatmeow.NewClient(gchatmeow.ClientOpts{Cookies: fakeCookies()})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	login := newTestUserLogin(&UserLoginMetadata{})

	// Block the sync RPC: if syncChats gave up it would reset the latch this test checks.
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	gc := &GChatClient{
		UserLogin: login,
		conn:      client,
		saveFn:    func(context.Context) error { return nil },
		paginatedWorldFn: func(ctx context.Context, _ *pb.PaginatedWorldRequest) (*pb.PaginatedWorldResponse, error) {
			<-release
			return nil, ctx.Err()
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	gc.handleConnState(ctx, gchatmeow.ConnStateConnected, nil) // 1st: consumes the slot
	gc.handleConnState(ctx, gchatmeow.ConnStateConnected, nil) // 2nd: must not re-consume

	if gc.shouldSyncOnConnect() {
		t.Error("shouldSyncOnConnect() = true after two Connected transitions on the same conn, want false (the slot was already consumed by the first)")
	}
}

func TestHandleConnStateDoesNotPersistOnTransient(t *testing.T) {
	client, err := gchatmeow.NewClient(gchatmeow.ClientOpts{Cookies: fakeCookies()})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	meta := &UserLoginMetadata{Cookies: map[string]string{"SID": "stale"}}
	login := newTestUserLogin(meta)
	var saveCount int
	gc := &GChatClient{
		UserLogin: login,
		conn:      client,
		saveFn:    func(context.Context) error { saveCount++; return nil },
	}

	gc.handleConnState(context.Background(), gchatmeow.ConnStateTransient, nil)

	if saveCount != 0 {
		t.Errorf("save called %d times on a transient disconnect, want 0", saveCount)
	}
	if meta.Cookies["SID"] != "stale" {
		t.Errorf("Metadata.Cookies mutated on a transient disconnect: %v", meta.Cookies)
	}
	if got := gc.getLastState(); got != status.StateTransientDisconnect {
		t.Errorf("lastState = %v, want StateTransientDisconnect", got)
	}
	if gc.IsLoggedIn() {
		t.Error("IsLoggedIn() = true after a transient disconnect, want false")
	}
}

func TestIsLoggedInDefaultsFalse(t *testing.T) {
	gc := &GChatClient{}
	if gc.IsLoggedIn() {
		t.Error("IsLoggedIn() = true before any connection-state transition, want false")
	}
}

func TestIsThisUser(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	gc := &GChatClient{UserLogin: login}

	if !gc.IsThisUser(context.Background(), gcid.MakeUserID("112233")) {
		t.Error("IsThisUser(own gaia id) = false, want true")
	}
	if gc.IsThisUser(context.Background(), networkid.UserID("someone-else")) {
		t.Error("IsThisUser(other id) = true, want false")
	}
}

func TestLogoutRemoteClearsCookiesAndDisconnects(t *testing.T) {
	client, err := gchatmeow.NewClient(gchatmeow.ClientOpts{Cookies: fakeCookies()})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	meta := &UserLoginMetadata{Cookies: fakeCookies()}
	login := newTestUserLogin(meta)
	var disconnected bool
	var saveCount int
	gc := &GChatClient{
		UserLogin:    login,
		conn:         client,
		disconnectFn: func(*gchatmeow.Client) { disconnected = true },
		saveFn:       func(context.Context) error { saveCount++; return nil },
	}

	gc.LogoutRemote(context.Background())

	if !disconnected {
		t.Error("LogoutRemote() did not disconnect the client")
	}
	if meta.Cookies != nil {
		t.Errorf("Metadata.Cookies = %v, want nil after logout", meta.Cookies)
	}
	if saveCount != 1 {
		t.Errorf("save called %d times, want 1", saveCount)
	}
}

// TestMetadataRaceLogoutVsPersist: run under -race; logout must always win over a concurrent persist.
func TestMetadataRaceLogoutVsPersist(t *testing.T) {
	for i := 0; i < 50; i++ {
		client, err := gchatmeow.NewClient(gchatmeow.ClientOpts{Cookies: fakeCookies()})
		if err != nil {
			t.Fatalf("NewClient: %v", err)
		}
		meta := &UserLoginMetadata{Cookies: fakeCookies()}
		login := newTestUserLogin(meta)
		gc := &GChatClient{
			UserLogin: login,
			conn:      client,
			saveFn:    func(context.Context) error { return nil },
		}

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			gc.persistCookies(context.Background())
		}()
		go func() {
			defer wg.Done()
			gc.LogoutRemote(context.Background())
		}()
		wg.Wait()

		if meta.Cookies != nil {
			t.Fatalf("iteration %d: Metadata.Cookies = %v, want nil (logout must win a race against a concurrent persist, never be resurrected)", i, meta.Cookies)
		}
	}
}

// TestPersistSkippedAfterLogout: persistCookies after logout must not write or save.
func TestPersistSkippedAfterLogout(t *testing.T) {
	client, err := gchatmeow.NewClient(gchatmeow.ClientOpts{Cookies: fakeCookies()})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	meta := &UserLoginMetadata{Cookies: fakeCookies()}
	login := newTestUserLogin(meta)
	var saveCount int
	gc := &GChatClient{
		UserLogin: login,
		conn:      client,
		saveFn:    func(context.Context) error { saveCount++; return nil },
	}

	gc.LogoutRemote(context.Background())
	if meta.Cookies != nil {
		t.Fatalf("LogoutRemote did not clear cookies: %v", meta.Cookies)
	}
	saveCountAfterLogout := saveCount

	gc.persistCookies(context.Background())

	if meta.Cookies != nil {
		t.Errorf("persistCookies() after LogoutRemote() resurrected cookies: %v, want nil", meta.Cookies)
	}
	if saveCount != saveCountAfterLogout {
		t.Errorf("persistCookies() after LogoutRemote() called save (count %d -> %d), want skipped entirely (no save call)", saveCountAfterLogout, saveCount)
	}
}
