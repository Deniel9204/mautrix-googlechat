package connector

// connstate_test.go -- callbacks arriving from a connection that has already
// been superseded must not act on the shared GChatClient state.
//
// wireAndStart resets the one-shot initial-sync latch and installs a new
// conn, then tears the old one down via gchatmeow.Client.Disconnect -- which
// only cancels a context and does NOT wait for that conn's supervision
// goroutine to exit. The old conn can therefore still emit one more
// ConnStateConnected afterwards. Since initialSyncDone/syncInProgress live on
// the shared client with no per-conn tag, that late callback would otherwise
// race the new conn's own first Connected transition for the same latch.

import (
	"context"
	"testing"
	"time"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
)

func newTestConn(t *testing.T) *gchatmeow.Client {
	t.Helper()
	conn, err := gchatmeow.NewClient(gchatmeow.ClientOpts{})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return conn
}

// TestSupersededConnStateCallbackIgnored: the losing side of the race. If a
// stale conn's Connected callback consumes the latch, it runs syncChats under
// its OWN (already cancelled) context, which fails immediately and queues
// zero portals -- while the new conn's genuine first Connected sees the latch
// already taken and takes the catch-up branch instead. The account then sits
// CONNECTED with no portals until some later reconnect happens to re-trigger
// the latch.
func TestSupersededConnStateCallbackIgnored(t *testing.T) {
	active := newTestConn(t)
	superseded := newTestConn(t)

	gc := &GChatClient{UserLogin: newTestUserLogin(&UserLoginMetadata{})}
	gc.replaceConn(active)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // as the superseded conn's context would be

	gc.connStateCallback(ctx, superseded)(gchatmeow.ConnStateConnected, nil)

	if !gc.shouldSyncOnConnect() {
		t.Error("a superseded conn's Connected callback consumed the initial-sync latch, which belongs to the conn that replaced it")
	}
	if gc.isSyncInProgress() {
		t.Error("a superseded conn's Connected callback started a chat-list sync")
	}
	if got := gc.getLastState(); got != "" {
		t.Errorf("a superseded conn's callback reported bridge state %v, which would clobber the live conn's state", got)
	}
}

// TestActiveConnStateCallbackHandled is the other half: the guard must not
// swallow callbacks from the conn that IS current.
func TestActiveConnStateCallbackHandled(t *testing.T) {
	active := newTestConn(t)

	// saveFn stands in for the DB write persistCookies performs on a
	// Connected transition; these lightweight test logins have no Bridge.
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		saveFn:    func(context.Context) error { return nil },
	}
	gc.replaceConn(active)

	// Pre-cancelled so the spawned syncChats goroutine returns immediately
	// instead of attempting a real paginated_world round trip -- same pattern
	// as the existing handleConnState tests in client_test.go.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	gc.connStateCallback(ctx, active)(gchatmeow.ConnStateConnected, nil)

	if gc.shouldSyncOnConnect() {
		t.Error("the active conn's Connected callback did not consume the initial-sync latch")
	}
	time.Sleep(20 * time.Millisecond) // let the spawned syncChats goroutine return
}
