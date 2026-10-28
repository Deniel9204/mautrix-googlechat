package connector

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

// TestSupersededConnStateCallbackIgnored: a stale conn's Connected must not consume the new conn's sync latch.
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

func TestActiveConnStateCallbackHandled(t *testing.T) {
	active := newTestConn(t)

	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		saveFn:    func(context.Context) error { return nil },
	}
	gc.replaceConn(active)

	// Pre-cancelled so the spawned syncChats returns immediately.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	gc.connStateCallback(ctx, active)(gchatmeow.ConnStateConnected, nil)

	if gc.shouldSyncOnConnect() {
		t.Error("the active conn's Connected callback did not consume the initial-sync latch")
	}
	time.Sleep(20 * time.Millisecond) // let the spawned syncChats goroutine return
}
