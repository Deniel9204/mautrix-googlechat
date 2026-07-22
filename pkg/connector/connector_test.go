package connector

import (
	"context"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// TestLoadUserLoginFirstCallBuildsShell covers the plain restart path: no
// prior client, LoadUserLogin just installs a fresh *GChatClient shell (no
// network I/O -- see connector.go's LoadUserLogin doc comment).
func TestLoadUserLoginFirstCallBuildsShell(t *testing.T) {
	login := &bridgev2.UserLogin{
		UserLogin: &database.UserLogin{ID: gcid.MakeUserLoginID("g1")},
	}
	gc := &GChatConnector{}

	if err := gc.LoadUserLogin(context.Background(), login); err != nil {
		t.Fatalf("LoadUserLogin() error = %v", err)
	}

	newClient, ok := login.Client.(*GChatClient)
	if !ok {
		t.Fatalf("login.Client is %T, want *GChatClient", login.Client)
	}
	if newClient.UserLogin != login {
		t.Errorf("new GChatClient.UserLogin = %v, want %v", newClient.UserLogin, login)
	}
}

// TestLoadUserLoginDisconnectsPriorClient is the carry-over (a) regression
// test: a resubmitted login (User.NewLogin reusing an existing UserLogin row)
// or Bridge.ResetNetworkConnections's recreateClient calls LoadUserLogin
// again on an already-running login. The previous *GChatClient's gchatmeow
// client must be disconnected before it is replaced, or its Connect goroutine
// (and live webchannel session) leaks forever.
func TestLoadUserLoginDisconnectsPriorClient(t *testing.T) {
	client, err := gchatmeow.NewClient(gchatmeow.ClientOpts{})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	var disconnectCount int
	old := &GChatClient{
		conn: client,
		disconnectFn: func(c *gchatmeow.Client) {
			if c == client {
				disconnectCount++
			}
		},
	}
	login := &bridgev2.UserLogin{
		UserLogin: &database.UserLogin{ID: gcid.MakeUserLoginID("g1")},
		Client:    old,
	}
	gc := &GChatConnector{}

	if err := gc.LoadUserLogin(context.Background(), login); err != nil {
		t.Fatalf("LoadUserLogin() error = %v", err)
	}

	if disconnectCount != 1 {
		t.Errorf("prior client's Disconnect called %d times, want 1", disconnectCount)
	}
	newClient, ok := login.Client.(*GChatClient)
	if !ok {
		t.Fatalf("login.Client is %T, want *GChatClient", login.Client)
	}
	if newClient == old {
		t.Fatal("LoadUserLogin() did not replace the old GChatClient")
	}
	if newClient.getConn() != nil {
		t.Error("new GChatClient already has a conn installed, want nil (built lazily by Connect)")
	}
}

// TestLoadUserLoginNilPriorClientIsSafe guards the type assertion in
// LoadUserLogin against a nil login.Client (the very first load) or a
// login.Client of some other concrete type -- neither should panic or call
// Disconnect.
func TestLoadUserLoginNilPriorClientIsSafe(t *testing.T) {
	login := &bridgev2.UserLogin{
		UserLogin: &database.UserLogin{ID: gcid.MakeUserLoginID("g1")},
	}
	gc := &GChatConnector{}
	if err := gc.LoadUserLogin(context.Background(), login); err != nil {
		t.Fatalf("LoadUserLogin() error = %v", err)
	}
	// Calling it again (as a second restart-style load) must also be safe:
	// the freshly-installed *GChatClient has a nil conn, so Disconnect() is a
	// real no-op, not a panic.
	if err := gc.LoadUserLogin(context.Background(), login); err != nil {
		t.Fatalf("second LoadUserLogin() error = %v", err)
	}
}

// TestSetMaxFileSize pins M5 Task 3's bridgev2.MaxFileSizeingNetwork wiring:
// bridgev2 calls SetMaxFileSize once soon after startup with the
// homeserver's own configured max upload size, and GChatClient.maxFileSize
// (media.go) must read back exactly that value.
func TestSetMaxFileSize(t *testing.T) {
	gc := &GChatConnector{}
	gc.SetMaxFileSize(12345)
	if gc.MaxFileSize != 12345 {
		t.Errorf("MaxFileSize = %d, want 12345", gc.MaxFileSize)
	}
	client := &GChatClient{Main: gc}
	if got := client.maxFileSize(); got != 12345 {
		t.Errorf("client.maxFileSize() = %d, want 12345", got)
	}
}

// TestMaxFileSizeNilMainFallsBackToZero mirrors msgConverter()'s identical
// bare-*GChatClient test fallback (client.go): a *GChatClient with no Main
// (as constructed by many other tests in this package) must not panic, and
// must report 0 ("no cap", per gchatmeow.DownloadAttachment's own
// maxSize<=0 contract) rather than some arbitrary default.
func TestMaxFileSizeNilMainFallsBackToZero(t *testing.T) {
	client := &GChatClient{}
	if got := client.maxFileSize(); got != 0 {
		t.Errorf("client.maxFileSize() = %d, want 0 for a bare *GChatClient", got)
	}
}

// TestAttachmentURLAndDownloadAttachmentFailCleanlyWhenNotConnected pins the
// nil-conn guard in both mediaFetcher methods (media.go): a *GChatClient
// with no live gchatmeow.Client (e.g. mid-reconnect, or never connected)
// must return a plain error, not panic -- convertOneAttachment (media.go)
// then skips that attachment like any other download failure.
func TestAttachmentURLAndDownloadAttachmentFailCleanlyWhenNotConnected(t *testing.T) {
	client := &GChatClient{}
	if _, _, err := client.attachmentURL(nil); err == nil {
		t.Error("attachmentURL on a disconnected client returned no error, want one")
	}
	if _, _, _, err := client.downloadAttachment(context.Background(), "https://chat.google.com/x", 0); err == nil {
		t.Error("downloadAttachment on a disconnected client returned no error, want one")
	}
}
