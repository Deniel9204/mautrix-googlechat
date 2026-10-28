package connector

import (
	"context"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

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

// TestLoadUserLoginDisconnectsPriorClient: reloading a running login must disconnect the old client.
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

func TestLoadUserLoginNilPriorClientIsSafe(t *testing.T) {
	login := &bridgev2.UserLogin{
		UserLogin: &database.UserLogin{ID: gcid.MakeUserLoginID("g1")},
	}
	gc := &GChatConnector{}
	if err := gc.LoadUserLogin(context.Background(), login); err != nil {
		t.Fatalf("LoadUserLogin() error = %v", err)
	}
	// A second load must also be safe: the new client's conn is nil.
	if err := gc.LoadUserLogin(context.Background(), login); err != nil {
		t.Fatalf("second LoadUserLogin() error = %v", err)
	}
}

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

func TestMaxFileSizeNilMainFallsBackToZero(t *testing.T) {
	client := &GChatClient{}
	if got := client.maxFileSize(); got != 0 {
		t.Errorf("client.maxFileSize() = %d, want 0 for a bare *GChatClient", got)
	}
}

func TestAttachmentURLAndDownloadAttachmentFailCleanlyWhenNotConnected(t *testing.T) {
	client := &GChatClient{}
	if _, _, err := client.attachmentURL(nil); err == nil {
		t.Error("attachmentURL on a disconnected client returned no error, want one")
	}
	if _, _, _, err := client.downloadAttachment(context.Background(), "https://chat.google.com/x", 0); err == nil {
		t.Error("downloadAttachment on a disconnected client returned no error, want one")
	}
}
