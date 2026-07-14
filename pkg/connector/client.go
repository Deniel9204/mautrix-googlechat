package connector

import (
	"context"
	"fmt"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"
	"maunium.net/go/mautrix/event"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
)

type GChatClient struct {
	UserLogin *bridgev2.UserLogin

	// Client is the gchatmeow client for this login. login.go's SubmitCookies
	// attaches an already-validated, "warm" client here (never nil after a
	// successful login); LoadUserLogin's rebuild-from-persisted-metadata path
	// on restart is Task 11's job.
	Client *gchatmeow.Client
}

var _ bridgev2.NetworkAPI = (*GChatClient)(nil)

func (c *GChatClient) Connect(_ context.Context) {
	c.UserLogin.BridgeState.Send(status.BridgeState{
		StateEvent: status.StateUnknownError,
		Error:      "gchat-not-implemented",
		Message:    "The Google Chat client is not implemented yet",
	})
}

func (c *GChatClient) Disconnect() {}

func (c *GChatClient) IsLoggedIn() bool { return false }

func (c *GChatClient) LogoutRemote(_ context.Context) {}

func (c *GChatClient) IsThisUser(_ context.Context, _ networkid.UserID) bool { return false }

func (c *GChatClient) GetChatInfo(_ context.Context, _ *bridgev2.Portal) (*bridgev2.ChatInfo, error) {
	return nil, fmt.Errorf("not implemented")
}

func (c *GChatClient) GetUserInfo(_ context.Context, _ *bridgev2.Ghost) (*bridgev2.UserInfo, error) {
	return nil, fmt.Errorf("not implemented")
}

func (c *GChatClient) GetCapabilities(_ context.Context, _ *bridgev2.Portal) *event.RoomFeatures {
	return &event.RoomFeatures{MaxTextLength: 4096}
}

func (c *GChatClient) HandleMatrixMessage(_ context.Context, _ *bridgev2.MatrixMessage) (*bridgev2.MatrixMessageResponse, error) {
	return nil, fmt.Errorf("sending messages is not implemented yet")
}
