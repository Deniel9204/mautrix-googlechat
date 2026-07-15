package connector

import (
	"context"
	"fmt"

	"go.mau.fi/util/configupgrade"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"

	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv"
)

type GChatConnector struct {
	Bridge *bridgev2.Bridge
	Config Config
	// MsgConv converts Google Chat proto messages into bridgev2's Matrix
	// event shape (events.go's inbound MESSAGE_POSTED handling, M2 Task 4).
	// Populated here rather than per-GChatClient: it holds no per-login
	// state (msgconv.go: "conversion configuration only"), so one shared
	// instance is enough for every UserLogin this connector serves, same as
	// mautrix-meta's MetaConnector.MsgConv (_reference/meta/pkg/connector/connector.go).
	MsgConv *msgconv.MessageConverter
}

var _ bridgev2.NetworkConnector = (*GChatConnector)(nil)

func (gc *GChatConnector) Init(bridge *bridgev2.Bridge) {
	gc.Bridge = bridge
	gc.MsgConv = msgconv.New()
}

func (gc *GChatConnector) Start(_ context.Context) error {
	return gc.Config.PostProcess()
}

func (gc *GChatConnector) GetName() bridgev2.BridgeName {
	return bridgev2.BridgeName{
		DisplayName:          "Google Chat",
		NetworkURL:           "https://chat.google.com",
		NetworkIcon:          "mxc://maunium.net/BDIWAQcbpPGASPUUBuEGWXnQ",
		NetworkID:            "googlechat",
		BeeperBridgeType:     "googlechat",
		DefaultPort:          29320,
		DefaultCommandPrefix: "!gc",
	}
}

func (gc *GChatConnector) GetDBMetaTypes() database.MetaTypes {
	return database.MetaTypes{
		Portal:    func() any { return &PortalMetadata{} },
		Ghost:     func() any { return &GhostMetadata{} },
		Message:   func() any { return &MessageMetadata{} },
		Reaction:  func() any { return &ReactionMetadata{} },
		UserLogin: func() any { return &UserLoginMetadata{} },
	}
}

// GetCapabilities deliberately leaves ImplicitReadReceipts at its zero value
// (false) -- unlike mautrix-meta, which sets it true for Messenger/Instagram
// (_reference/meta/pkg/connector/capabilities.go), because "should the
// bridge call HandleMatrixReadReceipt with fake data when receiving a new
// message" (networkinterface.go:365-368's own doc comment) only needs to be
// true when the network requires each message to be marked read
// independently and does NOT automatically do so when the same account
// sends a message. Google Chat is not that network: user.py's own
// handle_matrix_message (matrix.py) never calls User.mark_read after
// sending, and NOTHING else in portal.py calls it either except the one
// genuine-read-receipt path this connector already wires (handlereceipt.go,
// M4 Task 4) -- if Google Chat's own server needed an explicit nudge to
// treat a self-authored message as already read, the Python bridge would
// have needed one too, and it has none. This matches
// _reference/googlechat-megabridge/pkg/connector/connector.go's own
// GetCapabilities, which returns the same empty struct.
func (gc *GChatConnector) GetCapabilities() *bridgev2.NetworkGeneralCapabilities {
	return &bridgev2.NetworkGeneralCapabilities{}
}

func (gc *GChatConnector) GetBridgeInfoVersion() (info, capabilities int) {
	return 1, 1
}

func (gc *GChatConnector) GetConfig() (string, any, configupgrade.Upgrader) {
	return ExampleConfig, &gc.Config, configupgrade.SimpleUpgrader(upgradeConfig)
}

// LoadUserLogin fills login.Client with a fresh *GChatClient shell -- no
// network I/O here (docs/research/04 §8: "LoadUserLogin runs under the global
// cache lock -- construct the client from login.Metadata only; do network I/O
// in Connect"); GChatClient.Connect builds the actual gchatmeow.Client from
// login.Metadata lazily, whether this is the very first load after a restart
// or the login command resubmitting cookies for an existing row.
//
// bridgev2 calls LoadUserLogin again on an ALREADY-RUNNING login in two
// cases: User.NewLogin reusing an existing UserLogin row (a resubmitted
// login.go SubmitCookies) and Bridge.ResetNetworkConnections's
// recreateClient. Either way, login.Client may already hold a *GChatClient
// whose gchatmeow.Client is mid-connection; disconnecting it before
// overwriting login.Client is required, or its Connect goroutine (and live
// webchannel session) leaks forever -- the exact goroutine leak an earlier
// Task 10 review caught.
func (gc *GChatConnector) LoadUserLogin(_ context.Context, login *bridgev2.UserLogin) error {
	if old, ok := login.Client.(*GChatClient); ok && old != nil {
		old.Disconnect()
	}
	login.Client = &GChatClient{Main: gc, UserLogin: login}
	return nil
}

func (gc *GChatConnector) GetLoginFlows() []bridgev2.LoginFlow {
	return []bridgev2.LoginFlow{{
		Name:        "Cookies",
		Description: "Log in with cookies extracted from chat.google.com",
		ID:          "cookies",
	}}
}

func (gc *GChatConnector) CreateLogin(_ context.Context, user *bridgev2.User, flowID string) (bridgev2.LoginProcess, error) {
	if flowID != "cookies" {
		return nil, fmt.Errorf("login flow %s is not implemented yet (M1)", flowID)
	}
	return &GChatLogin{User: user, Main: gc}, nil
}
