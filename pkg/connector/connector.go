package connector

import (
	"context"
	"fmt"

	"go.mau.fi/util/configupgrade"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/bridgev2/database"

	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv"
)

type GChatConnector struct {
	Bridge *bridgev2.Bridge
	Config Config
	// MsgConv is shared by all logins; it holds no per-login state.
	MsgConv *msgconv.MessageConverter

	// MaxFileSize caps inbound attachment downloads; 0 (before SetMaxFileSize) means no cap.
	MaxFileSize int64

	// leavingLoginsFn / removeFromPortalFn are test seams for the leave command.
	leavingLoginsFn    func(ctx context.Context, portal *bridgev2.Portal, user *bridgev2.User) ([]*GChatClient, error)
	removeFromPortalFn func(ctx context.Context, portal *bridgev2.Portal, user *bridgev2.User, logins []*GChatClient) error
}

var _ bridgev2.NetworkConnector = (*GChatConnector)(nil)
var _ bridgev2.MaxFileSizeingNetwork = (*GChatConnector)(nil)

// SetMaxFileSize implements bridgev2.MaxFileSizeingNetwork.
func (gc *GChatConnector) SetMaxFileSize(maxSize int64) {
	gc.MaxFileSize = maxSize
}

func (gc *GChatConnector) Init(bridge *bridgev2.Bridge) {
	gc.Bridge = bridge
	gc.MsgConv = msgconv.New()
	if proc, ok := bridge.Commands.(*commands.Processor); ok {
		proc.AddHandlers(cmdLeave)
	}
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

// gchatGeneralCaps is returned by GetCapabilities. LookupEmail is true because create_dm
// accepts an email, though a resolve-only email lookup fails (no email-to-gaia lookup).
var gchatGeneralCaps = &bridgev2.NetworkGeneralCapabilities{
	Provisioning: bridgev2.ProvisioningCapabilities{
		ResolveIdentifier: bridgev2.ResolveIdentifierCapabilities{
			CreateDM:    true,
			LookupEmail: true,
		},
		GroupCreation: map[string]bridgev2.GroupTypeCapabilities{
			spaceGroupType: {
				TypeDescription: "a Google Chat space",
				Name:            bridgev2.GroupFieldCapability{Allowed: true, Required: true},
				Participants:    bridgev2.GroupFieldCapability{Allowed: true},
			},
		},
	},
}

func (gc *GChatConnector) GetCapabilities() *bridgev2.NetworkGeneralCapabilities {
	return gchatGeneralCaps
}

func (gc *GChatConnector) GetBridgeInfoVersion() (info, capabilities int) {
	return 1, 1
}

func (gc *GChatConnector) GetConfig() (string, any, configupgrade.Upgrader) {
	return ExampleConfig, &gc.Config, configupgrade.SimpleUpgrader(upgradeConfig)
}

// LoadUserLogin does no network I/O; Connect builds the gchatmeow.Client.
// An already-running client must be disconnected first or its connection leaks.
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
