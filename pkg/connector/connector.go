package connector

import (
	"context"
	"fmt"

	"go.mau.fi/util/configupgrade"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
)

type GChatConnector struct {
	Bridge *bridgev2.Bridge
	Config Config
}

var _ bridgev2.NetworkConnector = (*GChatConnector)(nil)

func (gc *GChatConnector) Init(bridge *bridgev2.Bridge) {
	gc.Bridge = bridge
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
		UserLogin: func() any { return &UserLoginMetadata{} },
	}
}

func (gc *GChatConnector) GetCapabilities() *bridgev2.NetworkGeneralCapabilities {
	return &bridgev2.NetworkGeneralCapabilities{}
}

func (gc *GChatConnector) GetBridgeInfoVersion() (info, capabilities int) {
	return 1, 1
}

func (gc *GChatConnector) GetConfig() (string, any, configupgrade.Upgrader) {
	return ExampleConfig, &gc.Config, configupgrade.SimpleUpgrader(upgradeConfig)
}

func (gc *GChatConnector) LoadUserLogin(_ context.Context, login *bridgev2.UserLogin) error {
	login.Client = &GChatClient{UserLogin: login}
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
