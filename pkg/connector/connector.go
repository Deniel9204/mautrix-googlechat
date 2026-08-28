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
	// event shape (events.go's inbound MESSAGE_POSTED handling). Populated
	// here rather than per-GChatClient: it holds no per-login state
	// (msgconv.go: "conversion configuration only"), so one shared instance
	// is enough for every UserLogin this connector serves, same as
	// mautrix-meta's MetaConnector.MsgConv (_reference/meta/pkg/connector/connector.go).
	MsgConv *msgconv.MessageConverter

	// MaxFileSize caps how large an inbound attachment download may be
	// (media.go's GChatClient.maxFileSize, threaded into
	// gchatmeow.Client.DownloadAttachment) -- the running homeserver's own
	// configured max upload size. bridgev2
	// calls SetMaxFileSize below "asynchronously soon after startup"
	// (bridgev2/networkinterface.go's MaxFileSizeingNetwork doc comment);
	// until that first call lands, MaxFileSize stays at its zero value,
	// which gchatmeow.DownloadAttachment's own maxSize<=0 contract already
	// treats as "no cap" (download.go) -- an intentionally permissive
	// default for the narrow startup race, not a design requiring a
	// separate fallback constant (mirrors mautrix-meta's MetaConnector,
	// _reference/meta/pkg/connector/connector.go, which does the same).
	MaxFileSize int64
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
// sends a message. Google Chat is not that network: sending a message
// never requires a follow-up mark-read, so nothing marks a self-authored
// message read except the one genuine-read-receipt path this connector
// already wires (handlereceipt.go) -- if Google Chat's own
// server needed an explicit nudge to treat a self-authored message as
// already read, that path would have surfaced it, and none is required.
// This matches
// _reference/googlechat-megabridge/pkg/connector/connector.go's own
// GetCapabilities, which returns the same empty struct.
// gchatGeneralCaps is returned by GetCapabilities. The Provisioning half
// tells the provisioning API what the start-chat UI may offer; createchat.go
// implements the handlers behind it.
//
// LookupEmail is true because an email CAN be turned into a chat -- create_dm
// accepts it as an invitee -- even though it cannot be resolved to a user
// WITHOUT doing so, since the private API has no email-to-gaia lookup. That
// distinction is ResolveIdentifier's to enforce (it refuses a resolve-only
// call for an email rather than creating a conversation as a side effect);
// advertising false here would hide the only way most people identify a
// colleague. LookupPhone and LookupUsername stay false: Google Chat has
// neither concept.
//
// GroupCreation is deliberately EMPTY. create_group is implemented at the RPC
// layer but every request shape tried so far -- including the one
// purple-googlechat uses -- is rejected with a bare, detail-free HTTP 400 by
// the account it was tested against, so the capability is not advertised:
// offering a space-creation affordance that always fails is worse than not
// offering one. See the follow-up issue linked from createchat.go.
var gchatGeneralCaps = &bridgev2.NetworkGeneralCapabilities{
	Provisioning: bridgev2.ProvisioningCapabilities{
		ResolveIdentifier: bridgev2.ResolveIdentifierCapabilities{
			CreateDM:    true,
			LookupEmail: true,
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

// LoadUserLogin fills login.Client with a fresh *GChatClient shell -- no
// network I/O here (LoadUserLogin runs under the global cache lock, so the
// client is constructed from login.Metadata only, with network I/O deferred
// to Connect); GChatClient.Connect builds the actual gchatmeow.Client from
// login.Metadata lazily, whether this is the very first load after a restart
// or the login command resubmitting cookies for an existing row.
//
// bridgev2 calls LoadUserLogin again on an ALREADY-RUNNING login in two
// cases: User.NewLogin reusing an existing UserLogin row (a resubmitted
// login.go SubmitCookies) and Bridge.ResetNetworkConnections's
// recreateClient. Either way, login.Client may already hold a *GChatClient
// whose gchatmeow.Client is mid-connection; disconnecting it before
// overwriting login.Client is required, or its Connect goroutine (and live
// webchannel session) leaks forever.
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
