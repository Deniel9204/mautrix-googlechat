package main

import (
	"maunium.net/go/mautrix/bridgev2/matrix/mxmain"

	"github.com/Deniel9204/mautrix-googlechat/pkg/connector"
)

// Filled at build time with -X linker flags.
var (
	Tag       = "unknown"
	Commit    = "unknown"
	BuildTime = "unknown"
)

var m = mxmain.BridgeMain{
	Name:        "mautrix-googlechat",
	URL:         "https://github.com/Deniel9204/mautrix-googlechat",
	Description: "A Matrix-Google Chat puppeting bridge.",
	Version:     "0.1.0",
	Connector:   &connector.GChatConnector{},
}

func main() {
	m.InitVersion(Tag, Commit, BuildTime)
	m.Run()
}
