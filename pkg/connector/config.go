package connector

import (
	_ "embed"
	"strings"
	"text/template"

	"go.mau.fi/util/configupgrade"
)

//go:embed example-config.yaml
var ExampleConfig string

type Config struct {
	DisplaynameTemplate string `yaml:"displayname_template"`
	InitialChatSync     int    `yaml:"initial_chat_sync"`

	// DisableOutboundMedia turns off sending Matrix media (m.image/m.file/
	// m.video/m.audio) to Google Chat, even though the send path is fully
	// implemented (handlematrix.go's HandleMatrixMessage media branch, M5
	// Task 5): Google's /uploads endpoint has reportedly returned HTTP 500
	// for every upload since ~Feb 2026 (upstream issue #114,
	// https://github.com/mautrix/googlechat/issues/114). When true,
	// HandleMatrixMessage rejects a media message immediately with
	// errOutboundMediaDisabled (handlematrix.go) -- a clean, explicit
	// "unsupported (upstream #114)" message-send-status failure -- instead
	// of attempting a download+upload that this bridge's own operator
	// already knows will fail against their Google Chat account. Left false
	// by default: the upload code itself is exercised and correct (M5 Task
	// 2's own gchatmeow.Client.UploadFile), and whether #114 actually
	// affects a given account/session is a live-server fact this bridge
	// cannot determine ahead of time.
	DisableOutboundMedia bool `yaml:"disable_outbound_media"`

	displaynameTemplate *template.Template `yaml:"-"`
}

type DisplaynameParams struct {
	Name      string
	FirstName string
	Email     string
}

func (c *Config) PostProcess() error {
	var err error
	c.displaynameTemplate, err = template.New("displayname").Parse(c.DisplaynameTemplate)
	return err
}

func (c *Config) FormatDisplayname(params DisplaynameParams) string {
	var buf strings.Builder
	_ = c.displaynameTemplate.Execute(&buf, params)
	return buf.String()
}

func upgradeConfig(helper configupgrade.Helper) {
	helper.Copy(configupgrade.Str, "displayname_template")
	helper.Copy(configupgrade.Int, "initial_chat_sync")
	helper.Copy(configupgrade.Bool, "disable_outbound_media")
}
