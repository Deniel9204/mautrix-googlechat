package connector

import (
	"context"
	_ "embed"
	"strings"
	"text/template"

	"github.com/rs/zerolog"
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

// FormatDisplayname renders params through the configured
// displayname_template. A template execution error (e.g. an operator's
// custom template referencing a field that doesn't exist on
// DisplaynameParams -- text/template only catches that at Execute time, not
// at Parse/PostProcess time) previously discarded the error outright,
// leaving whatever partial output Execute had written to buf before failing
// with no trace anywhere. Now logged via zerolog.Ctx(ctx): FormatDisplayname
// still returns buf.String() (whatever was rendered before the failure,
// same as before -- there is no sensible alternative displayname to fall
// back to here, and ghost creation/update must not fail outright over a
// cosmetic template bug), but an operator debugging "why does this ghost
// have a broken/truncated name" now gets a warning log instead of silence.
func (c *Config) FormatDisplayname(ctx context.Context, params DisplaynameParams) string {
	var buf strings.Builder
	if err := c.displaynameTemplate.Execute(&buf, params); err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).
			Msg("googlechat: displayname_template execution failed, name may be incomplete")
	}
	return buf.String()
}

func upgradeConfig(helper configupgrade.Helper) {
	helper.Copy(configupgrade.Str, "displayname_template")
	helper.Copy(configupgrade.Int, "initial_chat_sync")
	helper.Copy(configupgrade.Bool, "disable_outbound_media")
}
