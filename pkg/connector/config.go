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

	// DisableOutboundMedia rejects Matrix media sends; Google's /uploads has been returning HTTP 500.
	DisableOutboundMedia bool `yaml:"disable_outbound_media"`

	// DisableInlineURLMedia stops fetching url_metadata image_url media, the bridge's only third-party egress.
	DisableInlineURLMedia bool `yaml:"disable_inline_url_media"`

	// SkipLeaveConfirmation skips the `leave confirm` step; negative so the zero value keeps the confirmation.
	SkipLeaveConfirmation bool `yaml:"skip_leave_confirmation"`

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

// FormatDisplayname renders displayname_template, logging (not failing on) template execution errors.
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
	helper.Copy(configupgrade.Bool, "disable_inline_url_media")
	helper.Copy(configupgrade.Bool, "skip_leave_confirmation")
}
