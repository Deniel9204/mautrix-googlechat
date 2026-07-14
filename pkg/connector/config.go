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
}
