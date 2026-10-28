// Package msgconv converts between Google Chat proto messages and Matrix event content.
package msgconv

// MessageConverter holds conversion configuration only.
type MessageConverter struct{}

// New creates a MessageConverter.
func New() *MessageConverter {
	return &MessageConverter{}
}
