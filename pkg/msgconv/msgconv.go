// Package msgconv converts between the Google Chat proto message shape and
// Matrix event content. It is pure conversion: no HTTP calls, no
// gchatmeow.Client calls, and no bridgev2 network interfaces (NetworkAPI,
// NetworkConnector, etc.) are imported or invoked here. Importing bridgev2
// *data* types (bridgev2.ConvertedMessage/ConvertedMessagePart, event.
// MessageEventContent) is fine -- those are plain structs, not the network
// layer -- and doing so lets the connector (pkg/connector, M2 Task 4) hand
// msgconv's return value straight to bridgev2 without an adapter step. This
// mirrors how mautrix-meta's pkg/msgconv is structured (see
// _reference/meta/pkg/msgconv/msgconv.go and from-meta.go, which build
// *bridgev2.ConvertedMessage directly and only pull in bridgev2.Bridge/
// Portal/MatrixAPI for pieces msgconv does NOT need here: ghost/ID lookups
// and media re-upload, both out of scope until later milestones).
//
// M2 scope was plain text only: a Message's text_body became one m.text
// part (ToMatrix, from-gchat.go). M3 (Task 4) adds annotation-based HTML
// formatting on top of that same part, both directions -- gchatfmt.Parse
// inbound, matrixfmt.Parse outbound -- via a MentionResolver seam the
// connector supplies (pkg/connector/mentions.go); M5 adds attachment parts
// alongside it.
package msgconv

// MessageConverter holds conversion configuration only -- no portal, no
// intent, no gchatmeow client. Those belong to the connector, which owns
// the network side and calls into msgconv with plain data (a *pb.Message)
// and gets plain data back (*bridgev2.ConvertedMessage).
type MessageConverter struct{}

// New creates a MessageConverter. Takes no arguments in M2 (there are no
// config knobs yet); later milestones may add fields here (e.g. formatting
// options for M3) without changing ToMatrix's signature.
func New() *MessageConverter {
	return &MessageConverter{}
}
