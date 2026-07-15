package connector

// msgconv_adapter.go adapts msgconv.MessageConverter.ToMatrix (pure
// *pb.Message -> *bridgev2.ConvertedMessage conversion, pkg/msgconv/from-gchat.go)
// into the simplevent.Message[*pb.Message].ConvertMessageFunc shape
// events.go's MESSAGE_POSTED handling needs, and stamps MessageMetadata
// (dbmeta.go) onto every resulting part.
//
// This bookkeeping deliberately lives here, in the connector, and not in
// msgconv: msgconv's package doc (msgconv.go) states it holds "no portal, no
// intent, no gchatmeow client" and must stay ignorant of connector-local
// metadata types so it can be unit-tested (Task 3) against plain
// *bridgev2.ConvertedMessage output with no DB/connector dependencies at
// all.
import (
	"context"

	"maunium.net/go/mautrix/bridgev2"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv"
)

// convertMessageToMatrix returns a ConvertMessageFunc (see
// simplevent.Message[T]) that calls conv.ToMatrix and attaches
// MessageMetadata{TimestampMicro: msg.create_time} to every converted part.
// TimestampMicro round-trips Google Chat's microsecond create_time through
// the message's DB row so a later quote-reply (M3, SendReplyTarget) can
// reconstruct the original timestamp without re-fetching the source message.
//
// A fresh *MessageMetadata is allocated per part (rather than one shared
// pointer) so a future per-part mutation (e.g. M4's edit-dedup LastEditTime
// bump on a single part) can never alias into a sibling part's metadata.
//
// It also builds content.Mentions ("m.mentions") from msg's annotations via
// newInboundMentionResolver + inboundMentions (mentions.go, M3 Task 3, fix
// B2/gap G4 -- docs/research/08d §1.7/§6): every mention every converted
// part carries (there is normally exactly one text part, but this loop
// covers M5's future multi-part messages too) gets the same resolved
// Mentions block, since content.Mentions describes the whole event's
// mentions, not a per-part concept. conv.ToMatrix itself does not render
// annotations into HTML/pills yet (M2 plain-text scope, replaced by M3 Task
// 4) -- that is orthogonal to m.mentions, which this fixes now regardless.
func convertMessageToMatrix(conv *msgconv.MessageConverter) func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, msg *pb.Message) (*bridgev2.ConvertedMessage, error) {
	return func(ctx context.Context, portal *bridgev2.Portal, _ bridgev2.MatrixAPI, msg *pb.Message) (*bridgev2.ConvertedMessage, error) {
		cm := conv.ToMatrix(ctx, msg)
		ts := msg.GetCreateTime()
		mentions := inboundMentions(msg.GetAnnotations(), newInboundMentionResolver(portal))
		for _, part := range cm.Parts {
			part.DBMetadata = &MessageMetadata{TimestampMicro: ts}
			if mentions != nil {
				part.Content.Mentions = mentions
			}
		}
		return cm, nil
	}
}
