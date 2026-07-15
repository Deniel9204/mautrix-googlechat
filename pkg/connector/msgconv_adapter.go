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
// It also builds content.Mentions ("m.mentions") from the mentions
// conv.ToMatrix reports it ACTUALLY rendered (gchatfmt.ParsedMentions, fix
// B2/gap G4 -- docs/research/08d §1.7/§6): every converted part (there is
// normally exactly one text part, but this loop covers M5's future
// multi-part messages too) gets its OWN cloneMentions copy of the same
// resolved Mentions block (content.Mentions describes the whole event's
// mentions, not a per-part concept, but each part still gets an independent
// object) -- matching the adjacent *MessageMetadata allocation's own "never
// alias into a sibling part" rule, just above.
//
// Deriving content.Mentions from ParsedMentions (rather than an independent
// second walk of msg.GetAnnotations(), as an earlier version did via the
// now-removed inboundMentions) is the phantom-ping fix: "who gets pinged" is
// now, by construction, exactly "whose mention pill gchatfmt would emit for
// this message" -- both keyed on the same per-annotation validity gate
// (spanWithinParent bounds + chip filter + resolver ok) inside gchatfmt. A
// malformed/out-of-bounds mention annotation that renders no pill (and whose
// "@Name" text is therefore absent from the body) can no longer sneak a
// ping into content.Mentions.
//
// The resolver (built once, below) is still passed INTO conv.ToMatrix, which
// threads it through gchatfmt.Parse to both render pills and resolve the
// ParsedMentions gaia ids -- so a single resolver instance drives both
// within one conversion.
func convertMessageToMatrix(conv *msgconv.MessageConverter) func(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, msg *pb.Message) (*bridgev2.ConvertedMessage, error) {
	return func(ctx context.Context, portal *bridgev2.Portal, _ bridgev2.MatrixAPI, msg *pb.Message) (*bridgev2.ConvertedMessage, error) {
		resolve := newInboundMentionResolver(portal)
		cm, parsed := conv.ToMatrix(ctx, msg, resolve)
		ts := msg.GetCreateTime()
		mentions := mentionsFromParsed(parsed)
		for _, part := range cm.Parts {
			part.DBMetadata = &MessageMetadata{TimestampMicro: ts}
			if mentions != nil {
				part.Content.Mentions = cloneMentions(mentions)
			}
		}
		return cm, nil
	}
}
