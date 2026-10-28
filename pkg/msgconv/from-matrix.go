package msgconv

import (
	"context"

	"maunium.net/go/mautrix/event"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv/matrixfmt"
)

// FromMatrix extracts the Google Chat text_body and annotations for a Matrix message.
// Annotations are nil when the content has no HTML formatting.
func (mc *MessageConverter) FromMatrix(ctx context.Context, content *event.MessageEventContent, mention matrixfmt.MentionResolver) (string, []*pb.Annotation) {
	return matrixfmt.Parse(ctx, content, mention)
}
