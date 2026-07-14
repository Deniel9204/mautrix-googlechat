package connector

// handleGChatEvent is the OnStreamEvent callback installed on every
// gchatmeow.Client by GChatClient.wireAndStart (client.go). It runs
// synchronously and in order on the client's Connect goroutine (see
// pkg/gchatmeow/client.go's "Goroutine model" doc comment).
//
// M1 scope (this task) is a type-switch skeleton over every event body type
// the proto defines, logging "unhandled (M2+)" at debug and doing nothing
// else -- no business logic. Task 12 (chat-list sync) and later M2 fill in
// the bodies that matter (MessagePosted et al.); this stub exists purely so
// Connect has somewhere real to dispatch to instead of a nil callback.
import (
	"context"

	"github.com/rs/zerolog"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

func (c *GChatClient) handleGChatEvent(ctx context.Context, evt *pb.Event) {
	log := zerolog.Ctx(ctx)
	switch evt.GetBody().GetType().(type) {
	case *pb.Event_EventBody_GroupViewed:
		log.Debug().Msg("googlechat: unhandled GroupViewed event (M2+)")
	case *pb.Event_EventBody_GroupUpdated:
		log.Debug().Msg("googlechat: unhandled GroupUpdated event (M2+)")
	case *pb.Event_EventBody_MessagePosted:
		log.Debug().Msg("googlechat: unhandled MessagePosted event (M2+)")
	case *pb.Event_EventBody_WebPushNotification:
		log.Debug().Msg("googlechat: unhandled WebPushNotification event (M2+)")
	case *pb.Event_EventBody_MembershipChanged:
		log.Debug().Msg("googlechat: unhandled MembershipChanged event (M2+)")
	case *pb.Event_EventBody_MessageDeleted:
		log.Debug().Msg("googlechat: unhandled MessageDeleted event (M2+)")
	case *pb.Event_EventBody_MessageReaction:
		log.Debug().Msg("googlechat: unhandled MessageReaction event (M2+)")
	case *pb.Event_EventBody_UserStatusUpdated:
		log.Debug().Msg("googlechat: unhandled UserStatusUpdated event (M2+)")
	case *pb.Event_EventBody_TypingStateChanged:
		log.Debug().Msg("googlechat: unhandled TypingStateChanged event (M2+)")
	case *pb.Event_EventBody_ReadReceiptChanged:
		log.Debug().Msg("googlechat: unhandled ReadReceiptChanged event (M2+)")
	default:
		log.Debug().Msg("googlechat: unhandled event with no/unknown body type (M2+)")
	}
}
