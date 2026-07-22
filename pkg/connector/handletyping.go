package connector

// handletyping.go -- Matrix -> Google Chat outbound typing notifications
// (HandleMatrixTyping, M4 Task 5). Issues the set_typing_state RPC, whose
// request carries a TYPING/STOPPED state and a group_id-or-topic_id context
// oneof.
//
// bridgev2's own handleMatrixTyping (mautrix-go bridgev2/portal.go:1006-1071)
// already does all the per-user diffing BEFORE calling into the network
// connector: it diffs the room's m.typing user set against what it last saw
// (portal.currentlyTyping) and calls TypingHandlingNetworkAPI.HandleMatrixTyping
// once per user that STARTED typing (IsTyping: true) and once per user that
// STOPPED (IsTyping: false). So this file only needs to build the request
// (group/topic context oneof, TYPING/STOPPED state selection): one
// HandleMatrixTyping call always means exactly one set_typing_state call,
// never a set to diff.
//
// thread_id (topic-scoped typing): the request's context oneof can carry a
// topic_id, but typing notifications are always group-scoped in practice,
// never topic-scoped. bridgev2.MatrixTyping (mautrix-go
// bridgev2/networkinterface.go:1486-1490) carries no thread/topic field
// either (just Portal, IsTyping, Type), so there is no signal available here
// to route a typing notification into a specific Matrix-thread-mapped GC
// topic. typingContext (below) still accepts a topicID parameter and builds
// the topic_id oneof arm when given one -- preserving the full request shape
// and giving it direct test coverage (handletyping_test.go) -- but
// HandleMatrixTyping itself always calls it with "" (the topic branch is
// unreachable from this call site).
import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

var _ bridgev2.TypingHandlingNetworkAPI = (*GChatClient)(nil)

// typingContext builds the *pb.TypingContext oneof for a SetTypingStateRequest:
// topicID != "" builds the topic_id arm (group_id nested inside the TopicId,
// same as buildReplyTarget's identical TopicId shape, handlematrix.go);
// topicID == "" builds the plain group_id arm. See this file's top-of-file
// doc comment for why HandleMatrixTyping itself always passes "".
func typingContext(group gcid.GroupID, topicID string) *pb.TypingContext {
	groupID := gchatmeow.PartsToGroupID(group.ID, group.IsDM)
	if topicID != "" {
		return &pb.TypingContext{Context: &pb.TypingContext_TopicId{TopicId: &pb.TopicId{
			GroupId: groupID,
			TopicId: proto.String(topicID),
		}}}
	}
	return &pb.TypingContext{Context: &pb.TypingContext_GroupId{GroupId: groupID}}
}

// HandleMatrixTyping issues set_typing_state for a Matrix typing start/stop
// in a portal room, building its request body:
//
//   - context: typingContext(group, ""), gcid.ParsePortalID(msg.Portal.ID) --
//     the same GroupId-oneof derivation every other outbound call in this
//     package uses (handlematrix.go, handleedit.go, handleredact.go,
//     handlereaction.go, handlereceipt.go).
//   - state: TypingState_TYPING when msg.IsTyping, TypingState_STOPPED
//     otherwise. bridgev2's framework (mautrix-go
//     bridgev2/portal.go:1006-1071) calls this method once per user that
//     started typing (IsTyping: true) and once per user that stopped
//     (IsTyping: false), so both states are genuinely reached here -- this
//     is not a start-only notification.
//
// request_header is deliberately NOT set here: gchatmeow.Client.SetTypingState
// (pkg/gchatmeow/api.go) stamps it itself, matching every other outbound RPC
// in this package (see sendNewTopic's doc comment, handlematrix.go, for the
// "connector builds business fields only, gchatmeow owns the header" split).
//
// The set_typing_state response carries a start_timestamp_usec, but it is
// unused here -- typing is fire-and-forget. This method discards
// SetTypingStateResponse entirely once the RPC succeeds.
func (c *GChatClient) HandleMatrixTyping(ctx context.Context, msg *bridgev2.MatrixTyping) error {
	group, err := gcid.ParsePortalID(msg.Portal.ID)
	if err != nil {
		return fmt.Errorf("googlechat: %w", err)
	}

	state := pb.TypingState_STOPPED
	if msg.IsTyping {
		state = pb.TypingState_TYPING
	}

	req := &pb.SetTypingStateRequest{
		State:   state.Enum(),
		Context: typingContext(group, ""),
	}

	send := c.setTypingStateFn
	if send == nil {
		conn := c.getConn()
		if conn == nil {
			return fmt.Errorf("googlechat: not connected")
		}
		send = conn.SetTypingState
	}

	if _, err := send(ctx, req); err != nil {
		return fmt.Errorf("googlechat: set_typing_state failed: %w", err)
	}
	return nil
}
