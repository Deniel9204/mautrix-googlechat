package connector

// handletyping.go -- Matrix -> Google Chat outbound typing notifications
// (HandleMatrixTyping, M4 Task 5). Ports mautrix_googlechat/portal.py's
// handle_matrix_typing (portal.py:1133-1146), which delegates to maugclib's
// mark_typing (client.py:477-497):
//
//	async def handle_matrix_typing(self, users: set[UserID]) -> None:
//	    user_map = {mxid: await u.User.get_by_mxid(mxid, create=False) for mxid in users}
//	    stopped_typing = [
//	        user_map[mxid].client.mark_typing(self.gcid, typing=False)
//	        for mxid in self._typing - users if user_map.get(mxid)
//	    ]
//	    started_typing = [
//	        user_map[mxid].client.mark_typing(self.gcid, typing=True)
//	        for mxid in users - self._typing if user_map.get(mxid)
//	    ]
//	    self._typing = users
//	    await asyncio.gather(*stopped_typing, *started_typing)
//
//	async def mark_typing(self, conversation_id, thread_id=None, typing=True) -> int:
//	    group_id = parsers.group_id_from_id(conversation_id)
//	    if thread_id:
//	        context = TypingContext(topic_id=TopicId(group_id=group_id, topic_id=thread_id))
//	    else:
//	        context = TypingContext(group_id=group_id)
//	    resp = await self.proto_set_typing_state(SetTypingStateRequest(
//	        request_header=self.gc_request_header,
//	        state=TYPING if typing else STOPPED,
//	        context=context,
//	    ))
//	    return resp.start_timestamp_usec
//
// bridgev2's own handleMatrixTyping (mautrix-go bridgev2/portal.go:1006-1071)
// already does everything Python's handle_matrix_typing does BEFORE calling
// into the network connector: it diffs the room's m.typing user set against
// what it last saw (portal.currentlyTyping) and calls
// TypingHandlingNetworkAPI.HandleMatrixTyping once per user that STARTED
// typing (IsTyping: true) and once per user that STOPPED (IsTyping: false)
// -- exactly Python's own started_typing/stopped_typing split above. So this
// file only needs to port mark_typing's REQUEST-BUILDING half (group/topic
// context oneof, TYPING/STOPPED state selection); the per-user diffing
// Python does inside handle_matrix_typing itself is the framework's job
// here, not this file's -- one HandleMatrixTyping call always means exactly
// one mark_typing call, never a set to diff.
//
// thread_id (topic-scoped typing): mark_typing's own signature accepts a
// thread_id parameter, but grep across the ENTIRE Python bridge shows no
// caller ever passes it a truthy value -- handle_matrix_typing
// (portal.py:1136,1141) always calls mark_typing(self.gcid, typing=...) with
// no thread_id, so Python's own typing notifications are always group-scoped
// in practice, never topic-scoped. bridgev2.MatrixTyping (mautrix-go
// bridgev2/networkinterface.go:1486-1490) carries no thread/topic field
// either (just Portal, IsTyping, Type), so there is no signal available here
// to route a typing notification into a specific Matrix-thread-mapped GC
// topic even if we wanted to exceed Python's own behavior. typingContext
// (below) still accepts a topicID parameter and builds the topic_id oneof
// arm when given one -- preserving the full request shape client.py defines,
// and giving it direct test coverage (handletyping_test.go) -- but
// HandleMatrixTyping itself always calls it with "" (the topic branch is
// unreachable from this call site), matching Python's own real-world
// behavior exactly.
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

// typingContext builds the *pb.TypingContext oneof for a SetTypingStateRequest,
// porting mark_typing's own if/else (client.py:481-489) field-by-field:
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
// in a portal room, matching mark_typing's request body (client.py:490-496)
// field-by-field:
//
//   - context: typingContext(group, ""), gcid.ParsePortalID(msg.Portal.ID) --
//     the same GroupId-oneof derivation every other outbound call in this
//     package uses (handlematrix.go, handleedit.go, handleredact.go,
//     handlereaction.go, handlereceipt.go).
//   - state: TypingState_TYPING when msg.IsTyping, TypingState_STOPPED
//     otherwise -- Python's `TYPING if typing else STOPPED` (client.py:493).
//     bridgev2's framework (mautrix-go bridgev2/portal.go:1006-1071) calls
//     this method once per user that started typing (IsTyping: true) and
//     once per user that stopped (IsTyping: false), so both states are
//     genuinely reached here, matching Python's own stopped_typing AND
//     started_typing gather (portal.py:1135-1146) -- this is not a
//     start-only notification.
//
// request_header is deliberately NOT set here: gchatmeow.Client.SetTypingState
// (pkg/gchatmeow/api.go) stamps it itself, matching every other outbound RPC
// in this package (see sendNewTopic's doc comment, handlematrix.go, for the
// "connector builds business fields only, gchatmeow owns the header" split).
//
// Python's own mark_typing returns resp.start_timestamp_usec (client.py:497)
// to its caller, but handle_matrix_typing (portal.py:1133-1146) never reads
// that return value -- it's fire-and-forget via asyncio.gather. This method
// likewise discards SetTypingStateResponse entirely once the RPC succeeds.
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
