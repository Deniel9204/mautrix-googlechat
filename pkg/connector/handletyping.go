package connector

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

// typingContext builds the group_id arm, or the topic_id arm when topicID is set (unused in practice).
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

// HandleMatrixTyping issues set_typing_state; bridgev2 calls it once per start and once per stop.
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
