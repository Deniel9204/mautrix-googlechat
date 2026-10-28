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

var _ bridgev2.RedactionHandlingNetworkAPI = (*GChatClient)(nil)

// HandleMatrixMessageRemove issues delete_message for a bridged message redacted in a portal room.
func (c *GChatClient) HandleMatrixMessageRemove(ctx context.Context, msg *bridgev2.MatrixMessageRemove) error {
	// Relay mode only: refuse to delete a message another relayed user sent (relayauth.go).
	if err := checkRelayOwnership(msg.OrigSender, msg.TargetMessage.SenderMXID); err != nil {
		return err
	}

	group, err := gcid.ParsePortalID(msg.Portal.ID)
	if err != nil {
		return fmt.Errorf("googlechat: %w", err)
	}

	messageID := gcid.ParseMessageID(msg.TargetMessage.ID)
	topicID, _ := threadRootTopicID(msg.TargetMessage)

	req := &pb.DeleteMessageRequest{
		MessageId: &pb.MessageId{
			ParentId: &pb.MessageParentId{
				Parent: &pb.MessageParentId_TopicId{
					TopicId: &pb.TopicId{
						GroupId: gchatmeow.PartsToGroupID(group.ID, group.IsDM),
						TopicId: proto.String(topicID),
					},
				},
			},
			MessageId: proto.String(messageID),
		},
	}

	send := c.deleteMessageFn
	if send == nil {
		conn := c.getConn()
		if conn == nil {
			return fmt.Errorf("googlechat: not connected")
		}
		send = conn.DeleteMessage
	}

	if _, err := send(ctx, req); err != nil {
		return fmt.Errorf("googlechat: delete_message failed: %w", err)
	}
	return nil
}
