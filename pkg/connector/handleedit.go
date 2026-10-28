package connector

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

var _ bridgev2.EditHandlingNetworkAPI = (*GChatClient)(nil)

// HandleMatrixEdit issues edit_message for a bridged message edited in a portal room.
// On success it records the server's last_edit_time so the inbound echo dedups.
func (c *GChatClient) HandleMatrixEdit(ctx context.Context, edit *bridgev2.MatrixEdit) error {
	// Relay mode only: refuse to rewrite a message another relayed user sent (see relayauth.go).
	if err := checkRelayOwnership(edit.OrigSender, edit.EditTarget.SenderMXID); err != nil {
		return err
	}

	// Edits must be TEXT, stricter than new sends; bridgev2's generic cap check doesn't enforce this.
	if edit.Content.MsgType != event.MsgText {
		return bridgev2.ErrUnsupportedMessageType
	}

	group, err := gcid.ParsePortalID(edit.Portal.ID)
	if err != nil {
		return fmt.Errorf("googlechat: %w", err)
	}

	messageID := gcid.ParseMessageID(edit.EditTarget.ID)
	// bridgev2 guarantees EditTarget is non-nil here.
	topicID, _ := threadRootTopicID(edit.EditTarget)

	resolve := newOutboundMentionResolver(ctx, edit.Portal)
	text, annotations := c.msgConverter().FromMatrix(ctx, edit.Content, resolve)

	req := &pb.EditMessageRequest{
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
		TextBody:    proto.String(text),
		Annotations: annotations,
		MessageInfo: &pb.MessageInfo{
			AcceptFormatAnnotations: proto.Bool(true),
		},
	}

	send := c.editMessageFn
	if send == nil {
		conn := c.getConn()
		if conn == nil {
			return fmt.Errorf("googlechat: not connected")
		}
		send = conn.EditMessage
	}

	resp, err := send(ctx, req)
	if err != nil {
		return fmt.Errorf("googlechat: edit_message failed: %w", err)
	}

	lastEditTime := resp.GetMessage().GetLastEditTime()
	if meta, ok := edit.EditTarget.Metadata.(*MessageMetadata); ok && meta != nil {
		meta.LastEditTime = lastEditTime
	} else {
		edit.EditTarget.Metadata = &MessageMetadata{LastEditTime: lastEditTime}
	}

	return nil
}
