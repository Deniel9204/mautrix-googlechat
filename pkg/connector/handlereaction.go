package connector

// Google Chat reactions are per-emoji, not one-per-user, so EmojiID is always set and
// MaxReactions is 0 (unlimited). Emoji.unicode never carries U+FE0F; Matrix keys do.
import (
	"context"
	"fmt"

	"github.com/rs/zerolog"
	"go.mau.fi/util/variationselector"
	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

var _ bridgev2.ReactionHandlingNetworkAPI = (*GChatClient)(nil)

// PreHandleMatrixReaction strips the variation selector; EmojiID and Emoji are the same bare string.
func (c *GChatClient) PreHandleMatrixReaction(_ context.Context, msg *bridgev2.MatrixReaction) (bridgev2.MatrixReactionPreResponse, error) {
	emoji := variationselector.Remove(msg.Content.RelatesTo.Key)
	return bridgev2.MatrixReactionPreResponse{
		SenderID: gcid.MakeUserID(string(c.UserLogin.ID)),
		EmojiID:  networkid.EmojiID(emoji),
		Emoji:    emoji,
	}, nil
}

// HandleMatrixReaction issues update_reaction (ADD) and caches the resolved topic id in
// ReactionMetadata for later removal.
func (c *GChatClient) HandleMatrixReaction(ctx context.Context, msg *bridgev2.MatrixReaction) (*database.Reaction, error) {
	group, err := gcid.ParsePortalID(msg.Portal.ID)
	if err != nil {
		return nil, fmt.Errorf("googlechat: %w", err)
	}

	messageID := gcid.ParseMessageID(msg.TargetMessage.ID)
	topicID, _ := threadRootTopicID(msg.TargetMessage)

	req := &pb.UpdateReactionRequest{
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
		Emoji: &pb.Emoji{Content: &pb.Emoji_Unicode{Unicode: msg.PreHandleResp.Emoji}},
		Type:  pb.UpdateReactionRequest_ADD.Enum(),
	}

	send := c.updateReactionFn
	if send == nil {
		conn := c.getConn()
		if conn == nil {
			return nil, fmt.Errorf("googlechat: not connected")
		}
		send = conn.UpdateReaction
	}

	if _, err := send(ctx, req); err != nil {
		return nil, fmt.Errorf("googlechat: update_reaction (add) failed: %w", err)
	}

	return &database.Reaction{Metadata: &ReactionMetadata{TopicID: topicID}}, nil
}

// HandleMatrixReactionRemove issues update_reaction (REMOVE). The emoji is EmojiID, not the DB
// row's Emoji field, which bridgev2 leaves empty when EmojiID is set.
func (c *GChatClient) HandleMatrixReactionRemove(ctx context.Context, msg *bridgev2.MatrixReactionRemove) error {
	// Relay mode only: refuse to remove another relayed user's reaction.
	if err := checkRelayOwnership(msg.OrigSender, msg.TargetReaction.SenderMXID); err != nil {
		return err
	}

	group, err := gcid.ParsePortalID(msg.Portal.ID)
	if err != nil {
		return fmt.Errorf("googlechat: %w", err)
	}

	messageID := gcid.ParseMessageID(msg.TargetReaction.MessageID)
	topicID := c.reactionTopicID(ctx, msg.TargetReaction)

	req := &pb.UpdateReactionRequest{
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
		Emoji: &pb.Emoji{Content: &pb.Emoji_Unicode{Unicode: string(msg.TargetReaction.EmojiID)}},
		Type:  pb.UpdateReactionRequest_REMOVE.Enum(),
	}

	send := c.updateReactionFn
	if send == nil {
		conn := c.getConn()
		if conn == nil {
			return fmt.Errorf("googlechat: not connected")
		}
		send = conn.UpdateReaction
	}

	if _, err := send(ctx, req); err != nil {
		return fmt.Errorf("googlechat: update_reaction (remove) failed: %w", err)
	}
	return nil
}

// reactionTopicID returns the cached ReactionMetadata topic id, else looks up the target message's
// stored topic id (GC-originated reactions have no cached one), else the target message id.
func (c *GChatClient) reactionTopicID(ctx context.Context, r *database.Reaction) string {
	if meta, ok := r.Metadata.(*ReactionMetadata); ok && meta != nil && meta.TopicID != "" {
		return meta.TopicID
	}

	get := c.getMessageFn
	if get == nil {
		if c.UserLogin == nil || c.UserLogin.Bridge == nil {
			return string(r.MessageID)
		}
		get = c.UserLogin.Bridge.DB.Message.GetFirstPartByID
	}
	msg, err := get(ctx, c.UserLogin.ID, r.MessageID)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).
			Msg("googlechat: failed to look up reacted-to message for its topic id, falling back to the reaction's own message id")
		return string(r.MessageID)
	}
	if msg == nil {
		return string(r.MessageID)
	}
	topicID, _ := threadRootTopicID(msg)
	return topicID
}
