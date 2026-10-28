package connector

import (
	"context"
	"errors"
	"fmt"
	"math/rand"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// errOutboundMediaDisabled is returned for media sends when Config.DisableOutboundMedia is set
// (Google's /uploads endpoint returns HTTP 500 for every upload).
var errOutboundMediaDisabled = bridgev2.WrapErrorInStatus(errors.New("outbound media is disabled")).
	WithMessage("Sending media to Google Chat is disabled on this bridge (unsupported -- see upstream issue https://github.com/mautrix/googlechat/issues/114)").
	WithIsCertain(true).
	WithSendNotice(true).
	WithErrorReason(event.MessageStatusUnsupported)

// HandleMatrixMessage sends a Matrix message to Google Chat via create_topic, or
// create_message when msg.ThreadRoot is set.
func (c *GChatClient) HandleMatrixMessage(ctx context.Context, msg *bridgev2.MatrixMessage) (*bridgev2.MatrixMessageResponse, error) {
	isMedia := isOutboundMediaMsgType(msg.Content.MsgType)
	if msg.Content.MsgType != event.MsgText && msg.Content.MsgType != event.MsgNotice && !isMedia {
		return nil, bridgev2.ErrUnsupportedMessageType
	}

	group, err := gcid.ParsePortalID(msg.Portal.ID)
	if err != nil {
		return nil, fmt.Errorf("googlechat: %w", err)
	}

	var fileAnnotations []*pb.Annotation
	hasCaption := true
	if isMedia {
		if c.Main != nil && c.Main.Config.DisableOutboundMedia {
			return nil, errOutboundMediaDisabled
		}
		ann, err := c.buildUploadAnnotation(ctx, msg, group)
		if err != nil {
			return nil, err
		}
		fileAnnotations = []*pb.Annotation{ann}
		hasCaption = hasOutboundCaption(msg.Content)
	}

	var text string
	var textAnnotations []*pb.Annotation
	if hasCaption {
		resolve := newOutboundMentionResolver(ctx, msg.Portal)
		text, textAnnotations = c.msgConverter().FromMatrix(ctx, msg.Content, resolve)
	}
	// Append, never replace: a formatted caption must not clobber the file annotation.
	annotations := mergeAnnotations(fileAnnotations, textAnnotations)
	localID := newLocalID()
	txnID := networkid.TransactionID(localID)

	if topicID, ok := threadRootTopicID(msg.ThreadRoot); ok {
		return c.sendThreadedMessage(ctx, msg, group, topicID, text, annotations, localID, txnID)
	}
	return c.sendNewTopic(ctx, msg, group, text, annotations, localID, txnID)
}

// threadRootTopicID returns the root's stored TopicID, falling back to its message id
// (message_id == topic_id for a head-of-topic message).
func threadRootTopicID(root *database.Message) (string, bool) {
	if root == nil {
		return "", false
	}
	if meta, ok := root.Metadata.(*MessageMetadata); ok && meta != nil && meta.TopicID != "" {
		return meta.TopicID, true
	}
	return string(root.ID), true
}

// buildReplyTarget builds message_info.reply_to; topic_id is threadTopicID or else the
// reply target's own id. Returns nil when no stored create_time exists, since a
// SendReplyTarget without create_time gets the whole send rejected.
func buildReplyTarget(ctx context.Context, group gcid.GroupID, replyTo *database.Message, threadTopicID string) *pb.SendReplyTarget {
	if replyTo == nil {
		return nil
	}
	replyToID := gcid.ParseMessageID(replyTo.ID)
	meta, ok := replyTo.Metadata.(*MessageMetadata)
	if !ok || meta == nil || meta.TimestampMicro == 0 {
		zerolog.Ctx(ctx).Warn().
			Str("reply_to_id", replyToID).
			Msg("googlechat: reply target has no stored create_time, sending without a quote-reply target")
		return nil
	}
	topicID := threadTopicID
	if topicID == "" {
		topicID = replyToID
	}
	return &pb.SendReplyTarget{
		Id: &pb.MessageId{
			ParentId: &pb.MessageParentId{
				Parent: &pb.MessageParentId_TopicId{
					TopicId: &pb.TopicId{
						GroupId: gchatmeow.PartsToGroupID(group.ID, group.IsDM),
						TopicId: proto.String(topicID),
					},
				},
			},
			MessageId: proto.String(replyToID),
		},
		CreateTime: proto.Int64(meta.TimestampMicro),
	}
}

// sendNewTopic issues create_topic. request_header is stamped by gchatmeow.
// history_v2 exists only on CreateTopicRequest (field 8).
// TODO: clear a lingering typing indicator before sending (needs typing support).
func (c *GChatClient) sendNewTopic(ctx context.Context, msg *bridgev2.MatrixMessage, group gcid.GroupID, text string, annotations []*pb.Annotation, localID string, txnID networkid.TransactionID) (*bridgev2.MatrixMessageResponse, error) {
	send := c.createTopicFn
	if send == nil {
		conn := c.getConn()
		if conn == nil {
			return nil, fmt.Errorf("googlechat: not connected")
		}
		send = conn.CreateTopic
	}

	req := &pb.CreateTopicRequest{
		GroupId:     gchatmeow.PartsToGroupID(group.ID, group.IsDM),
		LocalId:     proto.String(localID),
		TextBody:    proto.String(text),
		Annotations: annotations,
		HistoryV2:   proto.Bool(true),
		MessageInfo: &pb.MessageInfo{
			AcceptFormatAnnotations: proto.Bool(true),
			// No thread on this path, so topic_id falls back to the reply target's id.
			ReplyTo: buildReplyTarget(ctx, group, msg.ReplyTo, ""),
		},
	}

	// Register before the RPC so an echo arriving before send() returns is still ignored.
	c.addPendingToIgnore(msg, txnID)

	resp, err := send(ctx, req)
	if err != nil {
		// Without this, outgoingMessages grows on every failed send.
		c.removePending(msg, txnID)
		return nil, fmt.Errorf("googlechat: create_topic failed: %w", err)
	}

	// message_id == topic_id for the head of a new topic; storing TopicID lets later
	// thread replies resolve it.
	topic := resp.GetTopic()
	createTimeUsec := topic.GetCreateTimeUsec()
	topicID := topic.GetId().GetTopicId()
	return &bridgev2.MatrixMessageResponse{
		DB: &database.Message{
			ID:        gcid.MakeMessageID(topicID),
			SenderID:  c.ownUserID(),
			Timestamp: gchatmeow.MicrosToTime(createTimeUsec),
			Metadata:  &MessageMetadata{TimestampMicro: createTimeUsec, TopicID: topicID},
		},
		// Success: the pending entry is no longer needed.
		RemovePending: txnID,
	}, nil
}

// sendThreadedMessage issues create_message for a reply posted into the existing topic topicID.
func (c *GChatClient) sendThreadedMessage(ctx context.Context, msg *bridgev2.MatrixMessage, group gcid.GroupID, topicID, text string, annotations []*pb.Annotation, localID string, txnID networkid.TransactionID) (*bridgev2.MatrixMessageResponse, error) {
	send := c.createMessageFn
	if send == nil {
		conn := c.getConn()
		if conn == nil {
			return nil, fmt.Errorf("googlechat: not connected")
		}
		send = conn.CreateMessage
	}

	req := &pb.CreateMessageRequest{
		ParentId: &pb.MessageParentId{
			Parent: &pb.MessageParentId_TopicId{
				TopicId: &pb.TopicId{
					GroupId: gchatmeow.PartsToGroupID(group.ID, group.IsDM),
					TopicId: proto.String(topicID),
				},
			},
		},
		LocalId:     proto.String(localID),
		TextBody:    proto.String(text),
		Annotations: annotations,
		MessageInfo: &pb.MessageInfo{
			AcceptFormatAnnotations: proto.Bool(true),
			// A reply can also be posted into a thread (both set at once).
			ReplyTo: buildReplyTarget(ctx, group, msg.ReplyTo, topicID),
		},
	}

	c.addPendingToIgnore(msg, txnID)

	resp, err := send(ctx, req)
	if err != nil {
		// Undo the pending registration on failure.
		c.removePending(msg, txnID)
		return nil, fmt.Errorf("googlechat: create_message failed: %w", err)
	}

	message := resp.GetMessage()
	createTime := message.GetCreateTime()
	return &bridgev2.MatrixMessageResponse{
		DB: &database.Message{
			ID:        gcid.MakeMessageID(message.GetId().GetMessageId()),
			SenderID:  c.ownUserID(),
			Timestamp: gchatmeow.MicrosToTime(createTime),
			Metadata:  &MessageMetadata{TimestampMicro: createTime, TopicID: topicID},
		},
		RemovePending: txnID,
	}, nil
}

// newLocalID generates a local_id dedup token; math/rand is fine, it only needs to be unique.
func newLocalID() string {
	return fmt.Sprintf("mautrix-googlechat%%%d", rand.Uint64())
}

// mergeAnnotations appends text annotations after existing ones, never replacing them,
// so a formatted caption cannot drop a media message's UPLOAD_METADATA annotation.
func mergeAnnotations(existing, text []*pb.Annotation) []*pb.Annotation {
	if len(text) == 0 {
		return existing
	}
	if len(existing) == 0 {
		return text
	}
	merged := make([]*pb.Annotation, 0, len(existing)+len(text))
	merged = append(merged, existing...)
	merged = append(merged, text...)
	return merged
}
