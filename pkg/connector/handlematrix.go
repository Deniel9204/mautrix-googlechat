package connector

// handlematrix.go -- Matrix -> Google Chat outbound message send
// (HandleMatrixMessage).
//
// The full thread-vs-no-thread branch: msg.ThreadRoot
// != nil (bridgev2 has already resolved a Matrix thread reply -- or, in a
// threads-only room, auto-converted a plain reply into one, mautrix-go
// bridgev2/portal.go:1259-1268 -- into a pre-fetched *database.Message)
// routes to create_message with parent_id.topic_id set to the root's own
// stored topic id (sendThreadedMessage, below); msg.ThreadRoot == nil keeps
// going through create_topic (sendNewTopic, below). This connector leans on
// bridgev2's own generic thread/reply resolution (roomFeatures' Thread/Reply
// capabilities, capabilities.go) to build that pre-resolved ThreadRoot
// rather than doing the message-row lookups itself.
//
// message_info.reply_to is the quote-reply half the above
// paragraph doesn't cover: msg.ReplyTo != nil (bridgev2's own pre-resolved
// quote-reply target, mautrix-go bridgev2/portal.go:1248-1273) builds a
// SendReplyTarget via buildReplyTarget, below, composing independently of
// ThreadRoot -- a reply can also be posted into a thread (both set at once).
//
// Field-by-field fidelity notes for each RPC's request live on sendNewTopic,
// sendThreadedMessage, and buildReplyTarget's own doc comments, below.
//
// Known gap, tracked rather than fixed here: a lingering "typing..."
// indicator is not force-cleared before each send (a mark_typing
// typing=False call, best-effort/log-only on failure, to clear it before the
// message lands). There is no equivalent call here -- this bridge has no
// TypingHandlingNetworkAPI/HandleMatrixTyping implementation yet for it to
// piggyback on, though the RPC primitive already exists
// (gchatmeow.Client.SetTypingState, pkg/gchatmeow/api.go). Pick this up
// alongside that future typing-support task rather than as a standalone fix:
// no data loss results from its absence (P2, not required for plain-text
// send correctness).
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

// errOutboundMediaDisabled is returned by HandleMatrixMessage's media branch
// when Config.DisableOutboundMedia is set (config.go): a clean, explicit
// message-send-status failure naming the upstream blocker (issue #114,
// https://github.com/mautrix/googlechat/issues/114 -- Google's /uploads
// endpoint has reportedly returned HTTP 500 for every upload since ~Feb
// 2026), rather than letting an operator who already knows uploads are
// broken for their account hit a live download+upload attempt (and its own
// less-specific error, buildUploadAnnotation, media.go) on every single
// media send. WrapErrorInStatus + WithErrorReason(MessageStatusUnsupported)
// mirrors this package's other static rejection errors (compare
// bridgev2.ErrUnsupportedMessageType/ErrCaptionsNotAllowed, mautrix-go
// bridgev2/errors.go) rather than a bare fmt.Errorf, so bridgev2 reports it
// as an "unsupported" failure (not a generic/retriable one) in the message
// status event shown to the user.
var errOutboundMediaDisabled = bridgev2.WrapErrorInStatus(errors.New("outbound media is disabled")).
	WithMessage("Sending media to Google Chat is disabled on this bridge (unsupported -- see upstream issue https://github.com/mautrix/googlechat/issues/114)").
	WithIsCertain(true).
	WithSendNotice(true).
	WithErrorReason(event.MessageStatusUnsupported)

// HandleMatrixMessage sends a Matrix message to Google Chat, routing it to
// create_topic (a brand-new top-level message) or create_message (a reply
// into an existing topic) depending on msg.ThreadRoot -- see the file doc
// comment -- with full HTML formatting/mention conversion
// and outbound media (m.image/m.file/m.video/m.audio).
//
// Every other message type is rejected with bridgev2.ErrUnsupportedMessageType
// for every msgtype that is neither TEXT/NOTICE nor is_media. In practice,
// bridgev2's own checkMessageContentCaps (driven by GetCapabilities' File
// map, capabilities.go) already rejects anything outside
// image/video/audio/file before this method is ever reached for a media
// msgtype -- this check is what actually enforces the msgtype gate for the
// types that DO reach here (e.g. m.emote, m.location).
//
// Media branch: isOutboundMediaMsgType gates on exactly the four
// msgtypes capabilities.go's gchatFile map advertises.
// Config.DisableOutboundMedia short-circuits with errOutboundMediaDisabled
// before any network I/O -- see its own doc comment for why (issue #114).
// Otherwise
// buildUploadAnnotation (media.go) downloads the Matrix file (decrypting it
// if encrypted), uploads it to Google Chat, and returns an
// UPLOAD_METADATA/RENDER annotation; a failure at either step (the #114
// upload 500 included) returns a clean, wrapped error here with NO request
// ever issued -- never a silent drop, and never a text-only fallback that
// would lose the file. hasOutboundCaption then decides whether msg.Content
// also carries a genuine caption (as opposed to Body merely repeating the
// file's own name): only then is msg.Content run through the SAME
// FromMatrix call a text message uses, so a media message's caption gets
// the identical formatting/mention treatment as a plain text body. The
// resulting file and caption annotations are combined via mergeAnnotations
// below exactly like text-only messages are -- never by outright
// replacement -- so a formatted caption can never clobber the file
// annotation (the B4 fix this file has guarded against append-only from the
// start, before any real UPLOAD_METADATA annotation existed to lose).
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
	// mergeAnnotations combines the media branch's own file annotation (nil
	// for a text/notice message) with the caption/body text's own
	// formatting annotations, always by APPENDING rather than replacing --
	// the fix for B4 (see mergeAnnotations' own doc comment). Shared by both
	// the create_topic and create_message branches below.
	annotations := mergeAnnotations(fileAnnotations, textAnnotations)
	localID := newLocalID()
	txnID := networkid.TransactionID(localID)

	if topicID, ok := threadRootTopicID(msg.ThreadRoot); ok {
		return c.sendThreadedMessage(ctx, msg, group, topicID, text, annotations, localID, txnID)
	}
	return c.sendNewTopic(ctx, msg, group, text, annotations, localID, txnID)
}

// threadRootTopicID resolves the topic id a reply must be posted into from
// bridgev2's pre-resolved ThreadRoot message, applying a `topic id or message
// id` fallback. The primary source is the root message's own stored
// MessageMetadata.TopicID (dbmeta.go, stamped on every bridged message both
// directions); if that is empty -- e.g. a legacy DB row, or a
// Metadata value of an unexpected type -- this falls back to the root
// message's own id, which is correct regardless: message_id == topic_id for
// any head-of-topic message, so a root whose OWN id IS the topic id (the
// common case) still routes correctly even with no TopicID recorded at all.
// ok is false only when msg.ThreadRoot itself is nil (no thread -> route to
// create_topic instead, HandleMatrixMessage above).
func threadRootTopicID(root *database.Message) (string, bool) {
	if root == nil {
		return "", false
	}
	if meta, ok := root.Metadata.(*MessageMetadata); ok && meta != nil && meta.TopicID != "" {
		return meta.TopicID, true
	}
	return string(root.ID), true
}

// buildReplyTarget builds message_info.reply_to, the
// SendReplyTarget proto:
//
//	SendReplyTarget{
//	    Id: MessageId{
//	        ParentId: MessageParentId{
//	            TopicId: TopicId{GroupId: ..., TopicId: thread_id or reply_to},
//	        },
//	        MessageId: reply_to,
//	    },
//	    CreateTime: reply_to_ts,
//	}   // built only when there is a reply target
//
// replyTo is msg.ReplyTo, bridgev2's own pre-resolved reply target
// (mautrix-go bridgev2/portal.go:1248-1273) -- nil (-> nil return) whenever
// the Matrix event carried no (non-fallback) m.in_reply_to relation.
//
// threadTopicID is the SAME "thread_id" sendNewTopic/sendThreadedMessage
// compute for routing: "" from sendNewTopic (no thread), or the topic being
// posted into from sendThreadedMessage. It drives the reply target's own
// nested topic_id via a "thread_id or reply_to" fallback: truthy
// threadTopicID (we ARE posting into a thread) wins outright; empty falls
// back to the reply target's own message id. That fallback is only ever
// exercised in the no-thread case, and is correct there: a reply target that
// survives to this point with no thread id is always the head of its own
// topic (message_id == topic_id). This connector leans on bridgev2's generic
// thread/reply resolution (see threadRootTopicID's own doc comment) and takes
// ReplyTo/ThreadRoot through independently rather than pre-clearing ReplyTo --
// see TestHandleMatrixMessageReplyAndThreadBothSet's doc comment for the
// resulting "both set at once" case this connector must (and does) still
// handle correctly.
//
// create_time is the target's own stored µs create_time
// (MessageMetadata.TimestampMicro, replyTo.Metadata -- stamped on every
// bridged message, both directions; see
// MessageMetadata's own doc comment on why it exists). A stored create_time
// is normally always available, but this Go port defensively covers the
// scenarios that would otherwise rule it out (a legacy DB row with
// no TimestampMicro stored, or a Metadata value of an unexpected type) by
// logging a warning and returning nil -- sending the message WITHOUT any
// reply target, rather than risking a malformed SendReplyTarget (id set,
// create_time missing/0) getting the whole create_topic/create_message call
// rejected server-side. The message itself still sends normally either way
// (as a plain message or thread post); only the quote-reply decoration is
// lost.
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

// sendNewTopic issues create_topic, building the request field-by-field:
//
//   - request_header is deliberately NOT set on the request built here:
//     every gchatmeow.Client RPC wrapper stamps it itself
//     (pkg/gchatmeow/api.go's newRequestHeader, invoked from CreateTopic) --
//     WEB client_type + client_version 2440378181258 + spam_room_invites
//     FULLY_SUPPORTED on every request. sync.go's PaginatedWorldRequest
//     follows the same "connector builds business fields only, gchatmeow owns
//     the header" split -- see its doc comment and pkg/gchatmeow/api.go's
//     "16 /api/* RPCs" comment.
//   - group_id: gchatmeow.PartsToGroupID(group.ID, group.IsDM), fed by
//     gcid.ParsePortalID(msg.Portal.ID) (the "dm:"/"space:" prefix half of
//     that derivation lives in pkg/gcid, per ids.go's doc comment).
//   - local_id: one random 64-bit id is generated per send (the literal
//     prefix "mautrix-googlechat%" followed by a random 64-bit integer,
//     newLocalID) and threaded through dedup so the later inbound echo of
//     this exact message can be recognized and dropped before it round-trips
//     back to Matrix as a duplicate. That token is generated and sent, and
//     wired into bridgev2's own pending-transaction mechanism
//     (msg.AddPendingToIgnore, via the addPendingToIgnoreFn seam, client.go),
//     registered before send() is called, and matched against the echo's own
//     local_id via queueMessagePosted's TransactionID field (events.go) --
//     RemoteMessageWithTransactionID.GetTransactionID(), which bridgev2's
//     checkPendingMessage (portal.go) compares against the pending table.
//   - text_body + annotations: msgConverter().FromMatrix
//     (pkg/msgconv/from-matrix.go) delegates to matrixfmt.Parse
//     -- text_body is the annotation-stripped text, annotations the
//     HTML-derived formatting/mention list; computed once by the caller
//     (HandleMatrixMessage) and shared with sendThreadedMessage.
//   - history_v2=true is sent unconditionally -- not gated on any config or
//     portal state. CreateMessageRequest has no history_v2 field at all
//     (proto field 8 belongs only to CreateTopicRequest), so it is never set
//     on the threaded branch either.
//   - message_info.accept_format_annotations=true is likewise unconditional
//     (required for outgoing formatting to render), shared by both the
//     create_topic and create_message branches, regardless of whether
//     annotations is empty -- never gated on len(annotations).
//     message_info.reply_to: buildReplyTarget(ctx, group,
//     msg.ReplyTo, "") -- nil when msg.ReplyTo is nil, else a SendReplyTarget
//     built from it. See buildReplyTarget's own doc comment for the full
//     field-by-field construction, including the "" passed here for its
//     threadTopicID parameter (no thread on this path).
//   - retention_settings is left unset: it is never set on this call.
//   - topic_and_message_id is never set (proto field 7, unused by this call
//     site) and is likewise left unset here.
//
// Known gap, tracked rather than fixed here: a lingering "typing..."
// indicator is not force-cleared before each send (a mark_typing
// typing=False call, best-effort/log-only on failure, to clear it before the
// message lands). There is no equivalent call here -- this bridge has no
// TypingHandlingNetworkAPI/HandleMatrixTyping implementation yet for it to
// piggyback on, though the RPC primitive already exists
// (gchatmeow.Client.SetTypingState, pkg/gchatmeow/api.go). Pick this up
// alongside that future typing-support task rather than as a standalone fix:
// no data loss results from its absence (P2, not required for plain-text
// send correctness).
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
			// threadTopicID is "" here (no thread) -- see
			// buildReplyTarget's own doc comment for the "thread_id or
			// reply_to" fallback this drives.
			ReplyTo: buildReplyTarget(ctx, group, msg.ReplyTo, ""),
		},
	}

	// Register localID as a pending-to-ignore transaction BEFORE issuing the
	// RPC -- see addPendingToIgnoreFn's doc comment (client.go) for why the
	// ordering matters (closes the race window the megabridge port
	// left open). If the echo somehow reaches
	// queueMessagePosted (events.go) before send() below even returns, it is
	// already covered.
	c.addPendingToIgnore(msg, txnID)

	resp, err := send(ctx, req)
	if err != nil {
		// Undo the registration above via bridgev2's own purpose-built hook
		// (MatrixMessage.RemovePending, "should only be called if sending
		// the message fails" per its doc comment). Leaving this unpaired
		// would be unbounded growth in outgoingMessages across every failed
		// send for the life of the process.
		c.removePending(msg, txnID)
		return nil, fmt.Errorf("googlechat: create_topic failed: %w", err)
	}

	// CreateTopicResponse: gcid=resp.topic.id.topic_id,
	// timestamp=resp.topic.create_time_usec.
	// The new topic's id also becomes MessageMetadata.TopicID:
	// message_id == topic_id for the head of a brand new topic, so this
	// message IS its own topic -- storing it here is what lets a LATER
	// Matrix thread reply targeting this exact message resolve the right
	// parent_id.topic_id via threadRootTopicID above.
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
		// RemovePending on the success path: once this response is saved, the
		// pending entry registered above is no longer needed.
		RemovePending: txnID,
	}, nil
}

// sendThreadedMessage issues create_message for a reply posted into topicID,
// an EXISTING topic (never a brand new one -- see threadRootTopicID). Field
// handling mirrors sendNewTopic's doc comment above except where noted:
//
//   - parent_id.topic_id.{group_id,topic_id}: the thread being replied into
//     (parent_id=MessageParentId(topic_id=TopicId(group_id=...,
//     topic_id=thread_id))).
//   - message_id (proto field 6, CreateMessageRequest only) is never set on
//     the threaded branch and is likewise left unset here.
//   - message_info.reply_to: buildReplyTarget(ctx, group,
//     msg.ReplyTo, topicID) -- same as sendNewTopic, except topicID (the
//     thread this message is being posted into) is passed as
//     threadTopicID here rather than "", driving the "thread_id or
//     reply_to" fallback's thread_id-truthy arm (see buildReplyTarget's own
//     doc comment).
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
			// threadTopicID is topicID here (the thread this
			// message is being posted into) -- see buildReplyTarget's own
			// doc comment for the "thread_id or reply_to" fallback this
			// drives, and TestHandleMatrixMessageReplyAndThreadBothSet for
			// the "reply also in a thread" composition case.
			ReplyTo: buildReplyTarget(ctx, group, msg.ReplyTo, topicID),
		},
	}

	// Same ordering discipline as sendNewTopic: register before issuing the
	// RPC (see its doc comment above).
	c.addPendingToIgnore(msg, txnID)

	resp, err := send(ctx, req)
	if err != nil {
		// Same cleanup discipline as sendNewTopic: undo the registration on
		// failure (see its doc comment above).
		c.removePending(msg, txnID)
		return nil, fmt.Errorf("googlechat: create_message failed: %w", err)
	}

	// CreateMessageResponse: gcid=resp.message.id.message_id,
	// timestamp=resp.message.create_time -- note this is the NEW reply's own
	// message id (never equal to topicID
	// here, since topicID names an EXISTING topic this reply was posted
	// into), unlike sendNewTopic's self-referencing case above.
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

// newLocalID generates one send's local_id/dedup token: the literal prefix
// "mautrix-googlechat%" followed by a uniformly random 64-bit integer.
// math/rand (not crypto/rand) is deliberate -- this token only needs to be
// practically unique for in-flight dedup, not unpredictable.
func newLocalID() string {
	return fmt.Sprintf("mautrix-googlechat%%%d", rand.Uint64())
}

// mergeAnnotations combines a message's already-decided annotations
// (existing -- nil for a text/notice message, or a media message's own
// UPLOAD_METADATA annotation, see HandleMatrixMessage's
// media branch) with the caption/body text's own formatting annotations
// (text, from matrixfmt.Parse via FromMatrix), always by APPENDING text
// after existing -- never by replacing existing outright.
//
// This is the fix for B4:
// megabridge's handlematrix.go built `annotations = []*proto.Annotation{
// {Type: UPLOAD_METADATA, ...}}` for a media message, then unconditionally
// ran the caption through its formatter and did `if entities != nil {
// annotations = entities }` -- REPLACING the UPLOAD_METADATA annotation
// (and silently dropping the attached file from the wire request) whenever
// the caption had ANY formatting at all; a plain-text caption only survived
// by accident, because entities was nil in that case. The correct
// combination is additive: keep the file annotation, and append the
// caption's own formatting annotations after it -- exactly what a media
// message needs to keep BOTH the attached file AND a formatted caption.
//
// Both nil-slice fast paths return the other argument's slice unchanged
// (no allocation), preserving the existing "plain-body outbound -> nil
// annotations, not an empty-but-non-nil slice" contract when text is empty
// (see matrixfmt.Parse's own doc comment).
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
