package connector

// handlematrix.go -- Matrix -> Google Chat outbound message send
// (HandleMatrixMessage). Ports mautrix_googlechat/portal.py's
// handle_matrix_message + _handle_matrix_text (portal.py:880-931,1051-1079)
// together with maugclib/client.py's send_message (client.py:413-475).
//
// M3 Task 6 implements send_message's full `if thread_id: ... else: ...`
// branch (client.py:441-472): msg.ThreadRoot != nil (bridgev2 has already
// resolved a Matrix thread reply -- or, in a threads-only room, auto-
// converted a plain reply into one, mautrix-go bridgev2/portal.go:1259-1268
// -- into a pre-fetched *database.Message) routes to create_message with
// parent_id.topic_id set to the root's own stored topic id (sendThreadedMessage,
// below); msg.ThreadRoot == nil keeps going through create_topic
// (sendNewTopic, below), exactly matching Python's own thread_id
// computation being restricted to msg.get_thread_parent()/reply-into-thread
// detection (portal.py:891-907) -- this connector leans on bridgev2's own
// generic thread/reply resolution (roomFeatures' Thread/Reply capabilities,
// capabilities.go) to build that pre-resolved ThreadRoot rather than
// re-implementing portal.py:886-907's DBMessage lookups itself.
//
// M3 Task 7 implements reply_to_wrapped/message_info.reply_to (the other
// half of client.py:423-438/453-456,467-469 the above paragraph doesn't
// cover): msg.ReplyTo != nil (bridgev2's own pre-resolved quote-reply
// target, mautrix-go bridgev2/portal.go:1248-1273) builds a SendReplyTarget
// via buildReplyTarget, below, composing independently of ThreadRoot -- a
// reply can also be posted into a thread (both set at once).
//
// Field-by-field fidelity notes for each RPC's request live on sendNewTopic,
// sendThreadedMessage, and buildReplyTarget's own doc comments, below.
//
// Known gap, tracked rather than fixed here (gchat-port-auditor, M2 Task 5
// review): portal.py's _handle_matrix_text also unconditionally calls
// `sender.client.mark_typing(self.gcid, typing=False)` immediately before
// every send (portal.py:1061-1065, best-effort/log-only on failure) to
// force-clear a lingering "typing..." indicator before the message lands.
// There is no equivalent call here -- this bridge has no
// TypingHandlingNetworkAPI/HandleMatrixTyping implementation yet for it to
// piggyback on, though the RPC primitive already exists
// (gchatmeow.Client.SetTypingState, pkg/gchatmeow/api.go). Pick this up
// alongside that future typing-support task rather than as a standalone
// fix: no data loss results from its absence (P2, not required for M2's
// plain-text send correctness).
import (
	"context"
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

// HandleMatrixMessage sends a Matrix message to Google Chat, routing it to
// create_topic (a brand-new top-level message) or create_message (a reply
// into an existing topic) depending on msg.ThreadRoot -- see the file doc
// comment -- with full HTML formatting/mention conversion as of M3 Task 4.
//
// Non-text/notice message types are rejected with
// bridgev2.ErrUnsupportedMessageType, matching handle_matrix_message's final
// else branch (`raise NotImplementedError(f"Unsupported msgtype
// {message.msgtype}")`, portal.py:923-924) for every msgtype that is
// neither TEXT/NOTICE nor is_media -- media sending itself is M5's job, so
// for now media messages are rejected the same way any other unhandled
// msgtype is (bridgev2's own checkMessageContentCaps, driven by
// GetCapabilities' empty File map, already rejects media before this method
// is ever reached in practice; this check is what actually mirrors Python's
// msgtype gate for anything that does reach here, e.g. m.emote).
func (c *GChatClient) HandleMatrixMessage(ctx context.Context, msg *bridgev2.MatrixMessage) (*bridgev2.MatrixMessageResponse, error) {
	if msg.Content.MsgType != event.MsgText && msg.Content.MsgType != event.MsgNotice {
		return nil, bridgev2.ErrUnsupportedMessageType
	}

	group, err := gcid.ParsePortalID(msg.Portal.ID)
	if err != nil {
		return nil, fmt.Errorf("googlechat: %w", err)
	}

	resolve := newOutboundMentionResolver(ctx, msg.Portal)
	text, textAnnotations := c.msgConverter().FromMatrix(ctx, msg.Content, resolve)
	// mergeAnnotations(nil, textAnnotations): no media/UPLOAD_METADATA
	// annotation exists yet in M3 (M5's job) -- the nil first argument is
	// the seam M5 will fill in with the attached file's own annotation.
	// Wiring it through mergeAnnotations NOW, rather than assigning
	// textAnnotations directly, is the fix for B4 (docs/research/08d §2.4,
	// see mergeAnnotations' own doc comment): it keeps this call site
	// append-only from day one, so M5 cannot reintroduce megabridge's
	// `annotations = entities` bug by simply setting req.Annotations =
	// textAnnotations after already having populated it with a file
	// annotation. Shared by both the create_topic and create_message
	// branches below, exactly like send_message shares message_info's
	// construction between them (client.py:441-471).
	annotations := mergeAnnotations(nil, textAnnotations)
	localID := newLocalID()
	txnID := networkid.TransactionID(localID)

	if topicID, ok := threadRootTopicID(msg.ThreadRoot); ok {
		return c.sendThreadedMessage(ctx, msg, group, topicID, text, annotations, localID, txnID)
	}
	return c.sendNewTopic(ctx, msg, group, text, annotations, localID, txnID)
}

// threadRootTopicID resolves the topic id a reply must be posted into from
// bridgev2's pre-resolved ThreadRoot message, porting Python's own fallback
// at portal.py:895: `thread_id = thread_parent.gc_parent_id or
// thread_parent.gcid`. The primary source is the root message's own stored
// MessageMetadata.TopicID (dbmeta.go, stamped on every bridged message both
// directions); if that is empty -- e.g. a pre-Task-6 legacy DB row, or a
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

// buildReplyTarget builds message_info.reply_to (M3 Task 7), porting
// send_message's reply_to_wrapped construction (client.py:423-438) field by
// field:
//
//	reply_to_wrapped = SendReplyTarget(
//	    id=MessageId(
//	        parent_id=MessageParentId(
//	            topic_id=TopicId(group_id=..., topic_id=thread_id or reply_to),
//	        ),
//	        message_id=reply_to,
//	    ),
//	    create_time=reply_to_ts,
//	) if reply_to else None
//
// replyTo is msg.ReplyTo, bridgev2's own pre-resolved reply target
// (mautrix-go bridgev2/portal.go:1248-1273) -- nil (-> nil return, matching
// Python's `if reply_to else None` gate) whenever the Matrix event carried
// no (non-fallback) m.in_reply_to relation.
//
// threadTopicID is the SAME "thread_id" sendNewTopic/sendThreadedMessage
// compute for routing: "" from sendNewTopic (no thread), or the topic being
// posted into from sendThreadedMessage. It drives the reply target's own
// nested topic_id via exactly Python's "thread_id or reply_to" fallback:
// truthy threadTopicID (we ARE posting into a thread) wins outright; empty
// falls back to the reply target's own message id. That fallback is only
// ever exercised in the no-thread case, and is correct there: portal.py's
// own upstream thread_id/reply_to selection (portal.py:896-900, `elif
// reply_to.gc_parent_id != reply_to.gcid: thread_id = reply_to.gc_parent_id;
// reply_to = None`) guarantees a reply_to that survives to this point with
// no thread_id is always the head of its own topic (message_id ==
// topic_id) -- this connector doesn't need to replicate that upstream
// selection itself (see threadRootTopicID's own doc comment on leaning on
// bridgev2's generic thread/reply resolution instead), because bridgev2
// hands ReplyTo/ThreadRoot through independently rather than pre-clearing
// ReplyTo the way portal.py's Python-side elif does -- see
// TestHandleMatrixMessageReplyAndThreadBothSet's doc comment for the
// resulting "both set at once" case this connector must (and does) still
// handle correctly.
//
// create_time is the target's own stored µs create_time
// (MessageMetadata.TimestampMicro, replyTo.Metadata -- stamped on every
// bridged message, both directions, since M2/M3 Task 6; see
// MessageMetadata's own doc comment on why it exists). Python's
// reply_to_ts is always available (DBMessage.timestamp is a NOT NULL
// column, mautrix_googlechat/db/message.py:40) so client.py never has a
// missing-timestamp case to handle; this Go port defensively covers the
// scenarios Python's schema rules out (a legacy pre-Task-7 DB row with no
// TimestampMicro stored, or a Metadata value of an unexpected type) by
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

// sendNewTopic issues create_topic, matching send_message's else branch
// (client.py:459-472) field-by-field:
//
//   - request_header is deliberately NOT set on the request built here:
//     every gchatmeow.Client RPC wrapper stamps it itself
//     (pkg/gchatmeow/api.go's newRequestHeader, invoked from CreateTopic)
//     exactly like client.py stamps self.gc_request_header
//     (client.py:103-109, WEB client_type + client_version 2440378181258 +
//     spam_room_invites FULLY_SUPPORTED) on every request it builds.
//     sync.go's PaginatedWorldRequest follows the same "connector builds
//     business fields only, gchatmeow owns the header" split -- see its doc
//     comment and pkg/gchatmeow/api.go's "16 /api/* RPCs" comment.
//   - group_id: parsers.group_id_from_id(conversation_id) is
//     gchatmeow.PartsToGroupID(group.ID, group.IsDM) here, fed by
//     gcid.ParsePortalID(msg.Portal.ID) (the "dm:"/"space:" prefix half of
//     that same Python function lives in pkg/gcid, per ids.go's doc
//     comment).
//   - local_id: Python generates one random 64-bit id per send
//     (`local_id = f"mautrix-googlechat%{random.randint(0,
//     0xffffffffffffffff)}"`, portal.py:908, BEFORE dispatching to
//     _handle_matrix_text/_handle_matrix_media) and threads it through
//     self._local_dedup so the later inbound echo of this exact message can
//     be recognized and dropped before it round-trips back to Matrix as a
//     duplicate (portal.py:909,931, and the check at portal.py:1341). M2
//     Task 5 generated and sent that token (newLocalID, same prefix and
//     64-bit random range as Python's random.randint(0,
//     0xffffffffffffffff)); Task 6 wired it into bridgev2's own
//     pending-transaction mechanism (msg.AddPendingToIgnore, via the
//     addPendingToIgnoreFn seam, client.go) as the Go
//     equivalent of self._local_dedup, registered before send() is called,
//     and matched against the echo's own local_id via
//     queueMessagePosted's TransactionID field (events.go) --
//     RemoteMessageWithTransactionID.GetTransactionID(), which bridgev2's
//     checkPendingMessage (portal.go) compares against the pending table.
//   - text_body + annotations: msgConverter().FromMatrix
//     (pkg/msgconv/from-matrix.go) delegates to matrixfmt.Parse (M3
//     Task 2), the full fmt.matrix_to_googlechat (portal.py:1059) --
//     text_body is the annotation-stripped text, annotations the
//     HTML-derived formatting/mention list; computed once by the caller
//     (HandleMatrixMessage) and shared with sendThreadedMessage.
//   - history_v2=true is sent unconditionally, matching send_message's own
//     unconditional value (client.py:465) -- not gated on any config or
//     portal state. CreateMessageRequest has no history_v2 field at all
//     (proto field 8 belongs only to CreateTopicRequest), matching
//     send_message never setting it on the `if thread_id:` branch either.
//   - message_info.accept_format_annotations=true is likewise unconditional
//     (client.py:467-469, shared by both the create_topic and
//     create_message branches, regardless of whether annotations is
//     empty -- grep-verified: send_message never gates this on
//     `len(annotations)`) -- so it stays unconditional here too, matching
//     Python exactly rather than gating it on len(annotations) > 0.
//     message_info.reply_to (M3 Task 7): buildReplyTarget(ctx, group,
//     msg.ReplyTo, "") -- nil (matching send_message's own reply_to=None ->
//     reply_to_wrapped=nil, client.py:423-437) when msg.ReplyTo is nil, else
//     a SendReplyTarget built from it. See buildReplyTarget's own doc
//     comment for the full field-by-field port, including the "" passed
//     here for its threadTopicID parameter (no thread on this path).
//   - retention_settings is left unset: it is never set by send_message at
//     all (client.py:460-471 never mentions it).
//   - topic_and_message_id is never set by send_message either (proto field
//     7, unused by this call site in Python) and is likewise left unset
//     here.
//
// Known gap, tracked rather than fixed here (gchat-port-auditor, M2 Task 5
// review): portal.py's _handle_matrix_text also unconditionally calls
// `sender.client.mark_typing(self.gcid, typing=False)` immediately before
// every send (portal.py:1061-1065, best-effort/log-only on failure) to
// force-clear a lingering "typing..." indicator before the message lands.
// There is no equivalent call here -- this bridge has no
// TypingHandlingNetworkAPI/HandleMatrixTyping implementation yet for it to
// piggyback on, though the RPC primitive already exists
// (gchatmeow.Client.SetTypingState, pkg/gchatmeow/api.go). Pick this up
// alongside that future typing-support task rather than as a standalone
// fix: no data loss results from its absence (P2, not required for M2's
// plain-text send correctness).
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
			// M3 Task 7: threadTopicID is "" here (no thread) -- see
			// buildReplyTarget's own doc comment for the "thread_id or
			// reply_to" fallback this drives.
			ReplyTo: buildReplyTarget(ctx, group, msg.ReplyTo, ""),
		},
	}

	// Register localID as a pending-to-ignore transaction BEFORE issuing the
	// RPC -- see addPendingToIgnoreFn's doc comment (client.go) for why the
	// ordering matters (Task 6: closes the race window the megabridge port
	// left open, docs/research/08b row 61). If the echo somehow reaches
	// queueMessagePosted (events.go) before send() below even returns, it is
	// already covered.
	c.addPendingToIgnore(msg, txnID)

	resp, err := send(ctx, req)
	if err != nil {
		// Undo the registration above via bridgev2's own purpose-built hook
		// (MatrixMessage.RemovePending, "should only be called if sending
		// the message fails" per its doc comment). Python's own local_id
		// leaks forever in self._local_dedup on this path (portal.py:925-931's
		// except branch never reaches the remove() call at portal.py:931) --
		// but that is an accidental limitation of Python's plain set(), not a
		// behavior worth reproducing: bridgev2 hands connectors exactly the
		// cleanup call Python never had, and both bridges are equally
		// long-running daemons, so leaving this unpaired would be unbounded
		// growth in outgoingMessages across every failed send for the life
		// of the process (gchat-port-auditor, Task 6 review).
		c.removePending(msg, txnID)
		return nil, fmt.Errorf("googlechat: create_topic failed: %w", err)
	}

	// _get_send_response's CreateTopicResponse arm (portal.py:1047-1048):
	// gcid=resp.topic.id.topic_id, timestamp=resp.topic.create_time_usec.
	// The new topic's id also becomes MessageMetadata.TopicID (M3 Task 6):
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
		// RemovePending mirrors portal.py:931's
		// `self._local_dedup.remove(local_id)` on the success path: once this
		// response is saved, the pending entry registered above is no longer
		// needed.
		RemovePending: txnID,
	}, nil
}

// sendThreadedMessage issues create_message, matching send_message's
// `if thread_id:` branch (client.py:441-458): a reply posted into topicID,
// an EXISTING topic (never a brand new one -- see threadRootTopicID). Field
// handling mirrors sendNewTopic's doc comment above except where noted:
//
//   - parent_id.topic_id.{group_id,topic_id}: the thread being replied
//     into, i.e. exactly what send_message builds at client.py:444-448
//     (`parent_id=MessageParentId(topic_id=TopicId(group_id=...,
//     topic_id=thread_id))`).
//   - message_id (proto field 6, CreateMessageRequest only) is never set by
//     send_message's threaded branch (client.py:442-457 never mentions it)
//     and is likewise left unset here.
//   - message_info.reply_to (M3 Task 7): buildReplyTarget(ctx, group,
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
			// M3 Task 7: threadTopicID is topicID here (the thread this
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

	// _get_send_response's CreateMessageResponse arm (portal.py:1049):
	// gcid=resp.message.id.message_id, timestamp=resp.message.create_time --
	// note this is the NEW reply's own message id (never equal to topicID
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

// newLocalID generates one send's local_id/dedup token, matching
// portal.py's `f"mautrix-googlechat%{random.randint(0,
// 0xffffffffffffffff)}"` (portal.py:908): the literal prefix
// "mautrix-googlechat%" followed by a uniformly random 64-bit integer.
// math/rand (not crypto/rand) matches Python's own use of the non-secure
// `random` module here -- this token only needs to be practically unique
// for in-flight dedup, not unpredictable.
func newLocalID() string {
	return fmt.Sprintf("mautrix-googlechat%%%d", rand.Uint64())
}

// mergeAnnotations combines a message's already-decided annotations
// (existing -- currently always nil in M3, since there is no media upload
// path yet; M5 will pass the attached file's own UPLOAD_METADATA annotation
// here) with the caption/body text's own formatting annotations (text,
// from matrixfmt.Parse via FromMatrix), always by APPENDING text after
// existing -- never by replacing existing outright.
//
// This is the fix for B4 (docs/research/08d-megabridge-msgconv.md §2.4):
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
