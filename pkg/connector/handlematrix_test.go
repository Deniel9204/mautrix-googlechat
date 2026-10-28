package connector

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv/gchatfmt"
)

func spacePortal(id string) *bridgev2.Portal {
	return &bridgev2.Portal{Portal: &database.Portal{
		PortalKey: networkid.PortalKey{ID: gcid.MakePortalID(gcid.GroupID{ID: id, IsDM: false})},
	}}
}

func dmPortal(id string) *bridgev2.Portal {
	return &bridgev2.Portal{Portal: &database.Portal{
		PortalKey: networkid.PortalKey{ID: gcid.MakePortalID(gcid.GroupID{ID: id, IsDM: true})},
	}}
}

func textMatrixMessage(portal *bridgev2.Portal, body string) *bridgev2.MatrixMessage {
	return &bridgev2.MatrixMessage{
		MatrixEventBase: bridgev2.MatrixEventBase[*event.MessageEventContent]{
			Portal:  portal,
			Content: &event.MessageEventContent{MsgType: event.MsgText, Body: body},
		},
	}
}

func createTopicResponse(topicID string, createTimeUsec int64) *pb.CreateTopicResponse {
	return &pb.CreateTopicResponse{
		Topic: &pb.Topic{
			Id:             &pb.TopicId{TopicId: proto.String(topicID)},
			CreateTimeUsec: proto.Int64(createTimeUsec),
		},
	}
}

func createMessageResponse(messageID string, createTime int64) *pb.CreateMessageResponse {
	return &pb.CreateMessageResponse{
		Message: &pb.Message{
			Id:         &pb.MessageId{MessageId: proto.String(messageID)},
			CreateTime: proto.Int64(createTime),
		},
	}
}

// threadedMatrixMessage builds a message whose ThreadRoot is pre-resolved with the given topic id.
func threadedMatrixMessage(portal *bridgev2.Portal, body, rootID, rootTopicID string) *bridgev2.MatrixMessage {
	msg := textMatrixMessage(portal, body)
	msg.ThreadRoot = &database.Message{
		ID:       gcid.MakeMessageID(rootID),
		Metadata: &MessageMetadata{TopicID: rootTopicID},
	}
	return msg
}

// replyMatrixMessage builds a message whose ReplyTo is pre-resolved with the given create_time.
func replyMatrixMessage(portal *bridgev2.Portal, body, targetID string, targetTimestampMicro int64) *bridgev2.MatrixMessage {
	msg := textMatrixMessage(portal, body)
	msg.ReplyTo = &database.Message{
		ID:       gcid.MakeMessageID(targetID),
		Metadata: &MessageMetadata{TimestampMicro: targetTimestampMicro},
	}
	return msg
}

// noopAddPendingToIgnore replaces the real AddPendingToIgnore, which panics on a bare Portal (nil map).
func noopAddPendingToIgnore(*bridgev2.MatrixMessage, networkid.TransactionID) {}

func TestHandleMatrixMessageSpacePortalBuildsCreateTopicRequest(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.CreateTopicRequest
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createTopicFn: func(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			gotReq = req
			return createTopicResponse("topic1", 1000), nil
		},
	}

	_, err := gc.HandleMatrixMessage(context.Background(), textMatrixMessage(spacePortal("space1"), "hello world"))
	if err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}
	if gotReq == nil {
		t.Fatal("createTopicFn was not called")
	}

	if got := gotReq.GetGroupId().GetSpaceId().GetSpaceId(); got != "space1" {
		t.Errorf("GroupId.SpaceId = %q, want %q", got, "space1")
	}
	if gotReq.GetGroupId().GetDmId() != nil {
		t.Error("GroupId.DmId is set for a space portal, want unset")
	}
	if got := gotReq.GetTextBody(); got != "hello world" {
		t.Errorf("TextBody = %q, want %q", got, "hello world")
	}
	if !gotReq.GetHistoryV2() {
		t.Error("HistoryV2 = false, want true")
	}
	if !gotReq.GetMessageInfo().GetAcceptFormatAnnotations() {
		t.Error("MessageInfo.AcceptFormatAnnotations = false, want true")
	}
	if gotReq.GetMessageInfo().GetReplyTo() != nil {
		t.Error("MessageInfo.ReplyTo is set, want nil (msg.ReplyTo is nil -- not a quote-reply)")
	}
	if gotReq.GetLocalId() == "" {
		t.Error("LocalId is empty, want a generated dedup token")
	}
	if !strings.HasPrefix(gotReq.GetLocalId(), "mautrix-googlechat%") {
		t.Errorf("LocalId = %q, want prefix %q", gotReq.GetLocalId(), "mautrix-googlechat%")
	}
}

func TestHandleMatrixMessageDMPortalBuildsDmGroupID(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.CreateTopicRequest
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createTopicFn: func(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			gotReq = req
			return createTopicResponse("topic2", 2000), nil
		},
	}

	_, err := gc.HandleMatrixMessage(context.Background(), textMatrixMessage(dmPortal("dm1"), "hi"))
	if err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}
	if gotReq == nil {
		t.Fatal("createTopicFn was not called")
	}

	if got := gotReq.GetGroupId().GetDmId().GetDmId(); got != "dm1" {
		t.Errorf("GroupId.DmId = %q, want %q", got, "dm1")
	}
	if gotReq.GetGroupId().GetSpaceId() != nil {
		t.Error("GroupId.SpaceId is set for a DM portal, want unset")
	}
}

// TestHandleMatrixMessageLocalIDsAreUnique guards against reusing one local_id for every send.
func TestHandleMatrixMessageLocalIDsAreUnique(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var ids []string
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createTopicFn: func(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			ids = append(ids, req.GetLocalId())
			return createTopicResponse("t", 1), nil
		},
	}

	for i := 0; i < 5; i++ {
		if _, err := gc.HandleMatrixMessage(context.Background(), textMatrixMessage(spacePortal("space1"), "x")); err != nil {
			t.Fatalf("HandleMatrixMessage() error = %v", err)
		}
	}

	seen := map[string]bool{}
	for _, id := range ids {
		if seen[id] {
			t.Fatalf("local_id %q generated more than once across 5 sends: %v", id, ids)
		}
		seen[id] = true
	}
}

func TestHandleMatrixMessageMapsResponseToDBMessage(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createTopicFn: func(context.Context, *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			return createTopicResponse("gc-msg-id-1", 1_700_000_000_000_000), nil
		},
	}

	resp, err := gc.HandleMatrixMessage(context.Background(), textMatrixMessage(spacePortal("space1"), "hello"))
	if err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}
	if resp == nil || resp.DB == nil {
		t.Fatal("HandleMatrixMessage() response/DB is nil")
	}

	if resp.DB.ID != gcid.MakeMessageID("gc-msg-id-1") {
		t.Errorf("DB.ID = %q, want %q", resp.DB.ID, gcid.MakeMessageID("gc-msg-id-1"))
	}
	if resp.DB.SenderID != gcid.MakeUserID("112233") {
		t.Errorf("DB.SenderID = %q, want own user id %q", resp.DB.SenderID, gcid.MakeUserID("112233"))
	}
	wantTS := time.UnixMicro(1_700_000_000_000_000).UTC()
	if !resp.DB.Timestamp.Equal(wantTS) {
		t.Errorf("DB.Timestamp = %v, want %v", resp.DB.Timestamp, wantTS)
	}
	meta, ok := resp.DB.Metadata.(*MessageMetadata)
	if !ok {
		t.Fatalf("DB.Metadata type = %T, want *MessageMetadata", resp.DB.Metadata)
	}
	if meta.TimestampMicro != 1_700_000_000_000_000 {
		t.Errorf("Metadata.TimestampMicro = %d, want %d", meta.TimestampMicro, 1_700_000_000_000_000)
	}
	// message_id == topic_id for the head of a new topic.
	if meta.TopicID != "gc-msg-id-1" {
		t.Errorf("Metadata.TopicID = %q, want %q (message_id == topic_id for a new topic's head)", meta.TopicID, "gc-msg-id-1")
	}
}

func TestHandleMatrixMessageUnsupportedMsgTypeRejected(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	called := false
	gc := &GChatClient{
		UserLogin: login,
		createTopicFn: func(context.Context, *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			called = true
			return createTopicResponse("t", 1), nil
		},
	}

	msg := textMatrixMessage(spacePortal("space1"), "a location")
	msg.Content.MsgType = event.MsgLocation

	_, err := gc.HandleMatrixMessage(context.Background(), msg)
	if !errors.Is(err, bridgev2.ErrUnsupportedMessageType) {
		t.Errorf("error = %v, want bridgev2.ErrUnsupportedMessageType", err)
	}
	if called {
		t.Error("createTopicFn was called for an unsupported msgtype")
	}
}

func TestHandleMatrixMessageNoticeMsgTypeAccepted(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.CreateTopicRequest
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createTopicFn: func(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			gotReq = req
			return createTopicResponse("t", 1), nil
		},
	}

	msg := textMatrixMessage(spacePortal("space1"), "a notice")
	msg.Content.MsgType = event.MsgNotice

	if _, err := gc.HandleMatrixMessage(context.Background(), msg); err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil (NOTICE must be accepted like TEXT)", err)
	}
	if gotReq.GetTextBody() != "a notice" {
		t.Errorf("TextBody = %q, want %q", gotReq.GetTextBody(), "a notice")
	}
}

func TestHandleMatrixMessageInvalidPortalIDErrors(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	called := false
	gc := &GChatClient{
		UserLogin: login,
		createTopicFn: func(context.Context, *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			called = true
			return createTopicResponse("t", 1), nil
		},
	}

	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: networkid.PortalID("garbage")}}}
	_, err := gc.HandleMatrixMessage(context.Background(), textMatrixMessage(portal, "hi"))
	if err == nil {
		t.Fatal("HandleMatrixMessage() error = nil, want an error for an unparseable portal id")
	}
	if called {
		t.Error("createTopicFn was called despite an invalid portal id")
	}
}

func TestHandleMatrixMessageNoConnNoSeamErrors(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	gc := &GChatClient{UserLogin: login}

	_, err := gc.HandleMatrixMessage(context.Background(), textMatrixMessage(spacePortal("space1"), "hi"))
	if err == nil {
		t.Fatal("HandleMatrixMessage() error = nil, want an error when not connected")
	}
}

func TestHandleMatrixMessagePropagatesRPCError(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	wantErr := errors.New("create_topic: boom")
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createTopicFn: func(context.Context, *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			return nil, wantErr
		},
	}

	_, err := gc.HandleMatrixMessage(context.Background(), textMatrixMessage(spacePortal("space1"), "hi"))
	if !errors.Is(err, wantErr) {
		t.Errorf("error = %v, want wrapping %v", err, wantErr)
	}
}

// TestHandleMatrixMessagePlainTextHasNoAnnotations: nil annotations, not an empty slice.
func TestHandleMatrixMessagePlainTextHasNoAnnotations(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.CreateTopicRequest
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createTopicFn: func(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			gotReq = req
			return createTopicResponse("t", 1), nil
		},
	}

	_, err := gc.HandleMatrixMessage(context.Background(), textMatrixMessage(spacePortal("space1"), "hello world"))
	if err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}
	if len(gotReq.GetAnnotations()) != 0 {
		t.Errorf("Annotations = %v, want none for a plain-text message", gotReq.GetAnnotations())
	}
}

// accept_format_annotations is always true, never gated on whether annotations is empty.
func TestHandleMatrixMessageFormattedTextBuildsAnnotations(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.CreateTopicRequest
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createTopicFn: func(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			gotReq = req
			return createTopicResponse("t", 1), nil
		},
	}

	msg := textMatrixMessage(spacePortal("space1"), "a b c")
	msg.Content.Format = event.FormatHTML
	msg.Content.FormattedBody = "a <strong>b</strong> c"

	_, err := gc.HandleMatrixMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}
	if got := gotReq.GetTextBody(); got != "a b c" {
		t.Errorf("TextBody = %q, want %q", got, "a b c")
	}
	wantAnn := gchatfmt.MakeFormatAnnotation(2, 1, pb.FormatMetadata_BOLD)
	if len(gotReq.GetAnnotations()) != 1 || gotReq.GetAnnotations()[0].String() != wantAnn.String() {
		t.Errorf("Annotations = %s, want [%s]", formatAnnotationsForTest(gotReq.GetAnnotations()), wantAnn.String())
	}
	if !gotReq.GetMessageInfo().GetAcceptFormatAnnotations() {
		t.Error("MessageInfo.AcceptFormatAnnotations = false, want true")
	}
}

func TestHandleMatrixMessageMentionPillBecomesMentionAnnotation(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	matrix := &fakeMatrixConnector{
		parseGhostMXID: func(mxid id.UserID) (networkid.UserID, bool) {
			if mxid == "@200_ghost:example.com" {
				return "200", true
			}
			return "", false
		},
	}
	portal := &bridgev2.Portal{
		Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: gcid.MakePortalID(gcid.GroupID{ID: "space1", IsDM: false})}},
		Bridge: &bridgev2.Bridge{Matrix: matrix},
	}
	var gotReq *pb.CreateTopicRequest
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createTopicFn: func(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			gotReq = req
			return createTopicResponse("t", 1), nil
		},
	}

	msg := textMatrixMessage(portal, "plain-text fallback")
	msg.Content.Format = event.FormatHTML
	msg.Content.FormattedBody = `Hi <a href="https://matrix.to/#/@200_ghost:example.com">Bob</a>!`

	_, err := gc.HandleMatrixMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}
	if got := gotReq.GetTextBody(); got != "Hi @Bob!" {
		t.Errorf("TextBody = %q, want %q", got, "Hi @Bob!")
	}
	wantAnn := gchatfmt.MakeMentionAnnotation(3, 4, "200")
	if len(gotReq.GetAnnotations()) != 1 || gotReq.GetAnnotations()[0].String() != wantAnn.String() {
		t.Errorf("Annotations = %s, want [%s] (fix B2's outbound half)", formatAnnotationsForTest(gotReq.GetAnnotations()), wantAnn.String())
	}
}

func TestHandleMatrixMessageUnresolvableMentionRendersPlainText(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.CreateTopicRequest
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createTopicFn: func(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			gotReq = req
			return createTopicResponse("t", 1), nil
		},
	}

	msg := textMatrixMessage(spacePortal("space1"), "plain-text fallback")
	msg.Content.Format = event.FormatHTML
	msg.Content.FormattedBody = `Hi <a href="https://matrix.to/#/@stranger:elsewhere.example">Stranger</a>!`

	_, err := gc.HandleMatrixMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}
	if got := gotReq.GetTextBody(); got != "Hi Stranger!" {
		t.Errorf("TextBody = %q, want %q", got, "Hi Stranger!")
	}
	if len(gotReq.GetAnnotations()) != 0 {
		t.Errorf("Annotations = %v, want none for an unresolvable mention", gotReq.GetAnnotations())
	}
}

// mergeAnnotations must append, never replace the file annotation.

func TestMergeAnnotations_KeepsFileAndTextAnnotations(t *testing.T) {
	// Any pre-existing annotation stands in for a real UPLOAD_METADATA one.
	fileAnnotation := &pb.Annotation{Type: pb.AnnotationType_UPLOAD_METADATA.Enum()}
	textAnnotations := []*pb.Annotation{gchatfmt.MakeFormatAnnotation(0, 4, pb.FormatMetadata_BOLD)}

	got := mergeAnnotations([]*pb.Annotation{fileAnnotation}, textAnnotations)

	if len(got) != 2 {
		t.Fatalf("mergeAnnotations returned %d annotations, want 2 (file + text) -- B4 regression: the file annotation must survive a formatted caption", len(got))
	}
	if got[0] != fileAnnotation {
		t.Errorf("mergeAnnotations()[0] = %v, want the file annotation preserved first", got[0])
	}
	if got[1] != textAnnotations[0] {
		t.Errorf("mergeAnnotations()[1] = %v, want the caption's BOLD annotation appended after it", got[1])
	}
}

func TestMergeAnnotations_NoTextReturnsExistingUnchanged(t *testing.T) {
	fileAnnotation := &pb.Annotation{Type: pb.AnnotationType_UPLOAD_METADATA.Enum()}
	existing := []*pb.Annotation{fileAnnotation}

	got := mergeAnnotations(existing, nil)

	if len(got) != 1 || got[0] != fileAnnotation {
		t.Errorf("mergeAnnotations(existing, nil) = %v, want existing unchanged (a plain-text caption keeps only the file annotation)", got)
	}
}

func TestMergeAnnotations_NoExistingReturnsTextUnchanged(t *testing.T) {
	textAnnotations := []*pb.Annotation{gchatfmt.MakeFormatAnnotation(0, 4, pb.FormatMetadata_BOLD)}

	got := mergeAnnotations(nil, textAnnotations)

	if len(got) != 1 || got[0] != textAnnotations[0] {
		t.Errorf("mergeAnnotations(nil, text) = %v, want text unchanged", got)
	}
}

func TestMergeAnnotations_BothNilReturnsNil(t *testing.T) {
	if got := mergeAnnotations(nil, nil); got != nil {
		t.Errorf("mergeAnnotations(nil, nil) = %v, want nil (plain-body outbound -> no annotations)", got)
	}
}

func TestHandleMatrixMessageThreadRootRoutesToCreateMessage(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.CreateMessageRequest
	topicCalled := false
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createTopicFn: func(context.Context, *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			topicCalled = true
			return createTopicResponse("wrong-path", 1), nil
		},
		createMessageFn: func(_ context.Context, req *pb.CreateMessageRequest) (*pb.CreateMessageResponse, error) {
			gotReq = req
			return createMessageResponse("reply-msg-1", 2000), nil
		},
	}

	msg := threadedMatrixMessage(spacePortal("space1"), "a reply", "topic1", "topic1")
	_, err := gc.HandleMatrixMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}
	if topicCalled {
		t.Error("createTopicFn was called for a threaded message, want createMessageFn only")
	}
	if gotReq == nil {
		t.Fatal("createMessageFn was not called")
	}
	if got := gotReq.GetParentId().GetTopicId().GetTopicId(); got != "topic1" {
		t.Errorf("ParentId.TopicId.TopicId = %q, want %q (the thread root's stored topic id)", got, "topic1")
	}
	if got := gotReq.GetParentId().GetTopicId().GetGroupId().GetSpaceId().GetSpaceId(); got != "space1" {
		t.Errorf("ParentId.TopicId.GroupId.SpaceId = %q, want %q", got, "space1")
	}
	if got := gotReq.GetTextBody(); got != "a reply" {
		t.Errorf("TextBody = %q, want %q", got, "a reply")
	}
	if !gotReq.GetMessageInfo().GetAcceptFormatAnnotations() {
		t.Error("MessageInfo.AcceptFormatAnnotations = false, want true")
	}
	if gotReq.GetLocalId() == "" {
		t.Error("LocalId is empty, want a generated dedup token")
	}
}

// TestHandleMatrixMessageThreadedDMPortalBuildsDmGroupID: DMs have no visible threads but the wire shape is identical.
func TestHandleMatrixMessageThreadedDMPortalBuildsDmGroupID(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.CreateMessageRequest
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createMessageFn: func(_ context.Context, req *pb.CreateMessageRequest) (*pb.CreateMessageResponse, error) {
			gotReq = req
			return createMessageResponse("reply-msg-2", 3000), nil
		},
	}

	msg := threadedMatrixMessage(dmPortal("dm1"), "hi", "topic1", "topic1")
	if _, err := gc.HandleMatrixMessage(context.Background(), msg); err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}
	if got := gotReq.GetParentId().GetTopicId().GetGroupId().GetDmId().GetDmId(); got != "dm1" {
		t.Errorf("ParentId.TopicId.GroupId.DmId = %q, want %q", got, "dm1")
	}
	if gotReq.GetParentId().GetTopicId().GetGroupId().GetSpaceId() != nil {
		t.Error("ParentId.TopicId.GroupId.SpaceId is set for a DM portal, want unset")
	}
}

func TestHandleMatrixMessageNoThreadRootRoutesToCreateTopic(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	messageCalled := false
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createTopicFn: func(context.Context, *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			return createTopicResponse("topic1", 1000), nil
		},
		createMessageFn: func(context.Context, *pb.CreateMessageRequest) (*pb.CreateMessageResponse, error) {
			messageCalled = true
			return createMessageResponse("wrong-path", 1), nil
		},
	}

	_, err := gc.HandleMatrixMessage(context.Background(), textMatrixMessage(spacePortal("space1"), "hello"))
	if err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}
	if messageCalled {
		t.Error("createMessageFn was called for a non-threaded message, want createTopicFn only")
	}
}

// TestHandleMatrixMessageThreadRootFallsBackToRootIDWhenTopicIDMissing: message_id == topic_id for a head message.
func TestHandleMatrixMessageThreadRootFallsBackToRootIDWhenTopicIDMissing(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.CreateMessageRequest
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createMessageFn: func(_ context.Context, req *pb.CreateMessageRequest) (*pb.CreateMessageResponse, error) {
			gotReq = req
			return createMessageResponse("reply-msg-3", 4000), nil
		},
	}

	msg := textMatrixMessage(spacePortal("space1"), "a reply")
	msg.ThreadRoot = &database.Message{ID: gcid.MakeMessageID("headmsg1")} // no Metadata at all

	if _, err := gc.HandleMatrixMessage(context.Background(), msg); err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}
	if got := gotReq.GetParentId().GetTopicId().GetTopicId(); got != "headmsg1" {
		t.Errorf("ParentId.TopicId.TopicId = %q, want %q (fallback to the root's own message id)", got, "headmsg1")
	}
}

// TestHandleMatrixMessageThreadedMapsResponseToDBMessage: the id is the new reply's, never the topic id.
func TestHandleMatrixMessageThreadedMapsResponseToDBMessage(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createMessageFn: func(context.Context, *pb.CreateMessageRequest) (*pb.CreateMessageResponse, error) {
			return createMessageResponse("reply-msg-4", 1_700_000_000_000_000), nil
		},
	}

	msg := threadedMatrixMessage(spacePortal("space1"), "a reply", "topic1", "topic1")
	resp, err := gc.HandleMatrixMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}
	if resp == nil || resp.DB == nil {
		t.Fatal("HandleMatrixMessage() response/DB is nil")
	}
	if resp.DB.ID != gcid.MakeMessageID("reply-msg-4") {
		t.Errorf("DB.ID = %q, want %q (the reply's own message id, not the topic id)", resp.DB.ID, gcid.MakeMessageID("reply-msg-4"))
	}
	if resp.DB.SenderID != gcid.MakeUserID("112233") {
		t.Errorf("DB.SenderID = %q, want own user id %q", resp.DB.SenderID, gcid.MakeUserID("112233"))
	}
	wantTS := time.UnixMicro(1_700_000_000_000_000).UTC()
	if !resp.DB.Timestamp.Equal(wantTS) {
		t.Errorf("DB.Timestamp = %v, want %v", resp.DB.Timestamp, wantTS)
	}
	meta, ok := resp.DB.Metadata.(*MessageMetadata)
	if !ok {
		t.Fatalf("DB.Metadata type = %T, want *MessageMetadata", resp.DB.Metadata)
	}
	if meta.TimestampMicro != 1_700_000_000_000_000 {
		t.Errorf("Metadata.TimestampMicro = %d, want %d", meta.TimestampMicro, 1_700_000_000_000_000)
	}
	if meta.TopicID != "topic1" {
		t.Errorf("Metadata.TopicID = %q, want %q (the topic this reply was posted into)", meta.TopicID, "topic1")
	}
}

func TestHandleMatrixMessageThreadedNoConnNoSeamErrors(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	gc := &GChatClient{UserLogin: login}

	msg := threadedMatrixMessage(spacePortal("space1"), "a reply", "topic1", "topic1")
	_, err := gc.HandleMatrixMessage(context.Background(), msg)
	if err == nil {
		t.Fatal("HandleMatrixMessage() error = nil, want an error when not connected")
	}
}

func TestHandleMatrixMessageThreadedPropagatesRPCError(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	wantErr := errors.New("create_message: boom")
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createMessageFn: func(context.Context, *pb.CreateMessageRequest) (*pb.CreateMessageResponse, error) {
			return nil, wantErr
		},
	}

	msg := threadedMatrixMessage(spacePortal("space1"), "a reply", "topic1", "topic1")
	_, err := gc.HandleMatrixMessage(context.Background(), msg)
	if !errors.Is(err, wantErr) {
		t.Errorf("error = %v, want wrapping %v", err, wantErr)
	}
}

func TestHandleMatrixMessageThreadedFailureRemovesPendingRegistration(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var registeredTxnID, removedTxnID networkid.TransactionID
	gc := &GChatClient{
		UserLogin: login,
		addPendingToIgnoreFn: func(_ *bridgev2.MatrixMessage, txnID networkid.TransactionID) {
			registeredTxnID = txnID
		},
		removePendingFn: func(_ *bridgev2.MatrixMessage, txnID networkid.TransactionID) {
			removedTxnID = txnID
		},
		createMessageFn: func(context.Context, *pb.CreateMessageRequest) (*pb.CreateMessageResponse, error) {
			return nil, context.DeadlineExceeded
		},
	}

	msg := threadedMatrixMessage(spacePortal("space1"), "a reply", "topic1", "topic1")
	resp, err := gc.HandleMatrixMessage(context.Background(), msg)
	if err == nil {
		t.Fatal("HandleMatrixMessage() error = nil, want an error from the failed RPC")
	}
	if resp != nil {
		t.Errorf("HandleMatrixMessage() response = %+v, want nil on error", resp)
	}
	if registeredTxnID == "" {
		t.Fatal("addPendingToIgnoreFn was never called -- local_id must still be registered before the failed RPC")
	}
	if removedTxnID != registeredTxnID {
		t.Errorf("removePendingFn txn id = %q, want it to match the registered txn id %q", removedTxnID, registeredTxnID)
	}
}

// TestHandleMatrixMessageThreadedUsesSameLocalIDPrefix: local_id is generated once, before thread branching.
func TestHandleMatrixMessageThreadedUsesSameLocalIDPrefix(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.CreateMessageRequest
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createMessageFn: func(_ context.Context, req *pb.CreateMessageRequest) (*pb.CreateMessageResponse, error) {
			gotReq = req
			return createMessageResponse("reply-msg-5", 1), nil
		},
	}

	msg := threadedMatrixMessage(spacePortal("space1"), "hi", "topic1", "topic1")
	if _, err := gc.HandleMatrixMessage(context.Background(), msg); err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}
	if !strings.HasPrefix(gotReq.GetLocalId(), "mautrix-googlechat%") {
		t.Errorf("LocalId = %q, want prefix %q", gotReq.GetLocalId(), "mautrix-googlechat%")
	}
}

func TestHandleMatrixMessageThreadedFormattingAndMentionsWired(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.CreateMessageRequest
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createMessageFn: func(_ context.Context, req *pb.CreateMessageRequest) (*pb.CreateMessageResponse, error) {
			gotReq = req
			return createMessageResponse("reply-msg-6", 1), nil
		},
	}

	msg := threadedMatrixMessage(spacePortal("space1"), "a b c", "topic1", "topic1")
	msg.Content.Format = event.FormatHTML
	msg.Content.FormattedBody = "a <strong>b</strong> c"

	if _, err := gc.HandleMatrixMessage(context.Background(), msg); err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}
	if got := gotReq.GetTextBody(); got != "a b c" {
		t.Errorf("TextBody = %q, want %q", got, "a b c")
	}
	wantAnn := gchatfmt.MakeFormatAnnotation(2, 1, pb.FormatMetadata_BOLD)
	if len(gotReq.GetAnnotations()) != 1 || gotReq.GetAnnotations()[0].String() != wantAnn.String() {
		t.Errorf("Annotations = %s, want [%s]", formatAnnotationsForTest(gotReq.GetAnnotations()), wantAnn.String())
	}
}

// TestHandleMatrixMessageReplyBuildsSendReplyTarget: with no thread, topic_id falls back to the target's own id.
func TestHandleMatrixMessageReplyBuildsSendReplyTarget(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.CreateTopicRequest
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createTopicFn: func(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			gotReq = req
			return createTopicResponse("t", 1), nil
		},
	}

	msg := replyMatrixMessage(spacePortal("space1"), "a reply", "target1", 555_000)
	if _, err := gc.HandleMatrixMessage(context.Background(), msg); err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}

	replyTarget := gotReq.GetMessageInfo().GetReplyTo()
	if replyTarget == nil {
		t.Fatal("MessageInfo.ReplyTo is nil, want a SendReplyTarget")
	}
	if got := replyTarget.GetId().GetMessageId(); got != "target1" {
		t.Errorf("ReplyTo.Id.MessageId = %q, want %q", got, "target1")
	}
	if got := replyTarget.GetCreateTime(); got != 555_000 {
		t.Errorf("ReplyTo.CreateTime = %d, want %d (the target's stored µs create_time)", got, 555_000)
	}
	if got := replyTarget.GetId().GetParentId().GetTopicId().GetTopicId(); got != "target1" {
		t.Errorf("ReplyTo.Id.ParentId.TopicId.TopicId = %q, want %q (thread_id empty -> falls back to the target's own id)", got, "target1")
	}
	if got := replyTarget.GetId().GetParentId().GetTopicId().GetGroupId().GetSpaceId().GetSpaceId(); got != "space1" {
		t.Errorf("ReplyTo.Id.ParentId.TopicId.GroupId.SpaceId = %q, want %q", got, "space1")
	}
}

func TestHandleMatrixMessageReplyInDMPortalBuildsDmGroupID(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.CreateTopicRequest
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createTopicFn: func(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			gotReq = req
			return createTopicResponse("t", 1), nil
		},
	}

	msg := replyMatrixMessage(dmPortal("dm1"), "a reply", "target1", 555_000)
	if _, err := gc.HandleMatrixMessage(context.Background(), msg); err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}

	if got := gotReq.GetMessageInfo().GetReplyTo().GetId().GetParentId().GetTopicId().GetGroupId().GetDmId().GetDmId(); got != "dm1" {
		t.Errorf("ReplyTo.Id.ParentId.TopicId.GroupId.DmId = %q, want %q", got, "dm1")
	}
}

// TestHandleMatrixMessageReplyAndThreadBothSet: routing follows ThreadRoot; reply_to's topic_id uses the thread's id.
func TestHandleMatrixMessageReplyAndThreadBothSet(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.CreateMessageRequest
	topicCalled := false
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createTopicFn: func(context.Context, *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			topicCalled = true
			return createTopicResponse("wrong-path", 1), nil
		},
		createMessageFn: func(_ context.Context, req *pb.CreateMessageRequest) (*pb.CreateMessageResponse, error) {
			gotReq = req
			return createMessageResponse("reply-msg-7", 1), nil
		},
	}

	msg := threadedMatrixMessage(spacePortal("space1"), "a reply", "topic1", "topic1")
	msg.ReplyTo = &database.Message{
		ID:       gcid.MakeMessageID("target-in-thread"),
		Metadata: &MessageMetadata{TimestampMicro: 777_000},
	}

	if _, err := gc.HandleMatrixMessage(context.Background(), msg); err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}
	if topicCalled {
		t.Error("createTopicFn was called for a threaded reply, want createMessageFn only (thread routing must stay governed by ThreadRoot)")
	}
	if got := gotReq.GetParentId().GetTopicId().GetTopicId(); got != "topic1" {
		t.Errorf("ParentId.TopicId.TopicId = %q, want %q (thread routing unaffected by ReplyTo)", got, "topic1")
	}

	replyTarget := gotReq.GetMessageInfo().GetReplyTo()
	if replyTarget == nil {
		t.Fatal("MessageInfo.ReplyTo is nil, want a SendReplyTarget")
	}
	if got := replyTarget.GetId().GetMessageId(); got != "target-in-thread" {
		t.Errorf("ReplyTo.Id.MessageId = %q, want %q", got, "target-in-thread")
	}
	if got := replyTarget.GetCreateTime(); got != 777_000 {
		t.Errorf("ReplyTo.CreateTime = %d, want %d", got, 777_000)
	}
	if got := replyTarget.GetId().GetParentId().GetTopicId().GetTopicId(); got != "topic1" {
		t.Errorf("ReplyTo.Id.ParentId.TopicId.TopicId = %q, want %q (thread_id truthy -> used verbatim, NOT the reply target's own id)", got, "topic1")
	}
}

// TestHandleMatrixMessageReplyMissingTimestampGraceful: no stored create_time sends without a reply target
// rather than a malformed one the server would reject.
func TestHandleMatrixMessageReplyMissingTimestampGraceful(t *testing.T) {
	cases := []struct {
		name     string
		metadata any
	}{
		{"zero TimestampMicro", &MessageMetadata{TimestampMicro: 0}},
		{"nil Metadata", nil},
		{"wrong Metadata type", "not-a-MessageMetadata"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			login := newTestUserLogin(&UserLoginMetadata{})
			var gotReq *pb.CreateTopicRequest
			gc := &GChatClient{
				UserLogin:            login,
				addPendingToIgnoreFn: noopAddPendingToIgnore,
				createTopicFn: func(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
					gotReq = req
					return createTopicResponse("t", 1), nil
				},
			}

			msg := textMatrixMessage(spacePortal("space1"), "a reply")
			msg.ReplyTo = &database.Message{ID: gcid.MakeMessageID("target-no-ts"), Metadata: tc.metadata}

			if _, err := gc.HandleMatrixMessage(context.Background(), msg); err != nil {
				t.Fatalf("HandleMatrixMessage() error = %v, want nil (missing timestamp must degrade gracefully, not error)", err)
			}
			if gotReq.GetMessageInfo().GetReplyTo() != nil {
				t.Errorf("MessageInfo.ReplyTo = %v, want nil (no usable create_time -> send without a reply target)", gotReq.GetMessageInfo().GetReplyTo())
			}
		})
	}
}

// mediaMatrixMessage builds a media message; filename != body means body is a caption (MSC2530).
func mediaMatrixMessage(portal *bridgev2.Portal, msgtype event.MessageType, body, filename, mimeType string) *bridgev2.MatrixMessage {
	return &bridgev2.MatrixMessage{
		MatrixEventBase: bridgev2.MatrixEventBase[*event.MessageEventContent]{
			Portal: portal,
			Content: &event.MessageEventContent{
				MsgType:  msgtype,
				Body:     body,
				FileName: filename,
				URL:      "mxc://example.com/abc",
				Info:     &event.FileInfo{MimeType: mimeType},
			},
		},
	}
}

func testUploadMetadata(contentName, contentType string) *pb.UploadMetadata {
	return &pb.UploadMetadata{
		ContentName: proto.String(contentName),
		ContentType: proto.String(contentType),
	}
}

// TestHandleMatrixMessageImageBuildsUploadAnnotation: an uncaptioned file sends no duplicate text_body.
func TestHandleMatrixMessageImageBuildsUploadAnnotation(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	wantMeta := testUploadMetadata("cat.png", "image/png")

	var gotDownloadURI id.ContentURIString
	var gotDownloadFile *event.EncryptedFileInfo
	var gotGroupID string
	var gotData []byte
	var gotFilename, gotMime string
	var gotReq *pb.CreateTopicRequest

	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		downloadMediaFn: func(_ context.Context, uri id.ContentURIString, file *event.EncryptedFileInfo) ([]byte, error) {
			gotDownloadURI = uri
			gotDownloadFile = file
			return []byte("fake png bytes"), nil
		},
		uploadFileFn: func(_ context.Context, groupID string, data []byte, filename, mimeType string) (*pb.UploadMetadata, error) {
			gotGroupID = groupID
			gotData = data
			gotFilename = filename
			gotMime = mimeType
			return wantMeta, nil
		},
		createTopicFn: func(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			gotReq = req
			return createTopicResponse("topic1", 1000), nil
		},
	}

	msg := mediaMatrixMessage(spacePortal("space1"), event.MsgImage, "cat.png", "cat.png", "image/png")
	_, err := gc.HandleMatrixMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}

	if gotDownloadURI != "mxc://example.com/abc" {
		t.Errorf("downloadMediaFn uri = %q, want %q", gotDownloadURI, "mxc://example.com/abc")
	}
	if gotDownloadFile != nil {
		t.Errorf("downloadMediaFn file = %v, want nil (unencrypted room)", gotDownloadFile)
	}
	if gotGroupID != "space1" {
		t.Errorf("uploadFileFn groupID = %q, want %q (plain, no space: prefix)", gotGroupID, "space1")
	}
	if string(gotData) != "fake png bytes" {
		t.Errorf("uploadFileFn data = %q, want the downloaded bytes", gotData)
	}
	if gotFilename != "cat.png" {
		t.Errorf("uploadFileFn filename = %q, want %q", gotFilename, "cat.png")
	}
	if gotMime != "image/png" {
		t.Errorf("uploadFileFn mimeType = %q, want %q", gotMime, "image/png")
	}

	if got := gotReq.GetTextBody(); got != "" {
		t.Errorf("TextBody = %q, want empty (no genuine caption, Body == FileName)", got)
	}
	anns := gotReq.GetAnnotations()
	if len(anns) != 1 {
		t.Fatalf("Annotations = %d, want exactly 1 (the UPLOAD_METADATA annotation)", len(anns))
	}
	if got := anns[0].GetType(); got != pb.AnnotationType_UPLOAD_METADATA {
		t.Errorf("Annotations[0].Type = %v, want UPLOAD_METADATA", got)
	}
	if got := anns[0].GetChipRenderType(); got != pb.Annotation_RENDER {
		t.Errorf("Annotations[0].ChipRenderType = %v, want RENDER", got)
	}
	if got := anns[0].GetUploadMetadata(); got != wantMeta {
		t.Errorf("Annotations[0].UploadMetadata = %v, want the uploaded metadata", got)
	}
}

func TestHandleMatrixMessageImageWithCaptionKeepsBothTextAndAnnotation(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	wantMeta := testUploadMetadata("vacation.jpg", "image/jpeg")

	var gotFilename string
	var gotReq *pb.CreateTopicRequest

	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		downloadMediaFn: func(context.Context, id.ContentURIString, *event.EncryptedFileInfo) ([]byte, error) {
			return []byte("fake jpeg bytes"), nil
		},
		uploadFileFn: func(_ context.Context, _ string, _ []byte, filename, _ string) (*pb.UploadMetadata, error) {
			gotFilename = filename
			return wantMeta, nil
		},
		createTopicFn: func(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			gotReq = req
			return createTopicResponse("topic1", 1000), nil
		},
	}

	msg := mediaMatrixMessage(spacePortal("space1"), event.MsgImage, "Look at this view!", "vacation.jpg", "image/jpeg")
	_, err := gc.HandleMatrixMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}

	if gotFilename != "vacation.jpg" {
		t.Errorf("uploadFileFn filename = %q, want %q (the real FileName, not the caption)", gotFilename, "vacation.jpg")
	}

	if got := gotReq.GetTextBody(); got != "Look at this view!" {
		t.Errorf("TextBody = %q, want the caption text %q", got, "Look at this view!")
	}
	anns := gotReq.GetAnnotations()
	if len(anns) != 1 || anns[0].GetType() != pb.AnnotationType_UPLOAD_METADATA {
		t.Fatalf("Annotations = %s, want exactly [UPLOAD_METADATA] (plain caption has no formatting annotations of its own)", formatAnnotationsForTest(anns))
	}
}

func TestHandleMatrixMessageFormattedCaptionKeepsFileAndFormattingAnnotations(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	wantMeta := testUploadMetadata("vacation.jpg", "image/jpeg")

	var gotReq *pb.CreateTopicRequest
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		downloadMediaFn: func(context.Context, id.ContentURIString, *event.EncryptedFileInfo) ([]byte, error) {
			return []byte("fake jpeg bytes"), nil
		},
		uploadFileFn: func(context.Context, string, []byte, string, string) (*pb.UploadMetadata, error) {
			return wantMeta, nil
		},
		createTopicFn: func(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			gotReq = req
			return createTopicResponse("topic1", 1000), nil
		},
	}

	msg := mediaMatrixMessage(spacePortal("space1"), event.MsgImage, "a b c", "vacation.jpg", "image/jpeg")
	msg.Content.Format = event.FormatHTML
	msg.Content.FormattedBody = "a <strong>b</strong> c"

	_, err := gc.HandleMatrixMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}

	if got := gotReq.GetTextBody(); got != "a b c" {
		t.Errorf("TextBody = %q, want %q", got, "a b c")
	}
	anns := gotReq.GetAnnotations()
	if len(anns) != 2 {
		t.Fatalf("Annotations = %s, want 2 (file + formatting) -- B4 regression: the file annotation must survive a formatted caption", formatAnnotationsForTest(anns))
	}
	if got := anns[0].GetType(); got != pb.AnnotationType_UPLOAD_METADATA {
		t.Errorf("Annotations[0].Type = %v, want UPLOAD_METADATA (file annotation first)", got)
	}
	wantFormat := gchatfmt.MakeFormatAnnotation(2, 1, pb.FormatMetadata_BOLD)
	if len(anns) < 2 || anns[1].String() != wantFormat.String() {
		t.Errorf("Annotations[1] = %s, want the BOLD formatting annotation %s", formatAnnotationsForTest(anns), wantFormat.String())
	}
}

func TestHandleMatrixMessageMediaReplyKeepsBothUploadAndReplyTarget(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	wantMeta := testUploadMetadata("cat.png", "image/png")

	var gotReq *pb.CreateTopicRequest
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		downloadMediaFn: func(context.Context, id.ContentURIString, *event.EncryptedFileInfo) ([]byte, error) {
			return []byte("fake png bytes"), nil
		},
		uploadFileFn: func(context.Context, string, []byte, string, string) (*pb.UploadMetadata, error) {
			return wantMeta, nil
		},
		createTopicFn: func(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			gotReq = req
			return createTopicResponse("t", 1), nil
		},
	}

	msg := mediaMatrixMessage(spacePortal("space1"), event.MsgImage, "cat.png", "cat.png", "image/png")
	msg.ReplyTo = &database.Message{
		ID:       gcid.MakeMessageID("target1"),
		Metadata: &MessageMetadata{TimestampMicro: 555_000},
	}

	if _, err := gc.HandleMatrixMessage(context.Background(), msg); err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}

	anns := gotReq.GetAnnotations()
	if len(anns) != 1 || anns[0].GetType() != pb.AnnotationType_UPLOAD_METADATA {
		t.Fatalf("Annotations = %s, want exactly [UPLOAD_METADATA] (the attached file must survive a reply)", formatAnnotationsForTest(anns))
	}
	if got := anns[0].GetUploadMetadata(); got != wantMeta {
		t.Errorf("Annotations[0].UploadMetadata = %v, want the uploaded metadata", got)
	}
	// The reply target must also be present.
	replyTarget := gotReq.GetMessageInfo().GetReplyTo()
	if replyTarget == nil {
		t.Fatal("MessageInfo.ReplyTo is nil, want a SendReplyTarget alongside the upload annotation")
	}
	if got := replyTarget.GetId().GetMessageId(); got != "target1" {
		t.Errorf("ReplyTo.Id.MessageId = %q, want %q", got, "target1")
	}
	if got := replyTarget.GetCreateTime(); got != 555_000 {
		t.Errorf("ReplyTo.CreateTime = %d, want %d", got, 555_000)
	}
}

// TestHandleMatrixMessageUploadFileErrorIsCleanNoPartialSend: create_topic must never be called.
func TestHandleMatrixMessageUploadFileErrorIsCleanNoPartialSend(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	wantErr := errors.New("gchatmeow: upload finalize request failed: unexpected status 500")
	createTopicCalled := false

	gc := &GChatClient{
		UserLogin: login,
		downloadMediaFn: func(context.Context, id.ContentURIString, *event.EncryptedFileInfo) ([]byte, error) {
			return []byte("fake png bytes"), nil
		},
		uploadFileFn: func(context.Context, string, []byte, string, string) (*pb.UploadMetadata, error) {
			return nil, wantErr
		},
		createTopicFn: func(context.Context, *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			createTopicCalled = true
			return createTopicResponse("t", 1), nil
		},
	}

	msg := mediaMatrixMessage(spacePortal("space1"), event.MsgImage, "cat.png", "cat.png", "image/png")
	_, err := gc.HandleMatrixMessage(context.Background(), msg)
	if err == nil {
		t.Fatal("HandleMatrixMessage() error = nil, want a clean error (the #114 upload failure)")
	}
	if !errors.Is(err, wantErr) {
		t.Errorf("error = %v, want wrapping %v", err, wantErr)
	}
	if createTopicCalled {
		t.Error("createTopicFn was called despite an UploadFile failure -- no partial send is allowed (#114 must not silently drop the file or send text-only)")
	}
}

func TestHandleMatrixMessageDownloadMediaErrorIsCleanNoPartialSend(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	wantErr := errors.New("homeserver: failed to download mxc://example.com/abc")
	uploadCalled := false
	createTopicCalled := false

	gc := &GChatClient{
		UserLogin: login,
		downloadMediaFn: func(context.Context, id.ContentURIString, *event.EncryptedFileInfo) ([]byte, error) {
			return nil, wantErr
		},
		uploadFileFn: func(context.Context, string, []byte, string, string) (*pb.UploadMetadata, error) {
			uploadCalled = true
			return testUploadMetadata("x", "image/png"), nil
		},
		createTopicFn: func(context.Context, *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			createTopicCalled = true
			return createTopicResponse("t", 1), nil
		},
	}

	msg := mediaMatrixMessage(spacePortal("space1"), event.MsgImage, "cat.png", "cat.png", "image/png")
	_, err := gc.HandleMatrixMessage(context.Background(), msg)
	if err == nil {
		t.Fatal("HandleMatrixMessage() error = nil, want a clean error")
	}
	if !errors.Is(err, wantErr) {
		t.Errorf("error = %v, want wrapping %v", err, wantErr)
	}
	if uploadCalled {
		t.Error("uploadFileFn was called despite a download failure")
	}
	if createTopicCalled {
		t.Error("createTopicFn was called despite a download failure -- no partial send is allowed")
	}
}

// TestHandleMatrixMessageEncryptedFileDownloadPath: File must be passed through so bridgev2 decrypts it.
func TestHandleMatrixMessageEncryptedFileDownloadPath(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	wantFile := &event.EncryptedFileInfo{URL: "mxc://example.com/encrypted-blob"}
	var gotFile *event.EncryptedFileInfo
	var gotURI id.ContentURIString

	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		downloadMediaFn: func(_ context.Context, uri id.ContentURIString, file *event.EncryptedFileInfo) ([]byte, error) {
			gotURI = uri
			gotFile = file
			return []byte("decrypted bytes"), nil
		},
		uploadFileFn: func(context.Context, string, []byte, string, string) (*pb.UploadMetadata, error) {
			return testUploadMetadata("secret.png", "image/png"), nil
		},
		createTopicFn: func(context.Context, *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			return createTopicResponse("t", 1), nil
		},
	}

	msg := mediaMatrixMessage(spacePortal("space1"), event.MsgImage, "secret.png", "secret.png", "image/png")
	msg.Content.URL = "" // encrypted rooms carry the ciphertext URL under File.URL, not Content.URL
	msg.Content.File = wantFile

	_, err := gc.HandleMatrixMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}
	if gotFile != wantFile {
		t.Errorf("downloadMediaFn file = %v, want the exact *event.EncryptedFileInfo from msg.Content.File", gotFile)
	}
	if gotURI != "" {
		t.Errorf("downloadMediaFn uri = %q, want empty (msg.Content.URL is unset for an encrypted upload; bridgev2's own DownloadMedia reads file.URL instead)", gotURI)
	}
}

func TestHandleMatrixMessageDisableOutboundMediaReturnsCleanError(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	downloadCalled := false
	createTopicCalled := false

	gc := &GChatClient{
		UserLogin: login,
		Main:      &GChatConnector{Config: Config{DisableOutboundMedia: true}},
		downloadMediaFn: func(context.Context, id.ContentURIString, *event.EncryptedFileInfo) ([]byte, error) {
			downloadCalled = true
			return []byte("x"), nil
		},
		createTopicFn: func(context.Context, *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			createTopicCalled = true
			return createTopicResponse("t", 1), nil
		},
	}

	msg := mediaMatrixMessage(spacePortal("space1"), event.MsgImage, "cat.png", "cat.png", "image/png")
	_, err := gc.HandleMatrixMessage(context.Background(), msg)
	if !errors.Is(err, errOutboundMediaDisabled) {
		t.Errorf("error = %v, want errOutboundMediaDisabled", err)
	}
	if downloadCalled {
		t.Error("downloadMediaFn was called despite DisableOutboundMedia")
	}
	if createTopicCalled {
		t.Error("createTopicFn was called despite DisableOutboundMedia")
	}
}

// TestHandleMatrixMessageMediaGroupIDPlainForDMAndSpace: UploadFile takes the plain numeric id, not the oneof shape.
func TestHandleMatrixMessageMediaGroupIDPlainForDMAndSpace(t *testing.T) {
	cases := []struct {
		name   string
		portal *bridgev2.Portal
	}{
		{"space", spacePortal("space42")},
		{"dm", dmPortal("dm42")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			login := newTestUserLogin(&UserLoginMetadata{})
			var gotGroupID string
			gc := &GChatClient{
				UserLogin:            login,
				addPendingToIgnoreFn: noopAddPendingToIgnore,
				downloadMediaFn: func(context.Context, id.ContentURIString, *event.EncryptedFileInfo) ([]byte, error) {
					return []byte("x"), nil
				},
				uploadFileFn: func(_ context.Context, groupID string, _ []byte, _, _ string) (*pb.UploadMetadata, error) {
					gotGroupID = groupID
					return testUploadMetadata("x.png", "image/png"), nil
				},
				createTopicFn: func(context.Context, *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
					return createTopicResponse("t", 1), nil
				},
			}

			expectedID := "space42"
			if tc.name == "dm" {
				expectedID = "dm42"
			}
			msg := mediaMatrixMessage(tc.portal, event.MsgImage, "x.png", "x.png", "image/png")
			if _, err := gc.HandleMatrixMessage(context.Background(), msg); err != nil {
				t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
			}
			if gotGroupID != expectedID {
				t.Errorf("uploadFileFn groupID = %q, want %q", gotGroupID, expectedID)
			}
		})
	}
}

func TestHandleMatrixMessageMediaThreadRootRoutesToCreateMessage(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	wantMeta := testUploadMetadata("clip.mp4", "video/mp4")
	var gotReq *pb.CreateMessageRequest
	createTopicCalled := false

	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		downloadMediaFn: func(context.Context, id.ContentURIString, *event.EncryptedFileInfo) ([]byte, error) {
			return []byte("fake video bytes"), nil
		},
		uploadFileFn: func(context.Context, string, []byte, string, string) (*pb.UploadMetadata, error) {
			return wantMeta, nil
		},
		createMessageFn: func(_ context.Context, req *pb.CreateMessageRequest) (*pb.CreateMessageResponse, error) {
			gotReq = req
			return createMessageResponse("msg1", 2000), nil
		},
		createTopicFn: func(context.Context, *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			createTopicCalled = true
			return createTopicResponse("t", 1), nil
		},
	}

	msg := threadedMatrixMessage(spacePortal("space1"), "", "root-msg", "root-topic")
	msg.Content = mediaMatrixMessage(spacePortal("space1"), event.MsgVideo, "clip.mp4", "clip.mp4", "video/mp4").Content

	if _, err := gc.HandleMatrixMessage(context.Background(), msg); err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}
	if createTopicCalled {
		t.Error("createTopicFn was called for a threaded media message, want create_message")
	}
	if gotReq == nil {
		t.Fatal("createMessageFn was not called")
	}
	anns := gotReq.GetAnnotations()
	if len(anns) != 1 || anns[0].GetType() != pb.AnnotationType_UPLOAD_METADATA {
		t.Fatalf("Annotations = %s, want exactly [UPLOAD_METADATA]", formatAnnotationsForTest(anns))
	}
	if got := anns[0].GetUploadMetadata(); got != wantMeta {
		t.Errorf("Annotations[0].UploadMetadata = %v, want %v", got, wantMeta)
	}
}

func TestHandleMatrixMessageMediaMsgTypesAccepted(t *testing.T) {
	for _, msgtype := range []event.MessageType{event.MsgImage, event.MsgFile, event.MsgVideo, event.MsgAudio} {
		t.Run(string(msgtype), func(t *testing.T) {
			login := newTestUserLogin(&UserLoginMetadata{})
			var gotReq *pb.CreateTopicRequest
			gc := &GChatClient{
				UserLogin:            login,
				addPendingToIgnoreFn: noopAddPendingToIgnore,
				downloadMediaFn: func(context.Context, id.ContentURIString, *event.EncryptedFileInfo) ([]byte, error) {
					return []byte("bytes"), nil
				},
				uploadFileFn: func(context.Context, string, []byte, string, string) (*pb.UploadMetadata, error) {
					return testUploadMetadata("f", "application/octet-stream"), nil
				},
				createTopicFn: func(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
					gotReq = req
					return createTopicResponse("t", 1), nil
				},
			}

			msg := mediaMatrixMessage(spacePortal("space1"), msgtype, "f", "f", "application/octet-stream")
			if _, err := gc.HandleMatrixMessage(context.Background(), msg); err != nil {
				t.Fatalf("HandleMatrixMessage() error = %v, want nil for msgtype %s", err, msgtype)
			}
			if len(gotReq.GetAnnotations()) != 1 {
				t.Errorf("Annotations = %d, want exactly 1 for msgtype %s", len(gotReq.GetAnnotations()), msgtype)
			}
		})
	}
}

func TestHandleMatrixMessageMediaNoConnNoSeamErrors(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	gc := &GChatClient{
		UserLogin: login,
		downloadMediaFn: func(context.Context, id.ContentURIString, *event.EncryptedFileInfo) ([]byte, error) {
			return []byte("bytes"), nil
		},
	}

	msg := mediaMatrixMessage(spacePortal("space1"), event.MsgImage, "cat.png", "cat.png", "image/png")
	_, err := gc.HandleMatrixMessage(context.Background(), msg)
	if err == nil {
		t.Fatal("HandleMatrixMessage() error = nil, want an error when not connected")
	}
}

// TestHandleMatrixMessageMediaDefaultDownloadUsesPortalBridgeBot exercises the default download path without the Fn seam.
func TestHandleMatrixMessageMediaDefaultDownloadUsesPortalBridgeBot(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	wantFile := &event.EncryptedFileInfo{URL: "mxc://example.com/ciphertext"}
	var gotURI id.ContentURIString
	var gotFile *event.EncryptedFileInfo

	portal := &bridgev2.Portal{
		Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: gcid.MakePortalID(gcid.GroupID{ID: "space1", IsDM: false})}},
		Bridge: &bridgev2.Bridge{Bot: fakeDownloadIntent{
			downloadFn: func(_ context.Context, uri id.ContentURIString, file *event.EncryptedFileInfo) ([]byte, error) {
				gotURI = uri
				gotFile = file
				return []byte("bytes"), nil
			},
		}},
	}

	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		uploadFileFn: func(context.Context, string, []byte, string, string) (*pb.UploadMetadata, error) {
			return testUploadMetadata("f.png", "image/png"), nil
		},
		createTopicFn: func(context.Context, *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			return createTopicResponse("t", 1), nil
		},
	}

	msg := mediaMatrixMessage(portal, event.MsgImage, "f.png", "f.png", "image/png")
	msg.Content.URL = ""
	msg.Content.File = wantFile

	if _, err := gc.HandleMatrixMessage(context.Background(), msg); err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}
	if gotFile != wantFile {
		t.Errorf("Portal.Bridge.Bot.DownloadMedia file = %v, want %v", gotFile, wantFile)
	}
	if gotURI != "" {
		t.Errorf("Portal.Bridge.Bot.DownloadMedia uri = %q, want empty", gotURI)
	}
}

// fakeDownloadIntent implements bridgev2.MatrixAPI, overriding only DownloadMedia.
type fakeDownloadIntent struct {
	bridgev2.MatrixAPI
	downloadFn func(ctx context.Context, uri id.ContentURIString, file *event.EncryptedFileInfo) ([]byte, error)
}

func (f fakeDownloadIntent) DownloadMedia(ctx context.Context, uri id.ContentURIString, file *event.EncryptedFileInfo) ([]byte, error) {
	if f.downloadFn != nil {
		return f.downloadFn(ctx, uri, file)
	}
	return nil, errors.New("fakeDownloadIntent: downloadFn not set")
}
