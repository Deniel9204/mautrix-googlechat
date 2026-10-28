package connector

import (
	"context"
	"errors"
	"testing"

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

func textMatrixEdit(portal *bridgev2.Portal, body string, target *database.Message) *bridgev2.MatrixEdit {
	return &bridgev2.MatrixEdit{
		MatrixEventBase: bridgev2.MatrixEventBase[*event.MessageEventContent]{
			Portal:  portal,
			Content: &event.MessageEventContent{MsgType: event.MsgText, Body: body},
		},
		EditTarget: target,
	}
}

func editMessageResponse(lastEditTime int64) *pb.EditMessageResponse {
	return &pb.EditMessageResponse{
		Message: &pb.Message{LastEditTime: proto.Int64(lastEditTime)},
	}
}

func TestHandleMatrixEditSpacePortalBuildsEditMessageRequest(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.EditMessageRequest
	gc := &GChatClient{
		UserLogin: login,
		editMessageFn: func(_ context.Context, req *pb.EditMessageRequest) (*pb.EditMessageResponse, error) {
			gotReq = req
			return editMessageResponse(5000), nil
		},
	}

	target := &database.Message{
		ID:       gcid.MakeMessageID("msg1"),
		Metadata: &MessageMetadata{TopicID: "msg1"},
	}
	edit := textMatrixEdit(spacePortal("space1"), "edited text", target)

	err := gc.HandleMatrixEdit(context.Background(), edit)
	if err != nil {
		t.Fatalf("HandleMatrixEdit() error = %v, want nil", err)
	}
	if gotReq == nil {
		t.Fatal("editMessageFn was not called")
	}

	if got := gotReq.GetMessageId().GetMessageId(); got != "msg1" {
		t.Errorf("MessageId.MessageId = %q, want %q", got, "msg1")
	}
	if got := gotReq.GetMessageId().GetParentId().GetTopicId().GetTopicId(); got != "msg1" {
		t.Errorf("MessageId.ParentId.TopicId.TopicId = %q, want %q", got, "msg1")
	}
	if got := gotReq.GetMessageId().GetParentId().GetTopicId().GetGroupId().GetSpaceId().GetSpaceId(); got != "space1" {
		t.Errorf("MessageId.ParentId.TopicId.GroupId.SpaceId = %q, want %q", got, "space1")
	}
	if gotReq.GetMessageId().GetParentId().GetTopicId().GetGroupId().GetDmId() != nil {
		t.Error("GroupId.DmId is set for a space portal, want unset")
	}
	if got := gotReq.GetTextBody(); got != "edited text" {
		t.Errorf("TextBody = %q, want %q", got, "edited text")
	}
	if !gotReq.GetMessageInfo().GetAcceptFormatAnnotations() {
		t.Error("MessageInfo.AcceptFormatAnnotations = false, want true")
	}
	if gotReq.GetMessageInfo().GetReplyTo() != nil {
		t.Error("MessageInfo.ReplyTo is set, want nil -- edit_message never sets reply_to")
	}
}

func TestHandleMatrixEditDMPortalBuildsDmGroupID(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.EditMessageRequest
	gc := &GChatClient{
		UserLogin: login,
		editMessageFn: func(_ context.Context, req *pb.EditMessageRequest) (*pb.EditMessageResponse, error) {
			gotReq = req
			return editMessageResponse(1), nil
		},
	}

	target := &database.Message{ID: gcid.MakeMessageID("msg1"), Metadata: &MessageMetadata{TopicID: "msg1"}}
	edit := textMatrixEdit(dmPortal("dm1"), "hi", target)

	if err := gc.HandleMatrixEdit(context.Background(), edit); err != nil {
		t.Fatalf("HandleMatrixEdit() error = %v, want nil", err)
	}
	if got := gotReq.GetMessageId().GetParentId().GetTopicId().GetGroupId().GetDmId().GetDmId(); got != "dm1" {
		t.Errorf("GroupId.DmId = %q, want %q", got, "dm1")
	}
	if gotReq.GetMessageId().GetParentId().GetTopicId().GetGroupId().GetSpaceId() != nil {
		t.Error("GroupId.SpaceId is set for a DM portal, want unset")
	}
}

func TestHandleMatrixEditUsesStoredTopicIDForThreadReply(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.EditMessageRequest
	gc := &GChatClient{
		UserLogin: login,
		editMessageFn: func(_ context.Context, req *pb.EditMessageRequest) (*pb.EditMessageResponse, error) {
			gotReq = req
			return editMessageResponse(1), nil
		},
	}

	target := &database.Message{
		ID:       gcid.MakeMessageID("reply-msg-1"),
		Metadata: &MessageMetadata{TopicID: "topic1"},
	}
	edit := textMatrixEdit(spacePortal("space1"), "edited reply", target)

	if err := gc.HandleMatrixEdit(context.Background(), edit); err != nil {
		t.Fatalf("HandleMatrixEdit() error = %v, want nil", err)
	}
	if got := gotReq.GetMessageId().GetMessageId(); got != "reply-msg-1" {
		t.Errorf("MessageId.MessageId = %q, want %q", got, "reply-msg-1")
	}
	if got := gotReq.GetMessageId().GetParentId().GetTopicId().GetTopicId(); got != "topic1" {
		t.Errorf("MessageId.ParentId.TopicId.TopicId = %q, want %q (the reply's own thread, not its own id)", got, "topic1")
	}
}

func TestHandleMatrixEditFallsBackToOwnIDWhenTopicIDMissing(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.EditMessageRequest
	gc := &GChatClient{
		UserLogin: login,
		editMessageFn: func(_ context.Context, req *pb.EditMessageRequest) (*pb.EditMessageResponse, error) {
			gotReq = req
			return editMessageResponse(1), nil
		},
	}

	target := &database.Message{ID: gcid.MakeMessageID("headmsg1")}
	edit := textMatrixEdit(spacePortal("space1"), "edited", target)

	if err := gc.HandleMatrixEdit(context.Background(), edit); err != nil {
		t.Fatalf("HandleMatrixEdit() error = %v, want nil", err)
	}
	if got := gotReq.GetMessageId().GetParentId().GetTopicId().GetTopicId(); got != "headmsg1" {
		t.Errorf("MessageId.ParentId.TopicId.TopicId = %q, want %q (fallback to the target's own id)", got, "headmsg1")
	}
}

func TestHandleMatrixEditUpdatesLastEditTimeOnSuccess(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	gc := &GChatClient{
		UserLogin: login,
		editMessageFn: func(context.Context, *pb.EditMessageRequest) (*pb.EditMessageResponse, error) {
			return editMessageResponse(9999), nil
		},
	}

	target := &database.Message{
		ID:       gcid.MakeMessageID("msg1"),
		Metadata: &MessageMetadata{TopicID: "msg1", TimestampMicro: 111, LastEditTime: 5000},
	}
	edit := textMatrixEdit(spacePortal("space1"), "edited", target)

	if err := gc.HandleMatrixEdit(context.Background(), edit); err != nil {
		t.Fatalf("HandleMatrixEdit() error = %v, want nil", err)
	}

	meta, ok := target.Metadata.(*MessageMetadata)
	if !ok {
		t.Fatalf("EditTarget.Metadata type = %T, want *MessageMetadata", target.Metadata)
	}
	if meta.LastEditTime != 9999 {
		t.Errorf("Metadata.LastEditTime = %d, want %d", meta.LastEditTime, 9999)
	}
	// An edit must not change TopicID or TimestampMicro.
	if meta.TopicID != "msg1" {
		t.Errorf("Metadata.TopicID = %q, want %q (unaffected by an edit)", meta.TopicID, "msg1")
	}
	if meta.TimestampMicro != 111 {
		t.Errorf("Metadata.TimestampMicro = %d, want %d (unaffected by an edit)", meta.TimestampMicro, 111)
	}
}

func TestHandleMatrixEditNoExistingMetadataStillWorks(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	gc := &GChatClient{
		UserLogin: login,
		editMessageFn: func(context.Context, *pb.EditMessageRequest) (*pb.EditMessageResponse, error) {
			return editMessageResponse(42), nil
		},
	}

	target := &database.Message{ID: gcid.MakeMessageID("msg1")}
	edit := textMatrixEdit(spacePortal("space1"), "edited", target)

	if err := gc.HandleMatrixEdit(context.Background(), edit); err != nil {
		t.Fatalf("HandleMatrixEdit() error = %v, want nil", err)
	}
	meta, ok := target.Metadata.(*MessageMetadata)
	if !ok {
		t.Fatalf("EditTarget.Metadata type = %T, want *MessageMetadata", target.Metadata)
	}
	if meta.LastEditTime != 42 {
		t.Errorf("Metadata.LastEditTime = %d, want %d", meta.LastEditTime, 42)
	}
}

func TestHandleMatrixEditNonTextMsgTypeRejected(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	called := false
	gc := &GChatClient{
		UserLogin: login,
		editMessageFn: func(context.Context, *pb.EditMessageRequest) (*pb.EditMessageResponse, error) {
			called = true
			return editMessageResponse(1), nil
		},
	}

	target := &database.Message{ID: gcid.MakeMessageID("msg1")}
	edit := textMatrixEdit(spacePortal("space1"), "an edited notice", target)
	edit.Content.MsgType = event.MsgNotice

	err := gc.HandleMatrixEdit(context.Background(), edit)
	if !errors.Is(err, bridgev2.ErrUnsupportedMessageType) {
		t.Errorf("error = %v, want bridgev2.ErrUnsupportedMessageType", err)
	}
	if called {
		t.Error("editMessageFn was called for a NOTICE edit, want rejected before the RPC")
	}
}

func TestHandleMatrixEditInvalidPortalIDErrors(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	called := false
	gc := &GChatClient{
		UserLogin: login,
		editMessageFn: func(context.Context, *pb.EditMessageRequest) (*pb.EditMessageResponse, error) {
			called = true
			return editMessageResponse(1), nil
		},
	}

	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: networkid.PortalID("garbage")}}}
	target := &database.Message{ID: gcid.MakeMessageID("msg1")}
	edit := textMatrixEdit(portal, "edited", target)

	err := gc.HandleMatrixEdit(context.Background(), edit)
	if err == nil {
		t.Fatal("HandleMatrixEdit() error = nil, want an error for an unparseable portal id")
	}
	if called {
		t.Error("editMessageFn was called despite an invalid portal id")
	}
}

func TestHandleMatrixEditNoConnNoSeamErrors(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	gc := &GChatClient{UserLogin: login}

	target := &database.Message{ID: gcid.MakeMessageID("msg1")}
	edit := textMatrixEdit(spacePortal("space1"), "edited", target)

	err := gc.HandleMatrixEdit(context.Background(), edit)
	if err == nil {
		t.Fatal("HandleMatrixEdit() error = nil, want an error when not connected")
	}
}

func TestHandleMatrixEditPropagatesRPCError(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	wantErr := errors.New("edit_message: boom")
	gc := &GChatClient{
		UserLogin: login,
		editMessageFn: func(context.Context, *pb.EditMessageRequest) (*pb.EditMessageResponse, error) {
			return nil, wantErr
		},
	}

	target := &database.Message{
		ID:       gcid.MakeMessageID("msg1"),
		Metadata: &MessageMetadata{LastEditTime: 5000},
	}
	edit := textMatrixEdit(spacePortal("space1"), "edited", target)

	err := gc.HandleMatrixEdit(context.Background(), edit)
	if !errors.Is(err, wantErr) {
		t.Errorf("error = %v, want wrapping %v", err, wantErr)
	}
	// A failed RPC must not bump LastEditTime.
	meta := target.Metadata.(*MessageMetadata)
	if meta.LastEditTime != 5000 {
		t.Errorf("Metadata.LastEditTime = %d, want unchanged 5000 after a failed RPC", meta.LastEditTime)
	}
}

func TestHandleMatrixEditFormattedTextBuildsAnnotations(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.EditMessageRequest
	gc := &GChatClient{
		UserLogin: login,
		editMessageFn: func(_ context.Context, req *pb.EditMessageRequest) (*pb.EditMessageResponse, error) {
			gotReq = req
			return editMessageResponse(1), nil
		},
	}

	target := &database.Message{ID: gcid.MakeMessageID("msg1")}
	edit := textMatrixEdit(spacePortal("space1"), "a b c", target)
	edit.Content.Format = event.FormatHTML
	edit.Content.FormattedBody = "a <strong>b</strong> c"

	if err := gc.HandleMatrixEdit(context.Background(), edit); err != nil {
		t.Fatalf("HandleMatrixEdit() error = %v, want nil", err)
	}
	if got := gotReq.GetTextBody(); got != "a b c" {
		t.Errorf("TextBody = %q, want %q", got, "a b c")
	}
	wantAnn := gchatfmt.MakeFormatAnnotation(2, 1, pb.FormatMetadata_BOLD)
	if len(gotReq.GetAnnotations()) != 1 || gotReq.GetAnnotations()[0].String() != wantAnn.String() {
		t.Errorf("Annotations = %v, want [%s]", gotReq.GetAnnotations(), wantAnn.String())
	}
}

func TestHandleMatrixEditMentionPillBecomesMentionAnnotation(t *testing.T) {
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
	var gotReq *pb.EditMessageRequest
	gc := &GChatClient{
		UserLogin: login,
		editMessageFn: func(_ context.Context, req *pb.EditMessageRequest) (*pb.EditMessageResponse, error) {
			gotReq = req
			return editMessageResponse(1), nil
		},
	}

	target := &database.Message{ID: gcid.MakeMessageID("msg1")}
	edit := textMatrixEdit(portal, "plain-text fallback", target)
	edit.Content.Format = event.FormatHTML
	edit.Content.FormattedBody = `Hi <a href="https://matrix.to/#/@200_ghost:example.com">Bob</a>!`

	if err := gc.HandleMatrixEdit(context.Background(), edit); err != nil {
		t.Fatalf("HandleMatrixEdit() error = %v, want nil", err)
	}
	if got := gotReq.GetTextBody(); got != "Hi @Bob!" {
		t.Errorf("TextBody = %q, want %q", got, "Hi @Bob!")
	}
	wantAnn := gchatfmt.MakeMentionAnnotation(3, 4, "200")
	if len(gotReq.GetAnnotations()) != 1 || gotReq.GetAnnotations()[0].String() != wantAnn.String() {
		t.Errorf("Annotations = %v, want [%s]", gotReq.GetAnnotations(), wantAnn.String())
	}
}
