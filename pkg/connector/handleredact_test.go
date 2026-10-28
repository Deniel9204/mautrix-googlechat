package connector

import (
	"context"
	"errors"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

func matrixMessageRemove(portal *bridgev2.Portal, target *database.Message) *bridgev2.MatrixMessageRemove {
	return &bridgev2.MatrixMessageRemove{
		MatrixEventBase: bridgev2.MatrixEventBase[*event.RedactionEventContent]{
			Portal:  portal,
			Content: &event.RedactionEventContent{},
		},
		TargetMessage: target,
	}
}

func TestHandleMatrixMessageRemoveSpacePortalBuildsDeleteMessageRequest(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.DeleteMessageRequest
	gc := &GChatClient{
		UserLogin: login,
		deleteMessageFn: func(_ context.Context, req *pb.DeleteMessageRequest) (*pb.DeleteMessageResponse, error) {
			gotReq = req
			return &pb.DeleteMessageResponse{}, nil
		},
	}

	target := &database.Message{
		ID:       gcid.MakeMessageID("msg1"),
		Metadata: &MessageMetadata{TopicID: "msg1"},
	}
	remove := matrixMessageRemove(spacePortal("space1"), target)

	err := gc.HandleMatrixMessageRemove(context.Background(), remove)
	if err != nil {
		t.Fatalf("HandleMatrixMessageRemove() error = %v, want nil", err)
	}
	if gotReq == nil {
		t.Fatal("deleteMessageFn was not called")
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
}

func TestHandleMatrixMessageRemoveDMPortalBuildsDmGroupID(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.DeleteMessageRequest
	gc := &GChatClient{
		UserLogin: login,
		deleteMessageFn: func(_ context.Context, req *pb.DeleteMessageRequest) (*pb.DeleteMessageResponse, error) {
			gotReq = req
			return &pb.DeleteMessageResponse{}, nil
		},
	}

	target := &database.Message{ID: gcid.MakeMessageID("msg1"), Metadata: &MessageMetadata{TopicID: "msg1"}}
	remove := matrixMessageRemove(dmPortal("dm1"), target)

	if err := gc.HandleMatrixMessageRemove(context.Background(), remove); err != nil {
		t.Fatalf("HandleMatrixMessageRemove() error = %v, want nil", err)
	}
	if got := gotReq.GetMessageId().GetParentId().GetTopicId().GetGroupId().GetDmId().GetDmId(); got != "dm1" {
		t.Errorf("GroupId.DmId = %q, want %q", got, "dm1")
	}
	if gotReq.GetMessageId().GetParentId().GetTopicId().GetGroupId().GetSpaceId() != nil {
		t.Error("GroupId.SpaceId is set for a DM portal, want unset")
	}
}

func TestHandleMatrixMessageRemoveUsesStoredTopicIDForThreadReply(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.DeleteMessageRequest
	gc := &GChatClient{
		UserLogin: login,
		deleteMessageFn: func(_ context.Context, req *pb.DeleteMessageRequest) (*pb.DeleteMessageResponse, error) {
			gotReq = req
			return &pb.DeleteMessageResponse{}, nil
		},
	}

	target := &database.Message{
		ID:       gcid.MakeMessageID("reply-msg-1"),
		Metadata: &MessageMetadata{TopicID: "topic1"},
	}
	remove := matrixMessageRemove(spacePortal("space1"), target)

	if err := gc.HandleMatrixMessageRemove(context.Background(), remove); err != nil {
		t.Fatalf("HandleMatrixMessageRemove() error = %v, want nil", err)
	}
	if got := gotReq.GetMessageId().GetMessageId(); got != "reply-msg-1" {
		t.Errorf("MessageId.MessageId = %q, want %q", got, "reply-msg-1")
	}
	if got := gotReq.GetMessageId().GetParentId().GetTopicId().GetTopicId(); got != "topic1" {
		t.Errorf("MessageId.ParentId.TopicId.TopicId = %q, want %q (the reply's own thread, not its own id)", got, "topic1")
	}
}

func TestHandleMatrixMessageRemoveFallsBackToOwnIDWhenTopicIDMissing(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.DeleteMessageRequest
	gc := &GChatClient{
		UserLogin: login,
		deleteMessageFn: func(_ context.Context, req *pb.DeleteMessageRequest) (*pb.DeleteMessageResponse, error) {
			gotReq = req
			return &pb.DeleteMessageResponse{}, nil
		},
	}

	target := &database.Message{ID: gcid.MakeMessageID("headmsg1")}
	remove := matrixMessageRemove(spacePortal("space1"), target)

	if err := gc.HandleMatrixMessageRemove(context.Background(), remove); err != nil {
		t.Fatalf("HandleMatrixMessageRemove() error = %v, want nil", err)
	}
	if got := gotReq.GetMessageId().GetParentId().GetTopicId().GetTopicId(); got != "headmsg1" {
		t.Errorf("MessageId.ParentId.TopicId.TopicId = %q, want %q (fallback to the target's own id)", got, "headmsg1")
	}
	if got := gotReq.GetMessageId().GetMessageId(); got != "headmsg1" {
		t.Errorf("MessageId.MessageId = %q, want %q", got, "headmsg1")
	}
}

func TestHandleMatrixMessageRemoveInvalidPortalIDErrors(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	called := false
	gc := &GChatClient{
		UserLogin: login,
		deleteMessageFn: func(context.Context, *pb.DeleteMessageRequest) (*pb.DeleteMessageResponse, error) {
			called = true
			return &pb.DeleteMessageResponse{}, nil
		},
	}

	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: networkid.PortalID("garbage")}}}
	target := &database.Message{ID: gcid.MakeMessageID("msg1")}
	remove := matrixMessageRemove(portal, target)

	err := gc.HandleMatrixMessageRemove(context.Background(), remove)
	if err == nil {
		t.Fatal("HandleMatrixMessageRemove() error = nil, want an error for an unparseable portal id")
	}
	if called {
		t.Error("deleteMessageFn was called despite an invalid portal id")
	}
}

func TestHandleMatrixMessageRemoveNoConnNoSeamErrors(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	gc := &GChatClient{UserLogin: login}

	target := &database.Message{ID: gcid.MakeMessageID("msg1")}
	remove := matrixMessageRemove(spacePortal("space1"), target)

	err := gc.HandleMatrixMessageRemove(context.Background(), remove)
	if err == nil {
		t.Fatal("HandleMatrixMessageRemove() error = nil, want an error when not connected")
	}
}

func TestHandleMatrixMessageRemovePropagatesRPCError(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	wantErr := errors.New("delete_message: boom")
	gc := &GChatClient{
		UserLogin: login,
		deleteMessageFn: func(context.Context, *pb.DeleteMessageRequest) (*pb.DeleteMessageResponse, error) {
			return nil, wantErr
		},
	}

	target := &database.Message{ID: gcid.MakeMessageID("msg1")}
	remove := matrixMessageRemove(spacePortal("space1"), target)

	err := gc.HandleMatrixMessageRemove(context.Background(), remove)
	if !errors.Is(err, wantErr) {
		t.Errorf("error = %v, want wrapping %v", err, wantErr)
	}
}
