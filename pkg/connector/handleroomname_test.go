package connector

// handleroomname_test.go -- HandleMatrixRoomName -> update_group RPC.
// Mirrors handlereceipt_test.go's request-construction / error-path shape.

import (
	"context"
	"errors"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

func matrixRoomName(portal *bridgev2.Portal, name string) *bridgev2.MatrixRoomName {
	return &bridgev2.MatrixRoomName{
		MatrixEventBase: bridgev2.MatrixEventBase[*event.RoomNameEventContent]{
			Content: &event.RoomNameEventContent{Name: name},
			Portal:  portal,
		},
	}
}

func TestHandleMatrixRoomNameSpaceSendsUpdateGroup(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.UpdateGroupRequest
	gc := &GChatClient{
		UserLogin: login,
		updateGroupFn: func(_ context.Context, req *pb.UpdateGroupRequest) (*pb.UpdateGroupResponse, error) {
			gotReq = req
			return &pb.UpdateGroupResponse{}, nil
		},
	}
	portal := spacePortal("space1")

	ok, err := gc.HandleMatrixRoomName(context.Background(), matrixRoomName(portal, "New Name"))
	if err != nil {
		t.Fatalf("HandleMatrixRoomName() error = %v, want nil", err)
	}
	if !ok {
		t.Error("HandleMatrixRoomName() = false, want true on success")
	}
	if gotReq == nil {
		t.Fatal("updateGroupFn was not called")
	}
	if got := gotReq.GetSpaceId().GetSpaceId(); got != "space1" {
		t.Errorf("space_id = %q, want %q", got, "space1")
	}
	if got := gotReq.GetName(); got != "New Name" {
		t.Errorf("name = %q, want %q", got, "New Name")
	}
	masks := gotReq.GetUpdateMasks()
	if len(masks) != 1 || masks[0] != pb.UpdateGroupRequest_NAME {
		t.Errorf("update_masks = %v, want [NAME]", masks)
	}
	// On success the handler must stamp the portal fields.
	if portal.Name != "New Name" || !portal.NameSet {
		t.Errorf("portal Name=%q NameSet=%v, want %q true", portal.Name, portal.NameSet, "New Name")
	}
}

func TestHandleMatrixRoomNameDMRejected(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	called := false
	gc := &GChatClient{
		UserLogin: login,
		updateGroupFn: func(context.Context, *pb.UpdateGroupRequest) (*pb.UpdateGroupResponse, error) {
			called = true
			return &pb.UpdateGroupResponse{}, nil
		},
	}

	ok, err := gc.HandleMatrixRoomName(context.Background(), matrixRoomName(dmPortal("dm1"), "Nope"))
	if err == nil {
		t.Error("HandleMatrixRoomName() on a DM error = nil, want an error")
	}
	if ok {
		t.Error("HandleMatrixRoomName() on a DM = true, want false")
	}
	if called {
		t.Error("update_group was sent for a DM, want no RPC")
	}
}

func TestHandleMatrixRoomNameFailureLeavesFieldsUntouched(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	gc := &GChatClient{
		UserLogin: login,
		updateGroupFn: func(context.Context, *pb.UpdateGroupRequest) (*pb.UpdateGroupResponse, error) {
			return nil, errors.New("permission denied")
		},
	}
	portal := spacePortal("space1")

	ok, err := gc.HandleMatrixRoomName(context.Background(), matrixRoomName(portal, "New Name"))
	if err == nil {
		t.Error("HandleMatrixRoomName() error = nil, want the RPC error")
	}
	if ok {
		t.Error("HandleMatrixRoomName() = true on RPC failure, want false")
	}
	// Critical: a failed rename must NOT stamp Name/NameSet.
	if portal.Name != "" || portal.NameSet {
		t.Errorf("portal Name=%q NameSet=%v after failure, want \"\" false", portal.Name, portal.NameSet)
	}
}
