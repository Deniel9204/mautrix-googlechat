package connector

// handleroomtopic_test.go -- HandleMatrixRoomTopic -> get_group + update_group
// (SPACE_DETAILS). Mirrors handleroomname_test.go.

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

func matrixRoomTopic(portal *bridgev2.Portal, topic string) *bridgev2.MatrixRoomTopic {
	return &bridgev2.MatrixRoomTopic{
		MatrixEventBase: bridgev2.MatrixEventBase[*event.TopicEventContent]{
			Content: &event.TopicEventContent{Topic: topic},
			Portal:  portal,
		},
	}
}

func groupWithDetails(details *pb.GroupDetails) func(context.Context, *pb.GetGroupRequest) (*pb.GetGroupResponse, error) {
	return func(context.Context, *pb.GetGroupRequest) (*pb.GetGroupResponse, error) {
		return &pb.GetGroupResponse{Group: &pb.Group{GroupDetails: details}}, nil
	}
}

func TestHandleMatrixRoomTopicKeepsGuidelines(t *testing.T) {
	var gotReq *pb.UpdateGroupRequest
	gc := &GChatClient{
		UserLogin:  newTestUserLogin(&UserLoginMetadata{}),
		getGroupFn: groupWithDetails(&pb.GroupDetails{Description: proto.String("old"), Guidelines: proto.String("be nice")}),
		updateGroupFn: func(_ context.Context, req *pb.UpdateGroupRequest) (*pb.UpdateGroupResponse, error) {
			gotReq = req
			return &pb.UpdateGroupResponse{}, nil
		},
	}
	portal := spacePortal("space1")

	ok, err := gc.HandleMatrixRoomTopic(context.Background(), matrixRoomTopic(portal, "new topic"))
	if err != nil || !ok {
		t.Fatalf("HandleMatrixRoomTopic() = %v, %v; want true, nil", ok, err)
	}
	if gotReq == nil {
		t.Fatal("update_group was not sent")
	}
	if got := gotReq.GetSpaceId().GetSpaceId(); got != "space1" {
		t.Errorf("space_id = %q, want space1", got)
	}
	if masks := gotReq.GetUpdateMasks(); len(masks) != 1 || masks[0] != pb.UpdateGroupRequest_SPACE_DETAILS {
		t.Errorf("update_masks = %v, want [SPACE_DETAILS]", masks)
	}
	if gotReq.Name != nil {
		t.Error("name set on a topic change; only the details may change")
	}
	d := gotReq.GetSpaceDetails()
	if d.GetDescription() != "new topic" {
		t.Errorf("description = %q, want %q", d.GetDescription(), "new topic")
	}
	if d.GetGuidelines() != "be nice" {
		t.Errorf("guidelines = %q, want the existing %q sent back unchanged", d.GetGuidelines(), "be nice")
	}
	if portal.Topic != "new topic" || !portal.TopicSet {
		t.Errorf("portal Topic = %q TopicSet = %v, want updated", portal.Topic, portal.TopicSet)
	}
}

// Absent guidelines must stay absent (proto2 presence), not become "".
func TestHandleMatrixRoomTopicNoGuidelinesStaysAbsent(t *testing.T) {
	var gotReq *pb.UpdateGroupRequest
	gc := &GChatClient{
		UserLogin:  newTestUserLogin(&UserLoginMetadata{}),
		getGroupFn: groupWithDetails(nil),
		updateGroupFn: func(_ context.Context, req *pb.UpdateGroupRequest) (*pb.UpdateGroupResponse, error) {
			gotReq = req
			return &pb.UpdateGroupResponse{}, nil
		},
	}

	if _, err := gc.HandleMatrixRoomTopic(context.Background(), matrixRoomTopic(spacePortal("space1"), "")); err != nil {
		t.Fatalf("HandleMatrixRoomTopic() error = %v", err)
	}
	if gotReq.GetSpaceDetails().Guidelines != nil {
		t.Error("guidelines present, want absent")
	}
	if gotReq.GetSpaceDetails().Description == nil {
		t.Error("description absent for a cleared topic, want an explicit empty description")
	}
}

func TestHandleMatrixRoomTopicRefusesWhenDetailsUnreadable(t *testing.T) {
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		getGroupFn: func(context.Context, *pb.GetGroupRequest) (*pb.GetGroupResponse, error) {
			return nil, errors.New("403")
		},
		updateGroupFn: func(context.Context, *pb.UpdateGroupRequest) (*pb.UpdateGroupResponse, error) {
			t.Error("update_group sent without knowing the current guidelines")
			return &pb.UpdateGroupResponse{}, nil
		},
	}
	portal := spacePortal("space1")

	if ok, err := gc.HandleMatrixRoomTopic(context.Background(), matrixRoomTopic(portal, "x")); err == nil || ok {
		t.Errorf("HandleMatrixRoomTopic() = %v, %v; want false and an error", ok, err)
	}
	if portal.TopicSet {
		t.Error("portal topic updated although nothing was sent")
	}
}

func TestHandleMatrixRoomTopicUpdateFailureLeavesPortal(t *testing.T) {
	gc := &GChatClient{
		UserLogin:  newTestUserLogin(&UserLoginMetadata{}),
		getGroupFn: groupWithDetails(nil),
		updateGroupFn: func(context.Context, *pb.UpdateGroupRequest) (*pb.UpdateGroupResponse, error) {
			return nil, errors.New("permission denied")
		},
	}
	portal := spacePortal("space1")

	if ok, err := gc.HandleMatrixRoomTopic(context.Background(), matrixRoomTopic(portal, "x")); err == nil || ok {
		t.Errorf("HandleMatrixRoomTopic() = %v, %v; want false and an error", ok, err)
	}
	if portal.TopicSet {
		t.Error("portal topic updated although update_group failed")
	}
}

func TestHandleMatrixRoomTopicDMRejected(t *testing.T) {
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		getGroupFn: func(context.Context, *pb.GetGroupRequest) (*pb.GetGroupResponse, error) {
			t.Error("get_group sent for a DM")
			return &pb.GetGroupResponse{}, nil
		},
	}
	if _, err := gc.HandleMatrixRoomTopic(context.Background(), matrixRoomTopic(dmPortal("dm1"), "x")); err == nil {
		t.Error("HandleMatrixRoomTopic(DM) = nil error, want a rejection")
	}
}

// A DM must not advertise the room state a space can change: the handlers
// reject it there, so a capability-aware client would offer a dead button.
func TestGetCapabilitiesDMAdvertisesNoRoomState(t *testing.T) {
	for _, meta := range []*PortalMetadata{{}, {ThreadsEnabled: true}} {
		caps := (&GChatClient{}).GetCapabilities(context.Background(), portalWithIDAndMeta("dm1", true, meta))
		if len(caps.State) != 0 {
			t.Errorf("DM (meta %+v) State = %v, want none", meta, caps.State)
		}
	}
}
