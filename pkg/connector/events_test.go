package connector

// events_test.go -- inbound MESSAGE_POSTED -> bridgev2.RemoteMessage
// (M2 Task 4). Mirrors sync_test.go's queueChatResyncFn capture pattern: a
// bare *GChatClient (no full bridgev2.Bridge+DB harness, see newTestUserLogin
// in client_test.go) with queueRemoteEventFn overridden to append into a
// local slice instead of dereferencing UserLogin.Bridge.

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// messagePostedEvent builds a *pb.Event shaped like a real MESSAGE_POSTED
// event reaching handleGChatEvent after gchatmeow's splitEventBodies has
// already copied the parent's group_id/type onto it (see
// pkg/gchatmeow/client_test.go's TestSplitEventBodies) -- i.e. exactly the
// per-event shape this package's handler receives, not the raw multi-body
// wire frame.
func messagePostedEvent(groupID *pb.GroupId, gcMessageID, creatorGaia, text string, createTimeMicros int64) *pb.Event {
	return &pb.Event{
		GroupId: groupID,
		Type:    pb.Event_MESSAGE_POSTED.Enum(),
		Body: &pb.Event_EventBody{
			EventType: pb.Event_MESSAGE_POSTED.Enum(),
			Type: &pb.Event_EventBody_MessagePosted{
				MessagePosted: &pb.MessageEvent{
					Message: &pb.Message{
						Id:         &pb.MessageId{MessageId: proto.String(gcMessageID)},
						Creator:    &pb.User{UserId: &pb.UserId{Id: proto.String(creatorGaia)}},
						CreateTime: proto.Int64(createTimeMicros),
						TextBody:   proto.String(text),
					},
				},
			},
		},
	}
}

// dmGroupID / spaceGroupID are defined in chatinfo_test.go.

// newEventTestClient builds a *GChatClient wired with a capturing
// queueRemoteEventFn, matching sync_test.go's queueChatResyncFn seam so
// handleGChatEvent can be exercised without a live bridgev2.Bridge+DB
// harness (UserLogin.QueueRemoteEvent would otherwise dereference
// UserLogin.Bridge, which is nil for newTestUserLogin's lightweight
// UserLogin -- client_test.go).
func newEventTestClient(ownGaia string) (*GChatClient, *[]bridgev2.RemoteEvent) {
	login := newTestUserLogin(&UserLoginMetadata{})
	login.ID = gcid.MakeUserLoginID(ownGaia)
	var queued []bridgev2.RemoteEvent
	gc := &GChatClient{
		UserLogin: login,
		queueRemoteEventFn: func(evt bridgev2.RemoteEvent) bridgev2.EventHandlingResult {
			queued = append(queued, evt)
			return bridgev2.EventHandlingResultQueued
		},
	}
	return gc, &queued
}

// --- MESSAGE_POSTED: space room --------------------------------------------

func TestHandleGChatEventMessagePostedSpaceQueuesRemoteMessage(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := messagePostedEvent(spaceGroupID("space-1"), "msg-1", "98765", "hello world", 1700000000123456)

	gc.handleGChatEvent(context.Background(), evt)

	if len(*queued) != 1 {
		t.Fatalf("len(queued) = %d, want 1", len(*queued))
	}
	msg, ok := (*queued)[0].(bridgev2.RemoteMessage)
	if !ok {
		t.Fatalf("queued event does not implement bridgev2.RemoteMessage: %T", (*queued)[0])
	}

	if got, want := msg.GetID(), gcid.MakeMessageID("msg-1"); got != want {
		t.Errorf("GetID() = %q, want %q", got, want)
	}

	wantPortalKey := gcid.MakePortalKey(gcid.GroupID{ID: "space-1", IsDM: false}, gc.UserLogin.ID)
	if got := msg.GetPortalKey(); got != wantPortalKey {
		t.Errorf("GetPortalKey() = %+v, want %+v", got, wantPortalKey)
	}
	if wantPortalKey.Receiver != "" {
		t.Fatalf("test setup: space portal key must have empty Receiver, got %q", wantPortalKey.Receiver)
	}

	sender := msg.GetSender()
	if got, want := sender.Sender, gcid.MakeUserID("98765"); got != want {
		t.Errorf("GetSender().Sender = %q, want %q", got, want)
	}
	if sender.IsFromMe {
		t.Error("GetSender().IsFromMe = true, want false (creator is not the login's own gaia)")
	}

	createPortal, ok := (*queued)[0].(bridgev2.RemoteEventThatMayCreatePortal)
	if !ok || !createPortal.ShouldCreatePortal() {
		t.Error("ShouldCreatePortal() = false, want true")
	}

	cm, err := msg.ConvertMessage(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("ConvertMessage: %v", err)
	}
	if len(cm.Parts) != 1 {
		t.Fatalf("len(cm.Parts) = %d, want 1", len(cm.Parts))
	}
	if got := cm.Parts[0].Content.Body; got != "hello world" {
		t.Errorf("part body = %q, want %q", got, "hello world")
	}
	if got := cm.Parts[0].ID; got != gcid.TextPartID {
		t.Errorf("part ID = %q, want gcid.TextPartID (empty)", got)
	}
	meta, ok := cm.Parts[0].DBMetadata.(*MessageMetadata)
	if !ok {
		t.Fatalf("part DBMetadata = %T, want *MessageMetadata", cm.Parts[0].DBMetadata)
	}
	if meta.TimestampMicro != 1700000000123456 {
		t.Errorf("TimestampMicro = %d, want 1700000000123456", meta.TimestampMicro)
	}
}

// --- MESSAGE_POSTED: DM room -- portal key is receiver-scoped --------------

func TestHandleGChatEventMessagePostedDMPortalKeyHasReceiver(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := messagePostedEvent(dmGroupID("dm-1"), "msg-2", "98765", "hi", 1)

	gc.handleGChatEvent(context.Background(), evt)

	if len(*queued) != 1 {
		t.Fatalf("len(queued) = %d, want 1", len(*queued))
	}
	msg := (*queued)[0].(bridgev2.RemoteMessage)
	wantPortalKey := gcid.MakePortalKey(gcid.GroupID{ID: "dm-1", IsDM: true}, gc.UserLogin.ID)
	if got := msg.GetPortalKey(); got != wantPortalKey {
		t.Errorf("GetPortalKey() = %+v, want %+v", got, wantPortalKey)
	}
	if wantPortalKey.Receiver != gc.UserLogin.ID {
		t.Fatalf("test setup: DM portal key must be receiver-scoped, got Receiver=%q", wantPortalKey.Receiver)
	}
}

// --- MESSAGE_POSTED: own message sets IsFromMe -----------------------------

func TestHandleGChatEventMessagePostedOwnMessageSetsIsFromMe(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := messagePostedEvent(spaceGroupID("space-1"), "msg-3", "112233", "echo", 1)

	gc.handleGChatEvent(context.Background(), evt)

	if len(*queued) != 1 {
		t.Fatalf("len(queued) = %d, want 1", len(*queued))
	}
	sender := (*queued)[0].(bridgev2.RemoteMessage).GetSender()
	if !sender.IsFromMe {
		t.Error("GetSender().IsFromMe = false, want true (creator gaia == login's own gaia)")
	}
	if got, want := sender.Sender, gcid.MakeUserID("112233"); got != want {
		t.Errorf("GetSender().Sender = %q, want %q (Sender is always set, even for own messages)", got, want)
	}
}

// --- MESSAGE_POSTED: malformed events are skipped, not queued or panicked --

func TestHandleGChatEventMessagePostedNoGroupIDSkipped(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := messagePostedEvent(nil, "msg-4", "98765", "hi", 1)

	gc.handleGChatEvent(context.Background(), evt)

	if len(*queued) != 0 {
		t.Fatalf("len(queued) = %d, want 0 (no usable group id)", len(*queued))
	}
}

func TestHandleGChatEventMessagePostedNoMessageIDSkipped(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := messagePostedEvent(spaceGroupID("space-1"), "", "98765", "hi", 1)

	gc.handleGChatEvent(context.Background(), evt)

	if len(*queued) != 0 {
		t.Fatalf("len(queued) = %d, want 0 (no message id)", len(*queued))
	}
}

func TestHandleGChatEventMessagePostedNilMessageSkipped(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := &pb.Event{
		GroupId: spaceGroupID("space-1"),
		Type:    pb.Event_MESSAGE_POSTED.Enum(),
		Body: &pb.Event_EventBody{
			EventType: pb.Event_MESSAGE_POSTED.Enum(),
			Type:      &pb.Event_EventBody_MessagePosted{MessagePosted: &pb.MessageEvent{}},
		},
	}

	gc.handleGChatEvent(context.Background(), evt)

	if len(*queued) != 0 {
		t.Fatalf("len(queued) = %d, want 0 (no message payload)", len(*queued))
	}
}

// --- MESSAGE_UPDATED (edit): body is the same message_posted arm, but the
// outer event type is MESSAGE_UPDATED -- M4's territory, must stay a no-op
// here rather than being queued as a brand new message. -----------------

func TestHandleGChatEventMessageUpdatedNotQueuedYet(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := messagePostedEvent(spaceGroupID("space-1"), "msg-5", "98765", "edited text", 1)
	evt.Type = pb.Event_MESSAGE_UPDATED.Enum()

	gc.handleGChatEvent(context.Background(), evt)

	if len(*queued) != 0 {
		t.Fatalf("len(queued) = %d, want 0 (MESSAGE_UPDATED/edits are M4)", len(*queued))
	}
}
