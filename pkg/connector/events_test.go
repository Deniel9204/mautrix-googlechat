package connector

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// messagePostedEvent builds a per-body event as produced by splitEventBodies.
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

// newEventTestClient returns a client whose queued remote events are captured.
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

func TestHandleGChatEventMessagePostedSetsStreamOrder(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := messagePostedEvent(spaceGroupID("space-1"), "msg-so", "98765", "hi", 1_000_000)

	gc.handleGChatEvent(context.Background(), evt)

	if len(*queued) != 1 {
		t.Fatalf("len(queued) = %d, want 1", len(*queued))
	}
	streamOrdered, ok := (*queued)[0].(bridgev2.RemoteEventWithStreamOrder)
	if !ok {
		t.Fatalf("queued event does not implement bridgev2.RemoteEventWithStreamOrder: %T", (*queued)[0])
	}
	if got := streamOrdered.GetStreamOrder(); got != 1_000_000 {
		t.Errorf("GetStreamOrder() = %d, want 1000000 (create_time in µs)", got)
	}
}

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

func messageUpdatedEvent(groupID *pb.GroupId, gcMessageID, creatorGaia, text string, lastEditTime int64) *pb.Event {
	evt := messagePostedEvent(groupID, gcMessageID, creatorGaia, text, 1)
	evt.Type = pb.Event_MESSAGE_UPDATED.Enum()
	evt.GetBody().GetMessagePosted().GetMessage().LastEditTime = proto.Int64(lastEditTime)
	return evt
}

func TestHandleGChatEventMessageUpdatedQueuesRemoteEdit(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := messageUpdatedEvent(spaceGroupID("space-1"), "msg-5", "98765", "edited text", 5000)

	gc.handleGChatEvent(context.Background(), evt)

	if len(*queued) != 1 {
		t.Fatalf("len(queued) = %d, want 1", len(*queued))
	}
	edit, ok := (*queued)[0].(bridgev2.RemoteEdit)
	if !ok {
		t.Fatalf("queued event does not implement bridgev2.RemoteEdit: %T", (*queued)[0])
	}
	if got, want := edit.GetTargetMessage(), gcid.MakeMessageID("msg-5"); got != want {
		t.Errorf("GetTargetMessage() = %q, want %q", got, want)
	}
	if got := edit.GetType(); got != bridgev2.RemoteEventEdit {
		t.Errorf("GetType() = %v, want RemoteEventEdit", got)
	}

	wantPortalKey := gcid.MakePortalKey(gcid.GroupID{ID: "space-1", IsDM: false}, gc.UserLogin.ID)
	if got := edit.GetPortalKey(); got != wantPortalKey {
		t.Errorf("GetPortalKey() = %+v, want %+v", got, wantPortalKey)
	}

	sender := edit.GetSender()
	if got, want := sender.Sender, gcid.MakeUserID("98765"); got != want {
		t.Errorf("GetSender().Sender = %q, want %q", got, want)
	}

	existing := []*database.Message{{
		ID:       gcid.MakeMessageID("msg-5"),
		Metadata: &MessageMetadata{LastEditTime: 0},
	}}
	converted, err := edit.ConvertEdit(context.Background(), nil, nil, existing)
	if err != nil {
		t.Fatalf("ConvertEdit: %v", err)
	}
	if len(converted.ModifiedParts) != 1 {
		t.Fatalf("len(ModifiedParts) = %d, want 1", len(converted.ModifiedParts))
	}
	if got := converted.ModifiedParts[0].Content.Body; got != "edited text" {
		t.Errorf("ModifiedParts[0].Content.Body = %q, want %q", got, "edited text")
	}
	if got := existing[0].Metadata.(*MessageMetadata).LastEditTime; got != 5000 {
		t.Errorf("LastEditTime = %d, want 5000 (updated by ConvertEdit)", got)
	}
}

func TestHandleGChatEventMessageUpdatedSetsStreamOrder(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := messageUpdatedEvent(spaceGroupID("space-1"), "msg-so-edit", "98765", "edited", 5000)

	gc.handleGChatEvent(context.Background(), evt)

	if len(*queued) != 1 {
		t.Fatalf("len(queued) = %d, want 1", len(*queued))
	}
	streamOrdered, ok := (*queued)[0].(bridgev2.RemoteEventWithStreamOrder)
	if !ok {
		t.Fatalf("queued event does not implement bridgev2.RemoteEventWithStreamOrder: %T", (*queued)[0])
	}
	if got := streamOrdered.GetStreamOrder(); got != 5000 {
		t.Errorf("GetStreamOrder() = %d, want 5000 (last_edit_time in µs)", got)
	}
}

func TestHandleGChatEventMessageUpdatedDMPortalKeyHasReceiver(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := messageUpdatedEvent(dmGroupID("dm-1"), "msg-6", "98765", "edited", 5000)

	gc.handleGChatEvent(context.Background(), evt)

	if len(*queued) != 1 {
		t.Fatalf("len(queued) = %d, want 1", len(*queued))
	}
	edit := (*queued)[0].(bridgev2.RemoteEdit)
	wantPortalKey := gcid.MakePortalKey(gcid.GroupID{ID: "dm-1", IsDM: true}, gc.UserLogin.ID)
	if got := edit.GetPortalKey(); got != wantPortalKey {
		t.Errorf("GetPortalKey() = %+v, want %+v", got, wantPortalKey)
	}
	if wantPortalKey.Receiver != gc.UserLogin.ID {
		t.Fatalf("test setup: DM portal key must be receiver-scoped, got Receiver=%q", wantPortalKey.Receiver)
	}
}

func TestHandleGChatEventMessageUpdatedDedupSkipsDuplicate(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := messageUpdatedEvent(spaceGroupID("space-1"), "msg-7", "98765", "duplicate", 5000)

	gc.handleGChatEvent(context.Background(), evt)

	edit := (*queued)[0].(bridgev2.RemoteEdit)
	existing := []*database.Message{{
		ID:       gcid.MakeMessageID("msg-7"),
		Metadata: &MessageMetadata{LastEditTime: 5000},
	}}
	_, err := edit.ConvertEdit(context.Background(), nil, nil, existing)
	if !errors.Is(err, bridgev2.ErrIgnoringRemoteEvent) {
		t.Errorf("ConvertEdit error = %v, want wrapping bridgev2.ErrIgnoringRemoteEvent", err)
	}
	if got := existing[0].Metadata.(*MessageMetadata).LastEditTime; got != 5000 {
		t.Errorf("LastEditTime = %d, want unchanged 5000", got)
	}
}

func TestHandleGChatEventMessageUpdatedNoGroupIDSkipped(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := messageUpdatedEvent(nil, "msg-8", "98765", "hi", 5000)

	gc.handleGChatEvent(context.Background(), evt)

	if len(*queued) != 0 {
		t.Fatalf("len(queued) = %d, want 0 (no usable group id)", len(*queued))
	}
}

func TestHandleGChatEventMessageUpdatedNoMessageIDSkipped(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := messageUpdatedEvent(spaceGroupID("space-1"), "", "98765", "hi", 5000)

	gc.handleGChatEvent(context.Background(), evt)

	if len(*queued) != 0 {
		t.Fatalf("len(queued) = %d, want 0 (no message id)", len(*queued))
	}
}

// TestHandleGChatEventMessagePostedOnHoldTypesAreQueued: any type but MESSAGE_UPDATED is a new message.
func TestHandleGChatEventMessagePostedOnHoldTypesAreQueued(t *testing.T) {
	onHoldTypes := []pb.Event_EventType{
		pb.Event_ON_HOLD_MESSAGE_POSTED,
		pb.Event_ON_HOLD_MESSAGE_UPDATED,
		pb.Event_ON_HOLD_MESSAGE_PUBLISHED,
	}
	for _, et := range onHoldTypes {
		t.Run(et.String(), func(t *testing.T) {
			gc, queued := newEventTestClient("112233")
			evt := messagePostedEvent(spaceGroupID("space-1"), "msg-on-hold", "98765", "held message", 1)
			evt.Type = et.Enum()

			gc.handleGChatEvent(context.Background(), evt)

			if len(*queued) != 1 {
				t.Fatalf("len(queued) = %d, want 1 (event type %v carries message_posted and is not MESSAGE_UPDATED, so the else branch bridges it as a new message)", len(*queued), et)
			}
		})
	}
}

func messageDeletedEvent(groupID *pb.GroupId, gcMessageID string, timestampMicros int64) *pb.Event {
	return &pb.Event{
		GroupId: groupID,
		Type:    pb.Event_MESSAGE_DELETED.Enum(),
		Body: &pb.Event_EventBody{
			EventType: pb.Event_MESSAGE_DELETED.Enum(),
			Type: &pb.Event_EventBody_MessageDeleted{
				MessageDeleted: &pb.MessageDeletedEvent{
					MessageId: &pb.MessageId{MessageId: proto.String(gcMessageID)},
					Timestamp: proto.Int64(timestampMicros),
				},
			},
		},
	}
}

func TestHandleGChatEventMessageDeletedSpaceQueuesRemoteMessageRemove(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := messageDeletedEvent(spaceGroupID("space-1"), "msg-1", 1700000000123456)

	res := gc.handleGChatEvent(context.Background(), evt)

	if !res.Success {
		t.Fatalf("handleGChatEvent() result = %+v, want Success", res)
	}
	if len(*queued) != 1 {
		t.Fatalf("len(queued) = %d, want 1", len(*queued))
	}
	remove, ok := (*queued)[0].(bridgev2.RemoteMessageRemove)
	if !ok {
		t.Fatalf("queued event does not implement bridgev2.RemoteMessageRemove: %T", (*queued)[0])
	}
	if got, want := remove.GetTargetMessage(), gcid.MakeMessageID("msg-1"); got != want {
		t.Errorf("GetTargetMessage() = %q, want %q", got, want)
	}
	if got := remove.GetType(); got != bridgev2.RemoteEventMessageRemove {
		t.Errorf("GetType() = %v, want RemoteEventMessageRemove", got)
	}

	wantPortalKey := gcid.MakePortalKey(gcid.GroupID{ID: "space-1", IsDM: false}, gc.UserLogin.ID)
	if got := remove.GetPortalKey(); got != wantPortalKey {
		t.Errorf("GetPortalKey() = %+v, want %+v", got, wantPortalKey)
	}
}

func TestHandleGChatEventMessageDeletedDMPortalKeyHasReceiver(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := messageDeletedEvent(dmGroupID("dm-1"), "msg-2", 1)

	gc.handleGChatEvent(context.Background(), evt)

	if len(*queued) != 1 {
		t.Fatalf("len(queued) = %d, want 1", len(*queued))
	}
	remove := (*queued)[0].(bridgev2.RemoteMessageRemove)
	wantPortalKey := gcid.MakePortalKey(gcid.GroupID{ID: "dm-1", IsDM: true}, gc.UserLogin.ID)
	if got := remove.GetPortalKey(); got != wantPortalKey {
		t.Errorf("GetPortalKey() = %+v, want %+v", got, wantPortalKey)
	}
	if wantPortalKey.Receiver != gc.UserLogin.ID {
		t.Fatalf("test setup: DM portal key must be receiver-scoped, got Receiver=%q", wantPortalKey.Receiver)
	}
}

// TestHandleGChatEventMessageDeletedThreadedTargetUsesSameMessageID: inbound deletes need no topic id.
func TestHandleGChatEventMessageDeletedThreadedTargetUsesSameMessageID(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := messageDeletedEvent(spaceGroupID("space-1"), "reply-msg-9", 1)

	gc.handleGChatEvent(context.Background(), evt)

	if len(*queued) != 1 {
		t.Fatalf("len(queued) = %d, want 1", len(*queued))
	}
	remove := (*queued)[0].(bridgev2.RemoteMessageRemove)
	if got, want := remove.GetTargetMessage(), gcid.MakeMessageID("reply-msg-9"); got != want {
		t.Errorf("GetTargetMessage() = %q, want %q", got, want)
	}
}

func TestHandleGChatEventMessageDeletedNoGroupIDSkipped(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := messageDeletedEvent(nil, "msg-3", 1)

	res := gc.handleGChatEvent(context.Background(), evt)

	if !res.Success {
		t.Fatalf("handleGChatEvent() result = %+v, want Success (Ignored, so the watermark still advances past the garbage)", res)
	}
	if len(*queued) != 0 {
		t.Fatalf("len(queued) = %d, want 0 (no usable group id)", len(*queued))
	}
}

func TestHandleGChatEventMessageDeletedNoMessageIDSkipped(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := messageDeletedEvent(spaceGroupID("space-1"), "", 1)

	gc.handleGChatEvent(context.Background(), evt)

	if len(*queued) != 0 {
		t.Fatalf("len(queued) = %d, want 0 (no message id)", len(*queued))
	}
}

func messageReactionEvent(groupID *pb.GroupId, gcMessageID, senderGaia, emojiUnicode string, reactionType pb.MessageReactionEvent_ReactionEventType, timestampMicros int64) *pb.Event {
	return &pb.Event{
		GroupId: groupID,
		Type:    pb.Event_MESSAGE_REACTED.Enum(),
		Body: &pb.Event_EventBody{
			EventType: pb.Event_MESSAGE_REACTED.Enum(),
			Type: &pb.Event_EventBody_MessageReaction{
				MessageReaction: &pb.MessageReactionEvent{
					MessageId: &pb.MessageId{MessageId: proto.String(gcMessageID)},
					Emoji:     &pb.Emoji{Content: &pb.Emoji_Unicode{Unicode: emojiUnicode}},
					UserId:    &pb.UserId{Id: proto.String(senderGaia)},
					Timestamp: proto.Int64(timestampMicros),
					Type:      reactionType.Enum(),
				},
			},
		},
	}
}

func TestHandleGChatEventMessageReactionAddQueuesRemoteReaction(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := messageReactionEvent(spaceGroupID("space-1"), "msg-1", "98765", "❤", pb.MessageReactionEvent_ADD, 1700000000123456)

	res := gc.handleGChatEvent(context.Background(), evt)

	if !res.Success {
		t.Fatalf("handleGChatEvent() result = %+v, want Success", res)
	}
	if len(*queued) != 1 {
		t.Fatalf("len(queued) = %d, want 1", len(*queued))
	}
	reaction, ok := (*queued)[0].(bridgev2.RemoteReaction)
	if !ok {
		t.Fatalf("queued event does not implement bridgev2.RemoteReaction: %T", (*queued)[0])
	}
	if got, want := reaction.GetTargetMessage(), gcid.MakeMessageID("msg-1"); got != want {
		t.Errorf("GetTargetMessage() = %q, want %q", got, want)
	}
	typed := (*queued)[0].(bridgev2.RemoteEvent)
	if got := typed.GetType(); got != bridgev2.RemoteEventReaction {
		t.Errorf("GetType() = %v, want RemoteEventReaction", got)
	}

	wantPortalKey := gcid.MakePortalKey(gcid.GroupID{ID: "space-1", IsDM: false}, gc.UserLogin.ID)
	if got := typed.GetPortalKey(); got != wantPortalKey {
		t.Errorf("GetPortalKey() = %+v, want %+v", got, wantPortalKey)
	}

	sender := typed.GetSender()
	if got, want := sender.Sender, gcid.MakeUserID("98765"); got != want {
		t.Errorf("GetSender().Sender = %q, want %q", got, want)
	}
	if sender.IsFromMe {
		t.Error("GetSender().IsFromMe = true, want false (reactor is not the login's own gaia)")
	}

	tsProvider, ok := (*queued)[0].(bridgev2.RemoteEventWithTimestamp)
	if !ok {
		t.Fatalf("queued event does not implement bridgev2.RemoteEventWithTimestamp: %T", (*queued)[0])
	}
	wantTS := gchatmeow.MicrosToTime(1700000000123456)
	if got := tsProvider.GetTimestamp(); !got.Equal(wantTS) {
		t.Errorf("GetTimestamp() = %v, want %v (evt.timestamp is microseconds)", got, wantTS)
	}

	emoji, emojiID := reaction.GetReactionEmoji()
	if emojiID != networkid.EmojiID("❤") {
		t.Errorf("EmojiID = %q, want %q (bare, for per-emoji dedup)", emojiID, "❤")
	}
	wantMatrixEmoji := "❤️" // variation selector added back toward Matrix
	if emoji != wantMatrixEmoji {
		t.Errorf("Emoji = %q, want %q (variation selector added toward Matrix)", emoji, wantMatrixEmoji)
	}
}

func TestHandleGChatEventMessageReactionAddNormalizesEmojiAlreadyHavingSelector(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := messageReactionEvent(spaceGroupID("space-1"), "msg-1", "98765", "❤️", pb.MessageReactionEvent_ADD, 1)

	gc.handleGChatEvent(context.Background(), evt)

	reaction := (*queued)[0].(bridgev2.RemoteReaction)
	_, emojiID := reaction.GetReactionEmoji()
	if emojiID != networkid.EmojiID("❤") {
		t.Errorf("EmojiID = %q, want %q (selector stripped)", emojiID, "❤")
	}
}

func TestHandleGChatEventMessageReactionRemoveQueuesRemoteReactionRemove(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := messageReactionEvent(spaceGroupID("space-1"), "msg-2", "98765", "\U0001F44D", pb.MessageReactionEvent_REMOVE, 1)

	res := gc.handleGChatEvent(context.Background(), evt)

	if !res.Success {
		t.Fatalf("handleGChatEvent() result = %+v, want Success", res)
	}
	if len(*queued) != 1 {
		t.Fatalf("len(queued) = %d, want 1", len(*queued))
	}
	removeEvt, ok := (*queued)[0].(bridgev2.RemoteReactionRemove)
	if !ok {
		t.Fatalf("queued event does not implement bridgev2.RemoteReactionRemove: %T", (*queued)[0])
	}
	if got, want := removeEvt.GetRemovedEmojiID(), networkid.EmojiID("\U0001F44D"); got != want {
		t.Errorf("GetRemovedEmojiID() = %q, want %q", got, want)
	}
	typed := (*queued)[0].(bridgev2.RemoteEvent)
	if got := typed.GetType(); got != bridgev2.RemoteEventReactionRemove {
		t.Errorf("GetType() = %v, want RemoteEventReactionRemove", got)
	}
}

func TestHandleGChatEventMessageReactionDMPortalKeyHasReceiver(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := messageReactionEvent(dmGroupID("dm-1"), "msg-3", "98765", "❤", pb.MessageReactionEvent_ADD, 1)

	gc.handleGChatEvent(context.Background(), evt)

	if len(*queued) != 1 {
		t.Fatalf("len(queued) = %d, want 1", len(*queued))
	}
	typed := (*queued)[0].(bridgev2.RemoteEvent)
	wantPortalKey := gcid.MakePortalKey(gcid.GroupID{ID: "dm-1", IsDM: true}, gc.UserLogin.ID)
	if got := typed.GetPortalKey(); got != wantPortalKey {
		t.Errorf("GetPortalKey() = %+v, want %+v", got, wantPortalKey)
	}
	if wantPortalKey.Receiver != gc.UserLogin.ID {
		t.Fatalf("test setup: DM portal key must be receiver-scoped, got Receiver=%q", wantPortalKey.Receiver)
	}
}

func TestHandleGChatEventMessageReactionOwnReactionSetsIsFromMe(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := messageReactionEvent(spaceGroupID("space-1"), "msg-4", "112233", "❤", pb.MessageReactionEvent_ADD, 1)

	gc.handleGChatEvent(context.Background(), evt)

	typed := (*queued)[0].(bridgev2.RemoteEvent)
	sender := typed.GetSender()
	if !sender.IsFromMe {
		t.Error("GetSender().IsFromMe = false, want true (reactor gaia == login's own gaia)")
	}
}

func TestHandleGChatEventMessageReactionNoGroupIDSkipped(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := messageReactionEvent(nil, "msg-5", "98765", "❤", pb.MessageReactionEvent_ADD, 1)

	res := gc.handleGChatEvent(context.Background(), evt)

	if !res.Success {
		t.Fatalf("handleGChatEvent() result = %+v, want Success (Ignored, so the watermark still advances past the garbage)", res)
	}
	if len(*queued) != 0 {
		t.Fatalf("len(queued) = %d, want 0 (no usable group id)", len(*queued))
	}
}

func TestHandleGChatEventMessageReactionNoMessageIDSkipped(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := messageReactionEvent(spaceGroupID("space-1"), "", "98765", "❤", pb.MessageReactionEvent_ADD, 1)

	gc.handleGChatEvent(context.Background(), evt)

	if len(*queued) != 0 {
		t.Fatalf("len(queued) = %d, want 0 (no message id)", len(*queued))
	}
}

func TestHandleGChatEventMessageReactionUnknownTypeSkipped(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := messageReactionEvent(spaceGroupID("space-1"), "msg-6", "98765", "❤", pb.MessageReactionEvent_ReactionEventType(99), 1)

	res := gc.handleGChatEvent(context.Background(), evt)

	if !res.Success {
		t.Fatalf("handleGChatEvent() result = %+v, want Success (Ignored, so the watermark still advances)", res)
	}
	if len(*queued) != 0 {
		t.Fatalf("len(queued) = %d, want 0 (unknown reaction event type)", len(*queued))
	}
}
