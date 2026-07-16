package connector

// events_test.go -- inbound MESSAGE_POSTED -> bridgev2.RemoteMessage
// (M2 Task 4). Mirrors sync_test.go's queueChatResyncFn capture pattern: a
// bare *GChatClient (no full bridgev2.Bridge+DB harness, see newTestUserLogin
// in client_test.go) with queueRemoteEventFn overridden to append into a
// local slice instead of dereferencing UserLogin.Bridge.

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

// TestHandleGChatEventMessagePostedSetsStreamOrder is the item-5 regression
// test (M7 Task 3): a live MESSAGE_POSTED event must set StreamOrder to the
// message's create-time microseconds, matching backfill.go's identical
// `StreamOrder: msg.GetCreateTime()` (backfill_test.go's
// TestFetchMessagesFlatThread's own StreamOrder assertion) -- previously
// this was left at its zero value on the live path, an M6-review-flagged
// inconsistency (harmless per bridgev2 treating 0 as "absent", but
// inconsistent with backfill for the same underlying fact).
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

// --- MESSAGE_UPDATED (edit, M4 Task 1): body is the same message_posted
// arm, but the outer event type is MESSAGE_UPDATED -- routed to a
// bridgev2.RemoteEdit (ConvertEdit re-runs msgconv, see
// msgconv_adapter_test.go's TestConvertEditToMatrix_* for the dedup/
// re-conversion logic itself), never queued as a brand new message. -------

// messageUpdatedEvent builds a *pb.Event shaped like a real MESSAGE_UPDATED
// event: the same message_posted body shape messagePostedEvent uses (per
// handleMessagePosted's doc comment, both share this one body arm), plus
// last_edit_time/last_update_time for the dedup gate.
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

// TestHandleGChatEventMessageUpdatedSetsStreamOrder is the item-5 regression
// test for the edit path: StreamOrder must track the SAME editTS value
// queueMessageEdit already uses for EventMeta.Timestamp (last_edit_time,
// falling back to last_update_time), for internal consistency between the
// two fields on the same event.
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

// TestHandleGChatEventMessageUpdatedDMPortalKeyHasReceiver mirrors
// TestHandleGChatEventMessagePostedDMPortalKeyHasReceiver for edits.
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

// TestHandleGChatEventMessageUpdatedDedupSkipsDuplicate proves the DEDUP gate
// end to end through the real dispatch path: ConvertEdit on a duplicate
// (equal last_edit_time) MESSAGE_UPDATED must report
// bridgev2.ErrIgnoringRemoteEvent and leave the stored metadata unchanged.
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

// TestHandleGChatEventMessageUpdatedNoGroupIDSkipped mirrors the
// MESSAGE_POSTED malformed-payload coverage for the edit path.
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

// TestHandleGChatEventMessagePostedOnHoldTypesAreQueued pins the
// gchat-port-auditor fix: portal.py's handle_event dispatches a
// message_posted body to handle_googlechat_message for ANY Event.type other
// than the literal MESSAGE_UPDATED (portal.py:539-543's `else` is
// unconditional), not just Event.type == MESSAGE_POSTED. docs/research/02
// documents ON_HOLD_MESSAGE_POSTED/_UPDATED/_PUBLISHED (Workspace DLP-held
// messages) as real, observed event types carrying this same body -- an
// earlier revision of handleMessagePosted keyed a switch on the literal
// MESSAGE_POSTED value and silently dropped these (and any other future
// non-MESSAGE_UPDATED type), which is message loss relative to Python.
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
				t.Fatalf("len(queued) = %d, want 1 (event type %v carries message_posted and is not MESSAGE_UPDATED, so Python's else branch bridges it as a new message)", len(*queued), et)
			}
		})
	}
}

// --- MESSAGE_DELETED (M4 Task 2): message_deleted body ->
// bridgev2.RemoteMessageRemove. Ports handle_googlechat_redaction's own
// extraction (portal.py:1210-1226): evt.message_id.message_id identifies
// the message to redact; group id comes from the outer Event (same as
// every other body arm, see this file's top-of-file doc comment), never
// from anything on MessageDeletedEvent itself (it carries no group id).
// The framework (bridgev2's handleRemoteMessageRemove) is responsible for
// looking up the target's existing DB rows and redacting every Matrix part
// -- mirroring Python's `for msg in target: ... self.main_intent.redact(...)`
// loop over EVERY stored row for that gcid, so this event only needs to
// carry the target's own message id, not any per-row detail. ------------

// messageDeletedEvent builds a *pb.Event shaped like a real MESSAGE_DELETED
// event reaching handleGChatEvent, mirroring messagePostedEvent's shape for
// the message_deleted body arm.
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

// TestHandleGChatEventMessageDeletedDMPortalKeyHasReceiver mirrors
// TestHandleGChatEventMessagePostedDMPortalKeyHasReceiver for deletions.
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

// TestHandleGChatEventMessageDeletedThreadedTargetUsesSameMessageID covers
// a deletion of a message that lives inside a thread: the target id is
// still just the message's own id (a thread reply and its own topic head
// have distinct ids; a delete targets the reply specifically), matching
// Python's evt.message_id.message_id with no topic-id substitution at all
// -- unlike outbound delete_message (handleredact.go), which DOES need the
// topic id to route the RPC, inbound deletion only ever needs the plain
// message id since bridgev2 looks up the existing DB row(s) by that id
// directly.
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

// --- MESSAGE_DELETED: malformed events are skipped, not queued or panicked

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

// --- MESSAGE_REACTION (M4 Task 3): message_reaction body ->
// bridgev2.RemoteReaction / bridgev2.RemoteReactionRemove (both the same
// simplevent.Reaction type). Ports handle_googlechat_reaction's own
// extraction (portal.py:1166-1208): evt.message_id.message_id identifies
// the reacted-to message; group id comes from the outer Event (same as
// every other body arm, see this file's top-of-file doc comment), never
// from anything on MessageReactionEvent itself (it carries no group id).
// evt.type (ADD/REMOVE) selects RemoteEventReaction/RemoteEventReactionRemove;
// evt.emoji.unicode is normalized both ways (variationselector.Remove for
// EmojiID, .Add for the value handed toward Matrix) -- see handlereaction.go's
// top-of-file doc comment. ------------------------------------------------

// messageReactionEvent builds a *pb.Event shaped like a real MessageReaction
// event reaching handleGChatEvent, mirroring messageDeletedEvent's shape for
// the message_reaction body arm.
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

// TestHandleGChatEventMessageReactionAddNormalizesEmojiAlreadyHavingSelector
// proves the EmojiID stays bare even if a MessageReactionEvent's
// evt.emoji.unicode were to unexpectedly carry a variation selector already
// (defensive normalization -- GC's own wire protocol is not documented to
// ever include one, see handlereaction.go's top-of-file doc comment).
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

// --- MESSAGE_REACTION: DM room -- portal key is receiver-scoped -----------

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

// --- MESSAGE_REACTION: own reaction sets IsFromMe --------------------------

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

// --- MESSAGE_REACTION: malformed/unknown events are skipped, not queued or
// panicked -------------------------------------------------------------

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

// TestHandleGChatEventMessageReactionUnknownTypeSkipped pins portal.py:1207-1208's
// `else: self.log.debug(...)` branch: a MessageReactionEvent.type outside
// {ADD, REMOVE} must be logged and ignored, not queued as either kind.
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
