package connector

// systemmessage_test.go -- inbound SYSTEM_MESSAGE (membership changes, room
// rename/topic changes) -> bridgev2.RemoteChatInfoChange (M4 Task 6). Mirrors
// events_test.go's newEventTestClient/messagePostedEvent capture pattern.

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// systemMessageEvent builds a *pb.Event shaped like a real SYSTEM_MESSAGE
// MESSAGE_POSTED event (message_type SYSTEM_MESSAGE, exactly one annotation)
// -- the same MessagePosted body shape messagePostedEvent (events_test.go)
// builds, just with MessageType/Annotations set and no TextBody (a real
// SYSTEM_MESSAGE for a membership/room-update annotation carries none).
func systemMessageEvent(groupID *pb.GroupId, gcMessageID, creatorGaia string, createTimeMicros int64, annotation *pb.Annotation) *pb.Event {
	return &pb.Event{
		GroupId: groupID,
		Type:    pb.Event_MESSAGE_POSTED.Enum(),
		Body: &pb.Event_EventBody{
			EventType: pb.Event_MESSAGE_POSTED.Enum(),
			Type: &pb.Event_EventBody_MessagePosted{
				MessagePosted: &pb.MessageEvent{
					Message: &pb.Message{
						Id:          &pb.MessageId{MessageId: proto.String(gcMessageID)},
						Creator:     &pb.User{UserId: &pb.UserId{Id: proto.String(creatorGaia)}},
						CreateTime:  proto.Int64(createTimeMicros),
						MessageType: pb.Message_SYSTEM_MESSAGE.Enum(),
						Annotations: []*pb.Annotation{annotation},
					},
				},
			},
		},
	}
}

func membershipChangedAnnotation(changeType pb.MembershipChangedMetadata_Type, affectedGaiaIDs ...string) *pb.Annotation {
	members := make([]*pb.MemberId, 0, len(affectedGaiaIDs))
	for _, id := range affectedGaiaIDs {
		members = append(members, &pb.MemberId{Id: &pb.MemberId_UserId{UserId: &pb.UserId{Id: proto.String(id)}}})
	}
	return &pb.Annotation{
		Type: pb.AnnotationType_MEMBERSHIP_CHANGED.Enum(),
		Metadata: &pb.Annotation_MembershipChanged{
			MembershipChanged: &pb.MembershipChangedMetadata{
				Type:            changeType.Enum(),
				AffectedMembers: members,
			},
		},
	}
}

func roomRenameAnnotation(newName string) *pb.Annotation {
	return &pb.Annotation{
		Type: pb.AnnotationType_ROOM_UPDATED.Enum(),
		Metadata: &pb.Annotation_RoomUpdated{
			RoomUpdated: &pb.RoomUpdatedMetadata{
				RenameMetadata: &pb.RoomUpdatedMetadata_RoomRenameMetadata{
					NewName: proto.String(newName),
				},
			},
		},
	}
}

func groupDetailsAnnotation(description string) *pb.Annotation {
	return &pb.Annotation{
		Type: pb.AnnotationType_ROOM_UPDATED.Enum(),
		Metadata: &pb.Annotation_RoomUpdated{
			RoomUpdated: &pb.RoomUpdatedMetadata{
				GroupDetailsMetadata: &pb.RoomUpdatedMetadata_GroupDetailsUpdatedMetadata{
					NewGroupDetails: &pb.GroupDetails{Description: proto.String(description)},
				},
			},
		},
	}
}

// --- MEMBERSHIP_CHANGED: someone joins a space -----------------------------

func TestHandleGChatEventSystemMessageMembershipJoinedQueuesJoinDelta(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := systemMessageEvent(spaceGroupID("space-1"), "sysmsg-1", "98765", 1700000000123456,
		membershipChangedAnnotation(pb.MembershipChangedMetadata_JOINED, "55555"))

	res := gc.handleGChatEvent(context.Background(), evt)

	if !res.Success {
		t.Fatalf("handleGChatEvent() result = %+v, want Success", res)
	}
	if len(*queued) != 1 {
		t.Fatalf("len(queued) = %d, want 1", len(*queued))
	}
	change, ok := (*queued)[0].(bridgev2.RemoteChatInfoChange)
	if !ok {
		t.Fatalf("queued event does not implement bridgev2.RemoteChatInfoChange: %T", (*queued)[0])
	}
	info, err := change.GetChatInfoChange(context.Background())
	if err != nil {
		t.Fatalf("GetChatInfoChange: %v", err)
	}
	if info.MemberChanges == nil {
		t.Fatal("MemberChanges = nil, want a join delta")
	}
	if info.MemberChanges.IsFull {
		t.Error("MemberChanges.IsFull = true, want false (this is a delta, not the whole member list)")
	}
	member, ok := info.MemberChanges.MemberMap[gcid.MakeUserID("55555")]
	if !ok {
		t.Fatalf("MemberMap missing entry for 55555: %+v", info.MemberChanges.MemberMap)
	}
	if member.Membership != event.MembershipJoin {
		t.Errorf("Membership = %q, want %q", member.Membership, event.MembershipJoin)
	}
	if member.PrevMembership != "" {
		t.Errorf("PrevMembership = %q, want \"\" (ensure_joined is unconditional/idempotent in Python, portal.py:1304-1305)", member.PrevMembership)
	}

	typed := (*queued)[0].(bridgev2.RemoteEvent)
	if got := typed.GetType(); got != bridgev2.RemoteEventChatInfoChange {
		t.Errorf("GetType() = %v, want RemoteEventChatInfoChange", got)
	}
	wantPortalKey := gcid.MakePortalKey(gcid.GroupID{ID: "space-1", IsDM: false}, gc.UserLogin.ID)
	if got := typed.GetPortalKey(); got != wantPortalKey {
		t.Errorf("GetPortalKey() = %+v, want %+v", got, wantPortalKey)
	}
	if wantPortalKey.Receiver != "" {
		t.Fatalf("test setup: space portal key must have empty Receiver, got %q", wantPortalKey.Receiver)
	}
	sender := typed.GetSender()
	if got, want := sender.Sender, gcid.MakeUserID("98765"); got != want {
		t.Errorf("GetSender().Sender = %q, want %q (msg.creator, not update.initiator)", got, want)
	}
	createPortal, ok := (*queued)[0].(bridgev2.RemoteEventThatMayCreatePortal)
	if !ok || !createPortal.ShouldCreatePortal() {
		t.Error("ShouldCreatePortal() = false, want true (Python's create_matrix_room runs before SYSTEM_MESSAGE dispatch, portal.py:573-574)")
	}
}

// --- MEMBERSHIP_CHANGED: someone leaves a space -----------------------------

func TestHandleGChatEventSystemMessageMembershipLeftQueuesLeaveDelta(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := systemMessageEvent(spaceGroupID("space-1"), "sysmsg-2", "98765", 1,
		membershipChangedAnnotation(pb.MembershipChangedMetadata_LEFT, "55555"))

	gc.handleGChatEvent(context.Background(), evt)

	if len(*queued) != 1 {
		t.Fatalf("len(queued) = %d, want 1", len(*queued))
	}
	change := (*queued)[0].(bridgev2.RemoteChatInfoChange)
	info, err := change.GetChatInfoChange(context.Background())
	if err != nil {
		t.Fatalf("GetChatInfoChange: %v", err)
	}
	member, ok := info.MemberChanges.MemberMap[gcid.MakeUserID("55555")]
	if !ok {
		t.Fatalf("MemberMap missing entry for 55555: %+v", info.MemberChanges.MemberMap)
	}
	if member.Membership != event.MembershipLeave {
		t.Errorf("Membership = %q, want %q", member.Membership, event.MembershipLeave)
	}
	if member.PrevMembership != event.MembershipJoin {
		t.Errorf("PrevMembership = %q, want %q (mautrix-meta's handleRemoveParticipant precedent)", member.PrevMembership, event.MembershipJoin)
	}
}

// --- membershipChangeDelta: all 9 MembershipChangedMetadata_Type values ----

// TestMembershipChangeDeltaMapsAllNineTypes pins the mapping from GC's 9
// MembershipChangedMetadata_Type values (docs/research/02-wire-protocol.md's
// wire inventory) onto (Membership, PrevMembership, ok), grouped exactly the
// way Python's own if/elif chain groups them (handle_googlechat_membership_change,
// portal.py:1296-1335): INVITED -> invite, {JOINED, ADDED, BOT_ADDED} -> join
// (unconditional/idempotent, no PrevMembership gate), {LEFT, REMOVED,
// BOT_REMOVED, KICKED_DUE_TO_OTR_CONFLICT} -> leave (gated on a prior join).
// ROLE_UPDATED and the zero-value TYPE_UNSPECIFIED both fall outside that
// chain -- ok=false, no Matrix membership action, matching Python's
// implicit no-op for both.
func TestMembershipChangeDeltaMapsAllNineTypes(t *testing.T) {
	tests := []struct {
		name               string
		changeType         pb.MembershipChangedMetadata_Type
		wantMembership     event.Membership
		wantPrevMembership event.Membership
		wantOK             bool
	}{
		{"INVITED", pb.MembershipChangedMetadata_INVITED, event.MembershipInvite, event.MembershipLeave, true},
		{"JOINED", pb.MembershipChangedMetadata_JOINED, event.MembershipJoin, "", true},
		{"ADDED", pb.MembershipChangedMetadata_ADDED, event.MembershipJoin, "", true},
		{"BOT_ADDED", pb.MembershipChangedMetadata_BOT_ADDED, event.MembershipJoin, "", true},
		{"LEFT", pb.MembershipChangedMetadata_LEFT, event.MembershipLeave, event.MembershipJoin, true},
		{"REMOVED", pb.MembershipChangedMetadata_REMOVED, event.MembershipLeave, event.MembershipJoin, true},
		{"BOT_REMOVED", pb.MembershipChangedMetadata_BOT_REMOVED, event.MembershipLeave, event.MembershipJoin, true},
		{"KICKED_DUE_TO_OTR_CONFLICT", pb.MembershipChangedMetadata_KICKED_DUE_TO_OTR_CONFLICT, event.MembershipLeave, event.MembershipJoin, true},
		{"ROLE_UPDATED", pb.MembershipChangedMetadata_ROLE_UPDATED, "", "", false},
		{"TYPE_UNSPECIFIED", pb.MembershipChangedMetadata_TYPE_UNSPECIFIED, "", "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			membership, prevMembership, ok := membershipChangeDelta(tc.changeType)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if membership != tc.wantMembership {
				t.Errorf("Membership = %q, want %q", membership, tc.wantMembership)
			}
			if prevMembership != tc.wantPrevMembership {
				t.Errorf("PrevMembership = %q, want %q", prevMembership, tc.wantPrevMembership)
			}
		})
	}
}

// --- MEMBERSHIP_CHANGED: ROLE_UPDATED produces no member delta, but the
// system message is still fully consumed (never bridged as ordinary text),
// matching Python's unconditional `return` after
// handle_googlechat_membership_change (portal.py:1370-1374) ----------------

func TestHandleGChatEventSystemMessageMembershipRoleUpdatedNoOpButHandled(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := systemMessageEvent(spaceGroupID("space-1"), "sysmsg-3", "98765", 1,
		membershipChangedAnnotation(pb.MembershipChangedMetadata_ROLE_UPDATED, "55555"))

	res := gc.handleGChatEvent(context.Background(), evt)

	if !res.Success {
		t.Fatalf("handleGChatEvent() result = %+v, want Success (Ignored, so the watermark still advances)", res)
	}
	if len(*queued) != 0 {
		t.Fatalf("len(queued) = %d, want 0 (ROLE_UPDATED is not a membership transition)", len(*queued))
	}
}

// --- MEMBERSHIP_CHANGED: multiple affected members in one annotation ------

func TestHandleGChatEventSystemMessageMembershipMultipleAffectedMembers(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := systemMessageEvent(spaceGroupID("space-1"), "sysmsg-4", "98765", 1,
		membershipChangedAnnotation(pb.MembershipChangedMetadata_ADDED, "111", "222"))

	gc.handleGChatEvent(context.Background(), evt)

	change := (*queued)[0].(bridgev2.RemoteChatInfoChange)
	info, _ := change.GetChatInfoChange(context.Background())
	if len(info.MemberChanges.MemberMap) != 2 {
		t.Fatalf("len(MemberMap) = %d, want 2: %+v", len(info.MemberChanges.MemberMap), info.MemberChanges.MemberMap)
	}
	for _, id := range []string{"111", "222"} {
		m, ok := info.MemberChanges.MemberMap[gcid.MakeUserID(id)]
		if !ok {
			t.Errorf("MemberMap missing %q", id)
			continue
		}
		if m.Membership != event.MembershipJoin {
			t.Errorf("member %q Membership = %q, want join", id, m.Membership)
		}
	}
}

// --- ROOM_UPDATED: rename -> ChatInfo.Name ----------------------------------

func TestHandleGChatEventSystemMessageRenameQueuesChatInfoChangeName(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := systemMessageEvent(spaceGroupID("space-1"), "sysmsg-5", "98765", 1700000000123456,
		roomRenameAnnotation("New Space Name"))

	res := gc.handleGChatEvent(context.Background(), evt)

	if !res.Success {
		t.Fatalf("handleGChatEvent() result = %+v, want Success", res)
	}
	if len(*queued) != 1 {
		t.Fatalf("len(queued) = %d, want 1", len(*queued))
	}
	change := (*queued)[0].(bridgev2.RemoteChatInfoChange)
	info, err := change.GetChatInfoChange(context.Background())
	if err != nil {
		t.Fatalf("GetChatInfoChange: %v", err)
	}
	if info.ChatInfo == nil || info.ChatInfo.Name == nil {
		t.Fatalf("ChatInfo.Name = nil, want %q", "New Space Name")
	}
	if *info.ChatInfo.Name != "New Space Name" {
		t.Errorf("ChatInfo.Name = %q, want %q", *info.ChatInfo.Name, "New Space Name")
	}
	if info.ChatInfo.Topic != nil {
		t.Errorf("ChatInfo.Topic = %q, want nil (unchanged; a rename doesn't touch the topic)", *info.ChatInfo.Topic)
	}
	if info.MemberChanges != nil {
		t.Errorf("MemberChanges = %+v, want nil (a rename carries no membership delta)", info.MemberChanges)
	}
}

// --- ROOM_UPDATED: group details -> ChatInfo.Topic --------------------------

func TestHandleGChatEventSystemMessageGroupDetailsQueuesChatInfoChangeTopic(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := systemMessageEvent(spaceGroupID("space-1"), "sysmsg-6", "98765", 1,
		groupDetailsAnnotation("New topic text"))

	gc.handleGChatEvent(context.Background(), evt)

	if len(*queued) != 1 {
		t.Fatalf("len(queued) = %d, want 1", len(*queued))
	}
	change := (*queued)[0].(bridgev2.RemoteChatInfoChange)
	info, err := change.GetChatInfoChange(context.Background())
	if err != nil {
		t.Fatalf("GetChatInfoChange: %v", err)
	}
	if info.ChatInfo == nil || info.ChatInfo.Topic == nil {
		t.Fatalf("ChatInfo.Topic = nil, want %q", "New topic text")
	}
	if *info.ChatInfo.Topic != "New topic text" {
		t.Errorf("ChatInfo.Topic = %q, want %q", *info.ChatInfo.Topic, "New topic text")
	}
	if info.ChatInfo.Name != nil {
		t.Errorf("ChatInfo.Name = %q, want nil (unchanged; a group-details update doesn't touch the name)", *info.ChatInfo.Name)
	}
}

// TestHandleGChatEventSystemMessageGroupDetailsEmptyDescriptionStillApplies
// pins chatinfo.go's own "Topic: unconditional" presence semantics
// (proto2 HasField, i.e. `!= nil`, NOT `!= ""`): an explicit empty-string
// description update must still produce a Topic pointer, not be treated as
// "no change" the way an empty rename is.
func TestHandleGChatEventSystemMessageGroupDetailsEmptyDescriptionStillApplies(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := systemMessageEvent(spaceGroupID("space-1"), "sysmsg-7", "98765", 1,
		groupDetailsAnnotation(""))

	gc.handleGChatEvent(context.Background(), evt)

	change := (*queued)[0].(bridgev2.RemoteChatInfoChange)
	info, _ := change.GetChatInfoChange(context.Background())
	if info.ChatInfo == nil || info.ChatInfo.Topic == nil {
		t.Fatal("ChatInfo.Topic = nil, want a non-nil pointer to \"\"")
	}
	if *info.ChatInfo.Topic != "" {
		t.Errorf("ChatInfo.Topic = %q, want \"\"", *info.ChatInfo.Topic)
	}
}

// --- ROOM_UPDATED: an empty rename with no group details metadata falls
// through to ordinary message bridging, matching Python's
// `else: return False` (portal.py:1278-1279) ---------------------------

func TestHandleGChatEventSystemMessageRoomUpdatedEmptyRenameFallsThrough(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := systemMessageEvent(spaceGroupID("space-1"), "sysmsg-8", "98765", 1,
		roomRenameAnnotation(""))
	// A real GC ROOM_UPDATED SYSTEM_MESSAGE carries no text_body, but this
	// falls through to the ordinary message path, so give it one to prove
	// exactly which path handled it.
	evt.GetBody().GetMessagePosted().Message.TextBody = proto.String("(unused fallback text)")

	gc.handleGChatEvent(context.Background(), evt)

	if len(*queued) != 1 {
		t.Fatalf("len(queued) = %d, want 1", len(*queued))
	}
	if _, ok := (*queued)[0].(bridgev2.RemoteMessage); !ok {
		t.Errorf("queued event = %T, want bridgev2.RemoteMessage (fell through to queueMessagePosted)", (*queued)[0])
	}
}

// --- SYSTEM_MESSAGE with an annotation type that is neither ROOM_UPDATED
// nor MEMBERSHIP_CHANGED also falls through, matching Python's implicit
// no-op when update_type matches neither elif (portal.py:1369-1374) --------

func TestHandleGChatEventSystemMessageUnrecognizedAnnotationTypeFallsThrough(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	annotation := &pb.Annotation{Type: pb.AnnotationType_URL.Enum()}
	evt := systemMessageEvent(spaceGroupID("space-1"), "sysmsg-9", "98765", 1, annotation)
	evt.GetBody().GetMessagePosted().Message.TextBody = proto.String("(unused fallback text)")

	gc.handleGChatEvent(context.Background(), evt)

	if len(*queued) != 1 {
		t.Fatalf("len(queued) = %d, want 1", len(*queued))
	}
	if _, ok := (*queued)[0].(bridgev2.RemoteMessage); !ok {
		t.Errorf("queued event = %T, want bridgev2.RemoteMessage (fell through to queueMessagePosted)", (*queued)[0])
	}
}

// --- SYSTEM_MESSAGE with 2 annotations falls through, matching Python's
// own `len(evt.annotations) == 1` gate (portal.py:1362) --------------------

func TestHandleGChatEventSystemMessageTwoAnnotationsFallsThrough(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := systemMessageEvent(spaceGroupID("space-1"), "sysmsg-10", "98765", 1,
		roomRenameAnnotation("Ignored Because Two Annotations"))
	msg := evt.GetBody().GetMessagePosted().Message
	msg.Annotations = append(msg.Annotations, roomRenameAnnotation("Second annotation"))
	msg.TextBody = proto.String("(unused fallback text)")

	gc.handleGChatEvent(context.Background(), evt)

	if len(*queued) != 1 {
		t.Fatalf("len(queued) = %d, want 1", len(*queued))
	}
	if _, ok := (*queued)[0].(bridgev2.RemoteMessage); !ok {
		t.Errorf("queued event = %T, want bridgev2.RemoteMessage (fell through, len(annotations) != 1)", (*queued)[0])
	}
}

// --- DM room -- membership is space-scoped in practice, but the routing
// logic itself must still work identically for a DM group id (receiver-scoped
// portal key) --------------------------------------------------------------

func TestHandleGChatEventSystemMessageMembershipDMPortalKeyHasReceiver(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := systemMessageEvent(dmGroupID("dm-1"), "sysmsg-11", "98765", 1,
		membershipChangedAnnotation(pb.MembershipChangedMetadata_JOINED, "55555"))

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

// --- malformed/unknown events are skipped, not queued or panicked ---------

func TestHandleGChatEventSystemMessageNoGroupIDSkipped(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := systemMessageEvent(nil, "sysmsg-12", "98765", 1,
		membershipChangedAnnotation(pb.MembershipChangedMetadata_JOINED, "55555"))

	res := gc.handleGChatEvent(context.Background(), evt)

	if !res.Success {
		t.Fatalf("handleGChatEvent() result = %+v, want Success (Ignored, so the watermark still advances past the garbage)", res)
	}
	if len(*queued) != 0 {
		t.Fatalf("len(queued) = %d, want 0 (no usable group id)", len(*queued))
	}
}

func TestHandleGChatEventSystemMessageNoAffectedMembersSkipped(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := systemMessageEvent(spaceGroupID("space-1"), "sysmsg-13", "98765", 1,
		membershipChangedAnnotation(pb.MembershipChangedMetadata_JOINED))

	res := gc.handleGChatEvent(context.Background(), evt)

	if !res.Success {
		t.Fatalf("handleGChatEvent() result = %+v, want Success (Ignored)", res)
	}
	if len(*queued) != 0 {
		t.Fatalf("len(queued) = %d, want 0 (no affected members)", len(*queued))
	}
}
