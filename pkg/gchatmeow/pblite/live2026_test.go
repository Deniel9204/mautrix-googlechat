package pblite_test

// Fields are looked up by number via protoreflect, so a missing field fails an assertion, not compilation.

import (
	"encoding/json"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/pblite"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

func mustField(t *testing.T, desc protoreflect.MessageDescriptor, num int32) protoreflect.FieldDescriptor {
	t.Helper()
	fd := desc.Fields().ByNumber(protoreflect.FieldNumber(num))
	if fd == nil {
		t.Fatalf("%s: field %d not present in descriptor -- proto not updated for the live 2026 fields", desc.FullName(), num)
	}
	return fd
}

func mustDecoded(t *testing.T, m protoreflect.Message, fd protoreflect.FieldDescriptor) protoreflect.Value {
	t.Helper()
	if !m.Has(fd) {
		t.Fatalf("%s: field %d present on the wire but not decoded (skipped)", m.Descriptor().FullName(), fd.Number())
	}
	return m.Get(fd)
}

// arrayOf builds a positional array of length n with only fieldNumber set.
func arrayOf(n int, fieldNumber int, value any) []byte {
	a := make([]any, n)
	a[fieldNumber-1] = value
	data, err := json.Marshal(a)
	if err != nil {
		panic(err)
	}
	return data
}

// dmGroupID builds a GroupId wrapping a DmId (oneof member 3).
func dmGroupID(dmID string) []any {
	return []any{nil, nil, []any{dmID}}
}

func TestEventBody_Field7_TopicMuteChanged(t *testing.T) {
	topicID := []any{nil, "topic-xyz", dmGroupID("dm-abc")}
	value := []any{topicID, true, "1700000000000000"}
	data := arrayOf(7, 7, value)

	var body pb.Event_EventBody
	if err := pblite.Unmarshal(data, &body); err != nil {
		t.Fatalf("Unmarshal errored (must never error): %v", err)
	}
	desc := body.ProtoReflect().Descriptor()
	fd := mustField(t, desc, 7)
	mustDecoded(t, body.ProtoReflect(), fd)
}

func TestEventBody_Field11_GroupUnreadSubscribedTopicCountUpdated(t *testing.T) {
	value := []any{"5", "1700000000000001", dmGroupID("dm-abc")}
	data := arrayOf(11, 11, value)

	var body pb.Event_EventBody
	if err := pblite.Unmarshal(data, &body); err != nil {
		t.Fatalf("Unmarshal errored (must never error): %v", err)
	}
	desc := body.ProtoReflect().Descriptor()
	fd := mustField(t, desc, 11)
	mustDecoded(t, body.ProtoReflect(), fd)
}

// TestEventBody_Field21_TopicCreated decodes a captured TOPIC_CREATED sample.
func TestEventBody_Field21_TopicCreated(t *testing.T) {
	// EventBody field 21 is TopicCreatedEvent{topic: ...}, one level above the captured Topic.
	const topicCreatedSample = `[[[null,"O0Oxx-5XEnc",[null,null,["nhbQbgAAAAE"]]],"1784057036779823"]]`
	a := make([]any, 21)
	a[20] = json.RawMessage(topicCreatedSample)
	data, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}

	var body pb.Event_EventBody
	if err := pblite.Unmarshal(data, &body); err != nil {
		t.Fatalf("Unmarshal errored (must never error): %v", err)
	}
	bodyDesc := body.ProtoReflect().Descriptor()
	f21 := mustField(t, bodyDesc, 21)
	topicCreated := mustDecoded(t, body.ProtoReflect(), f21).Message()

	topicFD := mustField(t, topicCreated.Descriptor(), 1) // TopicCreatedEvent.topic
	topic := mustDecoded(t, topicCreated, topicFD).Message()

	sortTimeFD := mustField(t, topic.Descriptor(), 2) // Topic.sort_time
	if got := mustDecoded(t, topic, sortTimeFD).Int(); got != 1784057036779823 {
		t.Fatalf("Topic.sort_time = %d, want 1784057036779823", got)
	}

	idFD := mustField(t, topic.Descriptor(), 1) // Topic.id
	topicID := mustDecoded(t, topic, idFD).Message()

	topicIDStrFD := mustField(t, topicID.Descriptor(), 2) // TopicId.topic_id
	if got := mustDecoded(t, topicID, topicIDStrFD).String(); got != "O0Oxx-5XEnc" {
		t.Fatalf("TopicId.topic_id = %q, want O0Oxx-5XEnc", got)
	}

	groupIDFD := mustField(t, topicID.Descriptor(), 3) // TopicId.group_id
	groupID := mustDecoded(t, topicID, groupIDFD).Message()

	dmIDFD := mustField(t, groupID.Descriptor(), 3) // GroupId.dm_id (oneof)
	dmID := mustDecoded(t, groupID, dmIDFD).Message()

	dmIDStrFD := mustField(t, dmID.Descriptor(), 1) // DmId.dm_id
	if got := mustDecoded(t, dmID, dmIDStrFD).String(); got != "nhbQbgAAAAE" {
		t.Fatalf("DmId.dm_id = %q, want nhbQbgAAAAE", got)
	}
}

func TestEventBody_Field25_MessageSmartReplies(t *testing.T) {
	value := []any{dmGroupID("dm-abc"), "1700000000000002"}
	data := arrayOf(25, 25, value)

	var body pb.Event_EventBody
	if err := pblite.Unmarshal(data, &body); err != nil {
		t.Fatalf("Unmarshal errored (must never error): %v", err)
	}
	desc := body.ProtoReflect().Descriptor()
	f25 := mustField(t, desc, 25)
	smartReplies := mustDecoded(t, body.ProtoReflect(), f25).Message()

	timeFD := mustField(t, smartReplies.Descriptor(), 2)
	if got := mustDecoded(t, smartReplies, timeFD).Int(); got != 1700000000000002 {
		t.Fatalf("MessageSmartRepliesEvent.event_time_usec = %d, want 1700000000000002", got)
	}
}

func TestEventBody_Field53_GroupDefaultSortOrderUpdated(t *testing.T) {
	value := []any{dmGroupID("dm-abc"), "1700000000000003"}
	data := arrayOf(53, 53, value)

	var body pb.Event_EventBody
	if err := pblite.Unmarshal(data, &body); err != nil {
		t.Fatalf("Unmarshal errored (must never error): %v", err)
	}
	desc := body.ProtoReflect().Descriptor()
	fd := mustField(t, desc, 53)
	mustDecoded(t, body.ProtoReflect(), fd)
}

func TestEventBody_Field65_GroupReadStateUpdated(t *testing.T) {
	value := []any{dmGroupID("dm-abc"), "1700000000000004", "1700000000000005", "1700000000000006"}
	data := arrayOf(65, 65, value)

	var body pb.Event_EventBody
	if err := pblite.Unmarshal(data, &body); err != nil {
		t.Fatalf("Unmarshal errored (must never error): %v", err)
	}
	desc := body.ProtoReflect().Descriptor()
	fd := mustField(t, desc, 65)
	mustDecoded(t, body.ProtoReflect(), fd)
}

// [3,14,10] decodes as BackendMetadataDimension{TYPE_SPACE, GROUP_SIZE_SMALL, PAYLOAD_SIZE_LT_1K}.
func TestEvent_Field9_BackendMetadata(t *testing.T) {
	value := []any{nil, 42, nil, "1700000000000007", []any{3, 14, 10}, true, 9}
	data := arrayOf(9, 9, value)

	var event pb.Event
	if err := pblite.Unmarshal(data, &event); err != nil {
		t.Fatalf("Unmarshal errored (must never error): %v", err)
	}
	desc := event.ProtoReflect().Descriptor()
	f9 := mustField(t, desc, 9)
	backendMetadata := mustDecoded(t, event.ProtoReflect(), f9).Message()

	subIDFD := mustField(t, backendMetadata.Descriptor(), 2)
	if got := mustDecoded(t, backendMetadata, subIDFD).Int(); got != 42 {
		t.Fatalf("BackendMetadata.dispatch_sub_identifier = %d, want 42", got)
	}
	microsFD := mustField(t, backendMetadata.Descriptor(), 4)
	if got := mustDecoded(t, backendMetadata, microsFD).Int(); got != 1700000000000007 {
		t.Fatalf("BackendMetadata.dispatch_timestamp_micros = %d, want 1700000000000007", got)
	}
	dimensionsFD := mustField(t, backendMetadata.Descriptor(), 5)
	dims := mustDecoded(t, backendMetadata, dimensionsFD).List()
	if dims.Len() != 3 || dims.Get(0).Enum() != 3 || dims.Get(1).Enum() != 14 || dims.Get(2).Enum() != 10 {
		t.Fatalf("BackendMetadata.dimensions = %v, want [3 14 10]", dims)
	}
	dualDispatchFD := mustField(t, backendMetadata.Descriptor(), 6)
	if got := mustDecoded(t, backendMetadata, dualDispatchFD).Bool(); !got {
		t.Fatal("BackendMetadata.user_targeted_event_dual_dispatch = false, want true")
	}
	payloadHashFD := mustField(t, backendMetadata.Descriptor(), 7)
	if got := mustDecoded(t, backendMetadata, payloadHashFD).Int(); got != 9 {
		t.Fatalf("BackendMetadata.payload_hash = %d, want 9", got)
	}
}

// Each LatencyData wraps an Interval of two [sec, nsec] Timestamps.
func TestEvent_Field11_LatencyData(t *testing.T) {
	latency := []any{3, []any{
		[]any{"1700000000", "123456789"},
		[]any{"1700000001", 0},
	}}
	value := []any{latency}
	data := arrayOf(11, 11, value)

	var event pb.Event
	if err := pblite.Unmarshal(data, &event); err != nil {
		t.Fatalf("Unmarshal errored (must never error): %v", err)
	}
	desc := event.ProtoReflect().Descriptor()
	f11 := mustField(t, desc, 11)
	list := mustDecoded(t, event.ProtoReflect(), f11).List()
	if list.Len() != 1 {
		t.Fatalf("Event.latency_data has %d elements, want 1", list.Len())
	}
	entry := list.Get(0).Message()

	serverFD := mustField(t, entry.Descriptor(), 1)
	if got := mustDecoded(t, entry, serverFD).Enum(); got != 3 {
		t.Fatalf("LatencyData.server = %d, want 3 (BACKEND)", got)
	}
	intervalFD := mustField(t, entry.Descriptor(), 2)
	interval := mustDecoded(t, entry, intervalFD).Message()

	startFD := mustField(t, interval.Descriptor(), 1)
	start := mustDecoded(t, interval, startFD).Message()
	startSecFD := mustField(t, start.Descriptor(), 1)
	startNanosFD := mustField(t, start.Descriptor(), 2)
	if got := mustDecoded(t, start, startSecFD).Int(); got != 1700000000 {
		t.Fatalf("Interval.start.seconds = %d, want 1700000000", got)
	}
	if got := mustDecoded(t, start, startNanosFD).Int(); got != 123456789 {
		t.Fatalf("Interval.start.nanos = %d, want 123456789", got)
	}

	endFD := mustField(t, interval.Descriptor(), 2)
	end := mustDecoded(t, interval, endFD).Message()
	endSecFD := mustField(t, end.Descriptor(), 1)
	if got := mustDecoded(t, end, endSecFD).Int(); got != 1700000001 {
		t.Fatalf("Interval.end.seconds = %d, want 1700000001", got)
	}
}

func TestMessage_Fields24And25_Permissions(t *testing.T) {
	a := make([]any, 25)
	a[23] = 2 // editable_by = CREATOR (2)
	a[24] = 2 // deletable_by = CREATOR (2)
	data, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}

	var msg pb.Message
	if err := pblite.Unmarshal(data, &msg); err != nil {
		t.Fatalf("Unmarshal errored (must never error): %v", err)
	}
	desc := msg.ProtoReflect().Descriptor()

	editableFD := mustField(t, desc, 24)
	if got := mustDecoded(t, msg.ProtoReflect(), editableFD).Enum(); got != 2 {
		t.Fatalf("Message.editable_by = %d, want 2 (CREATOR)", got)
	}
	deletableFD := mustField(t, desc, 25)
	if got := mustDecoded(t, msg.ProtoReflect(), deletableFD).Enum(); got != 2 {
		t.Fatalf("Message.deletable_by = %d, want 2 (CREATOR)", got)
	}
}

func TestMessageEvent_Field7_NumRecipients(t *testing.T) {
	data := arrayOf(7, 7, 2)

	var me pb.MessageEvent
	if err := pblite.Unmarshal(data, &me); err != nil {
		t.Fatalf("Unmarshal errored (must never error): %v", err)
	}
	desc := me.ProtoReflect().Descriptor()
	fd := mustField(t, desc, 7)
	if got := mustDecoded(t, me.ProtoReflect(), fd).Int(); got != 2 {
		t.Fatalf("MessageEvent.num_recipients = %d, want 2", got)
	}
}

// TestEventTypeEnumValues6483HaveNames checks the names, not decoding (enums are open).
func TestEventTypeEnumValues6483HaveNames(t *testing.T) {
	if got := pb.Event_EventType(64).String(); got != "GROUP_DEFAULT_SORT_ORDER_UPDATED" {
		t.Errorf("Event_EventType(64).String() = %q, want GROUP_DEFAULT_SORT_ORDER_UPDATED", got)
	}
	if got := pb.Event_EventType(83).String(); got != "GROUP_READ_STATE_UPDATED" {
		t.Errorf("Event_EventType(83).String() = %q, want GROUP_READ_STATE_UPDATED", got)
	}
}
