package pblite_test

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	gproto "google.golang.org/protobuf/proto"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/pblite"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

func s(v string) *string { return &v }
func i64(v int64) *int64 { return &v }
func i32(v int32) *int32 { return &v }
func ut(v pb.UserType) *pb.UserType {
	return &v
}

// TestRoundTrip covers the field-number <-> index mapping and nested-message recursion.
func TestRoundTrip(t *testing.T) {
	in := &pb.GroupId{Id: &pb.GroupId_SpaceId{SpaceId: &pb.SpaceId{SpaceId: s("AAAA-fixture-1")}}}
	data, err := pblite.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out pb.GroupId
	if err := pblite.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	if !gproto.Equal(in, &out) {
		t.Fatalf("round trip mismatch: %s vs %s", in, &out)
	}
}

func TestInt64AsString(t *testing.T) {
	in := &pb.Message{CreateTime: i64(1700000000000000)}
	data, _ := pblite.Marshal(in)
	if !strings.Contains(string(data), `"1700000000000000"`) {
		t.Fatalf("int64 not emitted as string: %s", data)
	}
	var out pb.Message
	if err := pblite.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	if out.GetCreateTime() != 1700000000000000 {
		t.Fatal("string int64 not decoded")
	}
	numeric := strings.Replace(string(data), `"1700000000000000"`, `1700000000000000`, 1)
	var out2 pb.Message
	if err := pblite.Unmarshal([]byte(numeric), &out2); err != nil {
		t.Fatal(err)
	}
	if out2.GetCreateTime() != 1700000000000000 {
		t.Fatal("numeric int64 not decoded")
	}
}

// TestTrailingSparseDict covers Message.reply_to (field 37) in the sparse trailing-dict form.
func TestTrailingSparseDict(t *testing.T) {
	in := &pb.Message{ReplyTo: &pb.ReplyToMessage{CreateTime: i64(42)}}
	dense, _ := pblite.Marshal(in)
	var out pb.Message
	if err := pblite.Unmarshal(dense, &out); err != nil {
		t.Fatal(err)
	}
	if out.GetReplyTo().GetCreateTime() != 42 {
		t.Fatal("dense high-field decode failed")
	}

	sparse := []byte(`[null, {"37": ` + string(mustMarshalField(t, in)) + `}]`)
	var out2 pb.Message
	if err := pblite.Unmarshal(sparse, &out2); err != nil {
		t.Fatal(err)
	}
	if out2.GetReplyTo().GetCreateTime() != 42 {
		t.Fatal("sparse dict decode failed")
	}
}

func mustMarshalField(t *testing.T, m *pb.Message) []byte {
	data, err := pblite.Marshal(m.GetReplyTo())
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestPermissiveness(t *testing.T) {
	for _, data := range []string{
		`[null, null, null, null, null, null, null, null, null, null, null, null, "unknown-tail", ["extra"]]`,
		`[]`,
		`[{"9999": "x"}]`,
	} {
		var out pb.GroupId
		if err := pblite.Unmarshal([]byte(data), &out); err != nil {
			t.Fatalf("Unmarshal(%s) errored: %v — must skip, never fail", data, err)
		}
	}
	if err := pblite.Unmarshal([]byte(`{"not":"array"}`), &pb.GroupId{}); err == nil {
		t.Fatal("non-array should error")
	}
}

// TestBytesBase64 uses the only bytes field in the schema: MeetingSpace.CallInfo.CseInfo.wrapped_key.
func TestBytesBase64(t *testing.T) {
	raw := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01}
	in := &pb.MeetingSpace_CallInfo_CseInfo{WrappedKey: raw}
	data, err := pblite.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	want := base64.StdEncoding.EncodeToString(raw)
	if !strings.Contains(string(data), `"`+want+`"`) {
		t.Fatalf("bytes not emitted as base64 %q: %s", want, data)
	}
	var out pb.MeetingSpace_CallInfo_CseInfo
	if err := pblite.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	if !gproto.Equal(in, &out) {
		t.Fatalf("bytes round trip mismatch: %v vs %v", in.WrappedKey, out.WrappedKey)
	}
}

func TestRepeatedFields(t *testing.T) {
	inScalar := &pb.StreamEventsRequest{SampleIds: []string{"a", "b", "c"}}
	data, err := pblite.Marshal(inScalar)
	if err != nil {
		t.Fatal(err)
	}
	var outScalar pb.StreamEventsRequest
	if err := pblite.Unmarshal(data, &outScalar); err != nil {
		t.Fatal(err)
	}
	if !gproto.Equal(inScalar, &outScalar) {
		t.Fatalf("repeated scalar round trip mismatch: %s vs %s", inScalar, &outScalar)
	}

	inMsg := &pb.Message{Annotations: []*pb.Annotation{
		{StartIndex: i32(1)},
		{StartIndex: i32(2)},
	}}
	data2, err := pblite.Marshal(inMsg)
	if err != nil {
		t.Fatal(err)
	}
	var outMsg pb.Message
	if err := pblite.Unmarshal(data2, &outMsg); err != nil {
		t.Fatal(err)
	}
	if !gproto.Equal(inMsg, &outMsg) {
		t.Fatalf("repeated message round trip mismatch: %s vs %s", inMsg, &outMsg)
	}
	if len(outMsg.GetAnnotations()) != 2 {
		t.Fatalf("expected 2 annotations, got %d", len(outMsg.GetAnnotations()))
	}
}

func TestEnumAsNumber(t *testing.T) {
	in := &pb.UserId{Id: s("123"), Type: ut(pb.UserType_BOT)}
	data, err := pblite.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `,1]`) && !strings.Contains(string(data), `,1,`) {
		t.Fatalf("enum not emitted as number: %s", data)
	}
	var out pb.UserId
	if err := pblite.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	if out.GetType() != pb.UserType_BOT {
		t.Fatalf("enum not decoded from number: got %v", out.GetType())
	}
}

// TestOneofLastSetWins: with both GroupId arms set, the higher field number wins.
func TestOneofLastSetWins(t *testing.T) {
	raw := `[["space-1"], null, ["dm-1"]]`
	var out pb.GroupId
	if err := pblite.Unmarshal([]byte(raw), &out); err != nil {
		t.Fatal(err)
	}
	if out.GetDmId().GetDmId() != "dm-1" {
		t.Fatalf("expected dm_id (field 3, processed last) to win, got dm_id=%q space_id=%v",
			out.GetDmId().GetDmId(), out.GetSpaceId())
	}
	if out.GetSpaceId() != nil {
		t.Fatalf("expected space_id to be cleared by the later oneof set, got %v", out.GetSpaceId())
	}
}

func TestNullSkipsField(t *testing.T) {
	var out pb.UserId
	if err := pblite.Unmarshal([]byte(`[null, 1]`), &out); err != nil {
		t.Fatal(err)
	}
	if out.GetId() != "" {
		t.Fatalf("expected Id to stay unset after null, got %q", out.GetId())
	}
	if out.GetType() != pb.UserType_BOT {
		t.Fatalf("expected Type to still decode past the null, got %v", out.GetType())
	}
}

func TestInt64NumberPrecision(t *testing.T) {
	const want int64 = 9007199254740993 // 2^53 + 1: the smallest int not exactly representable as float64.
	raw := `[null, null, 9007199254740993]`
	var out pb.Message
	if err := pblite.Unmarshal([]byte(raw), &out); err != nil {
		t.Fatal(err)
	}
	if out.GetCreateTime() != want {
		t.Fatalf("expected exact int64 %d, got %d (float64 round-trip precision loss)", want, out.GetCreateTime())
	}
}

func TestSparseDictFieldNumberOverflowIgnored(t *testing.T) {
	// 2^32 + 2 would wrap to field 2 (UserId.type) if truncated.
	raw := `[null, {"4294967298": 1}]`
	var out pb.UserId
	if err := pblite.Unmarshal([]byte(raw), &out); err != nil {
		t.Fatal(err)
	}
	if out.GetType() != pb.UserType_HUMAN {
		t.Fatalf("expected Type to stay unset (HUMAN is the zero value), got %v -- overflowing sparse-dict key aliased onto field 2", out.GetType())
	}
}

// TestZeroValuePresence: proto2 presence means an explicit 0 must round-trip as set.
func TestZeroValuePresence(t *testing.T) {
	in := &pb.Annotation{StartIndex: i32(0)}
	data, err := pblite.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out pb.Annotation
	if err := pblite.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	if out.StartIndex == nil {
		t.Fatal("expected StartIndex to be present after round trip, got nil (explicit 0 lost its has-bit)")
	}
	if !gproto.Equal(in, &out) {
		t.Fatalf("zero-value round trip mismatch: %s vs %s", in, &out)
	}
}

func TestNullInsideRepeatedList(t *testing.T) {
	scalarArr := make([]any, 7)
	scalarArr[6] = []any{"a", nil, "b"}
	scalarData, err := json.Marshal(scalarArr)
	if err != nil {
		t.Fatal(err)
	}
	var outScalar pb.StreamEventsRequest
	if err := pblite.Unmarshal(scalarData, &outScalar); err != nil {
		t.Fatal(err)
	}
	if got := outScalar.GetSampleIds(); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("expected [a b] with the null element skipped, got %v", got)
	}

	// Annotation.start_index is field 2, so each element is [nil, N].
	msgArr := make([]any, 11)
	msgArr[10] = []any{[]any{nil, 1}, nil, []any{nil, 2}}
	msgData, err := json.Marshal(msgArr)
	if err != nil {
		t.Fatal(err)
	}
	var outMsg pb.Message
	if err := pblite.Unmarshal(msgData, &outMsg); err != nil {
		t.Fatal(err)
	}
	annotations := outMsg.GetAnnotations()
	if len(annotations) != 2 || annotations[0].GetStartIndex() != 1 || annotations[1].GetStartIndex() != 2 {
		t.Fatalf("expected 2 annotations [1 2] with the null element skipped, got %v", annotations)
	}
}

// TestDecodeDepthIsCapped: Message.last_reply is self-referential.
func TestDecodeDepthIsCapped(t *testing.T) {
	const nested = 300 // far beyond any legitimate reply chain

	// last_reply is field 12 -> pblite array index 11.
	payload := "[]"
	for i := 0; i < nested; i++ {
		payload = "[" + strings.Repeat("null,", 11) + payload + "]"
	}

	var msg pb.Message
	if err := pblite.Unmarshal([]byte(payload), &msg); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	depth := 0
	for m := msg.GetLastReply(); m != nil; m = m.GetLastReply() {
		depth++
	}
	if depth > pblite.MaxDepth {
		t.Errorf("decoded a last_reply chain %d deep, want it capped at %d", depth, pblite.MaxDepth)
	}
	if depth == 0 {
		t.Error("decoded nothing at all; the cap must skip only the over-deep subtree")
	}
}
