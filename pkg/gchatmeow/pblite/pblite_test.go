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

// TestRoundTrip exercises the base field-number <-> array-index mapping and
// recursion into a nested message, via a real oneof member (GroupId.Id).
//
// Note: the task brief's starter code wrote `&pb.GroupId{SpaceId: ...}` but
// GroupId.SpaceId is not a direct struct field -- SpaceId is reachable only
// through the oneof wrapper type GroupId_SpaceId, since GroupId's only
// direct field is `Id isGroupId_Id`. Adjusted accordingly (verified against
// pkg/gchatmeow/proto/googlechat.pb.go).
func TestRoundTrip(t *testing.T) {
	// GroupId with nested SpaceId — field-number independent round-trip.
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
	// Marshal must emit int64 as string (JS compat).
	if !strings.Contains(string(data), `"1700000000000000"`) {
		t.Fatalf("int64 not emitted as string: %s", data)
	}
	// Unmarshal must accept both string and number forms.
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

// TestTrailingSparseDict covers Message.reply_to. The brief's starter code
// used `&pb.SendReplyTarget{...}` but Message.ReplyTo's Go type is actually
// *ReplyToMessage (field 37; CreateTime is ReplyToMessage's field 2), so the
// literal is adjusted below (verified against googlechat.pb.go: `ReplyTo
// *ReplyToMessage` protobuf:"bytes,37,...").
func TestTrailingSparseDict(t *testing.T) {
	// Build a message with a high-numbered field (Message.reply_to = 37),
	// marshal densely, then convert the tail to the server's sparse-dict form.
	in := &pb.Message{ReplyTo: &pb.ReplyToMessage{CreateTime: i64(42)}}
	dense, _ := pblite.Marshal(in)
	var out pb.Message
	if err := pblite.Unmarshal(dense, &out); err != nil {
		t.Fatal(err)
	}
	if out.GetReplyTo().GetCreateTime() != 42 {
		t.Fatal("dense high-field decode failed")
	}

	// Sparse form: array shorter than 37 entries, trailing object keyed by field number.
	sparse := []byte(`[null, {"37": ` + string(mustMarshalField(t, in)) + `}]`)
	var out2 pb.Message
	if err := pblite.Unmarshal(sparse, &out2); err != nil {
		t.Fatal(err)
	}
	if out2.GetReplyTo().GetCreateTime() != 42 {
		t.Fatal("sparse dict decode failed")
	}
}

// mustMarshalField marshals just the ReplyToMessage submessage as pblite.
func mustMarshalField(t *testing.T, m *pb.Message) []byte {
	data, err := pblite.Marshal(m.GetReplyTo())
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestPermissiveness(t *testing.T) {
	// Unknown trailing fields, nulls, and garbage values must not error.
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
	// Structural garbage DOES error.
	if err := pblite.Unmarshal([]byte(`{"not":"array"}`), &pb.GroupId{}); err == nil {
		t.Fatal("non-array should error")
	}
}

// TestBytesBase64 covers the bytes<->base64 mandatory behavior, using the
// one real bytes field in the schema: MeetingSpace.CallInfo.CseInfo.wrapped_key.
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

// TestRepeatedFields covers "repeated fields are nested arrays" for both a
// repeated scalar (StreamEventsRequest.sample_ids, field 7) and a repeated
// message (Message.annotations, field 11).
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

// TestEnumAsNumber covers "enums accept numbers" on both encode and decode.
func TestEnumAsNumber(t *testing.T) {
	in := &pb.UserId{Id: s("123"), Type: ut(pb.UserType_BOT)}
	data, err := pblite.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	// UserType_BOT == 1; must be a bare JSON number, not a string or name.
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

// TestOneofLastSetWins covers "oneof = last-set-wins": a raw array with both
// oneof arms of GroupId.Id populated (space_id=field 1, dm_id=field 3) must
// end up with only the higher-field-number (later-processed) arm set, since
// pblite has no special oneof encoding and decode walks fields in ascending
// field-number order (matching maugclib's reliance on protobuf's normal
// last-write-wins field assignment, pblite.py:122-125).
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

// TestNullSkipsField covers "null skips": an explicit null for a set field
// must leave it unset rather than clearing/erroring.
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

// TestInt64NumberPrecision guards against decoding bare (non-string) JSON
// numbers through float64, which only represents integers exactly up to
// 2^53 (9007199254740992). Message.create_time (field 3) is a microsecond
// int64 timestamp that routinely exceeds that boundary; json.Decoder must
// be configured with UseNumber() so the exact decimal text is preserved.
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

// TestSparseDictFieldNumberOverflowIgnored covers the sparse-dict field
// number parse path: a key that overflows int32 must be dropped (as an
// unparseable/unknown field), not silently truncated/wrapped into a real,
// unrelated field number.
func TestSparseDictFieldNumberOverflowIgnored(t *testing.T) {
	// 4294967298 == 2^32 + 2; naive truncation to int32 wraps this to 2,
	// which is UserId.type. It must NOT be decoded as if the key were "2".
	raw := `[null, {"4294967298": 1}]`
	var out pb.UserId
	if err := pblite.Unmarshal([]byte(raw), &out); err != nil {
		t.Fatal(err)
	}
	if out.GetType() != pb.UserType_HUMAN {
		t.Fatalf("expected Type to stay unset (HUMAN is the zero value), got %v -- overflowing sparse-dict key aliased onto field 2", out.GetType())
	}
}

// TestZeroValuePresence covers proto2 explicit field presence: an int32
// field explicitly set to its zero value (0) must round-trip as *present*,
// not as unset. This is exactly the defect class documents/research/08c
// flags against megabridge's proto3 conversion (implicit presence can't
// distinguish "set to 0" from "never set"); this codec must not regress it.
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

// TestNullInsideRepeatedList covers a null embedded inside a repeated
// field's array (not just the outer per-field null): it must be skipped
// like any other undecodable single value, without discarding the
// surrounding valid elements or aborting the whole list.
func TestNullInsideRepeatedList(t *testing.T) {
	// StreamEventsRequest.sample_ids is field 7 (repeated string).
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

	// Message.annotations is field 11 (repeated message); each Annotation's
	// start_index is its field 2, so a nested annotation array is [nil, N].
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
