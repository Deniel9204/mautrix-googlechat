// Package pblite implements the pblite (a.k.a. protojson-array) codec used
// for Google Chat's realtime webchannel stream. A pblite message is a JSON
// array where array index i (0-based) holds field number i+1; missing
// fields are null-padded. High field numbers may instead appear in a
// trailing JSON *object* keyed by field number ("{fieldNumber: value}",
// e.g. Message.reply_to = field 37) -- a sparse extension the server
// actually uses.
//
// Decoding is permissive by design: unknown field numbers, null values,
// and values that don't match the field's expected shape are debug-logged
// and skipped. Only a structural failure --
// the top-level payload not being a JSON array -- is a hard error. Google
// adds/changes wire fields often; one bad or new field must never take down
// decoding of the rest of the message.
//
// # Provenance
//
// The protoreflect field-walk structure is adapted from
// go.mau.fi/util/pblite (the package gmessages and googlechat-megabridge
// both import verbatim as
// `go.mau.fi/util/pblite`; that decode logic was upstreamed there and
// gmessages itself only keeps a debug CLI around it). That upstream core --
// walk msg.ProtoReflect().Descriptor().Fields(), map field number to array
// index, recurse for MessageKind, string-or-number for 64-bit kinds,
// base64 for BytesKind -- is the closest prior art and is reused here.
//
// It is NOT imported directly, because of two defects in how megabridge
// relies on it that this package must not replicate:
//   - non-permissive decode: go.mau.fi/util/pblite.Unmarshal aborts the
//     entire message on the first type-mismatched field (deserialize.go
//     propagates every per-field error up through Unmarshal), so on the
//     live stream any single unexpected value drops the whole
//     StreamEventsResponse.
//   - no trailing sparse-dict support: deserializeFromSlice only reads
//     data[fieldNumber-1] positionally, so Message.reply_to (field 37) and
//     any other field the server sends via the sparse-dict form either
//     lands out of range (silently dropped) or hits a message field by
//     accident and errors out.
//
// This package fixes both: per-field decode failures are logged and
// skipped rather than propagated (see decodeValue/decodeListField), and a
// trailing JSON object is split out and merged in as extra (field number,
// value) pairs (see splitTrailingDict/buildFieldEntries).
package pblite

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"

	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Unmarshal decodes a pblite JSON array into msg. It is permissive: unknown
// fields, nulls, and undecodable single values are debug-logged and
// skipped, never returned as errors. Only a structural failure -- data
// isn't a JSON array -- returns an error.
func Unmarshal(data []byte, msg proto.Message) error {
	// UseNumber() keeps every JSON number as the exact decimal text
	// (json.Number, a string underneath) instead of decoding it through
	// float64, which can only represent integers exactly up to 2^53.
	// create_time and friends are microsecond int64 timestamps well past
	// that, so a bare-number (non-stringified) int64/uint64 -- which the
	// mandatory decode behaviors require accepting -- must not round-trip
	// through float64 or it silently corrupts the value instead of
	// decoding it exactly.
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var raw any
	if err := dec.Decode(&raw); err != nil {
		return fmt.Errorf("pblite: invalid JSON: %w", err)
	}
	if dec.More() {
		return fmt.Errorf("pblite: trailing data after JSON value")
	}
	arr, ok := raw.([]any)
	if !ok {
		return fmt.Errorf("pblite: expected JSON array, got %T", raw)
	}
	decodeMessage(arr, msg.ProtoReflect())
	return nil
}

// Marshal encodes msg as a dense pblite JSON array (nulls for unset
// fields). int64/uint64 fields are emitted as JSON strings, matching the
// JS client (JS Number can't hold 64-bit integers precisely).
func Marshal(msg proto.Message) ([]byte, error) {
	values, err := encodeMessage(msg.ProtoReflect())
	if err != nil {
		return nil, err
	}
	return json.Marshal(values)
}

// --- decode ---

// fieldEntry is one (field number, raw JSON value) pair pulled out of a
// pblite array, in the order it should be applied.
type fieldEntry struct {
	number int
	value  any
}

// splitTrailingDict separates the sparse trailing-object extension (if
// present) from the positional part of a pblite array.
func splitTrailingDict(arr []any) (positional []any, dict map[string]any) {
	if len(arr) == 0 {
		return arr, nil
	}
	if d, ok := arr[len(arr)-1].(map[string]any); ok {
		return arr[:len(arr)-1], d
	}
	return arr, nil
}

// buildFieldEntries flattens a pblite array (plus optional trailing sparse
// dict) into an ordered list of (field number, value) pairs: positional
// entries first in ascending index order, then any sparse-dict entries in
// ascending field-number order. This ordering is what makes oneof
// last-set-wins deterministic -- pblite has no special oneof encoding, so
// "last" means "processed last" here, mirroring the array's natural
// low-to-high field order (the format relies on plain in-order field
// assignment).
func buildFieldEntries(arr []any) []fieldEntry {
	positional, dict := splitTrailingDict(arr)
	entries := make([]fieldEntry, 0, len(positional)+len(dict))
	for i, v := range positional {
		entries = append(entries, fieldEntry{number: i + 1, value: v})
	}
	if len(dict) > 0 {
		extra := make([]fieldEntry, 0, len(dict))
		for k, v := range dict {
			// ParseInt with bitSize 32, not Atoi (platform int, effectively
			// 64-bit): entry.number is later narrowed to
			// protoreflect.FieldNumber (int32) when looked up, and an
			// unchecked truncation there could wrap a huge/garbage key
			// around into a small, real field number and silently
			// overwrite an unrelated field instead of being skipped as
			// unknown.
			n, err := strconv.ParseInt(k, 10, 32)
			if err != nil {
				log.Debug().Str("key", k).Msg("pblite: skipping out-of-range/non-numeric sparse-dict key")
				continue
			}
			extra = append(extra, fieldEntry{number: int(n), value: v})
		}
		sort.Slice(extra, func(i, j int) bool { return extra[i].number < extra[j].number })
		entries = append(entries, extra...)
	}
	return entries
}

// MaxDepth caps how deep decoding will recurse into nested messages.
//
// googlechat.proto contains self-referential messages -- Message.last_reply
// is itself a Message -- so a crafted payload can nest arbitrarily deep, and
// each level costs a protoreflect descriptor lookup plus a message
// allocation. Without a cap of its own this package was bounded only
// incidentally, by encoding/json's internal nesting ceiling, which is not a
// limit this package controls. 64 is far beyond any real reply chain.
const MaxDepth = 64

// decodeMessage decodes a pblite array into an already-allocated protoreflect
// message. It never returns an error: every problem it finds (unknown field
// number, wrong-shaped value, bad base64, ...) is debug-logged and skipped.
func decodeMessage(arr []any, m protoreflect.Message) {
	decodeMessageAt(arr, m, 0)
}

// decodeMessageAt is decodeMessage with the current nesting depth threaded
// through. Exceeding MaxDepth skips just that subtree, consistent with this
// codec's log-and-skip contract for everything else it cannot handle.
func decodeMessageAt(arr []any, m protoreflect.Message, depth int) {
	if depth >= MaxDepth {
		logDepthExceeded(m.Descriptor(), depth)
		return
	}
	desc := m.Descriptor()
	for _, entry := range buildFieldEntries(arr) {
		if entry.value == nil {
			continue
		}
		fd := desc.Fields().ByNumber(protoreflect.FieldNumber(entry.number))
		if fd == nil {
			logUnknownField(desc, entry.number, entry.value)
			continue
		}
		if fd.IsMap() {
			// No map<> fields exist in googlechat.proto today; guard against
			// a future one instead of mishandling it as MessageKind below
			// (Message()/List() on a map value panics -- and a panic from
			// server-controlled bytes is worse than the ordinary skip path).
			logSkippedField(fd, entry.value)
			continue
		}
		if fd.IsList() {
			decodeListField(m, fd, entry.value, depth)
		} else {
			decodeSingularField(m, fd, entry.value, depth)
		}
	}
}

func decodeSingularField(m protoreflect.Message, fd protoreflect.FieldDescriptor, value any, depth int) {
	v, ok := decodeValue(value, m, nil, fd, depth)
	if !ok {
		logSkippedField(fd, value)
		return
	}
	m.Set(fd, v)
}

// decodeListField decodes a repeated field. If a single element fails to
// decode, this codec skips only the bad element and keeps the rest, per
// the stricter "undecodable single values are skipped" contract mandated
// for this port: one malformed element in an otherwise-good repeated field
// must not throw away the whole field.
func decodeListField(m protoreflect.Message, fd protoreflect.FieldDescriptor, value any, depth int) {
	items, ok := value.([]any)
	if !ok {
		logSkippedField(fd, value)
		return
	}
	list := m.NewField(fd).List()
	for _, item := range items {
		if item == nil {
			continue
		}
		v, ok := decodeValue(item, m, list, fd, depth)
		if !ok {
			logSkippedField(fd, item)
			continue
		}
		list.Append(v)
	}
	m.Set(fd, protoreflect.ValueOfList(list))
}

// decodeValue decodes a single JSON value for fd's kind. When fd is a list
// field, insideList is the list being built and a fresh element is
// allocated from it for MessageKind; otherwise a fresh field value is
// allocated from ref. Returns ok=false (never an error) for anything that
// doesn't match the expected shape -- the caller logs and skips.
func decodeValue(value any, ref protoreflect.Message, insideList protoreflect.List, fd protoreflect.FieldDescriptor, depth int) (protoreflect.Value, bool) {
	switch fd.Kind() {
	case protoreflect.MessageKind:
		arr, ok := value.([]any)
		if !ok {
			return protoreflect.Value{}, false
		}
		var nested protoreflect.Message
		if insideList != nil {
			nested = insideList.NewElement().Message()
		} else {
			nested = ref.NewField(fd).Message()
		}
		decodeMessageAt(arr, nested, depth+1)
		return protoreflect.ValueOfMessage(nested), true
	case protoreflect.BytesKind:
		str, ok := value.(string)
		if !ok {
			return protoreflect.Value{}, false
		}
		b, err := base64.StdEncoding.DecodeString(str)
		if err != nil {
			return protoreflect.Value{}, false
		}
		return protoreflect.ValueOfBytes(b), true
	case protoreflect.EnumKind:
		n, ok := toInt64(value)
		if !ok {
			return protoreflect.Value{}, false
		}
		return protoreflect.ValueOfEnum(protoreflect.EnumNumber(int32(n))), true
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		n, ok := toInt64(value)
		if !ok {
			return protoreflect.Value{}, false
		}
		return protoreflect.ValueOfInt32(int32(n)), true
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		n, ok := toInt64(value)
		if !ok {
			return protoreflect.Value{}, false
		}
		return protoreflect.ValueOfInt64(n), true
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		n, ok := toUint64(value)
		if !ok {
			return protoreflect.Value{}, false
		}
		return protoreflect.ValueOfUint32(uint32(n)), true
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		n, ok := toUint64(value)
		if !ok {
			return protoreflect.Value{}, false
		}
		return protoreflect.ValueOfUint64(n), true
	case protoreflect.FloatKind:
		f, ok := toFloat64(value)
		if !ok {
			return protoreflect.Value{}, false
		}
		return protoreflect.ValueOfFloat32(float32(f)), true
	case protoreflect.DoubleKind:
		f, ok := toFloat64(value)
		if !ok {
			return protoreflect.Value{}, false
		}
		return protoreflect.ValueOfFloat64(f), true
	case protoreflect.StringKind:
		str, ok := value.(string)
		if !ok {
			return protoreflect.Value{}, false
		}
		return protoreflect.ValueOfString(str), true
	case protoreflect.BoolKind:
		switch v := value.(type) {
		case bool:
			return protoreflect.ValueOfBool(v), true
		case json.Number:
			f, err := v.Float64()
			if err != nil {
				return protoreflect.Value{}, false
			}
			return protoreflect.ValueOfBool(f != 0), true
		default:
			return protoreflect.Value{}, false
		}
	default:
		return protoreflect.Value{}, false
	}
}

// toInt64 accepts a JSON number (decoded as json.Number, i.e. its exact
// decimal text -- see the UseNumber comment in Unmarshal) or a decimal
// string -- the pblite wire form for int64/int32 fields (the JS client
// stringifies 64-bit values it can't represent exactly as JS numbers;
// 32-bit fields are seen both ways in practice, so both are accepted for
// all integer kinds). Falls back to a float parse for the rare non-integer
// literal (e.g. "3.0").
func toInt64(value any) (int64, bool) {
	switch v := value.(type) {
	case string:
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n, true
		}
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return int64(f), true
		}
		return 0, false
	case json.Number:
		if n, err := v.Int64(); err == nil {
			return n, true
		}
		if f, err := v.Float64(); err == nil {
			return int64(f), true
		}
		return 0, false
	default:
		return 0, false
	}
}

func toUint64(value any) (uint64, bool) {
	switch v := value.(type) {
	case string:
		if n, err := strconv.ParseUint(v, 10, 64); err == nil {
			return n, true
		}
		if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 {
			return uint64(f), true
		}
		return 0, false
	case json.Number:
		if n, err := strconv.ParseUint(v.String(), 10, 64); err == nil {
			return n, true
		}
		if f, err := v.Float64(); err == nil && f >= 0 {
			return uint64(f), true
		}
		return 0, false
	default:
		return 0, false
	}
}

func toFloat64(value any) (float64, bool) {
	switch v := value.(type) {
	case json.Number:
		f, err := v.Float64()
		if err != nil {
			return 0, false
		}
		return f, true
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, false
		}
		return f, true
	default:
		return 0, false
	}
}

// logDepthExceeded records a subtree skipped for exceeding MaxDepth.
func logDepthExceeded(desc protoreflect.MessageDescriptor, depth int) {
	log.Debug().
		Str("message", string(desc.FullName())).
		Int("depth", depth).
		Msg("pblite: skipping nested message beyond the maximum decode depth")
}

func logSkippedField(fd protoreflect.FieldDescriptor, value any) {
	log.Debug().
		Str("field", string(fd.FullName())).
		Int("field_number", int(fd.Number())).
		Interface("value", value).
		Msg("pblite: skipping undecodable field value")
}

// logUnknownField gates the debug log: only log when the value is
// non-trivial, so routine null-padding-equivalent noise ("", 0, [], nil)
// from fields we don't know about doesn't spam logs.
func logUnknownField(desc protoreflect.MessageDescriptor, fieldNumber int, value any) {
	if isTrivialValue(value) {
		return
	}
	log.Debug().
		Str("message_type", string(desc.FullName())).
		Int("field_number", fieldNumber).
		Interface("value", value).
		Msg("pblite: skipping unknown field")
}

func isTrivialValue(v any) bool {
	switch t := v.(type) {
	case nil:
		return true
	case string:
		return t == ""
	case json.Number:
		f, err := t.Float64()
		return err == nil && f == 0
	case bool:
		// false counts as trivial, the same as the numeric zero.
		return !t
	case []any:
		return len(t) == 0
	default:
		return false
	}
}

// --- encode ---

// encodeMessage encodes every populated field of m into a dense,
// null-padded slice sized to the highest populated field number (padding
// only up to the fields actually present, not the full schema).
func encodeMessage(m protoreflect.Message) ([]any, error) {
	maxFieldNumber := 0
	m.Range(func(fd protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
		if n := int(fd.Number()); n > maxFieldNumber {
			maxFieldNumber = n
		}
		return true
	})
	result := make([]any, maxFieldNumber)
	var encodeErr error
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		encoded, err := encodeFieldValue(fd, v)
		if err != nil {
			encodeErr = fmt.Errorf("pblite: field %s: %w", fd.FullName(), err)
			return false
		}
		result[fd.Number()-1] = encoded
		return true
	})
	if encodeErr != nil {
		return nil, encodeErr
	}
	return result, nil
}

func encodeFieldValue(fd protoreflect.FieldDescriptor, v protoreflect.Value) (any, error) {
	if fd.IsMap() {
		return nil, fmt.Errorf("pblite: map fields are not supported: %s", fd.FullName())
	}
	if fd.IsList() {
		list := v.List()
		out := make([]any, list.Len())
		for i := 0; i < list.Len(); i++ {
			encoded, err := encodeValue(fd, list.Get(i))
			if err != nil {
				return nil, err
			}
			out[i] = encoded
		}
		return out, nil
	}
	return encodeValue(fd, v)
}

func encodeValue(fd protoreflect.FieldDescriptor, v protoreflect.Value) (any, error) {
	switch fd.Kind() {
	case protoreflect.MessageKind:
		return encodeMessage(v.Message())
	case protoreflect.BytesKind:
		return base64.StdEncoding.EncodeToString(v.Bytes()), nil
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		// JS compat: 64-bit ints as strings.
		return strconv.FormatInt(v.Int(), 10), nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return strconv.FormatUint(v.Uint(), 10), nil
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return v.Int(), nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return v.Uint(), nil
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return v.Float(), nil
	case protoreflect.EnumKind:
		return int32(v.Enum()), nil
	case protoreflect.BoolKind:
		return v.Bool(), nil
	case protoreflect.StringKind:
		return v.String(), nil
	default:
		return nil, fmt.Errorf("unsupported field kind %s", fd.Kind())
	}
}
