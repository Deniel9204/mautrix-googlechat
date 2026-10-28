// Package pblite implements Google Chat's pblite codec: a JSON array where index i holds field i+1,
// optionally ending in a sparse {fieldNumber: value} object. Decoding is permissive: bad fields are logged and skipped.
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

// Unmarshal decodes a pblite JSON array into msg; only a non-array payload is an error.
func Unmarshal(data []byte, msg proto.Message) error {
	// UseNumber keeps int64 timestamps exact instead of rounding through float64.
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

// Marshal encodes msg as a dense pblite array, with 64-bit ints as JSON strings.
func Marshal(msg proto.Message) ([]byte, error) {
	values, err := encodeMessage(msg.ProtoReflect())
	if err != nil {
		return nil, err
	}
	return json.Marshal(values)
}

type fieldEntry struct {
	number int
	value  any
}

func splitTrailingDict(arr []any) (positional []any, dict map[string]any) {
	if len(arr) == 0 {
		return arr, nil
	}
	if d, ok := arr[len(arr)-1].(map[string]any); ok {
		return arr[:len(arr)-1], d
	}
	return arr, nil
}

// buildFieldEntries returns positional entries then sparse-dict entries, both ascending,
// which makes oneof last-set-wins deterministic.
func buildFieldEntries(arr []any) []fieldEntry {
	positional, dict := splitTrailingDict(arr)
	entries := make([]fieldEntry, 0, len(positional)+len(dict))
	for i, v := range positional {
		entries = append(entries, fieldEntry{number: i + 1, value: v})
	}
	if len(dict) > 0 {
		extra := make([]fieldEntry, 0, len(dict))
		for k, v := range dict {
			// Parse as 32-bit so a huge key cannot wrap into a real field number.
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

// MaxDepth caps nested-message recursion; Message.last_reply is self-referential.
const MaxDepth = 64

// decodeMessage decodes arr into m, logging and skipping every problem.
func decodeMessage(arr []any, m protoreflect.Message) {
	decodeMessageAt(arr, m, 0)
}

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
			// No map fields exist today; skip rather than panic on one.
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

// decodeListField skips only the bad elements of a repeated field.
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

// toInt64 accepts a json.Number or decimal string, falling back to a float parse.
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

// logUnknownField skips logging trivial values ("", 0, false, [], nil).
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
		return !t
	case []any:
		return len(t) == 0
	default:
		return false
	}
}

// encodeMessage encodes m as a slice null-padded up to the highest populated field.
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
