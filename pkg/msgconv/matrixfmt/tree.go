package matrixfmt

import (
	"fmt"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// BodyRange is one formatting entity's UTF-16 code-unit span plus the value
// that describes what kind of formatting it is. It is the Go analogue of
// mautrix.util.formatter.entity_string.SimpleEntity (offset/length/
// extra_info) restricted to what EntityString (html.go) needs: adjusting a
// span when the string it covers is split, trimmed, or concatenated with
// other strings. This mirrors SimpleEntity.adjust_offset's three call
// sites (append/prepend/split/join in entity_string.py) exactly, just
// spelled as three small methods instead of one parameterized one.
type BodyRange struct {
	Start  int
	Length int
	Value  BodyRangeValue
}

// BodyRangeList is the Entities field type of EntityString.
type BodyRangeList []BodyRange

func (b BodyRange) String() string {
	return fmt.Sprintf("%d:%d:%v", b.Start, b.Length, b.Value)
}

// End returns the end index of the range (exclusive).
func (b BodyRange) End() int {
	return b.Start + b.Length
}

// Offset shifts the start of the range without affecting its length. Used
// whenever a string this range belongs to is concatenated after some other
// string of known length (Append/Join).
func (b BodyRange) Offset(offset int) *BodyRange {
	b.Start += offset
	return &b
}

// TruncateStart moves the range's start forward to startAt (shortening it
// correspondingly) if it currently starts before startAt. Used by Split/
// TrimSpace when discarding a leading slice of the string.
func (b BodyRange) TruncateStart(startAt int) *BodyRange {
	if b.Start < startAt {
		b.Length -= startAt - b.Start
		b.Start = startAt
	}
	return &b
}

// TruncateEnd shortens the range so it ends at or before maxEnd. Used by
// Split/TrimSpace when discarding a trailing slice of the string.
func (b BodyRange) TruncateEnd(maxEnd int) *BodyRange {
	if b.End() > maxEnd {
		b.Length = maxEnd - b.Start
	}
	return &b
}

// Proto builds this range's *pb.Annotation.
func (b BodyRange) Proto() *pb.Annotation {
	return b.Value.Annotation(int32(b.Start), int32(b.Length))
}
