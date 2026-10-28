package matrixfmt

import (
	"fmt"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// BodyRange is one entity's UTF-16 code-unit span plus its formatting value.
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

// Offset shifts the range's start without changing its length.
func (b BodyRange) Offset(offset int) *BodyRange {
	b.Start += offset
	return &b
}

// TruncateStart moves the start forward to startAt if it is earlier.
func (b BodyRange) TruncateStart(startAt int) *BodyRange {
	if b.Start < startAt {
		b.Length -= startAt - b.Start
		b.Start = startAt
	}
	return &b
}

// TruncateEnd shortens the range to end at or before maxEnd.
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
