package gchatmeow

// GroupId parts and microsecond timestamp helpers, ported from
// _reference/googlechat-python/maugclib/parsers.py (49 lines):
//
//   - id_from_group_id / group_id_from_id: check which oneof arm of GroupId
//     (dm_id vs space_id) is set and read/write the inner string. Python
//     fuses this with "dm:"/"space:" string prefixing; here that prefixing
//     is left to pkg/gcid (this package only deals in *pb.GroupId <-> raw
//     id + isDM), so GroupIDToParts/PartsToGroupID cover only the proto
//     oneof <-> parts half of those two functions.
//   - from_timestamp / to_timestamp: Google Chat timestamps are
//     microseconds since the Unix epoch (UTC). MicrosToTime/TimeToMicros
//     port these using integer arithmetic (time.UnixMicro/UnixMicro),
//     which avoids the float64-precision path Python's
//     `datetime.timestamp() * 1000000` takes and round-trips exactly for
//     the int64 microsecond range Google Chat uses.

import (
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// GroupIDToParts extracts the inner id and DM/space kind from a *pb.GroupId
// oneof (dm_id / space_id).
//
// Mirrors parsers.py's id_from_group_id, minus the "dm:"/"space:" string
// prefixing (that belongs to pkg/gcid). ok is false when gid is nil or
// neither oneof arm is set (Python's HasField("dm_id") and
// HasField("space_id") both false, i.e. the `else: return ""` branch).
func GroupIDToParts(gid *pb.GroupId) (id string, isDM bool, ok bool) {
	if gid == nil {
		return "", false, false
	}
	if dm := gid.GetDmId(); dm != nil {
		return dm.GetDmId(), true, true
	}
	if sp := gid.GetSpaceId(); sp != nil {
		return sp.GetSpaceId(), false, true
	}
	return "", false, false
}

// PartsToGroupID builds a *pb.GroupId oneof from an id and DM/space kind.
//
// Mirrors parsers.py's group_id_from_id, minus the "dm:"/"space:" prefix
// parsing (that belongs to pkg/gcid, which already knows isDM by the time
// it calls this).
func PartsToGroupID(id string, isDM bool) *pb.GroupId {
	if isDM {
		return &pb.GroupId{Id: &pb.GroupId_DmId{DmId: &pb.DmId{DmId: proto.String(id)}}}
	}
	return &pb.GroupId{Id: &pb.GroupId_SpaceId{SpaceId: &pb.SpaceId{SpaceId: proto.String(id)}}}
}

// MicrosToTime converts a microsecond-since-epoch timestamp (as used
// throughout the Google Chat API) to a UTC time.Time.
//
// Ports parsers.py's from_timestamp. MicrosToTime(0) is the Unix epoch,
// same as Python's from_timestamp(0).
func MicrosToTime(micros int64) time.Time {
	return time.UnixMicro(micros).UTC()
}

// TimeToMicros converts a time.Time to a microsecond-since-epoch timestamp.
//
// Ports parsers.py's to_timestamp, using integer microsecond arithmetic
// instead of Python's float64 `datetime.timestamp() * 1000000` to avoid
// float rounding error; both round-trip exactly for realistic Google Chat
// timestamps.
func TimeToMicros(t time.Time) int64 {
	return t.UnixMicro()
}
