package gchatmeow

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// Behavior under test:
//   - oneof presence check on dm_id/space_id, "dm:"/"space:" prefixing.
//     GroupIDToParts/PartsToGroupID split that into (id, isDM) parts -- the
//     "dm:"/"space:" prefixing itself belongs to pkg/gcid, not here.
//   - microsecond timestamp <-> UTC datetime, integer (not float) precision.

func TestGroupIDToParts_Nil(t *testing.T) {
	id, isDM, ok := GroupIDToParts(nil)
	if id != "" || isDM != false || ok != false {
		t.Fatalf("GroupIDToParts(nil) = (%q, %v, %v), want (\"\", false, false)", id, isDM, ok)
	}
}

func TestGroupIDToParts_EmptyGroupID(t *testing.T) {
	// Neither oneof arm set: dm_id and space_id are both absent.
	id, isDM, ok := GroupIDToParts(&pb.GroupId{})
	if id != "" || isDM != false || ok != false {
		t.Fatalf("GroupIDToParts(empty) = (%q, %v, %v), want (\"\", false, false)", id, isDM, ok)
	}
}

func TestGroupIDToParts_DM(t *testing.T) {
	gid := &pb.GroupId{Id: &pb.GroupId_DmId{DmId: &pb.DmId{DmId: proto.String("112233")}}}
	id, isDM, ok := GroupIDToParts(gid)
	if !ok {
		t.Fatalf("GroupIDToParts(dm) ok = false, want true")
	}
	if !isDM {
		t.Fatalf("GroupIDToParts(dm) isDM = false, want true")
	}
	if id != "112233" {
		t.Fatalf("GroupIDToParts(dm) id = %q, want %q", id, "112233")
	}
}

func TestGroupIDToParts_Space(t *testing.T) {
	gid := &pb.GroupId{Id: &pb.GroupId_SpaceId{SpaceId: &pb.SpaceId{SpaceId: proto.String("AAAAspace")}}}
	id, isDM, ok := GroupIDToParts(gid)
	if !ok {
		t.Fatalf("GroupIDToParts(space) ok = false, want true")
	}
	if isDM {
		t.Fatalf("GroupIDToParts(space) isDM = true, want false")
	}
	if id != "AAAAspace" {
		t.Fatalf("GroupIDToParts(space) id = %q, want %q", id, "AAAAspace")
	}
}

func TestGroupIDToParts_DMSetButEmptyInner(t *testing.T) {
	// dm_id submessage present but its own dm_id string unset: presence of
	// dm_id is still true, and group_id.dm_id.dm_id reads as "".
	gid := &pb.GroupId{Id: &pb.GroupId_DmId{DmId: &pb.DmId{}}}
	id, isDM, ok := GroupIDToParts(gid)
	if !ok || !isDM || id != "" {
		t.Fatalf("GroupIDToParts(dm w/ empty inner) = (%q, %v, %v), want (\"\", true, true)", id, isDM, ok)
	}
}

func TestPartsToGroupID_DM(t *testing.T) {
	gid := PartsToGroupID("998877", true)
	dm, ok := gid.GetId().(*pb.GroupId_DmId)
	if !ok {
		t.Fatalf("PartsToGroupID(isDM=true).Id = %T, want *pb.GroupId_DmId", gid.GetId())
	}
	if got := dm.DmId.GetDmId(); got != "998877" {
		t.Fatalf("PartsToGroupID(isDM=true) dm_id = %q, want %q", got, "998877")
	}
}

func TestPartsToGroupID_Space(t *testing.T) {
	gid := PartsToGroupID("BBBBspace", false)
	sp, ok := gid.GetId().(*pb.GroupId_SpaceId)
	if !ok {
		t.Fatalf("PartsToGroupID(isDM=false).Id = %T, want *pb.GroupId_SpaceId", gid.GetId())
	}
	if got := sp.SpaceId.GetSpaceId(); got != "BBBBspace" {
		t.Fatalf("PartsToGroupID(isDM=false) space_id = %q, want %q", got, "BBBBspace")
	}
}

func TestGroupIDRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		id   string
		isDM bool
	}{
		{"dm", "112233", true},
		{"space", "AAAAspace", false},
		{"dm empty id", "", true},
		{"space empty id", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gid := PartsToGroupID(tt.id, tt.isDM)
			id, isDM, ok := GroupIDToParts(gid)
			if !ok {
				t.Fatalf("round-trip ok = false, want true")
			}
			if id != tt.id || isDM != tt.isDM {
				t.Fatalf("round-trip = (%q, %v), want (%q, %v)", id, isDM, tt.id, tt.isDM)
			}
		})
	}
}

func TestMicrosToTime_Zero(t *testing.T) {
	got := MicrosToTime(0)
	want := time.Unix(0, 0).UTC()
	if !got.Equal(want) {
		t.Fatalf("MicrosToTime(0) = %v, want %v", got, want)
	}
	if got.Location() != time.UTC {
		t.Fatalf("MicrosToTime(0).Location() = %v, want UTC", got.Location())
	}
}

func TestMicrosToTime_SubSecond(t *testing.T) {
	// 1 second + 123456 microseconds after epoch.
	got := MicrosToTime(1_123_456)
	want := time.Date(1970, 1, 1, 0, 0, 1, 123456000, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("MicrosToTime(1123456) = %v, want %v", got, want)
	}
}

func TestTimeToMicros_Zero(t *testing.T) {
	got := TimeToMicros(time.Unix(0, 0).UTC())
	if got != 0 {
		t.Fatalf("TimeToMicros(epoch) = %d, want 0", got)
	}
}

func TestMicrosTimeRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		micros int64
	}{
		{"epoch", 0},
		{"one second", 1_000_000},
		{"sub-second remainder", 1_123_456},
		// Realistic 16-digit microsecond timestamp: 2025-01-01T00:00:00Z.
		{"realistic 16-digit", 1735689600000000},
		// Same, with a non-zero microsecond remainder (as GC message
		// timestamps typically have).
		{"realistic w/ remainder", 1735689600123456},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := TimeToMicros(MicrosToTime(tt.micros))
			if got != tt.micros {
				t.Fatalf("TimeToMicros(MicrosToTime(%d)) = %d, want %d", tt.micros, got, tt.micros)
			}
		})
	}
}
