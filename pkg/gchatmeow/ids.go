package gchatmeow

// GroupId parts and microsecond timestamp helpers:
//
//   - check which oneof arm of GroupId (dm_id vs space_id) is set and
//     read/write the inner string. The "dm:"/"space:" string prefixing is
//     left to pkg/gcid (this package only deals in *pb.GroupId <-> raw id +
//     isDM), so GroupIDToParts/PartsToGroupID cover only the proto oneof <->
//     parts half.
//   - Google Chat timestamps are microseconds since the Unix epoch (UTC).
//     MicrosToTime/TimeToMicros use integer arithmetic (time.UnixMicro),
//     which avoids a float64-precision path and round-trips exactly for the
//     int64 microsecond range Google Chat uses.

import (
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// GroupIDToParts extracts the inner id and DM/space kind from a *pb.GroupId
// oneof (dm_id / space_id).
//
// The "dm:"/"space:" string prefixing belongs to pkg/gcid, not here. ok is
// false when gid is nil or neither oneof arm is set.
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
// The "dm:"/"space:" prefix parsing belongs to pkg/gcid, which already knows
// isDM by the time it calls this.
func PartsToGroupID(id string, isDM bool) *pb.GroupId {
	if isDM {
		return &pb.GroupId{Id: &pb.GroupId_DmId{DmId: &pb.DmId{DmId: proto.String(id)}}}
	}
	return &pb.GroupId{Id: &pb.GroupId_SpaceId{SpaceId: &pb.SpaceId{SpaceId: proto.String(id)}}}
}

// SpaceID builds a bare *pb.SpaceId from a plain space id, for the requests
// that take a space directly rather than a GroupId oneof (e.g. update_group).
func SpaceID(id string) *pb.SpaceId {
	return &pb.SpaceId{SpaceId: proto.String(id)}
}

// UserMemberID builds a *pb.MemberId identifying a user by gaia id, for the
// member_ids lists in membership requests.
func UserMemberID(gaia string) *pb.MemberId {
	return &pb.MemberId{Id: &pb.MemberId_UserId{UserId: &pb.UserId{Id: proto.String(gaia)}}}
}

// UserInviteeMemberInfo builds a *pb.InviteeMemberInfo identifying a user by
// gaia id, for the invitee_member_infos list create_membership uses when
// inviting other users into a space.
func UserInviteeMemberInfo(gaia string) *pb.InviteeMemberInfo {
	return &pb.InviteeMemberInfo{Id: &pb.InviteeMemberInfo_InviteeInfo{
		InviteeInfo: &pb.InviteeInfo{UserId: &pb.UserId{Id: proto.String(gaia)}},
	}}
}

// MicrosToTime converts a microsecond-since-epoch timestamp (as used
// throughout the Google Chat API) to a UTC time.Time.
//
// MicrosToTime(0) is the Unix epoch.
func MicrosToTime(micros int64) time.Time {
	return time.UnixMicro(micros).UTC()
}

// TimeToMicros converts a time.Time to a microsecond-since-epoch timestamp.
//
// It uses integer microsecond arithmetic to avoid float rounding error and
// round-trips exactly for realistic Google Chat timestamps.
func TimeToMicros(t time.Time) int64 {
	return t.UnixMicro()
}
