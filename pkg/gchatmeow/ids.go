package gchatmeow

import (
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// GroupIDToParts extracts the inner id and DM flag from a GroupId; "dm:"/"space:" prefixing is pkg/gcid's job.
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
func PartsToGroupID(id string, isDM bool) *pb.GroupId {
	if isDM {
		return &pb.GroupId{Id: &pb.GroupId_DmId{DmId: &pb.DmId{DmId: proto.String(id)}}}
	}
	return &pb.GroupId{Id: &pb.GroupId_SpaceId{SpaceId: &pb.SpaceId{SpaceId: proto.String(id)}}}
}

// SpaceID builds a bare *pb.SpaceId, for requests that take a space directly.
func SpaceID(id string) *pb.SpaceId {
	return &pb.SpaceId{SpaceId: proto.String(id)}
}

// UserMemberID builds a *pb.MemberId identifying a user by gaia id.
func UserMemberID(gaia string) *pb.MemberId {
	return &pb.MemberId{Id: &pb.MemberId_UserId{UserId: &pb.UserId{Id: proto.String(gaia)}}}
}

// UserInviteeMemberInfo builds a *pb.InviteeMemberInfo identifying a user by gaia id.
func UserInviteeMemberInfo(gaia string) *pb.InviteeMemberInfo {
	return &pb.InviteeMemberInfo{Id: &pb.InviteeMemberInfo_InviteeInfo{
		InviteeInfo: &pb.InviteeInfo{UserId: &pb.UserId{Id: proto.String(gaia)}},
	}}
}

// MicrosToTime converts a microsecond Unix timestamp to a UTC time.Time.
func MicrosToTime(micros int64) time.Time {
	return time.UnixMicro(micros).UTC()
}

// TimeToMicros converts a time.Time to a microsecond Unix timestamp.
func TimeToMicros(t time.Time) int64 {
	return t.UnixMicro()
}

// UserID wraps a gaia id as a *pb.UserId.
func UserID(gaia string) *pb.UserId {
	return &pb.UserId{Id: &gaia}
}

// EmailInvitee wraps an email as a *pb.InviteeInfo; the private API has no email-to-gaia lookup.
func EmailInvitee(email string) *pb.InviteeInfo {
	return &pb.InviteeInfo{Email: &email}
}
