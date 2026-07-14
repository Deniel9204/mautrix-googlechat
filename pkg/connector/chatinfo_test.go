package connector

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

const ownID = networkid.UserID("100")

func userIDProto(id string) *pb.UserId {
	return &pb.UserId{Id: proto.String(id)}
}

func dmGroupID(id string) *pb.GroupId {
	return &pb.GroupId{Id: &pb.GroupId_DmId{DmId: &pb.DmId{DmId: proto.String(id)}}}
}

func spaceGroupID(id string) *pb.GroupId {
	return &pb.GroupId{Id: &pb.GroupId_SpaceId{SpaceId: &pb.SpaceId{SpaceId: proto.String(id)}}}
}

// --- chatInfoFromWorldItem ---------------------------------------------

func TestChatInfoFromWorldItemDMSetsOtherUserID(t *testing.T) {
	item := &pb.WorldItemLite{
		GroupId: dmGroupID("dm1"),
		DmMembers: &pb.WorldItemLite_DmMembers{
			Members: []*pb.UserId{userIDProto(string(ownID)), userIDProto("200")},
		},
	}

	info := chatInfoFromWorldItem(item, ownID)

	if info.Type == nil || *info.Type != database.RoomTypeDM {
		t.Fatalf("Type = %v, want RoomTypeDM", info.Type)
	}
	if info.Members == nil {
		t.Fatal("Members = nil, want a member list")
	}
	if info.Members.OtherUserID != networkid.UserID("200") {
		t.Errorf("OtherUserID = %q, want \"200\"", info.Members.OtherUserID)
	}
	if !info.Members.IsFull {
		t.Error("IsFull = false, want true (dm_members is the complete DM member list)")
	}
	if len(info.Members.MemberMap) != 2 {
		t.Errorf("len(MemberMap) = %d, want 2", len(info.Members.MemberMap))
	}
	self, ok := info.Members.MemberMap[ownID]
	if !ok || !self.IsFromMe {
		t.Errorf("MemberMap[ownID] = %+v, ok=%v, want IsFromMe=true", self, ok)
	}
	other, ok := info.Members.MemberMap[networkid.UserID("200")]
	if !ok || other.IsFromMe {
		t.Errorf("MemberMap[other] = %+v, ok=%v, want IsFromMe=false", other, ok)
	}
	if info.Name != nil {
		t.Errorf("Name = %q, want nil (DM names come from the other member's ghost)", *info.Name)
	}
}

func TestChatInfoFromWorldItemDMPrefersJoinedUsersOverDmMembers(t *testing.T) {
	item := &pb.WorldItemLite{
		GroupId: dmGroupID("dm1"),
		ReadState: &pb.GroupReadState{
			JoinedUsers: []*pb.UserId{
				{Id: proto.String(string(ownID)), Type: pb.UserType_HUMAN.Enum()},
				{Id: proto.String("200"), Type: pb.UserType_HUMAN.Enum()},
				{Id: proto.String("999"), Type: pb.UserType_BOT.Enum()},
			},
		},
		// dm_members deliberately different, to prove joined_users wins when present.
		DmMembers: &pb.WorldItemLite_DmMembers{
			Members: []*pb.UserId{userIDProto(string(ownID)), userIDProto("300")},
		},
	}

	info := chatInfoFromWorldItem(item, ownID)

	if info.Members.OtherUserID != networkid.UserID("200") {
		t.Errorf("OtherUserID = %q, want \"200\" (from joined_users, not dm_members)", info.Members.OtherUserID)
	}
	if len(info.Members.MemberMap) != 3 {
		t.Fatalf("len(MemberMap) = %d, want 3 (2 humans + 1 bot)", len(info.Members.MemberMap))
	}
	if _, ok := info.Members.MemberMap[networkid.UserID("999")]; !ok {
		t.Error("bot member missing from MemberMap")
	}
}

func TestChatInfoFromWorldItemDMGroupOfThreeHasNoOtherUser(t *testing.T) {
	item := &pb.WorldItemLite{
		GroupId: dmGroupID("dm1"),
		DmMembers: &pb.WorldItemLite_DmMembers{
			Members: []*pb.UserId{userIDProto(string(ownID)), userIDProto("200"), userIDProto("300")},
		},
	}

	info := chatInfoFromWorldItem(item, ownID)

	if info.Members.OtherUserID != "" {
		t.Errorf("OtherUserID = %q, want empty (3-person DM has no single other user)", info.Members.OtherUserID)
	}
	if len(info.Members.MemberMap) != 3 {
		t.Errorf("len(MemberMap) = %d, want 3", len(info.Members.MemberMap))
	}
}

func TestChatInfoFromWorldItemSpaceSetsNameNoMembers(t *testing.T) {
	item := &pb.WorldItemLite{
		GroupId:  spaceGroupID("space1"),
		RoomName: proto.String("Engineering"),
	}

	info := chatInfoFromWorldItem(item, ownID)

	if info.Type == nil || *info.Type != database.RoomTypeSpace {
		t.Fatalf("Type = %v, want RoomTypeSpace", info.Type)
	}
	if info.Name == nil || *info.Name != "Engineering" {
		t.Errorf("Name = %v, want \"Engineering\"", info.Name)
	}
	if info.Members != nil {
		t.Errorf("Members = %+v, want nil (no membership RPC at sync time for spaces, see doc comment)", info.Members)
	}
}

func TestChatInfoFromWorldItemNoRoomNameFieldLeavesNameNil(t *testing.T) {
	item := &pb.WorldItemLite{GroupId: spaceGroupID("space1")} // room_name entirely absent

	info := chatInfoFromWorldItem(item, ownID)

	if info.Name != nil {
		t.Errorf("Name = %q, want nil when room_name is absent (no update attempted)", *info.Name)
	}
}

func TestChatInfoFromWorldItemExplicitEmptyRoomNameClearsName(t *testing.T) {
	item := &pb.WorldItemLite{GroupId: spaceGroupID("space1"), RoomName: proto.String("")}

	info := chatInfoFromWorldItem(item, ownID)

	if info.Name == nil || *info.Name != "" {
		t.Errorf("Name = %v, want a non-nil pointer to \"\" (room_name explicitly present but empty must still propagate)", info.Name)
	}
}

func TestChatInfoFromWorldItemTopicAlwaysSetEvenWhenAbsent(t *testing.T) {
	item := &pb.WorldItemLite{GroupId: spaceGroupID("space1")} // group_lite/group_details entirely absent

	info := chatInfoFromWorldItem(item, ownID)

	if info.Topic == nil || *info.Topic != "" {
		t.Errorf("Topic = %v, want a non-nil pointer to \"\" (unconditional, matching Python's _update_description)", info.Topic)
	}
}

func TestChatInfoFromWorldItemThreadingFlags(t *testing.T) {
	cases := []struct {
		name               string
		item               *pb.WorldItemLite
		wantOnly, wantFlat bool
	}{
		{
			name: "threaded group sets both",
			item: &pb.WorldItemLite{
				GroupId:       spaceGroupID("s1"),
				ThreadedGroup: &pb.WorldItemLite_ThreadedGroup{},
			},
			wantOnly: true,
			wantFlat: true,
		},
		{
			name: "flat_threads_enabled without threaded_group sets only enabled",
			item: &pb.WorldItemLite{
				GroupId:            spaceGroupID("s2"),
				FlatThreadsEnabled: proto.Bool(true),
			},
			wantOnly: false,
			wantFlat: true,
		},
		{
			name:     "neither set",
			item:     &pb.WorldItemLite{GroupId: spaceGroupID("s3")},
			wantOnly: false,
			wantFlat: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			info := chatInfoFromWorldItem(tc.item, ownID)
			portal := &bridgev2.Portal{Portal: &database.Portal{Metadata: &PortalMetadata{}}}
			if info.ExtraUpdates == nil {
				t.Fatal("ExtraUpdates = nil")
			}
			info.ExtraUpdates(context.Background(), portal)
			meta := portal.Metadata.(*PortalMetadata)
			if meta.ThreadsOnly != tc.wantOnly {
				t.Errorf("ThreadsOnly = %v, want %v", meta.ThreadsOnly, tc.wantOnly)
			}
			if meta.ThreadsEnabled != tc.wantFlat {
				t.Errorf("ThreadsEnabled = %v, want %v", meta.ThreadsEnabled, tc.wantFlat)
			}
		})
	}
}

// --- chatInfoFromGetGroupResponse ---------------------------------------

func membership(userID string) *pb.Membership {
	return &pb.Membership{
		Id: &pb.MembershipId{
			MemberId: &pb.MemberId{Id: &pb.MemberId_UserId{UserId: userIDProto(userID)}},
		},
	}
}

func TestChatInfoFromGetGroupResponseDM(t *testing.T) {
	resp := &pb.GetGroupResponse{
		Group:       &pb.Group{Name: proto.String("ignored for DMs")},
		Memberships: []*pb.Membership{membership(string(ownID)), membership("200")},
	}

	info := chatInfoFromGetGroupResponse(gcid.GroupID{ID: "dm1", IsDM: true}, resp, ownID)

	if info.Type == nil || *info.Type != database.RoomTypeDM {
		t.Fatalf("Type = %v, want RoomTypeDM", info.Type)
	}
	if info.Name != nil {
		t.Errorf("Name = %q, want nil for DMs", *info.Name)
	}
	if info.Members.OtherUserID != networkid.UserID("200") {
		t.Errorf("OtherUserID = %q, want \"200\"", info.Members.OtherUserID)
	}
}

func TestChatInfoFromGetGroupResponseSpace(t *testing.T) {
	resp := &pb.GetGroupResponse{
		Group: &pb.Group{
			Name:         proto.String("Engineering"),
			GroupDetails: &pb.GroupDetails{Description: proto.String("The eng space")},
		},
		Memberships: []*pb.Membership{membership(string(ownID)), membership("200"), membership("300")},
	}

	info := chatInfoFromGetGroupResponse(gcid.GroupID{ID: "space1", IsDM: false}, resp, ownID)

	if info.Type == nil || *info.Type != database.RoomTypeSpace {
		t.Fatalf("Type = %v, want RoomTypeSpace", info.Type)
	}
	if info.Name == nil || *info.Name != "Engineering" {
		t.Errorf("Name = %v, want \"Engineering\"", info.Name)
	}
	if info.Topic == nil || *info.Topic != "The eng space" {
		t.Errorf("Topic = %v, want \"The eng space\"", info.Topic)
	}
	if info.Members.OtherUserID != "" {
		t.Errorf("OtherUserID = %q, want empty for a space", info.Members.OtherUserID)
	}
	if len(info.Members.MemberMap) != 3 {
		t.Errorf("len(MemberMap) = %d, want 3", len(info.Members.MemberMap))
	}
}

func TestChatInfoFromGetGroupResponseNoNameFieldLeavesNameNil(t *testing.T) {
	resp := &pb.GetGroupResponse{Group: &pb.Group{}} // name entirely absent

	info := chatInfoFromGetGroupResponse(gcid.GroupID{ID: "space1", IsDM: false}, resp, ownID)

	if info.Name != nil {
		t.Errorf("Name = %q, want nil when group.name is absent (no update attempted)", *info.Name)
	}
}

func TestChatInfoFromGetGroupResponseExplicitEmptyNameClears(t *testing.T) {
	resp := &pb.GetGroupResponse{Group: &pb.Group{Name: proto.String("")}}

	info := chatInfoFromGetGroupResponse(gcid.GroupID{ID: "space1", IsDM: false}, resp, ownID)

	if info.Name == nil || *info.Name != "" {
		t.Errorf("Name = %v, want a non-nil pointer to \"\" (name explicitly present but empty must still propagate)", info.Name)
	}
}

func TestChatInfoFromGetGroupResponseTopicAlwaysSetEvenWhenAbsent(t *testing.T) {
	resp := &pb.GetGroupResponse{Group: &pb.Group{}} // group_details entirely absent

	info := chatInfoFromGetGroupResponse(gcid.GroupID{ID: "space1", IsDM: false}, resp, ownID)

	if info.Topic == nil || *info.Topic != "" {
		t.Errorf("Topic = %v, want a non-nil pointer to \"\" (unconditional, matching Python's _update_description)", info.Topic)
	}
}

// --- deriveOtherUserID (direct table test of the shared helper) ----------

func TestDeriveOtherUserID(t *testing.T) {
	cases := []struct {
		name string
		ids  []string
		want networkid.UserID
	}{
		{"two members removes self", []string{"100", "200"}, "200"},
		{"self listed second", []string{"200", "100"}, "200"},
		{"three members no removal attempted", []string{"100", "200", "300"}, ""},
		{"single member (not own) sets it directly", []string{"200"}, "200"},
		{"empty list", nil, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := deriveOtherUserID(tc.ids, ownID)
			if got != tc.want {
				t.Errorf("deriveOtherUserID(%v) = %q, want %q", tc.ids, got, tc.want)
			}
		})
	}
}

// --- GetChatInfo: no-conn error path (RPC itself covered at Task 13) ----

func TestGetChatInfoNoConnIsError(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	gc := &GChatClient{UserLogin: login}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: gcid.MakePortalID(gcid.GroupID{ID: "dm1", IsDM: true})}}}

	_, err := gc.GetChatInfo(context.Background(), portal)
	if err == nil {
		t.Fatal("GetChatInfo with no live conn = nil error, want non-nil")
	}
}

func TestGetChatInfoInvalidPortalIDIsError(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	gc := &GChatClient{UserLogin: login}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: networkid.PortalID("garbage")}}}

	_, err := gc.GetChatInfo(context.Background(), portal)
	if err == nil {
		t.Fatal("GetChatInfo with an invalid portal id = nil error, want non-nil")
	}
}
