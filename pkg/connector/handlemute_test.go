package connector

// handlemute_test.go -- HandleMute -> update_group_notification_settings, and
// the chat list's notification state -> bridgev2's mute value (#31).

import (
	"context"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/pblite"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

func matrixMute(portal *bridgev2.Portal, mutedUntil int64) *bridgev2.MatrixMute {
	return &bridgev2.MatrixMute{
		MatrixEventBase: bridgev2.MatrixEventBase[*event.BeeperMuteEventContent]{
			Content: &event.BeeperMuteEventContent{MutedUntil: mutedUntil},
			Portal:  portal,
		},
	}
}

func sentMuteRequest(t *testing.T, portal *bridgev2.Portal, mutedUntil int64) *pb.UpdateGroupNotificationSettingsRequest {
	t.Helper()
	var got *pb.UpdateGroupNotificationSettingsRequest
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		updateNotificationSettingsFn: func(_ context.Context, req *pb.UpdateGroupNotificationSettingsRequest) (*pb.UpdateGroupNotificationSettingsResponse, error) {
			got = req
			return &pb.UpdateGroupNotificationSettingsResponse{}, nil
		},
	}
	if err := gc.HandleMute(context.Background(), matrixMute(portal, mutedUntil)); err != nil {
		t.Fatalf("HandleMute() error = %v", err)
	}
	if got == nil {
		t.Fatal("update_group_notification_settings was not sent")
	}
	return got
}

// The sent request must be exactly what the web client sent in the captures
// (request header aside): decoding the captured pblite into our schema and
// comparing proves both the field numbers and the values.
func TestHandleMuteMatchesWebClientCaptures(t *testing.T) {
	cases := []struct {
		name       string
		portal     *bridgev2.Portal
		mutedUntil int64
		captured   string
	}{
		{"mute space", spacePortal("AAQAiaFvQpo"), -1, `[[["AAQAiaFvQpo"]],[null,5,[2]]]`},
		{"unmute space", spacePortal("AAQAiaFvQpo"), 0, `[[["AAQAiaFvQpo"]],[null,5,[1]]]`},
		{"mute DM", dmPortal("rLcgAwAAAAE"), -1, `[[null,null,["rLcgAwAAAAE"]],[null,4,[2]]]`},
		{"unmute DM", dmPortal("rLcgAwAAAAE"), 0, `[[null,null,["rLcgAwAAAAE"]],[null,4,[1]]]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var want pb.UpdateGroupNotificationSettingsRequest
			if err := pblite.Unmarshal([]byte(tc.captured), &want); err != nil {
				t.Fatalf("decode capture: %v", err)
			}
			if len(want.ProtoReflect().GetUnknown()) != 0 || len(want.GetUpdate().ProtoReflect().GetUnknown()) != 0 {
				t.Fatal("capture has fields the schema does not define")
			}
			got := sentMuteRequest(t, tc.portal, tc.mutedUntil)
			got.RequestHeader = nil
			if !proto.Equal(got, &want) {
				t.Errorf("sent %v\nwant %v (the captured web-client request)", got, &want)
			}
		})
	}
}

func TestHandleMuteRefusesATimedMute(t *testing.T) {
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		updateNotificationSettingsFn: func(context.Context, *pb.UpdateGroupNotificationSettingsRequest) (*pb.UpdateGroupNotificationSettingsResponse, error) {
			t.Error("a timed mute was sent; Google Chat would make it permanent")
			return &pb.UpdateGroupNotificationSettingsResponse{}, nil
		},
	}
	until := time.Now().Add(time.Hour).UnixMilli()
	if err := gc.HandleMute(context.Background(), matrixMute(spacePortal("space1"), until)); err == nil {
		t.Error("HandleMute(1h) = nil error, want a refusal")
	}
}

// A timed mute that has already ended is an unmute, not a refusal.
func TestHandleMuteExpiredTimedMuteUnmutes(t *testing.T) {
	past := time.Now().Add(-time.Hour).UnixMilli()
	got := sentMuteRequest(t, spacePortal("space1"), past)
	if got.GetUpdate().GetMute().GetState() != pb.GroupNotificationSettingsUpdate_Mute_UNMUTED {
		t.Errorf("state = %v, want UNMUTED", got.GetUpdate().GetMute().GetState())
	}
}

func TestHandleMuteRPCFailurePropagates(t *testing.T) {
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		updateNotificationSettingsFn: func(context.Context, *pb.UpdateGroupNotificationSettingsRequest) (*pb.UpdateGroupNotificationSettingsResponse, error) {
			return nil, errors.New("400")
		},
	}
	if err := gc.HandleMute(context.Background(), matrixMute(spacePortal("space1"), -1)); err == nil {
		t.Error("HandleMute() = nil error although the RPC failed")
	}
}

// The settings bytes of two chats, straight from a live chat-list sync
// (2026-09-30): a muted space, and a DM that had been muted and unmuted.
func liveNotificationSettings(t *testing.T, hexBytes string) *pb.GroupReadState {
	t.Helper()
	raw, err := hex.DecodeString(hexBytes)
	if err != nil {
		t.Fatal(err)
	}
	var settings pb.GroupNotificationSettings
	if err := proto.Unmarshal(raw, &settings); err != nil {
		t.Fatal(err)
	}
	if len(settings.ProtoReflect().GetUnknown()) != 0 {
		t.Fatal("live settings carry fields the schema does not define")
	}
	return &pb.GroupReadState{NotificationSettings: &settings}
}

func TestMutedUntilFromReadState(t *testing.T) {
	if got := mutedUntilFromReadState(liveNotificationSettings(t, "080210051a020802")); got == nil || !got.MutedUntil.Equal(event.MutedForever) {
		t.Errorf("live muted space -> %v, want muted forever", got)
	}
	if got := mutedUntilFromReadState(liveNotificationSettings(t, "08021004")); got == nil || !got.MutedUntil.Equal(bridgev2.Unmuted) {
		t.Errorf("live unmuted DM -> %v, want unmuted", got)
	}
	if got := mutedUntilFromReadState(&pb.GroupReadState{}); got != nil {
		t.Errorf("no settings -> %v, want nil (leave the Matrix side alone)", got)
	}
}

func TestChatInfoFromWorldItemCarriesMuteState(t *testing.T) {
	item := worldItem("space1", 100)
	item.ReadState.NotificationSettings = &pb.GroupNotificationSettings{
		Mute: &pb.GroupNotificationSettingsUpdate_Mute{State: pb.GroupNotificationSettingsUpdate_Mute_MUTED.Enum()},
	}
	info := chatInfoFromWorldItem(item, ownID)
	if info.UserLocal == nil || info.UserLocal.MutedUntil == nil || !info.UserLocal.MutedUntil.Equal(event.MutedForever) {
		t.Errorf("UserLocal = %+v, want muted forever", info.UserLocal)
	}
}
