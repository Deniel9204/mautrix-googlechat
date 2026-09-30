package connector

// handlemute.go -- Matrix -> Google Chat per-chat mute (HandleMute): the
// built-in mute/unmute commands, and a client's mute action. Issue #31.
//
// Sent with update_group_notification_settings, copied from captures of the
// web client (2026-09-29): the conversation, a type value (5 for a space, 4
// for a DM -- its meaning is unknown, the values are what the web client
// sends) and the mute state (2 muted, 1 unmuted). It is per conversation;
// SetDndDuration, the obvious-looking alternative, silences the whole
// account and must not be used for this.
//
// A Google Chat mute has no end time, so a timed mute is refused rather than
// silently made permanent. A mute set in a Matrix client's own UI (a push
// rule) never reaches the bridge; that is a Matrix limitation.
import (
	"context"
	"errors"
	"fmt"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

var _ bridgev2.MuteHandlingNetworkAPI = (*GChatClient)(nil)

// Captured type values of GroupNotificationSettingsUpdate.setting_type.
const (
	notificationSettingTypeSpace = 5
	notificationSettingTypeDM    = 4
)

func (c *GChatClient) HandleMute(ctx context.Context, msg *bridgev2.MatrixMute) error {
	group, err := gcid.ParsePortalID(msg.Portal.ID)
	if err != nil {
		return fmt.Errorf("googlechat: %w", err)
	}
	state := pb.GroupNotificationSettingsUpdate_Mute_UNMUTED
	if msg.Content.IsMuted() {
		if msg.Content.MutedUntil > 0 {
			return errors.New("googlechat: Google Chat mutes have no end time; use mute without a duration, and unmute when you want")
		}
		state = pb.GroupNotificationSettingsUpdate_Mute_MUTED
	}
	settingType := int32(notificationSettingTypeSpace)
	if group.IsDM {
		settingType = notificationSettingTypeDM
	}

	send := c.updateNotificationSettingsFn
	if send == nil {
		conn := c.getConn()
		if conn == nil {
			return errors.New("googlechat: not connected")
		}
		send = conn.UpdateGroupNotificationSettings
	}
	if _, err := send(ctx, &pb.UpdateGroupNotificationSettingsRequest{
		GroupId: gchatmeow.PartsToGroupID(group.ID, group.IsDM),
		Update: &pb.GroupNotificationSettingsUpdate{
			SettingType: &settingType,
			Mute:        &pb.GroupNotificationSettingsUpdate_Mute{State: state.Enum()},
		},
	}); err != nil {
		return fmt.Errorf("googlechat: update_group_notification_settings failed: %w", err)
	}
	return nil
}

// mutedUntilFromReadState maps a chat's notification settings from the
// chat list to bridgev2's mute value. The mute is settings field 3, the same
// Mute message the update writes (live-verified): present and MUTED on a
// muted chat, absent once unmuted. A chat whose settings Google Chat never
// sent is left alone (nil) rather than unmuted.
func mutedUntilFromReadState(rs *pb.GroupReadState) *bridgev2.UserLocalPortalInfo {
	settings := rs.GetNotificationSettings()
	if settings == nil {
		return nil
	}
	if settings.GetMute().GetState() == pb.GroupNotificationSettingsUpdate_Mute_MUTED {
		return &bridgev2.UserLocalPortalInfo{MutedUntil: &event.MutedForever}
	}
	return &bridgev2.UserLocalPortalInfo{MutedUntil: &bridgev2.Unmuted}
}
