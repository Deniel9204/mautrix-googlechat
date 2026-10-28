package connector

// Per-chat mute uses update_group_notification_settings; must not use SetDndDuration, which
// silences the whole account. Google Chat mutes have no end time, so timed mutes are refused.
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

// Captured setting_type values (meaning unknown): 5 for a space, 4 for a DM.
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

// mutedUntilFromReadState maps notification settings field 3 (Mute) to bridgev2's mute; nil if absent.
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
