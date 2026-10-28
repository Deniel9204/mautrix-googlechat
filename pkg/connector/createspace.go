package connector

// create_group must carry attribute_checker_group_type=FLAT_ROOM or it fails with a bare 400.
import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

var _ bridgev2.GroupCreatingNetworkAPI = (*GChatClient)(nil)

// spaceGroupType is the single group type advertised for group creation.
const spaceGroupType = "space"

func (c *GChatClient) CreateGroup(ctx context.Context, params *bridgev2.GroupCreateParams) (*bridgev2.CreateChatResponse, error) {
	log := zerolog.Ctx(ctx)
	if params.Name == nil || params.Name.Name == "" {
		return nil, errors.New("googlechat: a space needs a name -- set the Matrix room's name first")
	}

	// Promote the bot before anything exists on Google Chat, so a refusal creates nothing.
	if params.RoomID != "" {
		ensure := c.ensureBotPowerFn
		if ensure == nil {
			ensure = c.ensureBotPower
		}
		if err := ensure(ctx, params.RoomID); err != nil {
			return nil, err
		}
	}

	resp, err := c.createSpace(ctx, params.Name.Name)
	if err != nil {
		return nil, err
	}
	id, isDM, ok := gchatmeow.GroupIDToParts(resp.GetGroup().GetGroupId())
	if !ok || isDM {
		return nil, fmt.Errorf("googlechat: create_group returned no space id")
	}
	log.Info().Str("space_id", id).Msg("googlechat: created a space")
	group := gcid.GroupID{ID: id}
	portalKey := gcid.MakePortalKey(group, c.UserLogin.ID)

	// Bind the command's room as the portal before inviting, or provisionutil creates a second room.
	var portal *bridgev2.Portal
	if params.RoomID != "" {
		bind := c.bindCreatedSpaceRoomFn
		if bind == nil {
			bind = c.bindCreatedSpaceRoom
		}
		if portal, err = bind(ctx, portalKey, params); err != nil {
			return nil, fmt.Errorf("googlechat: created space %s but could not bridge it to this room: %w", id, err)
		}
	}

	// Invite one by one so one refused invitee doesn't undo the space.
	var failed map[networkid.UserID]*bridgev2.CreateChatFailedParticipant
	for _, participant := range params.Participants {
		err := c.createMembership(ctx, &pb.CreateMembershipRequest{
			GroupId:            gchatmeow.PartsToGroupID(id, false),
			InviteeMemberInfos: []*pb.InviteeMemberInfo{gchatmeow.UserInviteeMemberInfo(string(participant))},
		})
		if err != nil {
			if failed == nil {
				failed = make(map[networkid.UserID]*bridgev2.CreateChatFailedParticipant)
			}
			failed[participant] = &bridgev2.CreateChatFailedParticipant{Reason: err.Error()}
		}
	}
	return &bridgev2.CreateChatResponse{
		PortalKey:          portalKey,
		Portal:             portal,
		FailedParticipants: failed,
	}, nil
}

// bindCreatedSpaceRoom makes params.RoomID the new space's portal room and syncs its info.
func (c *GChatClient) bindCreatedSpaceRoom(ctx context.Context, key networkid.PortalKey, params *bridgev2.GroupCreateParams) (*bridgev2.Portal, error) {
	portal, err := c.UserLogin.Bridge.GetPortalByKey(ctx, key)
	if err != nil {
		return nil, err
	}
	info, err := c.GetChatInfo(ctx, portal)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Msg("googlechat: get_group failed for the new space, bridging the room without its info")
		info = nil
	}
	return portal, portal.UpdateMatrixRoomID(ctx, params.RoomID, bridgev2.UpdateMatrixRoomIDParams{
		SyncDBMetadata: func() {
			portal.Name = params.Name.Name
			portal.NameSet = true
		},
		OverwriteOldPortal: true,
		TombstoneOldRoom:   true,
		DeleteOldRoom:      true,
		ChatInfo:           info,
		ChatInfoSource:     c.UserLogin,
	})
}

// createSpace sends create_group. Never retried: after a 5xx the space may already exist.
func (c *GChatClient) createSpace(ctx context.Context, name string) (*pb.CreateGroupResponse, error) {
	send := c.createGroupChatFn
	if send == nil {
		conn := c.getConn()
		if conn == nil {
			return nil, errors.New("googlechat: not connected")
		}
		send = conn.CreateGroup
	}
	resp, err := send(ctx, &pb.CreateGroupRequest{
		CreationInfo: &pb.CreateGroupRequest_Space{Space: &pb.SpaceCreationInfo{
			Name:                      proto.String(name),
			AttributeCheckerGroupType: pb.SharedAttributeCheckerGroupType_FLAT_ROOM.Enum(),
		}},
		LocalId:                 proto.String(newSpaceLocalID()),
		ShouldFindExistingSpace: proto.Bool(false),
	})
	if err != nil {
		return nil, fmt.Errorf("googlechat: create_group failed: %w", err)
	}
	return resp, nil
}

// botPowerLevel is the most a room creator can grant; bridge-created rooms give the bot 9001.
const botPowerLevel = 100

// ensureBotPower raises the bot to botPowerLevel via the user's double puppet.
func (c *GChatClient) ensureBotPower(ctx context.Context, roomID id.RoomID) error {
	br := c.UserLogin.Bridge
	pl, err := br.Matrix.GetPowerLevels(ctx, roomID)
	if err != nil {
		return fmt.Errorf("googlechat: reading this room's power levels failed: %w", err)
	}
	dp := c.UserLogin.User.DoublePuppet(ctx)
	changed, err := promoteBot(pl, br.Bot.GetMXID(), c.UserLogin.UserMXID, dp != nil)
	if err != nil || !changed {
		return err
	}
	if _, err := dp.SendState(ctx, roomID, event.StatePowerLevels, "", &event.Content{Parsed: pl}, time.Time{}); err != nil {
		return fmt.Errorf("googlechat: giving the bridge bot power level %d in this room failed: %w", botPowerLevel, err)
	}
	zerolog.Ctx(ctx).Info().Stringer("room_id", roomID).Msg("googlechat: raised the bridge bot's power level in the create-group room")
	return nil
}

// promoteBot raises the bot's level in pl if the user has a double puppet and at least botPowerLevel.
func promoteBot(pl *event.PowerLevelsEventContent, bot, user id.UserID, hasDoublePuppet bool) (bool, error) {
	if pl.GetUserLevel(bot) >= botPowerLevel {
		return false, nil
	}
	if !hasDoublePuppet {
		return false, fmt.Errorf("googlechat: the bridge bot needs power level %d in this room; give %s that level (or set up double puppeting so the bridge can) and retry", botPowerLevel, bot)
	}
	if pl.GetUserLevel(user) < botPowerLevel {
		return false, fmt.Errorf("googlechat: the bridge bot needs power level %d in this room, and you have %d, which is not enough to grant it; ask a room admin to give %s that level and retry", botPowerLevel, pl.GetUserLevel(user), bot)
	}
	pl.SetUserLevel(bot, botPowerLevel)
	return true, nil
}

// newSpaceLocalID mimics the web client's local_id: 11 URL-safe base64 characters.
func newSpaceLocalID() string {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	b := make([]byte, 11)
	_, _ = rand.Read(b)
	for i := range b {
		b[i] = alphabet[int(b[i])%len(alphabet)]
	}
	return string(b)
}
