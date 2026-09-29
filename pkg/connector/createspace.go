package connector

// createspace.go -- creating a Google Chat space from Matrix
// (GroupCreatingNetworkAPI, driven by the create-group command).
//
// The request copies what the Google Chat web client sends (captured and
// live-verified 2026-09-29): the space is created EMPTY -- name plus the room
// type attribute_checker_group_type=FLAT_ROOM, a client local_id and
// should_find_existing_space=false -- and people are invited afterwards with
// create_membership, the same call Matrix invites already use. Every shape
// tried before sent no room type at all and was refused with a bare 400.
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

// spaceGroupType is the single group type advertised in
// NetworkGeneralCapabilities.Provisioning.GroupCreation.
const spaceGroupType = "space"

func (c *GChatClient) CreateGroup(ctx context.Context, params *bridgev2.GroupCreateParams) (*bridgev2.CreateChatResponse, error) {
	log := zerolog.Ctx(ctx)
	if params.Name == nil || params.Name.Name == "" {
		return nil, errors.New("googlechat: a space needs a name -- set the Matrix room's name first")
	}

	// Give the bridge bot the power it needs in the command's room before
	// anything exists on Google Chat, so a refusal here creates nothing.
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

	// The command runs in an existing Matrix room, and that room must become
	// the space's portal: provisionutil only ever creates a NEW room for a
	// portal without one. Bound before inviting anyone, so the window in which
	// the space's own events can create a room is as short as possible; a
	// room they did create meanwhile is tombstoned and deleted.
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

	// Invited one by one, like the web client, so one refused invitee (an
	// address outside what the account may reach) does not undo the space.
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

// bindCreatedSpaceRoom makes params.RoomID the new space's portal room and
// syncs the space's info (members, power levels) into it.
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

// createSpace sends create_group in the web client's shape. It is never
// retried: after a 5xx or a transport error the space may already exist.
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

// botPowerLevel is what the bridge bot needs in a room it did not create to
// run it as a portal (room name, topic, power levels, member sync). 100 is
// the most a room creator can grant; rooms the bridge creates give it 9001.
const botPowerLevel = 100

// ensureBotPower raises the bridge bot to botPowerLevel in roomID if needed,
// acting as the user through their double puppet -- the user asked for this
// room to be bridged, and only they can grant power in it.
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

// promoteBot decides whether pl must change for the bot to reach
// botPowerLevel, and applies the change when the user can make it: they need
// a double puppet to act through, and at least botPowerLevel themselves
// (Matrix never lets anyone grant more than their own level).
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

// newSpaceLocalID mimics the web client's create_group local_id: 11
// characters from the URL-safe base64 alphabet.
func newSpaceLocalID() string {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	b := make([]byte, 11)
	_, _ = rand.Read(b)
	for i := range b {
		b[i] = alphabet[int(b[i])%len(alphabet)]
	}
	return string(b)
}
