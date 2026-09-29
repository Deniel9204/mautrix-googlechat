package connector

// createspace.go -- creating a Google Chat space from Matrix
// (GroupCreatingNetworkAPI, driven by the create-group command).
//
// The request copies what the Google Chat web client sends (captured
// 2026-09-29): the space is created EMPTY -- name plus the room type
// attribute_checker_group_type=FLAT_ROOM, a client local_id and
// should_find_existing_space=false -- and people are invited afterwards with
// create_membership, the same call Matrix invites already use. Every shape
// tried before sent no room type at all and was refused with a bare 400.
//
// TEST BUILD: if the minimal shape is refused, the web client's remaining
// fields are added and the request retried (a refused create creates
// nothing). The log names the shape that succeeded; only that one ships.
import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"

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

	resp, shape, err := c.createSpace(ctx, params.Name.Name)
	if err != nil {
		return nil, err
	}
	id, isDM, ok := gchatmeow.GroupIDToParts(resp.GetGroup().GetGroupId())
	if !ok || isDM {
		return nil, fmt.Errorf("googlechat: create_group returned no space id")
	}
	log.Info().Str("space_id", id).Str("request_shape", shape).Msg("googlechat: created a space")
	group := gcid.GroupID{ID: id}

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
		PortalKey:          gcid.MakePortalKey(group, c.UserLogin.ID),
		FailedParticipants: failed,
	}, nil
}

// createSpace sends create_group, trying the web client's request shape from
// minimal to complete and returning the name of the one that was accepted.
func (c *GChatClient) createSpace(ctx context.Context, name string) (*pb.CreateGroupResponse, string, error) {
	send := c.createGroupChatFn
	if send == nil {
		conn := c.getConn()
		if conn == nil {
			return nil, "", errors.New("googlechat: not connected")
		}
		send = conn.CreateGroup
	}
	localID := newSpaceLocalID()
	var lastErr error
	for _, shape := range []struct {
		name  string
		extra func(*pb.CreateGroupRequest)
	}{
		{"minimal (name + FLAT_ROOM + local_id)", func(*pb.CreateGroupRequest) {}},
		{"web extras without field 9", addWebExtras(false)},
		{"web extras with field 9", addWebExtras(true)},
	} {
		space := &pb.SpaceCreationInfo{
			Name:                      proto.String(name),
			AttributeCheckerGroupType: pb.SharedAttributeCheckerGroupType_FLAT_ROOM.Enum(),
		}
		req := &pb.CreateGroupRequest{
			CreationInfo:            &pb.CreateGroupRequest_Space{Space: space},
			LocalId:                 proto.String(localID),
			ShouldFindExistingSpace: proto.Bool(false),
		}
		shape.extra(req)
		resp, err := send(ctx, req)
		if err == nil {
			return resp, shape.name, nil
		}
		// Only a definite refusal (400) is safe to retry: after a 5xx or a
		// transport error the space may already exist, and a second attempt
		// could create a duplicate.
		var status *gchatmeow.UnexpectedStatusError
		if !errors.As(err, &status) || status.Status != 400 {
			return nil, "", fmt.Errorf("googlechat: create_group failed: %w", err)
		}
		zerolog.Ctx(ctx).Warn().Err(err).Str("request_shape", shape.name).Msg("googlechat: create_group refused, trying the next shape")
		lastErr = err
	}
	return nil, "", fmt.Errorf("googlechat: create_group failed: %w", lastErr)
}

// addWebExtras appends the captured web request's fields that are not in the
// schema, as raw wire fields: space fields 10 (empty avatar_info), 15 and 17
// (0), request fields 6 (9) and 8 ({2:16, 6:1000, 7:20}), and optionally
// space field 9 ([[1]], encoded as nested messages).
func addWebExtras(withField9 bool) func(*pb.CreateGroupRequest) {
	return func(req *pb.CreateGroupRequest) {
		var space []byte
		if withField9 {
			inner := protowire.AppendVarint(protowire.AppendTag(nil, 1, protowire.VarintType), 1)
			outer := protowire.AppendBytes(protowire.AppendTag(nil, 1, protowire.BytesType), inner)
			space = protowire.AppendBytes(protowire.AppendTag(space, 9, protowire.BytesType), outer)
		}
		space = protowire.AppendBytes(protowire.AppendTag(space, 10, protowire.BytesType), nil)
		space = protowire.AppendVarint(protowire.AppendTag(space, 15, protowire.VarintType), 0)
		space = protowire.AppendVarint(protowire.AppendTag(space, 17, protowire.VarintType), 0)
		req.GetSpace().ProtoReflect().SetUnknown(space)

		var opts []byte
		opts = protowire.AppendVarint(protowire.AppendTag(opts, 2, protowire.VarintType), 16)
		opts = protowire.AppendVarint(protowire.AppendTag(opts, 6, protowire.VarintType), 1000)
		opts = protowire.AppendVarint(protowire.AppendTag(opts, 7, protowire.VarintType), 20)
		var top []byte
		top = protowire.AppendVarint(protowire.AppendTag(top, 6, protowire.VarintType), 9)
		top = protowire.AppendBytes(protowire.AppendTag(top, 8, protowire.BytesType), opts)
		req.ProtoReflect().SetUnknown(top)
	}
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
