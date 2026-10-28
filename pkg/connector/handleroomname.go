package connector

// Rename is spaces only: UpdateGroupRequest has no DM arm. On failure Portal.Name/NameSet stay untouched.
import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

var _ bridgev2.RoomNameHandlingNetworkAPI = (*GChatClient)(nil)

func (c *GChatClient) HandleMatrixRoomName(ctx context.Context, msg *bridgev2.MatrixRoomName) (bool, error) {
	group, err := gcid.ParsePortalID(msg.Portal.ID)
	if err != nil {
		return false, fmt.Errorf("googlechat: %w", err)
	}
	if group.IsDM {
		return false, errors.New("googlechat: cannot rename a DM")
	}

	newName := msg.Content.Name
	req := &pb.UpdateGroupRequest{
		SpaceId:     gchatmeow.SpaceID(group.ID),
		Name:        proto.String(newName),
		UpdateMasks: []pb.UpdateGroupRequest_UpdateMask{pb.UpdateGroupRequest_NAME},
	}

	send := c.updateGroupFn
	if send == nil {
		conn := c.getConn()
		if conn == nil {
			return false, errors.New("googlechat: not connected")
		}
		send = conn.UpdateGroup
	}

	if _, err := send(ctx, req); err != nil {
		return false, fmt.Errorf("googlechat: update_group failed: %w", err)
	}

	msg.Portal.Name = newName
	msg.Portal.NameSet = true
	return true, nil
}
