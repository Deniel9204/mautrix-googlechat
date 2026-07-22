package connector

// handleroomname.go -- Matrix -> Google Chat outbound space rename
// (HandleMatrixRoomName). Issues the update_group RPC with the space id, the
// new name, and the NAME update mask.
//
// Spaces only: UpdateGroupRequest has no DM arm (its target is a bare
// SpaceId), and DMs have no user-settable name on Google Chat. A rename
// attempted in a DM portal is rejected without an RPC.
//
// Per RoomNameHandlingNetworkAPI's contract (mautrix-go
// bridgev2/networkinterface.go): on success this updates Portal.Name/NameSet
// and returns true; on failure it returns (false, err) and leaves those
// fields untouched, so a failed rename never leaves the portal wrongly
// marked as name-set.
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
