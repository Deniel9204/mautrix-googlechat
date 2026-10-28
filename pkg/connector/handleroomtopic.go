package connector

// update_group's SPACE_DETAILS mask replaces description AND guidelines, so guidelines are read
// back (get_group) and resent; if that read fails the change is refused. Spaces only.
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

var _ bridgev2.RoomTopicHandlingNetworkAPI = (*GChatClient)(nil)

func (c *GChatClient) HandleMatrixRoomTopic(ctx context.Context, msg *bridgev2.MatrixRoomTopic) (bool, error) {
	group, err := gcid.ParsePortalID(msg.Portal.ID)
	if err != nil {
		return false, fmt.Errorf("googlechat: %w", err)
	}
	if group.IsDM {
		return false, errors.New("googlechat: a DM has no description")
	}

	current, err := c.getGroup(ctx, group)
	if err != nil {
		return false, fmt.Errorf("googlechat: reading the space's current details failed, not changing them: %w", err)
	}
	details := &pb.GroupDetails{Description: proto.String(msg.Content.Topic)}
	if existing := current.GetGroup().GetGroupDetails(); existing != nil && existing.Guidelines != nil {
		details.Guidelines = proto.String(existing.GetGuidelines())
	}

	send := c.updateGroupFn
	if send == nil {
		conn := c.getConn()
		if conn == nil {
			return false, errors.New("googlechat: not connected")
		}
		send = conn.UpdateGroup
	}
	if _, err := send(ctx, &pb.UpdateGroupRequest{
		SpaceId:      gchatmeow.SpaceID(group.ID),
		SpaceDetails: details,
		UpdateMasks:  []pb.UpdateGroupRequest_UpdateMask{pb.UpdateGroupRequest_SPACE_DETAILS},
	}); err != nil {
		return false, fmt.Errorf("googlechat: update_group failed: %w", err)
	}

	msg.Portal.Topic = msg.Content.Topic
	msg.Portal.TopicSet = true
	return true, nil
}
