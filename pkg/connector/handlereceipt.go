package connector

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

var _ bridgev2.ReadReceiptHandlingNetworkAPI = (*GChatClient)(nil)

// readTimeMicros picks last_read_time (µs): the target's stored TimestampMicro, else its
// ms Timestamp, else the receipt's timestamp, else now (Go's zero time would go negative).
func readTimeMicros(msg *bridgev2.MatrixReadReceipt) int64 {
	if msg.ExactMessage != nil {
		if meta, ok := msg.ExactMessage.Metadata.(*MessageMetadata); ok && meta != nil && meta.TimestampMicro != 0 {
			return meta.TimestampMicro
		}
		if ts := msg.ExactMessage.Timestamp; !ts.IsZero() {
			return gchatmeow.TimeToMicros(ts)
		}
	} else if ts := msg.Receipt.Timestamp; !ts.IsZero() {
		return gchatmeow.TimeToMicros(ts)
	}
	return gchatmeow.TimeToMicros(time.Now())
}

// HandleMatrixReadReceipt marks the portal's group read on Google Chat via mark_group_readstate.
func (c *GChatClient) HandleMatrixReadReceipt(ctx context.Context, msg *bridgev2.MatrixReadReceipt) error {
	group, err := gcid.ParsePortalID(msg.Portal.ID)
	if err != nil {
		return fmt.Errorf("googlechat: %w", err)
	}

	req := &pb.MarkGroupReadstateRequest{
		Id:           gchatmeow.PartsToGroupID(group.ID, group.IsDM),
		LastReadTime: proto.Int64(readTimeMicros(msg)),
	}

	send := c.markGroupReadstateFn
	if send == nil {
		conn := c.getConn()
		if conn == nil {
			return fmt.Errorf("googlechat: not connected")
		}
		send = conn.MarkGroupReadstate
	}

	if _, err := send(ctx, req); err != nil {
		return fmt.Errorf("googlechat: mark_group_readstate failed: %w", err)
	}
	return nil
}
