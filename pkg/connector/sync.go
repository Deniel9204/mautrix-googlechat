package connector

import (
	"context"
	"sort"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/simplevent"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// syncMaxAttempts bounds syncChats' paginated_world retries before the sync latch is reset.
const syncMaxAttempts = 3

// defaultSyncRetryBackoffBase is the retry delay used when syncRetryBackoffBase is zero; doubles per attempt.
const defaultSyncRetryBackoffBase = 500 * time.Millisecond

// worldSectionPageSize is the world_section_requests page_size; paginated_world needs it to return items.
const worldSectionPageSize = 999

// syncChatItem pairs a world item with whether it is within the initial_chat_sync cap.
type syncChatItem struct {
	Item         *pb.WorldItemLite
	CreatePortal bool
}

// planChatSync sorts items newest first, drops blocked/hidden/non-joined chats, and marks the
// first maxSync by position in the full sorted list (skipped items still count) as CreatePortal.
func planChatSync(items []*pb.WorldItemLite, maxSync int) []syncChatItem {
	sorted := make([]*pb.WorldItemLite, len(items))
	copy(sorted, items)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].GetSortTimestamp() > sorted[j].GetSortTimestamp()
	})

	plan := make([]syncChatItem, 0, len(sorted))
	for i, item := range sorted {
		rs := item.GetReadState()
		if rs.GetBlocked() || rs.GetHideTimestamp() > 0 || rs.GetMembershipState() != pb.MembershipState_MEMBER_JOINED {
			continue
		}
		plan = append(plan, syncChatItem{Item: item, CreatePortal: i < maxSync})
	}
	return plan
}

// syncChats queues a ChatResync for every chat in paginated_world. The caller sets
// syncInProgress before spawning it; tests calling it directly must do the same.
func (c *GChatClient) syncChats(ctx context.Context) {
	log := zerolog.Ctx(ctx)
	defer c.setSyncInProgress(false)

	fetch := c.paginatedWorldFn
	if fetch == nil {
		conn := c.getConn()
		if conn == nil {
			log.Warn().Msg("googlechat: syncChats called with no live connection, skipping")
			c.resetSyncLatch()
			return
		}
		fetch = conn.PaginatedWorld
	}

	resp, err := c.fetchWorldWithRetry(ctx, fetch)
	if err != nil {
		log.Err(err).Msg("googlechat: paginated_world failed after retries, skipping chat-list sync (latch reset so a later Connected transition retries)")
		c.resetSyncLatch()
		return
	}

	// Set RemoteName first: the personal filtering space is named "Google Chat (<RemoteName>)".
	c.updateOwnLoginProfile(ctx)

	maxSync := 0
	if c.Main != nil {
		maxSync = c.Main.Config.InitialChatSync
	}
	plan := planChatSync(resp.GetWorldItems(), maxSync)
	own := c.ownUserID()

	// Drop invites already joined elsewhere before queuing, so an auto-accept echo can't match them.
	if err := c.dropJoinedPendingInvites(ctx, resp.GetWorldItems()); err != nil {
		log.Err(err).Msg("googlechat: failed to update pending space invites")
	}

	for _, entry := range plan {
		id, isDM, ok := gchatmeow.GroupIDToParts(entry.Item.GetGroupId())
		if !ok {
			log.Warn().Msg("googlechat: skipping world item with no usable group id")
			continue
		}
		group := gcid.GroupID{ID: id, IsDM: isDM}
		res := c.queueChatResync(&simplevent.ChatResync{
			EventMeta: simplevent.EventMeta{
				Type:         bridgev2.RemoteEventChatResync,
				PortalKey:    gcid.MakePortalKey(group, c.UserLogin.ID),
				CreatePortal: entry.CreatePortal,
			},
			ChatInfo:        chatInfoFromWorldItem(entry.Item, own),
			LatestMessageTS: gchatmeow.MicrosToTime(entry.Item.GetSortTimestamp()),
		})
		log.Debug().
			Str("gcid", id).
			Bool("is_dm", isDM).
			Bool("create_portal", entry.CreatePortal).
			Str("muted", entry.Item.GetReadState().GetNotificationSettings().GetMute().GetState().String()).
			Any("result", res).
			Msg("googlechat: queued chat-list sync event")
	}
}

func (c *GChatClient) queueChatResync(evt *simplevent.ChatResync) bridgev2.EventHandlingResult {
	if c.queueChatResyncFn != nil {
		return c.queueChatResyncFn(evt)
	}
	return c.UserLogin.QueueRemoteEvent(evt)
}

// fetchWorldWithRetry calls fetch up to syncMaxAttempts times with exponential backoff.
func (c *GChatClient) fetchWorldWithRetry(ctx context.Context, fetch func(context.Context, *pb.PaginatedWorldRequest) (*pb.PaginatedWorldResponse, error)) (*pb.PaginatedWorldResponse, error) {
	log := zerolog.Ctx(ctx)
	backoffBase := c.syncRetryBackoffBase
	if backoffBase <= 0 {
		backoffBase = defaultSyncRetryBackoffBase
	}

	// A world_section_requests entry with a page_size is required, or the server returns no world_items.
	req := &pb.PaginatedWorldRequest{
		FetchFromUserSpaces:  proto.Bool(true),
		FetchOptions:         []pb.PaginatedWorldRequest_FetchOptions{pb.PaginatedWorldRequest_EXCLUDE_GROUP_LITE},
		WorldSectionRequests: []*pb.WorldSectionRequest{{PageSize: proto.Int32(worldSectionPageSize)}},
	}

	var resp *pb.PaginatedWorldResponse
	var err error
	for attempt := 0; attempt < syncMaxAttempts; attempt++ {
		if attempt > 0 {
			if sleepErr := syncSleepOrDone(ctx, backoffBase*time.Duration(uint(1)<<uint(attempt-1))); sleepErr != nil {
				return nil, sleepErr
			}
		}
		resp, err = fetch(ctx, req)
		if err == nil {
			return resp, nil
		}
		log.Err(err).Int("attempt", attempt+1).Int("max_attempts", syncMaxAttempts).
			Msg("googlechat: paginated_world failed, retrying")
	}
	return nil, err
}

// syncSleepOrDone waits for d or until ctx is done.
func syncSleepOrDone(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
