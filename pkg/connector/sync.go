package connector

// sync.go -- chat-list sync: list the "world" (every conversation the
// account is in), then emit one simplevent.ChatResync per chat so bridgev2
// creates/backfill-checks portals for it. Ports mautrix_googlechat/user.py's
// sync() (user.py:609-665).
import (
	"context"
	"sort"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/simplevent"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// syncChatItem pairs one (post-filter) world item with whether it's within
// this login's initial_chat_sync cap.
type syncChatItem struct {
	Item         *pb.WorldItemLite
	CreatePortal bool
}

// planChatSync filters, sorts, and caps a raw PaginatedWorld item list into
// the per-chat sync plan syncChats emits as ChatResync events. Ports
// user.py's sync() loop (user.py:623-641):
//
//   - skip conditions copied verbatim from user.py:630-635 (blocked / hidden
//     (hide_timestamp > 0) / not MEMBER_JOINED);
//   - sort by sort_timestamp descending (user.py:624) -- sort.SliceStable so
//     ties keep the server's original relative order, matching Python
//     list.sort's stability guarantee;
//   - cap at maxSync (bridge.initial_chat_sync, user.py:625): the newest
//     maxSync (post-filter, post-sort) items are marked CreatePortal true,
//     the rest false.
//
// Deviation from user.py's literal loop: Python additionally keeps (marks
// for update/backfill, not creation) every item beyond the cap that ALREADY
// has a local portal.mxid (user.py:638, "if portal.mxid or index <
// max_sync"), and skips the rest outright rather than emitting anything for
// them. This function has no access to which portals already exist (it's a
// pure, unit-testable transform - see task-12-brief.md's testing note on
// this), so it takes the simpler "emit for every post-filter item" approach
// instead: bridgev2's own event dispatch (portal.go's handleRemoteEvent)
// already drops a CreatePortal:false ChatResync when no portal exists yet
// and processes it normally (update+backfill-check) when one does -- the
// same net effect as user.py's gate, just enforced downstream instead of
// here.
func planChatSync(items []*pb.WorldItemLite, maxSync int) []syncChatItem {
	filtered := make([]*pb.WorldItemLite, 0, len(items))
	for _, item := range items {
		rs := item.GetReadState()
		if rs.GetBlocked() || rs.GetHideTimestamp() > 0 || rs.GetMembershipState() != pb.MembershipState_MEMBER_JOINED {
			continue
		}
		filtered = append(filtered, item)
	}
	sort.SliceStable(filtered, func(i, j int) bool {
		return filtered[i].GetSortTimestamp() > filtered[j].GetSortTimestamp()
	})
	plan := make([]syncChatItem, len(filtered))
	for i, item := range filtered {
		plan[i] = syncChatItem{Item: item, CreatePortal: i < maxSync}
	}
	return plan
}

// syncChats lists the world (every conversation) via paginated_world
// (client.py:700, proto_paginated_world) and queues one
// simplevent.ChatResync per (post-filter) chat, matching the request shape
// user.py's sync() builds when called with no limit (user.py:613-619):
// fetch_from_user_spaces=true, fetch_options=[EXCLUDE_GROUP_LITE]. Intended
// to run once Connect reaches CONNECTED (client.go's handleConnState), same
// as Python's on_connect_later calling self.sync() right before pushing
// BridgeStateEvent.CONNECTED (user.py:555-560).
//
// Deviations from user.py's sync(): no `limit` parameter (that's user.py's
// periodic-recheck/reconnect-triggered "small sync", not part of this
// task's scope -- M1 only wires the one-time post-connect sync); no
// prefetch_users batching of DM member GetMembers calls (user.py:627,646 --
// bridgev2 already batches ghost info lookups on its own portal-creation
// path); no update_direct_chats()/m.direct sync at the end (user.py:664 --
// bridgev2 maintains m.direct itself from portal state, this bridge never
// needs to push it directly).
func (c *GChatClient) syncChats(ctx context.Context) {
	log := zerolog.Ctx(ctx)
	conn := c.getConn()
	if conn == nil {
		log.Warn().Msg("googlechat: syncChats called with no live connection, skipping")
		return
	}

	resp, err := conn.PaginatedWorld(ctx, &pb.PaginatedWorldRequest{
		FetchFromUserSpaces: proto.Bool(true),
		FetchOptions:        []pb.PaginatedWorldRequest_FetchOptions{pb.PaginatedWorldRequest_EXCLUDE_GROUP_LITE},
	})
	if err != nil {
		log.Err(err).Msg("googlechat: paginated_world failed, skipping chat-list sync")
		return
	}

	maxSync := 0
	if c.Main != nil {
		maxSync = c.Main.Config.InitialChatSync
	}
	plan := planChatSync(resp.GetWorldItems(), maxSync)
	own := c.ownUserID()

	for _, entry := range plan {
		id, isDM, ok := gchatmeow.GroupIDToParts(entry.Item.GetGroupId())
		if !ok {
			log.Warn().Msg("googlechat: skipping world item with no usable group id")
			continue
		}
		group := gcid.GroupID{ID: id, IsDM: isDM}
		res := c.UserLogin.QueueRemoteEvent(&simplevent.ChatResync{
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
			Any("result", res).
			Msg("googlechat: queued chat-list sync event")
	}
}
