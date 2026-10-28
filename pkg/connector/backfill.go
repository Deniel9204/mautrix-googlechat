package connector

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// catchUpMaxPages bounds catchUp's PAGINATED drain loop against a server that never says COMPLETED.
const catchUpMaxPages = 100

// catchUp replays events since UserLoginMetadata.Revision via catch_up_user, draining all pages through handleGChatEvent.
// Pagination uses a local cursor, not the shared watermark: live events may advance that past the un-drained backlog.
func (c *GChatClient) catchUp(ctx context.Context) {
	log := zerolog.Ctx(ctx)

	if c.isSyncInProgress() {
		log.Debug().Msg("googlechat: catchUp skipped, this conn's first chat-list sync is still running")
		return
	}

	fetch := c.catchUpUserFn
	if fetch == nil {
		conn := c.getConn()
		if conn == nil {
			log.Warn().Msg("googlechat: catchUp called with no live connection, skipping")
			return
		}
		fetch = conn.CatchUpUser
	}

	fromRevision := c.getRevision()
	cursor := fromRevision
	totalEvents := 0
	for page := 0; page < catchUpMaxPages; page++ {
		req := &pb.CatchUpUserRequest{
			Range: &pb.CatchUpRange{
				FromRevisionTimestamp: proto.Int64(cursor),
				// ToRevisionTimestamp unset: catch up to now.
			},
		}

		resp, err := fetch(ctx, req)
		if err != nil {
			log.Err(err).Int("page", page).Msg("googlechat: catch_up_user failed, revision watermark left at last drained page (retried on next reconnect)")
			return
		}
		status := resp.GetStatus()
		if status != pb.CatchUpResponse_COMPLETED && status != pb.CatchUpResponse_PAGINATED {
			// ABORTED_*: nothing safe to replay.
			log.Warn().Str("status", status.String()).Int("page", page).Msg("googlechat: catch_up_user did not complete, stopping drain")
			return
		}

		pageStartCursor := cursor
		for _, evt := range resp.GetEvents() {
			// The cursor follows user_revision only; group_revision is a separate revision space and could skip events.
			if r := userRevision(evt); r > cursor {
				cursor = r
			}
			for _, flat := range gchatmeow.SplitEventBodies(evt) {
				totalEvents++
				if res := c.handleGChatEvent(ctx, flat); !res.Success {
					// Stop so the watermark never advances past an unhandled event.
					log.Warn().Int("page", page).Msg("googlechat: caught-up event failed to handle, stopping drain (watermark left at last handled revision, retried on next reconnect)")
					return
				}
			}
		}

		if status == pb.CatchUpResponse_COMPLETED {
			log.Debug().
				Int("event_count", totalEvents).
				Int("pages", page+1).
				Int64("from_revision", fromRevision).
				Int64("to_revision", cursor).
				Msg("googlechat: catch_up_user drain complete")
			return
		}
		// PAGINATED but the cursor did not advance: stop rather than re-request the same page.
		if cursor <= pageStartCursor {
			log.Warn().Int("page", page).Int64("cursor", cursor).Msg("googlechat: catch_up_user reported more pages but the revision cursor did not advance, stopping drain")
			return
		}
	}
	log.Warn().Int("max_pages", catchUpMaxPages).Int("event_count", totalEvents).Int64("to_revision", cursor).Msg("googlechat: catch_up_user still PAGINATED after max pages, stopping drain (next reconnect resumes from the advanced watermark)")
}

// advanceUserRevision persists evt's user_revision as the catch_up_user watermark if newer.
// Never use group_revision here: it is a separate per-group space and would over-advance the watermark.
func (c *GChatClient) advanceUserRevision(ctx context.Context, evt *pb.Event) {
	r := userRevision(evt)
	if r <= 0 {
		return
	}
	if err := c.updateMetadata(ctx, func(meta *UserLoginMetadata) bool {
		if meta.Revision >= r {
			return false
		}
		meta.Revision = r
		return true
	}); err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("googlechat: failed to persist user revision watermark")
	}
}

// advancePortalRevision stores evt's group_revision on the portal's PortalMetadata.Revision if newer.
func (c *GChatClient) advancePortalRevision(ctx context.Context, evt *pb.Event) {
	r := groupRevision(evt)
	if r <= 0 {
		return
	}
	id, isDM, ok := gchatmeow.GroupIDToParts(evt.GetGroupId())
	if !ok {
		return
	}
	key := gcid.MakePortalKey(gcid.GroupID{ID: id, IsDM: isDM}, c.UserLogin.ID)
	c.savePortalRevision(ctx, key, r)
}

// savePortalRevision monotonically updates PortalMetadata.Revision; savePortalRevisionFn is a test seam.
func (c *GChatClient) savePortalRevision(ctx context.Context, key networkid.PortalKey, revision int64) {
	if c.savePortalRevisionFn != nil {
		c.savePortalRevisionFn(ctx, key, revision)
		return
	}
	if c.UserLogin == nil || c.UserLogin.Bridge == nil {
		return
	}
	portal, err := c.UserLogin.Bridge.GetPortalByKey(ctx, key)
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("googlechat: failed to get portal for group-revision watermark")
		return
	}
	meta, ok := portal.Metadata.(*PortalMetadata)
	if !ok || meta == nil || meta.Revision >= revision {
		return
	}
	meta.Revision = revision
	if err := portal.Save(ctx); err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("googlechat: failed to persist portal group-revision watermark")
	}
}

// getRevision reads UserLoginMetadata.Revision under metaMu.
func (c *GChatClient) getRevision() int64 {
	c.metaMu.Lock()
	defer c.metaMu.Unlock()
	meta, ok := c.UserLogin.Metadata.(*UserLoginMetadata)
	if !ok || meta == nil {
		return 0
	}
	return meta.Revision
}

// userRevision returns evt's user_revision timestamp (RevisionType oneof field 6), or 0.
func userRevision(evt *pb.Event) int64 {
	return evt.GetUserRevision().GetTimestamp()
}

// groupRevision returns evt's group_revision timestamp (RevisionType oneof field 7), or 0.
func groupRevision(evt *pb.Event) int64 {
	return evt.GetGroupRevision().GetTimestamp()
}

// region history backfill

// ListMessages can only list replies within one topic, so room history comes from a single ListTopics call
// (no page token exists), taking each topic's first reply as the message.

// defaultFetchMessagesCount is used when bridgev2 passes a non-positive params.Count.
const defaultFetchMessagesCount = 20

var _ bridgev2.BackfillingNetworkAPI = (*GChatClient)(nil)

// FetchMessages implements bridgev2.BackfillingNetworkAPI.
// ThreadRoot is checked before Forward because framework thread backfill always sets Forward=true.
func (c *GChatClient) FetchMessages(ctx context.Context, params bridgev2.FetchMessagesParams) (*bridgev2.FetchMessagesResponse, error) {
	if params.ThreadRoot != "" {
		return c.fetchThreadMessages(ctx, params)
	}
	if params.Forward && params.AnchorMessage != nil {
		// Forward catch-up on an existing room: list_topics can't query newer-than-X, and catch_up_user covers it.
		return &bridgev2.FetchMessagesResponse{HasMore: false}, nil
	}
	// The backward queue and the new-room forward seed (the only path on non-batch-send homeservers) land here.
	return c.fetchTopicHeadMessages(ctx, params)
}

// fetchTopicHeadMessages fetches top-level history in one list_topics call, using each topic's replies[0].
// Topics in a ThreadsOnly portal, or with thread_created_usec > 0, get ShouldBackfillThread.
func (c *GChatClient) fetchTopicHeadMessages(ctx context.Context, params bridgev2.FetchMessagesParams) (*bridgev2.FetchMessagesResponse, error) {
	group, err := gcid.ParsePortalID(params.Portal.ID)
	if err != nil {
		return nil, fmt.Errorf("googlechat: invalid portal id %q: %w", params.Portal.ID, err)
	}

	threadsOnly := false
	if meta, ok := params.Portal.Metadata.(*PortalMetadata); ok && meta != nil {
		threadsOnly = meta.ThreadsOnly
	}

	count := params.Count
	if count <= 0 {
		count = defaultFetchMessagesCount
	}

	list := c.listTopicsFn
	if list == nil {
		conn := c.getConn()
		if conn == nil {
			return nil, fmt.Errorf("googlechat: not connected")
		}
		list = conn.ListTopics
	}
	resp, err := list(ctx, &pb.ListTopicsRequest{
		GroupId:           gchatmeow.PartsToGroupID(group.ID, group.IsDM),
		PageSizeForTopics: proto.Int32(int32(count)),
	})
	if err != nil {
		return nil, fmt.Errorf("googlechat: list_topics failed: %w", err)
	}

	// Server order is newest-first; reverse, then sort by SortTime with topic id as a deterministic tiebreaker.
	topics := slices.Clone(resp.GetTopics())
	slices.Reverse(topics)
	slices.SortStableFunc(topics, func(a, b *pb.Topic) int {
		if bySortTime := cmp.Compare(a.GetSortTime(), b.GetSortTime()); bySortTime != 0 {
			return bySortTime
		}
		return cmp.Compare(a.GetId().GetTopicId(), b.GetId().GetTopicId())
	})

	getIntent := c.getIntentForFn
	if getIntent == nil {
		getIntent = func(ctx context.Context, portal *bridgev2.Portal, sender bridgev2.EventSender, evtType bridgev2.RemoteEventType) (bridgev2.MatrixAPI, bool) {
			return portal.GetIntentFor(ctx, sender, c.UserLogin, evtType)
		}
	}
	convert := convertMessageToMatrix(c.msgConverter(), c)

	hasAnchor := params.AnchorMessage != nil
	var anchorMicros int64
	if hasAnchor {
		anchorMicros = gchatmeow.TimeToMicros(params.AnchorMessage.Timestamp)
	}

	log := zerolog.Ctx(ctx)
	messages := make([]*bridgev2.BackfillMessage, 0, len(topics))
	for _, topic := range topics {
		replies := topic.GetReplies()
		if len(replies) == 0 {
			continue
		}
		msg := replies[0]
		gcMessageID := msg.GetId().GetMessageId()
		if gcMessageID == "" {
			log.Warn().Msg("googlechat: backfill topic head reply has no message id, skipping")
			continue
		}
		msgID := gcid.MakeMessageID(gcMessageID)
		// Drop the anchor and anything strictly newer. Not `>=`: that would drop distinct messages sharing the anchor's microsecond.
		if hasAnchor && (msgID == params.AnchorMessage.ID || msg.GetCreateTime() > anchorMicros) {
			continue
		}
		senderID := gcid.MakeUserID(msg.GetCreator().GetUserId().GetId())
		sender := bridgev2.EventSender{
			Sender:   senderID,
			IsFromMe: c.IsThisUser(ctx, senderID),
		}
		intent, ok := getIntent(ctx, params.Portal, sender, bridgev2.RemoteEventBackfill)
		if !ok {
			continue
		}
		cm, err := convert(ctx, params.Portal, intent, msg)
		if err != nil {
			log.Err(err).Str("gc_message_id", gcMessageID).Msg("googlechat: failed to convert backfill message, skipping")
			continue
		}
		// Reactions left nil: GC's Reaction proto has only counts, no reactor id, so they can't be attributed.
		// Pinned by TestFetchMessagesFlatOmitsReactionsEvenWhenGCMessageHasThem.
		bm := &bridgev2.BackfillMessage{
			ConvertedMessage: cm,
			Sender:           sender,
			ID:               msgID,
			Timestamp:        gchatmeow.MicrosToTime(msg.GetCreateTime()),
			StreamOrder:      msg.GetCreateTime(),
		}
		// LastThreadMessage is not read by bridgev2 yet; set for forward compatibility.
		if isThread := threadsOnly || topic.GetTopicReadState().GetThreadCreatedUsec() > 0; isThread {
			bm.ShouldBackfillThread = true
			bm.LastThreadMessage = msgID
			if last := replies[len(replies)-1]; last != msg {
				if lastID := last.GetId().GetMessageId(); lastID != "" {
					bm.LastThreadMessage = gcid.MakeMessageID(lastID)
				}
			}
		}
		messages = append(messages, bm)
	}

	return &bridgev2.FetchMessagesResponse{
		Messages: messages,
		HasMore:  false,
		Cursor:   "",
	}, nil
}

// endregion

// region threaded backfill (per-topic ListMessages)

// fetchThreadMessages serves ThreadRoot-scoped FetchMessages calls with one list_messages call for the topic.
// The topic id comes from the anchor's MessageMetadata.TopicID, since a message id doesn't encode its topic.
func (c *GChatClient) fetchThreadMessages(ctx context.Context, params bridgev2.FetchMessagesParams) (*bridgev2.FetchMessagesResponse, error) {
	log := zerolog.Ctx(ctx)

	if params.AnchorMessage == nil {
		log.Warn().Msg("googlechat: thread backfill requested with no anchor message, cannot resolve topic id")
		return &bridgev2.FetchMessagesResponse{HasMore: false}, nil
	}
	meta, ok := params.AnchorMessage.Metadata.(*MessageMetadata)
	if !ok || meta == nil || meta.TopicID == "" {
		log.Warn().Msg("googlechat: thread backfill anchor message has no topic id in its metadata, cannot resolve topic")
		return &bridgev2.FetchMessagesResponse{HasMore: false}, nil
	}
	topicID := meta.TopicID

	group, err := gcid.ParsePortalID(params.Portal.ID)
	if err != nil {
		return nil, fmt.Errorf("googlechat: invalid portal id %q: %w", params.Portal.ID, err)
	}

	count := params.Count
	if count <= 0 {
		count = defaultFetchMessagesCount
	}

	list := c.listMessagesFn
	if list == nil {
		conn := c.getConn()
		if conn == nil {
			return nil, fmt.Errorf("googlechat: not connected")
		}
		list = conn.ListMessages
	}
	resp, err := list(ctx, &pb.ListMessagesRequest{
		ParentId: &pb.MessageParentId{
			Parent: &pb.MessageParentId_TopicId{
				TopicId: &pb.TopicId{
					TopicId: proto.String(topicID),
					GroupId: gchatmeow.PartsToGroupID(group.ID, group.IsDM),
				},
			},
		},
		PageSize: proto.Int32(int32(count)),
	})
	if err != nil {
		return nil, fmt.Errorf("googlechat: list_messages failed: %w", err)
	}

	// Sort chronologically by CreateTime, tie-broken by message id.
	msgs := slices.Clone(resp.GetMessages())
	slices.SortStableFunc(msgs, func(a, b *pb.Message) int {
		if byTime := cmp.Compare(a.GetCreateTime(), b.GetCreateTime()); byTime != 0 {
			return byTime
		}
		return cmp.Compare(a.GetId().GetMessageId(), b.GetId().GetMessageId())
	})

	getIntent := c.getIntentForFn
	if getIntent == nil {
		getIntent = func(ctx context.Context, portal *bridgev2.Portal, sender bridgev2.EventSender, evtType bridgev2.RemoteEventType) (bridgev2.MatrixAPI, bool) {
			return portal.GetIntentFor(ctx, sender, c.UserLogin, evtType)
		}
	}
	convert := convertMessageToMatrix(c.msgConverter(), c)

	anchorID := params.AnchorMessage.ID
	anchorMicros := gchatmeow.TimeToMicros(params.AnchorMessage.Timestamp)

	messages := make([]*bridgev2.BackfillMessage, 0, len(msgs))
	for _, msg := range msgs {
		gcMessageID := msg.GetId().GetMessageId()
		if gcMessageID == "" {
			log.Warn().Msg("googlechat: backfill thread message has no message id, skipping")
			continue
		}
		msgID := gcid.MakeMessageID(gcMessageID)
		// Drop the anchor and anything strictly older; same-microsecond siblings survive.
		if msgID == anchorID || msg.GetCreateTime() < anchorMicros {
			continue
		}
		senderID := gcid.MakeUserID(msg.GetCreator().GetUserId().GetId())
		sender := bridgev2.EventSender{
			Sender:   senderID,
			IsFromMe: c.IsThisUser(ctx, senderID),
		}
		intent, ok := getIntent(ctx, params.Portal, sender, bridgev2.RemoteEventBackfill)
		if !ok {
			continue
		}
		cm, err := convert(ctx, params.Portal, intent, msg)
		if err != nil {
			log.Err(err).Str("gc_message_id", gcMessageID).Msg("googlechat: failed to convert backfill thread message, skipping")
			continue
		}
		// Reactions left nil; see fetchTopicHeadMessages.
		messages = append(messages, &bridgev2.BackfillMessage{
			ConvertedMessage: cm,
			Sender:           sender,
			ID:               msgID,
			Timestamp:        gchatmeow.MicrosToTime(msg.GetCreateTime()),
			StreamOrder:      msg.GetCreateTime(),
		})
	}

	return &bridgev2.FetchMessagesResponse{
		Messages: messages,
		HasMore:  false,
		Cursor:   "",
	}, nil
}

// endregion
