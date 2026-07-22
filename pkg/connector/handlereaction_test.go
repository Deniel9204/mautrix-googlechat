package connector

// handlereaction_test.go -- outbound Matrix reaction <-> update_reaction RPC
// (M4 Task 3): PreHandleMatrixReaction, HandleMatrixReaction (ADD),
// HandleMatrixReactionRemove (REMOVE). Mirrors handleedit_test.go's
// request-construction / error-path test shape, since update_reaction
// builds its MessageId the exact same "thread_id or message_id" way
// edit_message/delete_message do.

import (
	"context"
	"errors"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// matrixReaction builds a *bridgev2.MatrixReaction targeting target with a
// raw Matrix reaction key, matching what bridgev2's own handleMatrixReaction
// hands PreHandleMatrixReaction (mautrix-go bridgev2/portal.go:1607-1617)
// before PreHandleResp exists yet.
func matrixReaction(portal *bridgev2.Portal, target *database.Message, key string) *bridgev2.MatrixReaction {
	return &bridgev2.MatrixReaction{
		MatrixEventBase: bridgev2.MatrixEventBase[*event.ReactionEventContent]{
			Portal: portal,
			Content: &event.ReactionEventContent{
				RelatesTo: event.RelatesTo{Key: key},
			},
		},
		TargetMessage: target,
	}
}

// matrixReactionWithPreResp mirrors what bridgev2's own handleMatrixReaction
// hands HandleMatrixReaction AFTER PreHandleMatrixReaction has already run
// (portal.go:1617,1664): react.PreHandleResp is always non-nil by the time
// HandleMatrixReaction is called.
func matrixReactionWithPreResp(portal *bridgev2.Portal, target *database.Message, emoji string) *bridgev2.MatrixReaction {
	r := matrixReaction(portal, target, emoji)
	r.PreHandleResp = &bridgev2.MatrixReactionPreResponse{
		SenderID: gcid.MakeUserID("112233"),
		EmojiID:  networkid.EmojiID(emoji),
		Emoji:    emoji,
	}
	return r
}

// matrixReactionRemove builds a *bridgev2.MatrixReactionRemove targeting
// target, matching what bridgev2's own handleMatrixRedaction hands
// ReactionHandlingNetworkAPI.HandleMatrixReactionRemove (mautrix-go
// bridgev2/portal.go:2532-2542) after already resolving the redaction's
// target row from the DB by its Matrix event id (content.Redacts) -- so
// TargetReaction is always non-nil here, exactly like
// handleredact_test.go's TargetMessage.
func matrixReactionRemove(portal *bridgev2.Portal, target *database.Reaction) *bridgev2.MatrixReactionRemove {
	return &bridgev2.MatrixReactionRemove{
		MatrixEventBase: bridgev2.MatrixEventBase[*event.RedactionEventContent]{
			Portal:  portal,
			Content: &event.RedactionEventContent{},
		},
		TargetReaction: target,
	}
}

// --- PreHandleMatrixReaction ---------------------------------------------

func TestPreHandleMatrixReactionStripsVariationSelector(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	gc := &GChatClient{UserLogin: login}

	react := matrixReaction(spacePortal("space1"), &database.Message{ID: gcid.MakeMessageID("msg1")}, "❤️") // "❤️" WITH VS-16

	resp, err := gc.PreHandleMatrixReaction(context.Background(), react)
	if err != nil {
		t.Fatalf("PreHandleMatrixReaction() error = %v, want nil", err)
	}
	wantBare := "❤" // "❤" bare
	if resp.Emoji != wantBare {
		t.Errorf("Emoji = %q, want %q (variation selector stripped)", resp.Emoji, wantBare)
	}
	if string(resp.EmojiID) != wantBare {
		t.Errorf("EmojiID = %q, want %q", resp.EmojiID, wantBare)
	}
	if resp.SenderID != gcid.MakeUserID("112233") {
		t.Errorf("SenderID = %q, want %q (this login's own gaia id)", resp.SenderID, gcid.MakeUserID("112233"))
	}
	if resp.MaxReactions != 0 {
		t.Errorf("MaxReactions = %d, want 0 (unlimited -- Google Chat allows multiple distinct emoji per sender)", resp.MaxReactions)
	}
}

func TestPreHandleMatrixReactionAlreadyBareEmojiUnchanged(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	gc := &GChatClient{UserLogin: login}

	react := matrixReaction(spacePortal("space1"), &database.Message{ID: gcid.MakeMessageID("msg1")}, "\U0001F44D") // "👍" no VS-16 to begin with

	resp, err := gc.PreHandleMatrixReaction(context.Background(), react)
	if err != nil {
		t.Fatalf("PreHandleMatrixReaction() error = %v, want nil", err)
	}
	if resp.Emoji != "\U0001F44D" {
		t.Errorf("Emoji = %q, want %q (unchanged)", resp.Emoji, "\U0001F44D")
	}
	if string(resp.EmojiID) != "\U0001F44D" {
		t.Errorf("EmojiID = %q, want %q", resp.EmojiID, "\U0001F44D")
	}
}

// TestPreHandleMatrixReactionEmojiIDIsPerEmoji proves EmojiID differs for
// two distinct emoji reacted by the same sender to the same message --
// Google Chat's per-emoji semantics (not one-reaction-per-user), see this
// file's own handlereaction.go top-of-file doc comment.
func TestPreHandleMatrixReactionEmojiIDIsPerEmoji(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	gc := &GChatClient{UserLogin: login}
	target := &database.Message{ID: gcid.MakeMessageID("msg1")}

	heart, err := gc.PreHandleMatrixReaction(context.Background(), matrixReaction(spacePortal("space1"), target, "❤️"))
	if err != nil {
		t.Fatalf("PreHandleMatrixReaction() error = %v", err)
	}
	thumb, err := gc.PreHandleMatrixReaction(context.Background(), matrixReaction(spacePortal("space1"), target, "\U0001F44D"))
	if err != nil {
		t.Fatalf("PreHandleMatrixReaction() error = %v", err)
	}
	if heart.EmojiID == thumb.EmojiID {
		t.Errorf("EmojiID collided for distinct emoji: %q == %q, want distinct (per-emoji dedup key)", heart.EmojiID, thumb.EmojiID)
	}
}

// --- HandleMatrixReaction: request construction --------------------------

func TestHandleMatrixReactionSpacePortalBuildsUpdateReactionAddRequest(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.UpdateReactionRequest
	gc := &GChatClient{
		UserLogin: login,
		updateReactionFn: func(_ context.Context, req *pb.UpdateReactionRequest) (*pb.UpdateReactionResponse, error) {
			gotReq = req
			return &pb.UpdateReactionResponse{}, nil
		},
	}

	target := &database.Message{
		ID:       gcid.MakeMessageID("msg1"),
		Metadata: &MessageMetadata{TopicID: "msg1"},
	}
	react := matrixReactionWithPreResp(spacePortal("space1"), target, "❤")

	dbReaction, err := gc.HandleMatrixReaction(context.Background(), react)
	if err != nil {
		t.Fatalf("HandleMatrixReaction() error = %v, want nil", err)
	}
	if gotReq == nil {
		t.Fatal("updateReactionFn was not called")
	}

	if got := gotReq.GetMessageId().GetMessageId(); got != "msg1" {
		t.Errorf("MessageId.MessageId = %q, want %q", got, "msg1")
	}
	if got := gotReq.GetMessageId().GetParentId().GetTopicId().GetTopicId(); got != "msg1" {
		t.Errorf("MessageId.ParentId.TopicId.TopicId = %q, want %q", got, "msg1")
	}
	if got := gotReq.GetMessageId().GetParentId().GetTopicId().GetGroupId().GetSpaceId().GetSpaceId(); got != "space1" {
		t.Errorf("MessageId.ParentId.TopicId.GroupId.SpaceId = %q, want %q", got, "space1")
	}
	if gotReq.GetMessageId().GetParentId().GetTopicId().GetGroupId().GetDmId() != nil {
		t.Error("GroupId.DmId is set for a space portal, want unset")
	}
	if got := gotReq.GetEmoji().GetUnicode(); got != "❤" {
		t.Errorf("Emoji.Unicode = %q, want %q", got, "❤")
	}
	if got := gotReq.GetType(); got != pb.UpdateReactionRequest_ADD {
		t.Errorf("Type = %v, want ADD", got)
	}

	meta, ok := dbReaction.Metadata.(*ReactionMetadata)
	if !ok {
		t.Fatalf("dbReaction.Metadata = %T, want *ReactionMetadata", dbReaction.Metadata)
	}
	if meta.TopicID != "msg1" {
		t.Errorf("dbReaction.Metadata.TopicID = %q, want %q (cached for a later removal)", meta.TopicID, "msg1")
	}
}

func TestHandleMatrixReactionDMPortalBuildsDmGroupID(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.UpdateReactionRequest
	gc := &GChatClient{
		UserLogin: login,
		updateReactionFn: func(_ context.Context, req *pb.UpdateReactionRequest) (*pb.UpdateReactionResponse, error) {
			gotReq = req
			return &pb.UpdateReactionResponse{}, nil
		},
	}

	target := &database.Message{ID: gcid.MakeMessageID("msg1"), Metadata: &MessageMetadata{TopicID: "msg1"}}
	react := matrixReactionWithPreResp(dmPortal("dm1"), target, "\U0001F44D")

	if _, err := gc.HandleMatrixReaction(context.Background(), react); err != nil {
		t.Fatalf("HandleMatrixReaction() error = %v, want nil", err)
	}
	if got := gotReq.GetMessageId().GetParentId().GetTopicId().GetGroupId().GetDmId().GetDmId(); got != "dm1" {
		t.Errorf("GroupId.DmId = %q, want %q", got, "dm1")
	}
	if gotReq.GetMessageId().GetParentId().GetTopicId().GetGroupId().GetSpaceId() != nil {
		t.Error("GroupId.SpaceId is set for a DM portal, want unset")
	}
}

// TestHandleMatrixReactionUsesStoredTopicIDForThreadReply covers a
// reply-in-thread target: the target's own stored MessageMetadata.TopicID
// (the thread's topic, distinct from the target's own message id) must be
// used as the reaction's topic_id, not the target's own message id.
func TestHandleMatrixReactionUsesStoredTopicIDForThreadReply(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.UpdateReactionRequest
	gc := &GChatClient{
		UserLogin: login,
		updateReactionFn: func(_ context.Context, req *pb.UpdateReactionRequest) (*pb.UpdateReactionResponse, error) {
			gotReq = req
			return &pb.UpdateReactionResponse{}, nil
		},
	}

	target := &database.Message{
		ID:       gcid.MakeMessageID("reply-msg-1"),
		Metadata: &MessageMetadata{TopicID: "topic1"},
	}
	react := matrixReactionWithPreResp(spacePortal("space1"), target, "❤")

	if _, err := gc.HandleMatrixReaction(context.Background(), react); err != nil {
		t.Fatalf("HandleMatrixReaction() error = %v, want nil", err)
	}
	if got := gotReq.GetMessageId().GetMessageId(); got != "reply-msg-1" {
		t.Errorf("MessageId.MessageId = %q, want %q", got, "reply-msg-1")
	}
	if got := gotReq.GetMessageId().GetParentId().GetTopicId().GetTopicId(); got != "topic1" {
		t.Errorf("MessageId.ParentId.TopicId.TopicId = %q, want %q (the reply's own thread, not its own id)", got, "topic1")
	}
}

// TestHandleMatrixReactionFallsBackToOwnIDWhenTopicIDMissing pins the
// `thread_id or message_id` fallback: a target with no stored TopicID must
// fall back to its own message id, exactly like threadRootTopicID
// (handlematrix.go), which this method reuses.
func TestHandleMatrixReactionFallsBackToOwnIDWhenTopicIDMissing(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.UpdateReactionRequest
	gc := &GChatClient{
		UserLogin: login,
		updateReactionFn: func(_ context.Context, req *pb.UpdateReactionRequest) (*pb.UpdateReactionResponse, error) {
			gotReq = req
			return &pb.UpdateReactionResponse{}, nil
		},
	}

	target := &database.Message{ID: gcid.MakeMessageID("headmsg1")} // no Metadata at all
	react := matrixReactionWithPreResp(spacePortal("space1"), target, "❤")

	dbReaction, err := gc.HandleMatrixReaction(context.Background(), react)
	if err != nil {
		t.Fatalf("HandleMatrixReaction() error = %v, want nil", err)
	}
	if got := gotReq.GetMessageId().GetParentId().GetTopicId().GetTopicId(); got != "headmsg1" {
		t.Errorf("MessageId.ParentId.TopicId.TopicId = %q, want %q (fallback to the target's own id)", got, "headmsg1")
	}
	if got := dbReaction.Metadata.(*ReactionMetadata).TopicID; got != "headmsg1" {
		t.Errorf("dbReaction.Metadata.TopicID = %q, want %q (cached fallback value)", got, "headmsg1")
	}
}

// --- HandleMatrixReaction: error paths ------------------------------------

func TestHandleMatrixReactionInvalidPortalIDErrors(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	called := false
	gc := &GChatClient{
		UserLogin: login,
		updateReactionFn: func(context.Context, *pb.UpdateReactionRequest) (*pb.UpdateReactionResponse, error) {
			called = true
			return &pb.UpdateReactionResponse{}, nil
		},
	}

	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: networkid.PortalID("garbage")}}}
	target := &database.Message{ID: gcid.MakeMessageID("msg1")}
	react := matrixReactionWithPreResp(portal, target, "❤")

	_, err := gc.HandleMatrixReaction(context.Background(), react)
	if err == nil {
		t.Fatal("HandleMatrixReaction() error = nil, want an error for an unparseable portal id")
	}
	if called {
		t.Error("updateReactionFn was called despite an invalid portal id")
	}
}

func TestHandleMatrixReactionNoConnNoSeamErrors(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	gc := &GChatClient{UserLogin: login}

	target := &database.Message{ID: gcid.MakeMessageID("msg1")}
	react := matrixReactionWithPreResp(spacePortal("space1"), target, "❤")

	_, err := gc.HandleMatrixReaction(context.Background(), react)
	if err == nil {
		t.Fatal("HandleMatrixReaction() error = nil, want an error when not connected")
	}
}

func TestHandleMatrixReactionPropagatesRPCError(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	wantErr := errors.New("update_reaction: boom")
	gc := &GChatClient{
		UserLogin: login,
		updateReactionFn: func(context.Context, *pb.UpdateReactionRequest) (*pb.UpdateReactionResponse, error) {
			return nil, wantErr
		},
	}

	target := &database.Message{ID: gcid.MakeMessageID("msg1")}
	react := matrixReactionWithPreResp(spacePortal("space1"), target, "❤")

	_, err := gc.HandleMatrixReaction(context.Background(), react)
	if !errors.Is(err, wantErr) {
		t.Errorf("error = %v, want wrapping %v", err, wantErr)
	}
}

// --- HandleMatrixReactionRemove: request construction ---------------------

func TestHandleMatrixReactionRemoveBuildsUpdateReactionRemoveRequest(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.UpdateReactionRequest
	gc := &GChatClient{
		UserLogin: login,
		updateReactionFn: func(_ context.Context, req *pb.UpdateReactionRequest) (*pb.UpdateReactionResponse, error) {
			gotReq = req
			return &pb.UpdateReactionResponse{}, nil
		},
	}

	target := &database.Reaction{
		MessageID: gcid.MakeMessageID("msg1"),
		EmojiID:   networkid.EmojiID("❤"),
		Metadata:  &ReactionMetadata{TopicID: "topic1"},
	}
	remove := matrixReactionRemove(spacePortal("space1"), target)

	if err := gc.HandleMatrixReactionRemove(context.Background(), remove); err != nil {
		t.Fatalf("HandleMatrixReactionRemove() error = %v, want nil", err)
	}
	if gotReq == nil {
		t.Fatal("updateReactionFn was not called")
	}
	if got := gotReq.GetMessageId().GetMessageId(); got != "msg1" {
		t.Errorf("MessageId.MessageId = %q, want %q", got, "msg1")
	}
	if got := gotReq.GetMessageId().GetParentId().GetTopicId().GetTopicId(); got != "topic1" {
		t.Errorf("MessageId.ParentId.TopicId.TopicId = %q, want %q (from cached ReactionMetadata)", got, "topic1")
	}
	if got := gotReq.GetMessageId().GetParentId().GetTopicId().GetGroupId().GetSpaceId().GetSpaceId(); got != "space1" {
		t.Errorf("MessageId.ParentId.TopicId.GroupId.SpaceId = %q, want %q", got, "space1")
	}
	if got := gotReq.GetEmoji().GetUnicode(); got != "❤" {
		t.Errorf("Emoji.Unicode = %q, want %q (the reaction's own bare EmojiID)", got, "❤")
	}
	if got := gotReq.GetType(); got != pb.UpdateReactionRequest_REMOVE {
		t.Errorf("Type = %v, want REMOVE", got)
	}
}

func TestHandleMatrixReactionRemoveDMPortalBuildsDmGroupID(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.UpdateReactionRequest
	gc := &GChatClient{
		UserLogin: login,
		updateReactionFn: func(_ context.Context, req *pb.UpdateReactionRequest) (*pb.UpdateReactionResponse, error) {
			gotReq = req
			return &pb.UpdateReactionResponse{}, nil
		},
	}

	target := &database.Reaction{
		MessageID: gcid.MakeMessageID("msg1"),
		EmojiID:   networkid.EmojiID("\U0001F44D"),
		Metadata:  &ReactionMetadata{TopicID: "msg1"},
	}
	remove := matrixReactionRemove(dmPortal("dm1"), target)

	if err := gc.HandleMatrixReactionRemove(context.Background(), remove); err != nil {
		t.Fatalf("HandleMatrixReactionRemove() error = %v, want nil", err)
	}
	if got := gotReq.GetMessageId().GetParentId().GetTopicId().GetGroupId().GetDmId().GetDmId(); got != "dm1" {
		t.Errorf("GroupId.DmId = %q, want %q", got, "dm1")
	}
}

// TestHandleMatrixReactionRemoveFallsBackToMessageIDWhenNoTopicIDMetadata
// covers a reaction row with no cached ReactionMetadata AND no lookup
// capability at all (getMessageFn is nil and UserLogin.Bridge is nil, this
// package's lightweight test harness -- see reactionTopicID's own doc
// comment, handlereaction.go): the topic id must fall back to the
// reaction's own target message id as the last resort, exactly like
// HandleMatrixReaction/threadRootTopicID's identical fallback.
func TestHandleMatrixReactionRemoveFallsBackToMessageIDWhenNoTopicIDMetadata(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.UpdateReactionRequest
	gc := &GChatClient{
		UserLogin: login,
		updateReactionFn: func(_ context.Context, req *pb.UpdateReactionRequest) (*pb.UpdateReactionResponse, error) {
			gotReq = req
			return &pb.UpdateReactionResponse{}, nil
		},
	}

	target := &database.Reaction{
		MessageID: gcid.MakeMessageID("headmsg1"),
		EmojiID:   networkid.EmojiID("❤"),
		// Metadata is nil
	}
	remove := matrixReactionRemove(spacePortal("space1"), target)

	if err := gc.HandleMatrixReactionRemove(context.Background(), remove); err != nil {
		t.Fatalf("HandleMatrixReactionRemove() error = %v, want nil", err)
	}
	if got := gotReq.GetMessageId().GetParentId().GetTopicId().GetTopicId(); got != "headmsg1" {
		t.Errorf("MessageId.ParentId.TopicId.TopicId = %q, want %q", got, "headmsg1")
	}
}

// TestHandleMatrixReactionRemoveLooksUpTopicIDForGCOriginatedReaction pins
// the gchat-port-auditor P0 fix: a reaction added from the Google Chat side
// (queueMessageReaction, events.go) never populates ReactionMetadata (it has
// no per-message payload to read a topic id off directly), so a reaction row
// with NO cached metadata but that targets a genuine THREAD REPLY (whose own
// message id is NOT its topic id) must resolve the real topic id via a fresh
// DB.Message lookup (getMessageFn) -- not silently substitute the reply's
// own message id as if it were the topic, which would send update_reaction
// at the wrong thread entirely. An unconditional DB re-fetch avoids this gap
// entirely, since it does not depend on which side created the reaction.
func TestHandleMatrixReactionRemoveLooksUpTopicIDForGCOriginatedReaction(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.UpdateReactionRequest
	var gotReceiver networkid.UserLoginID
	var gotLookupID networkid.MessageID
	gc := &GChatClient{
		UserLogin: login,
		updateReactionFn: func(_ context.Context, req *pb.UpdateReactionRequest) (*pb.UpdateReactionResponse, error) {
			gotReq = req
			return &pb.UpdateReactionResponse{}, nil
		},
		getMessageFn: func(_ context.Context, receiver networkid.UserLoginID, id networkid.MessageID) (*database.Message, error) {
			gotReceiver = receiver
			gotLookupID = id
			return &database.Message{
				ID:       id,
				Metadata: &MessageMetadata{TopicID: "topic1"},
			}, nil
		},
	}

	// reply-msg-1 is a THREAD REPLY: its own message id is NOT its topic id
	// (topic1 is), and this row was never touched by HandleMatrixReaction
	// (no ReactionMetadata cached) -- exactly what a GC-originated reaction
	// looks like once bridgev2 fetches it back for a Matrix redaction.
	target := &database.Reaction{
		MessageID: gcid.MakeMessageID("reply-msg-1"),
		EmojiID:   networkid.EmojiID("❤"),
		// Metadata is nil -- no cache, forces the lookup path.
	}
	remove := matrixReactionRemove(spacePortal("space1"), target)

	if err := gc.HandleMatrixReactionRemove(context.Background(), remove); err != nil {
		t.Fatalf("HandleMatrixReactionRemove() error = %v, want nil", err)
	}
	if got := gotReq.GetMessageId().GetParentId().GetTopicId().GetTopicId(); got != "topic1" {
		t.Errorf("MessageId.ParentId.TopicId.TopicId = %q, want %q (looked up via getMessageFn, NOT the reply's own id)", got, "topic1")
	}
	if got := gotReq.GetMessageId().GetMessageId(); got != "reply-msg-1" {
		t.Errorf("MessageId.MessageId = %q, want %q", got, "reply-msg-1")
	}
	if gotReceiver != login.ID {
		t.Errorf("getMessageFn receiver = %q, want %q", gotReceiver, login.ID)
	}
	if gotLookupID != gcid.MakeMessageID("reply-msg-1") {
		t.Errorf("getMessageFn id = %q, want %q (the reacted-to message, not the reaction's own mxid)", gotLookupID, "reply-msg-1")
	}
}

// TestHandleMatrixReactionRemoveLookupFallsBackToMessagesOwnIDWhenHeadOfTopic
// covers the common non-threaded (or head-of-topic) case through the SAME
// lookup path: a looked-up message with no stored TopicID (or, here, one
// equal to its own id) must still resolve correctly via
// threadRootTopicID's own message_id==topic_id fallback -- the lookup path
// and the cached-metadata fast path must agree for this case.
func TestHandleMatrixReactionRemoveLookupFallsBackToMessagesOwnIDWhenHeadOfTopic(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.UpdateReactionRequest
	gc := &GChatClient{
		UserLogin: login,
		updateReactionFn: func(_ context.Context, req *pb.UpdateReactionRequest) (*pb.UpdateReactionResponse, error) {
			gotReq = req
			return &pb.UpdateReactionResponse{}, nil
		},
		getMessageFn: func(_ context.Context, _ networkid.UserLoginID, id networkid.MessageID) (*database.Message, error) {
			return &database.Message{ID: id}, nil // no Metadata at all
		},
	}

	target := &database.Reaction{MessageID: gcid.MakeMessageID("headmsg1"), EmojiID: networkid.EmojiID("❤")}
	remove := matrixReactionRemove(spacePortal("space1"), target)

	if err := gc.HandleMatrixReactionRemove(context.Background(), remove); err != nil {
		t.Fatalf("HandleMatrixReactionRemove() error = %v, want nil", err)
	}
	if got := gotReq.GetMessageId().GetParentId().GetTopicId().GetTopicId(); got != "headmsg1" {
		t.Errorf("MessageId.ParentId.TopicId.TopicId = %q, want %q", got, "headmsg1")
	}
}

// TestHandleMatrixReactionRemoveLookupErrorFallsBackGracefully proves a
// failed DB lookup does not abort the whole removal (the un-reaction RPC
// still proceeds -- the RPC is never conditioned on the lookup succeeding)
// -- it just falls back to the reaction's own target message id as a last
// resort, same as having no lookup capability at all.
func TestHandleMatrixReactionRemoveLookupErrorFallsBackGracefully(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.UpdateReactionRequest
	gc := &GChatClient{
		UserLogin: login,
		updateReactionFn: func(_ context.Context, req *pb.UpdateReactionRequest) (*pb.UpdateReactionResponse, error) {
			gotReq = req
			return &pb.UpdateReactionResponse{}, nil
		},
		getMessageFn: func(context.Context, networkid.UserLoginID, networkid.MessageID) (*database.Message, error) {
			return nil, errors.New("db: boom")
		},
	}

	target := &database.Reaction{MessageID: gcid.MakeMessageID("msg1"), EmojiID: networkid.EmojiID("❤")}
	remove := matrixReactionRemove(spacePortal("space1"), target)

	if err := gc.HandleMatrixReactionRemove(context.Background(), remove); err != nil {
		t.Fatalf("HandleMatrixReactionRemove() error = %v, want nil (lookup failure must not abort the RPC)", err)
	}
	if got := gotReq.GetMessageId().GetParentId().GetTopicId().GetTopicId(); got != "msg1" {
		t.Errorf("MessageId.ParentId.TopicId.TopicId = %q, want %q (fallback after a failed lookup)", got, "msg1")
	}
}

// TestHandleMatrixReactionRemoveLookupNilMessageFallsBackGracefully covers
// a lookup that succeeds but finds no row (nil, nil) -- e.g. the reacted-to
// message was itself since deleted -- falling back the same way a lookup
// error does, rather than panicking on a nil dereference.
func TestHandleMatrixReactionRemoveLookupNilMessageFallsBackGracefully(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.UpdateReactionRequest
	gc := &GChatClient{
		UserLogin: login,
		updateReactionFn: func(_ context.Context, req *pb.UpdateReactionRequest) (*pb.UpdateReactionResponse, error) {
			gotReq = req
			return &pb.UpdateReactionResponse{}, nil
		},
		getMessageFn: func(context.Context, networkid.UserLoginID, networkid.MessageID) (*database.Message, error) {
			return nil, nil
		},
	}

	target := &database.Reaction{MessageID: gcid.MakeMessageID("msg1"), EmojiID: networkid.EmojiID("❤")}
	remove := matrixReactionRemove(spacePortal("space1"), target)

	if err := gc.HandleMatrixReactionRemove(context.Background(), remove); err != nil {
		t.Fatalf("HandleMatrixReactionRemove() error = %v, want nil", err)
	}
	if got := gotReq.GetMessageId().GetParentId().GetTopicId().GetTopicId(); got != "msg1" {
		t.Errorf("MessageId.ParentId.TopicId.TopicId = %q, want %q (fallback after a nil lookup result)", got, "msg1")
	}
}

// --- HandleMatrixReactionRemove: error paths -------------------------------

func TestHandleMatrixReactionRemoveInvalidPortalIDErrors(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	called := false
	gc := &GChatClient{
		UserLogin: login,
		updateReactionFn: func(context.Context, *pb.UpdateReactionRequest) (*pb.UpdateReactionResponse, error) {
			called = true
			return &pb.UpdateReactionResponse{}, nil
		},
	}

	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: networkid.PortalID("garbage")}}}
	target := &database.Reaction{MessageID: gcid.MakeMessageID("msg1"), EmojiID: networkid.EmojiID("❤")}
	remove := matrixReactionRemove(portal, target)

	err := gc.HandleMatrixReactionRemove(context.Background(), remove)
	if err == nil {
		t.Fatal("HandleMatrixReactionRemove() error = nil, want an error for an unparseable portal id")
	}
	if called {
		t.Error("updateReactionFn was called despite an invalid portal id")
	}
}

func TestHandleMatrixReactionRemoveNoConnNoSeamErrors(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	gc := &GChatClient{UserLogin: login}

	target := &database.Reaction{MessageID: gcid.MakeMessageID("msg1"), EmojiID: networkid.EmojiID("❤")}
	remove := matrixReactionRemove(spacePortal("space1"), target)

	err := gc.HandleMatrixReactionRemove(context.Background(), remove)
	if err == nil {
		t.Fatal("HandleMatrixReactionRemove() error = nil, want an error when not connected")
	}
}

func TestHandleMatrixReactionRemovePropagatesRPCError(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	wantErr := errors.New("update_reaction: boom")
	gc := &GChatClient{
		UserLogin: login,
		updateReactionFn: func(context.Context, *pb.UpdateReactionRequest) (*pb.UpdateReactionResponse, error) {
			return nil, wantErr
		},
	}

	target := &database.Reaction{MessageID: gcid.MakeMessageID("msg1"), EmojiID: networkid.EmojiID("❤")}
	remove := matrixReactionRemove(spacePortal("space1"), target)

	err := gc.HandleMatrixReactionRemove(context.Background(), remove)
	if !errors.Is(err, wantErr) {
		t.Errorf("error = %v, want wrapping %v", err, wantErr)
	}
}
