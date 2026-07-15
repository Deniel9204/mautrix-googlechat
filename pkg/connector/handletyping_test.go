package connector

// handletyping_test.go -- typing notifications, both directions (M4 Task 5).
//
// Outbound: HandleMatrixTyping -> set_typing_state RPC, porting
// portal.py:1133-1146's handle_matrix_typing / client.py:477-497's
// mark_typing. Mirrors handlereceipt_test.go's request-construction /
// error-path test shape.
//
// Inbound: handleGChatEvent's TypingStateChanged body arm (events.go),
// porting handle_googlechat_typing (portal.py:1600-1610). Mirrors
// events_test.go's queueRemoteEventFn capture pattern (newEventTestClient).
// TestHandleGChatEventTypingStateChangedRoutesViaBodyContextNotOuterGroupID
// is the regression guard for the routing trap documented on
// queueTypingStateChanged (events.go): TYPING_STATE_CHANGED is the ONLY
// event type where Python overrides group_id from the body
// (user.py:674-682) instead of reading the outer Event.group_id every other
// handler in this package uses.

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// --- typingContext (outbound oneof helper) ---------------------------------

func TestTypingContextGroupOnlyBuildsGroupIdArm(t *testing.T) {
	tc := typingContext(gcid.GroupID{ID: "space1", IsDM: false}, "")
	if got := tc.GetGroupId().GetSpaceId().GetSpaceId(); got != "space1" {
		t.Errorf("GetGroupId().GetSpaceId().GetSpaceId() = %q, want %q", got, "space1")
	}
	if tc.GetTopicId() != nil {
		t.Error("GetTopicId() is set, want unset (group_id arm)")
	}
}

func TestTypingContextDMGroupOnlyBuildsDmIdArm(t *testing.T) {
	tc := typingContext(gcid.GroupID{ID: "dm1", IsDM: true}, "")
	if got := tc.GetGroupId().GetDmId().GetDmId(); got != "dm1" {
		t.Errorf("GetGroupId().GetDmId().GetDmId() = %q, want %q", got, "dm1")
	}
}

// TestTypingContextTopicScopedBuildsTopicIdArm covers the topic (threaded)
// oneof arm directly -- see handletyping.go's top-of-file doc comment on why
// HandleMatrixTyping itself never reaches this branch in practice (Python's
// own mark_typing is never called with a truthy thread_id either), but the
// proto shape client.py defines still gets full coverage here.
func TestTypingContextTopicScopedBuildsTopicIdArm(t *testing.T) {
	tc := typingContext(gcid.GroupID{ID: "space1", IsDM: false}, "topic-99")
	if tc.GetGroupId() != nil {
		t.Error("GetGroupId() is set, want unset (topic_id arm)")
	}
	topic := tc.GetTopicId()
	if topic == nil {
		t.Fatal("GetTopicId() = nil, want set")
	}
	if got := topic.GetTopicId(); got != "topic-99" {
		t.Errorf("GetTopicId().GetTopicId() = %q, want %q", got, "topic-99")
	}
	if got := topic.GetGroupId().GetSpaceId().GetSpaceId(); got != "space1" {
		t.Errorf("GetTopicId().GetGroupId().GetSpaceId().GetSpaceId() = %q, want %q", got, "space1")
	}
}

// --- HandleMatrixTyping: outbound -------------------------------------------

func matrixTyping(portal *bridgev2.Portal, isTyping bool) *bridgev2.MatrixTyping {
	return &bridgev2.MatrixTyping{Portal: portal, IsTyping: isTyping}
}

func TestHandleMatrixTypingSpacePortalStartSendsGroupContextTyping(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.SetTypingStateRequest
	gc := &GChatClient{
		UserLogin: login,
		setTypingStateFn: func(_ context.Context, req *pb.SetTypingStateRequest) (*pb.SetTypingStateResponse, error) {
			gotReq = req
			return &pb.SetTypingStateResponse{}, nil
		},
	}

	msg := matrixTyping(spacePortal("space1"), true)
	if err := gc.HandleMatrixTyping(context.Background(), msg); err != nil {
		t.Fatalf("HandleMatrixTyping() error = %v, want nil", err)
	}
	if gotReq == nil {
		t.Fatal("setTypingStateFn was not called")
	}
	if got := gotReq.GetState(); got != pb.TypingState_TYPING {
		t.Errorf("State = %v, want TYPING", got)
	}
	if got := gotReq.GetContext().GetGroupId().GetSpaceId().GetSpaceId(); got != "space1" {
		t.Errorf("Context.GroupId.SpaceId = %q, want %q", got, "space1")
	}
	if gotReq.GetContext().GetGroupId().GetDmId() != nil {
		t.Error("Context.GroupId.DmId is set for a space portal, want unset")
	}
	if gotReq.GetContext().GetTopicId() != nil {
		t.Error("Context.TopicId is set, want unset (HandleMatrixTyping never has a thread signal)")
	}
}

func TestHandleMatrixTypingDMPortalStopSendsGroupContextStopped(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.SetTypingStateRequest
	gc := &GChatClient{
		UserLogin: login,
		setTypingStateFn: func(_ context.Context, req *pb.SetTypingStateRequest) (*pb.SetTypingStateResponse, error) {
			gotReq = req
			return &pb.SetTypingStateResponse{}, nil
		},
	}

	msg := matrixTyping(dmPortal("dm1"), false)
	if err := gc.HandleMatrixTyping(context.Background(), msg); err != nil {
		t.Fatalf("HandleMatrixTyping() error = %v, want nil", err)
	}
	if got := gotReq.GetState(); got != pb.TypingState_STOPPED {
		t.Errorf("State = %v, want STOPPED", got)
	}
	if got := gotReq.GetContext().GetGroupId().GetDmId().GetDmId(); got != "dm1" {
		t.Errorf("Context.GroupId.DmId = %q, want %q", got, "dm1")
	}
	if gotReq.GetContext().GetGroupId().GetSpaceId() != nil {
		t.Error("Context.GroupId.SpaceId is set for a DM portal, want unset")
	}
}

// TestHandleMatrixTypingBothStartAndStopAreSent pins that BOTH IsTyping
// values reach the RPC -- unlike a naive "only send on start" reading,
// bridgev2's own framework calls HandleMatrixTyping once per user that
// started typing AND once per user that stopped (mautrix-go
// bridgev2/portal.go:1006-1071's sendTypings, called for both
// stoppedTyping and startedTyping), exactly matching Python's own
// stopped_typing/started_typing gather (portal.py:1135-1146).
func TestHandleMatrixTypingBothStartAndStopAreSent(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var states []pb.TypingState
	gc := &GChatClient{
		UserLogin: login,
		setTypingStateFn: func(_ context.Context, req *pb.SetTypingStateRequest) (*pb.SetTypingStateResponse, error) {
			states = append(states, req.GetState())
			return &pb.SetTypingStateResponse{}, nil
		},
	}

	portal := spacePortal("space1")
	if err := gc.HandleMatrixTyping(context.Background(), matrixTyping(portal, true)); err != nil {
		t.Fatalf("HandleMatrixTyping(start) error = %v, want nil", err)
	}
	if err := gc.HandleMatrixTyping(context.Background(), matrixTyping(portal, false)); err != nil {
		t.Fatalf("HandleMatrixTyping(stop) error = %v, want nil", err)
	}
	if len(states) != 2 || states[0] != pb.TypingState_TYPING || states[1] != pb.TypingState_STOPPED {
		t.Errorf("states = %v, want [TYPING, STOPPED]", states)
	}
}

func TestHandleMatrixTypingInvalidPortalIDErrors(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	called := false
	gc := &GChatClient{
		UserLogin: login,
		setTypingStateFn: func(context.Context, *pb.SetTypingStateRequest) (*pb.SetTypingStateResponse, error) {
			called = true
			return &pb.SetTypingStateResponse{}, nil
		},
	}

	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: networkid.PortalID("garbage")}}}
	msg := matrixTyping(portal, true)

	err := gc.HandleMatrixTyping(context.Background(), msg)
	if err == nil {
		t.Fatal("HandleMatrixTyping() error = nil, want an error for an unparseable portal id")
	}
	if called {
		t.Error("setTypingStateFn was called despite an invalid portal id")
	}
}

func TestHandleMatrixTypingNoConnNoSeamErrors(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	gc := &GChatClient{UserLogin: login}

	msg := matrixTyping(spacePortal("space1"), true)
	err := gc.HandleMatrixTyping(context.Background(), msg)
	if err == nil {
		t.Fatal("HandleMatrixTyping() error = nil, want an error when not connected")
	}
}

func TestHandleMatrixTypingPropagatesRPCError(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	wantErr := errors.New("set_typing_state: boom")
	gc := &GChatClient{
		UserLogin: login,
		setTypingStateFn: func(context.Context, *pb.SetTypingStateRequest) (*pb.SetTypingStateResponse, error) {
			return nil, wantErr
		},
	}

	msg := matrixTyping(spacePortal("space1"), true)
	err := gc.HandleMatrixTyping(context.Background(), msg)
	if !errors.Is(err, wantErr) {
		t.Errorf("error = %v, want wrapping %v", err, wantErr)
	}
}

// --- Inbound: TYPING_STATE_CHANGED ------------------------------------------

// typingStateChangedEvent builds a *pb.Event shaped like a real
// TYPING_STATE_CHANGED event reaching handleGChatEvent. outerGroupID is the
// FLATTENED event's own top-level group_id (as gchatmeow's splitEventBodies
// would set it -- empty/nil for a genuine typing event, see this file's
// top-of-file doc comment); bodyContext is the body's own TypingContext, the
// one that must actually drive routing.
func typingStateChangedEvent(outerGroupID *pb.GroupId, bodyContext *pb.TypingContext, state pb.TypingState, userGaia string, startTimestampUsec int64) *pb.Event {
	return &pb.Event{
		GroupId: outerGroupID,
		Type:    pb.Event_TYPING_STATE_CHANGED.Enum(),
		Body: &pb.Event_EventBody{
			EventType: pb.Event_TYPING_STATE_CHANGED.Enum(),
			Type: &pb.Event_EventBody_TypingStateChanged{
				TypingStateChanged: &pb.TypingStateChangedEvent{
					State:              state.Enum(),
					UserId:             &pb.UserId{Id: proto.String(userGaia)},
					Context:            bodyContext,
					StartTimestampUsec: proto.Int64(startTimestampUsec),
				},
			},
		},
	}
}

// TestHandleGChatEventTypingStateChangedRoutesViaBodyContextNotOuterGroupID
// is the regression guard for the routing trap: the outer Event.group_id is
// EMPTY (nil), exactly as it is on the real wire for typing events (this
// file's queueTypingStateChanged doc comment) -- if this function ever
// regresses to reading evt.GetGroupId() like every other body arm, this
// event would be silently dropped ("no usable group id") instead of routing
// via the body's own space-1 context.
func TestHandleGChatEventTypingStateChangedRoutesViaBodyContextNotOuterGroupID(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := typingStateChangedEvent(
		nil, // outer Event.group_id: EMPTY, as on the real wire
		&pb.TypingContext{Context: &pb.TypingContext_GroupId{GroupId: spaceGroupID("space-1")}},
		pb.TypingState_TYPING, "98765", 1700000000123456,
	)

	res := gc.handleGChatEvent(context.Background(), evt)

	if !res.Success {
		t.Fatalf("handleGChatEvent() result = %+v, want Success", res)
	}
	if len(*queued) != 1 {
		t.Fatalf("len(queued) = %d, want 1 (must not be dropped as 'no usable group id')", len(*queued))
	}
	typed := (*queued)[0].(bridgev2.RemoteEvent)
	wantPortalKey := gcid.MakePortalKey(gcid.GroupID{ID: "space-1", IsDM: false}, gc.UserLogin.ID)
	if got := typed.GetPortalKey(); got != wantPortalKey {
		t.Errorf("GetPortalKey() = %+v, want %+v (from the BODY's context, not the empty outer group_id)", got, wantPortalKey)
	}
}

// TestHandleGChatEventTypingStateChangedOuterGroupIDDifferentFromBodyUsesBody
// strengthens the trap regression guard: the outer Event.group_id is set,
// but to a DIFFERENT (wrong) group than the body's own context -- routing
// must still follow the body, never fall back to (or prefer) the outer
// field.
func TestHandleGChatEventTypingStateChangedOuterGroupIDDifferentFromBodyUsesBody(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := typingStateChangedEvent(
		spaceGroupID("wrong-outer-group"),
		&pb.TypingContext{Context: &pb.TypingContext_GroupId{GroupId: spaceGroupID("space-1")}},
		pb.TypingState_TYPING, "98765", 1,
	)

	gc.handleGChatEvent(context.Background(), evt)

	if len(*queued) != 1 {
		t.Fatalf("len(queued) = %d, want 1", len(*queued))
	}
	typed := (*queued)[0].(bridgev2.RemoteEvent)
	wantPortalKey := gcid.MakePortalKey(gcid.GroupID{ID: "space-1", IsDM: false}, gc.UserLogin.ID)
	if got := typed.GetPortalKey(); got != wantPortalKey {
		t.Errorf("GetPortalKey() = %+v, want %+v (body context, not the different outer group_id)", got, wantPortalKey)
	}
}

func TestHandleGChatEventTypingStateChangedStartedQueuesTimeout(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := typingStateChangedEvent(
		nil,
		&pb.TypingContext{Context: &pb.TypingContext_GroupId{GroupId: spaceGroupID("space-1")}},
		pb.TypingState_TYPING, "98765", 1,
	)

	gc.handleGChatEvent(context.Background(), evt)

	typing, ok := (*queued)[0].(bridgev2.RemoteTyping)
	if !ok {
		t.Fatalf("queued event does not implement bridgev2.RemoteTyping: %T", (*queued)[0])
	}
	if got := typing.GetTimeout(); got != 6*time.Second {
		t.Errorf("GetTimeout() = %v, want 6s (portal.py:1609's timeout=6000)", got)
	}
	typed := (*queued)[0].(bridgev2.RemoteEvent)
	if got := typed.GetType(); got != bridgev2.RemoteEventTyping {
		t.Errorf("GetType() = %v, want RemoteEventTyping", got)
	}
}

func TestHandleGChatEventTypingStateChangedStoppedQueuesZeroTimeout(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := typingStateChangedEvent(
		nil,
		&pb.TypingContext{Context: &pb.TypingContext_GroupId{GroupId: spaceGroupID("space-1")}},
		pb.TypingState_STOPPED, "98765", 1,
	)

	gc.handleGChatEvent(context.Background(), evt)

	typing := (*queued)[0].(bridgev2.RemoteTyping)
	if got := typing.GetTimeout(); got != 0 {
		t.Errorf("GetTimeout() = %v, want 0 (portal.py:1610's timeout=0 for a non-TYPING status)", got)
	}
}

func TestHandleGChatEventTypingStateChangedSenderAndNotFromMe(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := typingStateChangedEvent(
		nil,
		&pb.TypingContext{Context: &pb.TypingContext_GroupId{GroupId: spaceGroupID("space-1")}},
		pb.TypingState_TYPING, "98765", 1,
	)

	gc.handleGChatEvent(context.Background(), evt)

	sender := (*queued)[0].(bridgev2.RemoteEvent).GetSender()
	if got, want := sender.Sender, gcid.MakeUserID("98765"); got != want {
		t.Errorf("GetSender().Sender = %q, want %q", got, want)
	}
	if sender.IsFromMe {
		t.Error("GetSender().IsFromMe = true, want false (typist is not the login's own gaia)")
	}
}

func TestHandleGChatEventTypingStateChangedOwnUserSetsIsFromMe(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := typingStateChangedEvent(
		nil,
		&pb.TypingContext{Context: &pb.TypingContext_GroupId{GroupId: spaceGroupID("space-1")}},
		pb.TypingState_TYPING, "112233", 1,
	)

	gc.handleGChatEvent(context.Background(), evt)

	if !(*queued)[0].(bridgev2.RemoteEvent).GetSender().IsFromMe {
		t.Error("GetSender().IsFromMe = false, want true (typist gaia == login's own gaia)")
	}
}

func TestHandleGChatEventTypingStateChangedDMPortalKeyHasReceiver(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := typingStateChangedEvent(
		nil,
		&pb.TypingContext{Context: &pb.TypingContext_GroupId{GroupId: dmGroupID("dm-1")}},
		pb.TypingState_TYPING, "98765", 1,
	)

	gc.handleGChatEvent(context.Background(), evt)

	if len(*queued) != 1 {
		t.Fatalf("len(queued) = %d, want 1", len(*queued))
	}
	typed := (*queued)[0].(bridgev2.RemoteEvent)
	wantPortalKey := gcid.MakePortalKey(gcid.GroupID{ID: "dm-1", IsDM: true}, gc.UserLogin.ID)
	if got := typed.GetPortalKey(); got != wantPortalKey {
		t.Errorf("GetPortalKey() = %+v, want %+v", got, wantPortalKey)
	}
	if wantPortalKey.Receiver != gc.UserLogin.ID {
		t.Fatalf("test setup: DM portal key must be receiver-scoped, got Receiver=%q", wantPortalKey.Receiver)
	}
}

// TestHandleGChatEventTypingStateChangedTopicContextRoutesViaEmbeddedGroupID
// covers the topic_id oneof arm of TypingContext (a typing notification
// scoped to a thread): the group id lives inside TopicId.GroupId, not a
// direct TypingContext.GroupId -- typingContextGroupID's fallback branch
// (events.go) must still resolve the correct portal.
func TestHandleGChatEventTypingStateChangedTopicContextRoutesViaEmbeddedGroupID(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := typingStateChangedEvent(
		nil,
		&pb.TypingContext{Context: &pb.TypingContext_TopicId{TopicId: &pb.TopicId{
			GroupId: spaceGroupID("space-1"),
			TopicId: proto.String("topic-99"),
		}}},
		pb.TypingState_TYPING, "98765", 1,
	)

	gc.handleGChatEvent(context.Background(), evt)

	if len(*queued) != 1 {
		t.Fatalf("len(queued) = %d, want 1", len(*queued))
	}
	typed := (*queued)[0].(bridgev2.RemoteEvent)
	wantPortalKey := gcid.MakePortalKey(gcid.GroupID{ID: "space-1", IsDM: false}, gc.UserLogin.ID)
	if got := typed.GetPortalKey(); got != wantPortalKey {
		t.Errorf("GetPortalKey() = %+v, want %+v (group id embedded in TopicId)", got, wantPortalKey)
	}
}

func TestHandleGChatEventTypingStateChangedNoUsableContextSkipped(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := typingStateChangedEvent(nil, nil, pb.TypingState_TYPING, "98765", 1)

	res := gc.handleGChatEvent(context.Background(), evt)

	if !res.Success {
		t.Fatalf("handleGChatEvent() result = %+v, want Success (Ignored, so the watermark still advances past the garbage)", res)
	}
	if len(*queued) != 0 {
		t.Fatalf("len(queued) = %d, want 0 (no usable group id anywhere)", len(*queued))
	}
}

func TestHandleGChatEventTypingStateChangedEmptyContextOneofSkipped(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := typingStateChangedEvent(spaceGroupID("outer-unused"), &pb.TypingContext{}, pb.TypingState_TYPING, "98765", 1)

	res := gc.handleGChatEvent(context.Background(), evt)

	if !res.Success {
		t.Fatalf("handleGChatEvent() result = %+v, want Success", res)
	}
	if len(*queued) != 0 {
		t.Fatalf("len(queued) = %d, want 0 (TypingContext with neither oneof arm set)", len(*queued))
	}
}
