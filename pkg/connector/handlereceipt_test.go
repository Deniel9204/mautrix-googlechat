package connector

// handlereceipt_test.go -- read receipts, both directions (M4 Task 4).
//
// Outbound: HandleMatrixReadReceipt -> mark_group_readstate RPC, porting
// user.py:684-691's mark_read (the only caller of the outbound RPC is
// matrix.py:106-113's handle_read_receipt). Mirrors handleedit_test.go's
// request-construction / error-path test shape.
//
// Inbound: handleGChatEvent's ReadReceiptChanged and GroupViewed body arms
// (events.go), porting handle_googlechat_read_receipts (portal.py:1587-1592)
// and portal.py:556-557's group_viewed handling. Mirrors events_test.go's
// queueRemoteEventFn capture pattern (newEventTestClient).

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// --- HandleMatrixReadReceipt: outbound -------------------------------------

// matrixReadReceipt builds a *bridgev2.MatrixReadReceipt, matching what
// bridgev2's own handleMatrixReadReceipt hands
// ReadReceiptHandlingNetworkAPI.HandleMatrixReadReceipt (mautrix-go
// bridgev2/portal.go:949-966) after it has already resolved ExactMessage (or
// left it nil) and computed ReadUpTo.
func matrixReadReceipt(portal *bridgev2.Portal, exactMessage *database.Message, receiptTS time.Time) *bridgev2.MatrixReadReceipt {
	return &bridgev2.MatrixReadReceipt{
		Portal:       portal,
		ExactMessage: exactMessage,
		Receipt:      event.ReadReceipt{Timestamp: receiptTS},
	}
}

func TestHandleMatrixReadReceiptExactMessageUsesStoredTimestampMicro(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.MarkGroupReadstateRequest
	gc := &GChatClient{
		UserLogin: login,
		markGroupReadstateFn: func(_ context.Context, req *pb.MarkGroupReadstateRequest) (*pb.MarkGroupReadstateResponse, error) {
			gotReq = req
			return &pb.MarkGroupReadstateResponse{}, nil
		},
	}

	target := &database.Message{
		ID:        gcid.MakeMessageID("msg1"),
		Metadata:  &MessageMetadata{TimestampMicro: 1700000000123456},
		Timestamp: time.UnixMilli(1234567890), // deliberately different -- must NOT be used when TimestampMicro is present
	}
	// The receipt's own timestamp is deliberately different too, to prove
	// the target message's stored create_time wins over it.
	msg := matrixReadReceipt(spacePortal("space1"), target, time.UnixMilli(999999999))

	err := gc.HandleMatrixReadReceipt(context.Background(), msg)
	if err != nil {
		t.Fatalf("HandleMatrixReadReceipt() error = %v, want nil", err)
	}
	if gotReq == nil {
		t.Fatal("markGroupReadstateFn was not called")
	}
	if got, want := gotReq.GetLastReadTime(), int64(1700000000123456); got != want {
		t.Errorf("LastReadTime = %d, want %d (ExactMessage's stored MessageMetadata.TimestampMicro)", got, want)
	}
}

func TestHandleMatrixReadReceiptNilExactMessageUsesReceiptTimestampMsToMicros(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.MarkGroupReadstateRequest
	gc := &GChatClient{
		UserLogin: login,
		markGroupReadstateFn: func(_ context.Context, req *pb.MarkGroupReadstateRequest) (*pb.MarkGroupReadstateResponse, error) {
			gotReq = req
			return &pb.MarkGroupReadstateResponse{}, nil
		},
	}

	// ExactMessage == nil -- the interface's own required tolerance
	// (mautrix-go bridgev2/networkinterface.go:660's "Network connectors
	// must gracefully handle [MatrixReadReceipt.ExactMessage] being nil").
	msg := matrixReadReceipt(spacePortal("space1"), nil, time.UnixMilli(1700000000123))

	err := gc.HandleMatrixReadReceipt(context.Background(), msg)
	if err != nil {
		t.Fatalf("HandleMatrixReadReceipt() error = %v, want nil", err)
	}
	if gotReq == nil {
		t.Fatal("markGroupReadstateFn was not called")
	}
	wantMicros := int64(1700000000123) * 1000
	if got := gotReq.GetLastReadTime(); got != wantMicros {
		t.Errorf("LastReadTime = %d, want %d (receipt timestamp ms*1000, user.py:689)", got, wantMicros)
	}
}

// TestHandleMatrixReadReceiptExactMessageWithoutMetadataFallsBack covers a
// target with no usable stored TimestampMicro (e.g. a legacy pre-M3-Task-6
// row, or an unexpected Metadata type) -- the defensive fallback path,
// mirroring buildReplyTarget's identical Metadata-shape check
// (handlematrix.go).
func TestHandleMatrixReadReceiptExactMessageWithoutMetadataFallsBack(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.MarkGroupReadstateRequest
	gc := &GChatClient{
		UserLogin: login,
		markGroupReadstateFn: func(_ context.Context, req *pb.MarkGroupReadstateRequest) (*pb.MarkGroupReadstateResponse, error) {
			gotReq = req
			return &pb.MarkGroupReadstateResponse{}, nil
		},
	}

	target := &database.Message{
		ID:        gcid.MakeMessageID("msg1"),
		Timestamp: time.UnixMilli(1700000000000), // no Metadata at all
	}
	msg := matrixReadReceipt(spacePortal("space1"), target, time.UnixMilli(1))

	err := gc.HandleMatrixReadReceipt(context.Background(), msg)
	if err != nil {
		t.Fatalf("HandleMatrixReadReceipt() error = %v, want nil", err)
	}
	wantMicros := gchatmeow.TimeToMicros(target.Timestamp)
	if got := gotReq.GetLastReadTime(); got != wantMicros {
		t.Errorf("LastReadTime = %d, want %d (fallback to ExactMessage.Timestamp)", got, wantMicros)
	}
}

func TestHandleMatrixReadReceiptDMPortalBuildsDmGroupID(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.MarkGroupReadstateRequest
	gc := &GChatClient{
		UserLogin: login,
		markGroupReadstateFn: func(_ context.Context, req *pb.MarkGroupReadstateRequest) (*pb.MarkGroupReadstateResponse, error) {
			gotReq = req
			return &pb.MarkGroupReadstateResponse{}, nil
		},
	}

	msg := matrixReadReceipt(dmPortal("dm1"), nil, time.UnixMilli(1))

	if err := gc.HandleMatrixReadReceipt(context.Background(), msg); err != nil {
		t.Fatalf("HandleMatrixReadReceipt() error = %v, want nil", err)
	}
	if got := gotReq.GetId().GetDmId().GetDmId(); got != "dm1" {
		t.Errorf("Id.DmId = %q, want %q", got, "dm1")
	}
	if gotReq.GetId().GetSpaceId() != nil {
		t.Error("Id.SpaceId is set for a DM portal, want unset")
	}
}

func TestHandleMatrixReadReceiptSpacePortalBuildsSpaceGroupID(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.MarkGroupReadstateRequest
	gc := &GChatClient{
		UserLogin: login,
		markGroupReadstateFn: func(_ context.Context, req *pb.MarkGroupReadstateRequest) (*pb.MarkGroupReadstateResponse, error) {
			gotReq = req
			return &pb.MarkGroupReadstateResponse{}, nil
		},
	}

	msg := matrixReadReceipt(spacePortal("space1"), nil, time.UnixMilli(1))

	if err := gc.HandleMatrixReadReceipt(context.Background(), msg); err != nil {
		t.Fatalf("HandleMatrixReadReceipt() error = %v, want nil", err)
	}
	if got := gotReq.GetId().GetSpaceId().GetSpaceId(); got != "space1" {
		t.Errorf("Id.SpaceId = %q, want %q", got, "space1")
	}
	if gotReq.GetId().GetDmId() != nil {
		t.Error("Id.DmId is set for a space portal, want unset")
	}
}

func TestHandleMatrixReadReceiptInvalidPortalIDErrors(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	called := false
	gc := &GChatClient{
		UserLogin: login,
		markGroupReadstateFn: func(context.Context, *pb.MarkGroupReadstateRequest) (*pb.MarkGroupReadstateResponse, error) {
			called = true
			return &pb.MarkGroupReadstateResponse{}, nil
		},
	}

	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: networkid.PortalID("garbage")}}}
	msg := matrixReadReceipt(portal, nil, time.UnixMilli(1))

	err := gc.HandleMatrixReadReceipt(context.Background(), msg)
	if err == nil {
		t.Fatal("HandleMatrixReadReceipt() error = nil, want an error for an unparseable portal id")
	}
	if called {
		t.Error("markGroupReadstateFn was called despite an invalid portal id")
	}
}

func TestHandleMatrixReadReceiptNoConnNoSeamErrors(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	gc := &GChatClient{UserLogin: login}

	msg := matrixReadReceipt(spacePortal("space1"), nil, time.UnixMilli(1))

	err := gc.HandleMatrixReadReceipt(context.Background(), msg)
	if err == nil {
		t.Fatal("HandleMatrixReadReceipt() error = nil, want an error when not connected")
	}
}

func TestHandleMatrixReadReceiptPropagatesRPCError(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	wantErr := errors.New("mark_group_readstate: boom")
	gc := &GChatClient{
		UserLogin: login,
		markGroupReadstateFn: func(context.Context, *pb.MarkGroupReadstateRequest) (*pb.MarkGroupReadstateResponse, error) {
			return nil, wantErr
		},
	}

	msg := matrixReadReceipt(spacePortal("space1"), nil, time.UnixMilli(1))

	err := gc.HandleMatrixReadReceipt(context.Background(), msg)
	if !errors.Is(err, wantErr) {
		t.Errorf("error = %v, want wrapping %v", err, wantErr)
	}
}

// --- Inbound: READ_RECEIPT_CHANGED -----------------------------------------

// readReceiptChangedEvent builds a *pb.Event shaped like a real
// ReadReceiptChanged event reaching handleGChatEvent, mirroring
// messageReactionEvent's shape (events_test.go) for the read_receipt_changed
// body arm. Each (userGaia, readTimeMicros) pair becomes one ReadReceipt
// entry in the event's ReadReceiptSet.
func readReceiptChangedEvent(groupID *pb.GroupId, receipts ...[2]any) *pb.Event {
	rrs := make([]*pb.ReadReceipt, 0, len(receipts))
	for _, r := range receipts {
		rrs = append(rrs, &pb.ReadReceipt{
			User:           &pb.User{UserId: &pb.UserId{Id: proto.String(r[0].(string))}},
			ReadTimeMicros: proto.Int64(r[1].(int64)),
		})
	}
	return &pb.Event{
		GroupId: groupID,
		Type:    pb.Event_READ_RECEIPT_CHANGED.Enum(),
		Body: &pb.Event_EventBody{
			EventType: pb.Event_READ_RECEIPT_CHANGED.Enum(),
			Type: &pb.Event_EventBody_ReadReceiptChanged{
				ReadReceiptChanged: &pb.ReadReceiptChangedEvent{
					ReadReceiptSet: &pb.ReadReceiptSet{ReadReceipts: rrs},
				},
			},
		},
	}
}

func TestHandleGChatEventReadReceiptChangedQueuesRemoteReceipt(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := readReceiptChangedEvent(spaceGroupID("space-1"), [2]any{"98765", int64(1700000000123456)})

	res := gc.handleGChatEvent(context.Background(), evt)

	if !res.Success {
		t.Fatalf("handleGChatEvent() result = %+v, want Success", res)
	}
	if len(*queued) != 1 {
		t.Fatalf("len(queued) = %d, want 1", len(*queued))
	}
	receipt, ok := (*queued)[0].(bridgev2.RemoteReadReceipt)
	if !ok {
		t.Fatalf("queued event does not implement bridgev2.RemoteReadReceipt: %T", (*queued)[0])
	}
	wantTS := gchatmeow.MicrosToTime(1700000000123456)
	if got := receipt.GetReadUpTo(); !got.Equal(wantTS) {
		t.Errorf("GetReadUpTo() = %v, want %v (read_time_micros)", got, wantTS)
	}

	typed := (*queued)[0].(bridgev2.RemoteEvent)
	if got := typed.GetType(); got != bridgev2.RemoteEventReadReceipt {
		t.Errorf("GetType() = %v, want RemoteEventReadReceipt", got)
	}
	wantPortalKey := gcid.MakePortalKey(gcid.GroupID{ID: "space-1", IsDM: false}, gc.UserLogin.ID)
	if got := typed.GetPortalKey(); got != wantPortalKey {
		t.Errorf("GetPortalKey() = %+v, want %+v", got, wantPortalKey)
	}
	sender := typed.GetSender()
	if got, want := sender.Sender, gcid.MakeUserID("98765"); got != want {
		t.Errorf("GetSender().Sender = %q, want %q", got, want)
	}
	if sender.IsFromMe {
		t.Error("GetSender().IsFromMe = true, want false (reader is not the login's own gaia)")
	}
}

func TestHandleGChatEventReadReceiptChangedOwnUserSetsIsFromMe(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := readReceiptChangedEvent(spaceGroupID("space-1"), [2]any{"112233", int64(1)})

	gc.handleGChatEvent(context.Background(), evt)

	typed := (*queued)[0].(bridgev2.RemoteEvent)
	if !typed.GetSender().IsFromMe {
		t.Error("GetSender().IsFromMe = false, want true (reader gaia == login's own gaia)")
	}
}

// TestHandleGChatEventReadReceiptChangedMultipleReceiptsQueuesOnePerUser
// pins portal.py:1590-1592's `for rr in evt.read_receipt_set.read_receipts:`
// loop: a ReadReceiptSet announcing several users' read states at once must
// queue one bridgev2.RemoteReadReceipt per user, since simplevent.Receipt
// only carries a single Sender.
func TestHandleGChatEventReadReceiptChangedMultipleReceiptsQueuesOnePerUser(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := readReceiptChangedEvent(spaceGroupID("space-1"),
		[2]any{"98765", int64(1)},
		[2]any{"55555", int64(2)},
	)

	res := gc.handleGChatEvent(context.Background(), evt)

	if !res.Success {
		t.Fatalf("handleGChatEvent() result = %+v, want Success", res)
	}
	if len(*queued) != 2 {
		t.Fatalf("len(queued) = %d, want 2", len(*queued))
	}
	var senders []networkid.UserID
	for _, q := range *queued {
		senders = append(senders, q.(bridgev2.RemoteEvent).GetSender().Sender)
	}
	if senders[0] != gcid.MakeUserID("98765") || senders[1] != gcid.MakeUserID("55555") {
		t.Errorf("senders = %v, want [98765, 55555]", senders)
	}
}

func TestHandleGChatEventReadReceiptChangedDMPortalKeyHasReceiver(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := readReceiptChangedEvent(dmGroupID("dm-1"), [2]any{"98765", int64(1)})

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

func TestHandleGChatEventReadReceiptChangedNoGroupIDSkipped(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := readReceiptChangedEvent(nil, [2]any{"98765", int64(1)})

	res := gc.handleGChatEvent(context.Background(), evt)

	if !res.Success {
		t.Fatalf("handleGChatEvent() result = %+v, want Success (Ignored, so the watermark still advances past the garbage)", res)
	}
	if len(*queued) != 0 {
		t.Fatalf("len(queued) = %d, want 0 (no usable group id)", len(*queued))
	}
}

func TestHandleGChatEventReadReceiptChangedEmptySetSkipped(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := readReceiptChangedEvent(spaceGroupID("space-1")) // no receipts

	res := gc.handleGChatEvent(context.Background(), evt)

	if !res.Success {
		t.Fatalf("handleGChatEvent() result = %+v, want Success", res)
	}
	if len(*queued) != 0 {
		t.Fatalf("len(queued) = %d, want 0 (empty read receipt set)", len(*queued))
	}
}

// --- Inbound: GROUP_VIEWED --------------------------------------------------

// groupViewedEvent builds a *pb.Event shaped like a real GroupViewed event
// reaching handleGChatEvent, mirroring messageDeletedEvent's shape
// (events_test.go) for the group_viewed body arm.
func groupViewedEvent(groupID *pb.GroupId, viewTimeMicros int64) *pb.Event {
	return &pb.Event{
		GroupId: groupID,
		Type:    pb.Event_GROUP_VIEWED.Enum(),
		Body: &pb.Event_EventBody{
			EventType: pb.Event_GROUP_VIEWED.Enum(),
			Type: &pb.Event_EventBody_GroupViewed{
				GroupViewed: &pb.GroupViewedEvent{
					ViewTime: proto.Int64(viewTimeMicros),
				},
			},
		},
	}
}

func TestHandleGChatEventGroupViewedQueuesOwnUserReceipt(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := groupViewedEvent(spaceGroupID("space-1"), 1700000000123456)

	res := gc.handleGChatEvent(context.Background(), evt)

	if !res.Success {
		t.Fatalf("handleGChatEvent() result = %+v, want Success", res)
	}
	if len(*queued) != 1 {
		t.Fatalf("len(queued) = %d, want 1", len(*queued))
	}
	receipt, ok := (*queued)[0].(bridgev2.RemoteReadReceipt)
	if !ok {
		t.Fatalf("queued event does not implement bridgev2.RemoteReadReceipt: %T", (*queued)[0])
	}
	wantTS := gchatmeow.MicrosToTime(1700000000123456)
	if got := receipt.GetReadUpTo(); !got.Equal(wantTS) {
		t.Errorf("GetReadUpTo() = %v, want %v (view_time)", got, wantTS)
	}

	typed := (*queued)[0].(bridgev2.RemoteEvent)
	if got := typed.GetType(); got != bridgev2.RemoteEventReadReceipt {
		t.Errorf("GetType() = %v, want RemoteEventReadReceipt", got)
	}
	sender := typed.GetSender()
	if !sender.IsFromMe {
		t.Error("GetSender().IsFromMe = false, want true (group_viewed is always the login's own read marker)")
	}
	if sender.Sender != gcid.MakeUserID("112233") {
		t.Errorf("GetSender().Sender = %q, want %q (this login's own gaia id)", sender.Sender, gcid.MakeUserID("112233"))
	}

	wantPortalKey := gcid.MakePortalKey(gcid.GroupID{ID: "space-1", IsDM: false}, gc.UserLogin.ID)
	if got := typed.GetPortalKey(); got != wantPortalKey {
		t.Errorf("GetPortalKey() = %+v, want %+v", got, wantPortalKey)
	}
}

func TestHandleGChatEventGroupViewedDMPortalKeyHasReceiver(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := groupViewedEvent(dmGroupID("dm-1"), 1)

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

func TestHandleGChatEventGroupViewedNoGroupIDSkipped(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := groupViewedEvent(nil, 1)

	res := gc.handleGChatEvent(context.Background(), evt)

	if !res.Success {
		t.Fatalf("handleGChatEvent() result = %+v, want Success (Ignored, so the watermark still advances past the garbage)", res)
	}
	if len(*queued) != 0 {
		t.Fatalf("len(queued) = %d, want 0 (no usable group id)", len(*queued))
	}
}
