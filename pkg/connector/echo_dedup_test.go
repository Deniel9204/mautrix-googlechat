package connector

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// TestHandleMatrixMessageRegistersLocalIDAsPendingBeforeSend: the wire local_id
// must be registered as pending before the RPC, so a fast echo is caught.
func TestHandleMatrixMessageRegistersLocalIDAsPendingBeforeSend(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.CreateTopicRequest
	var pendingTxnID networkid.TransactionID
	var pendingRegisteredBeforeSend bool
	sendCalled := false
	gc := &GChatClient{
		UserLogin: login,
		addPendingToIgnoreFn: func(msg *bridgev2.MatrixMessage, txnID networkid.TransactionID) {
			pendingTxnID = txnID
			pendingRegisteredBeforeSend = !sendCalled
		},
		createTopicFn: func(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			sendCalled = true
			gotReq = req
			return createTopicResponse("topic1", 1000), nil
		},
	}

	_, err := gc.HandleMatrixMessage(context.Background(), textMatrixMessage(spacePortal("space1"), "hello"))
	if err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}
	if gotReq == nil {
		t.Fatal("createTopicFn was not called")
	}
	if pendingTxnID == "" {
		t.Fatal("addPendingToIgnoreFn was not called with a non-empty transaction id")
	}
	if string(pendingTxnID) != gotReq.GetLocalId() {
		t.Errorf("pending txn id = %q, want it to equal the request's LocalId %q (same local_id both ways)", pendingTxnID, gotReq.GetLocalId())
	}
	if !pendingRegisteredBeforeSend {
		t.Error("addPendingToIgnoreFn was called after createTopicFn (the RPC), want before -- this reopens the echo race window the dedup exists to close")
	}
}

// TestHandleMatrixMessageSuccessRemovesLocalIDFromPending: success asks the
// framework to drop the pending local_id.
func TestHandleMatrixMessageSuccessRemovesLocalIDFromPending(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.CreateTopicRequest
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: func(*bridgev2.MatrixMessage, networkid.TransactionID) {},
		createTopicFn: func(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			gotReq = req
			return createTopicResponse("topic1", 1000), nil
		},
	}

	resp, err := gc.HandleMatrixMessage(context.Background(), textMatrixMessage(spacePortal("space1"), "hello"))
	if err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}
	if resp == nil {
		t.Fatal("HandleMatrixMessage() response is nil")
	}
	if string(resp.RemovePending) != gotReq.GetLocalId() {
		t.Errorf("RemovePending = %q, want it to equal the request's LocalId %q", resp.RemovePending, gotReq.GetLocalId())
	}
}

// TestHandleMatrixMessageFailureRemovesPendingRegistration: a failed send undoes
// the pending registration, which still happens before the RPC.
func TestHandleMatrixMessageFailureRemovesPendingRegistration(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var registeredTxnID, removedTxnID networkid.TransactionID
	gc := &GChatClient{
		UserLogin: login,
		addPendingToIgnoreFn: func(_ *bridgev2.MatrixMessage, txnID networkid.TransactionID) {
			registeredTxnID = txnID
		},
		removePendingFn: func(_ *bridgev2.MatrixMessage, txnID networkid.TransactionID) {
			removedTxnID = txnID
		},
		createTopicFn: func(context.Context, *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			return nil, context.DeadlineExceeded
		},
	}

	resp, err := gc.HandleMatrixMessage(context.Background(), textMatrixMessage(spacePortal("space1"), "hello"))
	if err == nil {
		t.Fatal("HandleMatrixMessage() error = nil, want an error from the failed RPC")
	}
	if resp != nil {
		t.Errorf("HandleMatrixMessage() response = %+v, want nil on error", resp)
	}
	if registeredTxnID == "" {
		t.Fatal("addPendingToIgnoreFn was never called -- local_id must still be registered before the failed RPC")
	}
	if removedTxnID != registeredTxnID {
		t.Errorf("removePendingFn txn id = %q, want it to match the registered txn id %q (RemovePending must undo the exact registration the failed send made)", removedTxnID, registeredTxnID)
	}
}

// TestHandleMatrixMessagePropagatesRPCErrorRemovesPendingRegistration: the real
// RemovePending is safe on bare test portals.
func TestHandleMatrixMessagePropagatesRPCErrorRemovesPendingRegistration(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createTopicFn: func(context.Context, *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			return nil, context.DeadlineExceeded
		},
	}

	if _, err := gc.HandleMatrixMessage(context.Background(), textMatrixMessage(spacePortal("space1"), "hello")); err == nil {
		t.Fatal("HandleMatrixMessage() error = nil, want an error from the failed RPC")
	}
}

// TestHandleGChatEventMessagePostedEchoExposesLocalIDAsTransactionID: the echo's
// local_id is the transaction id bridgev2 matches.
func TestHandleGChatEventMessagePostedEchoExposesLocalIDAsTransactionID(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := messagePostedEvent(spaceGroupID("space-1"), "msg-echo-1", "112233", "hello", 1)
	wantLocalID := "mautrix-googlechat%12345"
	evt.GetBody().GetMessagePosted().GetMessage().LocalId = proto.String(wantLocalID)

	gc.handleGChatEvent(context.Background(), evt)

	if len(*queued) != 1 {
		t.Fatalf("len(queued) = %d, want 1", len(*queued))
	}
	msgWithTxn, ok := (*queued)[0].(bridgev2.RemoteMessageWithTransactionID)
	if !ok {
		t.Fatalf("queued event does not implement bridgev2.RemoteMessageWithTransactionID: %T", (*queued)[0])
	}
	if got := msgWithTxn.GetTransactionID(); string(got) != wantLocalID {
		t.Errorf("GetTransactionID() = %q, want %q", got, wantLocalID)
	}
}

// TestHandleGChatEventMessagePostedNoLocalIDHasEmptyTransactionID: no local_id
// means an empty transaction id, so the message is bridged.
func TestHandleGChatEventMessagePostedNoLocalIDHasEmptyTransactionID(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := messagePostedEvent(spaceGroupID("space-1"), "msg-other-device", "98765", "from another device", 1)

	gc.handleGChatEvent(context.Background(), evt)

	if len(*queued) != 1 {
		t.Fatalf("len(queued) = %d, want 1", len(*queued))
	}
	msgWithTxn, ok := (*queued)[0].(bridgev2.RemoteMessageWithTransactionID)
	if !ok {
		t.Fatalf("queued event does not implement bridgev2.RemoteMessageWithTransactionID: %T", (*queued)[0])
	}
	if got := msgWithTxn.GetTransactionID(); got != "" {
		t.Errorf("GetTransactionID() = %q, want empty (no local_id on the wire -- must not be mistaken for our own echo)", got)
	}
}

// TestHandleGChatEventMessagePostedDifferentLocalIDHasDistinctTransactionID: a
// foreign local_id passes through unchanged and never matches.
func TestHandleGChatEventMessagePostedDifferentLocalIDHasDistinctTransactionID(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := messagePostedEvent(spaceGroupID("space-1"), "msg-other-device-2", "98765", "from another device", 1)
	foreignLocalID := "mautrix-googlechat%99999"
	evt.GetBody().GetMessagePosted().GetMessage().LocalId = proto.String(foreignLocalID)

	gc.handleGChatEvent(context.Background(), evt)

	if len(*queued) != 1 {
		t.Fatalf("len(queued) = %d, want 1", len(*queued))
	}
	msgWithTxn, ok := (*queued)[0].(bridgev2.RemoteMessageWithTransactionID)
	if !ok {
		t.Fatalf("queued event does not implement bridgev2.RemoteMessageWithTransactionID: %T", (*queued)[0])
	}
	if got := msgWithTxn.GetTransactionID(); string(got) != foreignLocalID {
		t.Errorf("GetTransactionID() = %q, want %q (foreign local_id passed through verbatim)", got, foreignLocalID)
	}
}
