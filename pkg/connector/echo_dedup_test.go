package connector

// echo_dedup_test.go -- M2 Task 6: own-echo dedup via the local_id
// transaction-id round-trip. Ports portal.py's `_local_dedup` mechanism
// (portal.py:908-909,931,1341-1343) onto bridgev2's pending-transaction
// primitives: HandleMatrixMessage (handlematrix.go) registers the local_id
// it puts on the outgoing create_topic RPC as a pending-to-ignore
// transaction BEFORE issuing the RPC (closing the race window Python closes
// by adding to _local_dedup before dispatching -- see handlematrix.go's doc
// comment and docs/research/07 §"Echo dedup"/08b row 61 for the megabridge
// defect this fixes: AddPendingToIgnore called only AFTER the RPC returns,
// with no LocalId on CreateTopicRequest at all); queueMessagePosted
// (events.go) exposes the inbound MESSAGE_POSTED echo's own local_id
// (Message.LocalId, proto field 14) as the queued simplevent.Message's
// TransactionID, which is what bridgev2's checkPendingMessage (portal.go)
// matches against the pending map to recognize + drop the echo.
//
// These tests exercise the two halves in isolation (send-side registration,
// receive-side transaction-id exposure) via this package's existing
// createTopicFn/queueRemoteEventFn seams, plus the new addPendingToIgnoreFn
// seam (client.go) -- no full bridgev2.Bridge+DB harness is available (see
// client_test.go's newTestUserLogin), so the actual pending-map matching
// inside bridgev2.Portal (an unexported field, portal.go's outgoingMessages)
// is not itself under test here; that machinery is bridgev2's own and is
// exercised by mautrix-go's own test suite. What IS under test is that this
// connector wires the same local_id value into both halves of the
// round-trip, in the right order, matching Python's local_id fidelity.
import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// --- Send side: local_id becomes the pending-to-ignore transaction id -----

// TestHandleMatrixMessageRegistersLocalIDAsPendingBeforeSend pins the fix
// for the megabridge defect (docs/research/08b row 61 / 07 §"Echo dedup"):
// AddPendingToIgnore must be called with the SAME local_id that ends up on
// the wire in CreateTopicRequest, and it must happen BEFORE the RPC is
// issued -- not after the response comes back -- so a fast echo arriving on
// the event stream while the RPC is still in flight is already covered by
// the pending entry the instant it arrives.
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
		t.Error("addPendingToIgnoreFn was called after createTopicFn (the RPC), want before -- this reopens the echo race window Task 6 exists to close")
	}
}

// TestHandleMatrixMessageSuccessRemovesLocalIDFromPending mirrors
// portal.py:931's `self._local_dedup.remove(local_id)` on the success path:
// once create_topic succeeds and the DB message would be saved, the
// response must ask the framework to remove the same local_id from its
// pending-transaction table (MatrixMessageResponse.RemovePending), so it
// doesn't leak forever once the echo has already been (or never needs to
// be) matched.
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

// TestHandleMatrixMessageFailureLeavesPendingRegistered documents (rather
// than "fixes") a fidelity match with Python: on a failed send, Python never
// reaches the `self._local_dedup.remove(local_id)` line (portal.py:925-931's
// except branch skips the whole else block), so a failed send's local_id
// leaks in _local_dedup for the rest of the process lifetime. The Go
// equivalent is that RemovePending is only produced on the success return --
// an error return has no RemovePending for the caller to act on, so the
// pending-to-ignore entry this test's addPendingToIgnoreFn recorded is never
// asked to be removed either. This is intentional (matches Python), not an
// oversight.
func TestHandleMatrixMessageFailureLeavesPendingRegistered(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	registered := false
	gc := &GChatClient{
		UserLogin: login,
		addPendingToIgnoreFn: func(*bridgev2.MatrixMessage, networkid.TransactionID) {
			registered = true
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
	if !registered {
		t.Fatal("addPendingToIgnoreFn was never called -- local_id must still be registered before the failed RPC (Task 6 ordering), even though nothing removes it afterward")
	}
}

// --- Receive side: the echo's local_id becomes GetTransactionID() ---------

// TestHandleGChatEventMessagePostedEchoExposesLocalIDAsTransactionID pins
// the receive-side half of the round-trip: an inbound MESSAGE_POSTED whose
// Message.LocalId (proto field 14) matches a value this bridge itself
// generated must surface that same value from
// RemoteMessageWithTransactionID.GetTransactionID(), which is what lets
// bridgev2's own pending-transaction matching (portal.go's
// checkPendingMessage) recognize and drop the echo -- mirrors
// portal.py:1341's `if evt.local_id in self._local_dedup`.
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

// TestHandleGChatEventMessagePostedNoLocalIDHasEmptyTransactionID covers the
// "different device" case the task requires NOT be deduped: a message with
// no local_id on the wire at all (the common case for anything this bridge
// didn't itself just send -- another user's message, or the same user
// typing from a different, non-bridged client) must produce an empty
// GetTransactionID(). bridgev2's checkPendingMessage treats an empty
// transaction id as "not a pending echo" (portal.go: `if txnID == "" {
// return false, nil }`), so it bridges normally instead of being dropped.
func TestHandleGChatEventMessagePostedNoLocalIDHasEmptyTransactionID(t *testing.T) {
	gc, queued := newEventTestClient("112233")
	evt := messagePostedEvent(spaceGroupID("space-1"), "msg-other-device", "98765", "from another device", 1)
	// LocalId deliberately left unset (proto default: nil/"").

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

// TestHandleGChatEventMessagePostedDifferentLocalIDHasDistinctTransactionID
// covers the same "different device" requirement when the OTHER sender's
// client happens to set its own non-empty local_id (its own dedup token,
// meaningless to us): GetTransactionID() must carry that foreign value
// through unchanged rather than being blanked or coerced -- it will simply
// never match anything in this login's own pending-transaction table
// (a foreign local_id was never registered via AddPendingToIgnore here), so
// bridgev2 bridges the message normally instead of dropping it.
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
