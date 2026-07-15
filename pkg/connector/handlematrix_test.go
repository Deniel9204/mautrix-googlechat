package connector

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv/gchatfmt"
)

func spacePortal(id string) *bridgev2.Portal {
	return &bridgev2.Portal{Portal: &database.Portal{
		PortalKey: networkid.PortalKey{ID: gcid.MakePortalID(gcid.GroupID{ID: id, IsDM: false})},
	}}
}

func dmPortal(id string) *bridgev2.Portal {
	return &bridgev2.Portal{Portal: &database.Portal{
		PortalKey: networkid.PortalKey{ID: gcid.MakePortalID(gcid.GroupID{ID: id, IsDM: true})},
	}}
}

func textMatrixMessage(portal *bridgev2.Portal, body string) *bridgev2.MatrixMessage {
	return &bridgev2.MatrixMessage{
		MatrixEventBase: bridgev2.MatrixEventBase[*event.MessageEventContent]{
			Portal:  portal,
			Content: &event.MessageEventContent{MsgType: event.MsgText, Body: body},
		},
	}
}

func createTopicResponse(topicID string, createTimeUsec int64) *pb.CreateTopicResponse {
	return &pb.CreateTopicResponse{
		Topic: &pb.Topic{
			Id:             &pb.TopicId{TopicId: proto.String(topicID)},
			CreateTimeUsec: proto.Int64(createTimeUsec),
		},
	}
}

func createMessageResponse(messageID string, createTime int64) *pb.CreateMessageResponse {
	return &pb.CreateMessageResponse{
		Message: &pb.Message{
			Id:         &pb.MessageId{MessageId: proto.String(messageID)},
			CreateTime: proto.Int64(createTime),
		},
	}
}

// threadedMatrixMessage builds a *bridgev2.MatrixMessage whose ThreadRoot is
// pre-resolved (bridgev2's own job, mautrix-go bridgev2/portal.go:1233-1296 --
// not this connector's) to a *database.Message with the given root id and
// stored MessageMetadata.TopicID, matching what a genuine Matrix thread
// reply (or, in a threads-only room, bridgev2's reply -> thread
// auto-conversion) hands HandleMatrixMessage.
func threadedMatrixMessage(portal *bridgev2.Portal, body, rootID, rootTopicID string) *bridgev2.MatrixMessage {
	msg := textMatrixMessage(portal, body)
	msg.ThreadRoot = &database.Message{
		ID:       gcid.MakeMessageID(rootID),
		Metadata: &MessageMetadata{TopicID: rootTopicID},
	}
	return msg
}

// replyMatrixMessage builds a *bridgev2.MatrixMessage whose ReplyTo is
// pre-resolved (bridgev2's own job, mautrix-go bridgev2/portal.go:1248-1273 --
// not this connector's) to a *database.Message with the given target id and
// stored MessageMetadata.TimestampMicro, matching what a genuine Matrix
// quote-reply (m.in_reply_to) hands HandleMatrixMessage.
func replyMatrixMessage(portal *bridgev2.Portal, body, targetID string, targetTimestampMicro int64) *bridgev2.MatrixMessage {
	msg := textMatrixMessage(portal, body)
	msg.ReplyTo = &database.Message{
		ID:       gcid.MakeMessageID(targetID),
		Metadata: &MessageMetadata{TimestampMicro: targetTimestampMicro},
	}
	return msg
}

// noopAddPendingToIgnore is the addPendingToIgnoreFn override every
// HandleMatrixMessage test that doesn't itself assert on the pending
// registration needs: the real msg.AddPendingToIgnore (Task 6) writes into
// bridgev2.Portal's unexported outgoingMessages map, which is nil on the
// bare *bridgev2.Portal built by spacePortal/dmPortal/textMatrixMessage
// above (only a real bridgev2.Bridge's loadPortal, portal.go, initializes
// it) -- calling the real method against one would panic (assignment to
// entry in nil map). See echo_dedup_test.go for the tests that actually
// exercise this seam.
func noopAddPendingToIgnore(*bridgev2.MatrixMessage, networkid.TransactionID) {}

// --- HandleMatrixMessage: request construction -----------------------------

func TestHandleMatrixMessageSpacePortalBuildsCreateTopicRequest(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.CreateTopicRequest
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createTopicFn: func(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			gotReq = req
			return createTopicResponse("topic1", 1000), nil
		},
	}

	_, err := gc.HandleMatrixMessage(context.Background(), textMatrixMessage(spacePortal("space1"), "hello world"))
	if err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}
	if gotReq == nil {
		t.Fatal("createTopicFn was not called")
	}

	if got := gotReq.GetGroupId().GetSpaceId().GetSpaceId(); got != "space1" {
		t.Errorf("GroupId.SpaceId = %q, want %q", got, "space1")
	}
	if gotReq.GetGroupId().GetDmId() != nil {
		t.Error("GroupId.DmId is set for a space portal, want unset")
	}
	if got := gotReq.GetTextBody(); got != "hello world" {
		t.Errorf("TextBody = %q, want %q", got, "hello world")
	}
	if !gotReq.GetHistoryV2() {
		t.Error("HistoryV2 = false, want true (send_message always sets it, client.py:465)")
	}
	if !gotReq.GetMessageInfo().GetAcceptFormatAnnotations() {
		t.Error("MessageInfo.AcceptFormatAnnotations = false, want true (client.py:467-469)")
	}
	if gotReq.GetMessageInfo().GetReplyTo() != nil {
		t.Error("MessageInfo.ReplyTo is set, want nil (msg.ReplyTo is nil -- not a quote-reply)")
	}
	if gotReq.GetLocalId() == "" {
		t.Error("LocalId is empty, want a generated dedup token")
	}
	if !strings.HasPrefix(gotReq.GetLocalId(), "mautrix-googlechat%") {
		t.Errorf("LocalId = %q, want prefix %q (matching portal.py:908)", gotReq.GetLocalId(), "mautrix-googlechat%")
	}
}

func TestHandleMatrixMessageDMPortalBuildsDmGroupID(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.CreateTopicRequest
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createTopicFn: func(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			gotReq = req
			return createTopicResponse("topic2", 2000), nil
		},
	}

	_, err := gc.HandleMatrixMessage(context.Background(), textMatrixMessage(dmPortal("dm1"), "hi"))
	if err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}
	if gotReq == nil {
		t.Fatal("createTopicFn was not called")
	}

	if got := gotReq.GetGroupId().GetDmId().GetDmId(); got != "dm1" {
		t.Errorf("GroupId.DmId = %q, want %q", got, "dm1")
	}
	if gotReq.GetGroupId().GetSpaceId() != nil {
		t.Error("GroupId.SpaceId is set for a DM portal, want unset")
	}
}

// TestHandleMatrixMessageLocalIDsAreUnique guards against a lazy
// implementation that reuses a single local_id for every send -- Python
// generates a fresh random.randint per call (portal.py:908), so two sends
// must not collide (a collision would break Task 6's dedup-by-local_id
// mechanism, silently conflating two unrelated messages).
func TestHandleMatrixMessageLocalIDsAreUnique(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var ids []string
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createTopicFn: func(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			ids = append(ids, req.GetLocalId())
			return createTopicResponse("t", 1), nil
		},
	}

	for i := 0; i < 5; i++ {
		if _, err := gc.HandleMatrixMessage(context.Background(), textMatrixMessage(spacePortal("space1"), "x")); err != nil {
			t.Fatalf("HandleMatrixMessage() error = %v", err)
		}
	}

	seen := map[string]bool{}
	for _, id := range ids {
		if seen[id] {
			t.Fatalf("local_id %q generated more than once across 5 sends: %v", id, ids)
		}
		seen[id] = true
	}
}

// --- HandleMatrixMessage: response -> DB mapping ----------------------------

func TestHandleMatrixMessageMapsResponseToDBMessage(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createTopicFn: func(context.Context, *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			return createTopicResponse("gc-msg-id-1", 1_700_000_000_000_000), nil
		},
	}

	resp, err := gc.HandleMatrixMessage(context.Background(), textMatrixMessage(spacePortal("space1"), "hello"))
	if err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}
	if resp == nil || resp.DB == nil {
		t.Fatal("HandleMatrixMessage() response/DB is nil")
	}

	if resp.DB.ID != gcid.MakeMessageID("gc-msg-id-1") {
		t.Errorf("DB.ID = %q, want %q", resp.DB.ID, gcid.MakeMessageID("gc-msg-id-1"))
	}
	if resp.DB.SenderID != gcid.MakeUserID("112233") {
		t.Errorf("DB.SenderID = %q, want own user id %q", resp.DB.SenderID, gcid.MakeUserID("112233"))
	}
	wantTS := time.UnixMicro(1_700_000_000_000_000).UTC()
	if !resp.DB.Timestamp.Equal(wantTS) {
		t.Errorf("DB.Timestamp = %v, want %v", resp.DB.Timestamp, wantTS)
	}
	meta, ok := resp.DB.Metadata.(*MessageMetadata)
	if !ok {
		t.Fatalf("DB.Metadata type = %T, want *MessageMetadata", resp.DB.Metadata)
	}
	if meta.TimestampMicro != 1_700_000_000_000_000 {
		t.Errorf("Metadata.TimestampMicro = %d, want %d", meta.TimestampMicro, 1_700_000_000_000_000)
	}
	// M3 Task 6: a create_topic response's own topic id is the new message's
	// own id (message_id == topic_id for the head of a brand new topic) --
	// stored so a later Matrix thread reply targeting THIS message can be
	// routed to create_message with the right parent_id.topic_id.
	if meta.TopicID != "gc-msg-id-1" {
		t.Errorf("Metadata.TopicID = %q, want %q (message_id == topic_id for a new topic's head)", meta.TopicID, "gc-msg-id-1")
	}
}

// --- HandleMatrixMessage: error paths ---------------------------------------

func TestHandleMatrixMessageUnsupportedMsgTypeRejected(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	called := false
	gc := &GChatClient{
		UserLogin: login,
		createTopicFn: func(context.Context, *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			called = true
			return createTopicResponse("t", 1), nil
		},
	}

	// m.location: neither TEXT/NOTICE nor one of the four outbound media
	// msgtypes M5 Task 5 added (m.image is now accepted -- see
	// TestHandleMatrixMessageImageBuildsUploadAnnotation below), so this
	// still exercises handle_matrix_message's final else branch
	// (portal.py:923-924).
	msg := textMatrixMessage(spacePortal("space1"), "a location")
	msg.Content.MsgType = event.MsgLocation

	_, err := gc.HandleMatrixMessage(context.Background(), msg)
	if !errors.Is(err, bridgev2.ErrUnsupportedMessageType) {
		t.Errorf("error = %v, want bridgev2.ErrUnsupportedMessageType", err)
	}
	if called {
		t.Error("createTopicFn was called for an unsupported msgtype")
	}
}

func TestHandleMatrixMessageNoticeMsgTypeAccepted(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.CreateTopicRequest
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createTopicFn: func(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			gotReq = req
			return createTopicResponse("t", 1), nil
		},
	}

	msg := textMatrixMessage(spacePortal("space1"), "a notice")
	msg.Content.MsgType = event.MsgNotice

	if _, err := gc.HandleMatrixMessage(context.Background(), msg); err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil (NOTICE must be accepted like TEXT, portal.py:915)", err)
	}
	if gotReq.GetTextBody() != "a notice" {
		t.Errorf("TextBody = %q, want %q", gotReq.GetTextBody(), "a notice")
	}
}

func TestHandleMatrixMessageInvalidPortalIDErrors(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	called := false
	gc := &GChatClient{
		UserLogin: login,
		createTopicFn: func(context.Context, *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			called = true
			return createTopicResponse("t", 1), nil
		},
	}

	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: networkid.PortalID("garbage")}}}
	_, err := gc.HandleMatrixMessage(context.Background(), textMatrixMessage(portal, "hi"))
	if err == nil {
		t.Fatal("HandleMatrixMessage() error = nil, want an error for an unparseable portal id")
	}
	if called {
		t.Error("createTopicFn was called despite an invalid portal id")
	}
}

func TestHandleMatrixMessageNoConnNoSeamErrors(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	gc := &GChatClient{UserLogin: login}

	_, err := gc.HandleMatrixMessage(context.Background(), textMatrixMessage(spacePortal("space1"), "hi"))
	if err == nil {
		t.Fatal("HandleMatrixMessage() error = nil, want an error when not connected")
	}
}

func TestHandleMatrixMessagePropagatesRPCError(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	wantErr := errors.New("create_topic: boom")
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createTopicFn: func(context.Context, *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			return nil, wantErr
		},
	}

	_, err := gc.HandleMatrixMessage(context.Background(), textMatrixMessage(spacePortal("space1"), "hi"))
	if !errors.Is(err, wantErr) {
		t.Errorf("error = %v, want wrapping %v", err, wantErr)
	}
}

// --- HandleMatrixMessage: M3 Task 4 formatting/mention wiring --------------

// TestHandleMatrixMessagePlainTextHasNoAnnotations pins the outbound
// fast path: a plain (unformatted) Matrix message must produce a request
// with no annotations at all, not an empty-but-non-nil slice.
func TestHandleMatrixMessagePlainTextHasNoAnnotations(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.CreateTopicRequest
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createTopicFn: func(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			gotReq = req
			return createTopicResponse("t", 1), nil
		},
	}

	_, err := gc.HandleMatrixMessage(context.Background(), textMatrixMessage(spacePortal("space1"), "hello world"))
	if err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}
	if len(gotReq.GetAnnotations()) != 0 {
		t.Errorf("Annotations = %v, want none for a plain-text message", gotReq.GetAnnotations())
	}
}

// TestHandleMatrixMessageFormattedTextBuildsAnnotations is the headline M3
// Task 4 outbound behavior: a Matrix message with an HTML formatted_body
// must produce a request whose TextBody is the annotation-stripped text,
// whose Annotations carry the HTML-derived formatting, and whose
// MessageInfo.AcceptFormatAnnotations is (still, unconditionally) true --
// matching client.py's send_message, which never gates
// accept_format_annotations on whether annotations is empty
// (client.py:467-469).
func TestHandleMatrixMessageFormattedTextBuildsAnnotations(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.CreateTopicRequest
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createTopicFn: func(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			gotReq = req
			return createTopicResponse("t", 1), nil
		},
	}

	msg := textMatrixMessage(spacePortal("space1"), "a b c")
	msg.Content.Format = event.FormatHTML
	msg.Content.FormattedBody = "a <strong>b</strong> c"

	_, err := gc.HandleMatrixMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}
	if got := gotReq.GetTextBody(); got != "a b c" {
		t.Errorf("TextBody = %q, want %q", got, "a b c")
	}
	wantAnn := gchatfmt.MakeFormatAnnotation(2, 1, pb.FormatMetadata_BOLD)
	if len(gotReq.GetAnnotations()) != 1 || gotReq.GetAnnotations()[0].String() != wantAnn.String() {
		t.Errorf("Annotations = %s, want [%s]", formatAnnotationsForTest(gotReq.GetAnnotations()), wantAnn.String())
	}
	if !gotReq.GetMessageInfo().GetAcceptFormatAnnotations() {
		t.Error("MessageInfo.AcceptFormatAnnotations = false, want true")
	}
}

// TestHandleMatrixMessageMentionPillBecomesMentionAnnotation proves M3 Task
// 4's headline wiring gap is closed: newOutboundMentionResolver, built from
// msg.Portal, is now actually threaded into the send path (it previously
// existed -- Task 3 -- but nothing called it). A Matrix mention pill for a
// known ghost must become a MENTION annotation in the outbound request.
func TestHandleMatrixMessageMentionPillBecomesMentionAnnotation(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	matrix := &fakeMatrixConnector{
		parseGhostMXID: func(mxid id.UserID) (networkid.UserID, bool) {
			if mxid == "@200_ghost:example.com" {
				return "200", true
			}
			return "", false
		},
	}
	portal := &bridgev2.Portal{
		Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: gcid.MakePortalID(gcid.GroupID{ID: "space1", IsDM: false})}},
		Bridge: &bridgev2.Bridge{Matrix: matrix},
	}
	var gotReq *pb.CreateTopicRequest
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createTopicFn: func(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			gotReq = req
			return createTopicResponse("t", 1), nil
		},
	}

	msg := textMatrixMessage(portal, "plain-text fallback")
	msg.Content.Format = event.FormatHTML
	msg.Content.FormattedBody = `Hi <a href="https://matrix.to/#/@200_ghost:example.com">Bob</a>!`

	_, err := gc.HandleMatrixMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}
	if got := gotReq.GetTextBody(); got != "Hi @Bob!" {
		t.Errorf("TextBody = %q, want %q", got, "Hi @Bob!")
	}
	wantAnn := gchatfmt.MakeMentionAnnotation(3, 4, "200")
	if len(gotReq.GetAnnotations()) != 1 || gotReq.GetAnnotations()[0].String() != wantAnn.String() {
		t.Errorf("Annotations = %s, want [%s] (fix B2's outbound half)", formatAnnotationsForTest(gotReq.GetAnnotations()), wantAnn.String())
	}
}

// TestHandleMatrixMessageUnresolvableMentionRendersPlainText: a pill for an
// MXID this bridge has no record of must render as plain text with no
// MENTION annotation, not an error or a broken pill.
func TestHandleMatrixMessageUnresolvableMentionRendersPlainText(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.CreateTopicRequest
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createTopicFn: func(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			gotReq = req
			return createTopicResponse("t", 1), nil
		},
	}

	msg := textMatrixMessage(spacePortal("space1"), "plain-text fallback")
	msg.Content.Format = event.FormatHTML
	msg.Content.FormattedBody = `Hi <a href="https://matrix.to/#/@stranger:elsewhere.example">Stranger</a>!`

	_, err := gc.HandleMatrixMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}
	if got := gotReq.GetTextBody(); got != "Hi Stranger!" {
		t.Errorf("TextBody = %q, want %q", got, "Hi Stranger!")
	}
	if len(gotReq.GetAnnotations()) != 0 {
		t.Errorf("Annotations = %v, want none for an unresolvable mention", gotReq.GetAnnotations())
	}
}

// --- mergeAnnotations (B4 fix) ----------------------------------------------
// docs/research/08d-megabridge-msgconv.md §2.4: megabridge's handlematrix.go
// built annotations=[UPLOAD_METADATA] for a media message, then did
// `if entities != nil { annotations = entities }` -- unconditionally
// REPLACING the file annotation with the caption's own formatting
// annotations whenever the caption had ANY formatting, silently dropping
// the attachment from the wire request. mergeAnnotations fixes this by
// always appending rather than assigning.

func TestMergeAnnotations_KeepsFileAndTextAnnotations(t *testing.T) {
	// Stub file annotation: M3 has no real UPLOAD_METADATA-building code
	// yet (M5), but any pre-existing annotation stands in for one here --
	// the fix must not care what "existing" actually contains.
	fileAnnotation := &pb.Annotation{Type: pb.AnnotationType_UPLOAD_METADATA.Enum()}
	textAnnotations := []*pb.Annotation{gchatfmt.MakeFormatAnnotation(0, 4, pb.FormatMetadata_BOLD)}

	got := mergeAnnotations([]*pb.Annotation{fileAnnotation}, textAnnotations)

	if len(got) != 2 {
		t.Fatalf("mergeAnnotations returned %d annotations, want 2 (file + text) -- B4 regression: the file annotation must survive a formatted caption", len(got))
	}
	if got[0] != fileAnnotation {
		t.Errorf("mergeAnnotations()[0] = %v, want the file annotation preserved first", got[0])
	}
	if got[1] != textAnnotations[0] {
		t.Errorf("mergeAnnotations()[1] = %v, want the caption's BOLD annotation appended after it", got[1])
	}
}

func TestMergeAnnotations_NoTextReturnsExistingUnchanged(t *testing.T) {
	fileAnnotation := &pb.Annotation{Type: pb.AnnotationType_UPLOAD_METADATA.Enum()}
	existing := []*pb.Annotation{fileAnnotation}

	got := mergeAnnotations(existing, nil)

	if len(got) != 1 || got[0] != fileAnnotation {
		t.Errorf("mergeAnnotations(existing, nil) = %v, want existing unchanged (a plain-text caption keeps only the file annotation)", got)
	}
}

func TestMergeAnnotations_NoExistingReturnsTextUnchanged(t *testing.T) {
	textAnnotations := []*pb.Annotation{gchatfmt.MakeFormatAnnotation(0, 4, pb.FormatMetadata_BOLD)}

	got := mergeAnnotations(nil, textAnnotations)

	if len(got) != 1 || got[0] != textAnnotations[0] {
		t.Errorf("mergeAnnotations(nil, text) = %v, want text unchanged", got)
	}
}

func TestMergeAnnotations_BothNilReturnsNil(t *testing.T) {
	if got := mergeAnnotations(nil, nil); got != nil {
		t.Errorf("mergeAnnotations(nil, nil) = %v, want nil (plain-body outbound -> no annotations)", got)
	}
}

// --- HandleMatrixMessage: thread routing (M3 Task 6) ------------------------
//
// Ports handle_matrix_message's thread_id computation (portal.py:891-907)
// restricted to the ThreadRoot half bridgev2 hands this connector directly
// (bridgev2 has already resolved a Matrix thread reply -- or, in a
// threads-only room, auto-converted a plain reply into one, mautrix-go
// bridgev2/portal.go:1259-1268 -- into MatrixMessage.ThreadRoot, a
// pre-fetched *database.Message, before HandleMatrixMessage is ever called):
// msg.ThreadRoot != nil routes to create_message with parent_id.topic_id
// set to the root's own stored topic id (client.py's send_message,
// `if thread_id: CreateMessageRequest(...)`, client.py:441-458); msg.ThreadRoot
// == nil keeps the existing create_topic path (client.py's else branch).

// threadCreateTopicResponse and threadCreateMessageResponse below are used
// together in several tests to assert exactly one of the two RPCs fires.

func TestHandleMatrixMessageThreadRootRoutesToCreateMessage(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.CreateMessageRequest
	topicCalled := false
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createTopicFn: func(context.Context, *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			topicCalled = true
			return createTopicResponse("wrong-path", 1), nil
		},
		createMessageFn: func(_ context.Context, req *pb.CreateMessageRequest) (*pb.CreateMessageResponse, error) {
			gotReq = req
			return createMessageResponse("reply-msg-1", 2000), nil
		},
	}

	msg := threadedMatrixMessage(spacePortal("space1"), "a reply", "topic1", "topic1")
	_, err := gc.HandleMatrixMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}
	if topicCalled {
		t.Error("createTopicFn was called for a threaded message, want createMessageFn only")
	}
	if gotReq == nil {
		t.Fatal("createMessageFn was not called")
	}
	if got := gotReq.GetParentId().GetTopicId().GetTopicId(); got != "topic1" {
		t.Errorf("ParentId.TopicId.TopicId = %q, want %q (the thread root's stored topic id)", got, "topic1")
	}
	if got := gotReq.GetParentId().GetTopicId().GetGroupId().GetSpaceId().GetSpaceId(); got != "space1" {
		t.Errorf("ParentId.TopicId.GroupId.SpaceId = %q, want %q", got, "space1")
	}
	if got := gotReq.GetTextBody(); got != "a reply" {
		t.Errorf("TextBody = %q, want %q", got, "a reply")
	}
	if !gotReq.GetMessageInfo().GetAcceptFormatAnnotations() {
		t.Error("MessageInfo.AcceptFormatAnnotations = false, want true (client.py:453-456)")
	}
	if gotReq.GetLocalId() == "" {
		t.Error("LocalId is empty, want a generated dedup token")
	}
}

// TestHandleMatrixMessageThreadedDMPortalBuildsDmGroupID covers the DM half
// of "cover DM/space/threaded-space": a threaded reply in a DM portal must
// still build a dm_id GroupId inside ParentId.TopicId.GroupId (Google Chat
// DMs never actually have UI-visible threads, but the wire shape is
// identical -- this just proves the DM/space branch in group id
// construction isn't skipped on the create_message path).
func TestHandleMatrixMessageThreadedDMPortalBuildsDmGroupID(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.CreateMessageRequest
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createMessageFn: func(_ context.Context, req *pb.CreateMessageRequest) (*pb.CreateMessageResponse, error) {
			gotReq = req
			return createMessageResponse("reply-msg-2", 3000), nil
		},
	}

	msg := threadedMatrixMessage(dmPortal("dm1"), "hi", "topic1", "topic1")
	if _, err := gc.HandleMatrixMessage(context.Background(), msg); err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}
	if got := gotReq.GetParentId().GetTopicId().GetGroupId().GetDmId().GetDmId(); got != "dm1" {
		t.Errorf("ParentId.TopicId.GroupId.DmId = %q, want %q", got, "dm1")
	}
	if gotReq.GetParentId().GetTopicId().GetGroupId().GetSpaceId() != nil {
		t.Error("ParentId.TopicId.GroupId.SpaceId is set for a DM portal, want unset")
	}
}

// TestHandleMatrixMessageNoThreadRootRoutesToCreateTopic is the converse of
// the above: a message with no ThreadRoot (msg.ThreadRoot == nil, the
// default bridgev2 hands a non-thread-reply Matrix message) must go through
// create_topic and never touch createMessageFn at all.
func TestHandleMatrixMessageNoThreadRootRoutesToCreateTopic(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	messageCalled := false
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createTopicFn: func(context.Context, *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			return createTopicResponse("topic1", 1000), nil
		},
		createMessageFn: func(context.Context, *pb.CreateMessageRequest) (*pb.CreateMessageResponse, error) {
			messageCalled = true
			return createMessageResponse("wrong-path", 1), nil
		},
	}

	_, err := gc.HandleMatrixMessage(context.Background(), textMatrixMessage(spacePortal("space1"), "hello"))
	if err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}
	if messageCalled {
		t.Error("createMessageFn was called for a non-threaded message, want createTopicFn only")
	}
}

// TestHandleMatrixMessageThreadRootFallsBackToRootIDWhenTopicIDMissing pins
// Python's own fallback (portal.py:895, `thread_id = thread_parent.gc_parent_id
// or thread_parent.gcid`): if the thread root message's own stored
// MessageMetadata.TopicID is empty (e.g. a legacy DB row from before this
// task, or a metadata type mismatch), route using the root message's own id
// instead -- which is correct anyway, since message_id == topic_id for any
// head message.
func TestHandleMatrixMessageThreadRootFallsBackToRootIDWhenTopicIDMissing(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.CreateMessageRequest
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createMessageFn: func(_ context.Context, req *pb.CreateMessageRequest) (*pb.CreateMessageResponse, error) {
			gotReq = req
			return createMessageResponse("reply-msg-3", 4000), nil
		},
	}

	msg := textMatrixMessage(spacePortal("space1"), "a reply")
	msg.ThreadRoot = &database.Message{ID: gcid.MakeMessageID("headmsg1")} // no Metadata at all

	if _, err := gc.HandleMatrixMessage(context.Background(), msg); err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}
	if got := gotReq.GetParentId().GetTopicId().GetTopicId(); got != "headmsg1" {
		t.Errorf("ParentId.TopicId.TopicId = %q, want %q (fallback to the root's own message id)", got, "headmsg1")
	}
}

// TestHandleMatrixMessageThreadedMapsResponseToDBMessage: _get_send_response's
// CreateMessageResponse arm (portal.py:1049): gcid=resp.message.id.message_id,
// timestamp=resp.message.create_time -- note this is the NEW reply's own
// message id, never the topic id it was posted into.
func TestHandleMatrixMessageThreadedMapsResponseToDBMessage(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createMessageFn: func(context.Context, *pb.CreateMessageRequest) (*pb.CreateMessageResponse, error) {
			return createMessageResponse("reply-msg-4", 1_700_000_000_000_000), nil
		},
	}

	msg := threadedMatrixMessage(spacePortal("space1"), "a reply", "topic1", "topic1")
	resp, err := gc.HandleMatrixMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}
	if resp == nil || resp.DB == nil {
		t.Fatal("HandleMatrixMessage() response/DB is nil")
	}
	if resp.DB.ID != gcid.MakeMessageID("reply-msg-4") {
		t.Errorf("DB.ID = %q, want %q (the reply's own message id, not the topic id)", resp.DB.ID, gcid.MakeMessageID("reply-msg-4"))
	}
	if resp.DB.SenderID != gcid.MakeUserID("112233") {
		t.Errorf("DB.SenderID = %q, want own user id %q", resp.DB.SenderID, gcid.MakeUserID("112233"))
	}
	wantTS := time.UnixMicro(1_700_000_000_000_000).UTC()
	if !resp.DB.Timestamp.Equal(wantTS) {
		t.Errorf("DB.Timestamp = %v, want %v", resp.DB.Timestamp, wantTS)
	}
	meta, ok := resp.DB.Metadata.(*MessageMetadata)
	if !ok {
		t.Fatalf("DB.Metadata type = %T, want *MessageMetadata", resp.DB.Metadata)
	}
	if meta.TimestampMicro != 1_700_000_000_000_000 {
		t.Errorf("Metadata.TimestampMicro = %d, want %d", meta.TimestampMicro, 1_700_000_000_000_000)
	}
	if meta.TopicID != "topic1" {
		t.Errorf("Metadata.TopicID = %q, want %q (the topic this reply was posted into)", meta.TopicID, "topic1")
	}
}

// TestHandleMatrixMessageThreadedNoConnNoSeamErrors mirrors
// TestHandleMatrixMessageNoConnNoSeamErrors for the threaded path: no
// createMessageFn and no live conn must error, not panic or silently fall
// back to create_topic.
func TestHandleMatrixMessageThreadedNoConnNoSeamErrors(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	gc := &GChatClient{UserLogin: login}

	msg := threadedMatrixMessage(spacePortal("space1"), "a reply", "topic1", "topic1")
	_, err := gc.HandleMatrixMessage(context.Background(), msg)
	if err == nil {
		t.Fatal("HandleMatrixMessage() error = nil, want an error when not connected")
	}
}

// TestHandleMatrixMessageThreadedPropagatesRPCError mirrors
// TestHandleMatrixMessagePropagatesRPCError for the threaded path.
func TestHandleMatrixMessageThreadedPropagatesRPCError(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	wantErr := errors.New("create_message: boom")
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createMessageFn: func(context.Context, *pb.CreateMessageRequest) (*pb.CreateMessageResponse, error) {
			return nil, wantErr
		},
	}

	msg := threadedMatrixMessage(spacePortal("space1"), "a reply", "topic1", "topic1")
	_, err := gc.HandleMatrixMessage(context.Background(), msg)
	if !errors.Is(err, wantErr) {
		t.Errorf("error = %v, want wrapping %v", err, wantErr)
	}
}

// TestHandleMatrixMessageThreadedFailureRemovesPendingRegistration mirrors
// echo_dedup_test.go's TestHandleMatrixMessageFailureRemovesPendingRegistration
// for the threaded path: registration (before the RPC) must still happen,
// and a failed create_message must undo it via RemovePending.
func TestHandleMatrixMessageThreadedFailureRemovesPendingRegistration(t *testing.T) {
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
		createMessageFn: func(context.Context, *pb.CreateMessageRequest) (*pb.CreateMessageResponse, error) {
			return nil, context.DeadlineExceeded
		},
	}

	msg := threadedMatrixMessage(spacePortal("space1"), "a reply", "topic1", "topic1")
	resp, err := gc.HandleMatrixMessage(context.Background(), msg)
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
		t.Errorf("removePendingFn txn id = %q, want it to match the registered txn id %q", removedTxnID, registeredTxnID)
	}
}

// TestHandleMatrixMessageThreadedUsesSameLocalIDPrefix pins that the
// threaded path reuses the same newLocalID token format as create_topic --
// it is the same per-send dedup token regardless of which RPC it ends up
// used on (portal.py generates local_id once, before branching on
// thread_id, portal.py:908).
func TestHandleMatrixMessageThreadedUsesSameLocalIDPrefix(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.CreateMessageRequest
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createMessageFn: func(_ context.Context, req *pb.CreateMessageRequest) (*pb.CreateMessageResponse, error) {
			gotReq = req
			return createMessageResponse("reply-msg-5", 1), nil
		},
	}

	msg := threadedMatrixMessage(spacePortal("space1"), "hi", "topic1", "topic1")
	if _, err := gc.HandleMatrixMessage(context.Background(), msg); err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}
	if !strings.HasPrefix(gotReq.GetLocalId(), "mautrix-googlechat%") {
		t.Errorf("LocalId = %q, want prefix %q", gotReq.GetLocalId(), "mautrix-googlechat%")
	}
}

// TestHandleMatrixMessageThreadedFormattingAndMentionsWired proves the
// threaded path shares the same formatting/mention conversion as
// create_topic (M3 Task 4), not a stripped-down copy: an HTML body must
// still produce the annotation-stripped TextBody and the formatting
// annotation on the CreateMessageRequest.
func TestHandleMatrixMessageThreadedFormattingAndMentionsWired(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.CreateMessageRequest
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createMessageFn: func(_ context.Context, req *pb.CreateMessageRequest) (*pb.CreateMessageResponse, error) {
			gotReq = req
			return createMessageResponse("reply-msg-6", 1), nil
		},
	}

	msg := threadedMatrixMessage(spacePortal("space1"), "a b c", "topic1", "topic1")
	msg.Content.Format = event.FormatHTML
	msg.Content.FormattedBody = "a <strong>b</strong> c"

	if _, err := gc.HandleMatrixMessage(context.Background(), msg); err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}
	if got := gotReq.GetTextBody(); got != "a b c" {
		t.Errorf("TextBody = %q, want %q", got, "a b c")
	}
	wantAnn := gchatfmt.MakeFormatAnnotation(2, 1, pb.FormatMetadata_BOLD)
	if len(gotReq.GetAnnotations()) != 1 || gotReq.GetAnnotations()[0].String() != wantAnn.String() {
		t.Errorf("Annotations = %s, want [%s]", formatAnnotationsForTest(gotReq.GetAnnotations()), wantAnn.String())
	}
}

// --- HandleMatrixMessage: quote-replies (M3 Task 7, SendReplyTarget) -------
//
// Ports client.py's send_message reply_to_wrapped construction
// (client.py:423-438): SendReplyTarget{id: MessageId{parent_id.topic_id: {
// group_id, topic_id: thread_id or reply_to}, message_id: reply_to},
// create_time: reply_to_ts}, gated on `if reply_to else None`. thread_id is
// this connector's own topicID local (set when routing into an existing
// thread, sendThreadedMessage) or "" (sendNewTopic, no thread) -- the "or
// reply_to" fallback then uses the reply target's OWN message id as its
// (guessed) topic id, which portal.py's own upstream routing
// (portal.py:896-900, elif reply_to.gc_parent_id != reply_to.gcid: reroute
// to a thread post and clear reply_to) guarantees is correct whenever
// thread_id is empty: a reply_to that survives to this point with no
// thread_id is always the head of its own topic.

// TestHandleMatrixMessageReplyBuildsSendReplyTarget covers the plain
// (non-threaded) quote-reply case: msg.ReplyTo set, msg.ThreadRoot nil ->
// create_topic with message_info.reply_to populated from the target's own
// stored id + µs create_time (MessageMetadata.TimestampMicro), and --
// matching client.py's `thread_id or reply_to` fallback with thread_id ==
// "" here -- the reply target's own nested topic_id falls back to the
// target's own message id.
func TestHandleMatrixMessageReplyBuildsSendReplyTarget(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.CreateTopicRequest
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createTopicFn: func(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			gotReq = req
			return createTopicResponse("t", 1), nil
		},
	}

	msg := replyMatrixMessage(spacePortal("space1"), "a reply", "target1", 555_000)
	if _, err := gc.HandleMatrixMessage(context.Background(), msg); err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}

	replyTarget := gotReq.GetMessageInfo().GetReplyTo()
	if replyTarget == nil {
		t.Fatal("MessageInfo.ReplyTo is nil, want a SendReplyTarget")
	}
	if got := replyTarget.GetId().GetMessageId(); got != "target1" {
		t.Errorf("ReplyTo.Id.MessageId = %q, want %q", got, "target1")
	}
	if got := replyTarget.GetCreateTime(); got != 555_000 {
		t.Errorf("ReplyTo.CreateTime = %d, want %d (the target's stored µs create_time)", got, 555_000)
	}
	if got := replyTarget.GetId().GetParentId().GetTopicId().GetTopicId(); got != "target1" {
		t.Errorf("ReplyTo.Id.ParentId.TopicId.TopicId = %q, want %q (thread_id empty -> falls back to the target's own id, client.py:429)", got, "target1")
	}
	if got := replyTarget.GetId().GetParentId().GetTopicId().GetGroupId().GetSpaceId().GetSpaceId(); got != "space1" {
		t.Errorf("ReplyTo.Id.ParentId.TopicId.GroupId.SpaceId = %q, want %q", got, "space1")
	}
}

// TestHandleMatrixMessageReplyInDMPortalBuildsDmGroupID mirrors the DM/space
// coverage pattern used for the plain create_topic and threaded paths above,
// for the reply target's own nested GroupId.
func TestHandleMatrixMessageReplyInDMPortalBuildsDmGroupID(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.CreateTopicRequest
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createTopicFn: func(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			gotReq = req
			return createTopicResponse("t", 1), nil
		},
	}

	msg := replyMatrixMessage(dmPortal("dm1"), "a reply", "target1", 555_000)
	if _, err := gc.HandleMatrixMessage(context.Background(), msg); err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}

	if got := gotReq.GetMessageInfo().GetReplyTo().GetId().GetParentId().GetTopicId().GetGroupId().GetDmId().GetDmId(); got != "dm1" {
		t.Errorf("ReplyTo.Id.ParentId.TopicId.GroupId.DmId = %q, want %q", got, "dm1")
	}
}

// TestHandleMatrixMessageReplyAndThreadBothSet covers the "reply can also be
// in a thread" composition case the task brief calls out: bridgev2 can hand
// HandleMatrixMessage a MatrixMessage with BOTH ThreadRoot and ReplyTo set
// (an explicit Matrix thread reply that ALSO carries a non-fallback
// m.in_reply_to to a specific message within that thread, portal.py's own
// comment at portal.py:893-894: "If there's an additional non-fallback
// reply, it'll also be used." -- or bridgev2's own reply->thread-root
// auto-derivation, mautrix-go bridgev2/portal.go:1259-1271, which does NOT
// clear ReplyTo when the connector supports the Reply capability). Thread
// ROUTING must stay governed purely by ThreadRoot (create_message, Task 6,
// unchanged); the reply target's own nested topic_id must use the thread's
// topic id (client.py's `thread_id or reply_to`, thread_id truthy here),
// NOT the reply target's own message id.
func TestHandleMatrixMessageReplyAndThreadBothSet(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	var gotReq *pb.CreateMessageRequest
	topicCalled := false
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		createTopicFn: func(context.Context, *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			topicCalled = true
			return createTopicResponse("wrong-path", 1), nil
		},
		createMessageFn: func(_ context.Context, req *pb.CreateMessageRequest) (*pb.CreateMessageResponse, error) {
			gotReq = req
			return createMessageResponse("reply-msg-7", 1), nil
		},
	}

	msg := threadedMatrixMessage(spacePortal("space1"), "a reply", "topic1", "topic1")
	msg.ReplyTo = &database.Message{
		ID:       gcid.MakeMessageID("target-in-thread"),
		Metadata: &MessageMetadata{TimestampMicro: 777_000},
	}

	if _, err := gc.HandleMatrixMessage(context.Background(), msg); err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}
	if topicCalled {
		t.Error("createTopicFn was called for a threaded reply, want createMessageFn only (thread routing must stay governed by ThreadRoot)")
	}
	if got := gotReq.GetParentId().GetTopicId().GetTopicId(); got != "topic1" {
		t.Errorf("ParentId.TopicId.TopicId = %q, want %q (thread routing unaffected by ReplyTo)", got, "topic1")
	}

	replyTarget := gotReq.GetMessageInfo().GetReplyTo()
	if replyTarget == nil {
		t.Fatal("MessageInfo.ReplyTo is nil, want a SendReplyTarget")
	}
	if got := replyTarget.GetId().GetMessageId(); got != "target-in-thread" {
		t.Errorf("ReplyTo.Id.MessageId = %q, want %q", got, "target-in-thread")
	}
	if got := replyTarget.GetCreateTime(); got != 777_000 {
		t.Errorf("ReplyTo.CreateTime = %d, want %d", got, 777_000)
	}
	if got := replyTarget.GetId().GetParentId().GetTopicId().GetTopicId(); got != "topic1" {
		t.Errorf("ReplyTo.Id.ParentId.TopicId.TopicId = %q, want %q (thread_id truthy -> used verbatim, client.py:429, NOT the reply target's own id)", got, "topic1")
	}
}

// TestHandleMatrixMessageReplyMissingTimestampGraceful covers the Go-only
// defensive case Python never hits (its DBMessage.timestamp column is
// NOT NULL, mautrix_googlechat/db/message.py:40): a *database.Message handed
// in via msg.ReplyTo whose stored MessageMetadata.TimestampMicro is
// unavailable (zero, absent Metadata, or a Metadata value of an unexpected
// type -- e.g. a pre-Task-7 legacy row). Rather than send a malformed
// SendReplyTarget (id set, create_time missing/0, risking the WHOLE
// create_topic/create_message call being rejected server-side), the chosen
// graceful degradation is to log a warning and send the message with NO
// reply target at all -- it still lands as a normal message/thread post,
// just without the quote-reply decoration.
func TestHandleMatrixMessageReplyMissingTimestampGraceful(t *testing.T) {
	cases := []struct {
		name     string
		metadata any
	}{
		{"zero TimestampMicro", &MessageMetadata{TimestampMicro: 0}},
		{"nil Metadata", nil},
		{"wrong Metadata type", "not-a-MessageMetadata"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			login := newTestUserLogin(&UserLoginMetadata{})
			var gotReq *pb.CreateTopicRequest
			gc := &GChatClient{
				UserLogin:            login,
				addPendingToIgnoreFn: noopAddPendingToIgnore,
				createTopicFn: func(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
					gotReq = req
					return createTopicResponse("t", 1), nil
				},
			}

			msg := textMatrixMessage(spacePortal("space1"), "a reply")
			msg.ReplyTo = &database.Message{ID: gcid.MakeMessageID("target-no-ts"), Metadata: tc.metadata}

			if _, err := gc.HandleMatrixMessage(context.Background(), msg); err != nil {
				t.Fatalf("HandleMatrixMessage() error = %v, want nil (missing timestamp must degrade gracefully, not error)", err)
			}
			if gotReq.GetMessageInfo().GetReplyTo() != nil {
				t.Errorf("MessageInfo.ReplyTo = %v, want nil (no usable create_time -> send without a reply target)", gotReq.GetMessageInfo().GetReplyTo())
			}
		})
	}
}

// --- HandleMatrixMessage: outbound media (M5 Task 5) ------------------------
//
// Ports portal.py's _handle_matrix_media (portal.py:1081-1121) -- see
// media.go's own "Outbound (Matrix -> Google Chat) media" section doc
// comment for the full field-by-field port and the deliberate caption
// improvement over Python (Python drops any caption text outright,
// portal.py:1100).

// mediaMatrixMessage builds an outbound *bridgev2.MatrixMessage for one of
// the four msgtypes M5 Task 5 accepts. body/filename follow Matrix's own
// caption convention (MSC2530): filename == body (or filename == "") means
// "no caption, body IS the file name"; filename != body means "body is a
// genuine caption, filename is the file's real name" -- see
// hasOutboundCaption's doc comment (media.go).
func mediaMatrixMessage(portal *bridgev2.Portal, msgtype event.MessageType, body, filename, mimeType string) *bridgev2.MatrixMessage {
	return &bridgev2.MatrixMessage{
		MatrixEventBase: bridgev2.MatrixEventBase[*event.MessageEventContent]{
			Portal: portal,
			Content: &event.MessageEventContent{
				MsgType:  msgtype,
				Body:     body,
				FileName: filename,
				URL:      "mxc://example.com/abc",
				Info:     &event.FileInfo{MimeType: mimeType},
			},
		},
	}
}

func testUploadMetadata(contentName, contentType string) *pb.UploadMetadata {
	return &pb.UploadMetadata{
		ContentName: proto.String(contentName),
		ContentType: proto.String(contentType),
	}
}

// TestHandleMatrixMessageImageBuildsUploadAnnotation is the headline M5 Task
// 5 outbound behavior: an uncaptioned m.image (Body == FileName, the common
// case) downloads the Matrix file, uploads it, and attaches the result as
// an UPLOAD_METADATA/RENDER annotation on the create_topic request -- with
// NO duplicate text_body (Python never sends one either, portal.py:1113-1120
// has no text= argument at all).
func TestHandleMatrixMessageImageBuildsUploadAnnotation(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	wantMeta := testUploadMetadata("cat.png", "image/png")

	var gotDownloadURI id.ContentURIString
	var gotDownloadFile *event.EncryptedFileInfo
	var gotGroupID string
	var gotData []byte
	var gotFilename, gotMime string
	var gotReq *pb.CreateTopicRequest

	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		downloadMediaFn: func(_ context.Context, uri id.ContentURIString, file *event.EncryptedFileInfo) ([]byte, error) {
			gotDownloadURI = uri
			gotDownloadFile = file
			return []byte("fake png bytes"), nil
		},
		uploadFileFn: func(_ context.Context, groupID string, data []byte, filename, mimeType string) (*pb.UploadMetadata, error) {
			gotGroupID = groupID
			gotData = data
			gotFilename = filename
			gotMime = mimeType
			return wantMeta, nil
		},
		createTopicFn: func(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			gotReq = req
			return createTopicResponse("topic1", 1000), nil
		},
	}

	msg := mediaMatrixMessage(spacePortal("space1"), event.MsgImage, "cat.png", "cat.png", "image/png")
	_, err := gc.HandleMatrixMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}

	if gotDownloadURI != "mxc://example.com/abc" {
		t.Errorf("downloadMediaFn uri = %q, want %q", gotDownloadURI, "mxc://example.com/abc")
	}
	if gotDownloadFile != nil {
		t.Errorf("downloadMediaFn file = %v, want nil (unencrypted room)", gotDownloadFile)
	}
	if gotGroupID != "space1" {
		t.Errorf("uploadFileFn groupID = %q, want %q (plain, no space: prefix)", gotGroupID, "space1")
	}
	if string(gotData) != "fake png bytes" {
		t.Errorf("uploadFileFn data = %q, want the downloaded bytes", gotData)
	}
	if gotFilename != "cat.png" {
		t.Errorf("uploadFileFn filename = %q, want %q", gotFilename, "cat.png")
	}
	if gotMime != "image/png" {
		t.Errorf("uploadFileFn mimeType = %q, want %q", gotMime, "image/png")
	}

	if got := gotReq.GetTextBody(); got != "" {
		t.Errorf("TextBody = %q, want empty (no genuine caption, Body == FileName)", got)
	}
	anns := gotReq.GetAnnotations()
	if len(anns) != 1 {
		t.Fatalf("Annotations = %d, want exactly 1 (the UPLOAD_METADATA annotation)", len(anns))
	}
	if got := anns[0].GetType(); got != pb.AnnotationType_UPLOAD_METADATA {
		t.Errorf("Annotations[0].Type = %v, want UPLOAD_METADATA", got)
	}
	if got := anns[0].GetChipRenderType(); got != pb.Annotation_RENDER {
		t.Errorf("Annotations[0].ChipRenderType = %v, want RENDER", got)
	}
	if got := anns[0].GetUploadMetadata(); got != wantMeta {
		t.Errorf("Annotations[0].UploadMetadata = %v, want the uploaded metadata", got)
	}
}

// TestHandleMatrixMessageImageWithCaptionKeepsBothTextAndAnnotation covers
// the M3 B4 pattern the brief calls out: a genuine caption (FileName set,
// Body holding different text) must produce BOTH the UPLOAD_METADATA
// annotation AND a populated text_body -- unlike Python, which discards the
// caption text entirely (portal.py:1100 only ever uses it as the upload's
// reported file name).
func TestHandleMatrixMessageImageWithCaptionKeepsBothTextAndAnnotation(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	wantMeta := testUploadMetadata("vacation.jpg", "image/jpeg")

	var gotFilename string
	var gotReq *pb.CreateTopicRequest

	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		downloadMediaFn: func(context.Context, id.ContentURIString, *event.EncryptedFileInfo) ([]byte, error) {
			return []byte("fake jpeg bytes"), nil
		},
		uploadFileFn: func(_ context.Context, _ string, _ []byte, filename, _ string) (*pb.UploadMetadata, error) {
			gotFilename = filename
			return wantMeta, nil
		},
		createTopicFn: func(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			gotReq = req
			return createTopicResponse("topic1", 1000), nil
		},
	}

	// A genuine caption: FileName is the real file name, Body is different
	// (the caption text) -- the MSC2530 convention hasOutboundCaption keys
	// off of.
	msg := mediaMatrixMessage(spacePortal("space1"), event.MsgImage, "Look at this view!", "vacation.jpg", "image/jpeg")
	_, err := gc.HandleMatrixMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}

	// uploadFilename must prefer the real FileName, not the caption in Body
	// (unlike Python's portal.py:1100, which would upload it as "Look at
	// this view!").
	if gotFilename != "vacation.jpg" {
		t.Errorf("uploadFileFn filename = %q, want %q (the real FileName, not the caption)", gotFilename, "vacation.jpg")
	}

	if got := gotReq.GetTextBody(); got != "Look at this view!" {
		t.Errorf("TextBody = %q, want the caption text %q", got, "Look at this view!")
	}
	anns := gotReq.GetAnnotations()
	if len(anns) != 1 || anns[0].GetType() != pb.AnnotationType_UPLOAD_METADATA {
		t.Fatalf("Annotations = %s, want exactly [UPLOAD_METADATA] (plain caption has no formatting annotations of its own)", formatAnnotationsForTest(anns))
	}
}

// TestHandleMatrixMessageFormattedCaptionKeepsFileAndFormattingAnnotations
// proves the file annotation survives even when the caption ALSO carries
// its own HTML formatting -- the exact scenario B4 (docs/research/08d §2.4)
// describes megabridge silently losing the attached file over.
func TestHandleMatrixMessageFormattedCaptionKeepsFileAndFormattingAnnotations(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	wantMeta := testUploadMetadata("vacation.jpg", "image/jpeg")

	var gotReq *pb.CreateTopicRequest
	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		downloadMediaFn: func(context.Context, id.ContentURIString, *event.EncryptedFileInfo) ([]byte, error) {
			return []byte("fake jpeg bytes"), nil
		},
		uploadFileFn: func(context.Context, string, []byte, string, string) (*pb.UploadMetadata, error) {
			return wantMeta, nil
		},
		createTopicFn: func(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			gotReq = req
			return createTopicResponse("topic1", 1000), nil
		},
	}

	msg := mediaMatrixMessage(spacePortal("space1"), event.MsgImage, "a b c", "vacation.jpg", "image/jpeg")
	msg.Content.Format = event.FormatHTML
	msg.Content.FormattedBody = "a <strong>b</strong> c"

	_, err := gc.HandleMatrixMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}

	if got := gotReq.GetTextBody(); got != "a b c" {
		t.Errorf("TextBody = %q, want %q", got, "a b c")
	}
	anns := gotReq.GetAnnotations()
	if len(anns) != 2 {
		t.Fatalf("Annotations = %s, want 2 (file + formatting) -- B4 regression: the file annotation must survive a formatted caption", formatAnnotationsForTest(anns))
	}
	if got := anns[0].GetType(); got != pb.AnnotationType_UPLOAD_METADATA {
		t.Errorf("Annotations[0].Type = %v, want UPLOAD_METADATA (file annotation first)", got)
	}
	wantFormat := gchatfmt.MakeFormatAnnotation(2, 1, pb.FormatMetadata_BOLD)
	if len(anns) < 2 || anns[1].String() != wantFormat.String() {
		t.Errorf("Annotations[1] = %s, want the BOLD formatting annotation %s", formatAnnotationsForTest(anns), wantFormat.String())
	}
}

// TestHandleMatrixMessageUploadFileErrorIsCleanNoPartialSend is the #114
// contract: an UploadFile failure (the endpoint 500ing, or any other
// upload error) must propagate as a clean error from HandleMatrixMessage,
// with create_topic NEVER called -- no partial send, no silent drop.
func TestHandleMatrixMessageUploadFileErrorIsCleanNoPartialSend(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	wantErr := errors.New("gchatmeow: upload finalize request failed: unexpected status 500")
	createTopicCalled := false

	gc := &GChatClient{
		UserLogin: login,
		downloadMediaFn: func(context.Context, id.ContentURIString, *event.EncryptedFileInfo) ([]byte, error) {
			return []byte("fake png bytes"), nil
		},
		uploadFileFn: func(context.Context, string, []byte, string, string) (*pb.UploadMetadata, error) {
			return nil, wantErr
		},
		createTopicFn: func(context.Context, *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			createTopicCalled = true
			return createTopicResponse("t", 1), nil
		},
	}

	msg := mediaMatrixMessage(spacePortal("space1"), event.MsgImage, "cat.png", "cat.png", "image/png")
	_, err := gc.HandleMatrixMessage(context.Background(), msg)
	if err == nil {
		t.Fatal("HandleMatrixMessage() error = nil, want a clean error (the #114 upload failure)")
	}
	if !errors.Is(err, wantErr) {
		t.Errorf("error = %v, want wrapping %v", err, wantErr)
	}
	if createTopicCalled {
		t.Error("createTopicFn was called despite an UploadFile failure -- no partial send is allowed (#114 must not silently drop the file or send text-only)")
	}
}

// TestHandleMatrixMessageDownloadMediaErrorIsCleanNoPartialSend covers the
// other half of the same contract: a failure downloading the Matrix file in
// the first place (before any upload is even attempted) must also propagate
// cleanly, with neither UploadFile nor create_topic ever called.
func TestHandleMatrixMessageDownloadMediaErrorIsCleanNoPartialSend(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	wantErr := errors.New("homeserver: failed to download mxc://example.com/abc")
	uploadCalled := false
	createTopicCalled := false

	gc := &GChatClient{
		UserLogin: login,
		downloadMediaFn: func(context.Context, id.ContentURIString, *event.EncryptedFileInfo) ([]byte, error) {
			return nil, wantErr
		},
		uploadFileFn: func(context.Context, string, []byte, string, string) (*pb.UploadMetadata, error) {
			uploadCalled = true
			return testUploadMetadata("x", "image/png"), nil
		},
		createTopicFn: func(context.Context, *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			createTopicCalled = true
			return createTopicResponse("t", 1), nil
		},
	}

	msg := mediaMatrixMessage(spacePortal("space1"), event.MsgImage, "cat.png", "cat.png", "image/png")
	_, err := gc.HandleMatrixMessage(context.Background(), msg)
	if err == nil {
		t.Fatal("HandleMatrixMessage() error = nil, want a clean error")
	}
	if !errors.Is(err, wantErr) {
		t.Errorf("error = %v, want wrapping %v", err, wantErr)
	}
	if uploadCalled {
		t.Error("uploadFileFn was called despite a download failure")
	}
	if createTopicCalled {
		t.Error("createTopicFn was called despite a download failure -- no partial send is allowed")
	}
}

// TestHandleMatrixMessageEncryptedFileDownloadPath proves an encrypted
// room's msg.Content.File is threaded through to the download call
// unmodified -- bridgev2's own DownloadMedia (client.go's
// downloadMatrixMedia) handles the actual decryption when file != nil, so
// this connector's own job is just to pass it through rather than silently
// dropping it and downloading the (still-encrypted) URL directly.
func TestHandleMatrixMessageEncryptedFileDownloadPath(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	wantFile := &event.EncryptedFileInfo{URL: "mxc://example.com/encrypted-blob"}
	var gotFile *event.EncryptedFileInfo
	var gotURI id.ContentURIString

	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		downloadMediaFn: func(_ context.Context, uri id.ContentURIString, file *event.EncryptedFileInfo) ([]byte, error) {
			gotURI = uri
			gotFile = file
			return []byte("decrypted bytes"), nil
		},
		uploadFileFn: func(context.Context, string, []byte, string, string) (*pb.UploadMetadata, error) {
			return testUploadMetadata("secret.png", "image/png"), nil
		},
		createTopicFn: func(context.Context, *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			return createTopicResponse("t", 1), nil
		},
	}

	msg := mediaMatrixMessage(spacePortal("space1"), event.MsgImage, "secret.png", "secret.png", "image/png")
	msg.Content.URL = "" // encrypted rooms carry the ciphertext URL under File.URL, not Content.URL
	msg.Content.File = wantFile

	_, err := gc.HandleMatrixMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}
	if gotFile != wantFile {
		t.Errorf("downloadMediaFn file = %v, want the exact *event.EncryptedFileInfo from msg.Content.File", gotFile)
	}
	if gotURI != "" {
		t.Errorf("downloadMediaFn uri = %q, want empty (msg.Content.URL is unset for an encrypted upload; bridgev2's own DownloadMedia reads file.URL instead)", gotURI)
	}
}

// TestHandleMatrixMessageDisableOutboundMediaReturnsCleanError covers the
// config escape hatch (config.go's DisableOutboundMedia): when set, a media
// message must be rejected immediately with a clean, explicit error -- no
// download, no upload, no send attempt at all.
func TestHandleMatrixMessageDisableOutboundMediaReturnsCleanError(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	downloadCalled := false
	createTopicCalled := false

	gc := &GChatClient{
		UserLogin: login,
		Main:      &GChatConnector{Config: Config{DisableOutboundMedia: true}},
		downloadMediaFn: func(context.Context, id.ContentURIString, *event.EncryptedFileInfo) ([]byte, error) {
			downloadCalled = true
			return []byte("x"), nil
		},
		createTopicFn: func(context.Context, *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			createTopicCalled = true
			return createTopicResponse("t", 1), nil
		},
	}

	msg := mediaMatrixMessage(spacePortal("space1"), event.MsgImage, "cat.png", "cat.png", "image/png")
	_, err := gc.HandleMatrixMessage(context.Background(), msg)
	if !errors.Is(err, errOutboundMediaDisabled) {
		t.Errorf("error = %v, want errOutboundMediaDisabled", err)
	}
	if downloadCalled {
		t.Error("downloadMediaFn was called despite DisableOutboundMedia")
	}
	if createTopicCalled {
		t.Error("createTopicFn was called despite DisableOutboundMedia")
	}
}

// TestHandleMatrixMessageMediaGroupIDPlainForDMAndSpace pins that
// UploadFile's group_id parameter is always the PLAIN numeric id (Python's
// gcid_plain) for BOTH portal kinds, never gchatmeow.PartsToGroupID's oneof
// wire shape -- covering both DM and space portals, per the brief.
func TestHandleMatrixMessageMediaGroupIDPlainForDMAndSpace(t *testing.T) {
	cases := []struct {
		name   string
		portal *bridgev2.Portal
	}{
		{"space", spacePortal("space42")},
		{"dm", dmPortal("dm42")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			login := newTestUserLogin(&UserLoginMetadata{})
			var gotGroupID string
			gc := &GChatClient{
				UserLogin:            login,
				addPendingToIgnoreFn: noopAddPendingToIgnore,
				downloadMediaFn: func(context.Context, id.ContentURIString, *event.EncryptedFileInfo) ([]byte, error) {
					return []byte("x"), nil
				},
				uploadFileFn: func(_ context.Context, groupID string, _ []byte, _, _ string) (*pb.UploadMetadata, error) {
					gotGroupID = groupID
					return testUploadMetadata("x.png", "image/png"), nil
				},
				createTopicFn: func(context.Context, *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
					return createTopicResponse("t", 1), nil
				},
			}

			expectedID := "space42"
			if tc.name == "dm" {
				expectedID = "dm42"
			}
			msg := mediaMatrixMessage(tc.portal, event.MsgImage, "x.png", "x.png", "image/png")
			if _, err := gc.HandleMatrixMessage(context.Background(), msg); err != nil {
				t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
			}
			if gotGroupID != expectedID {
				t.Errorf("uploadFileFn groupID = %q, want %q", gotGroupID, expectedID)
			}
		})
	}
}

// TestHandleMatrixMessageMediaThreadRootRoutesToCreateMessage proves media
// composes with M3 Task 6's thread routing: a media message replying into
// an existing thread still uploads the file and attaches the annotation,
// routed through create_message (not create_topic).
func TestHandleMatrixMessageMediaThreadRootRoutesToCreateMessage(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	wantMeta := testUploadMetadata("clip.mp4", "video/mp4")
	var gotReq *pb.CreateMessageRequest
	createTopicCalled := false

	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		downloadMediaFn: func(context.Context, id.ContentURIString, *event.EncryptedFileInfo) ([]byte, error) {
			return []byte("fake video bytes"), nil
		},
		uploadFileFn: func(context.Context, string, []byte, string, string) (*pb.UploadMetadata, error) {
			return wantMeta, nil
		},
		createMessageFn: func(_ context.Context, req *pb.CreateMessageRequest) (*pb.CreateMessageResponse, error) {
			gotReq = req
			return createMessageResponse("msg1", 2000), nil
		},
		createTopicFn: func(context.Context, *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			createTopicCalled = true
			return createTopicResponse("t", 1), nil
		},
	}

	msg := threadedMatrixMessage(spacePortal("space1"), "", "root-msg", "root-topic")
	msg.Content = mediaMatrixMessage(spacePortal("space1"), event.MsgVideo, "clip.mp4", "clip.mp4", "video/mp4").Content

	if _, err := gc.HandleMatrixMessage(context.Background(), msg); err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}
	if createTopicCalled {
		t.Error("createTopicFn was called for a threaded media message, want create_message")
	}
	if gotReq == nil {
		t.Fatal("createMessageFn was not called")
	}
	anns := gotReq.GetAnnotations()
	if len(anns) != 1 || anns[0].GetType() != pb.AnnotationType_UPLOAD_METADATA {
		t.Fatalf("Annotations = %s, want exactly [UPLOAD_METADATA]", formatAnnotationsForTest(anns))
	}
	if got := anns[0].GetUploadMetadata(); got != wantMeta {
		t.Errorf("Annotations[0].UploadMetadata = %v, want %v", got, wantMeta)
	}
}

// TestHandleMatrixMessageMediaMsgTypesAccepted covers all four msgtypes the
// brief calls out (m.image/m.file/m.video/m.audio) -- each must build an
// UPLOAD_METADATA annotation, not be rejected as unsupported.
func TestHandleMatrixMessageMediaMsgTypesAccepted(t *testing.T) {
	for _, msgtype := range []event.MessageType{event.MsgImage, event.MsgFile, event.MsgVideo, event.MsgAudio} {
		t.Run(string(msgtype), func(t *testing.T) {
			login := newTestUserLogin(&UserLoginMetadata{})
			var gotReq *pb.CreateTopicRequest
			gc := &GChatClient{
				UserLogin:            login,
				addPendingToIgnoreFn: noopAddPendingToIgnore,
				downloadMediaFn: func(context.Context, id.ContentURIString, *event.EncryptedFileInfo) ([]byte, error) {
					return []byte("bytes"), nil
				},
				uploadFileFn: func(context.Context, string, []byte, string, string) (*pb.UploadMetadata, error) {
					return testUploadMetadata("f", "application/octet-stream"), nil
				},
				createTopicFn: func(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
					gotReq = req
					return createTopicResponse("t", 1), nil
				},
			}

			msg := mediaMatrixMessage(spacePortal("space1"), msgtype, "f", "f", "application/octet-stream")
			if _, err := gc.HandleMatrixMessage(context.Background(), msg); err != nil {
				t.Fatalf("HandleMatrixMessage() error = %v, want nil for msgtype %s", err, msgtype)
			}
			if len(gotReq.GetAnnotations()) != 1 {
				t.Errorf("Annotations = %d, want exactly 1 for msgtype %s", len(gotReq.GetAnnotations()), msgtype)
			}
		})
	}
}

// TestHandleMatrixMessageMediaNoConnNoSeamErrors mirrors
// TestHandleMatrixMessageNoConnNoSeamErrors for the media path: with no
// uploadFileFn override and no live gchatmeow.Client connection, UploadFile
// resolution must fail cleanly (not panic).
func TestHandleMatrixMessageMediaNoConnNoSeamErrors(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	gc := &GChatClient{
		UserLogin: login,
		downloadMediaFn: func(context.Context, id.ContentURIString, *event.EncryptedFileInfo) ([]byte, error) {
			return []byte("bytes"), nil
		},
	}

	msg := mediaMatrixMessage(spacePortal("space1"), event.MsgImage, "cat.png", "cat.png", "image/png")
	_, err := gc.HandleMatrixMessage(context.Background(), msg)
	if err == nil {
		t.Fatal("HandleMatrixMessage() error = nil, want an error when not connected")
	}
}

// TestHandleMatrixMessageMediaDefaultDownloadUsesPortalBridgeBot proves the
// production default (no downloadMediaFn override) actually reaches
// msg.Portal.Bridge.Bot.DownloadMedia -- downloadMatrixMedia's fallback
// wiring (client.go), exercised end-to-end via a fake bridgev2.MatrixAPI
// rather than the Fn seam every other test in this file uses.
func TestHandleMatrixMessageMediaDefaultDownloadUsesPortalBridgeBot(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	wantFile := &event.EncryptedFileInfo{URL: "mxc://example.com/ciphertext"}
	var gotURI id.ContentURIString
	var gotFile *event.EncryptedFileInfo

	portal := &bridgev2.Portal{
		Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: gcid.MakePortalID(gcid.GroupID{ID: "space1", IsDM: false})}},
		Bridge: &bridgev2.Bridge{Bot: fakeDownloadIntent{
			downloadFn: func(_ context.Context, uri id.ContentURIString, file *event.EncryptedFileInfo) ([]byte, error) {
				gotURI = uri
				gotFile = file
				return []byte("bytes"), nil
			},
		}},
	}

	gc := &GChatClient{
		UserLogin:            login,
		addPendingToIgnoreFn: noopAddPendingToIgnore,
		uploadFileFn: func(context.Context, string, []byte, string, string) (*pb.UploadMetadata, error) {
			return testUploadMetadata("f.png", "image/png"), nil
		},
		createTopicFn: func(context.Context, *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
			return createTopicResponse("t", 1), nil
		},
	}

	msg := mediaMatrixMessage(portal, event.MsgImage, "f.png", "f.png", "image/png")
	msg.Content.URL = ""
	msg.Content.File = wantFile

	if _, err := gc.HandleMatrixMessage(context.Background(), msg); err != nil {
		t.Fatalf("HandleMatrixMessage() error = %v, want nil", err)
	}
	if gotFile != wantFile {
		t.Errorf("Portal.Bridge.Bot.DownloadMedia file = %v, want %v", gotFile, wantFile)
	}
	if gotURI != "" {
		t.Errorf("Portal.Bridge.Bot.DownloadMedia uri = %q, want empty", gotURI)
	}
}

// fakeDownloadIntent implements bridgev2.MatrixAPI, overriding only
// DownloadMedia -- same "embed nil interface, override one method" pattern
// as media_test.go's fakeUploadIntent.
type fakeDownloadIntent struct {
	bridgev2.MatrixAPI
	downloadFn func(ctx context.Context, uri id.ContentURIString, file *event.EncryptedFileInfo) ([]byte, error)
}

func (f fakeDownloadIntent) DownloadMedia(ctx context.Context, uri id.ContentURIString, file *event.EncryptedFileInfo) ([]byte, error) {
	if f.downloadFn != nil {
		return f.downloadFn(ctx, uri, file)
	}
	return nil, errors.New("fakeDownloadIntent: downloadFn not set")
}
