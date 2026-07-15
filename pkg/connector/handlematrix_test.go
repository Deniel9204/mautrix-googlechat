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
		t.Error("MessageInfo.ReplyTo is set, want nil (M2 has no quote-reply support yet)")
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

	msg := textMatrixMessage(spacePortal("space1"), "an image")
	msg.Content.MsgType = event.MsgImage

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
