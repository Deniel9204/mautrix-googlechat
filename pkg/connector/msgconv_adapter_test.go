package connector

// msgconv_adapter_test.go -- convertMessageToMatrix's own bookkeeping
// (MessageMetadata stamping, already covered implicitly by every M2 test
// that reaches it) plus, as of M3 Task 3, the content.Mentions ("m.mentions")
// wiring fix for B2 (mentions.go, msgconv_adapter.go).
import (
	"context"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv"
	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv/gchatfmt"
)

func TestConvertMessageToMatrix_SetsMentionsFromAnnotations(t *testing.T) {
	matrix := &fakeMatrixConnector{
		ghostIntent: func(gaiaID networkid.UserID) id.UserID {
			return id.UserID("@" + string(gaiaID) + "_ghost:example.com")
		},
	}
	portal := &bridgev2.Portal{
		Portal: &database.Portal{},
		Bridge: &bridgev2.Bridge{Matrix: matrix},
	}

	msg := &pb.Message{
		TextBody:    proto.String("hi @Bob"),
		CreateTime:  proto.Int64(1234),
		Annotations: []*pb.Annotation{gchatfmt.MakeMentionAnnotation(3, 4, "200")},
	}

	convert := convertMessageToMatrix(msgconv.New())
	cm, err := convert(context.Background(), portal, nil, msg)
	if err != nil {
		t.Fatalf("convertMessageToMatrix returned error: %v", err)
	}
	if len(cm.Parts) != 1 {
		t.Fatalf("len(cm.Parts) = %d, want 1", len(cm.Parts))
	}

	part := cm.Parts[0]
	meta, ok := part.DBMetadata.(*MessageMetadata)
	if !ok || meta.TimestampMicro != 1234 {
		t.Errorf("DBMetadata = %+v, want TimestampMicro=1234", part.DBMetadata)
	}

	if part.Content.Mentions == nil || !part.Content.Mentions.Has("@200_ghost:example.com") {
		t.Errorf("content.Mentions = %+v, want to include @200_ghost:example.com (fix B2)", part.Content.Mentions)
	}
}

func TestConvertMessageToMatrix_NoAnnotationsLeavesMentionsNil(t *testing.T) {
	portal := &bridgev2.Portal{
		Portal: &database.Portal{},
		Bridge: &bridgev2.Bridge{Matrix: &fakeMatrixConnector{}},
	}

	msg := &pb.Message{
		TextBody:   proto.String("hello, no mentions here"),
		CreateTime: proto.Int64(5678),
	}

	convert := convertMessageToMatrix(msgconv.New())
	cm, err := convert(context.Background(), portal, nil, msg)
	if err != nil {
		t.Fatalf("convertMessageToMatrix returned error: %v", err)
	}
	if len(cm.Parts) != 1 {
		t.Fatalf("len(cm.Parts) = %d, want 1", len(cm.Parts))
	}
	if cm.Parts[0].Content.Mentions != nil {
		t.Errorf("content.Mentions = %+v, want nil for a message with no mention annotations", cm.Parts[0].Content.Mentions)
	}
}

func TestConvertMessageToMatrix_NilPortalDoesNotPanic(t *testing.T) {
	msg := &pb.Message{
		TextBody:   proto.String("hi"),
		CreateTime: proto.Int64(1),
	}
	convert := convertMessageToMatrix(msgconv.New())
	if _, err := convert(context.Background(), nil, nil, msg); err != nil {
		t.Fatalf("convertMessageToMatrix returned error: %v", err)
	}
}

// ghostPortal builds a portal whose bridge resolves a gaia id to a
// deterministic ghost MXID, so convertMessageToMatrix's real
// newInboundMentionResolver produces a resolvable pill.
func ghostPortal() *bridgev2.Portal {
	matrix := &fakeMatrixConnector{
		ghostIntent: func(gaiaID networkid.UserID) id.UserID {
			return id.UserID("@" + string(gaiaID) + "_ghost:example.com")
		},
	}
	return &bridgev2.Portal{
		Portal: &database.Portal{},
		Bridge: &bridgev2.Bridge{Matrix: matrix},
	}
}

// TestConvertMessageToMatrix_MalformedMentionNoPhantomPing is the headline
// phantom-ping regression at the connector level: a message whose ONLY
// mention annotation is out of bounds (no corresponding body text) must
// leave content.Mentions unset -- the removed independent inboundMentions
// walk would have pinged the user here (no bounds gate); deriving from
// gchatfmt's ParsedMentions does not.
func TestConvertMessageToMatrix_MalformedMentionNoPhantomPing(t *testing.T) {
	msg := &pb.Message{
		TextBody:   proto.String("hi"), // 2 UTF-16 units
		CreateTime: proto.Int64(1),
		// Mention claims [0,50): out of bounds, resolves fine but renders no pill.
		Annotations: []*pb.Annotation{gchatfmt.MakeMentionAnnotation(0, 50, "200")},
	}

	convert := convertMessageToMatrix(msgconv.New())
	cm, err := convert(context.Background(), ghostPortal(), nil, msg)
	if err != nil {
		t.Fatalf("convertMessageToMatrix returned error: %v", err)
	}
	if len(cm.Parts) != 1 {
		t.Fatalf("len(cm.Parts) = %d, want 1", len(cm.Parts))
	}
	if cm.Parts[0].Content.Mentions != nil {
		t.Errorf("content.Mentions = %+v, want nil -- a malformed mention with no body text must not ping", cm.Parts[0].Content.Mentions)
	}
}

// TestConvertMessageToMatrix_MentionAllSetsRoom: a valid MENTION_ALL sets
// content.Mentions.Room.
func TestConvertMessageToMatrix_MentionAllSetsRoom(t *testing.T) {
	msg := &pb.Message{
		TextBody:    proto.String("@all hi"),
		CreateTime:  proto.Int64(1),
		Annotations: []*pb.Annotation{gchatfmt.MakeMentionAllAnnotation(0, 4)},
	}

	convert := convertMessageToMatrix(msgconv.New())
	cm, err := convert(context.Background(), ghostPortal(), nil, msg)
	if err != nil {
		t.Fatalf("convertMessageToMatrix returned error: %v", err)
	}
	if cm.Parts[0].Content.Mentions == nil || !cm.Parts[0].Content.Mentions.Room {
		t.Errorf("content.Mentions = %+v, want Room=true", cm.Parts[0].Content.Mentions)
	}
}

// TestConvertMessageToMatrix_ValidMentionPingsDespiteMalformedFormatAnnotation
// pins the subtle case at the connector level: a valid mention alongside an
// unrelated malformed FORMAT annotation. The whole message falls back to
// plain text (no HTML), but the plain body still shows "@Bob", so content.
// Mentions still pings the user AND the body still contains the @name text.
func TestConvertMessageToMatrix_ValidMentionPingsDespiteMalformedFormatAnnotation(t *testing.T) {
	msg := &pb.Message{
		TextBody:   proto.String("hi @Bob"), // "@Bob" is [3,7)
		CreateTime: proto.Int64(1),
		Annotations: []*pb.Annotation{
			gchatfmt.MakeFormatAnnotation(0, 2147483647, pb.FormatMetadata_BOLD),
			gchatfmt.MakeMentionAnnotation(3, 4, "200"),
		},
	}

	convert := convertMessageToMatrix(msgconv.New())
	cm, err := convert(context.Background(), ghostPortal(), nil, msg)
	if err != nil {
		t.Fatalf("convertMessageToMatrix returned error: %v", err)
	}
	part := cm.Parts[0]
	if part.Content.Format != "" || part.Content.FormattedBody != "" {
		t.Errorf("content = {Format:%q FormattedBody:%q}, want plain (malformed FORMAT annotation forces fallback)", part.Content.Format, part.Content.FormattedBody)
	}
	if part.Content.Body != "hi @Bob" {
		t.Errorf("content.Body = %q, want %q (the @name text survives in the plain body)", part.Content.Body, "hi @Bob")
	}
	if part.Content.Mentions == nil || !part.Content.Mentions.Has("@200_ghost:example.com") {
		t.Errorf("content.Mentions = %+v, want to still include @200_ghost:example.com", part.Content.Mentions)
	}
}

// --- ThreadRoot / topic_id stamping (M3 Task 6) -----------------------------

// topicMsg builds a *pb.Message carrying an Id/ParentId.TopicId, mirroring
// pkg/msgconv/from-gchat_test.go's topicMessage helper (kept separate since
// that one lives in msgconv_test and is unexported).
func topicMsg(messageID, topicID, text string) *pb.Message {
	msg := &pb.Message{
		TextBody:   proto.String(text),
		CreateTime: proto.Int64(1),
		Id:         &pb.MessageId{MessageId: proto.String(messageID)},
	}
	if topicID != "" {
		msg.Id.ParentId = &pb.MessageParentId{
			Parent: &pb.MessageParentId_TopicId{
				TopicId: &pb.TopicId{TopicId: proto.String(topicID)},
			},
		}
	}
	return msg
}

func flatPortal() *bridgev2.Portal {
	return &bridgev2.Portal{
		Portal: &database.Portal{Metadata: &PortalMetadata{}},
		Bridge: &bridgev2.Bridge{Matrix: &fakeMatrixConnector{}},
	}
}

func threadsOnlyPortal() *bridgev2.Portal {
	return &bridgev2.Portal{
		Portal: &database.Portal{Metadata: &PortalMetadata{ThreadsOnly: true}},
		Bridge: &bridgev2.Bridge{Matrix: &fakeMatrixConnector{}},
	}
}

// TestConvertMessageToMatrix_StampsTopicIDMetadata pins the STORE half of
// the task: MessageMetadata.TopicID must be stamped from
// msg.id.parent_id.topic_id.topic_id on every converted part, regardless of
// whether this particular message ends up with a ThreadRoot set.
func TestConvertMessageToMatrix_StampsTopicIDMetadata(t *testing.T) {
	msg := topicMsg("reply1", "topic1", "a reply")

	convert := convertMessageToMatrix(msgconv.New())
	cm, err := convert(context.Background(), flatPortal(), nil, msg)
	if err != nil {
		t.Fatalf("convertMessageToMatrix returned error: %v", err)
	}
	if len(cm.Parts) != 1 {
		t.Fatalf("len(cm.Parts) = %d, want 1", len(cm.Parts))
	}
	meta, ok := cm.Parts[0].DBMetadata.(*MessageMetadata)
	if !ok {
		t.Fatalf("DBMetadata type = %T, want *MessageMetadata", cm.Parts[0].DBMetadata)
	}
	if meta.TopicID != "topic1" {
		t.Errorf("Metadata.TopicID = %q, want %q", meta.TopicID, "topic1")
	}
}

// TestConvertMessageToMatrix_ReplyMessageSetsThreadRootInFlatPortal: a
// genuine reply (message_id != topic_id) must get ThreadRoot set even in a
// non-threads-only portal -- the "message_id != topic_id" rule is
// unconditional (see ToMatrix's own doc comment, pkg/msgconv/from-gchat.go).
func TestConvertMessageToMatrix_ReplyMessageSetsThreadRootInFlatPortal(t *testing.T) {
	msg := topicMsg("reply1", "topic1", "a reply")

	convert := convertMessageToMatrix(msgconv.New())
	cm, err := convert(context.Background(), flatPortal(), nil, msg)
	if err != nil {
		t.Fatalf("convertMessageToMatrix returned error: %v", err)
	}
	if cm.ThreadRoot == nil || string(*cm.ThreadRoot) != "topic1" {
		t.Errorf("ThreadRoot = %v, want %q", cm.ThreadRoot, "topic1")
	}
}

// TestConvertMessageToMatrix_HeadMessageFlatPortalNoThreadRoot: the head of
// a brand new topic (message_id == topic_id) in a non-threads-only portal
// must NOT get a self-referencing ThreadRoot.
func TestConvertMessageToMatrix_HeadMessageFlatPortalNoThreadRoot(t *testing.T) {
	msg := topicMsg("topic1", "topic1", "hello")

	convert := convertMessageToMatrix(msgconv.New())
	cm, err := convert(context.Background(), flatPortal(), nil, msg)
	if err != nil {
		t.Fatalf("convertMessageToMatrix returned error: %v", err)
	}
	if cm.ThreadRoot != nil {
		t.Errorf("ThreadRoot = %v, want nil for a head message in a flat portal", *cm.ThreadRoot)
	}
}

// TestConvertMessageToMatrix_HeadMessageThreadsOnlyPortalSelfThreadRoot: the
// same head message in a PortalMetadata.ThreadsOnly portal DOES get a
// self-referencing ThreadRoot, so a later Matrix reply to it auto-converts
// into a Google Chat thread (bridgev2 portal.go:1259-1268).
func TestConvertMessageToMatrix_HeadMessageThreadsOnlyPortalSelfThreadRoot(t *testing.T) {
	msg := topicMsg("topic1", "topic1", "hello")

	convert := convertMessageToMatrix(msgconv.New())
	cm, err := convert(context.Background(), threadsOnlyPortal(), nil, msg)
	if err != nil {
		t.Fatalf("convertMessageToMatrix returned error: %v", err)
	}
	if cm.ThreadRoot == nil || string(*cm.ThreadRoot) != "topic1" {
		t.Errorf("ThreadRoot = %v, want self-reference %q in a threads-only portal", cm.ThreadRoot, "topic1")
	}
}

// TestConvertMessageToMatrix_NilPortalMetadataTreatedAsFlat: a portal whose
// Metadata isn't yet a *PortalMetadata (e.g. a brand new portal before its
// first chat_info sync) must be treated as flat/non-threads-only, matching
// roomFeatures' own nil-safe default (capabilities.go).
// --- convertEditToMatrix (M4 Task 1: inbound MESSAGE_UPDATED) ---------------
//
// Ports handle_googlechat_edit's dedup + re-conversion (portal.py:1228-1260).

// editMsg builds a *pb.Message shaped like a MESSAGE_UPDATED body's payload:
// the same message id as the original (edits never change the id), a new
// text_body, and last_edit_time/last_update_time for the dedup gate.
func editMsg(messageID, topicID, text string, lastEditTime, lastUpdateTime int64) *pb.Message {
	msg := topicMsg(messageID, topicID, text)
	if lastEditTime != 0 {
		msg.LastEditTime = proto.Int64(lastEditTime)
	}
	if lastUpdateTime != 0 {
		msg.LastUpdateTime = proto.Int64(lastUpdateTime)
	}
	return msg
}

func TestConvertEditToMatrix_ReconvertsBody(t *testing.T) {
	existing := []*database.Message{{
		ID:       gcid.MakeMessageID("msg1"),
		Metadata: &MessageMetadata{TimestampMicro: 111, TopicID: "msg1", LastEditTime: 0},
	}}
	msg := editMsg("msg1", "msg1", "edited text", 5000, 0)

	convert := convertEditToMatrix(msgconv.New())
	converted, err := convert(context.Background(), flatPortal(), nil, existing, msg)
	if err != nil {
		t.Fatalf("convertEditToMatrix returned error: %v", err)
	}
	if len(converted.ModifiedParts) != 1 {
		t.Fatalf("len(ModifiedParts) = %d, want 1", len(converted.ModifiedParts))
	}
	part := converted.ModifiedParts[0]
	if part.Part != existing[0] {
		t.Error("ModifiedParts[0].Part is not the existing[0] pointer -- only part 0 must ever be touched")
	}
	if got := part.Content.Body; got != "edited text" {
		t.Errorf("Content.Body = %q, want %q", got, "edited text")
	}
}

// TestConvertEditToMatrix_DedupSkipsStaleEdit pins portal.py:1238-1240's
// `if self._edit_dedup[msg_id] >= edit_ts: ... return` -- an edit whose
// last_edit_time is EQUAL to the stored value is a duplicate and must be
// ignored via bridgev2.ErrIgnoringRemoteEvent, leaving the stored metadata
// untouched.
func TestConvertEditToMatrix_DedupSkipsStaleEdit(t *testing.T) {
	existing := []*database.Message{{
		ID:       gcid.MakeMessageID("msg1"),
		Metadata: &MessageMetadata{LastEditTime: 5000},
	}}
	msg := editMsg("msg1", "msg1", "duplicate edit", 5000, 0)

	convert := convertEditToMatrix(msgconv.New())
	_, err := convert(context.Background(), flatPortal(), nil, existing, msg)
	if !errors.Is(err, bridgev2.ErrIgnoringRemoteEvent) {
		t.Errorf("error = %v, want wrapping bridgev2.ErrIgnoringRemoteEvent", err)
	}
	if existing[0].Metadata.(*MessageMetadata).LastEditTime != 5000 {
		t.Errorf("LastEditTime = %d, want unchanged 5000 after a duplicate edit", existing[0].Metadata.(*MessageMetadata).LastEditTime)
	}
}

// TestConvertEditToMatrix_DedupSkipsOlderEdit: an edit_ts strictly OLDER
// than the stored value (an out-of-order redelivery) must also be ignored.
func TestConvertEditToMatrix_DedupSkipsOlderEdit(t *testing.T) {
	existing := []*database.Message{{
		ID:       gcid.MakeMessageID("msg1"),
		Metadata: &MessageMetadata{LastEditTime: 5000},
	}}
	msg := editMsg("msg1", "msg1", "stale edit", 3000, 0)

	convert := convertEditToMatrix(msgconv.New())
	_, err := convert(context.Background(), flatPortal(), nil, existing, msg)
	if !errors.Is(err, bridgev2.ErrIgnoringRemoteEvent) {
		t.Errorf("error = %v, want wrapping bridgev2.ErrIgnoringRemoteEvent", err)
	}
}

// TestConvertEditToMatrix_AppliesNewerEditAndUpdatesLastEditTime is the
// converse: a genuinely newer edit_ts must apply AND bump the stored
// LastEditTime so a later duplicate of THIS edit dedups correctly.
func TestConvertEditToMatrix_AppliesNewerEditAndUpdatesLastEditTime(t *testing.T) {
	existing := []*database.Message{{
		ID:       gcid.MakeMessageID("msg1"),
		Metadata: &MessageMetadata{TimestampMicro: 111, TopicID: "msg1", LastEditTime: 3000},
	}}
	msg := editMsg("msg1", "msg1", "newer edit", 5000, 0)

	convert := convertEditToMatrix(msgconv.New())
	converted, err := convert(context.Background(), flatPortal(), nil, existing, msg)
	if err != nil {
		t.Fatalf("convertEditToMatrix returned error: %v", err)
	}
	if len(converted.ModifiedParts) != 1 {
		t.Fatalf("len(ModifiedParts) = %d, want 1", len(converted.ModifiedParts))
	}
	meta := existing[0].Metadata.(*MessageMetadata)
	if meta.LastEditTime != 5000 {
		t.Errorf("LastEditTime = %d, want 5000", meta.LastEditTime)
	}
	// TimestampMicro/TopicID must survive untouched -- an edit never
	// changes a message's original create_time or the topic it belongs to.
	if meta.TimestampMicro != 111 {
		t.Errorf("TimestampMicro = %d, want unchanged 111", meta.TimestampMicro)
	}
	if meta.TopicID != "msg1" {
		t.Errorf("TopicID = %q, want unchanged %q", meta.TopicID, "msg1")
	}
}

// TestConvertEditToMatrix_FallsBackToLastUpdateTimeWhenLastEditTimeUnset
// pins portal.py:1236's `edit_ts = evt.last_edit_time or evt.last_update_time`
// fallback.
func TestConvertEditToMatrix_FallsBackToLastUpdateTimeWhenLastEditTimeUnset(t *testing.T) {
	existing := []*database.Message{{
		ID:       gcid.MakeMessageID("msg1"),
		Metadata: &MessageMetadata{LastEditTime: 3000},
	}}
	msg := editMsg("msg1", "msg1", "no last_edit_time", 0, 7000)

	convert := convertEditToMatrix(msgconv.New())
	converted, err := convert(context.Background(), flatPortal(), nil, existing, msg)
	if err != nil {
		t.Fatalf("convertEditToMatrix returned error: %v", err)
	}
	if len(converted.ModifiedParts) != 1 {
		t.Fatalf("len(ModifiedParts) = %d, want 1", len(converted.ModifiedParts))
	}
	if got := existing[0].Metadata.(*MessageMetadata).LastEditTime; got != 7000 {
		t.Errorf("LastEditTime = %d, want 7000 (fell back to last_update_time)", got)
	}
}

// TestConvertEditToMatrix_EmptyTextBodyIgnored pins portal.py:1248-1251's
// `elif target.msgtype != "m.text" or not evt.text_body: ... return` --  an
// edit with no text_body at all (msgconv.ToMatrix's empty-text early return,
// from-gchat.go) must be dropped via ErrIgnoringRemoteEvent, not applied as
// an empty-body edit.
func TestConvertEditToMatrix_EmptyTextBodyIgnored(t *testing.T) {
	existing := []*database.Message{{
		ID:       gcid.MakeMessageID("msg1"),
		Metadata: &MessageMetadata{LastEditTime: 0},
	}}
	msg := editMsg("msg1", "msg1", "", 5000, 0)

	convert := convertEditToMatrix(msgconv.New())
	_, err := convert(context.Background(), flatPortal(), nil, existing, msg)
	if !errors.Is(err, bridgev2.ErrIgnoringRemoteEvent) {
		t.Errorf("error = %v, want wrapping bridgev2.ErrIgnoringRemoteEvent", err)
	}
}

// TestConvertEditToMatrix_NoExistingMetadataStillWorks covers a target with
// no prior MessageMetadata at all: LastEditTime dedup must treat that as "no
// edit ever applied" (0), apply the edit, and create a fresh *MessageMetadata
// rather than panicking on the type assertion.
func TestConvertEditToMatrix_NoExistingMetadataStillWorks(t *testing.T) {
	existing := []*database.Message{{ID: gcid.MakeMessageID("msg1")}} // Metadata is nil
	msg := editMsg("msg1", "msg1", "first edit", 5000, 0)

	convert := convertEditToMatrix(msgconv.New())
	converted, err := convert(context.Background(), flatPortal(), nil, existing, msg)
	if err != nil {
		t.Fatalf("convertEditToMatrix returned error: %v", err)
	}
	if len(converted.ModifiedParts) != 1 {
		t.Fatalf("len(ModifiedParts) = %d, want 1", len(converted.ModifiedParts))
	}
	meta, ok := existing[0].Metadata.(*MessageMetadata)
	if !ok {
		t.Fatalf("Metadata type = %T, want *MessageMetadata", existing[0].Metadata)
	}
	if meta.LastEditTime != 5000 {
		t.Errorf("LastEditTime = %d, want 5000", meta.LastEditTime)
	}
}

// TestConvertEditToMatrix_FormattingRoundTrips proves an edit's new
// annotations still drive gchatfmt.Parse (M3 composition: formatting works
// on edits too, both directions).
func TestConvertEditToMatrix_FormattingRoundTrips(t *testing.T) {
	existing := []*database.Message{{
		ID:       gcid.MakeMessageID("msg1"),
		Metadata: &MessageMetadata{LastEditTime: 0},
	}}
	msg := editMsg("msg1", "msg1", "a b c", 5000, 0)
	msg.Annotations = []*pb.Annotation{gchatfmt.MakeFormatAnnotation(2, 1, pb.FormatMetadata_BOLD)}

	convert := convertEditToMatrix(msgconv.New())
	converted, err := convert(context.Background(), flatPortal(), nil, existing, msg)
	if err != nil {
		t.Fatalf("convertEditToMatrix returned error: %v", err)
	}
	content := converted.ModifiedParts[0].Content
	if content.Format != event.FormatHTML || content.FormattedBody != "a <strong>b</strong> c" {
		t.Errorf("Content = {Format:%q FormattedBody:%q}, want HTML with bold %q", content.Format, content.FormattedBody, "b")
	}
}

func TestConvertMessageToMatrix_NilPortalMetadataTreatedAsFlat(t *testing.T) {
	portal := &bridgev2.Portal{
		Portal: &database.Portal{},
		Bridge: &bridgev2.Bridge{Matrix: &fakeMatrixConnector{}},
	}
	msg := topicMsg("topic1", "topic1", "hello")

	convert := convertMessageToMatrix(msgconv.New())
	cm, err := convert(context.Background(), portal, nil, msg)
	if err != nil {
		t.Fatalf("convertMessageToMatrix returned error: %v", err)
	}
	if cm.ThreadRoot != nil {
		t.Errorf("ThreadRoot = %v, want nil when PortalMetadata is absent", *cm.ThreadRoot)
	}
}
