package connector

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

	convert := convertMessageToMatrix(msgconv.New(), nil)
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

	convert := convertMessageToMatrix(msgconv.New(), nil)
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
	convert := convertMessageToMatrix(msgconv.New(), nil)
	if _, err := convert(context.Background(), nil, nil, msg); err != nil {
		t.Fatalf("convertMessageToMatrix returned error: %v", err)
	}
}

// ghostPortal builds a portal whose bridge resolves gaia ids to ghost MXIDs.
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

// TestConvertMessageToMatrix_MalformedMentionNoPhantomPing: an out-of-bounds
// mention must leave content.Mentions unset.
func TestConvertMessageToMatrix_MalformedMentionNoPhantomPing(t *testing.T) {
	msg := &pb.Message{
		TextBody:   proto.String("hi"), // 2 UTF-16 units
		CreateTime: proto.Int64(1),
		// Out of bounds: resolves, but renders no pill.
		Annotations: []*pb.Annotation{gchatfmt.MakeMentionAnnotation(0, 50, "200")},
	}

	convert := convertMessageToMatrix(msgconv.New(), nil)
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

func TestConvertMessageToMatrix_MentionAllSetsRoom(t *testing.T) {
	msg := &pb.Message{
		TextBody:    proto.String("@all hi"),
		CreateTime:  proto.Int64(1),
		Annotations: []*pb.Annotation{gchatfmt.MakeMentionAllAnnotation(0, 4)},
	}

	convert := convertMessageToMatrix(msgconv.New(), nil)
	cm, err := convert(context.Background(), ghostPortal(), nil, msg)
	if err != nil {
		t.Fatalf("convertMessageToMatrix returned error: %v", err)
	}
	if cm.Parts[0].Content.Mentions == nil || !cm.Parts[0].Content.Mentions.Room {
		t.Errorf("content.Mentions = %+v, want Room=true", cm.Parts[0].Content.Mentions)
	}
}

// TestConvertMessageToMatrix_ValidMentionPingsDespiteMalformedFormatAnnotation:
// the body falls back to plain text but still contains "@Bob", so Bob is pinged.
func TestConvertMessageToMatrix_ValidMentionPingsDespiteMalformedFormatAnnotation(t *testing.T) {
	msg := &pb.Message{
		TextBody:   proto.String("hi @Bob"), // "@Bob" is [3,7)
		CreateTime: proto.Int64(1),
		Annotations: []*pb.Annotation{
			gchatfmt.MakeFormatAnnotation(0, 2147483647, pb.FormatMetadata_BOLD),
			gchatfmt.MakeMentionAnnotation(3, 4, "200"),
		},
	}

	convert := convertMessageToMatrix(msgconv.New(), nil)
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

// TestConvertMessageToMatrix_StampsTopicIDMetadata: TopicID is stamped on every
// part, whether or not ThreadRoot is set.
func TestConvertMessageToMatrix_StampsTopicIDMetadata(t *testing.T) {
	msg := topicMsg("reply1", "topic1", "a reply")

	convert := convertMessageToMatrix(msgconv.New(), nil)
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

// TestConvertMessageToMatrix_ReplyMessageSetsThreadRootInFlatPortal: message_id
// != topic_id always sets ThreadRoot.
func TestConvertMessageToMatrix_ReplyMessageSetsThreadRootInFlatPortal(t *testing.T) {
	msg := topicMsg("reply1", "topic1", "a reply")

	convert := convertMessageToMatrix(msgconv.New(), nil)
	cm, err := convert(context.Background(), flatPortal(), nil, msg)
	if err != nil {
		t.Fatalf("convertMessageToMatrix returned error: %v", err)
	}
	if cm.ThreadRoot == nil || string(*cm.ThreadRoot) != "topic1" {
		t.Errorf("ThreadRoot = %v, want %q", cm.ThreadRoot, "topic1")
	}
}

func TestConvertMessageToMatrix_HeadMessageFlatPortalNoThreadRoot(t *testing.T) {
	msg := topicMsg("topic1", "topic1", "hello")

	convert := convertMessageToMatrix(msgconv.New(), nil)
	cm, err := convert(context.Background(), flatPortal(), nil, msg)
	if err != nil {
		t.Fatalf("convertMessageToMatrix returned error: %v", err)
	}
	if cm.ThreadRoot != nil {
		t.Errorf("ThreadRoot = %v, want nil for a head message in a flat portal", *cm.ThreadRoot)
	}
}

// TestConvertMessageToMatrix_HeadMessageThreadsOnlyPortalSelfThreadRoot: a
// self-referencing ThreadRoot makes Matrix replies become GC threads.
func TestConvertMessageToMatrix_HeadMessageThreadsOnlyPortalSelfThreadRoot(t *testing.T) {
	msg := topicMsg("topic1", "topic1", "hello")

	convert := convertMessageToMatrix(msgconv.New(), nil)
	cm, err := convert(context.Background(), threadsOnlyPortal(), nil, msg)
	if err != nil {
		t.Fatalf("convertMessageToMatrix returned error: %v", err)
	}
	if cm.ThreadRoot == nil || string(*cm.ThreadRoot) != "topic1" {
		t.Errorf("ThreadRoot = %v, want self-reference %q in a threads-only portal", cm.ThreadRoot, "topic1")
	}
}

// editMsg builds a MESSAGE_UPDATED payload with edit timestamps for the dedup gate.
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
	textPart := &database.Message{
		ID:       gcid.MakeMessageID("msg1"),
		PartID:   gcid.TextPartID,
		Metadata: &MessageMetadata{TimestampMicro: 111, TopicID: "msg1", LastEditTime: 0},
	}
	existing := []*database.Message{textPart}
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

	if part.Part != textPart {
		t.Error("ModifiedParts[0].Part is not the text part -- the edit must target the \"\"-PartID part")
	}
	if got := part.Content.Body; got != "edited text" {
		t.Errorf("Content.Body = %q, want %q", got, "edited text")
	}
}

// TestConvertEditToMatrix_AttachmentOnlyMessageIgnored: an edit adding text to an
// attachment-only message must not m.replace the image.
func TestConvertEditToMatrix_AttachmentOnlyMessageIgnored(t *testing.T) {
	attPart := &database.Message{
		ID:       gcid.MakeMessageID("msg1"),
		PartID:   gcid.MakeAttachmentPartID(0),
		Metadata: &MessageMetadata{LastEditTime: 0},
	}
	existing := []*database.Message{attPart}

	msg := editMsg("msg1", "msg1", "a link https://drive.google.com/x", 5000, 0)

	convert := convertEditToMatrix(msgconv.New())
	_, err := convert(context.Background(), flatPortal(), nil, existing, msg)
	if !errors.Is(err, bridgev2.ErrIgnoringRemoteEvent) {
		t.Errorf("error = %v, want wrapping bridgev2.ErrIgnoringRemoteEvent (attachment-only edits must be ignored, not applied to the image part)", err)
	}

	if attPart.Metadata.(*MessageMetadata).LastEditTime != 0 {
		t.Errorf("att_0 LastEditTime = %d, want unchanged 0 (the image part must never be touched by an edit)", attPart.Metadata.(*MessageMetadata).LastEditTime)
	}
}

// TestConvertEditToMatrix_TargetsTextPartNotFirstPart: the edit targets the ""
// part by PartID even when att_0 comes first (no ORDER BY on Postgres).
func TestConvertEditToMatrix_TargetsTextPartNotFirstPart(t *testing.T) {
	attPart := &database.Message{
		ID:       gcid.MakeMessageID("msg1"),
		PartID:   gcid.MakeAttachmentPartID(0),
		Metadata: &MessageMetadata{LastEditTime: 0},
	}
	textPart := &database.Message{
		ID:       gcid.MakeMessageID("msg1"),
		PartID:   gcid.TextPartID,
		Metadata: &MessageMetadata{TimestampMicro: 111, TopicID: "msg1", LastEditTime: 3000},
	}
	// att_0 first: the Postgres-after-first-edit order.
	existing := []*database.Message{attPart, textPart}
	msg := editMsg("msg1", "msg1", "edited caption", 5000, 0)

	convert := convertEditToMatrix(msgconv.New())
	converted, err := convert(context.Background(), flatPortal(), nil, existing, msg)
	if err != nil {
		t.Fatalf("convertEditToMatrix returned error: %v", err)
	}
	if len(converted.ModifiedParts) != 1 {
		t.Fatalf("len(ModifiedParts) = %d, want 1", len(converted.ModifiedParts))
	}
	if converted.ModifiedParts[0].Part != textPart {
		t.Error("edit did not target the \"\" text part -- it must be found by PartID, not slice position")
	}
	if got := converted.ModifiedParts[0].Content.Body; got != "edited caption" {
		t.Errorf("Content.Body = %q, want %q", got, "edited caption")
	}

	if textPart.Metadata.(*MessageMetadata).LastEditTime != 5000 {
		t.Errorf("text part LastEditTime = %d, want 5000", textPart.Metadata.(*MessageMetadata).LastEditTime)
	}
	if attPart.Metadata.(*MessageMetadata).LastEditTime != 0 {
		t.Errorf("att_0 LastEditTime = %d, want unchanged 0 (dedup/bump must never touch the attachment part)", attPart.Metadata.(*MessageMetadata).LastEditTime)
	}
}

// TestConvertEditToMatrix_DedupReadsFromTextPartNotFirstPart: dedup reads the
// text part's LastEditTime, not att_0's zero.
func TestConvertEditToMatrix_DedupReadsFromTextPartNotFirstPart(t *testing.T) {
	attPart := &database.Message{
		ID:       gcid.MakeMessageID("msg1"),
		PartID:   gcid.MakeAttachmentPartID(0),
		Metadata: &MessageMetadata{LastEditTime: 0},
	}
	textPart := &database.Message{
		ID:       gcid.MakeMessageID("msg1"),
		PartID:   gcid.TextPartID,
		Metadata: &MessageMetadata{LastEditTime: 5000},
	}
	existing := []*database.Message{attPart, textPart}
	msg := editMsg("msg1", "msg1", "duplicate", 5000, 0)

	convert := convertEditToMatrix(msgconv.New())
	_, err := convert(context.Background(), flatPortal(), nil, existing, msg)
	if !errors.Is(err, bridgev2.ErrIgnoringRemoteEvent) {
		t.Errorf("error = %v, want wrapping bridgev2.ErrIgnoringRemoteEvent (dedup must read the text part's 5000, not att_0's 0)", err)
	}
}

// TestConvertEditToMatrix_DedupSkipsStaleEdit: an equal edit_ts is a duplicate.
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
	// An edit never changes the original create_time or topic.
	if meta.TimestampMicro != 111 {
		t.Errorf("TimestampMicro = %d, want unchanged 111", meta.TimestampMicro)
	}
	if meta.TopicID != "msg1" {
		t.Errorf("TopicID = %q, want unchanged %q", meta.TopicID, "msg1")
	}
}

// TestConvertEditToMatrix_FallsBackToLastUpdateTimeWhenLastEditTimeUnset pins
// the last_edit_time-or-last_update_time fallback.
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

// TestConvertEditToMatrix_NoExistingMetadataStillWorks: nil metadata counts as
// LastEditTime 0 and must not panic.
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

// TestConvertMessageToMatrix_NilPortalMetadataTreatedAsFlat: a portal without
// PortalMetadata is treated as non-threads-only.
func TestConvertMessageToMatrix_NilPortalMetadataTreatedAsFlat(t *testing.T) {
	portal := &bridgev2.Portal{
		Portal: &database.Portal{},
		Bridge: &bridgev2.Bridge{Matrix: &fakeMatrixConnector{}},
	}
	msg := topicMsg("topic1", "topic1", "hello")

	convert := convertMessageToMatrix(msgconv.New(), nil)
	cm, err := convert(context.Background(), portal, nil, msg)
	if err != nil {
		t.Fatalf("convertMessageToMatrix returned error: %v", err)
	}
	if cm.ThreadRoot != nil {
		t.Errorf("ThreadRoot = %v, want nil when PortalMetadata is absent", *cm.ThreadRoot)
	}
}
