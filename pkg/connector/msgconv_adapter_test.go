package connector

// msgconv_adapter_test.go -- convertMessageToMatrix's own bookkeeping
// (MessageMetadata stamping, already covered implicitly by every M2 test
// that reaches it) plus, as of M3 Task 3, the content.Mentions ("m.mentions")
// wiring fix for B2 (mentions.go, msgconv_adapter.go).
import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
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
