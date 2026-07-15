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
