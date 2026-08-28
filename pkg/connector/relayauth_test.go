package connector

// relayauth_test.go -- relay-mode sender-ownership enforcement for the
// outbound edit / delete / reaction-removal handlers.
//
// In relay mode every relayed Matrix user's action is dispatched through ONE
// shared Google Chat account, so Google's own per-account authorization
// cannot tell those users apart: it sees the relay account acting on content
// the relay account genuinely authored. The only place the distinction still
// exists is the bridge's own message/reaction rows, whose SenderMXID records
// the real Matrix sender (bridgev2 fills it from the event sender). These
// tests pin that the handlers consult it.

import (
	"context"
	"errors"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/id"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

const (
	relayActor    = id.UserID("@alice:example.org")
	relayOtherOne = id.UserID("@bob:example.org")
)

// relayedBy marks an event as relayed on behalf of user, exactly as
// bridgev2's own handleMatrixEvent does when no login owns the sender.
func relayedBy(user id.UserID) *bridgev2.OrigSender {
	return &bridgev2.OrigSender{UserID: user}
}

func editTargetFrom(sender id.UserID) *database.Message {
	return &database.Message{
		ID:         gcid.MakeMessageID("msg1"),
		SenderMXID: sender,
		Metadata:   &MessageMetadata{TopicID: "msg1", TimestampMicro: 1},
	}
}

// --- edit ----------------------------------------------------------------

func TestRelayEditRejectsAnotherUsersMessage(t *testing.T) {
	called := false
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		editMessageFn: func(context.Context, *pb.EditMessageRequest) (*pb.EditMessageResponse, error) {
			called = true
			return editMessageResponse(1), nil
		},
	}
	edit := textMatrixEdit(spacePortal("space1"), "hijacked", editTargetFrom(relayOtherOne))
	edit.OrigSender = relayedBy(relayActor)

	err := gc.HandleMatrixEdit(context.Background(), edit)
	if !errors.Is(err, errRelayNotYourContent) {
		t.Fatalf("HandleMatrixEdit error = %v, want errRelayNotYourContent", err)
	}
	if called {
		t.Error("edit_message was sent for another relayed user's message")
	}
}

func TestRelayEditAllowsOwnMessage(t *testing.T) {
	called := false
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		editMessageFn: func(context.Context, *pb.EditMessageRequest) (*pb.EditMessageResponse, error) {
			called = true
			return editMessageResponse(1), nil
		},
	}
	edit := textMatrixEdit(spacePortal("space1"), "my own edit", editTargetFrom(relayActor))
	edit.OrigSender = relayedBy(relayActor)

	if err := gc.HandleMatrixEdit(context.Background(), edit); err != nil {
		t.Fatalf("HandleMatrixEdit(own message) error = %v, want nil", err)
	}
	if !called {
		t.Error("edit_message was not sent for the relayed user's own message")
	}
}

// TestRelayEditRejectsUnknownOriginalSender fails closed: a row whose
// SenderMXID is unset gives no basis to prove ownership, so a relayed edit of
// it must be refused rather than assumed safe.
func TestRelayEditRejectsUnknownOriginalSender(t *testing.T) {
	called := false
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		editMessageFn: func(context.Context, *pb.EditMessageRequest) (*pb.EditMessageResponse, error) {
			called = true
			return editMessageResponse(1), nil
		},
	}
	edit := textMatrixEdit(spacePortal("space1"), "edit", editTargetFrom(""))
	edit.OrigSender = relayedBy(relayActor)

	if err := gc.HandleMatrixEdit(context.Background(), edit); !errors.Is(err, errRelayNotYourContent) {
		t.Fatalf("HandleMatrixEdit error = %v, want errRelayNotYourContent", err)
	}
	if called {
		t.Error("edit_message was sent for a row with no recorded sender")
	}
}

// TestNonRelayEditIsUnaffected: with no OrigSender the user acts as
// themselves on Google Chat, which enforces its own authorization, so this
// check must not interfere.
func TestNonRelayEditIsUnaffected(t *testing.T) {
	called := false
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		editMessageFn: func(context.Context, *pb.EditMessageRequest) (*pb.EditMessageResponse, error) {
			called = true
			return editMessageResponse(1), nil
		},
	}
	// Target belongs to somebody else, but this is not a relayed event.
	edit := textMatrixEdit(spacePortal("space1"), "edit", editTargetFrom(relayOtherOne))

	if err := gc.HandleMatrixEdit(context.Background(), edit); err != nil {
		t.Fatalf("HandleMatrixEdit(non-relay) error = %v, want nil", err)
	}
	if !called {
		t.Error("edit_message was not sent for a non-relayed edit")
	}
}

// --- delete --------------------------------------------------------------

func TestRelayRedactRejectsAnotherUsersMessage(t *testing.T) {
	called := false
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		deleteMessageFn: func(context.Context, *pb.DeleteMessageRequest) (*pb.DeleteMessageResponse, error) {
			called = true
			return &pb.DeleteMessageResponse{}, nil
		},
	}
	msg := matrixMessageRemove(spacePortal("space1"), editTargetFrom(relayOtherOne))
	msg.OrigSender = relayedBy(relayActor)

	if err := gc.HandleMatrixMessageRemove(context.Background(), msg); !errors.Is(err, errRelayNotYourContent) {
		t.Fatalf("HandleMatrixMessageRemove error = %v, want errRelayNotYourContent", err)
	}
	if called {
		t.Error("delete_message was sent for another relayed user's message")
	}
}

func TestRelayRedactAllowsOwnMessage(t *testing.T) {
	called := false
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		deleteMessageFn: func(context.Context, *pb.DeleteMessageRequest) (*pb.DeleteMessageResponse, error) {
			called = true
			return &pb.DeleteMessageResponse{}, nil
		},
	}
	msg := matrixMessageRemove(spacePortal("space1"), editTargetFrom(relayActor))
	msg.OrigSender = relayedBy(relayActor)

	if err := gc.HandleMatrixMessageRemove(context.Background(), msg); err != nil {
		t.Fatalf("HandleMatrixMessageRemove(own message) error = %v, want nil", err)
	}
	if !called {
		t.Error("delete_message was not sent for the relayed user's own message")
	}
}

// --- reaction removal ----------------------------------------------------

func reactionTargetFrom(sender id.UserID) *database.Reaction {
	return &database.Reaction{
		MessageID:  gcid.MakeMessageID("msg1"),
		SenderMXID: sender,
		Emoji:      "👍",
		Metadata:   &ReactionMetadata{TopicID: "msg1"},
	}
}

func TestRelayReactionRemoveRejectsAnotherUsersReaction(t *testing.T) {
	called := false
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		updateReactionFn: func(context.Context, *pb.UpdateReactionRequest) (*pb.UpdateReactionResponse, error) {
			called = true
			return &pb.UpdateReactionResponse{}, nil
		},
	}
	msg := matrixReactionRemove(spacePortal("space1"), reactionTargetFrom(relayOtherOne))
	msg.OrigSender = relayedBy(relayActor)

	if err := gc.HandleMatrixReactionRemove(context.Background(), msg); !errors.Is(err, errRelayNotYourContent) {
		t.Fatalf("HandleMatrixReactionRemove error = %v, want errRelayNotYourContent", err)
	}
	if called {
		t.Error("update_reaction(REMOVE) was sent for another relayed user's reaction")
	}
}

func TestRelayReactionRemoveAllowsOwnReaction(t *testing.T) {
	called := false
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		updateReactionFn: func(context.Context, *pb.UpdateReactionRequest) (*pb.UpdateReactionResponse, error) {
			called = true
			return &pb.UpdateReactionResponse{}, nil
		},
	}
	msg := matrixReactionRemove(spacePortal("space1"), reactionTargetFrom(relayActor))
	msg.OrigSender = relayedBy(relayActor)

	if err := gc.HandleMatrixReactionRemove(context.Background(), msg); err != nil {
		t.Fatalf("HandleMatrixReactionRemove(own reaction) error = %v, want nil", err)
	}
	if !called {
		t.Error("update_reaction(REMOVE) was not sent for the relayed user's own reaction")
	}
}
