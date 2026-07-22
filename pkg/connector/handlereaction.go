package connector

// handlereaction.go -- Matrix <-> Google Chat reactions, both directions (M4
// Task 3): the outbound side (a Matrix reaction, plus the reaction-target
// branch of a Matrix redaction) and the inbound side (events.go's
// queueMessageReaction), driving Google Chat's react RPC (update_reaction).
//
// Google Chat reactions are PER-EMOJI, not one-per-user: a single sender may
// react to the same message with any number of DISTINCT emoji
// simultaneously (each is its own row, keyed by (emoji, sender, message)),
// and reacting again with an emoji already applied is a silent no-op
// duplicate, not a toggle/replace. bridgev2 models this generically via
// [bridgev2.MatrixReactionPreResponse.EmojiID]: when EmojiID is set (as it
// always is here), portal.handleMatrixReaction's own dedup check (mautrix-go
// bridgev2/portal.go:1651-1663) keys off (message, part, sender, EmojiID)
// rather than (message, part, sender) alone, so two different emoji from the
// same sender are never treated as duplicates of each other -- exactly Google
// Chat's per-emoji semantics. This is why PreHandleMatrixReaction below always
// sets EmojiID (unlike mautrix-meta's own PreHandleMatrixReaction, which
// leaves EmojiID blank for Messenger/WhatsApp's one-reaction-per-user model,
// MaxReactions: 1) and why MaxReactions is left at its zero value
// (0 = unlimited): Google Chat has no per-sender reaction-count cap for this
// bridge to enforce.
//
// variation selectors: Google Chat's own wire protocol (Emoji.unicode, the
// proto field carrying the reaction emoji) never includes U+FE0F
// (VARIATION SELECTOR-16); Matrix's spec-recommended reaction key form
// always does. Every value that crosses the direction boundary is
// normalized accordingly:
//   - PreHandleMatrixReaction strips the selector off the Matrix-supplied
//     key (variationselector.Remove) before it becomes both the EmojiID (the
//     per-emoji dedup key) and the Emoji sent to GC.
//   - queueMessageReaction (events.go) adds the selector back
//     (variationselector.Add) for the value handed to Matrix, while EmojiID
//     stays the bare form so an inbound echo of a Matrix-initiated reaction
//     dedups against the same key PreHandleMatrixReaction would have produced
//     for the identical emoji. bridgev2's own handleRemoteReaction
//     (portal.go:3527) applies Add() again unconditionally when building the
//     actual Matrix event content; variationselector.Add is idempotent (it
//     strips every existing selector before re-adding, per
//     go.mau.fi/util/variationselector's own doc comment), so pre-applying it
//     here is redundant but harmless.
import (
	"context"
	"fmt"

	"github.com/rs/zerolog"
	"go.mau.fi/util/variationselector"
	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

var _ bridgev2.ReactionHandlingNetworkAPI = (*GChatClient)(nil)

// PreHandleMatrixReaction resolves the emoji this login's own reaction
// should be keyed and sent by, stripping the variation selector off the
// Matrix-supplied key before even looking up the target message. EmojiID and
// Emoji are always the SAME bare (variation-selector-stripped) string:
// EmojiID becomes the per-emoji dedup key bridgev2's own handleMatrixReaction
// uses (see this file's top-of-file doc comment), and Emoji is what
// HandleMatrixReaction below sends to Google Chat's update_reaction RPC --
// GC's own wire form never carries the selector either, so no separate
// normalization is needed between the two.
func (c *GChatClient) PreHandleMatrixReaction(_ context.Context, msg *bridgev2.MatrixReaction) (bridgev2.MatrixReactionPreResponse, error) {
	emoji := variationselector.Remove(msg.Content.RelatesTo.Key)
	return bridgev2.MatrixReactionPreResponse{
		SenderID: gcid.MakeUserID(string(c.UserLogin.ID)),
		EmojiID:  networkid.EmojiID(emoji),
		Emoji:    emoji,
	}, nil
}

// HandleMatrixReaction issues update_reaction (type ADD) for a Matrix
// reaction on a previously-bridged message, building the request
// field-by-field:
//
//   - message_id.parent_id.topic_id.{group_id,topic_id}: group_id is
//     gcid.ParsePortalID(msg.Portal.ID), the same derivation every other
//     outbound call uses (handlematrix.go); topic_id reuses
//     threadRootTopicID(msg.TargetMessage) (handlematrix.go, M3 Task 6) --
//     the target's own stored MessageMetadata.TopicID, falling back to the
//     target's own message id when that's empty (a `thread_id or message_id`
//     fallback, where the thread id is the target's own stored topic id).
//   - message_id.message_id: gcid.ParseMessageID(msg.TargetMessage.ID).
//   - emoji.unicode: msg.PreHandleResp.Emoji, the bare
//     (variation-selector-stripped) form PreHandleMatrixReaction already
//     computed.
//   - type: ADD, unconditionally -- this method is only ever reached for a
//     brand new (non-duplicate) reaction; bridgev2's own handleMatrixReaction
//     (portal.go:1651-1663) already filters out duplicates before calling
//     this.
//
// The returned *database.Reaction carries a *ReactionMetadata caching the
// resolved topic id (see ReactionMetadata's own doc comment, dbmeta.go, for
// why this is cached here rather than re-derived on removal); every other
// field is left at its zero value, matching HandleMatrixEdit-adjacent
// connectors (e.g. gmessages' own HandleMatrixReaction) that lean on
// bridgev2's own documented "the central bridge module already has all the
// required fields and will fill them automatically" behavior
// (ReactionHandlingNetworkAPI.HandleMatrixReaction's own doc comment,
// mautrix-go bridgev2/networkinterface.go).
func (c *GChatClient) HandleMatrixReaction(ctx context.Context, msg *bridgev2.MatrixReaction) (*database.Reaction, error) {
	group, err := gcid.ParsePortalID(msg.Portal.ID)
	if err != nil {
		return nil, fmt.Errorf("googlechat: %w", err)
	}

	messageID := gcid.ParseMessageID(msg.TargetMessage.ID)
	// ok is unused: msg.TargetMessage is never nil here (bridgev2's own
	// handleMatrixReaction already resolved it from the DB before ever
	// calling this method, mautrix-go bridgev2/portal.go:1590-1597).
	topicID, _ := threadRootTopicID(msg.TargetMessage)

	req := &pb.UpdateReactionRequest{
		MessageId: &pb.MessageId{
			ParentId: &pb.MessageParentId{
				Parent: &pb.MessageParentId_TopicId{
					TopicId: &pb.TopicId{
						GroupId: gchatmeow.PartsToGroupID(group.ID, group.IsDM),
						TopicId: proto.String(topicID),
					},
				},
			},
			MessageId: proto.String(messageID),
		},
		Emoji: &pb.Emoji{Content: &pb.Emoji_Unicode{Unicode: msg.PreHandleResp.Emoji}},
		Type:  pb.UpdateReactionRequest_ADD.Enum(),
	}

	send := c.updateReactionFn
	if send == nil {
		conn := c.getConn()
		if conn == nil {
			return nil, fmt.Errorf("googlechat: not connected")
		}
		send = conn.UpdateReaction
	}

	if _, err := send(ctx, req); err != nil {
		return nil, fmt.Errorf("googlechat: update_reaction (add) failed: %w", err)
	}

	return &database.Reaction{Metadata: &ReactionMetadata{TopicID: topicID}}, nil
}

// HandleMatrixReactionRemove issues update_reaction (type REMOVE) for a
// Matrix redaction of a previously-bridged reaction, the same way
// HandleMatrixReaction above does, except:
//
//   - message_id.message_id: gcid.ParseMessageID(msg.TargetReaction.MessageID)
//     -- the reacted-to message's own id (NOT the reaction's own Matrix event
//     id).
//   - message_id.parent_id.topic_id.topic_id: c.reactionTopicID(ctx, msg.TargetReaction),
//     which prefers the *ReactionMetadata a Matrix-initiated HandleMatrixReaction
//     call already cached (the fast path -- no lookup needed, see
//     ReactionMetadata's doc comment, dbmeta.go) and otherwise falls back to
//     a fresh DB.Message lookup -- see reactionTopicID's own doc comment for
//     why the fallback is required (a reaction added from the Google Chat
//     side, queueMessageReaction in events.go, has nothing to cache a topic
//     id from at add-time).
//   - emoji.unicode: string(msg.TargetReaction.EmojiID) -- the SAME bare
//     emoji this reaction's own PreHandleMatrixReaction/HandleMatrixReaction
//     pair stored as the per-emoji dedup key (EmojiID, not the DB row's
//     separate Emoji field, which bridgev2 only populates when EmojiID is
//     left blank -- see this file's top-of-file doc comment on why EmojiID
//     is always set here).
//   - type: REMOVE, unconditionally.
func (c *GChatClient) HandleMatrixReactionRemove(ctx context.Context, msg *bridgev2.MatrixReactionRemove) error {
	group, err := gcid.ParsePortalID(msg.Portal.ID)
	if err != nil {
		return fmt.Errorf("googlechat: %w", err)
	}

	messageID := gcid.ParseMessageID(msg.TargetReaction.MessageID)
	topicID := c.reactionTopicID(ctx, msg.TargetReaction)

	req := &pb.UpdateReactionRequest{
		MessageId: &pb.MessageId{
			ParentId: &pb.MessageParentId{
				Parent: &pb.MessageParentId_TopicId{
					TopicId: &pb.TopicId{
						GroupId: gchatmeow.PartsToGroupID(group.ID, group.IsDM),
						TopicId: proto.String(topicID),
					},
				},
			},
			MessageId: proto.String(messageID),
		},
		Emoji: &pb.Emoji{Content: &pb.Emoji_Unicode{Unicode: string(msg.TargetReaction.EmojiID)}},
		Type:  pb.UpdateReactionRequest_REMOVE.Enum(),
	}

	send := c.updateReactionFn
	if send == nil {
		conn := c.getConn()
		if conn == nil {
			return fmt.Errorf("googlechat: not connected")
		}
		send = conn.UpdateReaction
	}

	if _, err := send(ctx, req); err != nil {
		return fmt.Errorf("googlechat: update_reaction (remove) failed: %w", err)
	}
	return nil
}

// reactionTopicID resolves the topic id an outbound un-reaction RPC must be
// posted into from an already-resolved *database.Reaction. Two sources are
// consulted, in order:
//
//  1. The *ReactionMetadata HandleMatrixReaction cached at add-time (see
//     ReactionMetadata's own doc comment, dbmeta.go) -- the fast path, no
//     lookup needed. This is ALWAYS present for a reaction that was itself
//     added via Matrix (HandleMatrixReaction always sets it), but is NEVER
//     present for a reaction that was added from the Google Chat side
//     instead (queueMessageReaction, events.go, mirrors an inbound
//     MessageReactionEvent -- a proto message with no per-message payload
//     to read a topic id off directly, unlike HandleMatrixReaction, which
//     already has the full, DB-resolved msg.TargetMessage in hand). A
//     reaction added on Google Chat and later un-reacted via a Matrix
//     redaction (e.g. a double-puppeted user removing their own
//     GC-mirrored reaction from Element, or any room moderator redacting
//     someone else's -- bridgev2's own handleMatrixRedaction does not
//     require the redacter to be the reaction's own sender, mautrix-go
//     bridgev2/portal.go:2531's `// TODO ignore if sender doesn't match?`)
//     is exactly the case an earlier revision of this function got wrong:
//     it fell back straight to the reaction's own target message id,
//     silently treating a THREAD REPLY's message id as if it were the
//     thread's topic id whenever no cached metadata existed -- which,
//     since queueMessageReaction never populates one, was every single
//     GC-originated reaction, not a rare legacy-row edge case. Caught by
//     the M4 Task 3 gchat-port-auditor pass.
//  2. A fresh DB.Message lookup (getMessageFn, defaulting to
//     c.UserLogin.Bridge.DB.Message.GetFirstPartByID) for the reaction's
//     own target message, reading that message's OWN stored
//     MessageMetadata.TopicID via threadRootTopicID (handlematrix.go) --
//     an unconditional lookup that never caches and always re-fetches
//     regardless of which side originally created the reaction. Every
//     bridged message (both directions) already stamps its own
//     MessageMetadata.TopicID at ingest time (msgconv_adapter.go /
//     handlematrix.go, M3 Task 6), so this lookup is always able to
//     resolve the real topic id when the message row still exists.
//
// Only if BOTH sources come up empty (no cached metadata AND either no live
// bridgev2.Bridge to query -- e.g. this package's lightweight tests -- or
// the lookup itself fails or finds nothing) does this fall back to the
// reaction's own target message id, matching threadRootTopicID's identical
// last-resort fallback: message_id == topic_id for any head-of-topic
// message, so a target whose OWN id IS the topic id (the common,
// non-threaded case) still routes correctly either way. r is never nil
// here: bridgev2's own handleMatrixRedaction already checked
// redactionTargetReaction != nil before ever calling
// HandleMatrixReactionRemove (mautrix-go bridgev2/portal.go:2523-2526),
// exactly like HandleMatrixMessageRemove's TargetMessage guarantee
// (handleredact.go).
func (c *GChatClient) reactionTopicID(ctx context.Context, r *database.Reaction) string {
	if meta, ok := r.Metadata.(*ReactionMetadata); ok && meta != nil && meta.TopicID != "" {
		return meta.TopicID
	}

	get := c.getMessageFn
	if get == nil {
		if c.UserLogin == nil || c.UserLogin.Bridge == nil {
			return string(r.MessageID)
		}
		get = c.UserLogin.Bridge.DB.Message.GetFirstPartByID
	}
	msg, err := get(ctx, c.UserLogin.ID, r.MessageID)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).
			Msg("googlechat: failed to look up reacted-to message for its topic id, falling back to the reaction's own message id")
		return string(r.MessageID)
	}
	if msg == nil {
		return string(r.MessageID)
	}
	topicID, _ := threadRootTopicID(msg)
	return topicID
}
