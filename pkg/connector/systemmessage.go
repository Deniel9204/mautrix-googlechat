package connector

// systemmessage.go -- Google Chat's own convention for announcing membership
// changes and room rename/topic changes: a Message with MessageType
// SYSTEM_MESSAGE carrying exactly one Annotation. The gate + dispatch: when
// message_type == SYSTEM_MESSAGE and len(annotations) == 1, read the single
// annotation's type -- a ROOM_UPDATED annotation routes to the room-update
// branch (rename/topic) and a MEMBERSHIP_CHANGED annotation routes to the
// membership-change branch; both consume the message rather than bridging it
// as ordinary text.
//
// This SYSTEM_MESSAGE arrives on the exact same MessagePosted event body
// every ordinary message uses (events.go's handleMessagePosted routes
// MESSAGE_UPDATED to queueMessageEdit and everything else -- including these
// system messages -- to what would otherwise be queueMessagePosted); it is
// NOT the dedicated Event.body.group_updated/membership_changed oneof arms
// (GroupUpdatedEvent, MembershipChangedEvent), which this bridge never reads
// at all. Both (event types 5 and 15) are decodable but not handled -- the
// bridge uses the SYSTEM_MESSAGE annotation path instead -- and for good
// reason: those two dedicated bodies carry a much thinner shape
// (MembershipChangedEvent is a single member's new/prior state with no
// affected-members list or 9-value Type at all; GroupUpdatedEvent is a bare
// old/new *Group pair with no separate rename-vs-topic split) that cannot
// reproduce the dispatch this file needs. Megabridge's own attempt to use
// those two dedicated bodies instead of this SYSTEM_MESSAGE path is a known,
// unverified-against-live-traffic defect -- not one this file repeats.
// events.go's
// dispatchGChatEvent switch therefore keeps logging-and-ignoring those two
// body arms verbatim (see its own case comments), while trySystemMessage
// (below) is the thing actually wired into handleMessagePosted.
//
// Both branches route to bridgev2's generic simplevent.ChatInfoChange
// (portal.ProcessChatInfoChange, mautrix-go bridgev2/portal.go), which
// itself performs the Matrix room-state RPCs (invite/join/leave state
// events for MemberChanges, m.room.name/m.room.topic for ChatInfo) that a
// hand-written handler would otherwise issue via individual
// main/sender/target intent calls -- no direct Matrix API calls are made
// from this file, matching every other M2-M4 inbound handler in this
// package (events.go, handlereaction.go, ...).
import (
	"context"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

// trySystemMessage checks whether evt's MessagePosted body is a Google Chat
// SYSTEM_MESSAGE with exactly one annotation (the `len(annotations) == 1`
// gate -- zero or 2+ annotations on a SYSTEM_MESSAGE, though not observed in
// practice, are left to the normal message path rather than guessed at) and,
// if so, queues the matching ChatInfoChange and reports handled=true so
// handleMessagePosted (events.go) does not also bridge it as ordinary chat
// content.
//
// handled=false tells the caller to fall through to queueMessagePosted --
// as when tryRoomUpdated's annotation carried neither a usable rename nor a
// group-details update (its `else` case, below), and when the annotation's
// own Type is neither ROOM_UPDATED nor MEMBERSHIP_CHANGED (any other
// AnnotationType matches neither dispatch arm, so the message is bridged
// normally). A MEMBERSHIP_CHANGED annotation, by contrast, is ALWAYS
// handled=true once matched -- the membership arm unconditionally consumes
// the message regardless of what queueMembershipChanged did, so even a
// ROLE_UPDATED-only annotation (queueMembershipChanged, below, produces no
// member delta for that type) still consumes the system message rather than
// falling through to be bridged as text.
func (c *GChatClient) trySystemMessage(ctx context.Context, evt *pb.Event) (res bridgev2.EventHandlingResult, handled bool) {
	msg := evt.GetBody().GetMessagePosted().GetMessage()
	if msg.GetMessageType() != pb.Message_SYSTEM_MESSAGE || len(msg.GetAnnotations()) != 1 {
		return bridgev2.EventHandlingResultIgnored, false
	}
	annotation := msg.GetAnnotations()[0]
	switch annotation.GetType() {
	case pb.AnnotationType_ROOM_UPDATED:
		return c.tryRoomUpdated(ctx, evt, msg, annotation.GetRoomUpdated())
	case pb.AnnotationType_MEMBERSHIP_CHANGED:
		return c.queueMembershipChanged(ctx, evt, msg, annotation.GetMembershipChanged()), true
	default:
		return bridgev2.EventHandlingResultIgnored, false
	}
}

// tryRoomUpdated handles the ROOM_UPDATED annotation type: a rename (if
// rename_metadata is present AND carries a non-empty new_name -- the
// non-empty check filters out an explicitly-empty new_name) takes priority
// over a topic/description update (group_details_metadata's mere presence --
// a proto2 HasField check, so `!= nil`, NOT `!= ""`: an explicit
// empty-string description update must still apply, matching chatinfo.go's
// own "Topic: unconditional" reasoning). Neither present -> handled=false
// here.
func (c *GChatClient) tryRoomUpdated(ctx context.Context, evt *pb.Event, msg *pb.Message, update *pb.RoomUpdatedMetadata) (bridgev2.EventHandlingResult, bool) {
	info := &bridgev2.ChatInfo{}
	switch {
	case update.GetRenameMetadata().GetNewName() != "":
		name := update.GetRenameMetadata().GetNewName()
		info.Name = &name
	case update.GetGroupDetailsMetadata() != nil:
		desc := update.GetGroupDetailsMetadata().GetNewGroupDetails().GetDescription()
		info.Topic = &desc
	default:
		return bridgev2.EventHandlingResultIgnored, false
	}
	return c.queueSystemMessageChatInfoChange(ctx, evt, msg, &bridgev2.ChatInfoChange{ChatInfo: info}, "room_updated"), true
}

// membershipChangeDelta maps one of MembershipChangedMetadata's 9 non-zero
// Type values (INVITED(1) JOINED(2) ADDED(3) REMOVED(4) LEFT(5)
// BOT_ADDED(6) BOT_REMOVED(7) KICKED_DUE_TO_OTR_CONFLICT(8)
// ROLE_UPDATED(9)) onto the
// (Membership, PrevMembership, ok) triple every affected member's
// bridgev2.ChatMember delta needs, one Type at a time -- ok=false means "no
// Matrix membership action" for that type.
func membershipChangeDelta(t pb.MembershipChangedMetadata_Type) (membership, prevMembership event.Membership, ok bool) {
	switch t {
	case pb.MembershipChangedMetadata_INVITED:
		// Invite the user, falling back to the main intent on MForbidden.
		// PrevMembership=leave: an invite is only meaningful for someone who
		// isn't already a member -- if the ghost's actual Matrix state is
		// already "join" (e.g. GC re-sends an invite notice for an existing
		// member), this gate correctly skips downgrading them back to
		// "invite", which matches the real Matrix-server outcome of an
		// invite to an already-joined member (a no-op/error) rather than
		// diverging from it.
		return event.MembershipInvite, event.MembershipLeave, true
	case pb.MembershipChangedMetadata_JOINED:
		// Ensure-joined -- unconditional and idempotent regardless of the
		// ghost's current Matrix membership. PrevMembership is deliberately
		// left unset (NOT "invite"): gating this on an assumed prior
		// "invite" state would silently DROP a real join whenever the
		// ghost's actual Matrix membership isn't exactly "invite" yet (e.g.
		// the bridge only learned about this portal after the invite
		// happened, or GC's own JOINED notice arrives without a preceding
		// INVITED one on some join paths) -- an ensure-joined has no such
		// gate, so neither does this.
		return event.MembershipJoin, "", true
	case pb.MembershipChangedMetadata_ADDED, pb.MembershipChangedMetadata_BOT_ADDED:
		// Invite the user (tolerating MForbidden -- will auto-invite in
		// ensure-joined) immediately followed by an ensure-joined -- a
		// direct add skips the separate accept step JOINED represents, but
		// the net Matrix effect is the identical unconditional join, so
		// PrevMembership is left unset for the same reason as JOINED above.
		return event.MembershipJoin, "", true
	case pb.MembershipChangedMetadata_LEFT:
		// A leave -- self-initiated departure. PrevMembership=join matches
		// mautrix-meta's own handleRemoveParticipant precedent for the
		// identical "someone left the room" shape
		// (_reference/meta/pkg/connector/handlemeta.go):
		// only actually apply the leave if the portal's own membership
		// tracking still has them as join, guarding a stale/duplicate LEFT
		// notice from re-leaving an already-left (or never-joined) ghost.
		return event.MembershipLeave, event.MembershipJoin, true
	case pb.MembershipChangedMetadata_REMOVED, pb.MembershipChangedMetadata_BOT_REMOVED:
		// A kick, falling back to the main intent on MForbidden. Same
		// PrevMembership=join reasoning as LEFT above -- REMOVED/BOT_REMOVED
		// and LEFT differ only in who initiated the departure, not in its
		// Matrix membership shape.
		return event.MembershipLeave, event.MembershipJoin, true
	case pb.MembershipChangedMetadata_KICKED_DUE_TO_OTR_CONFLICT:
		// A kick with an OTR-conflict reason string, falling back to the
		// main intent on MForbidden -- same net Matrix effect (a kick to
		// "leave") as REMOVED/BOT_REMOVED above; the human-readable reason
		// has no field on bridgev2.ChatMember to carry it.
		return event.MembershipLeave, event.MembershipJoin, true
	default:
		// ROLE_UPDATED (9 -- a member's ROLE changed, not their membership)
		// and TYPE_UNSPECIFIED (0) both fall outside the dispatch: no Matrix
		// membership action for either (the per-member ghost-profile sync
		// that precedes the dispatch still runs, but that is ghost-info sync
		// handled elsewhere in this bridge -- see userinfo.go/GetUserInfo --
		// and out of this file's membership-delta scope). ok=false tells the
		// caller to skip this member entirely rather than invent a
		// membership transition that never happens.
		return "", "", false
	}
}

// queueMembershipChanged handles the MEMBERSHIP_CHANGED annotation type,
// iterating update.affected_members (MemberId list, proto field 3) -- the
// newer, richer affected_memberships list (field 6, which additionally
// carries each member's own prior membership/role state) is deliberately
// NOT read here; reading the newer field instead would risk silently
// processing zero members if Google's wire traffic doesn't actually populate
// it.
//
// update.type (MembershipChangedMetadata_Type) is a SINGLE value for the
// whole annotation, applied uniformly to every affected member, so this
// function computes the (Membership, PrevMembership) pair once
// (membershipChangeDelta, above) and applies it to each affected member's
// gaia id.
func (c *GChatClient) queueMembershipChanged(ctx context.Context, evt *pb.Event, msg *pb.Message, update *pb.MembershipChangedMetadata) bridgev2.EventHandlingResult {
	log := zerolog.Ctx(ctx)
	membership, prevMembership, ok := membershipChangeDelta(update.GetType())
	change := &bridgev2.ChatInfoChange{}
	if ok {
		memberMap := make(bridgev2.ChatMemberMap)
		ownID := c.ownUserID()
		for _, member := range update.GetAffectedMembers() {
			gcUserID := member.GetUserId().GetId()
			if gcUserID == "" {
				continue
			}
			uid := gcid.MakeUserID(gcUserID)
			memberMap[uid] = bridgev2.ChatMember{
				EventSender:    bridgev2.EventSender{Sender: uid, IsFromMe: uid == ownID},
				Membership:     membership,
				PrevMembership: prevMembership,
			}
		}
		if len(memberMap) > 0 {
			// IsFull is deliberately left false (the zero value): this is a
			// delta of the members who changed, NOT the portal's whole
			// member list -- ChatMemberList.IsFull's own doc comment
			// (mautrix-go bridgev2/portal.go) warns that true here would
			// remove every OTHER current member not listed in this map,
			// which is not what a membership-changed announcement about a
			// handful of members means. Contrast chatinfo.go's
			// chatMemberList/dmMemberListFromWorldItem, which build a
			// COMPLETE snapshot from GetChatInfo and correctly set
			// IsFull: true.
			change.MemberChanges = &bridgev2.ChatMemberList{MemberMap: memberMap}
		}
	}
	if change.MemberChanges == nil {
		// No actionable member delta (ok=false's ROLE_UPDATED/TYPE_UNSPECIFIED,
		// or an affected_members list with no usable ids) -- still queued as
		// an otherwise-empty ChatInfoChange, NOT returned as Ignored without
		// queuing anything: room creation runs unconditionally BEFORE the
		// SYSTEM_MESSAGE dispatch even begins, for every message including
		// this one, so a ROLE_UPDATED notice arriving as literally the first
		// event this bridge ever sees for a portal-less space must still
		// create the Matrix room, even though it has nothing else to apply.
		// On an already-existing portal this is a true no-op
		// (ProcessChatInfoChange, mautrix-go bridgev2/portal.go, does nothing
		// when both ChatInfo and MemberChanges are nil).
		log.Debug().
			Str("gc_membership_change_type", update.GetType().String()).
			Msg("googlechat: SYSTEM_MESSAGE membership change with no actionable member delta")
	}
	return c.queueSystemMessageChatInfoChange(ctx, evt, msg, change, "membership_changed")
}

// queueSystemMessageChatInfoChange builds and queues the
// simplevent.ChatInfoChange shared by both tryRoomUpdated and
// queueMembershipChanged, mirroring extractPostedMessage/queueMessagePosted's
// (events.go) own group-id and sender derivation for a MessagePosted-bodied
// event:
//
//   - group: evt.GetGroupId() (the outer Event's own group_id) -- SYSTEM_MESSAGE
//     is carried on the exact same MessagePosted body every ordinary message
//     uses, so it is routed identically (this file's own top-of-file doc
//     comment); TYPING_STATE_CHANGED is the ONLY body arm in this package
//     that overrides group_id from a body-level field (events.go's
//     typingContextGroupID doc comment), and this is not that.
//   - sender: msg.creator.user_id.id -- the SAME field/derivation
//     queueMessagePosted uses, applied to both the room-update and
//     membership-change branches -- NOT MembershipChangedMetadata's own
//     `initiator` field (proto field 2), which is never read for this
//     purpose.
//   - timestamp: msg.create_time, converted via gchatmeow.MicrosToTime,
//     computed once and reused for both the ROOM_UPDATED and
//     MEMBERSHIP_CHANGED branches.
//   - CreatePortal: true -- room creation runs unconditionally for ANY
//     message before the SYSTEM_MESSAGE dispatch even begins, including
//     these -- so a rename/membership-change system message arriving before
//     any other message has bridged this portal (e.g. the very first event
//     after being added to a brand new space) still creates the Matrix
//     room.
func (c *GChatClient) queueSystemMessageChatInfoChange(ctx context.Context, evt *pb.Event, msg *pb.Message, change *bridgev2.ChatInfoChange, kind string) bridgev2.EventHandlingResult {
	log := zerolog.Ctx(ctx)
	id, isDM, groupOK := gchatmeow.GroupIDToParts(evt.GetGroupId())
	if !groupOK {
		log.Warn().Str("gc_system_message_kind", kind).
			Msg("googlechat: SYSTEM_MESSAGE event with no usable group id, skipping")
		return bridgev2.EventHandlingResultIgnored
	}
	group := gcid.GroupID{ID: id, IsDM: isDM}
	senderUserID := gcid.MakeUserID(msg.GetCreator().GetUserId().GetId())

	res := c.queueRemoteEvent(&simplevent.ChatInfoChange{
		EventMeta: simplevent.EventMeta{
			Type:         bridgev2.RemoteEventChatInfoChange,
			PortalKey:    gcid.MakePortalKey(group, c.UserLogin.ID),
			CreatePortal: true,
			Sender: bridgev2.EventSender{
				Sender:   senderUserID,
				IsFromMe: c.IsThisUser(ctx, senderUserID),
			},
			Timestamp: gchatmeow.MicrosToTime(msg.GetCreateTime()),
		},
		ChatInfoChange: change,
	})
	log.Debug().
		Str("gc_group_id", group.ID).
		Bool("is_dm", group.IsDM).
		Str("gc_system_message_kind", kind).
		Any("result", res).
		Msg("googlechat: queued inbound SYSTEM_MESSAGE chat info change")
	return res
}
