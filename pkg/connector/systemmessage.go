package connector

// systemmessage.go -- Google Chat's own convention for announcing membership
// changes and room rename/topic changes: a Message with MessageType
// SYSTEM_MESSAGE carrying exactly one Annotation, ported from
// handle_googlechat_message's own gate + dispatch (portal.py:1362-1374):
//
//	if evt.message_type == googlechat.Message.SYSTEM_MESSAGE and len(evt.annotations) == 1:
//	    update_type = evt.annotations[0].type
//	    if update_type == googlechat.ROOM_UPDATED:
//	        if await self.handle_googlechat_room_update(
//	            sender, update=evt.annotations[0].room_updated, timestamp=matrix_ts
//	        ):
//	            return
//	    elif update_type == googlechat.MEMBERSHIP_CHANGED:
//	        await self.handle_googlechat_membership_change(
//	            source, sender, update=evt.annotations[0].membership_changed
//	        )
//	        return
//
// This SYSTEM_MESSAGE arrives on the exact same MessagePosted event body
// every ordinary message uses (events.go's handleMessagePosted routes
// MESSAGE_UPDATED to queueMessageEdit and everything else -- including these
// system messages -- to what would otherwise be queueMessagePosted); it is
// NOT the dedicated Event.body.group_updated/membership_changed oneof arms
// (GroupUpdatedEvent, MembershipChangedEvent), which Python never reads at
// all. docs/research/02-wire-protocol.md's own wire inventory (rows for
// event types 5 and 15) records both as "decodable but not handled" --
// "bridge uses SYSTEM_MESSAGE annotation path instead" -- and for good
// reason: those two dedicated bodies carry a much thinner shape
// (MembershipChangedEvent is a single member's new/prior state with no
// affected-members list or 9-value Type at all; GroupUpdatedEvent is a bare
// old/new *Group pair with no separate rename-vs-topic split) that cannot
// reproduce the dispatch this file needs. Megabridge's own attempt to use
// those two dedicated bodies instead of this SYSTEM_MESSAGE path is a known,
// unverified-against-live-traffic defect (docs/research/08b-megabridge-connector.md's
// "MESSAGE_POSTED with MessageType == SYSTEM_MESSAGE" / "GROUP_UPDATED" /
// "MEMBERSHIP_CHANGED" rows) -- not one this file repeats. events.go's
// dispatchGChatEvent switch therefore keeps logging-and-ignoring those two
// body arms verbatim (see its own case comments), while trySystemMessage
// (below) is the thing actually wired into handleMessagePosted.
//
// Both branches route to bridgev2's generic simplevent.ChatInfoChange
// (portal.ProcessChatInfoChange, mautrix-go bridgev2/portal.go), which
// itself performs the Matrix room-state RPCs (invite/join/leave state
// events for MemberChanges, m.room.name/m.room.topic for ChatInfo) that
// Python's handle_googlechat_room_update/handle_googlechat_membership_change
// issue by hand via main_intent/sender_intent/target_intent calls --
// no direct Matrix API calls are made from this file, matching every other
// M2-M4 inbound handler in this package (events.go, handlereaction.go, ...).
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
// SYSTEM_MESSAGE with exactly one annotation (Python's own
// `len(evt.annotations) == 1` gate, portal.py:1362 -- zero or 2+ annotations
// on a SYSTEM_MESSAGE, though not observed in practice, are left to the
// normal message path exactly as Python leaves them, rather than guessed
// at) and, if so, queues the matching ChatInfoChange and reports
// handled=true so handleMessagePosted (events.go) does not also bridge it
// as ordinary chat content.
//
// handled=false tells the caller to fall through to queueMessagePosted --
// matching Python's own fallthrough when handle_googlechat_room_update's
// annotation carried neither a usable rename nor a group-details update
// (portal.py:1279's `else: return False`, tryRoomUpdated below), and matching
// Python's implicit no-op when the annotation's own Type is neither
// ROOM_UPDATED nor MEMBERSHIP_CHANGED (any other AnnotationType matches
// neither of Python's two elif arms, portal.py:1369-1374, so execution falls
// out of the `if evt.message_type == SYSTEM_MESSAGE...` block entirely and
// the message is bridged normally). A MEMBERSHIP_CHANGED annotation, by
// contrast, is ALWAYS handled=true once matched -- Python's own elif arm
// unconditionally `return`s after calling handle_googlechat_membership_change
// regardless of what that call did (portal.py:1370-1374), so even a
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

// tryRoomUpdated handles the ROOM_UPDATED annotation type, porting
// handle_googlechat_room_update (portal.py:1262-1279) exactly: a rename (if
// rename_metadata is present AND carries a non-empty new_name -- Python's
// `update.HasField("rename_metadata") and update.rename_metadata.new_name`,
// the second half filtering out an explicitly-empty new_name the same way a
// falsy Python string does) takes priority over a topic/description update
// (group_details_metadata's mere presence, `update.HasField("group_details_metadata")`
// -- a proto2 HasField check, so `!= nil`, NOT `!= ""`: an explicit
// empty-string description update must still apply, matching
// chatinfo.go's own "Topic: unconditional" reasoning). Neither present is
// Python's `else: return False` (portal.py:1278-1279) -- handled=false here,
// identically.
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
// Type values (docs/research/02-wire-protocol.md's own wire inventory:
// INVITED(1) JOINED(2) ADDED(3) REMOVED(4) LEFT(5) BOT_ADDED(6)
// BOT_REMOVED(7) KICKED_DUE_TO_OTR_CONFLICT(8) ROLE_UPDATED(9)) onto the
// (Membership, PrevMembership, ok) triple every affected member's
// bridgev2.ChatMember delta needs, porting Python's own per-type dispatch
// (handle_googlechat_membership_change, portal.py:1295-1335) one arm at a
// time -- ok=false means "no Matrix membership action", matching whichever
// of Python's if/elif arms (or lack thereof) took no action for that type.
func membershipChangeDelta(t pb.MembershipChangedMetadata_Type) (membership, prevMembership event.Membership, ok bool) {
	switch t {
	case pb.MembershipChangedMetadata_INVITED:
		// portal.py:1297-1303: sender_intent.invite_user, falling back to
		// main_intent on MForbidden. PrevMembership=leave: an invite is
		// only meaningful for someone who isn't already a member -- if the
		// ghost's actual Matrix state is already "join" (e.g. GC re-sends
		// an invite notice for an existing member), this gate correctly
		// skips downgrading them back to "invite", which matches the real
		// Matrix-server outcome of Python's own invite_user call to an
		// already-joined member (a no-op/error) rather than diverging
		// from it.
		return event.MembershipInvite, event.MembershipLeave, true
	case pb.MembershipChangedMetadata_JOINED:
		// portal.py:1295-1296: target_intent.ensure_joined -- unconditional
		// and idempotent regardless of the ghost's current Matrix
		// membership. PrevMembership is deliberately left unset (NOT
		// "invite"): gating this on an assumed prior "invite" state would
		// silently DROP a real join whenever the ghost's actual Matrix
		// membership isn't exactly "invite" yet (e.g. the bridge only
		// learned about this portal after the invite happened, or GC's own
		// JOINED notice arrives without a preceding INVITED one on some
		// join paths) -- Python's ensure_joined has no such gate, so
		// neither does this.
		return event.MembershipJoin, "", true
	case pb.MembershipChangedMetadata_ADDED, pb.MembershipChangedMetadata_BOT_ADDED:
		// portal.py:1304-1312: sender_intent.invite_user (tolerating
		// MForbidden -- "will auto-invite in ensure_joined") immediately
		// followed by target_intent.ensure_joined -- a direct add skips
		// the separate accept step JOINED represents, but the net Matrix
		// effect is the identical unconditional join, so PrevMembership is
		// left unset for the same reason as JOINED above.
		return event.MembershipJoin, "", true
	case pb.MembershipChangedMetadata_LEFT:
		// portal.py:1313-1314: target_intent.leave_room -- self-initiated
		// departure. PrevMembership=join matches mautrix-meta's own
		// handleRemoveParticipant precedent for the identical "someone
		// left the room" shape (_reference/meta/pkg/connector/handlemeta.go):
		// only actually apply the leave if the portal's own membership
		// tracking still has them as join, guarding a stale/duplicate LEFT
		// notice from re-leaving an already-left (or never-joined) ghost.
		return event.MembershipLeave, event.MembershipJoin, true
	case pb.MembershipChangedMetadata_REMOVED, pb.MembershipChangedMetadata_BOT_REMOVED:
		// portal.py:1315-1324: sender_intent.kick_user, falling back to
		// main_intent on MForbidden. Same PrevMembership=join reasoning as
		// LEFT above -- REMOVED/BOT_REMOVED and LEFT differ only in who
		// initiated the departure, not in its Matrix membership shape.
		return event.MembershipLeave, event.MembershipJoin, true
	case pb.MembershipChangedMetadata_KICKED_DUE_TO_OTR_CONFLICT:
		// portal.py:1325-1335: sender_intent.kick_user with an
		// OTR-conflict reason string, falling back to main_intent on
		// MForbidden -- same net Matrix effect (a kick to "leave") as
		// REMOVED/BOT_REMOVED above; the human-readable reason Python
		// passes to the kick RPC has no field on bridgev2.ChatMember to
		// carry it.
		return event.MembershipLeave, event.MembershipJoin, true
	default:
		// ROLE_UPDATED (9 -- a member's ROLE changed, not their membership)
		// and TYPE_UNSPECIFIED (0) both fall outside Python's own
		// if/elif chain (portal.py:1295-1335): Python takes no Matrix
		// membership action for either (the per-member ghost-profile sync
		// that precedes the chain, `await target.update_info(source, info)`,
		// still runs there, but that is puppet-info sync handled elsewhere
		// in this bridge -- see userinfo.go/GetUserInfo -- and out of this
		// file's membership-delta scope). ok=false tells the caller to
		// skip this member entirely rather than invent a membership
		// transition Python never performs.
		return "", "", false
	}
}

// queueMembershipChanged handles the MEMBERSHIP_CHANGED annotation type,
// porting handle_googlechat_membership_change's own member loop
// (portal.py:1281-1335): update.affected_members (MemberId list, proto
// field 3) is the SAME field Python iterates
// (`for member_id, info in zip(update.affected_members, infos)`) -- the
// newer, richer affected_memberships list (field 6, which additionally
// carries each member's own prior membership/role state) is deliberately
// NOT read here, matching Python's exclusive use of the older field; reading
// the newer field instead would risk silently processing zero members if
// Google's wire traffic (matching what Python has always received) doesn't
// actually populate it.
//
// update.type (MembershipChangedMetadata_Type) is a SINGLE value for the
// whole annotation, applied uniformly to every affected member -- Python's
// per-iteration `if update.type == ...` branch reads the same `update.type`
// on every pass of the loop, so this function computes the
// (Membership, PrevMembership) pair once (membershipChangeDelta, above) and
// applies it to each affected member's gaia id.
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
		// queuing anything: Python's create_matrix_room (portal.py:573-574)
		// runs unconditionally BEFORE the SYSTEM_MESSAGE dispatch even
		// begins, for every message including this one, so a ROLE_UPDATED
		// notice arriving as literally the first event this bridge ever
		// sees for a portal-less space must still create the Matrix room,
		// even though it has nothing else to apply. On an already-existing
		// portal this is a true no-op (ProcessChatInfoChange,
		// mautrix-go bridgev2/portal.go, does nothing when both ChatInfo and
		// MemberChanges are nil), matching Python's own no-op there too.
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
//     queueMessagePosted uses, and the SAME `sender` local variable Python
//     passes into BOTH handle_googlechat_room_update and
//     handle_googlechat_membership_change (portal.py:1338's
//     `sender = await p.Puppet.get_by_gcid(evt.creator.user_id.id)`, at the
//     top of handle_googlechat_message) -- NOT MembershipChangedMetadata's
//     own `initiator` field (proto field 2), which Python never reads for
//     this purpose either.
//   - timestamp: msg.create_time, converted via gchatmeow.MicrosToTime --
//     Python's own `matrix_ts = evt.create_time // 1000`
//     (portal.py:1360), computed once and reused for both the ROOM_UPDATED
//     and MEMBERSHIP_CHANGED branches (portal.py:1265,1267,1274 pass this
//     same matrix_ts through as `timestamp`).
//   - CreatePortal: true -- Python's create_matrix_room(source) call
//     (portal.py:573-574) runs unconditionally for ANY message reaching
//     handle_googlechat_message before the SYSTEM_MESSAGE dispatch even
//     begins, including these -- so a rename/membership-change system
//     message arriving before any other message has bridged this portal
//     (e.g. the very first event after being added to a brand new space)
//     still creates the Matrix room, matching Python.
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
