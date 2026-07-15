package connector

// mentions.go -- the real MentionResolver implementations gchatfmt.Parse
// (pkg/msgconv/gchatfmt, M3 Task 1) and matrixfmt.Parse (pkg/msgconv/matrixfmt,
// M3 Task 2) leave to their callers, plus content.Mentions ("m.mentions")
// construction for inbound messages. Both fmt packages' MentionResolver doc
// comments name this file explicitly as the seam that fixes bug B2
// (docs/research/08d-megabridge-msgconv.md §1.7 / §6): megabridge resolved
// an incoming mention either by looking up a coincidentally-shared DM
// portal's room MXID (wrong target -- a room pill, not a user pill) or a
// logged-in bridge user's own MXID (self-mentions only), never pilled a
// ghost at all, and never set content.Mentions for either a real mention or
// MENTION_ALL (gap G4).
//
// # Python source read in full for this task
// (_reference/googlechat-python/mautrix_googlechat/formatter/from_googlechat.py:187-195):
//
//	gcid = annotation.user_mention_metadata.id.id
//	user: u.User = await u.User.get_by_gcid(gcid)
//	mxid = user.mxid if user else pu.Puppet.get_mxid_from_id(gcid)
//	mention_text = entity_text
//	if user:
//	    member = await portal.bridge.state_store.get_member(portal.mxid, user.mxid)
//	    if member and member.displayname:
//	        mention_text = member.displayname
//	html.append(f"<a href='https://matrix.to/#/{mxid}'>{mention_text}</a>")
//
// Every mentioned gaia id resolves to SOME pill: a double-puppeted bridge
// user's own MXID (`user.mxid`) when the gaia id belongs to one of this
// bridge's own logged-in accounts, otherwise a ghost's MXID computed by a
// pure, deterministic formula (`pu.Puppet.get_mxid_from_id`, no DB lookup,
// no requirement that a ghost row already exist -- a gaia id mentioned for
// the first time still resolves). gchatMentionResolver below ports this
// exactly, via bridgev2's equivalents: Bridge.GetCachedUserLoginByID for
// `User.get_by_gcid`, and Bridge.Matrix.GhostIntent(id).GetMXID() (the same
// formula bridgev2 itself uses to name a ghost's Matrix user, ghost.go:58 in
// mautrix-go -- also pure, no I/O) for `Puppet.get_mxid_from_id`. The
// mention_text override is NOT ported: it only fires for the double-puppet
// branch and requires a live room-member-state-store lookup, a dependency
// gchatfmt's MentionResolver seam deliberately has no room for (see its doc
// comment) -- name is left "" here for every case, which gchatfmt's
// renderMention already falls back to entity_text for, matching Python's own
// unconditional `mention_text = entity_text` default.
//
// # Design: plain-function dependencies, not bridgev2 types
//
// gchatMentionResolver/matrixMentionResolver below take plain functions
// (ghostMXID, getUserLogin, parseGhostMXID, getExistingUser,
// findPreferredLogin) rather than *bridgev2.Bridge/*bridgev2.Portal
// directly, matching pkg/connector/client.go's established "*Fn seam"
// pattern (paginatedWorldFn, createTopicFn, addPendingToIgnoreFn, etc.).
// This is not just style-matching: every lookup here is either pure
// (GhostIntent(id).GetMXID(), Bridge.Matrix.ParseGhostMXID -- no I/O) or a
// single bridgev2.Bridge/Portal method (GetCachedUserLoginByID,
// GetExistingUserByMXID, FindPreferredLogin), so tests can supply
// lightweight fakes without a live database or Matrix connector -- nothing
// in bridgev2, mautrix-meta, or the megabridge reference stands up a real
// *bridgev2.Bridge for a unit test either (grep-verified against all three);
// *bridgev2.Bridge's cache maps (ghostsByID etc.) are unexported and only
// safely populated through bridgev2.NewBridge's full networking-connector
// wiring, which this package's other tests (client_test.go,
// handlematrix_test.go) also avoid for exactly this reason.
//
// newInboundMentionResolver/newOutboundMentionResolver are the thin
// production wiring: they extract the plain functions above from a real
// *bridgev2.Portal at the two call sites that need them (msgconv_adapter.go
// for GC->Matrix; handlematrix.go, M3 Task 4, for Matrix->GC).
import (
	"context"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv/gchatfmt"
	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv/matrixfmt"
)

// gchatMentionResolver builds the gchatfmt.MentionResolver gchatfmt.Parse
// needs to pill a Google Chat mention (fix B2). For a gaia id:
//
//  1. If getUserLogin reports it as one of THIS BRIDGE's own logged-in
//     accounts (double puppet -- Python's `user = await User.get_by_gcid(gcid)`
//     branch), the mention resolves to that account's real Matrix user
//     (login.UserMXID), with login.RemoteName as the display name.
//  2. Otherwise it resolves to a ghost, via ghostMXID -- Python's
//     `pu.Puppet.get_mxid_from_id(gcid)` fallback -- with name left "" (see
//     the package doc comment on why the display-name override isn't
//     ported).
//
// Both dependencies are nil-safe (a nil getUserLogin/ghostMXID is treated as
// "that lookup always misses"); an empty gaiaID, or a ghostMXID that comes
// back "", resolves to ok=false rather than an empty-string pill -- gchatfmt
// then falls back to rendering the annotation's own entity_text unchanged
// (no text silently dropped, no broken pill emitted).
func gchatMentionResolver(
	ghostMXID func(networkid.UserID) id.UserID,
	getUserLogin func(networkid.UserLoginID) *bridgev2.UserLogin,
) gchatfmt.MentionResolver {
	return func(gaiaID string) (id.UserID, string, bool) {
		if gaiaID == "" {
			return "", "", false
		}
		if getUserLogin != nil {
			if login := getUserLogin(gcid.MakeUserLoginID(gaiaID)); login != nil && login.UserMXID != "" {
				return login.UserMXID, login.RemoteName, true
			}
		}
		if ghostMXID == nil {
			return "", "", false
		}
		mxid := ghostMXID(gcid.MakeUserID(gaiaID))
		if mxid == "" {
			return "", "", false
		}
		return mxid, "", true
	}
}

// newInboundMentionResolver wires gchatMentionResolver to a real portal's
// bridge (msgconv_adapter.go's convertMessageToMatrix). nil-safe: a portal
// (or its Bridge/Matrix) that isn't fully wired up -- e.g. a bare test
// fixture, matching the "often nil in tests" pattern already documented on
// GChatClient.Main (client.go) -- yields a resolver that always returns
// ok=false rather than panicking.
func newInboundMentionResolver(portal *bridgev2.Portal) gchatfmt.MentionResolver {
	if portal == nil || portal.Bridge == nil || portal.Bridge.Matrix == nil {
		return gchatMentionResolver(nil, nil)
	}
	matrix := portal.Bridge.Matrix
	return gchatMentionResolver(
		func(gaiaUserID networkid.UserID) id.UserID {
			intent := matrix.GhostIntent(gaiaUserID)
			if intent == nil {
				return ""
			}
			return intent.GetMXID()
		},
		portal.Bridge.GetCachedUserLoginByID,
	)
}

// inboundMentions scans a Google Chat message's annotations for
// user_mention_metadata and builds the event.Mentions ("m.mentions") block
// that fixes B2/gap G4 (docs/research/08d §1.7): neither of megabridge's
// mention paths ever populated content.Mentions, so spec-compliant Matrix
// clients never actually pinged anyone, including for "@all".
//
// MENTION_ALL -> Room=true: gchatfmt's renderMention hardcodes the literal
// "@room" text for this case WITHOUT ever calling the resolver (see
// gchatfmt/convert.go), so it has to be detected here independently by
// walking the same annotations, rather than observed as a side effect of
// resolving. Every other user_mention_metadata annotation is resolved via
// resolve and, when ok, added (event.Mentions.Add already dedupes).
//
// Returns nil -- not an empty-but-non-nil *event.Mentions -- when nothing in
// annotations produced a mention, so callers can leave content.Mentions
// completely unset for a message with no mentions, matching Matrix's own
// "absent m.mentions" contract rather than emitting an explicit empty one.
func inboundMentions(annotations []*pb.Annotation, resolve gchatfmt.MentionResolver) *event.Mentions {
	if resolve == nil {
		return nil
	}
	mentions := &event.Mentions{}
	found := false
	for _, a := range annotations {
		um := a.GetUserMentionMetadata()
		if um == nil {
			continue
		}
		if um.GetType() == pb.UserMentionMetadata_MENTION_ALL {
			mentions.Room = true
			found = true
			continue
		}
		if mxid, _, ok := resolve(um.GetId().GetId()); ok {
			mentions.Add(mxid)
			found = true
		}
	}
	if !found {
		return nil
	}
	return mentions
}

// matrixMentionResolver builds the matrixfmt.MentionResolver matrixfmt.Parse
// needs to turn a Matrix mention pill into a MENTION annotation (Matrix ->
// GC direction). ctx is captured at construction time: matrixfmt.MentionResolver
// itself carries no context parameter (see its doc comment), so the caller
// (newOutboundMentionResolver) builds a fresh resolver per Parse call.
//
// Resolution order mirrors mautrix-meta's textfmt.MatrixHTMLParser.convertPill
// (_reference/meta/pkg/msgconv/textfmt/from-matrix.go, read for this task):
//
//  1. parseGhostMXID: is this MXID one of THIS BRIDGE's own ghosts? If so,
//     reverse it straight to the gaia id (ghost IDs and gaia ids are the
//     same string, gcid.MakeUserID is an identity cast -- see gcid.go's doc
//     comment on frozen ID formats).
//  2. Otherwise, is this MXID a real Matrix user with a login in THIS
//     portal (getExistingUser + findPreferredLogin -- the double-puppet
//     reverse: "I mentioned myself", or another Beeper user sharing this
//     bridge)? If so, that login's own id IS its gaia id
//     (gcid.MakeUserLoginID, same identity-cast property).
//  3. Otherwise ok=false: an MXID this bridge has no record of at all
//     (some unrelated Matrix user) renders as plain text by matrixfmt, no
//     crash, no MENTION annotation -- matching the brief's "unknown user"
//     requirement.
//
// All three dependencies are nil-safe.
func matrixMentionResolver(
	ctx context.Context,
	parseGhostMXID func(id.UserID) (networkid.UserID, bool),
	getExistingUser func(context.Context, id.UserID) (*bridgev2.User, error),
	findPreferredLogin func(context.Context, *bridgev2.User) (*bridgev2.UserLogin, error),
) matrixfmt.MentionResolver {
	return func(mxid id.UserID) (string, bool) {
		if parseGhostMXID != nil {
			if gaiaID, ok := parseGhostMXID(mxid); ok && gaiaID != "" {
				return string(gaiaID), true
			}
		}
		if getExistingUser == nil || findPreferredLogin == nil {
			return "", false
		}
		user, err := getExistingUser(ctx, mxid)
		if err != nil || user == nil {
			return "", false
		}
		login, err := findPreferredLogin(ctx, user)
		if err != nil || login == nil || login.ID == "" {
			return "", false
		}
		return string(login.ID), true
	}
}

// newOutboundMentionResolver wires matrixMentionResolver to a real portal's
// bridge (M3 Task 4's handlematrix.go call site). nil-safe like
// newInboundMentionResolver above.
func newOutboundMentionResolver(ctx context.Context, portal *bridgev2.Portal) matrixfmt.MentionResolver {
	if portal == nil || portal.Bridge == nil || portal.Bridge.Matrix == nil {
		return matrixMentionResolver(ctx, nil, nil, nil)
	}
	return matrixMentionResolver(
		ctx,
		portal.Bridge.Matrix.ParseGhostMXID,
		portal.Bridge.GetExistingUserByMXID,
		func(ctx context.Context, user *bridgev2.User) (*bridgev2.UserLogin, error) {
			// allowRelay=false: a relay-sent mention has no real Matrix user
			// to resolve a login for in the first place (FindPreferredLogin
			// only returns a nil,nil,nil "use the relay" triple when
			// allowRelay is true), and matrixfmt already treats ok=false as
			// "render plain text" -- exactly what should happen for a relay
			// message's pills.
			login, _, err := portal.FindPreferredLogin(ctx, user, false)
			return login, err
		},
	)
}
