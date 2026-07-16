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
// mautrix-go -- also pure, no I/O) for `Puppet.get_mxid_from_id`. Python's
// mention_text override -- a live Matrix room-member-state-store lookup,
// only for the double-puppet branch -- is NOT ported as-is: gchatfmt's
// MentionResolver seam has no room for a state-store dependency (see its
// doc comment). For a plain ghost mention, name is left "" here, which
// gchatfmt's renderMention falls back to entity_text for, matching
// Python's OWN unconditional `mention_text = entity_text` default for that
// branch. For the double-puppet branch specifically, gchatMentionResolver
// substitutes login.RemoteName (the double-puppeted account's network-side
// display name) instead of a room-member displayname -- a deliberately
// different, DB-free source for a similar effect, not the same value
// Python would show; see gchatMentionResolver's own doc comment.
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

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

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
		func(gaiaUserID networkid.UserID) (mxid id.UserID) {
			intent := matrix.GhostIntent(gaiaUserID)
			if intent == nil {
				return ""
			}
			// GhostIntent's concrete implementation (bridgev2/matrix.Connector)
			// can return a non-nil MatrixAPI wrapping a nil *appservice.IntentAPI
			// when the encoded gaia id somehow produces an empty ghost
			// localpart -- shouldn't happen for a real (non-empty) gaia id,
			// but GetMXID() would nil-pointer-panic in that case, and this
			// runs on every live message conversion. Message conversion must
			// degrade to "no pill for this mention" on that, never crash the
			// whole conversion -- matching this codebase's broader rule that
			// malformed/unexpected server data returns/falls back rather than
			// panics (see gchatfmt/convert.go's renderAnnotations bounds
			// check).
			defer func() {
				if recover() != nil {
					mxid = ""
				}
			}()
			return intent.GetMXID()
		},
		portal.Bridge.GetCachedUserLoginByID,
	)
}

// mentionsFromParsed turns the gchatfmt.ParsedMentions gchatfmt.Parse
// reports it ACTUALLY rendered (via ToMatrix) into the event.Mentions
// ("m.mentions") block that fixes B2/gap G4 (docs/research/08d §1.7):
// neither of megabridge's mention paths ever populated content.Mentions, so
// spec-compliant Matrix clients never actually pinged anyone, including for
// "@all".
//
// This replaces an earlier independent second walk of the raw annotations
// (the removed inboundMentions) that fixed B2 but re-introduced a phantom-
// ping bug: it applied no bounds/validity gate, so a malformed/out-of-bounds
// mention annotation -- one gchatfmt itself renders no pill for, whose
// "@Name" text is therefore absent from the delivered body -- still pinged
// the user. Sourcing content.Mentions from ParsedMentions instead makes "who
// gets pinged" identical to "whose pill gchatfmt rendered", by construction
// (gchatfmt.collectMentions applies the very same spanWithinParent bounds
// gate + chip filter + resolver-ok check the HTML renderer does). The
// MENTION_ALL/@room flag likewise comes straight from ParsedMentions.Room.
//
// Returns nil -- not an empty-but-non-nil *event.Mentions -- when the
// message mentions no one, so callers can leave content.Mentions completely
// unset, matching Matrix's own "absent m.mentions" contract rather than
// emitting an explicit empty one. event.Mentions.Add is still used per
// user id, but ParsedMentions.UserIDs is already deduplicated, so this is
// belt-and-suspenders.
func mentionsFromParsed(parsed gchatfmt.ParsedMentions) *event.Mentions {
	if len(parsed.UserIDs) == 0 && !parsed.Room {
		return nil
	}
	mentions := &event.Mentions{Room: parsed.Room}
	for _, mxid := range parsed.UserIDs {
		mentions.Add(mxid)
	}
	return mentions
}

// cloneMentions returns a deep-enough copy of m -- a fresh *event.Mentions
// with its own UserIDs backing array -- so that handing the "same" resolved
// mentions to multiple bridgev2.ConvertedMessagePart.Content values (one
// mentionsFromParsed result covers a whole event, but a message can have
// several parts) never lets a later mutation of one part's Mentions alias
// into a sibling's. Mirrors convertMessageToMatrix's adjacent per-part
// *MessageMetadata allocation, which the same rationale is already
// documented for (msgconv_adapter.go). nil-safe: cloning nil returns nil.
func cloneMentions(m *event.Mentions) *event.Mentions {
	if m == nil {
		return nil
	}
	return &event.Mentions{
		UserIDs: append([]id.UserID(nil), m.UserIDs...),
		Room:    m.Room,
	}
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
		if err != nil {
			// A genuine DB-lookup error (not just "no such user"), e.g. a
			// closed/broken connection -- previously discarded outright,
			// silently degrading to "render this pill as plain text" with
			// no trace left anywhere. Logged (not returned): this resolver
			// runs deep inside msgconv's Parse tree walk, whose
			// MentionResolver signature has no error return (see this
			// file's own package doc comment on the plain-function seam
			// design) -- ok=false is still the correct fallback, but an
			// operator debugging "why didn't my mention pill work" now has
			// a log line instead of silence.
			zerolog.Ctx(ctx).Warn().Err(err).Str("mxid", string(mxid)).
				Msg("googlechat: matrixMentionResolver: GetExistingUserByMXID failed, rendering mention as plain text")
			return "", false
		}
		if user == nil {
			return "", false
		}
		login, err := findPreferredLogin(ctx, user)
		if err != nil {
			// Same rationale as the getExistingUser error above -- e.g. a
			// DB error fetching the user's logins (bridgev2.ErrNotLoggedIn
			// itself is an expected, unremarkable outcome bundled into this
			// same err return, but logging it too is harmless: it's exactly
			// the same "no login, render plain text" outcome either way,
			// just now visible instead of silent).
			zerolog.Ctx(ctx).Warn().Err(err).Str("mxid", string(mxid)).
				Msg("googlechat: matrixMentionResolver: FindPreferredLogin failed, rendering mention as plain text")
			return "", false
		}
		if login == nil || login.ID == "" {
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
