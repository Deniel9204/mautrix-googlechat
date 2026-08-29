package connector

// createchat.go -- starting a NEW conversation from Matrix: resolving an
// identifier to a Google Chat user, opening a DM with them, and creating a
// space.
//
// # Resolving an identifier
//
// Google Chat addresses users by gaia id, and the private API this bridge
// speaks exposes no lookup that turns an email address into one. That single
// fact shapes the whole flow:
//
//   - A gaia id (all digits) resolves locally, with no round trip at all.
//   - An email can only be ACTED on, never merely resolved: create_dm accepts
//     it as an invitee, and the resulting DM's own membership list is what
//     finally reveals the gaia id. So ResolveIdentifier with createChat=false
//     cannot answer for an email, and says so rather than inventing a user id
//     or silently creating a conversation the caller did not ask for.
//
// A DM is unique per pair on Google Chat, so create_dm doubles as "find the
// existing one": starting a chat with someone already in a DM returns that
// DM instead of a second one.
import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

var (
	_ bridgev2.IdentifierResolvingNetworkAPI = (*GChatClient)(nil)
	_ bridgev2.GhostDMCreatingNetworkAPI     = (*GChatClient)(nil)
	// On the CONNECTOR, not the client: the framework resolves this as
	// login.Bridge.Network.(IdentifierValidatingNetwork), which is always the
	// NetworkConnector. A ValidateUserID method on *GChatClient would compile
	// and do nothing at all -- hence this assertion.
	_ bridgev2.IdentifierValidatingNetwork = (*GChatConnector)(nil)
)

// ErrCannotResolveEmailWithoutCreating is returned when an email is offered
// for resolution only. See this file's doc comment: there is no email-to-gaia
// lookup, so the only way to learn the user behind an address is to open the
// DM, which a resolve-only call must not do as a side effect.
//
// The deliberate rejections in this file are bridgev2.RespError VALUES (never
// pointers, and never type-annotated `error`), matching login.go's sentinels.
// The type is what makes the provisioning API answer 400 with the message
// below instead of 500 "Internal error resolving identifier": RespondWithError
// looks for a WritableError, which RespError satisfies by value. It stays
// invisible on the bot-command path, because bridgev2.RespError.Error()
// returns only the message, without the errcode.
//
// Value, not pointer, is load-bearing twice over. RespError contains maps, so
// declaring these concrete turns an accidental `err == ErrX` into a compile
// error rather than a runtime panic -- and RespError.Is falls back to
// comparing ERRCODES when handed a pointer, which would silently conflate two
// sentinels that share one. errors.Is is safe throughout: it skips its `==`
// fast path for a non-comparable target and still consults RespError.Is.
// Status codes are literals so this package imports no HTTP client (see
// ErrLoginCookiesInvalid).
var ErrCannotResolveEmailWithoutCreating = bridgev2.RespError{
	ErrCode:    "FI.MAU.GOOGLECHAT.EMAIL_REQUIRES_CREATE",
	Err:        "googlechat: an email address can only be resolved by starting the chat (Google Chat has no email lookup)",
	StatusCode: 400,
}

// ErrIdentifierNotSingle is returned when an identifier contains internal
// whitespace.
//
// This is almost always the same mistake: `start-chat`'s optional first
// argument is a LOGIN ID, not a sender, and bridgev2 folds it back into the
// identifier when it is not one it recognises (commands/startchat.go's
// getClientForStartingChat). So `start-chat me@x.com them@y.com` arrives here
// as one string with a space in it. Shipping that to Google produces a bare
// HTTP 500 that says nothing about the real problem, so it is caught here
// with an explanation instead.
var ErrIdentifierNotSingle = bridgev2.RespError{
	ErrCode:    "FI.MAU.GOOGLECHAT.IDENTIFIER_NOT_SINGLE",
	Err:        "googlechat: identifier contains whitespace -- pass exactly one identifier (start-chat's optional first argument is a login ID, not the sender)",
	StatusCode: 400,
}

// ErrCannotDMYourself is returned when the identifier names the acting
// account itself. Google Chat has no self-DM through create_dm; it answers
// with a bare HTTP 400 that says nothing about the cause, so the check is
// made locally where the reason is still known.
//
// Detected two different ways. A gaia id is compared against the acting
// login's id before the request goes out, which is exact. An EMAIL cannot be
// resolved to a gaia id at all -- the private API has no such lookup -- so
// that case is recognised the other way round: the request goes out, and if
// Google rejects it with a 400, the address is compared against the acting
// login's own (userinfo.go stores it). After the fact rather than before it,
// so a stale or aliased address can never block a DM that would have worked.
var ErrCannotDMYourself = bridgev2.RespError{
	ErrCode:    "FI.MAU.GOOGLECHAT.CANNOT_DM_SELF",
	Err:        "googlechat: that is your own account, and Google Chat cannot open a direct message with yourself",
	StatusCode: 400,
}

// ErrIdentifierMissing is returned when no identifier survives argument
// parsing.
//
// On Google Chat this has one cause worth naming. A login ID *is* a gaia id
// -- gcid.MakeUserID and gcid.MakeUserLoginID are the same identity cast, and
// login.go fills UserLogin.ID from get_self_user_status -- so every one of
// the user's own login ids is also a syntactically valid identifier.
// bridgev2 consumes the first argument as a login selector whenever it names
// one of this user's logins, which means `start-chat <your-own-login-id>`
// has its ONLY argument eaten and arrives here empty.
var ErrIdentifierMissing = bridgev2.RespError{
	ErrCode:    "FI.MAU.GOOGLECHAT.IDENTIFIER_MISSING",
	Err:        "googlechat: no identifier left to resolve -- on Google Chat a login ID is also a user id, so a bare id that is one of your own logins is taken as the login selector; pass the target after it, as `start-chat <your-login-id> <target-id>`",
	StatusCode: 400,
}

// ErrNotAGoogleChatIdentifier is returned for something that is clearly a
// Matrix identifier rather than a Google Chat one. Both contain "@", which
// was previously the whole test for an email, so these used to be forwarded
// to create_dm and come back as an opaque server rejection.
var ErrNotAGoogleChatIdentifier = bridgev2.RespError{
	ErrCode:    "FI.MAU.GOOGLECHAT.NOT_A_GOOGLECHAT_IDENTIFIER",
	Err:        "googlechat: that looks like a Matrix ID, not a Google Chat identifier -- pass an email address or a numeric Google Chat user id",
	StatusCode: 400,
}

// ErrGhostUnidentified is CreateChatWithGhost's own missing-input rejection.
// It gets its own errcode rather than sharing ErrIdentifierMissing's: they are
// different callers with different remedies, and two sentinels sharing an
// errcode is exactly the pair that would conflate first if these ever became
// pointers.
var ErrGhostUnidentified = bridgev2.RespError{
	ErrCode:    "FI.MAU.GOOGLECHAT.GHOST_UNIDENTIFIED",
	Err:        "googlechat: cannot start a chat with an unidentified user",
	StatusCode: 400,
}

// selfDMError reports a self-DM in a way that lets bridgev2 try the user's
// OTHER logins before giving up.
//
// The target being THIS login's own account does not make it unreachable: two
// Google accounts bridged by the same Matrix user can DM each other perfectly
// well, and which login is "acting" is chosen by the framework rather than by
// the user. Wrapping ErrResolveIdentifierTryNext is what the framework
// documents for precisely this situation ("trying to resolve another login's
// user ID"), so the request is retried against the other logins. The
// explanation is kept in the message because, when there is no other login,
// this is the text the user actually reads.
func selfDMError() error {
	return fmt.Errorf("%w: %w", ErrCannotDMYourself, bridgev2.ErrResolveIdentifierTryNext)
}

// isGaiaID reports whether identifier is a bare Google Chat user id, which is
// always a run of digits. Anything else is treated as an email.
func isGaiaID(identifier string) bool {
	if identifier == "" {
		return false
	}
	for _, r := range identifier {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// ValidateUserID implements bridgev2.IdentifierValidatingNetwork: it reports
// whether a networkid.UserID has the SHAPE of a Google Chat user id.
// gcid.MakeUserID is an identity cast, so the test is isGaiaID on the raw
// string.
//
// This matters because a ghost id LOOKS pre-validated but is user input on two
// paths. The appservice ghost-MXID pattern matches an arbitrary localpart, so
// anything a Matrix user types as @googlechat_<junk>:server becomes a
// networkid.UserID -- reaching CreateChatWithGhost from provisionutil (the
// start-chat command and the provisioning API) and from the ghost-DM invite
// handler. This hook is the only place that can stop it EARLY: the framework
// checks it before materialising the ghost row, and before the invite path
// registers that ghost as a real appservice user and joins it to the room. A
// guard inside CreateChatWithGhost fires after all of that has happened.
//
// Shape only, per the framework's contract -- deliberately no existence check
// and, in particular, no rejection of the acting account's own id: self-DMs
// are refused PER-LOGIN with ErrResolveIdentifierTryNext so bridgev2 can try
// the user's other logins, and the connector has no login context to make
// that call with anyway.
//
// Bots share the same UserId.id field as humans with no separate format, and
// every bot id seen so far is numeric; if Google ever emits a non-numeric one,
// this would refuse to START a chat with that bot from Matrix. Bridging an
// existing bot DM is unaffected -- ValidateUserID has no inbound call sites --
// and ResolveIdentifier already rejects any non-digit, non-"@" identifier, so
// this makes the ghost-MXID path agree with the bare-id path rather than
// adding a new limit.
func (gc *GChatConnector) ValidateUserID(userID networkid.UserID) bool {
	return isGaiaID(string(userID))
}

// isOwnEmail reports whether identifier is the acting login's OWN address, as
// learned by updateOwnLoginProfile (userinfo.go). False whenever the address
// is not known, which is the safe answer: it means "carry on and let the
// server decide", i.e. exactly the behaviour that existed before this.
//
// Case-folded, and nothing else. Gmail's dot-and-plus aliasing
// (foo.bar+x@gmail.com and foobar@gmail.com are one consumer mailbox) is
// deliberately NOT normalised: the rule is @gmail.com-only, and on a Workspace
// domain first.last@ and firstlast@ can be two different colleagues. The
// asymmetry decides it -- a miss costs nothing (the request goes to the server
// exactly as it does today), while a false match would mislabel a real
// person's address as the user's own.
func (c *GChatClient) isOwnEmail(identifier string) bool {
	own := c.ownEmail()
	return own != "" && strings.EqualFold(own, identifier)
}

// hasOtherLogins reports whether this user has a login OTHER than the acting
// one, i.e. whether "try the next login" has anywhere to go.
//
// It matters because bridgev2's start-chat loop restarts at index 0 over ALL
// of the user's logins, so it re-runs the one that just failed. For a check
// made BEFORE the request that costs nothing. For a failure classified AFTER
// it, deferring with no sibling login means a second, identical create_dm
// against the same account, which cannot answer differently -- exactly the
// "repeat it once per login" cost this file refuses to pay for 403 and 429.
//
// True when the user is unknown: only a bare UserLogin built by a test has no
// User attached, and assuming a sibling there keeps the classification under
// test. In production UserLogin.User is always populated.
func (c *GChatClient) hasOtherLogins() bool {
	if c.UserLogin == nil || c.UserLogin.User == nil {
		return true
	}
	return len(c.UserLogin.User.GetUserLogins()) > 1
}

// createDMRejectedTarget reports whether a create_dm failure is a statement
// about THIS account's view of the target rather than about the request or the
// transport -- the only kind worth re-issuing against the user's other logins.
//
// 400 only, deliberately. It is the status live probing has actually seen for
// an un-DM-able id, and it is what a self-DM comes back as. 403 is tempting
// but this API family uses it as a quota signal, and 404 shows up as a
// request-shape symptom; replaying either once per login would repeat a
// throttle or a bug N times over. Anything without a status at all -- a
// timeout, a dead connection -- is not deferred either: fail closed.
func createDMRejectedTarget(err error) bool {
	var status *gchatmeow.UnexpectedStatusError
	if !errors.As(err, &status) {
		return false
	}
	return status.Status == 400
}

func (c *GChatClient) ResolveIdentifier(ctx context.Context, identifier string, createChat bool) (*bridgev2.ResolveIdentifierResponse, error) {
	identifier = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(identifier), "mailto:"))
	if identifier == "" {
		return nil, ErrIdentifierMissing
	}
	// Internal whitespace means two identifiers were run together; see
	// ErrIdentifierNotSingle. Checked after trimming, so surrounding padding
	// is still forgiven.
	if strings.ContainsFunc(identifier, unicode.IsSpace) {
		return nil, ErrIdentifierNotSingle
	}
	// A comma- or semicolon-joined list is the same mistake as the
	// whitespace one, reached by a user who took "pass exactly one
	// identifier" as an invitation to use a separator.
	if strings.ContainsAny(identifier, ",;") {
		return nil, ErrIdentifierNotSingle
	}
	// An MXID and a matrix.to link both contain "@", which is the entire
	// test for an email below, so without this they would be forwarded to
	// create_dm as if they were addresses. A ghost MXID never reaches here --
	// bridgev2 resolves those to CreateChatWithGhost first.
	if strings.HasPrefix(identifier, "@") || strings.Contains(identifier, "matrix.to/") {
		return nil, ErrNotAGoogleChatIdentifier
	}

	if isGaiaID(identifier) {
		if identifier == string(c.UserLogin.ID) {
			return nil, selfDMError()
		}
		resp := &bridgev2.ResolveIdentifierResponse{UserID: gcid.MakeUserID(identifier)}
		if !createChat {
			return resp, nil
		}
		chat, _, err := c.createDM(ctx, &pb.CreateDmRequest{Members: []*pb.UserId{gchatmeow.UserID(identifier)}})
		if err != nil {
			return nil, err
		}
		resp.Chat = chat
		return resp, nil
	}

	if !strings.Contains(identifier, "@") {
		// Dynamic message, so it cannot be a package sentinel; the helper
		// flattens it into the same 4xx-carrying shape.
		return nil, bridgev2.WrapRespErrManual(
			fmt.Errorf("googlechat: %q is neither a Google Chat user id nor an email address", identifier),
			"FI.MAU.GOOGLECHAT.NOT_A_GOOGLECHAT_IDENTIFIER", 400)
	}
	if !createChat {
		return nil, ErrCannotResolveEmailWithoutCreating
	}

	chat, otherUser, err := c.createDM(ctx, &pb.CreateDmRequest{
		Invitees: []*pb.InviteeInfo{gchatmeow.EmailInvitee(identifier)},
	})
	if err != nil {
		// Name the self-DM only AFTER the server has actually refused, not
		// instead of asking it. Pre-empting would be one round trip cheaper
		// and strictly worse: the stored address can be stale or an alias, and
		// nobody has established that Google even refuses a self-DM -- Chat has
		// a note-to-self conversation -- so a local refusal could block
		// something that works. Reading a 400 this way costs nothing, because
		// that request is issued today regardless.
		if createDMRejectedTarget(err) && c.isOwnEmail(identifier) {
			// Keep the server's own answer in the chain. This branch is a
			// DEDUCTION -- Google's 400 does not say "self-DM" -- so if the
			// deduction is ever wrong, the body that says what really happened
			// has to still be there for the log and the API to show.
			if !c.hasOtherLogins() {
				return nil, fmt.Errorf("%w: %w", ErrCannotDMYourself, err)
			}
			return nil, fmt.Errorf("%w: %w", selfDMError(), err)
		}
		return nil, err
	}
	return &bridgev2.ResolveIdentifierResponse{UserID: otherUser, Chat: chat}, nil
}

// CreateChatWithGhost opens the DM with an already-known user, skipping
// identifier parsing entirely: a ghost's id IS the gaia id (gcid.MakeUserID
// is an identity mapping, see gcid's frozen-format doc comment).
func (c *GChatClient) CreateChatWithGhost(ctx context.Context, ghost *bridgev2.Ghost) (*bridgev2.CreateChatResponse, error) {
	if ghost == nil || ghost.ID == "" {
		return nil, ErrGhostUnidentified
	}
	if string(ghost.ID) == string(c.UserLogin.ID) {
		return nil, selfDMError()
	}
	chat, _, err := c.createDM(ctx, &pb.CreateDmRequest{
		Members: []*pb.UserId{gchatmeow.UserID(string(ghost.ID))},
	})
	return chat, err
}

// createDM issues create_dm and turns the response into the portal key
// bridgev2 needs, plus the gaia id of the other participant when the response
// carried enough to identify them (which is how the email path learns who it
// just started talking to).
func (c *GChatClient) createDM(ctx context.Context, req *pb.CreateDmRequest) (*bridgev2.CreateChatResponse, networkid.UserID, error) {
	send := c.createDmFn
	if send == nil {
		conn := c.getConn()
		if conn == nil {
			return nil, "", errors.New("googlechat: not connected")
		}
		send = conn.CreateDm
	}

	resp, err := send(ctx, req)
	if err != nil {
		// The server's rejections here are opaque -- an unusable target and a
		// self-DM both come back as a bare 400 -- so name what is worth
		// checking rather than leaving the reader with a status code.
		wrapped := fmt.Errorf("googlechat: create_dm failed (is the target reachable from this account, and not your own account?): %w", err)
		if createDMRejectedTarget(err) && c.hasOtherLogins() {
			// A 400 is Google saying this ACCOUNT cannot open that DM, which
			// says nothing about the user's other logins -- and the target
			// being reachable from only one of them is the ordinary case for
			// someone bridging a work and a personal account. The error text
			// already names reachability first; this makes bridgev2 act on it.
			return nil, "", fmt.Errorf("%w: %w", wrapped, bridgev2.ErrResolveIdentifierTryNext)
		}
		return nil, "", wrapped
	}
	id, isDM, ok := gchatmeow.GroupIDToParts(resp.GetDm().GetGroupId())
	if !ok || id == "" {
		return nil, "", fmt.Errorf("googlechat: create_dm response carried no usable group id")
	}
	// A DM response should always describe a DM; trust the server's own
	// classification rather than assuming, so a group returned here still
	// produces a correctly-keyed portal.
	group := gcid.GroupID{ID: id, IsDM: isDM}

	return &bridgev2.CreateChatResponse{
		PortalKey: gcid.MakePortalKey(group, c.UserLogin.ID),
	}, c.otherMember(resp.GetMemberships()), nil
}

// otherMember picks the participant of a freshly created DM who is not this
// login. Returns "" when the response listed no usable member, which is
// deliberately not an error -- the DM the server just created is real and must
// not be thrown away because the response did not name the peer.
//
// The portal is unaffected: createDM returns a CreateChatResponse with no
// PortalInfo, and bridgev2's CreateMatrixRoom refetches chat info whenever the
// info or its member list is nil, so membership is resolved from GetChatInfo
// rather than from this. The cost of "" is confined to the caller's own
// best-effort identity field: the bot's "Created chat with <id>" line, and the
// provisioning response's id.
//
// A membership with no user id at all is skipped rather than returned as an
// empty id, so an unresolvable entry cannot be mistaken for the peer.
func (c *GChatClient) otherMember(memberships []*pb.Membership) networkid.UserID {
	self := string(c.UserLogin.ID)
	for _, m := range memberships {
		gaia := m.GetId().GetMemberId().GetUserId().GetId()
		if gaia != "" && gaia != self {
			return gcid.MakeUserID(gaia)
		}
	}
	return ""
}
