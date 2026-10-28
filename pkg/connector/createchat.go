package connector

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
	// Must be on the connector: the framework looks it up on the NetworkConnector.
	_ bridgev2.IdentifierValidatingNetwork = (*GChatConnector)(nil)
)

// ErrCannotResolveEmailWithoutCreating is returned when an email is offered for resolution only:
// there is no email-to-gaia lookup without opening the DM.
//
// These rejections are RespError values, never pointers: RespError.Is compares errcodes for pointers.
var ErrCannotResolveEmailWithoutCreating = bridgev2.RespError{
	ErrCode:    "FI.MAU.GOOGLECHAT.EMAIL_REQUIRES_CREATE",
	Err:        "googlechat: an email address can only be resolved by starting the chat (Google Chat has no email lookup)",
	StatusCode: 400,
}

// ErrIdentifierNotSingle is returned when an identifier contains internal whitespace, usually
// because start-chat's optional first argument (a login ID) was folded into it.
var ErrIdentifierNotSingle = bridgev2.RespError{
	ErrCode:    "FI.MAU.GOOGLECHAT.IDENTIFIER_NOT_SINGLE",
	Err:        "googlechat: identifier contains whitespace -- pass exactly one identifier (start-chat's optional first argument is a login ID, not the sender)",
	StatusCode: 400,
}

// ErrCannotDMYourself is returned when the identifier names the acting account; create_dm
// answers a self-DM with a bare 400.
var ErrCannotDMYourself = bridgev2.RespError{
	ErrCode:    "FI.MAU.GOOGLECHAT.CANNOT_DM_SELF",
	Err:        "googlechat: that is your own account, and Google Chat cannot open a direct message with yourself",
	StatusCode: 400,
}

// ErrIdentifierMissing is returned when no identifier survives argument parsing. A login ID is
// also a gaia id, so bridgev2 may consume a lone own id as the login selector.
var ErrIdentifierMissing = bridgev2.RespError{
	ErrCode:    "FI.MAU.GOOGLECHAT.IDENTIFIER_MISSING",
	Err:        "googlechat: no identifier left to resolve -- on Google Chat a login ID is also a user id, so a bare id that is one of your own logins is taken as the login selector; pass the target after it, as `start-chat <your-login-id> <target-id>`",
	StatusCode: 400,
}

// ErrNotAGoogleChatIdentifier is returned for a Matrix identifier.
var ErrNotAGoogleChatIdentifier = bridgev2.RespError{
	ErrCode:    "FI.MAU.GOOGLECHAT.NOT_A_GOOGLECHAT_IDENTIFIER",
	Err:        "googlechat: that looks like a Matrix ID, not a Google Chat identifier -- pass an email address or a numeric Google Chat user id",
	StatusCode: 400,
}

// ErrGhostUnidentified is CreateChatWithGhost's missing-input rejection.
var ErrGhostUnidentified = bridgev2.RespError{
	ErrCode:    "FI.MAU.GOOGLECHAT.GHOST_UNIDENTIFIED",
	Err:        "googlechat: cannot start a chat with an unidentified user",
	StatusCode: 400,
}

// selfDMError wraps ErrResolveIdentifierTryNext so bridgev2 tries the user's other logins.
func selfDMError() error {
	return fmt.Errorf("%w: %w", ErrCannotDMYourself, bridgev2.ErrResolveIdentifierTryNext)
}

// isGaiaID reports whether identifier is a bare Google Chat user id (all digits).
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

// ValidateUserID implements bridgev2.IdentifierValidatingNetwork (shape check only); it is the
// only early guard against junk ghost MXIDs.
func (gc *GChatConnector) ValidateUserID(userID networkid.UserID) bool {
	return isGaiaID(string(userID))
}

// isOwnEmail reports whether identifier is the acting login's own address, case-folded only:
// Gmail dot/plus aliasing is deliberately not normalised.
func (c *GChatClient) isOwnEmail(identifier string) bool {
	own := c.ownEmail()
	return own != "" && strings.EqualFold(own, identifier)
}

// hasOtherLogins reports whether the user has a login other than the acting one.
// True when the user is unknown (tests only).
func (c *GChatClient) hasOtherLogins() bool {
	if c.UserLogin == nil || c.UserLogin.User == nil {
		return true
	}
	return len(c.UserLogin.User.GetUserLogins()) > 1
}

// createDMRejectedTarget reports whether a create_dm failure is worth retrying on other logins.
// 400 only: 403 is a quota signal and 404 a request-shape symptom here.
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
	// Internal whitespace means two identifiers were run together.
	if strings.ContainsFunc(identifier, unicode.IsSpace) {
		return nil, ErrIdentifierNotSingle
	}
	// A comma- or semicolon-joined list is the same mistake.
	if strings.ContainsAny(identifier, ",;") {
		return nil, ErrIdentifierNotSingle
	}
	// MXIDs and matrix.to links contain "@", which would otherwise pass as an email.
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
		// Dynamic message, so it cannot be a package sentinel.
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
		// Name the self-DM only after the server refused: the stored address may be stale or an alias.
		if createDMRejectedTarget(err) && c.isOwnEmail(identifier) {
			// Keep the server's error in the chain; the self-DM reading is only a deduction.
			if !c.hasOtherLogins() {
				return nil, fmt.Errorf("%w: %w", ErrCannotDMYourself, err)
			}
			return nil, fmt.Errorf("%w: %w", selfDMError(), err)
		}
		return nil, err
	}
	return &bridgev2.ResolveIdentifierResponse{UserID: otherUser, Chat: chat}, nil
}

// CreateChatWithGhost opens the DM with an already-known user; a ghost's id is the gaia id.
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

// createDM issues create_dm and returns the portal key plus the other participant's id, if known.
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
		// The server's rejections are opaque, so name what is worth checking.
		wrapped := fmt.Errorf("googlechat: create_dm failed (is the target reachable from this account, and not your own account?): %w", err)
		if createDMRejectedTarget(err) && c.hasOtherLogins() {
			// A 400 says nothing about the user's other logins.
			return nil, "", fmt.Errorf("%w: %w", wrapped, bridgev2.ErrResolveIdentifierTryNext)
		}
		return nil, "", wrapped
	}
	id, isDM, ok := gchatmeow.GroupIDToParts(resp.GetDm().GetGroupId())
	if !ok || id == "" {
		return nil, "", fmt.Errorf("googlechat: create_dm response carried no usable group id")
	}
	// Trust the server's own DM/space classification.
	group := gcid.GroupID{ID: id, IsDM: isDM}

	return &bridgev2.CreateChatResponse{
		PortalKey: gcid.MakePortalKey(group, c.UserLogin.ID),
	}, c.otherMember(resp.GetMemberships()), nil
}

// otherMember picks the participant of a new DM who is not this login, or "" when none is
// listed; that is not an error, since the DM was created.
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
