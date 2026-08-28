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
)

// ErrCannotResolveEmailWithoutCreating is returned when an email is offered
// for resolution only. See this file's doc comment: there is no email-to-gaia
// lookup, so the only way to learn the user behind an address is to open the
// DM, which a resolve-only call must not do as a side effect.
var ErrCannotResolveEmailWithoutCreating = errors.New("googlechat: an email address can only be resolved by starting the chat (Google Chat has no email lookup)")

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
var ErrIdentifierNotSingle = errors.New("googlechat: identifier contains whitespace -- pass exactly one identifier (start-chat's optional first argument is a login ID, not the sender)")

// ErrCannotDMYourself is returned when the identifier names the acting
// account itself. Google Chat has no self-DM through create_dm; it answers
// with a bare HTTP 400 that says nothing about the cause, so the check is
// made locally where the reason is still known.
//
// Only detectable for a gaia id. An EMAIL cannot be compared against the
// acting account without an email-to-gaia lookup, which the private API does
// not provide -- so a self-DM by address still reaches the server, and
// createDM's error names this as a likely cause.
var ErrCannotDMYourself = errors.New("googlechat: that is your own account, and Google Chat cannot open a direct message with yourself")

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

func (c *GChatClient) ResolveIdentifier(ctx context.Context, identifier string, createChat bool) (*bridgev2.ResolveIdentifierResponse, error) {
	identifier = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(identifier), "mailto:"))
	if identifier == "" {
		return nil, fmt.Errorf("googlechat: empty identifier")
	}
	// Internal whitespace means two identifiers were run together; see
	// ErrIdentifierNotSingle. Checked after trimming, so surrounding padding
	// is still forgiven.
	if strings.ContainsFunc(identifier, unicode.IsSpace) {
		return nil, ErrIdentifierNotSingle
	}

	if isGaiaID(identifier) {
		if identifier == string(c.UserLogin.ID) {
			return nil, ErrCannotDMYourself
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
		return nil, fmt.Errorf("googlechat: %q is neither a Google Chat user id nor an email address", identifier)
	}
	if !createChat {
		return nil, ErrCannotResolveEmailWithoutCreating
	}

	chat, otherUser, err := c.createDM(ctx, &pb.CreateDmRequest{
		Invitees: []*pb.InviteeInfo{gchatmeow.EmailInvitee(identifier)},
	})
	if err != nil {
		return nil, err
	}
	return &bridgev2.ResolveIdentifierResponse{UserID: otherUser, Chat: chat}, nil
}

// CreateChatWithGhost opens the DM with an already-known user, skipping
// identifier parsing entirely: a ghost's id IS the gaia id (gcid.MakeUserID
// is an identity mapping, see gcid's frozen-format doc comment).
func (c *GChatClient) CreateChatWithGhost(ctx context.Context, ghost *bridgev2.Ghost) (*bridgev2.CreateChatResponse, error) {
	if ghost == nil || ghost.ID == "" {
		return nil, fmt.Errorf("googlechat: cannot start a chat with an unidentified user")
	}
	if string(ghost.ID) == string(c.UserLogin.ID) {
		return nil, ErrCannotDMYourself
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
		return nil, "", fmt.Errorf("googlechat: create_dm failed (is the target reachable from this account, and not your own account?): %w", err)
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
// login. Returns "" when the response listed no usable member, which is not
// an error: the portal sync that follows resolves membership properly, and
// the caller only uses this as a best-effort identity for the response.
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
