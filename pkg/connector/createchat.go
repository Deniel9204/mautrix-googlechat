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

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

var (
	_ bridgev2.IdentifierResolvingNetworkAPI = (*GChatClient)(nil)
	_ bridgev2.GhostDMCreatingNetworkAPI     = (*GChatClient)(nil)
	_ bridgev2.GroupCreatingNetworkAPI       = (*GChatClient)(nil)
)

// ErrCannotResolveEmailWithoutCreating is returned when an email is offered
// for resolution only. See this file's doc comment: there is no email-to-gaia
// lookup, so the only way to learn the user behind an address is to open the
// DM, which a resolve-only call must not do as a side effect.
var ErrCannotResolveEmailWithoutCreating = errors.New("googlechat: an email address can only be resolved by starting the chat (Google Chat has no email lookup)")

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

	if isGaiaID(identifier) {
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
		return nil, "", fmt.Errorf("googlechat: create_dm failed: %w", err)
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

// CreateGroup creates a space. Participants are attached as invitees on the
// creation request itself rather than through a follow-up create_membership,
// so a half-created space cannot be left behind if the second call fails.
//
// Google Chat requires a name for a space, unlike a DM. should_find_existing
// _space is deliberately NOT set: the caller explicitly asked for a NEW
// group, and silently handing back a pre-existing one with the same shape
// would be a surprising answer to that.
func (c *GChatClient) CreateGroup(ctx context.Context, params *bridgev2.GroupCreateParams) (*bridgev2.CreateChatResponse, error) {
	name := ""
	if params != nil && params.Name != nil {
		name = strings.TrimSpace(params.Name.Name)
	}
	if name == "" {
		return nil, fmt.Errorf("googlechat: a Google Chat space needs a name")
	}

	info := &pb.SpaceCreationInfo{Name: proto.String(name)}
	for _, participant := range params.Participants {
		if participant == "" {
			continue
		}
		info.InviteeMemberInfos = append(info.InviteeMemberInfos, gchatmeow.UserInviteeMemberInfo(string(participant)))
	}

	send := c.createGroupChatFn
	if send == nil {
		conn := c.getConn()
		if conn == nil {
			return nil, errors.New("googlechat: not connected")
		}
		send = conn.CreateGroup
	}

	resp, err := send(ctx, &pb.CreateGroupRequest{
		CreationInfo: &pb.CreateGroupRequest_Space{Space: info},
	})
	if err != nil {
		return nil, fmt.Errorf("googlechat: create_group failed: %w", err)
	}
	id, isDM, ok := gchatmeow.GroupIDToParts(resp.GetGroup().GetGroupId())
	if !ok || id == "" {
		return nil, fmt.Errorf("googlechat: create_group response carried no usable group id")
	}
	return &bridgev2.CreateChatResponse{
		PortalKey: gcid.MakePortalKey(gcid.GroupID{ID: id, IsDM: isDM}, c.UserLogin.ID),
	}, nil
}
