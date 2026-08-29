package connector

// handlemembership.go -- Matrix -> Google Chat outbound membership actions
// (HandleMatrixMembership): invite/add, kick/remove, and leave.
//
// Endpoint routing (verified against the maintained purple-googlechat client):
//   - Invite (leave->invite): create_membership, carrying the target in
//     invitee_member_infos (how purple invites OTHER users).
//   - Kick (join->leave, not self): remove_memberships, member_ids = target.
//   - Leave (join->leave, self): remove_memberships, member_ids = own gaia id
//     (a leave is a self-removal; there is no distinct "leave" endpoint).
//
// Spaces only: these actions are meaningless in a DM, and the request GroupId
// only ever carries the space. A membership change in a DM portal is rejected
// without an RPC. Ban/Knock membership types have no Google Chat equivalent
// and are rejected as unsupported.
//
// The framework echoes the Matrix-side state itself; on success we return an
// empty *MatrixMembershipResult (RedirectTo is only used by the framework's
// invite-redirect path, which does not apply here). On RPC failure we return
// the error so the framework reports the action as failed.
import (
	"context"
	"errors"
	"fmt"

	"maunium.net/go/mautrix/bridgev2"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

var _ bridgev2.MembershipHandlingNetworkAPI = (*GChatClient)(nil)

// targetGaiaID extracts the Google Chat gaia id of a membership change's
// target. Both a ghost's and a user login's networkid id ARE the gaia id
// (gcid.MakeUserID / MakeUserLoginID are identity mappings), so this is a
// plain type switch over the two GhostOrUserLogin implementations.
func targetGaiaID(target bridgev2.GhostOrUserLogin) (string, bool) {
	switch t := target.(type) {
	case *bridgev2.Ghost:
		return string(t.ID), true
	case *bridgev2.UserLogin:
		return string(t.ID), true
	default:
		return "", false
	}
}

func (c *GChatClient) HandleMatrixMembership(ctx context.Context, msg *bridgev2.MatrixMembershipChange) (*bridgev2.MatrixMembershipResult, error) {
	// Self-membership changes that need nothing done on Google Chat, handled
	// before anything else -- including the DM guard below, since a DM portal's
	// own join is just as much a no-op as a space portal's.
	//
	// These are mostly the bridge's OWN echo. Creating a portal invites the
	// logged-in user and then accepts on their behalf with their double
	// puppet; that join is not tagged as a double-puppet event
	// (ASIntent.SendState does not add the marker, and nothing in
	// bridgev2/matrix adds it to outgoing state), so it is not suppressed and
	// arrives back here as a genuine AcceptInvite. Without this case it fell
	// through to the default below and was reported as a failed membership
	// change, once per portal created -- and in a DM as the flatly wrong
	// "membership changes are not supported in DMs", about the user joining
	// their own portal.
	//
	// Answering success is not a shortcut, it is the correct answer: there is
	// nothing to send. The user is already a Google Chat member of the
	// conversation -- that is why the portal exists at all -- and
	// ProfileChange is a Matrix displayname or avatar edit with no Google Chat
	// counterpart.
	//
	// Deliberately NOT extended to RejectInvite or Knock: those are real user
	// decisions that would need an RPC, and answering success would silently
	// swallow them. They keep erroring until there is a path that can reach
	// them (see #32/#36).
	switch msg.Type {
	case bridgev2.AcceptInvite, bridgev2.Join, bridgev2.ProfileChange:
		return &bridgev2.MatrixMembershipResult{}, nil
	}

	group, err := gcid.ParsePortalID(msg.Portal.ID)
	if err != nil {
		return nil, fmt.Errorf("googlechat: %w", err)
	}
	if group.IsDM {
		return nil, errors.New("googlechat: membership changes are not supported in DMs")
	}
	groupID := gchatmeow.PartsToGroupID(group.ID, false)

	switch msg.Type {
	case bridgev2.Invite:
		gaia, ok := targetGaiaID(msg.Target)
		if !ok {
			return nil, errors.New("googlechat: invite target is not a Google Chat user")
		}
		req := &pb.CreateMembershipRequest{
			GroupId:            groupID,
			InviteeMemberInfos: []*pb.InviteeMemberInfo{gchatmeow.UserInviteeMemberInfo(gaia)},
		}
		if err := c.createMembership(ctx, req); err != nil {
			return nil, err
		}
	case bridgev2.Kick:
		gaia, ok := targetGaiaID(msg.Target)
		if !ok {
			return nil, errors.New("googlechat: kick target is not a Google Chat user")
		}
		if err := c.removeMember(ctx, groupID, gaia); err != nil {
			return nil, err
		}
	case bridgev2.Leave:
		// A leave is a self-removal; the target is the logged-in user.
		if err := c.removeMember(ctx, groupID, string(c.UserLogin.ID)); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("googlechat: unsupported membership change (%s -> %s)", msg.Type.From, msg.Type.To)
	}
	return &bridgev2.MatrixMembershipResult{}, nil
}

// removeMember issues remove_memberships for one gaia id in a space. Used for
// both Kick (another user) and Leave (own id). membership_state is
// MEMBER_INVITED, matching what purple-googlechat sends on removals.
func (c *GChatClient) removeMember(ctx context.Context, groupID *pb.GroupId, gaia string) error {
	req := &pb.RemoveMembershipsRequest{
		GroupId:         groupID,
		MemberIds:       []*pb.MemberId{gchatmeow.UserMemberID(gaia)},
		MembershipState: pb.MembershipState_MEMBER_INVITED.Enum(),
	}
	send := c.removeMembershipsFn
	if send == nil {
		conn := c.getConn()
		if conn == nil {
			return errors.New("googlechat: not connected")
		}
		send = conn.RemoveMemberships
	}
	if _, err := send(ctx, req); err != nil {
		return fmt.Errorf("googlechat: remove_memberships failed: %w", err)
	}
	return nil
}

// createMembership issues create_membership (an invite).
func (c *GChatClient) createMembership(ctx context.Context, req *pb.CreateMembershipRequest) error {
	send := c.createMembershipFn
	if send == nil {
		conn := c.getConn()
		if conn == nil {
			return errors.New("googlechat: not connected")
		}
		send = conn.CreateMembership
	}
	if _, err := send(ctx, req); err != nil {
		return fmt.Errorf("googlechat: create_membership failed: %w", err)
	}
	return nil
}
