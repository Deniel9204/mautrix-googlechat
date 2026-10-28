package connector

import (
	"context"
	"errors"
	"fmt"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

var _ bridgev2.MembershipHandlingNetworkAPI = (*GChatClient)(nil)

// targetGaiaID returns the target's gaia id (ghost and login ids are both the gaia id).
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
	// Self-membership echoes (e.g. the bridge's own portal-creation join) need no RPC, except
	// answering a pending space invite (invites.go).
	if msg.Type == bridgev2.AcceptInvite || msg.Type == bridgev2.RejectInvite {
		if group, err := gcid.ParsePortalID(msg.Portal.ID); err == nil && !group.IsDM && c.hasPendingInvite(group.ID) {
			return c.answerInvite(ctx, group, msg.Type == bridgev2.AcceptInvite)
		}
	}
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
		if err := c.removeMember(ctx, groupID, string(c.UserLogin.ID)); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("googlechat: unsupported membership change (%s -> %s)", msg.Type.From, msg.Type.To)
	}
	return &bridgev2.MatrixMembershipResult{}, nil
}

// answerInvite accepts (create_membership with own id) or declines (self-removal) a pending space invite.
func (c *GChatClient) answerInvite(ctx context.Context, group gcid.GroupID, accept bool) (*bridgev2.MatrixMembershipResult, error) {
	groupID := gchatmeow.PartsToGroupID(group.ID, false)
	own := string(c.UserLogin.ID)
	var err error
	if accept {
		err = c.createMembership(ctx, &pb.CreateMembershipRequest{
			GroupId:   groupID,
			MemberIds: []*pb.MemberId{gchatmeow.UserMemberID(own)},
		})
	} else {
		err = c.removeMember(ctx, groupID, own)
	}
	if err != nil {
		return nil, err
	}
	if err := c.clearPendingInvite(ctx, group.ID); err != nil {
		zerolog.Ctx(ctx).Err(err).Str("space_id", group.ID).Msg("googlechat: failed to clear answered space invite")
	}
	if accept {
		c.resyncAcceptedSpace(group)
	}
	return &bridgev2.MatrixMembershipResult{}, nil
}

// removeMember issues remove_memberships for one gaia id; membership_state is MEMBER_INVITED.
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
