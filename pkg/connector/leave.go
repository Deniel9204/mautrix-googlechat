package connector

// Leaving from Matrix: spaces are left with every login that has them, DMs are hidden,
// then the bridge bot (not the double puppet, whose leave would echo back) removes the user.
import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/event"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

var _ bridgev2.DeleteChatHandlingNetworkAPI = (*GChatClient)(nil)

var cmdLeave = &commands.FullHandler{
	Func: fnLeave,
	Name: "leave",
	Help: commands.HelpMeta{
		Section:     commands.HelpSectionChats,
		Description: "Leave this chat on Google Chat: a space with all your logins in it, or hide a DM",
		Args:        "[confirm]",
	},
	RequiresPortal: true,
	RequiresLogin:  true,
}

func fnLeave(ce *commands.Event) {
	gc, ok := ce.Bridge.Network.(*GChatConnector)
	if !ok {
		ce.Reply("This bridge's network connector is not Google Chat.")
		return
	}
	// Not Reply's format string: the text may contain '%'.
	reply := gc.runLeaveCommand(ce.Ctx, ce.Portal, ce.User, ce.Args)
	ce.Reply("%s", strings.ReplaceAll(reply, "$cmdprefix", ce.Bridge.Config.CommandPrefix))
}

// leaveResult is one login's outcome on Google Chat.
type leaveResult struct {
	Login *GChatClient
	Err   error
}

func (gc *GChatConnector) runLeaveCommand(ctx context.Context, portal *bridgev2.Portal, user *bridgev2.User, args []string) string {
	group, err := gcid.ParsePortalID(portal.ID)
	if err != nil {
		return fmt.Sprintf("This room's chat id is not a Google Chat one: %v", err)
	}
	logins, err := gc.leavingLogins(ctx, portal, user)
	if err != nil {
		return fmt.Sprintf("Could not look up your Google Chat logins in this chat: %v", err)
	}
	if len(logins) == 0 {
		return "None of your Google Chat logins is in this chat."
	}
	confirmed := gc.Config.SkipLeaveConfirmation || (len(args) > 0 && strings.EqualFold(args[0], "confirm"))
	if !confirmed {
		return leavePreview(group, portal, logins)
	}
	results, err := gc.leaveWith(ctx, portal, user, group, logins)
	return leaveReport(group, portal, results, err)
}

// HandleMatrixDeleteChat leaves without confirmation; deleting a space for everyone has no known RPC.
func (c *GChatClient) HandleMatrixDeleteChat(ctx context.Context, msg *bridgev2.MatrixDeleteChat) error {
	if msg.Content != nil && msg.Content.DeleteForEveryone {
		return errors.New("googlechat: a space can only be deleted for everyone in Google Chat itself; this can leave it instead")
	}
	if c.Main == nil {
		return errors.New("googlechat: connector not initialised")
	}
	group, err := gcid.ParsePortalID(msg.Portal.ID)
	if err != nil {
		return fmt.Errorf("googlechat: %w", err)
	}
	logins, err := c.Main.leavingLogins(ctx, msg.Portal, c.UserLogin.User)
	if err != nil {
		return fmt.Errorf("googlechat: looking up your logins in this chat failed: %w", err)
	}
	if len(logins) == 0 {
		return errors.New("googlechat: none of your Google Chat logins is in this chat")
	}
	results, err := c.Main.leaveWith(ctx, msg.Portal, c.UserLogin.User, group, logins)
	if err != nil {
		return fmt.Errorf("googlechat: left on Google Chat, but %w", err)
	}
	for _, r := range results {
		if r.Err != nil {
			return fmt.Errorf("googlechat: %s", leaveReport(group, msg.Portal, results, nil))
		}
	}
	return nil
}

// leaveWith leaves with every login and removes the user from the Matrix room only if all succeeded.
func (gc *GChatConnector) leaveWith(ctx context.Context, portal *bridgev2.Portal, user *bridgev2.User, group gcid.GroupID, logins []*GChatClient) ([]leaveResult, error) {
	results := leaveLogins(ctx, group, logins)
	for _, r := range results {
		if r.Err != nil {
			return results, nil
		}
	}
	remove := gc.removeFromPortalFn
	if remove == nil {
		remove = gc.removeFromPortal
	}
	if err := remove(ctx, portal, user, logins); err != nil {
		return results, fmt.Errorf("removing you from this room failed: %w", err)
	}
	return results, nil
}

// leaveLogins leaves group on Google Chat with each login: a space by
// self-removal, a DM by hiding it.
func leaveLogins(ctx context.Context, group gcid.GroupID, logins []*GChatClient) []leaveResult {
	results := make([]leaveResult, 0, len(logins))
	for _, login := range logins {
		var err error
		if group.IsDM {
			err = login.hideGroup(ctx, group)
		} else {
			err = login.removeMember(ctx, gchatmeow.PartsToGroupID(group.ID, false), string(login.UserLogin.ID))
		}
		results = append(results, leaveResult{Login: login, Err: err})
	}
	return results
}

// hideGroup hides a conversation in this login's chat list.
func (c *GChatClient) hideGroup(ctx context.Context, group gcid.GroupID) error {
	send := c.hideGroupFn
	if send == nil {
		conn := c.getConn()
		if conn == nil {
			return errors.New("googlechat: not connected")
		}
		send = conn.HideGroup
	}
	if _, err := send(ctx, &pb.HideGroupRequest{
		Id:   gchatmeow.PartsToGroupID(group.ID, group.IsDM),
		Hide: proto.Bool(true),
	}); err != nil {
		return fmt.Errorf("googlechat: hide_group failed: %w", err)
	}
	return nil
}

func (gc *GChatConnector) leavingLogins(ctx context.Context, portal *bridgev2.Portal, user *bridgev2.User) ([]*GChatClient, error) {
	if gc.leavingLoginsFn != nil {
		return gc.leavingLoginsFn(ctx, portal, user)
	}
	var logins []*GChatClient
	add := func(login *bridgev2.UserLogin) {
		if login == nil || login.UserMXID != user.MXID {
			return
		}
		if client, ok := login.Client.(*GChatClient); ok {
			logins = append(logins, client)
		}
	}
	if portal.Receiver != "" {
		add(gc.Bridge.GetCachedUserLoginByID(portal.Receiver))
		return logins, nil
	}
	rows, err := gc.Bridge.DB.UserPortal.GetAllForUserInPortal(ctx, user.MXID, portal.PortalKey)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		add(gc.Bridge.GetCachedUserLoginByID(row.LoginID))
	}
	return logins, nil
}

// removeFromPortal forgets the logins' user-portal rows and has the bot remove the user from the room.
func (gc *GChatConnector) removeFromPortal(ctx context.Context, portal *bridgev2.Portal, user *bridgev2.User, logins []*GChatClient) error {
	for _, login := range logins {
		row, err := gc.Bridge.DB.UserPortal.Get(ctx, login.UserLogin.UserLogin, portal.PortalKey)
		if err == nil && row != nil {
			err = gc.Bridge.DB.UserPortal.Delete(ctx, row)
		}
		if err != nil {
			return fmt.Errorf("forgetting %s's place in this chat: %w", leaveAccountName(login), err)
		}
	}
	if portal.MXID == "" {
		return nil
	}
	_, err := gc.Bridge.Bot.SendState(ctx, portal.MXID, event.StateMember, user.MXID.String(), &event.Content{
		Parsed: &event.MemberEventContent{Membership: event.MembershipLeave, Reason: "Left on Google Chat"},
	}, time.Time{})
	return err
}

func leaveAccountName(login *GChatClient) string {
	if name := login.UserLogin.RemoteName; name != "" {
		return name
	}
	return string(login.UserLogin.ID)
}

func leaveAccountNames(logins []*GChatClient) string {
	names := make([]string, len(logins))
	for i, login := range logins {
		names[i] = leaveAccountName(login)
	}
	return strings.Join(names, ", ")
}

func leaveSpaceName(portal *bridgev2.Portal) string {
	if portal.Name != "" {
		return "**" + portal.Name + "**"
	}
	return "this space"
}

func leavePreview(group gcid.GroupID, portal *bridgev2.Portal, logins []*GChatClient) string {
	if group.IsDM {
		return fmt.Sprintf("This will hide this DM in Google Chat for %s and remove you from this room. "+
			"Google Chat shows it again when a new message arrives. Send `$cmdprefix leave confirm` to go ahead.",
			leaveAccountNames(logins))
	}
	return fmt.Sprintf("This will leave %s on Google Chat as %s and remove you from this room. "+
		"Getting back into a private space needs a new invite. Send `$cmdprefix leave confirm` to go ahead.",
		leaveSpaceName(portal), leaveAccountNames(logins))
}

func leaveReport(group gcid.GroupID, portal *bridgev2.Portal, results []leaveResult, removeErr error) string {
	var ok, failed []string
	for _, r := range results {
		if r.Err != nil {
			failed = append(failed, fmt.Sprintf("%s (%v)", leaveAccountName(r.Login), r.Err))
		} else {
			ok = append(ok, leaveAccountName(r.Login))
		}
	}
	if len(failed) > 0 {
		msg := "Could not leave on Google Chat as " + strings.Join(failed, "; ") + "."
		if len(ok) > 0 {
			msg += " Left as " + strings.Join(ok, ", ") + "."
		}
		msg += " You are still in this room."
		if !group.IsDM {
			msg += " If you are the space's only manager, Google Chat may want the space deleted instead."
		}
		return msg
	}
	var msg string
	if group.IsDM {
		msg = "Hid this DM in Google Chat."
	} else {
		msg = fmt.Sprintf("Left %s on Google Chat as %s.", leaveSpaceName(portal), strings.Join(ok, ", "))
	}
	if removeErr != nil {
		msg += " But " + removeErr.Error() + "."
	}
	return msg
}
