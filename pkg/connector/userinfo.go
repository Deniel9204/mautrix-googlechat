package connector

import (
	"context"
	"fmt"
	"strings"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// GetUserInfo resolves a ghost's user info via a single-member get_members RPC.
func (c *GChatClient) GetUserInfo(ctx context.Context, ghost *bridgev2.Ghost) (*bridgev2.UserInfo, error) {
	conn := c.getConn()
	if conn == nil {
		return nil, fmt.Errorf("googlechat: not connected")
	}
	resp, err := conn.GetMembers(ctx, &pb.GetMembersRequest{
		MemberIds: []*pb.MemberId{
			{Id: &pb.MemberId_UserId{UserId: &pb.UserId{Id: strPtr(string(ghost.ID))}}},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("googlechat: get_members failed: %w", err)
	}
	if len(resp.GetMembers()) == 0 {
		return nil, fmt.Errorf("googlechat: get_members returned no members for %s", ghost.ID)
	}
	return c.userInfoFromUser(ctx, resp.GetMembers()[0].GetUser()), nil
}

// updateOwnLoginProfile stores the account's own name and email on the
// UserLogin so the personal space isn't named "Google Chat ()". Best-effort.
func (c *GChatClient) updateOwnLoginProfile(ctx context.Context) {
	log := zerolog.Ctx(ctx)
	if c.UserLogin == nil {
		return
	}
	getMembers := c.getMembersFn
	if getMembers == nil {
		conn := c.getConn()
		if conn == nil {
			return
		}
		getMembers = conn.GetMembers
	}
	gaia := string(c.UserLogin.ID)
	resp, err := getMembers(ctx, &pb.GetMembersRequest{
		MemberIds: []*pb.MemberId{
			{Id: &pb.MemberId_UserId{UserId: &pb.UserId{Id: strPtr(gaia)}}},
		},
	})
	if err != nil {
		log.Warn().Err(err).Msg("googlechat: failed to fetch own user info for login profile")
		return
	}
	if len(resp.GetMembers()) == 0 {
		return
	}
	// Raw name, not FormatDisplayname: bridgev2 already wraps it in the space name.
	user := resp.GetMembers()[0].GetUser()
	name := displaynameParams(user).Name
	if name == "" {
		name = user.GetEmail()
	}
	// Kept so createchat.go can recognise a self-DM typed as an email. Stored in
	// RemoteProfile, which survives a cookie re-submit, unlike Metadata.
	email := strings.TrimSpace(user.GetEmail())

	// Independent checks so a login with a correct name still learns its email.
	// updateProfile because persistCookies may write the same row concurrently.
	err = c.updateProfile(ctx, func() (persist bool) {
		changed := false
		if name != "" && (c.UserLogin.RemoteName != name || c.UserLogin.RemoteProfile.Name != name) {
			c.UserLogin.RemoteName = name
			c.UserLogin.RemoteProfile.Name = name
			changed = true
		}
		// Never erase a known email with an empty one.
		if email != "" && c.UserLogin.RemoteProfile.Email != email {
			c.UserLogin.RemoteProfile.Email = email
			changed = true
		}
		return changed
	})
	if err != nil {
		log.Warn().Err(err).Msg("googlechat: failed to save own login profile")
		return
	}
	log.Debug().Str("remote_name", name).Bool("has_email", email != "").
		Msg("googlechat: updated own login profile")
}

func strPtr(s string) *string { return &s }

// userInfoFromUser converts a pb.User into bridgev2.UserInfo.
func (c *GChatClient) userInfoFromUser(ctx context.Context, user *pb.User) *bridgev2.UserInfo {
	name := c.Main.Config.FormatDisplayname(ctx, displaynameParams(user))
	isBot := user.GetUserId().GetType() == pb.UserType_BOT
	email := user.GetEmail()
	return &bridgev2.UserInfo{
		Identifiers: []string{"mailto:" + email},
		Name:        &name,
		Avatar:      wrapAvatar(user.GetAvatarUrl()),
		IsBot:       &isBot,
		ExtraUpdates: func(_ context.Context, ghost *bridgev2.Ghost) bool {
			meta, ok := ghost.Metadata.(*GhostMetadata)
			if !ok || meta.Email == email {
				return false
			}
			meta.Email = email
			return true
		},
	}
}

// displaynameParams resolves the name fallback chain: full name, else
// first+last. FirstName is derived from the full name when missing.
func displaynameParams(user *pb.User) DisplaynameParams {
	name := user.GetName()
	first := user.GetFirstName()
	last := user.GetLastName()
	if name == "" {
		parts := make([]string, 0, 2)
		if first != "" {
			parts = append(parts, first)
		}
		if last != "" {
			parts = append(parts, last)
		}
		name = strings.Join(parts, " ")
	} else if first == "" {
		first = name
		if last != "" && strings.HasSuffix(first, last) {
			first = strings.TrimSpace(strings.TrimSuffix(first, last))
		}
	}
	return DisplaynameParams{
		Name:      name,
		FirstName: first,
		Email:     user.GetEmail(),
	}
}

// wrapAvatar builds a Ghost avatar from avatar_url (empty means remove). The
// raw URL is the change-detection ID; the download forces https.
func wrapAvatar(avatarURL string) *bridgev2.Avatar {
	if avatarURL == "" {
		return &bridgev2.Avatar{Remove: true}
	}
	httpsURL := gchatmeow.ForceHTTPS(avatarURL)
	return &bridgev2.Avatar{
		ID: networkid.AvatarID(avatarURL),
		Get: func(ctx context.Context) ([]byte, error) {
			return gchatmeow.DownloadAvatar(ctx, httpsURL)
		},
	}
}
