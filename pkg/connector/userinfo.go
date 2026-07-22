package connector

// userinfo.go -- GetUserInfo (per-ghost user info via get_members) and the
// displayname fallback chain, plus per-ghost avatar download and reupload.
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

// GetUserInfo resolves ghost.ID's Google Chat user info via a single-member
// get_members RPC. Design note: batching and caching lookups across a whole
// sync pass is intentionally not done here, since NetworkAPI.GetUserInfo is
// always called for one ghost at a time -- sync.go does its own prefetch
// batching separately.
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

// updateOwnLoginProfile fetches the logged-in account's OWN display name (via
// the same get_members path GetUserInfo uses for ghosts) and stores it on the
// UserLogin as RemoteName + RemoteProfile.Name. GetSelfUserStatus (login.go)
// only yields the gaia id, so without this the UserLogin's RemoteName stays
// empty and bridgev2's per-login "personal filtering space" is named
// "Google Chat ()" with an empty identity. Called from syncChats BEFORE any
// portal/space is created, so the space is named correctly the first time;
// running on every fresh connect also backfills a login created before this
// existed. Best-effort: a fetch/save failure is logged and skipped, never fatal
// to the sync.
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
	// The RAW name, not Config.FormatDisplayname: bridgev2 builds the space
	// name as "<network> (<RemoteName>)" (space.go), so a formatted displayname
	// like "Abigel Varga (Google Chat)" would double-wrap into
	// "Google Chat (Abigel Varga (Google Chat))". displaynameParams resolves the
	// full-name / first+last chain; fall back to email when it's blank.
	user := resp.GetMembers()[0].GetUser()
	name := displaynameParams(user).Name
	if name == "" {
		name = user.GetEmail()
	}
	if name == "" {
		return
	}
	if c.UserLogin.RemoteName == name && c.UserLogin.RemoteProfile.Name == name {
		return
	}
	c.UserLogin.RemoteName = name
	c.UserLogin.RemoteProfile.Name = name
	if err := c.save(ctx); err != nil {
		log.Warn().Err(err).Msg("googlechat: failed to save own login profile")
		return
	}
	log.Debug().Str("remote_name", name).Msg("googlechat: updated own login profile (RemoteName)")
}

func strPtr(s string) *string { return &s }

// userInfoFromUser wraps a googlechat.User into bridgev2.UserInfo:
// displayname via the fallback chain (see displaynameParams), avatar via
// wrapAvatar, contact identifiers as com.beeper.bridge.identifiers
// (unconditionally "mailto:<email>", even when email is empty), and is_bot
// from the user id's type.
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

// displaynameParams resolves the name fallback chain before handing off to
// Config.FormatDisplayname: full name used as-is if present; otherwise
// first+last (space-joined, skipping whichever half is empty) if either is
// present; otherwise left blank so the displayname_template's own
// `{{ or .Name .Email "Unknown user" }}` fallback (example-config.yaml)
// applies -- the further "else email" / blank steps are left to the template
// rather than duplicated here.
//
// The other branch: when the API gives a full name but no explicit
// first_name, FirstName is derived from the full name instead of staying
// empty, stripping a trailing last_name suffix if one is present (e.g.
// name="Ada Lovelace", last_name="Lovelace" -> FirstName="Ada"). This only
// affects FirstName (the default displayname_template doesn't reference
// {{.FirstName}} at all -- example-config.yaml -- so it only matters for a
// customized template).
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

// wrapAvatar builds the bridgev2.Avatar bridgev2.Ghost.UpdateAvatar reuploads
// from a raw avatar_url: an empty URL means "no avatar" (Remove: true); a
// non-empty one is downloaded (scheme forced to https via
// gchatmeow.ForceHTTPS) with gchatmeow.DownloadAvatar rather than net/http
// directly -- pkg/connector never touches the network itself, everything
// routes through gchatmeow (this bridge's one networking layer, RPCs and
// avatar downloads alike). Avatar.ID is the RAW (pre-https-forcing) URL used
// as the change-detection key (the untransformed URL is what's compared);
// bridgev2.Ghost itself skips Get and the whole reupload when AvatarID is
// unchanged (ghost.go's prepareAvatar), so sha256 hashing /
// re-upload-skipped-when-unchanged doesn't need separate handling here.
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
