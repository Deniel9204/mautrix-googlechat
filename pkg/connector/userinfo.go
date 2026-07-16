package connector

// userinfo.go -- GetUserInfo (per-ghost user info via get_members) and the
// displayname fallback chain. Ports mautrix_googlechat/puppet.py's
// update_info (puppet.py:145), get_name_from_info (puppet.py:179-201), and
// _update_photo/_reupload_gc_photo (puppet.py:219-266, avatar download).
import (
	"context"
	"fmt"
	"strings"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// GetUserInfo resolves ghost.ID's Google Chat user info via a single-member
// get_members RPC (client.py:691, proto_get_members). Fidelity note:
// user.py's get_users (user.py:466) batches and caches lookups across a
// whole sync pass; that batching wrapper is not ported here since
// NetworkAPI.GetUserInfo is always called for one ghost at a time -- sync.go
// does its own prefetch batching separately, matching sync()'s
// prefetch_users set (user.py:627,646).
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

func strPtr(s string) *string { return &s }

// userInfoFromUser wraps a googlechat.User into bridgev2.UserInfo. Ports
// puppet.py's update_info (puppet.py:145-155): displayname via
// get_name_from_info (see displaynameParams), avatar via _update_photo (see
// wrapAvatar), contact identifiers via _update_contact_info's
// com.beeper.bridge.identifiers (puppet.py:167 -- unconditionally
// "mailto:<email>", even when email is empty; matched here as-is for
// fidelity), and is_bot from the user id's type.
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
// Config.FormatDisplayname, porting puppet.py's get_name_from_info
// (puppet.py:179-201): full name used as-is if present; otherwise
// first+last (space-joined, skipping whichever half is empty) if either is
// present; otherwise left blank so the displayname_template's own
// `{{ or .Name .Email "Unknown user" }}` fallback (example-config.yaml)
// applies -- matching Python's further "else info.email" / "return None"
// steps without duplicating them here.
//
// Also ports get_name_from_info's other branch (puppet.py:194-198): when the
// API gives a full name but no explicit first_name, FirstName is derived
// from the full name instead of staying empty, stripping a trailing
// last_name suffix if one is present (e.g. name="Ada Lovelace",
// last_name="Lovelace" -> FirstName="Ada"). This only affects FirstName (the
// default displayname_template doesn't reference {{.FirstName}} at all --
// example-config.yaml -- so it only matters for a customized template).
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
// from a raw avatar_url. Ports puppet.py's _update_photo (puppet.py:219-243):
// an empty URL means "no avatar" (Remove: true); a non-empty one is
// downloaded (scheme forced to https, puppet.py:249, ported as
// gchatmeow.ForceHTTPS) via gchatmeow.DownloadAvatar rather than net/http
// directly -- pkg/connector never touches the network itself, everything
// routes through gchatmeow (this bridge's one networking layer, RPCs and
// avatar downloads alike). Avatar.ID is the RAW (pre-https-forcing) URL,
// matching Python's own change-detection key (`photo_url != self.photo_id`,
// puppet.py:220-221, compares the untransformed URL); bridgev2.Ghost itself
// skips Get and the whole reupload when AvatarID is unchanged (ghost.go's
// prepareAvatar), so sha256 hashing / re-upload-skipped-when-unchanged
// (puppet.py:251-256) doesn't need separate porting here.
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
