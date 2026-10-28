package connector

import (
	"context"
	"errors"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv/gchatfmt"
	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv/matrixfmt"
)

// gchatMentionResolver resolves a gaia id to a double-puppeted login's MXID
// (named by RemoteName) or else a ghost MXID. Unresolvable ids return ok=false.
func gchatMentionResolver(
	ghostMXID func(networkid.UserID) id.UserID,
	getUserLogin func(networkid.UserLoginID) *bridgev2.UserLogin,
) gchatfmt.MentionResolver {
	return func(gaiaID string) (id.UserID, string, bool) {
		if gaiaID == "" {
			return "", "", false
		}
		if getUserLogin != nil {
			if login := getUserLogin(gcid.MakeUserLoginID(gaiaID)); login != nil && login.UserMXID != "" {
				return login.UserMXID, login.RemoteName, true
			}
		}
		if ghostMXID == nil {
			return "", "", false
		}
		mxid := ghostMXID(gcid.MakeUserID(gaiaID))
		if mxid == "" {
			return "", "", false
		}
		return mxid, "", true
	}
}

// newInboundMentionResolver wires gchatMentionResolver to portal; nil-safe.
func newInboundMentionResolver(portal *bridgev2.Portal) gchatfmt.MentionResolver {
	if portal == nil || portal.Bridge == nil || portal.Bridge.Matrix == nil {
		return gchatMentionResolver(nil, nil)
	}
	matrix := portal.Bridge.Matrix
	return gchatMentionResolver(
		func(gaiaUserID networkid.UserID) (mxid id.UserID) {
			intent := matrix.GhostIntent(gaiaUserID)
			if intent == nil {
				return ""
			}
			// GetMXID can panic on a nil wrapped IntentAPI; degrade to no pill.
			defer func() {
				if recover() != nil {
					mxid = ""
				}
			}()
			return intent.GetMXID()
		},
		portal.Bridge.GetCachedUserLoginByID,
	)
}

// mentionsFromParsed builds m.mentions from the mentions gchatfmt actually
// rendered, so malformed annotations never ping. Returns nil if empty.
func mentionsFromParsed(parsed gchatfmt.ParsedMentions) *event.Mentions {
	if len(parsed.UserIDs) == 0 && !parsed.Room {
		return nil
	}
	mentions := &event.Mentions{Room: parsed.Room}
	for _, mxid := range parsed.UserIDs {
		mentions.Add(mxid)
	}
	return mentions
}

// cloneMentions copies m so message parts never share a UserIDs slice.
func cloneMentions(m *event.Mentions) *event.Mentions {
	if m == nil {
		return nil
	}
	return &event.Mentions{
		UserIDs: append([]id.UserID(nil), m.UserIDs...),
		Room:    m.Room,
	}
}

// matrixMentionResolver maps a Matrix pill to a gaia id: our ghost, else a
// portal user's login, else ok=false (plain text).
func matrixMentionResolver(
	ctx context.Context,
	parseGhostMXID func(id.UserID) (networkid.UserID, bool),
	getExistingUser func(context.Context, id.UserID) (*bridgev2.User, error),
	findPreferredLogin func(context.Context, *bridgev2.User) (*bridgev2.UserLogin, error),
) matrixfmt.MentionResolver {
	return func(mxid id.UserID) (string, bool) {
		if parseGhostMXID != nil {
			if gaiaID, ok := parseGhostMXID(mxid); ok && gaiaID != "" {
				return string(gaiaID), true
			}
		}
		if getExistingUser == nil || findPreferredLogin == nil {
			return "", false
		}
		user, err := getExistingUser(ctx, mxid)
		if err != nil {
			// MentionResolver has no error return, so log it and fall back.
			zerolog.Ctx(ctx).Warn().Err(err).Str("mxid", string(mxid)).
				Msg("googlechat: matrixMentionResolver: GetExistingUserByMXID failed, rendering mention as plain text")
			return "", false
		}
		if user == nil {
			return "", false
		}
		login, err := findPreferredLogin(ctx, user)
		if err != nil {
			// ErrNotLoggedIn is expected for users without a login; don't warn.
			if !errors.Is(err, bridgev2.ErrNotLoggedIn) {
				zerolog.Ctx(ctx).Warn().Err(err).Str("mxid", string(mxid)).
					Msg("googlechat: matrixMentionResolver: FindPreferredLogin failed, rendering mention as plain text")
			}
			return "", false
		}
		if login == nil || login.ID == "" {
			return "", false
		}
		return string(login.ID), true
	}
}

// newOutboundMentionResolver wires matrixMentionResolver to portal; nil-safe.
func newOutboundMentionResolver(ctx context.Context, portal *bridgev2.Portal) matrixfmt.MentionResolver {
	if portal == nil || portal.Bridge == nil || portal.Bridge.Matrix == nil {
		return matrixMentionResolver(ctx, nil, nil, nil)
	}
	return matrixMentionResolver(
		ctx,
		portal.Bridge.Matrix.ParseGhostMXID,
		portal.Bridge.GetExistingUserByMXID,
		func(ctx context.Context, user *bridgev2.User) (*bridgev2.UserLogin, error) {
			// allowRelay=false: a relay message has no login to resolve.
			login, _, err := portal.FindPreferredLogin(ctx, user, false)
			return login, err
		},
	)
}
