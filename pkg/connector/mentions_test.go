package connector

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv/gchatfmt"
	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv/matrixfmt"
)

// loggedCtx returns a context whose zerolog.Logger writes to buf.
func loggedCtx(buf *bytes.Buffer) context.Context {
	logger := zerolog.New(buf)
	return logger.WithContext(context.Background())
}

func TestGChatMentionResolver_GhostPill(t *testing.T) {
	resolve := gchatMentionResolver(
		func(gaiaID networkid.UserID) id.UserID {
			if gaiaID == "200" {
				return "@200_ghost:example.com"
			}
			return ""
		},
		nil, // no double-puppet accounts known
	)

	mxid, name, ok := resolve("200")
	if !ok || mxid != "@200_ghost:example.com" {
		t.Fatalf("resolve(200) = (%q, %q, %v), want (@200_ghost:example.com, _, true)", mxid, name, ok)
	}
	// Ghost mentions leave name "" so gchatfmt falls back to entity_text.
	if name != "" {
		t.Errorf("name = %q, want \"\" (caller should fall back to entity_text)", name)
	}
}

func TestGChatMentionResolver_OwnUserDoublePuppet(t *testing.T) {
	resolve := gchatMentionResolver(
		func(networkid.UserID) id.UserID {
			t.Fatal("ghostMXID should not be consulted when getUserLogin already resolved a double puppet")
			return ""
		},
		func(loginID networkid.UserLoginID) *bridgev2.UserLogin {
			if loginID == gcid.MakeUserLoginID("100") {
				return &bridgev2.UserLogin{UserLogin: &database.UserLogin{
					UserMXID:   "@alice:example.com",
					RemoteName: "Alice",
				}}
			}
			return nil
		},
	)

	mxid, name, ok := resolve("100")
	if !ok || mxid != "@alice:example.com" || name != "Alice" {
		t.Fatalf("resolve(100) = (%q, %q, %v), want (@alice:example.com, Alice, true)", mxid, name, ok)
	}
}

func TestGChatMentionResolver_EmptyGaiaID(t *testing.T) {
	resolve := gchatMentionResolver(
		func(networkid.UserID) id.UserID { return "@should-not:be.used" },
		nil,
	)
	if _, _, ok := resolve(""); ok {
		t.Error("resolve(\"\") should be ok=false")
	}
}

func TestGChatMentionResolver_NilDeps(t *testing.T) {
	resolve := gchatMentionResolver(nil, nil)
	if _, _, ok := resolve("200"); ok {
		t.Error("resolve with nil deps should be ok=false, not panic")
	}
}

func TestGChatMentionResolver_GhostMXIDEmpty(t *testing.T) {
	resolve := gchatMentionResolver(
		func(networkid.UserID) id.UserID { return "" },
		nil,
	)
	if _, _, ok := resolve("200"); ok {
		t.Error("resolve should be ok=false when ghostMXID comes back empty")
	}
}

func TestGChatMentionResolver_LoginWithoutUserMXIDFallsBackToGhost(t *testing.T) {
	// A login with no UserMXID must not yield an empty pill target.
	resolve := gchatMentionResolver(
		func(gaiaID networkid.UserID) id.UserID {
			if gaiaID == "100" {
				return "@100_ghost:example.com"
			}
			return ""
		},
		func(networkid.UserLoginID) *bridgev2.UserLogin {
			return &bridgev2.UserLogin{UserLogin: &database.UserLogin{}}
		},
	)
	mxid, _, ok := resolve("100")
	if !ok || mxid != "@100_ghost:example.com" {
		t.Errorf("resolve(100) = (%q, _, %v), want ghost fallback (@100_ghost:example.com, true)", mxid, ok)
	}
}

func TestMentionsFromParsed_EmptyReturnsNil(t *testing.T) {
	if got := mentionsFromParsed(gchatfmt.ParsedMentions{}); got != nil {
		t.Errorf("mentionsFromParsed(empty) = %+v, want nil (absent m.mentions)", got)
	}
}

func TestMentionsFromParsed_Room(t *testing.T) {
	got := mentionsFromParsed(gchatfmt.ParsedMentions{Room: true})
	if got == nil || !got.Room {
		t.Fatalf("mentionsFromParsed(Room) = %+v, want Room=true", got)
	}
	if len(got.UserIDs) != 0 {
		t.Errorf("UserIDs = %v, want empty for a pure @room mention", got.UserIDs)
	}
}

func TestMentionsFromParsed_UserIDs(t *testing.T) {
	got := mentionsFromParsed(gchatfmt.ParsedMentions{UserIDs: []id.UserID{"@200_ghost:example.com"}})
	if got == nil || got.Room {
		t.Fatalf("mentionsFromParsed(user) = %+v, want Room=false with a UserID", got)
	}
	if !got.Has("@200_ghost:example.com") {
		t.Errorf("UserIDs = %v, want to include @200_ghost:example.com", got.UserIDs)
	}
}

func TestMentionsFromParsed_RoomAndUser(t *testing.T) {
	got := mentionsFromParsed(gchatfmt.ParsedMentions{
		UserIDs: []id.UserID{"@200_ghost:example.com"},
		Room:    true,
	})
	if got == nil || !got.Room || !got.Has("@200_ghost:example.com") {
		t.Fatalf("mentionsFromParsed(room+user) = %+v, want Room=true and the UserID", got)
	}
}

func TestCloneMentions_Nil(t *testing.T) {
	if got := cloneMentions(nil); got != nil {
		t.Errorf("cloneMentions(nil) = %+v, want nil", got)
	}
}

func TestCloneMentions_IndependentBackingArray(t *testing.T) {
	original := &event.Mentions{UserIDs: []id.UserID{"@a:example.com"}, Room: true}
	clone := cloneMentions(original)

	if clone == original {
		t.Fatal("cloneMentions returned the same pointer, not a copy")
	}
	if !clone.Room || !clone.Has("@a:example.com") {
		t.Fatalf("clone = %+v, want a faithful copy of the original", clone)
	}

	clone.Add("@b:example.com")
	if original.Has("@b:example.com") {
		t.Error("mutating the clone's UserIDs leaked back into the original -- shared backing array")
	}
}

func TestGChatMention_EndToEnd_PillAndMentionsBothCorrect(t *testing.T) {
	resolve := gchatMentionResolver(
		func(gaiaID networkid.UserID) id.UserID {
			if gaiaID == "200" {
				return "@200_ghost:example.com"
			}
			return ""
		},
		nil,
	)
	anns := []*pb.Annotation{gchatfmt.MakeMentionAnnotation(0, 4, "200")}

	_, html, parsed := gchatfmt.Parse(context.Background(), "@Bob hi", anns, resolve)
	wantHTML := `<a href="https://matrix.to/#/@200_ghost:example.com">@Bob</a> hi`
	if html != wantHTML {
		t.Errorf("html = %q, want %q", html, wantHTML)
	}

	mentions := mentionsFromParsed(parsed)
	if mentions == nil || !mentions.Has("@200_ghost:example.com") {
		t.Errorf("content.Mentions = %+v, want to include @200_ghost:example.com (fix B2)", mentions)
	}
}

func TestGChatMention_EndToEnd_RoomMention(t *testing.T) {
	resolve := gchatMentionResolver(nil, nil)
	anns := []*pb.Annotation{gchatfmt.MakeMentionAllAnnotation(0, 4)}

	_, html, parsed := gchatfmt.Parse(context.Background(), "@all hi", anns, resolve)
	if html != "@room hi" {
		t.Errorf("html = %q, want literal @room substitution", html)
	}

	mentions := mentionsFromParsed(parsed)
	if mentions == nil || !mentions.Room {
		t.Errorf("content.Mentions = %+v, want Room=true", mentions)
	}
}

func TestGChatMention_EndToEnd_UnknownGaiaFallsBackToPlainText(t *testing.T) {
	resolve := gchatMentionResolver(nil, nil) // resolves nothing
	anns := []*pb.Annotation{gchatfmt.MakeMentionAnnotation(0, 4, "999")}

	_, html, parsed := gchatfmt.Parse(context.Background(), "@Eve hi", anns, resolve)
	if html != "@Eve hi" {
		t.Errorf("html = %q, want the original entity_text left unpilled", html)
	}
	if got := mentionsFromParsed(parsed); got != nil {
		t.Errorf("content.Mentions = %+v, want nil for an unresolvable mention", got)
	}
}

// TestGChatMention_EndToEnd_PhantomPingPrevented: an out-of-bounds mention renders
// as plain text and must not ping.
func TestGChatMention_EndToEnd_PhantomPingPrevented(t *testing.T) {
	resolve := gchatMentionResolver(
		func(networkid.UserID) id.UserID { return "@200_ghost:example.com" },
		nil,
	)
	// Body is 2 UTF-16 units but the mention claims [0,50).
	anns := []*pb.Annotation{gchatfmt.MakeMentionAnnotation(0, 50, "200")}

	_, html, parsed := gchatfmt.Parse(context.Background(), "hi", anns, resolve)
	if html != "" {
		t.Errorf("html = %q, want empty (out-of-bounds annotation -> plain-text fallback)", html)
	}
	if got := mentionsFromParsed(parsed); got != nil {
		t.Errorf("content.Mentions = %+v, want nil -- a mention with no body text must not ping (phantom ping)", got)
	}
}

// TestGChatMention_EndToEnd_ValidMentionStillPingsDespiteUnrelatedMalformedAnnotation:
// a malformed FORMAT annotation forces plain text, but "@Bob" is still in the body.
func TestGChatMention_EndToEnd_ValidMentionStillPingsDespiteUnrelatedMalformedAnnotation(t *testing.T) {
	resolve := gchatMentionResolver(
		func(gaiaID networkid.UserID) id.UserID {
			if gaiaID == "200" {
				return "@200_ghost:example.com"
			}
			return ""
		},
		nil,
	)
	body := "hi @Bob" // 7 UTF-16 units; "@Bob" is [3,7)
	anns := []*pb.Annotation{
		// Out of bounds: forces a plain-text fallback.
		gchatfmt.MakeFormatAnnotation(0, 2147483647, pb.FormatMetadata_BOLD),
		// A valid mention over "@Bob".
		gchatfmt.MakeMentionAnnotation(3, 4, "200"),
	}

	_, html, parsed := gchatfmt.Parse(context.Background(), body, anns, resolve)
	if html != "" {
		t.Errorf("html = %q, want empty (the malformed FORMAT annotation forces plain-text fallback)", html)
	}
	mentions := mentionsFromParsed(parsed)
	if mentions == nil || !mentions.Has("@200_ghost:example.com") {
		t.Errorf("content.Mentions = %+v, want to still include @200_ghost:example.com (body still shows @Bob)", mentions)
	}
}

func TestMatrixMentionResolver_GhostPill(t *testing.T) {
	resolve := matrixMentionResolver(
		context.Background(),
		func(mxid id.UserID) (networkid.UserID, bool) {
			if mxid == "@200_ghost:example.com" {
				return "200", true
			}
			return "", false
		},
		func(context.Context, id.UserID) (*bridgev2.User, error) {
			t.Fatal("getExistingUser should not be consulted once parseGhostMXID matched")
			return nil, nil
		},
		nil,
	)

	gaiaID, ok := resolve("@200_ghost:example.com")
	if !ok || gaiaID != "200" {
		t.Fatalf("resolve(ghost pill) = (%q, %v), want (200, true)", gaiaID, ok)
	}
}

func TestMatrixMentionResolver_OwnUserMXID(t *testing.T) {
	wantUser := &bridgev2.User{User: &database.User{MXID: "@alice:example.com"}}
	resolve := matrixMentionResolver(
		context.Background(),
		func(id.UserID) (networkid.UserID, bool) { return "", false }, // not a ghost
		func(_ context.Context, mxid id.UserID) (*bridgev2.User, error) {
			if mxid == "@alice:example.com" {
				return wantUser, nil
			}
			return nil, nil
		},
		func(_ context.Context, user *bridgev2.User) (*bridgev2.UserLogin, error) {
			if user == wantUser {
				return &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "100"}}, nil
			}
			return nil, nil
		},
	)

	gaiaID, ok := resolve("@alice:example.com")
	if !ok || gaiaID != "100" {
		t.Fatalf("resolve(own MXID) = (%q, %v), want (100, true)", gaiaID, ok)
	}
}

func TestMatrixMentionResolver_UnknownMXID(t *testing.T) {
	resolve := matrixMentionResolver(
		context.Background(),
		func(id.UserID) (networkid.UserID, bool) { return "", false },
		func(context.Context, id.UserID) (*bridgev2.User, error) { return nil, nil },
		func(context.Context, *bridgev2.User) (*bridgev2.UserLogin, error) { return nil, nil },
	)

	if gaiaID, ok := resolve("@stranger:elsewhere.example"); ok {
		t.Errorf("resolve(unknown MXID) = (%q, true), want ok=false", gaiaID)
	}
}

func TestMatrixMentionResolver_GetExistingUserError(t *testing.T) {
	resolve := matrixMentionResolver(
		context.Background(),
		func(id.UserID) (networkid.UserID, bool) { return "", false },
		func(context.Context, id.UserID) (*bridgev2.User, error) { return nil, errors.New("db error") },
		func(context.Context, *bridgev2.User) (*bridgev2.UserLogin, error) {
			t.Fatal("findPreferredLogin should not run after a GetExistingUser error")
			return nil, nil
		},
	)
	if _, ok := resolve("@x:example.com"); ok {
		t.Error("resolve should be ok=false when getExistingUser errors")
	}
}

func TestMatrixMentionResolver_GetExistingUserError_LogsError(t *testing.T) {
	var buf bytes.Buffer
	resolve := matrixMentionResolver(
		loggedCtx(&buf),
		func(id.UserID) (networkid.UserID, bool) { return "", false },
		func(context.Context, id.UserID) (*bridgev2.User, error) { return nil, errors.New("db error") },
		func(context.Context, *bridgev2.User) (*bridgev2.UserLogin, error) {
			t.Fatal("findPreferredLogin should not run after a GetExistingUser error")
			return nil, nil
		},
	)
	if _, ok := resolve("@x:example.com"); ok {
		t.Error("resolve should be ok=false when getExistingUser errors")
	}
	if !strings.Contains(buf.String(), "db error") {
		t.Errorf("log output = %q, want the DB error to be logged, not silently swallowed", buf.String())
	}
}

func TestMatrixMentionResolver_FindPreferredLoginError(t *testing.T) {
	resolve := matrixMentionResolver(
		context.Background(),
		func(id.UserID) (networkid.UserID, bool) { return "", false },
		func(context.Context, id.UserID) (*bridgev2.User, error) {
			return &bridgev2.User{User: &database.User{}}, nil
		},
		func(context.Context, *bridgev2.User) (*bridgev2.UserLogin, error) {
			return nil, bridgev2.ErrNotLoggedIn
		},
	)
	if _, ok := resolve("@x:example.com"); ok {
		t.Error("resolve should be ok=false when findPreferredLogin errors (e.g. not logged in)")
	}
}

func TestMatrixMentionResolver_FindPreferredLoginError_LogsError(t *testing.T) {
	var buf bytes.Buffer
	dbErr := errors.New("login lookup: connection reset")
	resolve := matrixMentionResolver(
		loggedCtx(&buf),
		func(id.UserID) (networkid.UserID, bool) { return "", false },
		func(context.Context, id.UserID) (*bridgev2.User, error) {
			return &bridgev2.User{User: &database.User{}}, nil
		},
		func(context.Context, *bridgev2.User) (*bridgev2.UserLogin, error) {
			return nil, dbErr
		},
	)
	if _, ok := resolve("@x:example.com"); ok {
		t.Error("resolve should be ok=false when findPreferredLogin errors")
	}
	if !strings.Contains(buf.String(), dbErr.Error()) {
		t.Errorf("log output = %q, want the genuine login-lookup error to be logged, not silently swallowed", buf.String())
	}
}

// TestMatrixMentionResolver_NotLoggedIn_NotLogged: ErrNotLoggedIn is expected and not warned.
func TestMatrixMentionResolver_NotLoggedIn_NotLogged(t *testing.T) {
	var buf bytes.Buffer
	resolve := matrixMentionResolver(
		loggedCtx(&buf),
		func(id.UserID) (networkid.UserID, bool) { return "", false },
		func(context.Context, id.UserID) (*bridgev2.User, error) {
			return &bridgev2.User{User: &database.User{}}, nil
		},
		func(context.Context, *bridgev2.User) (*bridgev2.UserLogin, error) {
			return nil, bridgev2.ErrNotLoggedIn
		},
	)
	if _, ok := resolve("@x:example.com"); ok {
		t.Error("resolve should be ok=false when the user is not logged in")
	}
	if buf.String() != "" {
		t.Errorf("log output = %q, want empty (ErrNotLoggedIn is expected, must not be logged)", buf.String())
	}
}

func TestMatrixMentionResolver_NilDeps(t *testing.T) {
	resolve := matrixMentionResolver(context.Background(), nil, nil, nil)
	if _, ok := resolve("@x:example.com"); ok {
		t.Error("resolve with nil deps should be ok=false, not panic")
	}
}

func TestMatrixMention_EndToEnd_GhostPillBecomesMentionAnnotation(t *testing.T) {
	resolve := matrixMentionResolver(
		context.Background(),
		func(mxid id.UserID) (networkid.UserID, bool) {
			if mxid == "@200_ghost:example.com" {
				return "200", true
			}
			return "", false
		},
		nil, nil,
	)
	content := &event.MessageEventContent{
		MsgType:       event.MsgText,
		Body:          "plain-text fallback",
		Format:        event.FormatHTML,
		FormattedBody: `Hi <a href="https://matrix.to/#/@200_ghost:example.com">Bob</a>!`,
	}

	text, anns := matrixfmt.Parse(context.Background(), content, resolve)
	if text != "Hi @Bob!" {
		t.Errorf("text = %q, want %q", text, "Hi @Bob!")
	}
	want := []*pb.Annotation{gchatfmt.MakeMentionAnnotation(3, 4, "200")}
	if len(anns) != 1 || anns[0].String() != want[0].String() {
		t.Errorf("annotations = %s, want %s", formatAnnotationsForTest(anns), formatAnnotationsForTest(want))
	}
}

func TestMatrixMention_EndToEnd_UnknownMXIDRendersPlainText(t *testing.T) {
	resolve := matrixMentionResolver(
		context.Background(),
		func(id.UserID) (networkid.UserID, bool) { return "", false },
		func(context.Context, id.UserID) (*bridgev2.User, error) { return nil, nil },
		func(context.Context, *bridgev2.User) (*bridgev2.UserLogin, error) { return nil, nil },
	)
	content := &event.MessageEventContent{
		MsgType:       event.MsgText,
		Body:          "plain-text fallback",
		Format:        event.FormatHTML,
		FormattedBody: `Hi <a href="https://matrix.to/#/@stranger:elsewhere.example">Stranger</a>!`,
	}

	text, anns := matrixfmt.Parse(context.Background(), content, resolve)
	if text != "Hi Stranger!" {
		t.Errorf("text = %q, want %q (no crash, plain text)", text, "Hi Stranger!")
	}
	if len(anns) != 0 {
		t.Errorf("annotations = %s, want none for an unresolvable pill", formatAnnotationsForTest(anns))
	}
}

func formatAnnotationsForTest(anns []*pb.Annotation) string {
	s := "["
	for i, a := range anns {
		if i > 0 {
			s += ", "
		}
		s += a.String()
	}
	return s + "]"
}

func TestNewInboundMentionResolver_NilPortal(t *testing.T) {
	resolve := newInboundMentionResolver(nil)
	if _, _, ok := resolve("200"); ok {
		t.Error("newInboundMentionResolver(nil) should yield an always-ok=false resolver, not panic")
	}
}

func TestNewInboundMentionResolver_NilBridge(t *testing.T) {
	portal := &bridgev2.Portal{Portal: &database.Portal{}}
	resolve := newInboundMentionResolver(portal)
	if _, _, ok := resolve("200"); ok {
		t.Error("newInboundMentionResolver(portal with nil Bridge) should be ok=false, not panic")
	}
}

func TestNewOutboundMentionResolver_NilPortal(t *testing.T) {
	resolve := newOutboundMentionResolver(context.Background(), nil)
	if _, ok := resolve("@x:example.com"); ok {
		t.Error("newOutboundMentionResolver(nil) should yield an always-ok=false resolver, not panic")
	}
}

func TestNewOutboundMentionResolver_NilBridge(t *testing.T) {
	portal := &bridgev2.Portal{Portal: &database.Portal{}}
	resolve := newOutboundMentionResolver(context.Background(), portal)
	if _, ok := resolve("@x:example.com"); ok {
		t.Error("newOutboundMentionResolver(portal with nil Bridge) should be ok=false, not panic")
	}
}

// Fake MatrixConnector/MatrixAPI overriding only the methods the resolver wiring calls.

type fakeMatrixAPI struct {
	bridgev2.MatrixAPI
	mxid id.UserID
}

func (f fakeMatrixAPI) GetMXID() id.UserID { return f.mxid }

type fakeMatrixConnector struct {
	bridgev2.MatrixConnector
	ghostIntent func(networkid.UserID) id.UserID
	// ghostIntentAPI, when set, overrides ghostIntent entirely.
	ghostIntentAPI func(networkid.UserID) bridgev2.MatrixAPI
	parseGhostMXID func(id.UserID) (networkid.UserID, bool)
}

func (f *fakeMatrixConnector) GhostIntent(userID networkid.UserID) bridgev2.MatrixAPI {
	if f.ghostIntentAPI != nil {
		return f.ghostIntentAPI(userID)
	}
	return fakeMatrixAPI{mxid: f.ghostIntent(userID)}
}

func (f *fakeMatrixConnector) ParseGhostMXID(mxid id.UserID) (networkid.UserID, bool) {
	return f.parseGhostMXID(mxid)
}

func TestNewInboundMentionResolver_RealWiring(t *testing.T) {
	matrix := &fakeMatrixConnector{
		ghostIntent: func(gaiaID networkid.UserID) id.UserID {
			return id.UserID("@" + string(gaiaID) + "_ghost:example.com")
		},
	}
	portal := &bridgev2.Portal{
		Portal: &database.Portal{},
		Bridge: &bridgev2.Bridge{Matrix: matrix},
	}

	resolve := newInboundMentionResolver(portal)
	mxid, _, ok := resolve("200")
	if !ok || mxid != "@200_ghost:example.com" {
		t.Errorf("resolve(200) = (%q, _, %v), want (@200_ghost:example.com, true)", mxid, ok)
	}
}

// panicMatrixAPI's GetMXID panics, like a GhostIntent wrapping a nil IntentAPI.
type panicMatrixAPI struct {
	bridgev2.MatrixAPI
}

func (panicMatrixAPI) GetMXID() id.UserID {
	panic("simulated: GetMXID on a MatrixAPI wrapping a nil intent")
}

func TestNewInboundMentionResolver_RecoversFromGhostIntentPanic(t *testing.T) {
	matrix := &fakeMatrixConnector{
		ghostIntentAPI: func(networkid.UserID) bridgev2.MatrixAPI { return panicMatrixAPI{} },
	}
	portal := &bridgev2.Portal{
		Portal: &database.Portal{},
		Bridge: &bridgev2.Bridge{Matrix: matrix},
	}

	resolve := newInboundMentionResolver(portal)
	mxid, _, ok := resolve("200")
	if ok || mxid != "" {
		t.Errorf("resolve(200) = (%q, _, %v), want ok=false after a recovered panic, not a crashed test", mxid, ok)
	}
}

func TestNewOutboundMentionResolver_RealWiring(t *testing.T) {
	matrix := &fakeMatrixConnector{
		parseGhostMXID: func(mxid id.UserID) (networkid.UserID, bool) {
			if mxid == "@200_ghost:example.com" {
				return "200", true
			}
			return "", false
		},
	}
	portal := &bridgev2.Portal{
		Portal: &database.Portal{},
		Bridge: &bridgev2.Bridge{Matrix: matrix},
	}

	resolve := newOutboundMentionResolver(context.Background(), portal)
	gaiaID, ok := resolve("@200_ghost:example.com")
	if !ok || gaiaID != "200" {
		t.Errorf("resolve(ghost pill) = (%q, %v), want (200, true)", gaiaID, ok)
	}
}
