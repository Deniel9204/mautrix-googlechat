package connector

import (
	"context"
	"errors"
	"testing"

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

// --- gchatMentionResolver (GC gaia id -> Matrix mention target) -----------

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
	// Ghost mentions leave name "" -- gchatfmt falls back to entity_text
	// (the original "@Name" text already in the message), matching
	// from_googlechat.py's unconditional mention_text = entity_text default
	// for the non-double-puppet branch (see mentions.go's package doc
	// comment).
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
	// Defensive edge case: a UserLogin row with no UserMXID set yet should
	// not short-circuit resolution with an empty pill target.
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

// --- mentionsFromParsed (ParsedMentions -> content.Mentions) --------------
// The chip-filter / bounds / dedup / MENTION_ALL logic itself now lives in
// gchatfmt.collectMentions (exercised via gchatfmt.Parse's 3rd return in
// pkg/msgconv/gchatfmt/convert_test.go); mentionsFromParsed is the thin
// ParsedMentions -> *event.Mentions adapter, so its tests focus on that
// mapping. The end-to-end tests further down prove the whole
// resolver+Parse+mentionsFromParsed chain agrees with the rendered pills.

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

// --- cloneMentions ----------------------------------------------------

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

// --- End-to-end: gchatMentionResolver + gchatfmt.Parse + mentionsFromParsed
// (the brief's headline requirement: "GC mention gaia -> correct ghost MXID
// pill + content.Mentions populated" -- now from the SAME rendered-mention
// output, so the pill and the ping can never disagree).

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

// TestGChatMention_EndToEnd_PhantomPingPrevented is the headline regression
// test for this fix: a mention annotation whose span is out of bounds (no
// corresponding "@Name" text in the body) forces gchatfmt to fall back to
// plain text AND must NOT ping the user -- previously the connector's
// independent annotation walk (inboundMentions) had no bounds gate and would
// have pinged them regardless (a phantom notification).
func TestGChatMention_EndToEnd_PhantomPingPrevented(t *testing.T) {
	resolve := gchatMentionResolver(
		func(networkid.UserID) id.UserID { return "@200_ghost:example.com" },
		nil,
	)
	// Body is 2 UTF-16 units ("hi"), but the mention claims [0,50): out of
	// bounds -> no pill rendered, no reference to the user in the body.
	anns := []*pb.Annotation{gchatfmt.MakeMentionAnnotation(0, 50, "200")}

	_, html, parsed := gchatfmt.Parse(context.Background(), "hi", anns, resolve)
	if html != "" {
		t.Errorf("html = %q, want empty (out-of-bounds annotation -> plain-text fallback)", html)
	}
	if got := mentionsFromParsed(parsed); got != nil {
		t.Errorf("content.Mentions = %+v, want nil -- a mention with no body text must not ping (phantom ping)", got)
	}
}

// TestGChatMention_EndToEnd_ValidMentionStillPingsDespiteUnrelatedMalformedAnnotation
// pins the subtle case: a VALID mention alongside an UNRELATED malformed
// FORMAT annotation. The malformed annotation forces the whole HTML render
// to fall back to plain text, but the plain body still literally contains
// the mention's "@Bob" text, so pinging Bob is correct, not phantom --
// mentions are keyed on each mention annotation's own validity, independent
// of overall render success.
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
		// A malformed FORMAT annotation (huge length, out of bounds) that
		// makes renderAnnotations error -> whole message falls back to plain.
		gchatfmt.MakeFormatAnnotation(0, 2147483647, pb.FormatMetadata_BOLD),
		// A perfectly valid, resolvable mention over the real "@Bob" text.
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

// --- matrixMentionResolver (Matrix pill MXID -> GC gaia id) ---------------

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

func TestMatrixMentionResolver_NilDeps(t *testing.T) {
	resolve := matrixMentionResolver(context.Background(), nil, nil, nil)
	if _, ok := resolve("@x:example.com"); ok {
		t.Error("resolve with nil deps should be ok=false, not panic")
	}
}

// --- End-to-end: matrixMentionResolver + matrixfmt.Parse ------------------
// (the brief's "Matrix pill MXID (a bridge ghost) -> correct gaia MENTION")

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

// --- newInboundMentionResolver / newOutboundMentionResolver nil-safety ---
// (portal/Bridge/Matrix not fully wired -- bare test fixtures, matching
// this package's established "often nil in tests" pattern, client.go)

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

// --- newInboundMentionResolver / newOutboundMentionResolver real wiring --
// A minimal fake bridgev2.MatrixConnector/MatrixAPI (embedding the real
// interfaces as nil and overriding only the methods these two functions
// actually call) proves the wiring closures call the right
// bridgev2.MatrixConnector methods with the right arguments, without
// needing a live database (GetCachedUserLoginByID reads only bridgev2.Bridge's
// unexported-but-nil-safe cache map; ParseGhostMXID is pure).

type fakeMatrixAPI struct {
	bridgev2.MatrixAPI
	mxid id.UserID
}

func (f fakeMatrixAPI) GetMXID() id.UserID { return f.mxid }

type fakeMatrixConnector struct {
	bridgev2.MatrixConnector
	ghostIntent func(networkid.UserID) id.UserID
	// ghostIntentAPI, when set, overrides ghostIntent entirely and lets a
	// test hand back an arbitrary bridgev2.MatrixAPI -- used by
	// TestNewInboundMentionResolver_RecoversFromGhostIntentPanic to inject
	// a GetMXID() that panics, the way a real ASIntent{Matrix: nil} would.
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

// panicMatrixAPI simulates the theoretical bridgev2 edge case flagged by
// the M3 Task 3 port audit: GhostIntent(id) can return a non-nil MatrixAPI
// wrapping a nil *appservice.IntentAPI (e.g. a malformed/empty encoded
// ghost localpart), whose GetMXID() then nil-pointer-panics. This runs on
// every live message conversion, so newInboundMentionResolver must degrade
// to "no pill for this mention" rather than crash the whole conversion.
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
