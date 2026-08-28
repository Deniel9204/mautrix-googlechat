package connector

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
	"strings"
)

func dmResponse(dmID string, memberGaias ...string) *pb.CreateDmResponse {
	resp := &pb.CreateDmResponse{
		Dm: &pb.Group{GroupId: &pb.GroupId{
			Id: &pb.GroupId_DmId{DmId: &pb.DmId{DmId: proto.String(dmID)}},
		}},
	}
	for _, gaia := range memberGaias {
		resp.Memberships = append(resp.Memberships, &pb.Membership{
			Id: &pb.MembershipId{MemberId: &pb.MemberId{
				Id: &pb.MemberId_UserId{UserId: &pb.UserId{Id: proto.String(gaia)}},
			}},
		})
	}
	return resp
}

// --- ResolveIdentifier ----------------------------------------------------

// TestResolveIdentifierGaiaWithoutCreatingMakesNoRequest: a gaia id is
// self-describing, so resolving one must not cost a round trip -- and must
// certainly not open a conversation the caller did not ask for.
func TestResolveIdentifierGaiaWithoutCreatingMakesNoRequest(t *testing.T) {
	called := false
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		createDmFn: func(context.Context, *pb.CreateDmRequest) (*pb.CreateDmResponse, error) {
			called = true
			return dmResponse("dm1"), nil
		},
	}

	// Deliberately NOT the acting login's own id (112233): that is a self-DM
	// and is refused separately, see TestResolveIdentifierRejectsOwnAccount.
	resp, err := gc.ResolveIdentifier(context.Background(), "778899", false)
	if err != nil {
		t.Fatalf("ResolveIdentifier: %v", err)
	}
	if resp.UserID != gcid.MakeUserID("778899") {
		t.Errorf("UserID = %q, want %q", resp.UserID, gcid.MakeUserID("778899"))
	}
	if resp.Chat != nil {
		t.Error("a resolve-only call returned a chat")
	}
	if called {
		t.Error("create_dm was sent for a resolve-only call")
	}
}

func TestResolveIdentifierGaiaWithCreateOpensDM(t *testing.T) {
	var gotReq *pb.CreateDmRequest
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		createDmFn: func(_ context.Context, req *pb.CreateDmRequest) (*pb.CreateDmResponse, error) {
			gotReq = req
			return dmResponse("dm1", "112233"), nil
		},
	}

	resp, err := gc.ResolveIdentifier(context.Background(), "445566", true)
	if err != nil {
		t.Fatalf("ResolveIdentifier: %v", err)
	}
	if len(gotReq.GetMembers()) != 1 || gotReq.GetMembers()[0].GetId() != "445566" {
		t.Errorf("members = %v, want the gaia id in members (not invitees)", gotReq.GetMembers())
	}
	if len(gotReq.GetInvitees()) != 0 {
		t.Errorf("invitees = %v, want none for a known gaia id", gotReq.GetInvitees())
	}
	if resp.Chat == nil {
		t.Fatal("Chat = nil, want the created DM")
	}
	wantKey := gcid.MakePortalKey(gcid.GroupID{ID: "dm1", IsDM: true}, gc.UserLogin.ID)
	if resp.Chat.PortalKey != wantKey {
		t.Errorf("PortalKey = %+v, want %+v", resp.Chat.PortalKey, wantKey)
	}
}

// TestResolveIdentifierEmailWithoutCreatingIsRefused: there is no
// email-to-gaia lookup, so the only way to answer would be to open the DM.
// Doing that silently, for a call that merely asked to resolve, would create
// a conversation the user never requested.
func TestResolveIdentifierEmailWithoutCreatingIsRefused(t *testing.T) {
	called := false
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		createDmFn: func(context.Context, *pb.CreateDmRequest) (*pb.CreateDmResponse, error) {
			called = true
			return dmResponse("dm1"), nil
		},
	}

	_, err := gc.ResolveIdentifier(context.Background(), "someone@example.com", false)
	if !errors.Is(err, ErrCannotResolveEmailWithoutCreating) {
		t.Fatalf("error = %v, want ErrCannotResolveEmailWithoutCreating", err)
	}
	if called {
		t.Error("create_dm was sent for a resolve-only call on an email")
	}
}

// TestResolveIdentifierEmailWithCreateUsesInviteesAndLearnsGaia: the email
// goes in invitees (members only takes gaia ids), and the response's own
// membership list is what finally reveals who the address belonged to.
func TestResolveIdentifierEmailWithCreateUsesInviteesAndLearnsGaia(t *testing.T) {
	const selfGaia = "112233" // matches newTestUserLogin's id
	var gotReq *pb.CreateDmRequest
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		createDmFn: func(_ context.Context, req *pb.CreateDmRequest) (*pb.CreateDmResponse, error) {
			gotReq = req
			return dmResponse("dm7", selfGaia, "998877"), nil
		},
	}

	resp, err := gc.ResolveIdentifier(context.Background(), "mailto:someone@example.com", true)
	if err != nil {
		t.Fatalf("ResolveIdentifier: %v", err)
	}
	if len(gotReq.GetInvitees()) != 1 || gotReq.GetInvitees()[0].GetEmail() != "someone@example.com" {
		t.Errorf("invitees = %v, want the mailto:-stripped address", gotReq.GetInvitees())
	}
	if len(gotReq.GetMembers()) != 0 {
		t.Errorf("members = %v, want none for an email", gotReq.GetMembers())
	}
	if want := gcid.MakeUserID("998877"); resp.UserID != want {
		t.Errorf("UserID = %q, want %q (the member who is not this login)", resp.UserID, want)
	}
}

func TestResolveIdentifierRejectsGarbage(t *testing.T) {
	gc := &GChatClient{UserLogin: newTestUserLogin(&UserLoginMetadata{})}
	for _, id := range []string{"", "   ", "not-an-email-or-id"} {
		if _, err := gc.ResolveIdentifier(context.Background(), id, true); err == nil {
			t.Errorf("ResolveIdentifier(%q) = nil error, want a rejection", id)
		}
	}
}

// --- CreateChatWithGhost --------------------------------------------------

func TestCreateChatWithGhostUsesGhostIDAsGaia(t *testing.T) {
	var gotReq *pb.CreateDmRequest
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		createDmFn: func(_ context.Context, req *pb.CreateDmRequest) (*pb.CreateDmResponse, error) {
			gotReq = req
			return dmResponse("dm2"), nil
		},
	}

	resp, err := gc.CreateChatWithGhost(context.Background(), ghostWithID("778899"))
	if err != nil {
		t.Fatalf("CreateChatWithGhost: %v", err)
	}
	if len(gotReq.GetMembers()) != 1 || gotReq.GetMembers()[0].GetId() != "778899" {
		t.Errorf("members = %v, want the ghost's id used directly as the gaia id", gotReq.GetMembers())
	}
	wantKey := gcid.MakePortalKey(gcid.GroupID{ID: "dm2", IsDM: true}, gc.UserLogin.ID)
	if resp.PortalKey != wantKey {
		t.Errorf("PortalKey = %+v, want %+v", resp.PortalKey, wantKey)
	}
}

func TestCreateChatWithGhostRejectsUnidentified(t *testing.T) {
	gc := &GChatClient{UserLogin: newTestUserLogin(&UserLoginMetadata{})}
	if _, err := gc.CreateChatWithGhost(context.Background(), nil); err == nil {
		t.Error("CreateChatWithGhost(nil) = nil error, want a rejection")
	}
}

// ghostWithID builds the minimal ghost CreateChatWithGhost needs: its id,
// which IS the gaia id (gcid.MakeUserID is an identity mapping).
func ghostWithID(gaia string) *bridgev2.Ghost {
	return &bridgev2.Ghost{Ghost: &database.Ghost{ID: gcid.MakeUserID(gaia)}}
}

// TestResolveIdentifierRejectsWhitespaceIdentifier: `start-chat`'s optional
// first argument is a LOGIN ID, and bridgev2 folds it back into the
// identifier when it is not a recognised one (commands/startchat.go's
// getClientForStartingChat). So `start-chat me@x.com them@y.com` arrives here
// as the single string "me@x.com them@y.com", which used to be shipped
// straight to Google and come back as a bare 500 with no hint about what was
// wrong. Reject it locally with an explanation instead.
func TestResolveIdentifierRejectsWhitespaceIdentifier(t *testing.T) {
	for _, identifier := range []string{
		"alice@example.com bob@example.com", // the real-world case: two emails
		"445566778899 bob@example.com",
		"me@example.com\tthem@example.com",
	} {
		t.Run(identifier, func(t *testing.T) {
			called := false
			gc := &GChatClient{
				UserLogin: newTestUserLogin(&UserLoginMetadata{}),
				createDmFn: func(context.Context, *pb.CreateDmRequest) (*pb.CreateDmResponse, error) {
					called = true
					return dmResponse("dm1"), nil
				},
			}

			_, err := gc.ResolveIdentifier(context.Background(), identifier, true)
			if !errors.Is(err, ErrIdentifierNotSingle) {
				t.Fatalf("ResolveIdentifier(%q) error = %v, want ErrIdentifierNotSingle", identifier, err)
			}
			if called {
				t.Error("create_dm was sent for a multi-part identifier")
			}
		})
	}
}

// TestResolveIdentifierAcceptsSurroundingWhitespace: only INTERNAL whitespace
// signals two identifiers; padding is just sloppy typing and is trimmed.
func TestResolveIdentifierAcceptsSurroundingWhitespace(t *testing.T) {
	var gotReq *pb.CreateDmRequest
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		createDmFn: func(_ context.Context, req *pb.CreateDmRequest) (*pb.CreateDmResponse, error) {
			gotReq = req
			return dmResponse("dm1", "112233", "998877"), nil
		},
	}

	if _, err := gc.ResolveIdentifier(context.Background(), "  someone@example.com  ", true); err != nil {
		t.Fatalf("ResolveIdentifier: %v", err)
	}
	if got := gotReq.GetInvitees()[0].GetEmail(); got != "someone@example.com" {
		t.Errorf("invitee email = %q, want it trimmed", got)
	}
}

// TestResolveIdentifierRejectsOwnAccount: Google Chat has no self-DM through
// create_dm -- it answers with a bare 400. Observed live: `start-chat
// <own-login-id> <own-email>` produced exactly that, with nothing to tell the
// user they had simply addressed themselves.
func TestResolveIdentifierRejectsOwnAccount(t *testing.T) {
	called := false
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		createDmFn: func(context.Context, *pb.CreateDmRequest) (*pb.CreateDmResponse, error) {
			called = true
			return dmResponse("dm1"), nil
		},
	}
	own := string(gc.UserLogin.ID)

	if _, err := gc.ResolveIdentifier(context.Background(), own, true); !errors.Is(err, ErrCannotDMYourself) {
		t.Fatalf("ResolveIdentifier(own id) error = %v, want ErrCannotDMYourself", err)
	}
	if called {
		t.Error("create_dm was sent for the acting account's own id")
	}
}

// TestResolveIdentifierRejectsOwnAccountEvenWithoutCreating: resolving is
// harmless, but answering "yes, that's a user you can chat with" about
// yourself would just set up the failure one step later.
func TestResolveIdentifierRejectsOwnAccountWithoutCreating(t *testing.T) {
	gc := &GChatClient{UserLogin: newTestUserLogin(&UserLoginMetadata{})}
	own := string(gc.UserLogin.ID)

	if _, err := gc.ResolveIdentifier(context.Background(), own, false); !errors.Is(err, ErrCannotDMYourself) {
		t.Fatalf("ResolveIdentifier(own id, resolve-only) error = %v, want ErrCannotDMYourself", err)
	}
}

func TestCreateChatWithGhostRejectsOwnAccount(t *testing.T) {
	called := false
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		createDmFn: func(context.Context, *pb.CreateDmRequest) (*pb.CreateDmResponse, error) {
			called = true
			return dmResponse("dm1"), nil
		},
	}
	self := ghostWithID(string(gc.UserLogin.ID))

	if _, err := gc.CreateChatWithGhost(context.Background(), self); !errors.Is(err, ErrCannotDMYourself) {
		t.Fatalf("CreateChatWithGhost(own ghost) error = %v, want ErrCannotDMYourself", err)
	}
	if called {
		t.Error("create_dm was sent for the acting account's own ghost")
	}
}

// TestCreateDMFailureNamesLikelyCauses: an email cannot be checked against the
// acting account locally (there is no email-to-gaia lookup), so a self-DM by
// EMAIL still reaches the server and comes back as a bare 400. The wrapped
// error should at least name the causes worth checking, since the raw status
// says nothing.
func TestCreateDMFailureNamesLikelyCauses(t *testing.T) {
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		createDmFn: func(context.Context, *pb.CreateDmRequest) (*pb.CreateDmResponse, error) {
			return nil, errors.New("unexpected status 400")
		},
	}

	_, err := gc.ResolveIdentifier(context.Background(), "someone@example.com", true)
	if err == nil {
		t.Fatal("ResolveIdentifier = nil error, want the create_dm failure surfaced")
	}
	for _, want := range []string{"400", "your own account"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err.Error(), want)
		}
	}
}

// TestResolveIdentifierOwnAccountDefersToOtherLogin: refusing a self-DM must
// not be TERMINAL. A gaia id that is this login's own may still be a
// perfectly ordinary DM target for one of the user's OTHER logins -- two
// accounts bridged by the same Matrix user can legitimately DM each other.
// bridgev2 only tries the next login when the error wraps
// ErrResolveIdentifierTryNext, which the framework documents for exactly this
// case ("trying to resolve another login's user ID").
func TestResolveIdentifierOwnAccountDefersToOtherLogin(t *testing.T) {
	gc := &GChatClient{UserLogin: newTestUserLogin(&UserLoginMetadata{})}
	own := string(gc.UserLogin.ID)

	_, err := gc.ResolveIdentifier(context.Background(), own, true)
	if !errors.Is(err, bridgev2.ErrResolveIdentifierTryNext) {
		t.Fatalf("error = %v, want it to wrap ErrResolveIdentifierTryNext so the other login is tried", err)
	}
	// The explanation must survive too: when there is no other login, this is
	// the text the user actually reads.
	if !errors.Is(err, ErrCannotDMYourself) {
		t.Errorf("error = %v, want it to still identify the self-DM cause", err)
	}
}

func TestCreateChatWithGhostOwnAccountDefersToOtherLogin(t *testing.T) {
	gc := &GChatClient{UserLogin: newTestUserLogin(&UserLoginMetadata{})}
	self := ghostWithID(string(gc.UserLogin.ID))

	_, err := gc.CreateChatWithGhost(context.Background(), self)
	if !errors.Is(err, bridgev2.ErrResolveIdentifierTryNext) {
		t.Fatalf("error = %v, want it to wrap ErrResolveIdentifierTryNext", err)
	}
	if !errors.Is(err, ErrCannotDMYourself) {
		t.Errorf("error = %v, want it to still identify the self-DM cause", err)
	}
}

// TestResolveIdentifierEmptyIdentifierExplainsArgFolding: on Google Chat a
// login ID *is* a gaia id, so `start-chat <your-own-login-id>` has its only
// argument consumed as the login selector and arrives here empty. "empty
// identifier" describes neither the cause nor the workaround.
func TestResolveIdentifierEmptyIdentifierExplainsArgFolding(t *testing.T) {
	gc := &GChatClient{UserLogin: newTestUserLogin(&UserLoginMetadata{})}

	_, err := gc.ResolveIdentifier(context.Background(), "   ", true)
	if !errors.Is(err, ErrIdentifierMissing) {
		t.Fatalf("error = %v, want ErrIdentifierMissing", err)
	}
	if !strings.Contains(err.Error(), "login ID") {
		t.Errorf("error %q should explain that the first argument is taken as a login ID", err.Error())
	}
}

// TestResolveIdentifierRejectsNonGoogleChatIdentifiers: "contains @" was the
// only test for an email, so a Matrix ID or a comma-joined pair sailed
// through to create_dm and came back as an opaque server error.
func TestResolveIdentifierRejectsNonGoogleChatIdentifiers(t *testing.T) {
	cases := []struct {
		identifier string
		want       error
	}{
		{"@someone:example.org", ErrNotAGoogleChatIdentifier},
		{"https://matrix.to/#/@someone:example.org", ErrNotAGoogleChatIdentifier},
		{"alice@example.com,bob@example.com", ErrIdentifierNotSingle},
		{"alice@example.com;bob@example.com", ErrIdentifierNotSingle},
	}
	for _, tc := range cases {
		t.Run(tc.identifier, func(t *testing.T) {
			called := false
			gc := &GChatClient{
				UserLogin: newTestUserLogin(&UserLoginMetadata{}),
				createDmFn: func(context.Context, *pb.CreateDmRequest) (*pb.CreateDmResponse, error) {
					called = true
					return dmResponse("dm1"), nil
				},
			}
			if _, err := gc.ResolveIdentifier(context.Background(), tc.identifier, true); !errors.Is(err, tc.want) {
				t.Fatalf("ResolveIdentifier(%q) error = %v, want %v", tc.identifier, err, tc.want)
			}
			if called {
				t.Errorf("create_dm was sent for %q", tc.identifier)
			}
		})
	}
}
