package connector

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
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

// --- ValidateUserID -------------------------------------------------------

// TestValidateUserIDAcceptsGaiaIDsOnly pins the shape gate the framework uses
// before it will materialise a ghost row or register a ghost on the
// homeserver. The valid cases are the ones that would silently break if the
// predicate were tightened: "999" is this repo's own BOT fixture id, and
// "112233" is the acting login's own id, which must stay valid or the
// multi-login self-DM deferral is killed.
func TestValidateUserIDAcceptsGaiaIDsOnly(t *testing.T) {
	gc := &GChatConnector{}
	for _, valid := range []string{"0", "999", "112233", "123456789012345678901"} {
		if !gc.ValidateUserID(gcid.MakeUserID(valid)) {
			t.Errorf("ValidateUserID(%q) = false, want true", valid)
		}
	}
	for _, invalid := range []string{
		"", "notagaia", "112233abc", "112233:example.org",
		"@googlechat_112233", "11 22", " 112233", "112233 ", "+112233", "112233.0",
	} {
		if gc.ValidateUserID(gcid.MakeUserID(invalid)) {
			t.Errorf("ValidateUserID(%q) = true, want false", invalid)
		}
	}
}

// TestGChatConnectorImplementsIdentifierValidatingNetwork asserts through the
// NetworkConnector static type, which is exactly the assertion the framework
// performs. Without it, defining ValidateUserID on the wrong receiver compiles
// cleanly and does nothing.
func TestGChatConnectorImplementsIdentifierValidatingNetwork(t *testing.T) {
	var network bridgev2.NetworkConnector = &GChatConnector{}
	if _, ok := network.(bridgev2.IdentifierValidatingNetwork); !ok {
		t.Fatal("*GChatConnector does not satisfy bridgev2.IdentifierValidatingNetwork; " +
			"the framework looks this up on Bridge.Network, so a method on *GChatClient would never be called")
	}
}

// --- create_dm failure classification -------------------------------------

// TestCreateDMDefersToOtherLoginsOnlyForA400 pins which create_dm failures
// mean "try the user's other logins". A 400 is Google saying THIS account
// cannot open that DM, which says nothing about the others; a throttle, a
// server error or a dead connection must not be replayed once per login.
func TestCreateDMDefersToOtherLoginsOnlyForA400(t *testing.T) {
	tests := []struct {
		name    string
		failure error
		wantTry bool
	}{
		{
			name:    "400 is about this account's view of the target",
			failure: &gchatmeow.UnexpectedStatusError{Status: 400, Body: "INVALID_ARGUMENT"},
			wantTry: true,
		},
		{
			name:    "403 is this API family's quota signal",
			failure: &gchatmeow.UnexpectedStatusError{Status: 403},
			wantTry: false,
		},
		{
			name:    "404 shows up as a request-shape symptom",
			failure: &gchatmeow.UnexpectedStatusError{Status: 404},
			wantTry: false,
		},
		{
			name:    "429 would replay a throttle once per login",
			failure: &gchatmeow.UnexpectedStatusError{Status: 429},
			wantTry: false,
		},
		{
			name:    "500 is not about the target at all",
			failure: &gchatmeow.UnexpectedStatusError{Status: 500},
			wantTry: false,
		},
		{
			name:    "a retry-exhausted 500 stays undeferred through NetworkError",
			failure: &gchatmeow.NetworkError{Err: &gchatmeow.UnexpectedStatusError{Status: 500}},
			wantTry: false,
		},
		{
			name:    "an error carrying no status fails closed",
			failure: errors.New("connection reset"),
			wantTry: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gc := &GChatClient{
				UserLogin: newTestUserLogin(&UserLoginMetadata{}),
				createDmFn: func(context.Context, *pb.CreateDmRequest) (*pb.CreateDmResponse, error) {
					return nil, tc.failure
				},
			}
			_, err := gc.ResolveIdentifier(context.Background(), "778899", true)
			if err == nil {
				t.Fatal("ResolveIdentifier succeeded despite a failing create_dm")
			}
			if got := errors.Is(err, bridgev2.ErrResolveIdentifierTryNext); got != tc.wantTry {
				t.Errorf("errors.Is(err, ErrResolveIdentifierTryNext) = %v, want %v (err = %v)", got, tc.wantTry, err)
			}
		})
	}
}

// TestCreateDMErrorRemainsClassifiableByStatus pins the seam the
// classification rests on: the connector's own wrap must keep the gchatmeow
// status reachable. Changing the %w to a %v would silently disable every
// decision above.
func TestCreateDMErrorRemainsClassifiableByStatus(t *testing.T) {
	for _, tc := range []struct {
		name    string
		failure error
		want    int
	}{
		{"bare status error", &gchatmeow.UnexpectedStatusError{Status: 400}, 400},
		{"through NetworkError", &gchatmeow.NetworkError{Err: &gchatmeow.UnexpectedStatusError{Status: 500}}, 500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gc := &GChatClient{
				UserLogin: newTestUserLogin(&UserLoginMetadata{}),
				createDmFn: func(context.Context, *pb.CreateDmRequest) (*pb.CreateDmResponse, error) {
					return nil, tc.failure
				},
			}
			_, err := gc.ResolveIdentifier(context.Background(), "778899", true)
			var status *gchatmeow.UnexpectedStatusError
			if !errors.As(err, &status) {
				t.Fatalf("the wrapped error no longer exposes the HTTP status: %v", err)
			}
			if status.Status != tc.want {
				t.Errorf("Status = %d, want %d", status.Status, tc.want)
			}
		})
	}
}

// --- self-DM by email -----------------------------------------------------

// newLoginWithOwnEmail builds a login that has already learned its own address,
// which is what updateOwnLoginProfile does on connect.
func newLoginWithOwnEmail(email string) *bridgev2.UserLogin {
	login := newTestUserLogin(&UserLoginMetadata{})
	login.RemoteProfile.Email = email
	return login
}

// TestResolveIdentifierOwnEmailExplainsA400: Google refuses a self-DM with a
// bare 400 that says nothing. The acting login's own address is known, so the
// cause can be named -- but only once the server has actually refused.
func TestResolveIdentifierOwnEmailExplainsA400(t *testing.T) {
	sent := false
	gc := &GChatClient{
		UserLogin: newLoginWithOwnEmail("ada@example.com"),
		createDmFn: func(context.Context, *pb.CreateDmRequest) (*pb.CreateDmResponse, error) {
			sent = true
			return nil, &gchatmeow.UnexpectedStatusError{Status: 400}
		},
	}
	_, err := gc.ResolveIdentifier(context.Background(), "ADA@Example.com", true)
	if !errors.Is(err, ErrCannotDMYourself) {
		t.Fatalf("error = %v, want ErrCannotDMYourself", err)
	}
	if !errors.Is(err, bridgev2.ErrResolveIdentifierTryNext) {
		t.Error("the self-DM answer must still let bridgev2 try the user's other logins")
	}
	if !sent {
		t.Error("create_dm was never sent: the check must classify the server's refusal, not pre-empt it")
	}
}

// TestResolveIdentifierOwnEmailDoesNotPreemptASuccess is the regression guard
// for the tempting version of this fix. Nothing establishes that Google
// refuses a self-DM -- Chat has a note-to-self conversation -- so a local
// refusal could block something that works.
func TestResolveIdentifierOwnEmailDoesNotPreemptASuccess(t *testing.T) {
	gc := &GChatClient{
		UserLogin: newLoginWithOwnEmail("ada@example.com"),
		createDmFn: func(context.Context, *pb.CreateDmRequest) (*pb.CreateDmResponse, error) {
			return dmResponse("dm-self"), nil
		},
	}
	resp, err := gc.ResolveIdentifier(context.Background(), "ada@example.com", true)
	if err != nil {
		t.Fatalf("ResolveIdentifier refused an address the server accepted: %v", err)
	}
	if resp.Chat == nil {
		t.Fatal("Chat = nil, want the DM the server created")
	}
}

// TestResolveIdentifierOwnEmailOnlyClaimsSelfDMForA400: a transport failure or
// a server error must not be relabelled "that is your own account".
func TestResolveIdentifierOwnEmailOnlyClaimsSelfDMForA400(t *testing.T) {
	for _, failure := range []error{
		&gchatmeow.UnexpectedStatusError{Status: 500},
		&gchatmeow.UnexpectedStatusError{Status: 429},
		errors.New("connection reset"),
	} {
		gc := &GChatClient{
			UserLogin: newLoginWithOwnEmail("ada@example.com"),
			createDmFn: func(context.Context, *pb.CreateDmRequest) (*pb.CreateDmResponse, error) {
				return nil, failure
			},
		}
		_, err := gc.ResolveIdentifier(context.Background(), "ada@example.com", true)
		if errors.Is(err, ErrCannotDMYourself) {
			t.Errorf("a %v failure was reported as a self-DM", failure)
		}
	}
}

// TestResolveIdentifierOtherEmailIsNotSelf: a 400 for somebody ELSE's address
// keeps the server's own explanation instead of being blamed on the user.
func TestResolveIdentifierOtherEmailIsNotSelf(t *testing.T) {
	gc := &GChatClient{
		UserLogin: newLoginWithOwnEmail("ada@example.com"),
		createDmFn: func(context.Context, *pb.CreateDmRequest) (*pb.CreateDmResponse, error) {
			return nil, &gchatmeow.UnexpectedStatusError{Status: 400, Body: "INVALID_ARGUMENT"}
		},
	}
	_, err := gc.ResolveIdentifier(context.Background(), "grace@example.com", true)
	if errors.Is(err, ErrCannotDMYourself) {
		t.Fatalf("a stranger's address was reported as the user's own: %v", err)
	}
	if !strings.Contains(err.Error(), "create_dm failed") {
		t.Errorf("error = %v, want the create_dm failure to survive", err)
	}
}

// TestResolveIdentifierUnknownOwnEmailChangesNothing: until the login has
// connected and learned its own address, every email behaves exactly as it did
// before. An unknown address must never be treated as a match.
func TestResolveIdentifierUnknownOwnEmailChangesNothing(t *testing.T) {
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}), // RemoteProfile.Email == ""
		createDmFn: func(context.Context, *pb.CreateDmRequest) (*pb.CreateDmResponse, error) {
			return nil, &gchatmeow.UnexpectedStatusError{Status: 400}
		},
	}
	_, err := gc.ResolveIdentifier(context.Background(), "someone@example.com", true)
	if errors.Is(err, ErrCannotDMYourself) {
		t.Fatalf("an unknown own-address was treated as a match: %v", err)
	}
}

// TestIsOwnEmailDoesNotNormaliseAliases pins the deliberate non-normalisation.
// Dot/plus folding is a @gmail.com rule; on a Workspace domain first.last@ and
// firstlast@ can be two different colleagues, so folding them would mislabel a
// real person's address as the user's own.
func TestIsOwnEmailDoesNotNormaliseAliases(t *testing.T) {
	gc := &GChatClient{UserLogin: newLoginWithOwnEmail("foo.bar@gmail.com")}
	for _, match := range []string{"foo.bar@gmail.com", "FOO.BAR@GMAIL.COM", "Foo.Bar@Gmail.com"} {
		if !gc.isOwnEmail(match) {
			t.Errorf("isOwnEmail(%q) = false, want true (case must fold)", match)
		}
	}
	for _, notMatch := range []string{"foobar@gmail.com", "foo.bar+work@gmail.com", "foo.bar@example.com", ""} {
		if gc.isOwnEmail(notMatch) {
			t.Errorf("isOwnEmail(%q) = true; aliases are deliberately not normalised", notMatch)
		}
	}
}

// --- otherMember ----------------------------------------------------------

// TestOtherMemberPicksThePeer pins the rule the email branch depends on: the
// response's membership list is the only place the peer's gaia id ever
// appears, and an entry that resolves to nothing must be skipped rather than
// returned as an empty id.
func TestOtherMemberPicksThePeer(t *testing.T) {
	gc := &GChatClient{UserLogin: newTestUserLogin(&UserLoginMetadata{})} // own id 112233
	membership := func(gaia string) *pb.Membership {
		return &pb.Membership{Id: &pb.MembershipId{MemberId: &pb.MemberId{
			Id: &pb.MemberId_UserId{UserId: &pb.UserId{Id: proto.String(gaia)}},
		}}}
	}
	tests := []struct {
		name        string
		memberships []*pb.Membership
		want        networkid.UserID
	}{
		{"no memberships", nil, ""},
		{"only this login", []*pb.Membership{membership("112233")}, ""},
		{
			name:        "a membership naming nobody is skipped, not returned empty",
			memberships: []*pb.Membership{{Id: &pb.MembershipId{MemberId: &pb.MemberId{}}}},
			want:        "",
		},
		{"peer after self", []*pb.Membership{membership("112233"), membership("998877")}, gcid.MakeUserID("998877")},
		{"peer before self", []*pb.Membership{membership("998877"), membership("112233")}, gcid.MakeUserID("998877")},
		{
			name:        "an unresolvable entry does not mask a real peer",
			memberships: []*pb.Membership{{Id: &pb.MembershipId{MemberId: &pb.MemberId{}}}, membership("998877")},
			want:        gcid.MakeUserID("998877"),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := gc.otherMember(tc.memberships); got != tc.want {
				t.Errorf("otherMember = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestResolveIdentifierKeepsADMWhoseMembersAreUnknown: when the server created
// the DM but the response named nobody, the chat must still come back. The
// portal resolves its own membership afterwards; discarding a real
// conversation over a missing echo field would be far worse.
func TestResolveIdentifierKeepsADMWhoseMembersAreUnknown(t *testing.T) {
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		createDmFn: func(context.Context, *pb.CreateDmRequest) (*pb.CreateDmResponse, error) {
			return dmResponse("dm5"), nil // no memberships at all
		},
	}
	resp, err := gc.ResolveIdentifier(context.Background(), "someone@example.com", true)
	if err != nil {
		t.Fatalf("ResolveIdentifier: %v", err)
	}
	if resp.Chat == nil {
		t.Fatal("Chat = nil: a DM the server actually created was discarded")
	}
	want := gcid.MakePortalKey(gcid.GroupID{ID: "dm5", IsDM: true}, gc.UserLogin.ID)
	if resp.Chat.PortalKey != want {
		t.Errorf("PortalKey = %+v, want %+v", resp.Chat.PortalKey, want)
	}
}
