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

func TestResolveIdentifierGaiaWithoutCreatingMakesNoRequest(t *testing.T) {
	called := false
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		createDmFn: func(context.Context, *pb.CreateDmRequest) (*pb.CreateDmResponse, error) {
			called = true
			return dmResponse("dm1"), nil
		},
	}

	// Not the acting login's own id (112233), which is a self-DM.
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

// TestResolveIdentifierEmailWithCreateUsesInviteesAndLearnsGaia: members only takes gaia ids.
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

func ghostWithID(gaia string) *bridgev2.Ghost {
	return &bridgev2.Ghost{Ghost: &database.Ghost{ID: gcid.MakeUserID(gaia)}}
}

// TestResolveIdentifierRejectsWhitespaceIdentifier: bridgev2 folds an unrecognised login ID into the identifier.
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

// TestResolveIdentifierOwnAccountDefersToOtherLogin: another login may legitimately DM this account.
func TestResolveIdentifierOwnAccountDefersToOtherLogin(t *testing.T) {
	gc := &GChatClient{UserLogin: newTestUserLogin(&UserLoginMetadata{})}
	own := string(gc.UserLogin.ID)

	_, err := gc.ResolveIdentifier(context.Background(), own, true)
	if !errors.Is(err, bridgev2.ErrResolveIdentifierTryNext) {
		t.Fatalf("error = %v, want it to wrap ErrResolveIdentifierTryNext so the other login is tried", err)
	}
	// When there is no other login, this is the text the user reads.
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

// TestValidateUserIDAcceptsGaiaIDsOnly: "112233" (own login id) must stay valid for the self-DM deferral.
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

// TestGChatConnectorImplementsIdentifierValidatingNetwork: ValidateUserID on the wrong receiver compiles and does nothing.
func TestGChatConnectorImplementsIdentifierValidatingNetwork(t *testing.T) {
	var network bridgev2.NetworkConnector = &GChatConnector{}
	if _, ok := network.(bridgev2.IdentifierValidatingNetwork); !ok {
		t.Fatal("*GChatConnector does not satisfy bridgev2.IdentifierValidatingNetwork; " +
			"the framework looks this up on Bridge.Network, so a method on *GChatClient would never be called")
	}
}

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

// TestCreateDMErrorRemainsClassifiableByStatus: the wrap must use %w, not %v.
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

func newLoginWithOwnEmail(email string) *bridgev2.UserLogin {
	login := newTestUserLogin(&UserLoginMetadata{})
	login.RemoteProfile.Email = email
	return login
}

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

// TestResolveIdentifierOwnEmailDoesNotPreemptASuccess: Google may allow a self-DM (note-to-self).
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

// TestIsOwnEmailDoesNotNormaliseAliases: dot/plus folding is a @gmail.com-only rule.
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
			// The skip itself is pinned by the sibling case below.
			name:        "a lone unresolvable entry yields no peer",
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
