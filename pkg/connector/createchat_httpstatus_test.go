package connector

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/matrix"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

func renderError(t *testing.T, err error) (status int, errcode string, message string) {
	t.Helper()
	rec := httptest.NewRecorder()
	matrix.RespondWithError(rec, err, "Internal error resolving identifier")
	var body struct {
		ErrCode string `json:"errcode"`
		Error   string `json:"error"`
	}
	if jsonErr := json.Unmarshal(rec.Body.Bytes(), &body); jsonErr != nil {
		t.Fatalf("response body %q is not JSON: %v", rec.Body.String(), jsonErr)
	}
	return rec.Code, body.ErrCode, body.Error
}

// clientRejectingEverything's create_dm would succeed, so any error comes from validation.
func clientRejectingEverything(t *testing.T) *GChatClient {
	t.Helper()
	return &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		createDmFn: func(context.Context, *pb.CreateDmRequest) (*pb.CreateDmResponse, error) {
			t.Error("create_dm was sent for an identifier that should have been rejected locally")
			return dmResponse("dm1"), nil
		},
	}
}

func TestResolveIdentifierRejectionsRenderAsClientErrors(t *testing.T) {
	tests := []struct {
		name        string
		identifier  string
		createChat  bool
		wantErrCode string
	}{
		{"missing", "", true, "FI.MAU.GOOGLECHAT.IDENTIFIER_MISSING"},
		{"runTogether", "me@example.com them@example.com", true, "FI.MAU.GOOGLECHAT.IDENTIFIER_NOT_SINGLE"},
		{"separatorList", "me@example.com,them@example.com", true, "FI.MAU.GOOGLECHAT.IDENTIFIER_NOT_SINGLE"},
		{"mxid", "@someone:example.org", true, "FI.MAU.GOOGLECHAT.NOT_A_GOOGLECHAT_IDENTIFIER"},
		{"neitherIDNorEmail", "not-an-email-or-id", true, "FI.MAU.GOOGLECHAT.NOT_A_GOOGLECHAT_IDENTIFIER"},
		{"emailResolveOnly", "someone@example.com", false, "FI.MAU.GOOGLECHAT.EMAIL_REQUIRES_CREATE"},
		// 112233 is newTestUserLogin's own id.
		{"selfDM", "112233", true, "FI.MAU.GOOGLECHAT.CANNOT_DM_SELF"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gc := clientRejectingEverything(t)
			_, err := gc.ResolveIdentifier(context.Background(), tc.identifier, tc.createChat)
			if err == nil {
				t.Fatal("ResolveIdentifier succeeded, want a rejection")
			}
			status, errcode, message := renderError(t, err)
			if status != http.StatusBadRequest {
				t.Errorf("status = %d, want 400 (a 500 is indistinguishable from a bridge bug)", status)
			}
			if errcode != tc.wantErrCode {
				t.Errorf("errcode = %q, want %q", errcode, tc.wantErrCode)
			}
			if !strings.HasPrefix(message, "googlechat: ") {
				t.Errorf("message = %q, want the sentinel's own text rather than a generic one", message)
			}
		})
	}
}

func TestCreateChatWithGhostUnidentifiedRendersAsClientError(t *testing.T) {
	gc := clientRejectingEverything(t)
	_, err := gc.CreateChatWithGhost(context.Background(), nil)
	if err == nil {
		t.Fatal("CreateChatWithGhost(nil) succeeded, want a rejection")
	}
	status, errcode, _ := renderError(t, err)
	if status != http.StatusBadRequest || errcode != "FI.MAU.GOOGLECHAT.GHOST_UNIDENTIFIED" {
		t.Errorf("status/errcode = %d/%q, want 400/FI.MAU.GOOGLECHAT.GHOST_UNIDENTIFIED", status, errcode)
	}
}

// TestGenuineFailuresStillRenderAs500: an upstream RPC failure must not be reported as a 4xx.
func TestGenuineFailuresStillRenderAs500(t *testing.T) {
	gc := &GChatClient{
		UserLogin: newTestUserLogin(&UserLoginMetadata{}),
		createDmFn: func(context.Context, *pb.CreateDmRequest) (*pb.CreateDmResponse, error) {
			return nil, errors.New("unexpected status 500")
		},
	}
	_, err := gc.ResolveIdentifier(context.Background(), "778899", true)
	if err == nil {
		t.Fatal("ResolveIdentifier succeeded despite a failing create_dm")
	}
	status, errcode, _ := renderError(t, err)
	if status != http.StatusInternalServerError || errcode != "M_UNKNOWN" {
		t.Errorf("status/errcode = %d/%q, want 500/M_UNKNOWN for an upstream failure", status, errcode)
	}
}

// TestSelfDMErrorTextCarriesNoErrCode: mautrix.RespError.Error() would prepend the errcode.
func TestSelfDMErrorTextCarriesNoErrCode(t *testing.T) {
	err := selfDMError()
	text := err.Error()
	if !strings.HasPrefix(text, "googlechat: that is your own account") {
		t.Errorf("error text = %q, want it to start with the human-readable message", text)
	}
	for _, leak := range []string{"FI.MAU", "M_UNKNOWN", "M_INVALID_PARAM"} {
		if strings.Contains(text, leak) {
			t.Errorf("error text = %q leaks the errcode %q into the bot reply", text, leak)
		}
	}
	if !errors.Is(err, bridgev2.ErrResolveIdentifierTryNext) {
		t.Error("selfDMError no longer wraps ErrResolveIdentifierTryNext; bridgev2 would stop trying the user's other logins")
	}
	if !errors.Is(err, ErrCannotDMYourself) {
		t.Error("selfDMError no longer matches ErrCannotDMYourself")
	}
}

func TestValidationSentinelsAreNotConflated(t *testing.T) {
	sentinels := map[string]error{
		"ErrCannotResolveEmailWithoutCreating": ErrCannotResolveEmailWithoutCreating,
		"ErrIdentifierNotSingle":               ErrIdentifierNotSingle,
		"ErrCannotDMYourself":                  ErrCannotDMYourself,
		"ErrIdentifierMissing":                 ErrIdentifierMissing,
		"ErrNotAGoogleChatIdentifier":          ErrNotAGoogleChatIdentifier,
		"ErrGhostUnidentified":                 ErrGhostUnidentified,
	}
	for nameA, a := range sentinels {
		// Pointer sentinels would make RespError.Is compare errcodes.
		if _, isValue := a.(bridgev2.RespError); !isValue {
			t.Errorf("%s is %T, want a bridgev2.RespError VALUE", nameA, a)
		}
		if !errors.Is(a, a) {
			t.Errorf("errors.Is(%s, %s) = false; the sentinel no longer matches itself", nameA, nameA)
		}
		for nameB, b := range sentinels {
			if nameA == nameB {
				continue
			}
			if errors.Is(a, b) {
				t.Errorf("errors.Is(%s, %s) = true; two distinct rejections are indistinguishable", nameA, nameB)
			}
		}
	}
}

// TestSelfDMByEmailKeepsTheServersAnswer: the self-DM relabel is a deduction, so the server's body must survive.
func TestSelfDMByEmailKeepsTheServersAnswer(t *testing.T) {
	gc := &GChatClient{
		UserLogin: newLoginWithOwnEmail("ada@example.com"),
		createDmFn: func(context.Context, *pb.CreateDmRequest) (*pb.CreateDmResponse, error) {
			return nil, &gchatmeow.UnexpectedStatusError{Status: 400, Body: "POLICY_DENIED: external chat disabled"}
		},
	}
	_, err := gc.ResolveIdentifier(context.Background(), "ada@example.com", true)
	if !errors.Is(err, ErrCannotDMYourself) {
		t.Fatalf("error = %v, want ErrCannotDMYourself", err)
	}
	if !strings.Contains(err.Error(), "POLICY_DENIED: external chat disabled") {
		t.Errorf("error = %v, want the server's own reason to survive the relabel", err)
	}
	// The HTTP rendering must use ErrCannotDMYourself.
	status, errcode, _ := renderError(t, err)
	if status != http.StatusBadRequest || errcode != "FI.MAU.GOOGLECHAT.CANNOT_DM_SELF" {
		t.Errorf("status/errcode = %d/%q, want 400/FI.MAU.GOOGLECHAT.CANNOT_DM_SELF", status, errcode)
	}
}

// TestHasOtherLoginsDefaultsToTrueWhenTheUserIsUnknown: a bare UserLogin has no User.
func TestHasOtherLoginsDefaultsToTrueWhenTheUserIsUnknown(t *testing.T) {
	if !(&GChatClient{UserLogin: newTestUserLogin(&UserLoginMetadata{})}).hasOtherLogins() {
		t.Error("hasOtherLogins() = false for a login with no User; the deferral would go untested everywhere")
	}
	if !(&GChatClient{}).hasOtherLogins() {
		t.Error("hasOtherLogins() = false for a nil UserLogin")
	}
}
