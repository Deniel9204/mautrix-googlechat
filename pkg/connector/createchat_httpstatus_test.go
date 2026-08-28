package connector

// createchat_httpstatus_test.go -- the deliberate rejections in createchat.go
// must reach a provisioning-API client as 4xx with their own message, not as
// 500 "Internal error resolving identifier", which is indistinguishable from a
// genuine bug.
//
// These tests render each error through the REAL framework function rather
// than a replica, so they pin the actual errors.As(WritableError) behaviour --
// including its descent through selfDMError's Unwrap() []error. Test files are
// exempt from the connector-imports-no-HTTP rule the CI grep enforces.

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

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// renderError puts err through the framework's own error responder and reports
// the status and errcode a provisioning client would see.
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

// clientRejectingEverything is a client whose create_dm would succeed, so any
// error a test sees came from validation rather than from the RPC.
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

// TestCreateChatWithGhostUnidentifiedRendersAsClientError covers the rejection
// on the other entry point. The existing TestCreateChatWithGhostRejectsUnidentified
// only asserts err != nil, which passes either way.
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

// TestGenuineFailuresStillRenderAs500 is what makes the 4xx mean anything: a
// blanket "wrap everything in a 400" would satisfy the tests above while
// destroying the signal they exist to create. An upstream RPC failure is a
// bridge/network problem and must keep saying so.
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

// TestSelfDMErrorTextCarriesNoErrCode pins the bot-command path, which is the
// one the user actually types. bridgev2.RespError.Error() returns only the
// message; its mautrix.RespError namesake prepends the errcode. That
// substitution compiles, still renders 400, and still satisfies every
// errors.Is assertion -- this is the only test that catches it.
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

// TestValidationSentinelsAreNotConflated guards the string-comparison
// semantics of bridgev2.RespError.Is. It goes red both for a duplicated
// message and for a switch to pointer sentinels, which would fall back to
// comparing errcodes.
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
