package gchatmeow

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"google.golang.org/protobuf/proto"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// newTestClient builds a Client wired to an httptest server via the
// unexported baseURL override (task-5-brief.md: "baseURL string for
// tests"), with session pointed so cookies don't matter for these tests
// (no cookie assertions here -- session.go's cookie/host-allowlist behavior
// is covered by session_test.go).
func newTestClient(t *testing.T, srv *httptest.Server, xsrfToken string) *Client {
	t.Helper()
	sess, err := NewSession(nil, "")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	c := &Client{
		session: sess,
		baseURL: srv.URL,
	}
	if xsrfToken != "" {
		c.SetXSRFToken(xsrfToken)
	}
	return c
}

// TestGetSelfUserStatusRoundTrip is the RED/GREEN anchor test for the whole
// wire format: POST method, path, query params (c/rt/alt/key), headers
// (Content-Type, x-framework-xsrf-token, X-Goog-Encode-Response-If-Executable),
// and a binary-proto request/response body round trip.
func TestGetSelfUserStatusRoundTrip(t *testing.T) {
	const wantToken = "test-xsrf-token"

	var (
		gotMethod  string
		gotPath    string
		gotQuery   url.Values
		gotHeaders http.Header
		gotBody    []byte
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotQuery = r.URL.Query()
		gotHeaders = r.Header.Clone()

		body := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(body)
		gotBody = body

		resp := &pb.GetSelfUserStatusResponse{
			UserStatus: &pb.UserStatus{
				UserId: &pb.UserId{Id: proto.String("112233")},
			},
		}
		respBytes, err := proto.Marshal(resp)
		if err != nil {
			t.Fatalf("marshal test response: %v", err)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(respBytes)
	}))
	defer srv.Close()

	c := newTestClient(t, srv, wantToken)

	resp, err := c.GetSelfUserStatus(context.Background(), &pb.GetSelfUserStatusRequest{})
	if err != nil {
		t.Fatalf("GetSelfUserStatus: %v", err)
	}

	// -- Method / path --
	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/api/get_self_user_status" {
		t.Errorf("path = %q, want /api/get_self_user_status", gotPath)
	}

	// -- Query params: c=<counter>&rt=b&alt=proto&key=<APIKey> --
	if got := gotQuery.Get("c"); got != "1" {
		t.Errorf("query c = %q, want %q (first request on a fresh Client)", got, "1")
	}
	if got := gotQuery.Get("rt"); got != "b" {
		t.Errorf("query rt = %q, want %q", got, "b")
	}
	if got := gotQuery.Get("alt"); got != "proto" {
		t.Errorf("query alt = %q, want %q", got, "proto")
	}
	if got := gotQuery.Get("key"); got != apiKey {
		t.Errorf("query key = %q, want %q", got, apiKey)
	}

	// -- Headers --
	if got := gotHeaders.Get("Content-Type"); got != "application/x-protobuf" {
		t.Errorf("Content-Type = %q, want application/x-protobuf", got)
	}
	if got := gotHeaders.Get("x-framework-xsrf-token"); got != wantToken {
		t.Errorf("x-framework-xsrf-token = %q, want %q", got, wantToken)
	}
	if got := gotHeaders.Get("X-Goog-Encode-Response-If-Executable"); got != "base64" {
		t.Errorf("X-Goog-Encode-Response-If-Executable = %q, want base64", got)
	}

	// -- Request body proto round trip: RequestHeader must be populated by
	// the wrapper (client.py:103-109's WEB/2440378181258/FULLY_SUPPORTED). --
	var gotReq pb.GetSelfUserStatusRequest
	if err := proto.Unmarshal(gotBody, &gotReq); err != nil {
		t.Fatalf("unmarshal request body sent to server: %v", err)
	}
	hdr := gotReq.GetRequestHeader()
	if hdr == nil {
		t.Fatal("request_header not set on outgoing request")
	}
	if hdr.GetClientType() != pb.RequestHeader_WEB {
		t.Errorf("client_type = %v, want WEB", hdr.GetClientType())
	}
	if hdr.GetClientVersion() != clientVersion {
		t.Errorf("client_version = %d, want %d", hdr.GetClientVersion(), clientVersion)
	}
	if got := hdr.GetClientFeatureCapabilities().GetSpamRoomInvitesLevel(); got != pb.ClientFeatureCapabilities_FULLY_SUPPORTED {
		t.Errorf("spam_room_invites_level = %v, want FULLY_SUPPORTED", got)
	}

	// -- Response body proto round trip --
	if got := resp.GetUserStatus().GetUserId().GetId(); got != "112233" {
		t.Errorf("response UserStatus.UserId.Id = %q, want %q", got, "112233")
	}
}

// TestGetSelfUserStatusRequestCounterIncrements verifies the "c" query
// param increments per call on the same Client (client.py:123-126's
// _api_reqid, incremented before every request).
func TestGetSelfUserStatusRequestCounterIncrements(t *testing.T) {
	var gotCounters []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCounters = append(gotCounters, r.URL.Query().Get("c"))
		respBytes, _ := proto.Marshal(&pb.GetSelfUserStatusResponse{})
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(respBytes)
	}))
	defer srv.Close()

	c := newTestClient(t, srv, "")

	for i := 0; i < 3; i++ {
		if _, err := c.GetSelfUserStatus(context.Background(), &pb.GetSelfUserStatusRequest{}); err != nil {
			t.Fatalf("GetSelfUserStatus call %d: %v", i, err)
		}
	}

	want := []string{"1", "2", "3"}
	if len(gotCounters) != len(want) {
		t.Fatalf("got %d requests, want %d", len(gotCounters), len(want))
	}
	for i, w := range want {
		if gotCounters[i] != w {
			t.Errorf("request %d: c = %q, want %q", i, gotCounters[i], w)
		}
	}
}

// TestGetSelfUserStatusBase64Response covers the dual binary/base64
// response path (task-5-brief.md: "detect base64-encoded responses -- if
// the body isn't valid proto, try base64-decode first").
func TestGetSelfUserStatusBase64Response(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := &pb.GetSelfUserStatusResponse{
			UserStatus: &pb.UserStatus{
				UserId: &pb.UserId{Id: proto.String("998877")},
			},
		}
		respBytes, err := proto.Marshal(resp)
		if err != nil {
			t.Fatalf("marshal test response: %v", err)
		}
		encoded := base64.StdEncoding.EncodeToString(respBytes)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(encoded))
	}))
	defer srv.Close()

	c := newTestClient(t, srv, "")

	resp, err := c.GetSelfUserStatus(context.Background(), &pb.GetSelfUserStatusRequest{})
	if err != nil {
		t.Fatalf("GetSelfUserStatus with base64-encoded response body: %v", err)
	}
	if got := resp.GetUserStatus().GetUserId().GetId(); got != "998877" {
		t.Errorf("response UserStatus.UserId.Id = %q, want %q (decoded from base64)", got, "998877")
	}
}

// TestGetSelfUserStatus401IsAuthError verifies a non-200 response surfaces
// as *UnexpectedStatusError with the status preserved, and that
// IsAuthError recognizes 401 -- required so the connector can map this to
// BAD_CREDENTIALS (doc 01 §6).
func TestGetSelfUserStatus401IsAuthError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"invalid_grant"}`))
	}))
	defer srv.Close()

	c := newTestClient(t, srv, "sometoken")

	_, err := c.GetSelfUserStatus(context.Background(), &pb.GetSelfUserStatusRequest{})
	if err == nil {
		t.Fatal("GetSelfUserStatus: want error for 401 response, got nil")
	}
	if !IsAuthError(err) {
		t.Errorf("IsAuthError(%v) = false, want true", err)
	}
	var statusErr *UnexpectedStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("error is not *UnexpectedStatusError: %v (%T)", err, err)
	}
	if statusErr.Status != http.StatusUnauthorized {
		t.Errorf("Status = %d, want %d", statusErr.Status, http.StatusUnauthorized)
	}
}

// hasRequestHeader is satisfied by every *Request proto in this schema
// (field 100, or field 1 for PaginatedWorldRequest) -- but by NO *Response
// proto (request_header is a request-only concept here). Wrappers are
// expected to mutate the caller-supplied request in place, matching
// megabridge's `request.RequestHeader = c.gcRequestHeader` pattern, so the
// table test below can inspect the same pointer it passed in after the
// call returns.
type hasRequestHeader interface {
	GetRequestHeader() *pb.RequestHeader
}

// TestAllSixteenRPCsSetEndpointAndRequestHeader is a table-driven smoke
// test over all 16 /api/* wrappers: each must POST to the endpoint named in
// docs/research/01 §3.2 / maugclib/client.py's proto_* table, and must
// stamp request_header on the outgoing request. It does not assert per-RPC
// response field plumbing beyond "no error" -- GetSelfUserStatus above
// covers the full wire format (including the response side) in detail.
func TestAllSixteenRPCsSetEndpointAndRequestHeader(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		call     func(c *Client) (hasRequestHeader, error)
	}{
		{"GetUserPresence", "get_user_presence", func(c *Client) (hasRequestHeader, error) {
			req := &pb.GetUserPresenceRequest{}
			_, err := c.GetUserPresence(context.Background(), req)
			return req, err
		}},
		{"GetMembers", "get_members", func(c *Client) (hasRequestHeader, error) {
			req := &pb.GetMembersRequest{}
			_, err := c.GetMembers(context.Background(), req)
			return req, err
		}},
		{"PaginatedWorld", "paginated_world", func(c *Client) (hasRequestHeader, error) {
			req := &pb.PaginatedWorldRequest{}
			_, err := c.PaginatedWorld(context.Background(), req)
			return req, err
		}},
		{"GetSelfUserStatus", "get_self_user_status", func(c *Client) (hasRequestHeader, error) {
			req := &pb.GetSelfUserStatusRequest{}
			_, err := c.GetSelfUserStatus(context.Background(), req)
			return req, err
		}},
		{"GetGroup", "get_group", func(c *Client) (hasRequestHeader, error) {
			req := &pb.GetGroupRequest{}
			_, err := c.GetGroup(context.Background(), req)
			return req, err
		}},
		{"MarkGroupReadstate", "mark_group_readstate", func(c *Client) (hasRequestHeader, error) {
			req := &pb.MarkGroupReadstateRequest{}
			_, err := c.MarkGroupReadstate(context.Background(), req)
			return req, err
		}},
		{"CreateTopic", "create_topic", func(c *Client) (hasRequestHeader, error) {
			req := &pb.CreateTopicRequest{}
			_, err := c.CreateTopic(context.Background(), req)
			return req, err
		}},
		{"CreateMessage", "create_message", func(c *Client) (hasRequestHeader, error) {
			req := &pb.CreateMessageRequest{}
			_, err := c.CreateMessage(context.Background(), req)
			return req, err
		}},
		{"UpdateReaction", "update_reaction", func(c *Client) (hasRequestHeader, error) {
			req := &pb.UpdateReactionRequest{}
			_, err := c.UpdateReaction(context.Background(), req)
			return req, err
		}},
		{"DeleteMessage", "delete_message", func(c *Client) (hasRequestHeader, error) {
			req := &pb.DeleteMessageRequest{}
			_, err := c.DeleteMessage(context.Background(), req)
			return req, err
		}},
		{"EditMessage", "edit_message", func(c *Client) (hasRequestHeader, error) {
			req := &pb.EditMessageRequest{}
			_, err := c.EditMessage(context.Background(), req)
			return req, err
		}},
		{"SetTypingState", "set_typing_state", func(c *Client) (hasRequestHeader, error) {
			req := &pb.SetTypingStateRequest{}
			_, err := c.SetTypingState(context.Background(), req)
			return req, err
		}},
		{"CatchUpUser", "catch_up_user", func(c *Client) (hasRequestHeader, error) {
			req := &pb.CatchUpUserRequest{}
			_, err := c.CatchUpUser(context.Background(), req)
			return req, err
		}},
		{"CatchUpGroup", "catch_up_group", func(c *Client) (hasRequestHeader, error) {
			req := &pb.CatchUpGroupRequest{}
			_, err := c.CatchUpGroup(context.Background(), req)
			return req, err
		}},
		{"ListTopics", "list_topics", func(c *Client) (hasRequestHeader, error) {
			req := &pb.ListTopicsRequest{}
			_, err := c.ListTopics(context.Background(), req)
			return req, err
		}},
		{"ListMessages", "list_messages", func(c *Client) (hasRequestHeader, error) {
			req := &pb.ListMessagesRequest{}
			_, err := c.ListMessages(context.Background(), req)
			return req, err
		}},
	}

	if len(tests) != 16 {
		t.Fatalf("table has %d cases, want 16", len(tests))
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				gotMethod string
				gotPath   string
			)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotMethod = r.Method
				gotPath = r.URL.Path
				// Zero-length body is a valid (empty) proto message for
				// every response type in this schema.
				w.WriteHeader(http.StatusOK)
			}))
			defer srv.Close()

			c := newTestClient(t, srv, "tok")
			req, err := tt.call(c)
			if err != nil {
				t.Fatalf("%s: %v", tt.name, err)
			}

			if gotMethod != http.MethodPost {
				t.Errorf("%s: method = %q, want POST", tt.name, gotMethod)
			}
			wantPath := "/api/" + tt.endpoint
			if gotPath != wantPath {
				t.Errorf("%s: path = %q, want %q", tt.name, gotPath, wantPath)
			}
			if req.GetRequestHeader() == nil {
				t.Errorf("%s: request_header not set on outgoing request", tt.name)
			}
		})
	}
}
