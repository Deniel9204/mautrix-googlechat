package gchatmeow

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"google.golang.org/protobuf/proto"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"sync"
	"sync/atomic"
)

// newTestClient builds a Client pointed at srv via the baseURL override.
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

// TestGetSelfUserStatusRoundTrip pins the whole wire format: method, path, query, headers and proto body.
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

		// t.Errorf, not Fatalf: this runs on the server goroutine.
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("reading request body: %v", err)
		}
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

	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/api/get_self_user_status" {
		t.Errorf("path = %q, want /api/get_self_user_status", gotPath)
	}

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

	if got := gotHeaders.Get("Content-Type"); got != "application/x-protobuf" {
		t.Errorf("Content-Type = %q, want application/x-protobuf", got)
	}
	if got := gotHeaders.Get("x-framework-xsrf-token"); got != wantToken {
		t.Errorf("x-framework-xsrf-token = %q, want %q", got, wantToken)
	}
	if got := gotHeaders.Get("X-Goog-Encode-Response-If-Executable"); got != "base64" {
		t.Errorf("X-Goog-Encode-Response-If-Executable = %q, want base64", got)
	}

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

	if got := resp.GetUserStatus().GetUserId().GetId(); got != "112233" {
		t.Errorf("response UserStatus.UserId.Id = %q, want %q", got, "112233")
	}
}

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

// TestGetSelfUserStatus401IsAuthError: a 401 must surface as an auth error (BAD_CREDENTIALS).
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

// hasRequestHeader matches every *Request proto (field 100, or 1 for PaginatedWorldRequest).
type hasRequestHeader interface {
	GetRequestHeader() *pb.RequestHeader
}

// TestAllRPCsSetEndpointAndRequestHeader checks every wrapper's endpoint and request_header.
func TestAllRPCsSetEndpointAndRequestHeader(t *testing.T) {
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
		{"CreateMembership", "create_membership", func(c *Client) (hasRequestHeader, error) {
			req := &pb.CreateMembershipRequest{}
			_, err := c.CreateMembership(context.Background(), req)
			return req, err
		}},
		{"RemoveMemberships", "remove_memberships", func(c *Client) (hasRequestHeader, error) {
			req := &pb.RemoveMembershipsRequest{}
			_, err := c.RemoveMemberships(context.Background(), req)
			return req, err
		}},
		{"UpdateGroupNotificationSettings", "update_group_notification_settings", func(c *Client) (hasRequestHeader, error) {
			req := &pb.UpdateGroupNotificationSettingsRequest{}
			_, err := c.UpdateGroupNotificationSettings(context.Background(), req)
			return req, err
		}},
		{"HideGroup", "hide_group", func(c *Client) (hasRequestHeader, error) {
			req := &pb.HideGroupRequest{}
			_, err := c.HideGroup(context.Background(), req)
			return req, err
		}},
		{"UpdateGroup", "update_group", func(c *Client) (hasRequestHeader, error) {
			req := &pb.UpdateGroupRequest{}
			_, err := c.UpdateGroup(context.Background(), req)
			return req, err
		}},
	}

	if len(tests) != 21 {
		t.Fatalf("table has %d cases, want 21", len(tests))
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

// TestCreateTopicDoesNotRetryOn5xx: a retried 502 could post the message twice.
func TestCreateTopicDoesNotRetryOn5xx(t *testing.T) {
	var requests int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	c := newTestClient(t, srv, "tok")
	if _, err := c.CreateTopic(context.Background(), &pb.CreateTopicRequest{}); err == nil {
		t.Fatal("CreateTopic = nil error, want the 502 surfaced")
	}
	if got := atomic.LoadInt32(&requests); got != 1 {
		t.Errorf("create_topic was sent %d times, want 1: retrying a create can duplicate the message", got)
	}
}

func TestCreateMessageDoesNotRetryOn5xx(t *testing.T) {
	var requests int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	c := newTestClient(t, srv, "tok")
	if _, err := c.CreateMessage(context.Background(), &pb.CreateMessageRequest{}); err == nil {
		t.Fatal("CreateMessage = nil error, want the 502 surfaced")
	}
	if got := atomic.LoadInt32(&requests); got != 1 {
		t.Errorf("create_message was sent %d times, want 1", got)
	}
}

func TestIdempotentRPCStillRetriesOn5xx(t *testing.T) {
	var mu sync.Mutex
	requests := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		n := requests
		mu.Unlock()
		if n < 2 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := newTestClient(t, srv, "tok")
	if _, err := c.GetSelfUserStatus(context.Background(), &pb.GetSelfUserStatusRequest{}); err != nil {
		t.Fatalf("GetSelfUserStatus: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if requests != 2 {
		t.Errorf("requests = %d, want 2: idempotent RPCs must still retry a 5xx", requests)
	}
}
