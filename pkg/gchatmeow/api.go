package gchatmeow

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"

	"google.golang.org/protobuf/proto"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// Constants ported VERBATIM from maugclib/client.py (docs/research/01
// §3.1/§7, read 2026-07-13). Task 13's live protocol spike is responsible
// for validating these against the real Google Chat server before this
// client is trusted in production.
const (
	// apiKey is the `key` query param on every /api/* request -- inherited
	// from the Hangouts web client, required to avoid a 403 "Daily Limit
	// for Unauthenticated Use Exceeded" (client.py:29, client.py:662-664).
	apiKey = "AIzaSyD7InnYR3VKdb4j2rMUEbTCIr2VyEazl6k"

	// apiBaseURL is GC_BASE_URL, client.py:31. Overridable per-Client via
	// the baseURL field for tests.
	apiBaseURL = "https://chat.google.com/u/0"

	// clientVersion is the RequestHeader.client_version stamped on every
	// request (client.py:105; RequestHeader schema at
	// googlechat.proto:106-139).
	clientVersion = int64(2440378181258)
)

// Client is the binary-proto RPC layer for the Google Chat private web API:
// one method per proto_* endpoint in maugclib/client.py, all POSTing binary
// protobuf to https://chat.google.com/u/0/api/*.
//
// This type carries ONLY the fields the RPC layer itself needs. Task 8
// (client.go, same package) adds the BrowserChannel/callback/supervision
// fields to this same struct -- methods living in separate files within one
// package is intentional, not a layering violation.
type Client struct {
	// session is the authenticated HTTP layer (cookies, retries) every RPC
	// is sent through.
	session *Session

	// mu guards xsrfToken, which Task 8's connect loop may refresh
	// concurrently with in-flight RPCs.
	mu        sync.RWMutex
	xsrfToken string

	// requestCounter is the `c` query param -- an incrementing per-Client
	// request id (client.py:123-126's _api_reqid). The server appears to
	// ignore duplicates; kept only "to not stand out" from the real web
	// client, per client.py's own comment. atomic because RPCs may be
	// issued concurrently from multiple goroutines.
	requestCounter atomic.Int64

	// baseURL overrides apiBaseURL when non-empty, so tests can point a
	// Client at an httptest server instead of the real Google Chat host.
	baseURL string
}

// XSRFToken returns the token currently sent as x-framework-xsrf-token on
// every /api/* request.
func (c *Client) XSRFToken() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.xsrfToken
}

// SetXSRFToken updates the token sent as x-framework-xsrf-token on every
// subsequent /api/* request (see Session.FetchXSRFToken, auth.go, for how
// it's obtained). Safe to call concurrently with in-flight RPCs.
func (c *Client) SetXSRFToken(token string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.xsrfToken = token
}

// newRequestHeader builds the RequestHeader every /api/* request carries,
// mirroring client.py:103-109 exactly: client_type=WEB,
// client_version=2440378181258, and
// client_feature_capabilities.spam_room_invites_level=FULLY_SUPPORTED. A
// fresh struct is built per call (rather than shared/cached) since it's
// cheap and avoids any risk of concurrent mutation of a shared proto
// message across in-flight requests.
func newRequestHeader() *pb.RequestHeader {
	return &pb.RequestHeader{
		ClientType:    pb.RequestHeader_WEB.Enum(),
		ClientVersion: proto.Int64(clientVersion),
		ClientFeatureCapabilities: &pb.ClientFeatureCapabilities{
			SpamRoomInvitesLevel: pb.ClientFeatureCapabilities_FULLY_SUPPORTED.Enum(),
		},
	}
}

// doRequest implements Client._gc_request (client.py:582-615): marshal
// requestPB to binary protobuf, POST it to
// {baseURL}/api/{endpoint}?c=<counter>&rt=b&alt=proto&key=<apiKey>, and
// unmarshal the response body into responsePB.
//
// Wire format (docs/research/01 §3.1, cross-checked against
// docs/research/08c):
//
//	POST https://chat.google.com/u/0/api/{endpoint}?c={reqid}&rt=b&alt=proto&key={API_KEY}
//	Content-Type: application/x-protobuf
//	X-Goog-Encode-Response-If-Executable: base64
//	x-framework-xsrf-token: <token>
//	<binary serialized request proto>
//
// Non-200 responses surface as *UnexpectedStatusError with the status
// preserved (Session.Fetch's job) -- an explicit improvement over Python,
// which collapses every non-200 /api/* response into a bare NetworkError
// with no status (doc 01 §6).
func (c *Client) doRequest(ctx context.Context, endpoint string, requestPB, responsePB proto.Message) error {
	reqBody, err := proto.Marshal(requestPB)
	if err != nil {
		return fmt.Errorf("gchatmeow: failed to marshal %s request: %w", endpoint, err)
	}

	counter := c.requestCounter.Add(1)

	params := url.Values{}
	params.Set("c", strconv.FormatInt(counter, 10))
	params.Set("rt", "b")
	params.Set("alt", "proto")
	params.Set("key", apiKey)

	base := c.baseURL
	if base == "" {
		base = apiBaseURL
	}
	reqURL := fmt.Sprintf("%s/api/%s?%s", base, endpoint, params.Encode())

	headers := http.Header{}
	headers.Set("Content-Type", "application/x-protobuf")
	headers.Set("X-Goog-Encode-Response-If-Executable", "base64")
	if token := c.XSRFToken(); token != "" {
		headers.Set("x-framework-xsrf-token", token)
	}

	res, err := c.session.Fetch(ctx, http.MethodPost, reqURL, headers, reqBody)
	if err != nil {
		return err
	}

	if err := unmarshalAPIResponse(res.Body, responsePB); err != nil {
		return fmt.Errorf("gchatmeow: failed to decode %s response: %w", endpoint, err)
	}
	return nil
}

// unmarshalAPIResponse implements the dual binary/base64 response path
// (task-5-brief.md wire-format note): /api/* responses are ordinarily raw
// binary protobuf, parsed directly like Python's response_pb.ParseFromString
// (client.py:609-614). But the X-Goog-Encode-Response-If-Executable: base64
// header this client sends on every request (matching client.py:650-653)
// means the server MAY base64-encode the body instead -- so if the raw
// bytes don't parse as valid protobuf, this retries the same bytes after a
// base64 decode before giving up.
func unmarshalAPIResponse(body []byte, responsePB proto.Message) error {
	if err := proto.Unmarshal(body, responsePB); err == nil {
		return nil
	}

	// proto.Unmarshal may have partially populated responsePB before
	// failing; reset it so the base64 retry starts from a clean message.
	proto.Reset(responsePB)

	decoded, decErr := base64.StdEncoding.DecodeString(string(bytes.TrimSpace(body)))
	if decErr != nil {
		return fmt.Errorf("response is neither valid binary protobuf nor base64: %w", decErr)
	}
	if err := proto.Unmarshal(decoded, responsePB); err != nil {
		return fmt.Errorf("failed to parse base64-decoded response as protobuf: %w", err)
	}
	return nil
}

// The 16 /api/* RPCs, matching maugclib/client.py's proto_* methods 1:1
// (docs/research/01 §3.2). Each wrapper stamps request_header on the
// caller-supplied request in place (mirroring
// googlechat-megabridge/pkg/gchatmeow/api.go's `request.RequestHeader =
// c.gcRequestHeader` pattern) and returns a freshly-allocated response.
//
// Endpoint-name spelling cross-checked 1:1 against client.py:682-810.

// GetUserPresence returns presence for one or more users.
// Endpoint: get_user_presence (client.py:682, proto_get_user_presence).
func (c *Client) GetUserPresence(ctx context.Context, req *pb.GetUserPresenceRequest) (*pb.GetUserPresenceResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.GetUserPresenceResponse{}
	return resp, c.doRequest(ctx, "get_user_presence", req, resp)
}

// GetMembers resolves user/member info by id.
// Endpoint: get_members (client.py:691, proto_get_members).
func (c *Client) GetMembers(ctx context.Context, req *pb.GetMembersRequest) (*pb.GetMembersResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.GetMembersResponse{}
	return resp, c.doRequest(ctx, "get_members", req, resp)
}

// PaginatedWorld lists all conversations ("world view"); used for chat sync.
// Endpoint: paginated_world (client.py:700, proto_paginated_world).
func (c *Client) PaginatedWorld(ctx context.Context, req *pb.PaginatedWorldRequest) (*pb.PaginatedWorldResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.PaginatedWorldResponse{}
	return resp, c.doRequest(ctx, "paginated_world", req, resp)
}

// GetSelfUserStatus returns the current user's own status, including Gaia
// ID and user revision.
// Endpoint: get_self_user_status (client.py:710, proto_get_self_user_status).
func (c *Client) GetSelfUserStatus(ctx context.Context, req *pb.GetSelfUserStatusRequest) (*pb.GetSelfUserStatusResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.GetSelfUserStatusResponse{}
	return resp, c.doRequest(ctx, "get_self_user_status", req, resp)
}

// GetGroup fetches one group/DM, including membership.
// Endpoint: get_group (client.py:722, proto_get_group).
func (c *Client) GetGroup(ctx context.Context, req *pb.GetGroupRequest) (*pb.GetGroupResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.GetGroupResponse{}
	return resp, c.doRequest(ctx, "get_group", req, resp)
}

// MarkGroupReadstate sends a read receipt / last-read time.
// Endpoint: mark_group_readstate (client.py:730, proto_mark_group_read_state).
func (c *Client) MarkGroupReadstate(ctx context.Context, req *pb.MarkGroupReadstateRequest) (*pb.MarkGroupReadstateResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.MarkGroupReadstateResponse{}
	return resp, c.doRequest(ctx, "mark_group_readstate", req, resp)
}

// CreateTopic sends a message that starts a new topic/thread.
// Endpoint: create_topic (client.py:738, proto_create_topic).
func (c *Client) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.CreateTopicResponse{}
	return resp, c.doRequest(ctx, "create_topic", req, resp)
}

// CreateMessage sends a message into an existing thread.
// Endpoint: create_message (client.py:746, proto_create_message).
func (c *Client) CreateMessage(ctx context.Context, req *pb.CreateMessageRequest) (*pb.CreateMessageResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.CreateMessageResponse{}
	return resp, c.doRequest(ctx, "create_message", req, resp)
}

// UpdateReaction adds/removes an emoji reaction.
// Endpoint: update_reaction (client.py:754, proto_update_reaction).
func (c *Client) UpdateReaction(ctx context.Context, req *pb.UpdateReactionRequest) (*pb.UpdateReactionResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.UpdateReactionResponse{}
	return resp, c.doRequest(ctx, "update_reaction", req, resp)
}

// DeleteMessage deletes/redacts a message.
// Endpoint: delete_message (client.py:762, proto_delete_message).
func (c *Client) DeleteMessage(ctx context.Context, req *pb.DeleteMessageRequest) (*pb.DeleteMessageResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.DeleteMessageResponse{}
	return resp, c.doRequest(ctx, "delete_message", req, resp)
}

// EditMessage edits message text.
// Endpoint: edit_message (client.py:769, proto_edit_message).
func (c *Client) EditMessage(ctx context.Context, req *pb.EditMessageRequest) (*pb.EditMessageResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.EditMessageResponse{}
	return resp, c.doRequest(ctx, "edit_message", req, resp)
}

// SetTypingState sends a typing notification (group or topic context).
// Endpoint: set_typing_state (client.py:776, proto_set_typing_state).
func (c *Client) SetTypingState(ctx context.Context, req *pb.SetTypingStateRequest) (*pb.SetTypingStateResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.SetTypingStateResponse{}
	return resp, c.doRequest(ctx, "set_typing_state", req, resp)
}

// CatchUpUser replays missed events for the whole user since a revision.
// Endpoint: catch_up_user (client.py:783, proto_catch_up_user).
//
// NOTE (proto type mismatch, see task-5-brief.md's "note any mismatch"
// instruction): the brief's interface sketch names the response type
// *pb.CatchUpUserResponse, but pkg/gchatmeow/proto/googlechat.pb.go has no
// such type -- proto_catch_up_user (client.py:783) and proto_catch_up_group
// (client.py:790) both return the SAME googlechat_pb2.CatchUpResponse in
// the Python original, and our generated proto matches that (only
// CatchUpResponse exists, not CatchUpUserResponse/CatchUpGroupResponse).
// This wrapper returns *pb.CatchUpResponse accordingly.
func (c *Client) CatchUpUser(ctx context.Context, req *pb.CatchUpUserRequest) (*pb.CatchUpResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.CatchUpResponse{}
	return resp, c.doRequest(ctx, "catch_up_user", req, resp)
}

// CatchUpGroup replays missed events for one group since a revision.
// Endpoint: catch_up_group (client.py:790, proto_catch_up_group). See
// CatchUpUser's doc comment for the CatchUpResponse type-name note.
func (c *Client) CatchUpGroup(ctx context.Context, req *pb.CatchUpGroupRequest) (*pb.CatchUpResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.CatchUpResponse{}
	return resp, c.doRequest(ctx, "catch_up_group", req, resp)
}

// ListTopics pages through topics of a group (backfill).
// Endpoint: list_topics (client.py:797, proto_list_topics).
func (c *Client) ListTopics(ctx context.Context, req *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.ListTopicsResponse{}
	return resp, c.doRequest(ctx, "list_topics", req, resp)
}

// ListMessages pages through messages of a topic (backfill).
// Endpoint: list_messages (client.py:804, proto_list_messages).
func (c *Client) ListMessages(ctx context.Context, req *pb.ListMessagesRequest) (*pb.ListMessagesResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.ListMessagesResponse{}
	return resp, c.doRequest(ctx, "list_messages", req, resp)
}
