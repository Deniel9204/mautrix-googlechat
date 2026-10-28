package gchatmeow

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

const (
	// apiKey is the `key` query param; without it /api/* returns 403.
	apiKey = "AIzaSyD7InnYR3VKdb4j2rMUEbTCIr2VyEazl6k"

	apiBaseURL = "https://chat.google.com/u/0"

	clientVersion = int64(2440378181258)
)

// Client is the binary-proto RPC layer for the private /api/* endpoints; connection fields and methods live in client.go.
type Client struct {
	// session is the authenticated HTTP layer.
	session *Session

	// mu guards xsrfToken, channel, cancel and lastTokenRefresh.
	mu        sync.RWMutex
	xsrfToken string

	// requestCounter is the `c` query param; the server appears to ignore it.
	requestCounter atomic.Int64

	// baseURL and uploadBaseURL override the API and upload hosts for tests.
	baseURL string

	uploadBaseURL string

	// OnStreamEvent is called per event, in order, from the Connect goroutine. Set before Connect.
	OnStreamEvent func(ctx context.Context, ev *pb.Event)
	// OnConnectionState reports connection-state transitions from the Connect goroutine. Set before Connect.
	OnConnectionState func(state ConnState, err error)

	// newChannel builds a fresh channel per (re)connect; tests swap in a fake.
	newChannel func() channelListener

	channel channelListener

	cancel context.CancelFunc

	maxRetries          int           // passed to Channel.Listen (default 3)
	retryBackoffBase    time.Duration // passed to Channel.Listen (default 2)
	reconnectBackoffMin time.Duration // supervision-loop transient backoff floor (default 4s)
	reconnectBackoffMax time.Duration // supervision-loop transient backoff ceiling (default 60s)
	sidFatalRetryFloor  time.Duration // post-fatal outer-retry floor (default 5m)
	sidFatalRetryCap    time.Duration // post-fatal outer-retry ceiling (default 1h)

	// XSRF refresh (24h staleness or on 401); refreshMu stops concurrent 401s stampeding /mole/world.
	xsrfRefreshInterval time.Duration
	lastTokenRefresh    time.Time
	refreshMu           sync.Mutex

	// sleepFn replaces sleepOrDone in tests; nil in production.
	sleepFn func(ctx context.Context, d time.Duration) error
}

// XSRFToken returns the token sent as x-framework-xsrf-token.
func (c *Client) XSRFToken() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.xsrfToken
}

// SetXSRFToken updates the x-framework-xsrf-token header. Safe for concurrent use.
func (c *Client) SetXSRFToken(token string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.xsrfToken = token
}

func newRequestHeader() *pb.RequestHeader {
	return &pb.RequestHeader{
		ClientType:    pb.RequestHeader_WEB.Enum(),
		ClientVersion: proto.Int64(clientVersion),
		ClientFeatureCapabilities: &pb.ClientFeatureCapabilities{
			SpamRoomInvitesLevel: pb.ClientFeatureCapabilities_FULLY_SUPPORTED.Enum(),
		},
	}
}

// doRequest retries once after refreshing the XSRF token on HTTP 401.
func (c *Client) doRequest(ctx context.Context, endpoint string, requestPB, responsePB proto.Message) error {
	return c.doRequestWithPolicy(ctx, endpoint, requestPB, responsePB, true)
}

// doRequestNonIdempotent is doRequest without 5xx retries, for RPCs that create something.
func (c *Client) doRequestNonIdempotent(ctx context.Context, endpoint string, requestPB, responsePB proto.Message) error {
	return c.doRequestWithPolicy(ctx, endpoint, requestPB, responsePB, false)
}

func (c *Client) doRequestWithPolicy(ctx context.Context, endpoint string, requestPB, responsePB proto.Message, idempotent bool) error {
	err := c.doRequestOnce(ctx, endpoint, requestPB, responsePB, idempotent)
	if err == nil || !isUnauthorizedStatus(err) {
		return err
	}
	if refreshErr := c.refreshXSRFToken(ctx); refreshErr != nil {
		// Surface the original 401 so callers see a consistent auth signal.
		return err
	}
	proto.Reset(responsePB)
	return c.doRequestOnce(ctx, endpoint, requestPB, responsePB, idempotent)
}

func isUnauthorizedStatus(err error) bool {
	var ue *UnexpectedStatusError
	return errors.As(err, &ue) && ue.Status == http.StatusUnauthorized
}

// doRequestOnce POSTs binary protobuf to {baseURL}/api/{endpoint}?c=..&rt=b&alt=proto&key=...
func (c *Client) doRequestOnce(ctx context.Context, endpoint string, requestPB, responsePB proto.Message, idempotent bool) error {
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

	fetch := c.session.Fetch
	if !idempotent {
		fetch = c.session.FetchNonIdempotent
	}
	res, err := fetch(ctx, http.MethodPost, reqURL, headers, reqBody)
	if err != nil {
		return err
	}

	if err := unmarshalAPIResponse(res.Body, responsePB); err != nil {
		return fmt.Errorf("gchatmeow: failed to decode %s response: %w", endpoint, err)
	}
	return nil
}

// unmarshalAPIResponse parses raw binary protobuf, falling back to base64 (the server may base64 the body).
func unmarshalAPIResponse(body []byte, responsePB proto.Message) error {
	if err := proto.Unmarshal(body, responsePB); err == nil {
		return nil
	}

	// Unmarshal may have partially populated responsePB.
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

// GetUserPresence returns presence for one or more users.
func (c *Client) GetUserPresence(ctx context.Context, req *pb.GetUserPresenceRequest) (*pb.GetUserPresenceResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.GetUserPresenceResponse{}
	return resp, c.doRequest(ctx, "get_user_presence", req, resp)
}

// GetMembers resolves user/member info by id.
func (c *Client) GetMembers(ctx context.Context, req *pb.GetMembersRequest) (*pb.GetMembersResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.GetMembersResponse{}
	return resp, c.doRequest(ctx, "get_members", req, resp)
}

// PaginatedWorld lists all conversations; used for chat sync.
func (c *Client) PaginatedWorld(ctx context.Context, req *pb.PaginatedWorldRequest) (*pb.PaginatedWorldResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.PaginatedWorldResponse{}
	return resp, c.doRequest(ctx, "paginated_world", req, resp)
}

// GetSelfUserStatus returns the current user's own status.
func (c *Client) GetSelfUserStatus(ctx context.Context, req *pb.GetSelfUserStatusRequest) (*pb.GetSelfUserStatusResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.GetSelfUserStatusResponse{}
	return resp, c.doRequest(ctx, "get_self_user_status", req, resp)
}

// GetGroup fetches one group/DM, including membership.
func (c *Client) GetGroup(ctx context.Context, req *pb.GetGroupRequest) (*pb.GetGroupResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.GetGroupResponse{}
	return resp, c.doRequest(ctx, "get_group", req, resp)
}

// MarkGroupReadstate sends a read receipt.
func (c *Client) MarkGroupReadstate(ctx context.Context, req *pb.MarkGroupReadstateRequest) (*pb.MarkGroupReadstateResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.MarkGroupReadstateResponse{}
	return resp, c.doRequest(ctx, "mark_group_readstate", req, resp)
}

// CreateTopic sends a message that starts a new thread.
func (c *Client) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.CreateTopicResponse{}
	// Non-idempotent: retrying after a 5xx could post the message twice.
	return resp, c.doRequestNonIdempotent(ctx, "create_topic", req, resp)
}

// CreateMessage sends a message into an existing thread.
func (c *Client) CreateMessage(ctx context.Context, req *pb.CreateMessageRequest) (*pb.CreateMessageResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.CreateMessageResponse{}
	// Non-idempotent: retrying after a 5xx could post the message twice.
	return resp, c.doRequestNonIdempotent(ctx, "create_message", req, resp)
}

// UpdateReaction adds or removes an emoji reaction.
func (c *Client) UpdateReaction(ctx context.Context, req *pb.UpdateReactionRequest) (*pb.UpdateReactionResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.UpdateReactionResponse{}
	return resp, c.doRequest(ctx, "update_reaction", req, resp)
}

// DeleteMessage deletes a message.
func (c *Client) DeleteMessage(ctx context.Context, req *pb.DeleteMessageRequest) (*pb.DeleteMessageResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.DeleteMessageResponse{}
	return resp, c.doRequest(ctx, "delete_message", req, resp)
}

// EditMessage edits message text.
func (c *Client) EditMessage(ctx context.Context, req *pb.EditMessageRequest) (*pb.EditMessageResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.EditMessageResponse{}
	return resp, c.doRequest(ctx, "edit_message", req, resp)
}

// SetTypingState sends a typing notification.
func (c *Client) SetTypingState(ctx context.Context, req *pb.SetTypingStateRequest) (*pb.SetTypingStateResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.SetTypingStateResponse{}
	return resp, c.doRequest(ctx, "set_typing_state", req, resp)
}

// CatchUpUser replays missed events since a revision. Both catch-up endpoints return CatchUpResponse.
func (c *Client) CatchUpUser(ctx context.Context, req *pb.CatchUpUserRequest) (*pb.CatchUpResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.CatchUpResponse{}
	return resp, c.doRequest(ctx, "catch_up_user", req, resp)
}

// CatchUpGroup replays missed events for one group since a revision.
func (c *Client) CatchUpGroup(ctx context.Context, req *pb.CatchUpGroupRequest) (*pb.CatchUpResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.CatchUpResponse{}
	return resp, c.doRequest(ctx, "catch_up_group", req, resp)
}

// ListTopics pages through topics of a group.
func (c *Client) ListTopics(ctx context.Context, req *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.ListTopicsResponse{}
	return resp, c.doRequest(ctx, "list_topics", req, resp)
}

// ListMessages pages through messages of a topic.
func (c *Client) ListMessages(ctx context.Context, req *pb.ListMessagesRequest) (*pb.ListMessagesResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.ListMessagesResponse{}
	return resp, c.doRequest(ctx, "list_messages", req, resp)
}

// CreateMembership adds members to a space. Spaces only.
func (c *Client) CreateMembership(ctx context.Context, req *pb.CreateMembershipRequest) (*pb.CreateMembershipResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.CreateMembershipResponse{}
	return resp, c.doRequest(ctx, "create_membership", req, resp)
}

// RemoveMemberships removes members from a space; removing yourself is a leave. Spaces only.
func (c *Client) RemoveMemberships(ctx context.Context, req *pb.RemoveMembershipsRequest) (*pb.RemoveMembershipsResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.RemoveMembershipsResponse{}
	return resp, c.doRequest(ctx, "remove_memberships", req, resp)
}

// CreateDm creates or returns the existing DM; Invitees takes an email when the gaia id is unknown.
func (c *Client) CreateDm(ctx context.Context, req *pb.CreateDmRequest) (*pb.CreateDmResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.CreateDmResponse{}
	// Non-idempotent: a retried 5xx could create a second conversation.
	return resp, c.doRequestNonIdempotent(ctx, "create_dm", req, resp)
}

// CreateGroup creates a space. Not live-verified: real accounts get a bare HTTP 400.
func (c *Client) CreateGroup(ctx context.Context, req *pb.CreateGroupRequest) (*pb.CreateGroupResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.CreateGroupResponse{}
	return resp, c.doRequestNonIdempotent(ctx, "create_group", req, resp)
}

// UpdateGroupNotificationSettings mutes or unmutes one conversation.
func (c *Client) UpdateGroupNotificationSettings(ctx context.Context, req *pb.UpdateGroupNotificationSettingsRequest) (*pb.UpdateGroupNotificationSettingsResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.UpdateGroupNotificationSettingsResponse{}
	return resp, c.doRequest(ctx, "update_group_notification_settings", req, resp)
}

// HideGroup hides or unhides a conversation in the user's chat list (the only way to put a DM away).
func (c *Client) HideGroup(ctx context.Context, req *pb.HideGroupRequest) (*pb.HideGroupResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.HideGroupResponse{}
	return resp, c.doRequest(ctx, "hide_group", req, resp)
}

// UpdateGroup updates a space's metadata (used to rename). Spaces only.
func (c *Client) UpdateGroup(ctx context.Context, req *pb.UpdateGroupRequest) (*pb.UpdateGroupResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.UpdateGroupResponse{}
	return resp, c.doRequest(ctx, "update_group", req, resp)
}
