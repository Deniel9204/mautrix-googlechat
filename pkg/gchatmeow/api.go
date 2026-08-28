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

// Constants ported VERBATIM from the Google Chat web client. A live protocol
// spike should validate these against the real Google Chat server before this
// client is trusted in production.
const (
	// apiKey is the `key` query param on every /api/* request -- inherited
	// from the Hangouts web client, required to avoid a 403 "Daily Limit
	// for Unauthenticated Use Exceeded".
	apiKey = "AIzaSyD7InnYR3VKdb4j2rMUEbTCIr2VyEazl6k"

	// apiBaseURL is the /api/* base URL. Overridable per-Client via the
	// baseURL field for tests.
	apiBaseURL = "https://chat.google.com/u/0"

	// clientVersion is the RequestHeader.client_version stamped on every
	// request (RequestHeader schema at googlechat.proto:106-139).
	clientVersion = int64(2440378181258)
)

// Client is the binary-proto RPC layer for the Google Chat private web API:
// one method per /api/* endpoint, all POSTing binary protobuf to
// https://chat.google.com/u/0/api/*.
//
// This type carries ONLY the fields the RPC layer itself needs. The
// BrowserChannel/callback/supervision fields are added to this same struct in
// client.go (same package) -- methods living in separate files within one
// package is intentional, not a layering violation.
type Client struct {
	// session is the authenticated HTTP layer (cookies, retries) every RPC
	// is sent through.
	session *Session

	// mu guards xsrfToken, which the connect loop may refresh
	// concurrently with in-flight RPCs.
	mu        sync.RWMutex
	xsrfToken string

	// requestCounter is the `c` query param -- an incrementing per-Client
	// request id. The server appears to ignore duplicates; kept only "to not
	// stand out" from the real web client. atomic because RPCs may be issued
	// concurrently from multiple goroutines.
	requestCounter atomic.Int64

	// baseURL overrides apiBaseURL when non-empty, so tests can point a
	// Client at an httptest server instead of the real Google Chat host.
	baseURL string

	// uploadBaseURL overrides uploadURL (upload.go) when non-empty, same
	// seam as baseURL above but for the separate /uploads endpoint (which
	// is NOT under apiBaseURL's /u/0/api/ path).
	uploadBaseURL string

	// --- Orchestration state (methods live in client.go) ---
	//
	// OnStreamEvent is called once per flattened event body, synchronously and
	// in order, from the Connect goroutine (see client.go's goroutine model).
	// Set before Connect; read-only afterwards.
	OnStreamEvent func(ctx context.Context, ev *pb.Event)
	// OnConnectionState reports connection-state transitions
	// (CONNECTED/TRANSIENT/BAD_CREDENTIALS/FATAL), synchronously from the
	// Connect goroutine. Set before Connect; read-only afterwards.
	OnConnectionState func(state ConnState, err error)

	// newChannel builds a fresh channel per (re)connect. Defaults to a real
	// *Channel bound to session; overridable in tests to inject a fake
	// (channelListener). Never mutated after construction.
	newChannel func() channelListener

	// channel is the live channel for the current connect cycle, guarded by
	// mu: the Connect goroutine swaps it each iteration while an external
	// SendStreamEvent caller reads it.
	channel channelListener

	// cancel cancels the Connect loop's derived context; Disconnect calls it.
	// Guarded by mu.
	cancel context.CancelFunc

	// Supervision tunables (defaults from NewClient; overridable in tests).
	maxRetries          int           // passed to Channel.Listen (default 3)
	retryBackoffBase    time.Duration // passed to Channel.Listen (default 2)
	reconnectBackoffMin time.Duration // supervision-loop transient backoff floor (default 4s)
	reconnectBackoffMax time.Duration // supervision-loop transient backoff ceiling (default 60s)

	// XSRF refresh scheduling (a 24h staleness check plus a 401-triggered
	// refresh). lastTokenRefresh is guarded by mu;
	// refreshMu serializes the fetch itself so concurrent 401s don't stampede
	// /mole/world.
	xsrfRefreshInterval time.Duration
	lastTokenRefresh    time.Time
	refreshMu           sync.Mutex

	// sleepFn, when non-nil, replaces sleepOrDone for the supervision loop's
	// backoff waits. Tests inject it to observe pacing deterministically
	// without real sleeps; nil in production. Set before Connect.
	sleepFn func(ctx context.Context, d time.Duration) error
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

// newRequestHeader builds the RequestHeader every /api/* request carries:
// client_type=WEB, client_version=2440378181258, and
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

// doRequest wraps doRequestOnce with a 401-triggered XSRF refresh:
// a stale x-framework-xsrf-token surfaces as HTTP 401, so on that (and only
// that) status we re-fetch the token via /mole/world (client.go's
// refreshXSRFToken) and retry the RPC exactly once. Any other error -- and a
// second 401 after the refresh -- propagates to the caller unchanged. This
// covers every /api/* RPC uniformly, not just the ones issued from Connect.
func (c *Client) doRequest(ctx context.Context, endpoint string, requestPB, responsePB proto.Message) error {
	return c.doRequestWithPolicy(ctx, endpoint, requestPB, responsePB, true)
}

// doRequestNonIdempotent is doRequest for an RPC that must not be repeated
// once the server may have acted on it -- the message creates. Its only
// difference is that a 5xx is surfaced instead of retried; see
// Session.FetchNonIdempotent for why a duplicate message is the worse
// outcome. The 401 refresh-and-retry below still applies: a 401 means the
// request was rejected outright, so replaying it cannot duplicate anything.
func (c *Client) doRequestNonIdempotent(ctx context.Context, endpoint string, requestPB, responsePB proto.Message) error {
	return c.doRequestWithPolicy(ctx, endpoint, requestPB, responsePB, false)
}

func (c *Client) doRequestWithPolicy(ctx context.Context, endpoint string, requestPB, responsePB proto.Message, idempotent bool) error {
	err := c.doRequestOnce(ctx, endpoint, requestPB, responsePB, idempotent)
	if err == nil || !isUnauthorizedStatus(err) {
		return err
	}
	if refreshErr := c.refreshXSRFToken(ctx); refreshErr != nil {
		// Refresh failed (e.g. cookies dead -> ErrNotLoggedIn): surface the
		// original 401 so the caller's auth handling sees a consistent signal.
		return err
	}
	proto.Reset(responsePB)
	return c.doRequestOnce(ctx, endpoint, requestPB, responsePB, idempotent)
}

// isUnauthorizedStatus reports whether err is (or wraps) an
// *UnexpectedStatusError carrying HTTP 401 -- the signal that the XSRF token
// is stale and a refresh+retry is worth attempting.
func isUnauthorizedStatus(err error) bool {
	var ue *UnexpectedStatusError
	return errors.As(err, &ue) && ue.Status == http.StatusUnauthorized
}

// doRequestOnce issues one raw /api/* request: marshal requestPB to binary
// protobuf, POST it to
// {baseURL}/api/{endpoint}?c=<counter>&rt=b&alt=proto&key=<apiKey>, and
// unmarshal the response body into responsePB.
//
// Wire format:
//
//	POST https://chat.google.com/u/0/api/{endpoint}?c={reqid}&rt=b&alt=proto&key={API_KEY}
//	Content-Type: application/x-protobuf
//	X-Goog-Encode-Response-If-Executable: base64
//	x-framework-xsrf-token: <token>
//	<binary serialized request proto>
//
// Non-200 responses surface as *UnexpectedStatusError with the status
// preserved (Session.Fetch's job), so callers can map a specific status
// (e.g. 401 -> BAD_CREDENTIALS) instead of seeing an opaque network error.
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

// unmarshalAPIResponse implements the dual binary/base64 response path:
// /api/* responses are ordinarily raw binary protobuf, parsed directly. But
// the
// X-Goog-Encode-Response-If-Executable: base64 header this client sends on
// every request means the server MAY base64-encode the body instead -- so if
// the raw bytes don't parse as valid protobuf, this retries the same bytes
// after a base64 decode before giving up.
//
// Residual risk (worth carrying forward to a live spike rather than fixed
// here): protobuf's wire format is permissive enough that a genuinely
// base64-encoded body could in principle parse "successfully" as a
// differently-structured, wrong message on the FIRST attempt, before ever
// reaching the base64 fallback -- silently returning a wrong-but-non-nil
// response instead of an error. There's no header or other positive signal
// to disambiguate the two encodings up front; this is inherent to the "try
// raw, then try base64" strategy. Low probability for realistically-sized
// responses, but worth an explicit live-server check.
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

// The 16 /api/* RPCs. Each wrapper stamps
// request_header on the caller-supplied request in place (mirroring
// googlechat-megabridge/pkg/gchatmeow/api.go's `request.RequestHeader =
// c.gcRequestHeader` pattern) and returns a freshly-allocated response.

// GetUserPresence returns presence for one or more users.
// Endpoint: get_user_presence.
func (c *Client) GetUserPresence(ctx context.Context, req *pb.GetUserPresenceRequest) (*pb.GetUserPresenceResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.GetUserPresenceResponse{}
	return resp, c.doRequest(ctx, "get_user_presence", req, resp)
}

// GetMembers resolves user/member info by id.
// Endpoint: get_members.
func (c *Client) GetMembers(ctx context.Context, req *pb.GetMembersRequest) (*pb.GetMembersResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.GetMembersResponse{}
	return resp, c.doRequest(ctx, "get_members", req, resp)
}

// PaginatedWorld lists all conversations ("world view"); used for chat sync.
// Endpoint: paginated_world.
func (c *Client) PaginatedWorld(ctx context.Context, req *pb.PaginatedWorldRequest) (*pb.PaginatedWorldResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.PaginatedWorldResponse{}
	return resp, c.doRequest(ctx, "paginated_world", req, resp)
}

// GetSelfUserStatus returns the current user's own status, including Gaia
// ID and user revision.
// Endpoint: get_self_user_status.
func (c *Client) GetSelfUserStatus(ctx context.Context, req *pb.GetSelfUserStatusRequest) (*pb.GetSelfUserStatusResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.GetSelfUserStatusResponse{}
	return resp, c.doRequest(ctx, "get_self_user_status", req, resp)
}

// GetGroup fetches one group/DM, including membership.
// Endpoint: get_group.
func (c *Client) GetGroup(ctx context.Context, req *pb.GetGroupRequest) (*pb.GetGroupResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.GetGroupResponse{}
	return resp, c.doRequest(ctx, "get_group", req, resp)
}

// MarkGroupReadstate sends a read receipt / last-read time.
// Endpoint: mark_group_readstate.
func (c *Client) MarkGroupReadstate(ctx context.Context, req *pb.MarkGroupReadstateRequest) (*pb.MarkGroupReadstateResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.MarkGroupReadstateResponse{}
	return resp, c.doRequest(ctx, "mark_group_readstate", req, resp)
}

// CreateTopic sends a message that starts a new topic/thread.
// Endpoint: create_topic.
func (c *Client) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.CreateTopicResponse{}
	// Non-idempotent: retrying after a 5xx could post the message twice.
	return resp, c.doRequestNonIdempotent(ctx, "create_topic", req, resp)
}

// CreateMessage sends a message into an existing thread.
// Endpoint: create_message.
func (c *Client) CreateMessage(ctx context.Context, req *pb.CreateMessageRequest) (*pb.CreateMessageResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.CreateMessageResponse{}
	// Non-idempotent: retrying after a 5xx could post the message twice.
	return resp, c.doRequestNonIdempotent(ctx, "create_message", req, resp)
}

// UpdateReaction adds/removes an emoji reaction.
// Endpoint: update_reaction.
func (c *Client) UpdateReaction(ctx context.Context, req *pb.UpdateReactionRequest) (*pb.UpdateReactionResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.UpdateReactionResponse{}
	return resp, c.doRequest(ctx, "update_reaction", req, resp)
}

// DeleteMessage deletes/redacts a message.
// Endpoint: delete_message.
func (c *Client) DeleteMessage(ctx context.Context, req *pb.DeleteMessageRequest) (*pb.DeleteMessageResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.DeleteMessageResponse{}
	return resp, c.doRequest(ctx, "delete_message", req, resp)
}

// EditMessage edits message text.
// Endpoint: edit_message.
func (c *Client) EditMessage(ctx context.Context, req *pb.EditMessageRequest) (*pb.EditMessageResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.EditMessageResponse{}
	return resp, c.doRequest(ctx, "edit_message", req, resp)
}

// SetTypingState sends a typing notification (group or topic context).
// Endpoint: set_typing_state.
func (c *Client) SetTypingState(ctx context.Context, req *pb.SetTypingStateRequest) (*pb.SetTypingStateResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.SetTypingStateResponse{}
	return resp, c.doRequest(ctx, "set_typing_state", req, resp)
}

// CatchUpUser replays missed events for the whole user since a revision.
// Endpoint: catch_up_user.
//
// NOTE (proto type mismatch): the response type might be expected to be
// *pb.CatchUpUserResponse, but pkg/gchatmeow/proto/googlechat.pb.go has no
// such type -- the catch_up_user and catch_up_group endpoints both return
// the SAME CatchUpResponse, and our generated proto matches that (only
// CatchUpResponse exists, not CatchUpUserResponse/CatchUpGroupResponse).
// This wrapper returns *pb.CatchUpResponse accordingly.
func (c *Client) CatchUpUser(ctx context.Context, req *pb.CatchUpUserRequest) (*pb.CatchUpResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.CatchUpResponse{}
	return resp, c.doRequest(ctx, "catch_up_user", req, resp)
}

// CatchUpGroup replays missed events for one group since a revision.
// Endpoint: catch_up_group. See CatchUpUser's doc comment for the
// CatchUpResponse type-name note.
func (c *Client) CatchUpGroup(ctx context.Context, req *pb.CatchUpGroupRequest) (*pb.CatchUpResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.CatchUpResponse{}
	return resp, c.doRequest(ctx, "catch_up_group", req, resp)
}

// ListTopics pages through topics of a group (backfill).
// Endpoint: list_topics.
func (c *Client) ListTopics(ctx context.Context, req *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.ListTopicsResponse{}
	return resp, c.doRequest(ctx, "list_topics", req, resp)
}

// ListMessages pages through messages of a topic (backfill).
// Endpoint: list_messages.
func (c *Client) ListMessages(ctx context.Context, req *pb.ListMessagesRequest) (*pb.ListMessagesResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.ListMessagesResponse{}
	return resp, c.doRequest(ctx, "list_messages", req, resp)
}

// CreateMembership adds member(s) to a space (an invite/add). Spaces only.
// Endpoint: create_membership.
func (c *Client) CreateMembership(ctx context.Context, req *pb.CreateMembershipRequest) (*pb.CreateMembershipResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.CreateMembershipResponse{}
	return resp, c.doRequest(ctx, "create_membership", req, resp)
}

// RemoveMemberships removes member(s) from a space. Removing the logged-in
// user's own id is how a "leave" is performed. Spaces only.
// Endpoint: remove_memberships.
func (c *Client) RemoveMemberships(ctx context.Context, req *pb.RemoveMembershipsRequest) (*pb.RemoveMembershipsResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.RemoveMembershipsResponse{}
	return resp, c.doRequest(ctx, "remove_memberships", req, resp)
}

// CreateDm creates (or, for a pair that already has one, returns) the direct
// message with the given member(s).
//
// A DM is unique per pair on Google Chat, so this doubles as "find the
// existing DM": create_dm with a member who already has one returns that
// one rather than a second. Members carries a resolved gaia id; Invitees
// carries an email for a user this account has never interacted with, which
// is the only way to reach someone whose gaia id is unknown -- the private
// API exposes no email-to-gaia lookup.
// Endpoint: create_dm.
func (c *Client) CreateDm(ctx context.Context, req *pb.CreateDmRequest) (*pb.CreateDmResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.CreateDmResponse{}
	// Non-idempotent for the same reason the message creates are: a 5xx
	// returned after the server acted would create a second conversation.
	return resp, c.doRequestNonIdempotent(ctx, "create_dm", req, resp)
}

// CreateGroup creates a space. should_find_existing_space asks the server to
// return an equivalent existing space instead of making a duplicate.
// Endpoint: create_group.
func (c *Client) CreateGroup(ctx context.Context, req *pb.CreateGroupRequest) (*pb.CreateGroupResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.CreateGroupResponse{}
	return resp, c.doRequestNonIdempotent(ctx, "create_group", req, resp)
}

// UpdateGroup updates a space's metadata; used here to rename a space (Name +
// the NAME update mask). Spaces only -- the request has no DM arm.
// Endpoint: update_group.
func (c *Client) UpdateGroup(ctx context.Context, req *pb.UpdateGroupRequest) (*pb.UpdateGroupResponse, error) {
	req.RequestHeader = newRequestHeader()
	resp := &pb.UpdateGroupResponse{}
	return resp, c.doRequest(ctx, "update_group", req, resp)
}
