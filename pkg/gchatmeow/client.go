package gchatmeow

// Client orchestration ported from
// _reference/googlechat-python/maugclib/client.py (connect/disconnect,
// _on_receive_array, split_event_bodies, refresh_tokens) plus the supervision
// policy from _reference/googlechat-python/mautrix_googlechat/user.py
// (_start's reconnect ladder, docs/research/07 §6). The RPC layer and the
// Client struct definition live in api.go; this file adds the connection
// lifecycle on top.
//
// # Goroutine model
//
// Connect(ctx) runs the supervision loop ON THE CALLER'S GOROUTINE and blocks
// until ctx is cancelled or a terminal (fatal/bad-credentials) error occurs.
// Within each iteration it calls channel.Listen SYNCHRONOUSLY, so Listen -- and
// every callback it fires (OnReceiveArray -> OnStreamEvent, and
// OnConnect/OnReconnect/OnDisconnect -> OnConnectionState) -- runs on that same
// single goroutine, delivering events serially and in order (matching Python's
// single-threaded asyncio model). The ONLY other goroutines Client spawns are:
//   - the channel's internal long-poll reader (Task 7), which never invokes
//     callbacks; and
//   - the XSRF refresh timer (xsrfRefreshLoop), which only refreshes the token
//     and never invokes OnStreamEvent/OnConnectionState.
//
// External goroutines may call the /api/* RPC methods (which may trigger a
// 401 token refresh) and Disconnect concurrently; the shared state they touch
// (xsrfToken, lastTokenRefresh, channel, cancel) is guarded by mu, and the
// token fetch itself is serialized by refreshMu. Callbacks (OnStreamEvent,
// OnConnectionState) must be set before Connect and are then read-only.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/pblite"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// Supervision defaults, mirroring the values user.py/client.py pass in Python.
const (
	// defaultConnectMaxRetries is Channel.Listen's max_retries (user.py:264
	// builds Client with max_retries=3).
	defaultConnectMaxRetries = 3
	// defaultConnectRetryBackoffBase is Channel.Listen's retry_backoff_base
	// (user.py:265 retry_backoff_base=2). Python's base is an integer number
	// of seconds; Task 7's Channel takes a Duration.
	defaultConnectRetryBackoffBase = 2 * time.Second
	// defaultReconnectBackoffMin/Max bound the supervision loop's own transient
	// backoff. Python (user.py:302,367-382) starts at 4s, grows x1.5 with an
	// UNCAPPED sleep, resets to 4s after 60s of stability, and escalates the
	// REPORTED STATE to UNKNOWN_ERROR once backoff exceeds 60s (i.e. `backoff >
	// 60` is a state threshold, not a sleep cap). This port simplifies to a x2
	// schedule capped at 60s (nextBackoff), reset on any non-network terminal
	// outcome, and does NOT model the separate UNKNOWN_ERROR escalation -- every
	// transient disconnect reports ConnStateTransient. Documented divergence
	// (timing/observability only; the connector's bridge-state layer in Task 11
	// can reintroduce the persistent-vs-blip signal if wanted).
	defaultReconnectBackoffMin = 4 * time.Second
	defaultReconnectBackoffMax = 60 * time.Second
	// defaultXSRFRefreshInterval is the 24h token-staleness window
	// (client.py:147, 86400 seconds).
	defaultXSRFRefreshInterval = 24 * time.Hour
	// maxSIDInvalidResyncs is the ladder's resync cap: after this many
	// consecutive SID-invalid resyncs without an intervening healthy cycle,
	// the connection is declared fatal (brief/research 07 §6).
	maxSIDInvalidResyncs = 3
)

// ConnState is a connection-state transition reported via
// Client.OnConnectionState.
type ConnState int

const (
	// ConnStateConnected: the channel connected or reconnected and events are
	// flowing (Python on_connect / on_reconnect -> CONNECTED).
	ConnStateConnected ConnState = iota
	// ConnStateTransient: a recoverable disconnect; the loop is backing off and
	// will reconnect (Python TRANSIENT_DISCONNECT).
	ConnStateTransient
	// ConnStateBadCredentials: the session is dead (401 / invalid_grant /
	// not-logged-in); Connect returns (Python BAD_CREDENTIALS + logout).
	ConnStateBadCredentials
	// ConnStateFatal: an unrecoverable condition (SID-invalid storm); Connect
	// returns.
	ConnStateFatal
)

func (s ConnState) String() string {
	switch s {
	case ConnStateConnected:
		return "CONNECTED"
	case ConnStateTransient:
		return "TRANSIENT"
	case ConnStateBadCredentials:
		return "BAD_CREDENTIALS"
	case ConnStateFatal:
		return "FATAL"
	default:
		return fmt.Sprintf("ConnState(%d)", int(s))
	}
}

// ClientOpts configures NewClient.
type ClientOpts struct {
	// Cookies is the 5-cookie auth set (COMPASS/SSID/SID/OSID/HSID). Keys are
	// upper-cased by NewSession.
	Cookies map[string]string
	// UserAgent is the browser UA to present; empty uses the default, and a
	// non-empty one has its Chrome/Firefox version pinned (see NewSession).
	UserAgent string
}

// channelListener is the subset of *Channel that Client depends on. It exists
// solely so the supervision loop can be tested against a scripted fake: the
// concrete *Channel (channel.go) is not otherwise an interface. Both *Channel
// and the test fake satisfy it. The Set* methods mirror *Channel's exported
// callback fields (channel.go adds thin setters for them).
type channelListener interface {
	Listen(ctx context.Context, maxRetries int, retryBackoffBase time.Duration) error
	SendStreamEvent(ctx context.Context, ev *pb.StreamEventsRequest) error
	SetOnReceiveArray(func(ctx context.Context, arr []byte) error)
	SetOnConnect(func(ctx context.Context))
	SetOnReconnect(func(ctx context.Context))
	SetOnDisconnect(func(ctx context.Context))
}

// NewClient builds a Client with an authenticated Session and the Python
// bridge's supervision defaults. It does NOT fetch an XSRF token or connect --
// Connect does that lazily (see ensureToken), matching Python where
// refresh_tokens/connect are separate steps.
func NewClient(opts ClientOpts) (*Client, error) {
	sess, err := NewSession(opts.Cookies, opts.UserAgent)
	if err != nil {
		return nil, err
	}
	c := &Client{
		session:             sess,
		maxRetries:          defaultConnectMaxRetries,
		retryBackoffBase:    defaultConnectRetryBackoffBase,
		reconnectBackoffMin: defaultReconnectBackoffMin,
		reconnectBackoffMax: defaultReconnectBackoffMax,
		xsrfRefreshInterval: defaultXSRFRefreshInterval,
	}
	c.newChannel = func() channelListener { return NewChannel(c.session) }
	return c, nil
}

// Cookies returns the current (post-rotation) values of the required auth
// cookies, for persistence. Mirrors client.py's `cookies` property
// (client.py:132-134 -> Session.get_auth_cookies).
func (c *Client) Cookies() map[string]string {
	return c.session.Cookies()
}

// Connect runs the supervision loop: refresh the XSRF token if stale, register
// and long-poll via a fresh Channel, and on each terminal Listen result consult
// the ladder (research 07 §6 / user.py:299-388) to reconnect, back off, or
// stop. It blocks until ctx is cancelled (or Disconnect is called), returning
// nil for a clean stop, or the terminal error for a fatal / bad-credentials
// outcome.
func (c *Client) Connect(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	c.mu.Lock()
	c.cancel = cancel
	c.mu.Unlock()
	defer func() {
		cancel()
		c.mu.Lock()
		c.cancel = nil
		c.channel = nil
		c.mu.Unlock()
	}()

	// XSRF refresh timer (brief: "24h periodic"). Runs concurrently with the
	// loop and stops when ctx is cancelled.
	if c.xsrfRefreshInterval > 0 {
		go c.xsrfRefreshLoop(ctx)
	}

	resyncCount := 0
	backoff := c.reconnectBackoffMin

	for {
		if ctx.Err() != nil {
			return nil // clean stop (Disconnect or parent cancel)
		}

		// 24h staleness check before (re)connecting (client.py:147-149). A dead
		// session surfaces here as an auth error -> BadCredentials; a transient
		// failure backs off and retries.
		if err := c.ensureToken(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if IsAuthError(err) {
				c.emitState(ConnStateBadCredentials, err)
				return err
			}
			c.emitState(ConnStateTransient, err)
			if sleepOrDone(ctx, backoff) != nil {
				return nil
			}
			backoff = c.nextBackoff(backoff)
			continue
		}

		ch := c.newChannel()
		c.wireChannel(ch)
		c.setChannel(ch)

		err := ch.Listen(ctx, c.maxRetries, c.retryBackoffBase)

		// ---- supervision ladder (research 07 §6 / user.py:299-388) ----
		if ctx.Err() != nil {
			return nil // ctx cancelled mid-Listen -> clean return
		}
		switch {
		case errors.Is(err, ErrChannelLifetimeExpired):
			// 1.5h recycle: silent reconnect, no state change (user.py:322-325).
			resyncCount = 0
			backoff = c.reconnectBackoffMin
			continue
		case errors.Is(err, ErrSIDExpiring):
			// SID expiring surfaced to the supervisor: reconnect immediately
			// (Channel normally re-registers internally; if it propagates, the
			// brief says reconnect at once).
			resyncCount = 0
			backoff = c.reconnectBackoffMin
			continue
		case errors.Is(err, ErrSIDInvalid):
			// Unknown SID: resync by reconnecting, up to maxSIDInvalidResyncs
			// consecutive times; beyond that the SID is persistently bad and the
			// connection is fatal (user.py:326-351 + brief's cap).
			resyncCount++
			if resyncCount > maxSIDInvalidResyncs {
				c.emitState(ConnStateFatal, err)
				return err
			}
			backoff = c.reconnectBackoffMin
			continue
		case IsAuthError(err):
			// 401 / invalid_grant / not-logged-in -> dead session
			// (user.py:357-364 logs out).
			c.emitState(ConnStateBadCredentials, err)
			return err
		case isNetworkError(err):
			// Transient transport failure: back off and reconnect
			// (user.py:352-382).
			resyncCount = 0
			c.emitState(ConnStateTransient, err)
			if sleepOrDone(ctx, backoff) != nil {
				return nil
			}
			backoff = c.nextBackoff(backoff)
			continue
		case err == nil:
			// Listen returned without error and without cancellation: Python
			// treats this "finished unexpectedly" as the backoff branch
			// (user.py:319-321).
			resyncCount = 0
			c.emitState(ConnStateTransient, nil)
			if sleepOrDone(ctx, backoff) != nil {
				return nil
			}
			backoff = c.nextBackoff(backoff)
			continue
		default:
			// Any other error (e.g. a non-401 UnexpectedStatusError or a
			// register failure): treat as transient and back off
			// (user.py:352-365, the generic Exception branch).
			resyncCount = 0
			c.emitState(ConnStateTransient, err)
			if sleepOrDone(ctx, backoff) != nil {
				return nil
			}
			backoff = c.nextBackoff(backoff)
			continue
		}
	}
}

// Disconnect gracefully stops a running Connect by cancelling its derived
// context; Connect then returns nil. Safe to call concurrently and when not
// connected (no-op). Mirrors client.py:172-180 (cancel the listen task).
func (c *Client) Disconnect() {
	c.mu.Lock()
	cancel := c.cancel
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// SendStreamEvent forwards a StreamEventsRequest on the live channel's forward
// channel (typing/active pings). Mirrors client.py:811-814
// (proto_send_stream_event -> channel.send_stream_event). Returns an error if
// there is no live channel.
func (c *Client) SendStreamEvent(ctx context.Context, req *pb.StreamEventsRequest) error {
	ch := c.getChannel()
	if ch == nil {
		return errors.New("gchatmeow: not connected")
	}
	return ch.SendStreamEvent(ctx, req)
}

// wireChannel connects a channel's callbacks to the Client's state emitters.
// on_connect/on_reconnect both map to CONNECTED and on_disconnect to TRANSIENT,
// matching user.py:432-572. OnReceiveArray drives split_event_bodies dispatch.
//
// NOTE for the connector (Task 11/12): a CONNECTED emitted after a prior
// connection is a RECONNECT, and a SID-expiring re-register resets the channel
// AID to 0, so events during the gap are not server-replayed -- the connector
// should trigger a catch-up/gap-sync on reconnect. The client library itself
// has no sync capability, so that decision belongs to the connector, driven off
// these OnConnectionState transitions.
func (c *Client) wireChannel(ch channelListener) {
	ch.SetOnReceiveArray(c.onReceiveArray)
	ch.SetOnConnect(func(ctx context.Context) { c.emitState(ConnStateConnected, nil) })
	ch.SetOnReconnect(func(ctx context.Context) { c.emitState(ConnStateConnected, nil) })
	ch.SetOnDisconnect(func(ctx context.Context) { c.emitState(ConnStateTransient, nil) })
}

// onReceiveArray decodes one webchannel data_array into a StreamEventsResponse,
// flattens its event via split_event_bodies, and dispatches each result to
// OnStreamEvent synchronously and in order. Ports client.py:545-563
// (_on_receive_array).
//
// Returning an error here is TERMINAL (it tears down the poll), so per
// docs/research/07 risk #4 decoding is permissive: a malformed frame is logged
// and skipped rather than propagated. The only hard structural requirement is
// that the data_array is JSON.
func (c *Client) onReceiveArray(ctx context.Context, arr []byte) error {
	var dataArray []json.RawMessage
	if err := json.Unmarshal(arr, &dataArray); err != nil {
		// A non-array frame is a structural anomaly; log and skip rather than
		// kill the channel.
		log.Debug().Err(err).Msg("gchatmeow: skipping non-array webchannel frame")
		return nil
	}
	if len(dataArray) == 0 {
		return nil
	}
	// "noop" keep-alive (client.py:547-548): data_array[0] == "noop".
	var maybeNoop string
	if err := json.Unmarshal(dataArray[0], &maybeNoop); err == nil && maybeNoop == "noop" {
		return nil
	}

	// data_array[0] is the StreamEventsResponse pblite array (client.py:550-553).
	resp := &pb.StreamEventsResponse{}
	if err := pblite.Unmarshal(dataArray[0], resp); err != nil {
		log.Debug().Err(err).Msg("gchatmeow: skipping undecodable stream event")
		return nil
	}

	for _, ev := range splitEventBodies(resp.GetEvent()) {
		if c.OnStreamEvent != nil {
			c.OnStreamEvent(ctx, ev) // synchronous, in order (client.py:561-563)
		}
	}
	return nil
}

// splitEventBodies flattens an Event that carries a top-level body (field 4)
// plus repeated embedded bodies (field 8) into one Event per body, copying the
// parent's other fields onto each. Exact port of client.py:565-580
// (split_event_bodies):
//
//   - if there are embedded bodies, they are removed from the parent
//     (ClearField("bodies"));
//   - if the parent has its own body, the parent is yielded first, as-is;
//   - then one copy per embedded body, with that body swapped into the top-level
//     body field and the copy's type set from body.event_type.
//
// Like the Python original this mutates evt (clearing its bodies). evt is a
// freshly-decoded message owned by onReceiveArray, so that is safe.
func splitEventBodies(evt *pb.Event) []*pb.Event {
	if evt == nil {
		return nil
	}
	embedded := evt.Bodies
	if len(embedded) > 0 {
		evt.Bodies = nil // ClearField("bodies")
	}

	var out []*pb.Event
	if evt.Body != nil { // HasField("body")
		out = append(out, evt)
	}
	for _, body := range embedded {
		cp := proto.Clone(evt).(*pb.Event)
		cp.Body = proto.Clone(body).(*pb.Event_EventBody) // evt_copy.body.CopyFrom(body)
		cp.Type = body.GetEventType().Enum()              // evt_copy.type = body.event_type
		out = append(out, cp)
	}
	return out
}

// ensureToken refreshes the XSRF token when it is missing or older than
// xsrfRefreshInterval, mirroring client.py:147-149's pre-connect check. A fresh
// token is a no-op.
func (c *Client) ensureToken(ctx context.Context) error {
	c.mu.RLock()
	fresh := c.xsrfToken != "" && (c.xsrfRefreshInterval <= 0 || time.Since(c.lastTokenRefresh) < c.xsrfRefreshInterval)
	c.mu.RUnlock()
	if fresh {
		return nil
	}
	return c.refreshXSRFToken(ctx)
}

// refreshXSRFToken fetches a new token via /mole/world and stores it. Ports the
// side-effecting half of client.py:499-539 (refresh_tokens) that assigns
// self.xsrf_token / self._last_token_refresh; the fetch+scrape itself is
// Session.FetchXSRFToken (auth.go). refreshMu serializes concurrent refreshes
// (e.g. several RPCs hitting 401 at once) so only one /mole/world round-trip
// happens; a refresh that already ran while we waited for the lock is reused.
func (c *Client) refreshXSRFToken(ctx context.Context) error {
	before := c.XSRFToken()

	c.refreshMu.Lock()
	defer c.refreshMu.Unlock()

	// If another goroutine refreshed while we blocked on refreshMu, reuse it.
	if cur := c.XSRFToken(); cur != "" && cur != before {
		return nil
	}

	token, err := c.session.FetchXSRFToken(ctx)
	if err != nil {
		return err
	}
	c.SetXSRFToken(token)
	c.mu.Lock()
	c.lastTokenRefresh = time.Now()
	c.mu.Unlock()
	return nil
}

// xsrfRefreshLoop periodically refreshes the XSRF token every
// xsrfRefreshInterval until ctx is cancelled. A refresh failure is logged and
// ignored: the connection's own Listen/RPC paths surface a genuinely dead
// session through the supervision ladder, so this best-effort timer must not
// itself tear anything down.
func (c *Client) xsrfRefreshLoop(ctx context.Context) {
	ticker := time.NewTicker(c.xsrfRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.refreshXSRFToken(ctx); err != nil && ctx.Err() == nil {
				log.Warn().Err(err).Msg("gchatmeow: periodic XSRF refresh failed")
			}
		}
	}
}

// nextBackoff doubles the current transient backoff up to reconnectBackoffMax.
func (c *Client) nextBackoff(cur time.Duration) time.Duration {
	next := cur * 2
	if c.reconnectBackoffMax > 0 && next > c.reconnectBackoffMax {
		next = c.reconnectBackoffMax
	}
	if next <= 0 {
		next = c.reconnectBackoffMin
	}
	return next
}

// emitState reports a connection-state transition, if a callback is set.
func (c *Client) emitState(state ConnState, err error) {
	if c.OnConnectionState != nil {
		c.OnConnectionState(state, err)
	}
}

func (c *Client) getChannel() channelListener {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.channel
}

func (c *Client) setChannel(ch channelListener) {
	c.mu.Lock()
	c.channel = ch
	c.mu.Unlock()
}
