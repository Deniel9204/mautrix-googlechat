package gchatmeow

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

const (
	// defaultConnectMaxRetries is Channel.Listen's max_retries.
	defaultConnectMaxRetries = 3
	// defaultConnectRetryBackoffBase is Channel.Listen's retry backoff base.
	defaultConnectRetryBackoffBase = 2 * time.Second
	// defaultReconnectBackoffMin/Max bound the transient backoff (x2, capped).
	defaultReconnectBackoffMin = 4 * time.Second
	defaultReconnectBackoffMax = 60 * time.Second
	// defaultXSRFRefreshInterval is the token-staleness window.
	defaultXSRFRefreshInterval = 24 * time.Hour
	// maxSIDInvalidResyncs consecutive SID-invalid resyncs make the connection fatal.
	maxSIDInvalidResyncs = 3

	// defaultSIDFatalRetryFloor must exceed both the resync ladder (up to 180s) and bridgev2's 72s minimum auto-reconnect.
	defaultSIDFatalRetryFloor = 5 * time.Minute
	// defaultSIDFatalRetryCap bounds the post-fatal doubling; retrying never stops entirely.
	defaultSIDFatalRetryCap = time.Hour
)

// ConnState is a connection-state transition reported via Client.OnConnectionState.
type ConnState int

const (
	// ConnStateConnected: connected or reconnected; events are flowing.
	ConnStateConnected ConnState = iota
	// ConnStateTransient: recoverable disconnect; the loop will reconnect.
	ConnStateTransient
	// ConnStateBadCredentials: the session is dead; Connect returns.
	ConnStateBadCredentials
	// ConnStateFatal: SID-invalid storm; Connect waits and retries.
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
	// Cookies is the auth cookie set (COMPASS/SSID/SID/OSID/HSID).
	Cookies map[string]string
	// UserAgent is the browser UA; empty uses the default.
	UserAgent string
}

// channelListener is the subset of *Channel that Client uses, so tests can inject a fake.
type channelListener interface {
	Listen(ctx context.Context, maxRetries int, retryBackoffBase time.Duration) error
	SendStreamEvent(ctx context.Context, ev *pb.StreamEventsRequest) error
	SetOnReceiveArray(func(ctx context.Context, arr []byte) error)
	SetOnConnect(func(ctx context.Context))
	SetOnReconnect(func(ctx context.Context))
	SetOnDisconnect(func(ctx context.Context))
}

// NewClient builds a Client. It does not fetch an XSRF token or connect.
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
		sidFatalRetryFloor:  defaultSIDFatalRetryFloor,
		sidFatalRetryCap:    defaultSIDFatalRetryCap,
		xsrfRefreshInterval: defaultXSRFRefreshInterval,
	}
	c.newChannel = func() channelListener { return NewChannel(c.session) }
	return c, nil
}

// Cookies returns the current (post-rotation) auth cookies, for persistence.
func (c *Client) Cookies() map[string]string {
	return c.session.Cookies()
}

// UserAgent returns the normalized User-Agent actually sent, for persistence.
func (c *Client) UserAgent() string {
	return c.session.UserAgent()
}

// FetchXSRFToken validates the cookies and fetches an XSRF token unconditionally; returns ErrNotLoggedIn if rejected.
func (c *Client) FetchXSRFToken(ctx context.Context) error {
	return c.refreshXSRFToken(ctx)
}

// Connect runs the supervision loop on the caller's goroutine, which also runs every callback in order.
// It blocks until ctx is cancelled or Disconnect is called, and returns the error for bad credentials.
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

	if c.xsrfRefreshInterval > 0 {
		go c.xsrfRefreshLoop(ctx)
	}

	resyncCount := 0
	backoff := c.reconnectBackoffMin
	fatalBackoff := c.fatalRetryFloor()

	for {
		if ctx.Err() != nil {
			return nil
		}

		if err := c.ensureToken(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if IsAuthError(err) {
				c.emitState(ConnStateBadCredentials, err)
				return err
			}
			c.emitState(ConnStateTransient, err)
			if c.sleep(ctx, backoff) != nil {
				return nil
			}
			backoff = c.nextBackoff(backoff)
			continue
		}

		ch := c.newChannel()
		c.wireChannel(ch)
		c.setChannel(ch)

		err := ch.Listen(ctx, c.maxRetries, c.retryBackoffBase)

		if ctx.Err() != nil {
			return nil
		}
		switch {
		case errors.Is(err, ErrChannelLifetimeExpired):
			// 1.5h recycle: silent reconnect, no state change.
			resyncCount = 0
			backoff = c.reconnectBackoffMin
			// Only a full-lifetime channel proves health, so only it resets the post-fatal wait.
			fatalBackoff = c.fatalRetryFloor()
			continue
		case errors.Is(err, ErrSIDExpiring):
			resyncCount = 0
			backoff = c.reconnectBackoffMin
			continue
		case errors.Is(err, ErrSIDInvalid):
			// Resync up to maxSIDInvalidResyncs consecutive times, then fatal.
			resyncCount++
			if resyncCount > maxSIDInvalidResyncs {
				// Report the fatal, then retry on a slow tier instead of returning, which would kill the account.
				c.emitState(ConnStateFatal, err)
				// Drop the dead channel so SendStreamEvent fails fast during the wait.
				c.setChannel(nil)
				if c.sleep(ctx, fatalBackoff) != nil {
					return nil
				}
				fatalBackoff = c.growFatalBackoff(fatalBackoff)
				resyncCount = 0
				backoff = c.reconnectBackoffMin
				continue
			}
			// Pace resyncs with a growing backoff (do not reset to min).
			if c.sleep(ctx, backoff) != nil {
				return nil
			}
			backoff = c.nextBackoff(backoff)
			continue
		case IsAuthError(err):
			// 401 / invalid_grant / not-logged-in.
			c.emitState(ConnStateBadCredentials, err)
			return err
		case isNetworkError(err):
			resyncCount = 0
			c.emitState(ConnStateTransient, err)
			if c.sleep(ctx, backoff) != nil {
				return nil
			}
			backoff = c.nextBackoff(backoff)
			continue
		case err == nil:
			resyncCount = 0
			c.emitState(ConnStateTransient, nil)
			if c.sleep(ctx, backoff) != nil {
				return nil
			}
			backoff = c.nextBackoff(backoff)
			continue
		default:
			resyncCount = 0
			c.emitState(ConnStateTransient, err)
			if c.sleep(ctx, backoff) != nil {
				return nil
			}
			backoff = c.nextBackoff(backoff)
			continue
		}
	}
}

// Disconnect stops a running Connect; Connect then returns nil. Safe when not connected.
func (c *Client) Disconnect() {
	c.mu.Lock()
	cancel := c.cancel
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// SendStreamEvent sends a StreamEventsRequest (typing/active pings) on the live channel.
func (c *Client) SendStreamEvent(ctx context.Context, req *pb.StreamEventsRequest) error {
	ch := c.getChannel()
	if ch == nil {
		return errors.New("gchatmeow: not connected")
	}
	return ch.SendStreamEvent(ctx, req)
}

// wireChannel maps channel callbacks to state emitters.
// Events during a reconnect gap are not replayed; the connector must catch up.
func (c *Client) wireChannel(ch channelListener) {
	ch.SetOnReceiveArray(c.onReceiveArray)
	ch.SetOnConnect(func(ctx context.Context) { c.emitState(ConnStateConnected, nil) })
	ch.SetOnReconnect(func(ctx context.Context) { c.emitState(ConnStateConnected, nil) })
	ch.SetOnDisconnect(func(ctx context.Context) { c.emitState(ConnStateTransient, nil) })
}

// onReceiveArray decodes a data_array and dispatches each event body in order.
// Returning an error is terminal, so malformed frames are logged and skipped.
func (c *Client) onReceiveArray(ctx context.Context, arr []byte) error {
	var dataArray []json.RawMessage
	if err := json.Unmarshal(arr, &dataArray); err != nil {
		// Log and skip; never kill the channel.
		log.Debug().Err(err).Msg("gchatmeow: skipping non-array webchannel frame")
		return nil
	}
	if len(dataArray) == 0 {
		return nil
	}

	var maybeNoop string
	if err := json.Unmarshal(dataArray[0], &maybeNoop); err == nil && maybeNoop == "noop" {
		return nil
	}

	resp := &pb.StreamEventsResponse{}
	if err := pblite.Unmarshal(dataArray[0], resp); err != nil {
		log.Debug().Err(err).Msg("gchatmeow: skipping undecodable stream event")
		return nil
	}

	for _, ev := range splitEventBodies(resp.GetEvent()) {
		if c.OnStreamEvent != nil {
			c.OnStreamEvent(ctx, ev)
		}
	}
	return nil
}

// splitEventBodies flattens an Event with a body (field 4) and embedded bodies (field 8) into one Event per body. Mutates evt.
func splitEventBodies(evt *pb.Event) []*pb.Event {
	if evt == nil {
		return nil
	}
	embedded := evt.Bodies
	if len(embedded) > 0 {
		evt.Bodies = nil // clear the embedded bodies
	}

	var out []*pb.Event
	if evt.Body != nil { // parent has its own body
		out = append(out, evt)
	}
	for _, body := range embedded {
		cp := proto.Clone(evt).(*pb.Event)
		cp.Body = proto.Clone(body).(*pb.Event_EventBody) // swap this body in
		cp.Type = body.GetEventType().Enum()              // type from the body
		out = append(out, cp)
	}
	return out
}

// SplitEventBodies is splitEventBodies exported for the connector's catch-up replay.
func SplitEventBodies(evt *pb.Event) []*pb.Event {
	return splitEventBodies(evt)
}

// ensureToken refreshes the XSRF token when missing or older than xsrfRefreshInterval.
func (c *Client) ensureToken(ctx context.Context) error {
	c.mu.RLock()
	fresh := c.xsrfToken != "" && (c.xsrfRefreshInterval <= 0 || time.Since(c.lastTokenRefresh) < c.xsrfRefreshInterval)
	c.mu.RUnlock()
	if fresh {
		return nil
	}
	return c.refreshXSRFToken(ctx)
}

// refreshXSRFToken fetches a new token via /mole/world; concurrent callers share one fetch.
func (c *Client) refreshXSRFToken(ctx context.Context) error {
	before := c.XSRFToken()

	c.refreshMu.Lock()
	defer c.refreshMu.Unlock()

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

// xsrfRefreshAuthFailuresBeforeGivingUp is two so a single spurious sign-in page doesn't kill a healthy session.
const xsrfRefreshAuthFailuresBeforeGivingUp = 2

// xsrfRefreshLoop refreshes the token periodically. Network failures are ignored; confirmed auth failures report bad credentials and disconnect.
func (c *Client) xsrfRefreshLoop(ctx context.Context) {
	ticker := time.NewTicker(c.xsrfRefreshInterval)
	defer ticker.Stop()
	authFails := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := c.refreshXSRFToken(ctx)
			if err == nil {
				authFails = 0
				continue
			}
			if ctx.Err() != nil {
				return
			}
			if !IsAuthError(err) {
				// Reset so only consecutive auth failures count.
				authFails = 0
				log.Warn().Err(err).Msg("gchatmeow: periodic XSRF refresh failed")
				continue
			}
			authFails++
			if authFails < xsrfRefreshAuthFailuresBeforeGivingUp {
				log.Warn().Err(err).Int("consecutive", authFails).
					Msg("gchatmeow: XSRF refresh was rejected as unauthenticated; waiting for confirmation")
				continue
			}
			log.Warn().Err(err).Int("consecutive", authFails).
				Msg("gchatmeow: XSRF refresh rejected as unauthenticated twice; treating the session as logged out")
			c.emitState(ConnStateBadCredentials, err)
			c.Disconnect()
			return
		}
	}
}

// sleep waits for d or until ctx is done; tests override it via sleepFn.
func (c *Client) sleep(ctx context.Context, d time.Duration) error {
	if c.sleepFn != nil {
		return c.sleepFn(ctx, d)
	}
	return sleepOrDone(ctx, d)
}

// fatalRetryFloor defaults a zero field; a zero wait would busy-loop register.
func (c *Client) fatalRetryFloor() time.Duration {
	if c.sidFatalRetryFloor > 0 {
		return c.sidFatalRetryFloor
	}
	return defaultSIDFatalRetryFloor
}

// growFatalBackoff doubles the post-fatal wait up to sidFatalRetryCap.
func (c *Client) growFatalBackoff(cur time.Duration) time.Duration {
	ceiling := c.sidFatalRetryCap
	if ceiling <= 0 {
		ceiling = defaultSIDFatalRetryCap
	}
	next := cur * 2
	if next > ceiling {
		next = ceiling
	}
	if next <= 0 {
		next = c.fatalRetryFloor()
	}
	return next
}

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
