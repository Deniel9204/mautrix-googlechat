package gchatmeow

// BrowserChannel long-poll client, ported from
// _reference/googlechat-python/maugclib/channel.py (the authoritative spec;
// docs/research/01 §2, docs/research/07 risk #2). This is the single most
// fidelity-critical module: subtle bugs cause silent event loss.
//
// Deliberate divergences from the second reference
// (_reference/googlechat-megabridge/pkg/gchatmeow/channel.go), whose four
// documented defects (docs/research/08c) this port must NOT reproduce:
//   1. megabridge set a 90s http.Client.Timeout that killed every long poll;
//      here the poll uses Session.FetchRaw (pollClient.Timeout == 0) and we
//      own cancellation via a 60s read-idle watchdog (not a request deadline).
//   2. megabridge ran Listen in a fire-and-forget goroutine that discarded the
//      terminal error; here Listen RETURNS its error to the caller.
//   3. megabridge mapped its own "use of closed network connection" to
//      SIDExpiring and never re-registered on a real SID invalidation; here
//      the read-error ladder maps io.ErrUnexpectedEOF (truncated body) to
//      ErrSIDExpiring and HTTP 400 "Unknown SID" to ErrSIDInvalid.
//   4. megabridge commented out the 1.5h recycle; here Listen enforces it and
//      returns ErrChannelLifetimeExpired.
//
// OnReceiveArray is invoked SYNCHRONOUSLY and IN ORDER from Listen's own
// goroutine (channel.py:487-495 processes each inner array in a plain loop);
// megabridge's unordered Event fan-out is a defect we avoid.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf16"

	"google.golang.org/protobuf/proto"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/pblite"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

const (
	// channelURLBase mirrors CHANNEL_URL_BASE (channel.py:40).
	channelURLBase = "https://chat.google.com/u/0/webchannel/"

	// pushTimeout mirrors PUSH_TIMEOUT = 60 (channel.py:41-43): long polls
	// heartbeat every 15-30s, so 60s with no bytes at all means the
	// connection is dead. Implemented as a resettable READ-IDLE watchdog
	// wrapping resp.Body reads (channel.py:447-449 wraps each read in
	// async_timeout.timeout(PUSH_TIMEOUT)), NOT a whole-request deadline.
	pushTimeout = 60 * time.Second

	// maxReadBytes mirrors MAX_READ_BYTES = 1 MiB (channel.py:44).
	maxReadBytes = 1024 * 1024

	// channelProtocolVersion is the VER=8 query param (channel.py:371).
	channelProtocolVersion = "8"

	// defaultChannelMaxAge is the 1.5h lifetime recycle. Python keeps this in
	// user.py supervision (max_age=1.5h) and channel.listen(max_age) enforces
	// it (channel.py:218-219); this port folds the timer into Listen itself
	// for cohesion (documented move, task-7-brief.md choreography point 6).
	defaultChannelMaxAge = 90 * time.Minute
)

// Channel is the client side of Google's BrowserChannel protocol
// (channel.py:152-153). One Channel owns one long-poll session.
type Channel struct {
	session *Session

	// baseURL is channelURLBase in production; overridable by tests to point
	// at an httptest server (same pattern as Client.baseURL / Session
	// .moleWorldBaseURL). Always ends with "/".
	baseURL string

	// maxAge is the lifetime recycle threshold (defaultChannelMaxAge);
	// unexported so tests can inject a tiny value (channel.py:218-219).
	maxAge time.Duration

	// readIdleTimeout is the read-idle watchdog interval (pushTimeout);
	// unexported so tests can inject a short value without waiting 60s.
	readIdleTimeout time.Duration

	// parser is only ever touched from Listen's own goroutine (reset per
	// attempt, fed from onPushData) so it needs no lock.
	parser *ChunkParser

	// mu guards the mutable session parameters below plus the connection-state
	// flags: Listen runs on one goroutine while the EXPORTED SendStreamEvent
	// may be called concurrently by Task 8 (forward-channel typing/read
	// state). Python has no lock because its whole client is single-threaded
	// asyncio; Go must serialize the rid/ofs/aid/sid sequence counters or the
	// server rejects out-of-order ofs. No blocking I/O is ever performed while
	// holding mu.
	mu sync.Mutex

	// Connection state, mirroring channel.py:183-188. onConnectCalled makes
	// the first-ever connect fire OnConnect exactly once (channel.py:474-482).
	isConnected     bool
	onConnectCalled bool

	// Discovered/session parameters (channel.py:192-198), all guarded by mu.
	sid        string // _sid_param
	csessionid string // _csessionid_param (currently unused downstream, but
	// the webchannel COMPASS cookie it comes from must be present on
	// subsequent requests; it lives in the Session jar -- channel.py:288-301).
	// Written only by the Listen goroutine (post-register) but guarded by mu
	// so Task 8 can read it via Csessionid() without a race.
	aid int // _aid: last acknowledged array id
	ofs int // _ofs: sent-map counter, resets on re-register
	rid int // _rid: request identifier

	// OnReceiveArray is called synchronously, in order, once per decoded
	// pblite data_array (the raw JSON bytes; Task 8 decodes). Returning an
	// error is terminal: it propagates out of Listen (channel.py fires
	// on_receive_array uncaught, so an observer error tears down the poll).
	OnReceiveArray func(ctx context.Context, arr []byte) error
	// OnConnect fires once, the first time a chunk is EVER received
	// (channel.py:474-482, on_connect branch).
	OnConnect func(ctx context.Context)
	// OnReconnect fires when the first chunk arrives after a prior disconnect
	// (channel.py:476-478, on_reconnect branch). Task 8 wires this to a
	// gap-sync: a SIDExpiring re-register resets AID to 0, so events dropped
	// during the gap are NOT server-replayed and must be caught up here.
	OnReconnect func(ctx context.Context)
	// OnDisconnect fires when a live poll drops with a NetworkError
	// (channel.py:251-253). Signals the connection is temporarily down.
	OnDisconnect func(ctx context.Context)
}

// NewChannel creates a channel bound to session. max_retries and
// retry_backoff_base are passed to Listen (unlike Python/megabridge, which
// take them in the constructor), matching task-7-brief.md's interface.
func NewChannel(session *Session) *Channel {
	return &Channel{
		session:         session,
		baseURL:         channelURLBase,
		maxAge:          defaultChannelMaxAge,
		readIdleTimeout: pushTimeout,
		// _rid = random.randint(10000, 99999) (channel.py:198).
		rid: 10000 + rand.Intn(90000),
	}
}

// IsConnected reports whether the channel currently has a live poll
// (channel.py:200-203).
func (ch *Channel) IsConnected() bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.isConnected
}

// Csessionid returns the current webchannel csessionid (mu-guarded, safe to
// call concurrently with Listen).
func (ch *Channel) Csessionid() string {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.csessionid
}

// setCsessionid stores the csessionid under mu (channel.py:214, 236).
func (ch *Channel) setCsessionid(v string) {
	ch.mu.Lock()
	ch.csessionid = v
	ch.mu.Unlock()
}

// The Set* methods below let Task 8's Client wire its callbacks through the
// unexported channelListener interface (client.go) rather than by touching the
// exported fields directly. They exist ONLY so a test fake can satisfy the
// same interface; each is a plain field assignment, always called before
// Listen (single-goroutine setup), so no locking is needed.

// SetOnReceiveArray sets the per-data-array callback (see OnReceiveArray).
func (ch *Channel) SetOnReceiveArray(f func(ctx context.Context, arr []byte) error) {
	ch.OnReceiveArray = f
}

// SetOnConnect sets the first-connect callback (see OnConnect).
func (ch *Channel) SetOnConnect(f func(ctx context.Context)) { ch.OnConnect = f }

// SetOnReconnect sets the reconnect callback (see OnReconnect).
func (ch *Channel) SetOnReconnect(f func(ctx context.Context)) { ch.OnReconnect = f }

// SetOnDisconnect sets the disconnect callback (see OnDisconnect).
func (ch *Channel) SetOnDisconnect(f func(ctx context.Context)) { ch.OnDisconnect = f }

// Listen registers and runs the long-poll loop until ctx is cancelled or a
// terminal error occurs, returning that error to the caller (client.go
// supervision). Ports channel.py:205-258 (listen).
func (ch *Channel) Listen(ctx context.Context, maxRetries int, retryBackoffBase time.Duration) error {
	retries := 0
	skipBackoff := false
	var lastNetErr error // last NetworkError, returned wrapped on exhaustion

	// channel.py:214 -- register once before the loop.
	csid, err := ch.register(ctx)
	if err != nil {
		return fmt.Errorf("register: %w", err)
	}
	ch.setCsessionid(csid)
	start := time.Now() // channel.py:215

	for retries <= maxRetries { // channel.py:217
		if err := ctx.Err(); err != nil {
			return err
		}
		// Lifetime recycle (channel.py:218-219).
		if time.Since(start) > ch.maxAge {
			return ErrChannelLifetimeExpired
		}
		// Exponential backoff after the first failed retry (channel.py:222-226).
		// Python computes retry_backoff_base ** retries with an int base; here
		// retryBackoffBase is a Duration, so the brief mandates
		// retryBackoffBase * 2^(retries-1) (base, 2*base, 4*base, ...), the
		// same doubling schedule as session.go's backoffDelay.
		if retries > 0 && !skipBackoff {
			backoff := retryBackoffBase * time.Duration(1<<uint(retries-1))
			if err := sleepOrDone(ctx, backoff); err != nil {
				return err
			}
		}
		skipBackoff = false

		// Fresh parser per attempt: stale error data must not leak across
		// polls (channel.py:228-230).
		ch.parser = &ChunkParser{}

		err := ch.longpollRequest(ctx)
		switch {
		case err == nil:
			// Clean exit (server closed after ~1h): reset retries and re-poll
			// with the same SID (channel.py:243-247).
			retries = 0
			continue
		case errors.Is(err, ErrSIDExpiring):
			// Truncated body ~ aiohttp ClientPayloadError: re-register
			// immediately, retries incremented but backoff skipped
			// (channel.py:233-240).
			csid, rerr := ch.register(ctx)
			if rerr != nil {
				return fmt.Errorf("re-register: %w", rerr)
			}
			ch.setCsessionid(csid)
			retries++
			skipBackoff = true
			continue
		case isNetworkError(err):
			// aiohttp TimeoutError / ServerDisconnectedError / ClientError ->
			// NetworkError: count a retry and back off (channel.py:241-256).
			lastNetErr = err
			retries++
			ch.mu.Lock()
			wasConnected := ch.isConnected
			ch.isConnected = false
			ch.mu.Unlock()
			if wasConnected && ch.OnDisconnect != nil {
				ch.OnDisconnect(ctx) // channel.py:251-253
			}
			continue
		default:
			// SIDInvalidError and UnexpectedStatusError (incl. 401) are NOT
			// caught by listen's except blocks in Python, so they propagate
			// out (channel.py:231-247; research 01 §2.6). A ctx error or an
			// OnReceiveArray error propagates here too.
			return err
		}
	}

	// channel.py:258 -- ran out of retries. Python's listen returns None here
	// (silent); the brief mandates returning a wrapped NetworkError so the
	// caller (Task 8 supervision) treats exhaustion as transient/reconnectable
	// rather than terminal.
	if lastNetErr != nil {
		return &NetworkError{Err: fmt.Errorf("ran out of retries for long-polling request: %w", lastNetErr)}
	}
	return &NetworkError{Err: errors.New("ran out of retries for long-polling request")}
}

// register performs the pre-poll GET /register that seeds the webchannel
// COMPASS cookie, resets SID/AID/OFS, and returns the csessionid suffix.
// Ports channel.py:260-301 (_register).
func (ch *Channel) register(ctx context.Context) (string, error) {
	// channel.py:263-265.
	ch.mu.Lock()
	ch.sid = ""
	ch.aid = 0
	ch.ofs = 0
	ch.mu.Unlock()

	headers := http.Header{"Content-Type": {"application/x-protobuf"}} // channel.py:270
	resp, err := ch.session.FetchRaw(ctx, http.MethodGet, ch.baseURL+"register?ignore_compass_cookie=1", headers, nil)
	if err != nil {
		return "", fmt.Errorf("register request: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxReadBytes)) // channel.py:275

	if resp.StatusCode != http.StatusOK { // channel.py:277-286
		return "", &UnexpectedStatusError{
			URL:    ch.baseURL + "register",
			Status: resp.StatusCode,
			Body:   truncateBody(body),
		}
	}

	// Extract csessionid from the webchannel COMPASS cookie
	// (channel.py:288-301). resp.Cookies() parses this response's Set-Cookie
	// headers directly; the value itself is also absorbed into the shared jar
	// by FetchRaw, so subsequent /events requests carry it.
	for _, c := range resp.Cookies() {
		if c.Name == "COMPASS" {
			if strings.HasPrefix(c.Value, "dynamite-ui=") {
				return strings.TrimPrefix(c.Value, "dynamite-ui="), nil
			}
			// COMPASS present but unexpected prefix (channel.py:298-300):
			// fall through and return "" like Python.
		}
	}
	return "", nil
}

// longpollRequest opens one long-poll GET and reads arrays until the response
// ends or an error occurs. Ports channel.py:362-467 (_longpoll_request).
//
// Return contract (mapped to Listen's error ladder):
//   - nil                 => clean EOF (server closed) => reset retries, re-poll
//   - ErrSIDExpiring       => truncated body => re-register, no backoff
//   - ErrSIDInvalid        => HTTP 400 "Unknown SID" => propagate
//   - *NetworkError        => connect error / read-idle 60s => backoff
//   - *UnexpectedStatusError => any other non-200 (incl. 401) => propagate
//   - ctx.Err()            => caller cancelled => propagate
func (ch *Channel) longpollRequest(ctx context.Context) error {
	// Common params (channel.py:370-376). Built under mu so the rid/aid/sid
	// reads and rid++ don't race a concurrent SendStreamEvent.
	ch.mu.Lock()
	params := url.Values{
		"VER": {channelProtocolVersion},
		"RID": {strconv.Itoa(ch.rid)},
		"t":   {"1"},
		"zx":  {uniqueID()},
	}
	if ch.sid == "" {
		// First request, no SID yet (channel.py:378-387).
		params.Set("CVER", "22")
		params.Set("$req", "count=1&ofs=0&req0_data=%5B%5D")
		params.Set("SID", "null")
		ch.rid++
	} else {
		// Subsequent requests, SID acquired (channel.py:389).
		params.Set("CI", "0")
		params.Set("TYPE", "xmlhttp")
		params.Set("RID", "rpc")
		params.Set("AID", strconv.Itoa(ch.aid))
		params.Set("SID", ch.sid)
	}
	ch.mu.Unlock()

	headers := http.Header{"referer": {"https://chat.google.com/"}} // channel.py:391-393
	resp, err := ch.session.FetchRaw(ctx, http.MethodGet, ch.eventsURL(params), headers, nil)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		// aiohttp.ClientError connecting -> NetworkError (channel.py:464-466).
		return &NetworkError{Err: err}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK { // channel.py:402-417
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxReadBytes))
		if resp.StatusCode == http.StatusBadRequest {
			// HTTP 400 "Unknown SID" in reason OR body -> SIDInvalid, matching
			// channel.py:408-411's check-both semantics (res.reason == "Unknown
			// SID" or "Unknown SID" in text). Go's resp.Status is the FULL
			// status line ("400 Unknown SID"), not the bare reason phrase, so
			// an equality check against "Unknown SID" would be dead code and
			// collapse onto the body substring alone -- a 400 that carries
			// "Unknown SID" only in the status line (empty/different body) would
			// then misclassify as a terminal *UnexpectedStatusError and force a
			// full channel restart instead of the targeted SID-invalid resync.
			if strings.Contains(resp.Status, "Unknown SID") || strings.Contains(string(body), "Unknown SID") {
				return ErrSIDInvalid
			}
		}
		return &UnexpectedStatusError{
			URL:    ch.baseURL + "events",
			Status: resp.StatusCode,
			Body:   truncateBody(body),
		}
	}

	// SID acquisition from the first response (channel.py:419-445).
	if initial := resp.Header.Get("X-HTTP-Initial-Response"); initial != "" {
		sid, err := parseSIDResponse(initial)
		if err != nil {
			return fmt.Errorf("parse SID response: %w", err)
		}
		ch.mu.Lock()
		changed := ch.sid != sid // channel.py:422
		if changed {
			ch.sid = sid
			ch.aid = 0
			ch.ofs = 0
		}
		curSID, curAID := ch.sid, ch.aid
		ch.mu.Unlock()

		if changed {
			// Ack GET: "required, unclear why" (channel.py:427-442).
			ackParams := url.Values{
				"VER":  {channelProtocolVersion},
				"RID":  {"rpc"},
				"SID":  {curSID},
				"AID":  {strconv.Itoa(curAID)},
				"CI":   {"0"},
				"TYPE": {"xmlhttp"},
				"zx":   {uniqueID()},
				"t":    {"1"},
			}
			ackResp, err := ch.session.FetchRaw(ctx, http.MethodGet, ch.eventsURL(ackParams), nil, nil)
			if err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return ctxErr
				}
				return &NetworkError{Err: err}
			}
			_, _ = io.Copy(io.Discard, ackResp.Body)
			ackResp.Body.Close()

			// Initial ping: without it the server never streams events
			// (channel.py:444-445, _send_initial_ping channel.py:347-360).
			if err := ch.sendInitialPing(ctx); err != nil {
				return err
			}
		}
	}

	// Stream the body with a 60s read-idle watchdog (channel.py:447-453).
	return ch.readBody(ctx, resp)
}

// readBody streams resp.Body, framing bytes through the ChunkParser and firing
// OnReceiveArray. A dedicated reader goroutine performs the blocking reads so
// the select loop can enforce the read-idle timeout by closing the body;
// OnReceiveArray is only ever called from THIS goroutine, preserving the
// synchronous, in-order delivery contract. Ports the read loop and error
// mapping of channel.py:447-467.
func (ch *Channel) readBody(ctx context.Context, resp *http.Response) error {
	type readResult struct {
		data []byte
		err  error
	}
	results := make(chan readResult, 1)
	readCtx, cancelRead := context.WithCancel(ctx)
	defer cancelRead()

	go func() {
		buf := make([]byte, maxReadBytes)
		for {
			n, err := resp.Body.Read(buf)
			if n > 0 {
				data := make([]byte, n)
				copy(data, buf[:n])
				select {
				case results <- readResult{data: data}:
				case <-readCtx.Done():
					return
				}
			}
			if err != nil {
				select {
				case results <- readResult{err: err}:
				case <-readCtx.Done():
				}
				return
			}
		}
	}()

	// The idle watchdog is armed ONLY while waiting for a read to complete,
	// mirroring channel.py:448-449 which wraps async_timeout.timeout(PUSH_TIMEOUT)
	// around res.content.read() alone -- NOT around _on_push_data processing.
	// We therefore stop it before running onPushData and re-arm it before the
	// next wait, so slow OnReceiveArray work can never trip a spurious timeout.
	idle := time.NewTimer(ch.readIdleTimeout)
	defer idle.Stop()

	for {
		select {
		case <-ctx.Done():
			// Caller cancelled: unblock the reader and propagate.
			resp.Body.Close()
			return ctx.Err()

		case <-idle.C:
			// No bytes for readIdleTimeout: aiohttp asyncio.TimeoutError ->
			// NetworkError (channel.py:455-457). Close the body to unblock the
			// reader.
			resp.Body.Close()
			return &NetworkError{Err: errReadIdleTimeout}

		case res := <-results:
			// A read completed: disarm the watchdog for the duration of
			// processing (drain if it fired between select and Stop).
			if !idle.Stop() {
				select {
				case <-idle.C:
				default:
				}
			}

			if res.err != nil {
				return ch.mapReadError(ctx, res.err)
			}
			if err := ch.onPushData(ctx, res.data); err != nil {
				return err
			}

			// Re-arm for the next read wait (fresh interval per read).
			idle.Reset(ch.readIdleTimeout)
		}
	}
}

// errReadIdleTimeout is the underlying error wrapped by NetworkError when the
// read-idle watchdog fires (channel.py:455-457).
var errReadIdleTimeout = errors.New("long poll read idle timeout")

// mapReadError translates a body-read error into Listen's error ladder,
// mirroring channel.py:455-467's except clauses:
//   - io.EOF               => clean exit (nil): server closed after ~1h
//   - io.ErrUnexpectedEOF  => ErrSIDExpiring: truncated body ~ ClientPayloadError
//   - ctx cancelled        => ctx.Err()
//   - anything else        => *NetworkError: ServerDisconnected / ClientError
//
// Note: we deliberately do NOT map our own body-close ("use of closed network
// connection") here -- that path is handled by the idle/ctx select branches
// before this is reached. Megabridge mapped that string to SIDExpiring, a
// defect (docs/research/08c) that never re-registered on a real invalidation.
func (ch *Channel) mapReadError(ctx context.Context, err error) error {
	if errors.Is(err, io.EOF) {
		return nil
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return ErrSIDExpiring
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return &NetworkError{Err: err}
}

// onPushData frames data into chunks and fires OnReceiveArray per inner array.
// Ports channel.py:469-495 (_on_push_data).
func (ch *Channel) onPushData(ctx context.Context, data []byte) error {
	for _, chunk := range ch.parser.Feed(data) {
		// Connected once the first chunk arrives; OnConnect fires exactly once,
		// OnReconnect on every subsequent reconnection (channel.py:474-482).
		// Decide the transition under mu, then fire the callback WITHOUT the
		// lock held (callbacks may be slow / call back in).
		ch.mu.Lock()
		fireConnect, fireReconnect := false, false
		if !ch.isConnected {
			ch.isConnected = true
			if !ch.onConnectCalled {
				ch.onConnectCalled = true
				fireConnect = true
			} else {
				fireReconnect = true
			}
		}
		ch.mu.Unlock()
		if fireConnect && ch.OnConnect != nil {
			ch.OnConnect(ctx) // channel.py:479-482
		}
		if fireReconnect && ch.OnReconnect != nil {
			ch.OnReconnect(ctx) // channel.py:476-478
		}

		// Dump the reconstructed WIRE frame (with its "<utf16len>\n" length
		// prefix, byte-exact to what Google sent), not just the payload -- the
		// capture-fixtures skill expects each .raw to be a self-contained,
		// replayable frame whose length prefix can be recomputed after
		// sanitization (SKILL.md step 5).
		if os.Getenv("GCHAT_DEBUG_DUMP") != "" {
			frame := strconv.Itoa(len(utf16.Encode([]rune(chunk)))) + "\n" + chunk
			ch.debugDump("chunk", ".raw", []byte(frame))
		}

		// chunk is a JSON container array of inner [array_id, data_array]
		// pairs (channel.py:485-495). Parse positionally with RawMessage to
		// hand OnReceiveArray the data_array bytes verbatim (Task 8 decodes).
		var container []json.RawMessage
		if err := json.Unmarshal([]byte(chunk), &container); err != nil {
			return fmt.Errorf("unmarshal container array: %w", err)
		}
		for _, innerRaw := range container {
			var inner []json.RawMessage
			if err := json.Unmarshal(innerRaw, &inner); err != nil {
				return fmt.Errorf("unmarshal inner array: %w", err)
			}
			if len(inner) != 2 {
				return fmt.Errorf("inner array length = %d, want 2", len(inner))
			}
			var arrayID int
			if err := json.Unmarshal(inner[0], &arrayID); err != nil {
				return fmt.Errorf("unmarshal array_id: %w", err)
			}
			dataArray := inner[1]

			ch.debugDump("array", ".json", dataArray)

			if ch.OnReceiveArray != nil {
				if err := ch.OnReceiveArray(ctx, dataArray); err != nil {
					return err
				}
			}
			// Update last acknowledged id AFTER processing (channel.py:495).
			ch.mu.Lock()
			ch.aid = arrayID
			ch.mu.Unlock()
		}
	}
	return nil
}

// sendInitialPing sends the PingEvent that kicks off the event stream
// (channel.py:347-360, _send_initial_ping).
func (ch *Channel) sendInitialPing(ctx context.Context) error {
	ping := &pb.PingEvent{
		State:                      pb.PingEvent_ACTIVE.Enum(),
		ApplicationFocusState:      pb.PingEvent_FOCUS_STATE_FOREGROUND.Enum(),
		ClientInteractiveState:     pb.PingEvent_INTERACTIVE.Enum(),
		ClientNotificationsEnabled: proto.Bool(true),
	}
	return ch.SendStreamEvent(ctx, &pb.StreamEventsRequest{PingEvent: ping})
}

// SendStreamEvent POSTs a StreamEventsRequest on the forward channel.
// Ports channel.py:303-341 (send_stream_event).
func (ch *Channel) SendStreamEvent(ctx context.Context, ev *pb.StreamEventsRequest) error {
	// pblite.Marshal already returns the JSON-encoded array bytes, i.e. it
	// does BOTH of Python's steps (pblite.encode -> a list, then json.dumps
	// -> a string; channel.py:324-325). Do NOT json.Marshal it again --
	// megabridge double-encoded here. Done before taking mu (no shared state).
	body, err := pblite.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal stream event: %w", err)
	}

	// Snapshot + advance the sequence counters under mu so a concurrent
	// SendStreamEvent / longpollRequest can't interleave rid/ofs (channel.py
	// is single-threaded asyncio and needs no lock).
	ch.mu.Lock()
	// Query params (channel.py:304-312).
	params := url.Values{
		"VER": {channelProtocolVersion},
		"RID": {strconv.Itoa(ch.rid)},
		"t":   {"1"},
		"SID": {ch.sid},
		"AID": {strconv.Itoa(ch.aid)},
	}
	ch.rid++ // channel.py:314
	// Form body (channel.py:326-330).
	form := url.Values{
		"count":     {"1"},
		"ofs":       {strconv.Itoa(ch.ofs)},
		"req0_data": {string(body)},
	}
	ch.ofs++ // channel.py:331
	ch.mu.Unlock()

	headers := http.Header{"Content-Type": {"application/x-www-form-urlencoded"}} // channel.py:320-322
	resp, err := ch.session.FetchRaw(ctx, http.MethodPost, ch.eventsURL(params), headers, form)
	if err != nil {
		return fmt.Errorf("send stream event: %w", err)
	}
	// Python returns the response without reading it; we must drain+close to
	// release the connection back to the pool.
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return nil
}

// eventsURL builds {baseURL}events?<params>. FetchRaw takes query params baked
// into the URL (params in url.Values.Encode form).
func (ch *Channel) eventsURL(params url.Values) string {
	return ch.baseURL + "events?" + params.Encode()
}

// debugDump writes raw chunks and decoded arrays under $GCHAT_DEBUG_DUMP for
// /capture-fixtures (task-7-brief.md choreography point 7). No-op when unset.
func (ch *Channel) debugDump(prefix, ext string, data []byte) {
	dir := os.Getenv("GCHAT_DEBUG_DUMP")
	if dir == "" {
		return
	}
	seq := atomic.AddInt64(&debugDumpSeq, 1)
	name := fmt.Sprintf("%s-%d-%d%s", prefix, time.Now().UnixNano(), seq, ext)
	_ = os.WriteFile(filepath.Join(dir, name), data, 0o644)
}

// debugDumpSeq disambiguates dumps written within the same nanosecond.
var debugDumpSeq int64

// uniqueID mirrors _unique_id (channel.py:137-149): base36 of 64 random bits.
// Used only as the zx cache-buster query param, so the exact encoding is not
// wire-critical.
func uniqueID() string {
	const keyspace = "abcdefghijklmnopqrstuvwxyz0123456789"
	x := rand.Uint64()
	if x == 0 {
		return "a"
	}
	var b []byte
	for x != 0 {
		b = append([]byte{keyspace[x%uint64(len(keyspace))]}, b...)
		x /= uint64(len(keyspace))
	}
	return string(b)
}

// parseSIDResponse extracts the SID from an X-HTTP-Initial-Response header.
// Ports _parse_sid_response (channel.py:124-134): parse JSON, return
// res[0][1][1] from a shape like [[0,["c","<sid>","",8,12]]].
func parseSIDResponse(res string) (string, error) {
	var outer []json.RawMessage
	if err := json.Unmarshal([]byte(res), &outer); err != nil {
		return "", fmt.Errorf("invalid SID response JSON: %w", err)
	}
	if len(outer) < 1 {
		return "", errors.New("SID response: empty outer array")
	}
	var mid []json.RawMessage // res[0] == [0, ["c","<sid>","",8,12]]
	if err := json.Unmarshal(outer[0], &mid); err != nil {
		return "", fmt.Errorf("invalid SID response element: %w", err)
	}
	if len(mid) < 2 {
		return "", errors.New("SID response: element too short")
	}
	var inner []json.RawMessage // res[0][1] == ["c","<sid>","",8,12]
	if err := json.Unmarshal(mid[1], &inner); err != nil {
		return "", fmt.Errorf("invalid SID response inner: %w", err)
	}
	if len(inner) < 2 {
		return "", errors.New("SID response: inner too short")
	}
	var sid string // res[0][1][1]
	if err := json.Unmarshal(inner[1], &sid); err != nil {
		return "", fmt.Errorf("invalid SID value: %w", err)
	}
	return sid, nil
}

// isNetworkError reports whether err is (or wraps) a *NetworkError, so Listen
// can back off rather than propagate.
func isNetworkError(err error) bool {
	var ne *NetworkError
	return errors.As(err, &ne)
}
