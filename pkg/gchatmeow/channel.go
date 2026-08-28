package gchatmeow

// BrowserChannel long-poll client implementing Google's webchannel protocol.
// This is the single most fidelity-critical module: subtle bugs cause silent
// event loss.
//
// Deliberate divergences from the second reference
// (_reference/googlechat-megabridge/pkg/gchatmeow/channel.go), whose four
// documented defects this port must NOT reproduce:
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
// goroutine (each inner array is processed in a plain loop); megabridge's
// unordered Event fan-out is a defect we avoid.

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

	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/pblite"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

const (
	// channelURLBase is the webchannel endpoint base URL.
	channelURLBase = "https://chat.google.com/u/0/webchannel/"

	// pushTimeout is 60s: long polls heartbeat every 15-30s, so 60s with no
	// bytes at all means the connection is dead. Implemented as a resettable
	// READ-IDLE watchdog wrapping each resp.Body read individually, NOT a
	// whole-request deadline.
	pushTimeout = 60 * time.Second

	// maxReadBytes is the 1 MiB read cap.
	maxReadBytes = 1024 * 1024

	// channelProtocolVersion is the VER=8 query param.
	channelProtocolVersion = "8"

	// defaultChannelMaxAge is the 1.5h lifetime recycle. Listen folds this
	// timer into itself for cohesion.
	defaultChannelMaxAge = 90 * time.Minute
)

// Channel is the client side of Google's BrowserChannel protocol. One
// Channel owns one long-poll session.
type Channel struct {
	session *Session

	// baseURL is channelURLBase in production; overridable by tests to point
	// at an httptest server (same pattern as Client.baseURL / Session
	// .moleWorldBaseURL). Always ends with "/".
	baseURL string

	// maxAge is the lifetime recycle threshold (defaultChannelMaxAge);
	// unexported so tests can inject a tiny value.
	maxAge time.Duration

	// readIdleTimeout is the read-idle watchdog interval (pushTimeout);
	// unexported so tests can inject a short value without waiting 60s.
	readIdleTimeout time.Duration

	// parser is only ever touched from Listen's own goroutine (reset per
	// attempt, fed from onPushData) so it needs no lock.
	parser *ChunkParser

	// mu guards the mutable session parameters below plus the connection-state
	// flags: Listen runs on one goroutine while the EXPORTED SendStreamEvent
	// may be called concurrently (forward-channel typing/read
	// state). Go must serialize the rid/ofs/aid/sid sequence counters or the
	// server rejects out-of-order ofs. No blocking I/O is ever performed while
	// holding mu.
	mu sync.Mutex

	// sendMu serializes the forward channel. ofs is a strict sequence the
	// server rejects out of order, so assigning the numbers under mu is not
	// enough: the requests must also REACH THE WIRE in that order, which
	// means the POST itself has to be inside the critical section. It is
	// therefore deliberately held across network I/O -- forward-channel
	// sends are rare and short, and the alternative is a protocol violation.
	//
	// It also spans the whole SID-adoption sequence (adoptSID): the counter
	// reset, the ack round trip and the initial ping are one indivisible
	// unit, or a concurrent send could consume ofs=0 and push the ping to
	// ofs=1. Lock ordering is sendMu -> mu; never the reverse.
	sendMu sync.Mutex

	// Connection state. onConnectCalled makes the first-ever connect fire
	// OnConnect exactly once.
	isConnected     bool
	onConnectCalled bool

	// Discovered/session parameters, all guarded by mu.
	sid        string
	csessionid string // currently unused downstream, but the webchannel
	// COMPASS cookie it comes from must be present on subsequent requests; it
	// lives in the Session jar. Written only by the Listen goroutine
	// (post-register) but guarded by mu so callers can read it via
	// Csessionid() without a race.
	aid int // last acknowledged array id
	ofs int // sent-map counter, resets on re-register
	rid int // request identifier

	// OnReceiveArray is called synchronously, in order, once per decoded
	// pblite data_array (the raw JSON bytes; the caller decodes). Returning an
	// error is terminal: it propagates out of Listen, so an observer error
	// tears down the poll.
	OnReceiveArray func(ctx context.Context, arr []byte) error
	// OnConnect fires once, the first time a chunk is EVER received.
	OnConnect func(ctx context.Context)
	// OnReconnect fires when the first chunk arrives after a prior disconnect.
	// The caller wires this to a gap-sync: a SIDExpiring re-register resets AID
	// to 0, so events dropped during the gap are NOT server-replayed and must
	// be caught up here.
	OnReconnect func(ctx context.Context)
	// OnDisconnect fires when a live poll drops with a NetworkError. Signals
	// the connection is temporarily down.
	OnDisconnect func(ctx context.Context)
}

// NewChannel creates a channel bound to session. max_retries and
// retry_backoff_base are passed to Listen (unlike megabridge, which takes
// them in the constructor).
func NewChannel(session *Session) *Channel {
	return &Channel{
		session:         session,
		baseURL:         channelURLBase,
		maxAge:          defaultChannelMaxAge,
		readIdleTimeout: pushTimeout,
		// rid starts as a random int in [10000, 99999].
		rid: 10000 + rand.Intn(90000),
	}
}

// IsConnected reports whether the channel currently has a live poll.
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

// setCsessionid stores the csessionid under mu.
func (ch *Channel) setCsessionid(v string) {
	ch.mu.Lock()
	ch.csessionid = v
	ch.mu.Unlock()
}

// The Set* methods below let the Client wire its callbacks through the
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
// supervision).
func (ch *Channel) Listen(ctx context.Context, maxRetries int, retryBackoffBase time.Duration) error {
	retries := 0
	skipBackoff := false
	var lastNetErr error // last NetworkError, returned wrapped on exhaustion

	// Register once before the loop.
	csid, err := ch.register(ctx)
	if err != nil {
		return fmt.Errorf("register: %w", err)
	}
	ch.setCsessionid(csid)
	start := time.Now()

	for retries <= maxRetries {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Lifetime recycle.
		if time.Since(start) > ch.maxAge {
			return ErrChannelLifetimeExpired
		}
		// Exponential backoff after the first failed retry. retryBackoffBase
		// is a Duration, so the backoff is retryBackoffBase * 2^(retries-1)
		// (base, 2*base, 4*base, ...), the same doubling schedule as
		// session.go's backoffDelay.
		if retries > 0 && !skipBackoff {
			backoff := retryBackoffBase * time.Duration(1<<uint(retries-1))
			if err := sleepOrDone(ctx, backoff); err != nil {
				return err
			}
		}
		skipBackoff = false

		// Fresh parser per attempt: stale error data must not leak across
		// polls.
		ch.parser = &ChunkParser{}

		err := ch.longpollRequest(ctx)
		switch {
		case err == nil:
			// Clean exit (server closed after ~1h): reset retries and re-poll
			// with the same SID.
			retries = 0
			continue
		case errors.Is(err, ErrSIDExpiring):
			// Truncated body: re-register immediately, retries incremented but
			// backoff skipped.
			csid, rerr := ch.register(ctx)
			if rerr != nil {
				return fmt.Errorf("re-register: %w", rerr)
			}
			ch.setCsessionid(csid)
			retries++
			skipBackoff = true
			continue
		case isNetworkError(err):
			// Timeout / server-disconnected / connection error -> NetworkError:
			// count a retry and back off.
			lastNetErr = err
			retries++
			ch.mu.Lock()
			wasConnected := ch.isConnected
			ch.isConnected = false
			ch.mu.Unlock()
			if wasConnected && ch.OnDisconnect != nil {
				ch.OnDisconnect(ctx)
			}
			continue
		default:
			// SIDInvalidError and UnexpectedStatusError (incl. 401) are
			// terminal and propagate out rather than being retried. A ctx
			// error or an OnReceiveArray error propagates here too.
			return err
		}
	}

	// Ran out of retries. Return a wrapped NetworkError so the caller (the
	// supervision loop) treats exhaustion as transient/reconnectable rather
	// than terminal.
	if lastNetErr != nil {
		return &NetworkError{Err: fmt.Errorf("ran out of retries for long-polling request: %w", lastNetErr)}
	}
	return &NetworkError{Err: errors.New("ran out of retries for long-polling request")}
}

// register performs the pre-poll GET /register that seeds the webchannel
// COMPASS cookie, resets SID/AID/OFS, and returns the csessionid suffix.
func (ch *Channel) register(ctx context.Context) (string, error) {
	// Reset the discovered session parameters under mu.
	ch.mu.Lock()
	ch.sid = ""
	ch.aid = 0
	ch.ofs = 0
	ch.mu.Unlock()

	headers := http.Header{"Content-Type": {"application/x-protobuf"}}
	resp, err := ch.session.FetchRaw(ctx, http.MethodGet, ch.baseURL+"register?ignore_compass_cookie=1", headers, nil)
	if err != nil {
		return "", fmt.Errorf("register request: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxReadBytes))

	if resp.StatusCode != http.StatusOK {
		return "", &UnexpectedStatusError{
			URL:    ch.baseURL + "register",
			Status: resp.StatusCode,
			Body:   truncateBody(body),
		}
	}

	// Extract csessionid from the webchannel COMPASS cookie. resp.Cookies()
	// parses this response's Set-Cookie headers directly; the value itself is
	// also absorbed into the shared jar by FetchRaw, so subsequent /events
	// requests carry it.
	for _, c := range resp.Cookies() {
		if c.Name == "COMPASS" {
			if strings.HasPrefix(c.Value, "dynamite-ui=") {
				return strings.TrimPrefix(c.Value, "dynamite-ui="), nil
			}
			// COMPASS present but unexpected prefix: fall through and
			// return "".
		}
	}
	return "", nil
}

// longpollRequest opens one long-poll GET and reads arrays until the response
// ends or an error occurs.
//
// Return contract (mapped to Listen's error ladder):
//   - nil                 => clean EOF (server closed) => reset retries, re-poll
//   - ErrSIDExpiring       => truncated body => re-register, no backoff
//   - ErrSIDInvalid        => HTTP 400 "Unknown SID" => propagate
//   - *NetworkError        => connect error / read-idle 60s => backoff
//   - *UnexpectedStatusError => any other non-200 (incl. 401) => propagate
//   - ctx.Err()            => caller cancelled => propagate
func (ch *Channel) longpollRequest(ctx context.Context) error {
	// Common params. Built under mu so the rid/aid/sid reads and rid++ don't
	// race a concurrent SendStreamEvent.
	ch.mu.Lock()
	params := url.Values{
		"VER": {channelProtocolVersion},
		"RID": {strconv.Itoa(ch.rid)},
		"t":   {"1"},
		"zx":  {uniqueID()},
	}
	if ch.sid == "" {
		// First request, no SID yet.
		params.Set("CVER", "22")
		params.Set("$req", "count=1&ofs=0&req0_data=%5B%5D")
		params.Set("SID", "null")
		ch.rid++
	} else {
		// Subsequent requests, SID acquired.
		params.Set("CI", "0")
		params.Set("TYPE", "xmlhttp")
		params.Set("RID", "rpc")
		params.Set("AID", strconv.Itoa(ch.aid))
		params.Set("SID", ch.sid)
	}
	ch.mu.Unlock()

	headers := http.Header{"referer": {"https://chat.google.com/"}}
	resp, err := ch.session.FetchRaw(ctx, http.MethodGet, ch.eventsURL(params), headers, nil)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		// Connection error -> NetworkError.
		return &NetworkError{Err: err}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxReadBytes))
		if resp.StatusCode == http.StatusBadRequest {
			// HTTP 400 "Unknown SID" in reason OR body -> SIDInvalid; the
			// check must cover both. Go's resp.Status is the FULL status line
			// ("400 Unknown SID"), not the bare reason phrase, so
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

	// SID acquisition from the first response.
	if initial := resp.Header.Get("X-HTTP-Initial-Response"); initial != "" {
		sid, err := parseSIDResponse(initial)
		if err != nil {
			return fmt.Errorf("parse SID response: %w", err)
		}
		if err := ch.adoptSID(ctx, sid); err != nil {
			return err
		}
	}

	// Stream the body with a 60s read-idle watchdog.
	return ch.readBody(ctx, resp)
}

// readBody streams resp.Body, framing bytes through the ChunkParser and firing
// OnReceiveArray. A dedicated reader goroutine performs the blocking reads so
// the select loop can enforce the read-idle timeout by closing the body;
// OnReceiveArray is only ever called from THIS goroutine, preserving the
// synchronous, in-order delivery contract.
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
	// wrapping the read alone -- NOT the onPushData processing. We therefore
	// stop it before running onPushData and re-arm it before the next wait, so
	// slow OnReceiveArray work can never trip a spurious timeout.
	idle := time.NewTimer(ch.readIdleTimeout)
	defer idle.Stop()

	for {
		select {
		case <-ctx.Done():
			// Caller cancelled: unblock the reader and propagate.
			resp.Body.Close()
			return ctx.Err()

		case <-idle.C:
			// No bytes for readIdleTimeout: timeout -> NetworkError. Close the
			// body to unblock the reader.
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
// read-idle watchdog fires.
var errReadIdleTimeout = errors.New("long poll read idle timeout")

// mapReadError translates a body-read error into Listen's error ladder:
//   - io.EOF               => clean exit (nil): server closed after ~1h
//   - io.ErrUnexpectedEOF  => ErrSIDExpiring: truncated body
//   - ctx cancelled        => ctx.Err()
//   - anything else        => *NetworkError: server-disconnected / transport
//
// Note: we deliberately do NOT map our own body-close ("use of closed network
// connection") here -- that path is handled by the idle/ctx select branches
// before this is reached. Megabridge mapped that string to SIDExpiring, a
// defect that never re-registered on a real invalidation.
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
func (ch *Channel) onPushData(ctx context.Context, data []byte) error {
	for _, chunk := range ch.parser.Feed(data) {
		// Connected once the first chunk arrives; OnConnect fires exactly once,
		// OnReconnect on every subsequent reconnection.
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
			ch.OnConnect(ctx)
		}
		if fireReconnect && ch.OnReconnect != nil {
			ch.OnReconnect(ctx)
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
		// pairs. Parse positionally with RawMessage to hand OnReceiveArray the
		// data_array bytes verbatim (the caller decodes).
		//
		// Every decode failure below is logged and SKIPPED rather than
		// returned. Returning would propagate out through readBody and
		// longpollRequest into Listen's `default:` branch, which is terminal:
		// one malformed element would tear down the whole poll session and
		// discard the well-formed arrays beside it in the same chunk. That is
		// exactly what this project's stream-decode invariant forbids, and it
		// matches how the pblite payload decode one level down
		// (client.go's onReceiveArray) and the frame parser one level up
		// (ChunkParser's poison-byte resync) already behave. An error from
		// OnReceiveArray itself stays terminal -- that is the documented
		// contract of the callback, not a decode failure.
		var container []json.RawMessage
		if err := json.Unmarshal([]byte(chunk), &container); err != nil {
			log.Warn().Err(err).Msg("gchatmeow: skipping undecodable webchannel container frame")
			continue
		}
		for _, innerRaw := range container {
			var inner []json.RawMessage
			if err := json.Unmarshal(innerRaw, &inner); err != nil {
				log.Warn().Err(err).Msg("gchatmeow: skipping undecodable webchannel inner array")
				continue
			}
			if len(inner) != 2 {
				log.Warn().Int("length", len(inner)).Msg("gchatmeow: skipping webchannel inner array of unexpected length, want [array_id, data_array]")
				continue
			}
			var arrayID int
			if err := json.Unmarshal(inner[0], &arrayID); err != nil {
				log.Warn().Err(err).Msg("gchatmeow: skipping webchannel inner array with a non-numeric array_id")
				continue
			}
			dataArray := inner[1]

			ch.debugDump("array", ".json", dataArray)

			if ch.OnReceiveArray != nil {
				if err := ch.OnReceiveArray(ctx, dataArray); err != nil {
					return err
				}
			}
			// Update last acknowledged id AFTER processing.
			ch.mu.Lock()
			ch.aid = arrayID
			ch.mu.Unlock()
		}
	}
	return nil
}

// adoptSID installs a newly issued SID and, when it actually changed, runs
// the ack round trip and initial ping the server requires before it will
// stream anything.
//
// The whole sequence is held under sendMu. The counters were just reset to
// zero for the new SID, and the ack is a network round trip, so without the
// lock a concurrent SendStreamEvent would slot into that window, take ofs=0
// and leave the ping with ofs=1 -- which the server rejects. A rejected ping
// means the server never starts streaming, so the channel would sit
// "connected" and permanently eventless until the read-idle watchdog gave
// up 60s later.
func (ch *Channel) adoptSID(ctx context.Context, sid string) error {
	ch.sendMu.Lock()
	defer ch.sendMu.Unlock()

	ch.mu.Lock()
	changed := ch.sid != sid
	if changed {
		ch.sid = sid
		ch.aid = 0
		ch.ofs = 0
	}
	curSID, curAID := ch.sid, ch.aid
	ch.mu.Unlock()

	if !changed {
		return nil
	}

	// Ack GET: required, unclear why.
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

	// Initial ping: without it the server never streams events.
	return ch.sendInitialPing(ctx)
}

// sendInitialPing sends the PingEvent that kicks off the event stream.
// Callers must already hold sendMu (only adoptSID sends this).
func (ch *Channel) sendInitialPing(ctx context.Context) error {
	ping := &pb.PingEvent{
		State:                      pb.PingEvent_ACTIVE.Enum(),
		ApplicationFocusState:      pb.PingEvent_FOCUS_STATE_FOREGROUND.Enum(),
		ClientInteractiveState:     pb.PingEvent_INTERACTIVE.Enum(),
		ClientNotificationsEnabled: proto.Bool(true),
	}
	return ch.sendStreamEventLocked(ctx, &pb.StreamEventsRequest{PingEvent: ping})
}

// SendStreamEvent POSTs a StreamEventsRequest on the forward channel,
// serialized against every other forward-channel send (see sendMu).
func (ch *Channel) SendStreamEvent(ctx context.Context, ev *pb.StreamEventsRequest) error {
	ch.sendMu.Lock()
	defer ch.sendMu.Unlock()
	return ch.sendStreamEventLocked(ctx, ev)
}

// sendStreamEventLocked is SendStreamEvent's body; callers must hold sendMu.
func (ch *Channel) sendStreamEventLocked(ctx context.Context, ev *pb.StreamEventsRequest) error {
	// pblite.Marshal already returns the JSON-encoded array bytes (it both
	// encodes to a list and serializes it to a string). Do NOT json.Marshal it
	// again -- megabridge double-encoded here. Done before taking mu (no
	// shared state).
	body, err := pblite.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal stream event: %w", err)
	}

	// Snapshot + advance the sequence counters under mu so a concurrent
	// SendStreamEvent / longpollRequest can't interleave rid/ofs.
	ch.mu.Lock()
	// register() clears the SID for the whole re-register round trip, so a
	// send landing in that window would put an empty SID on the wire and be
	// rejected. Tell the caller instead of burning an ofs on it.
	if ch.sid == "" {
		ch.mu.Unlock()
		return ErrChannelNotReady
	}
	// Query params.
	params := url.Values{
		"VER": {channelProtocolVersion},
		"RID": {strconv.Itoa(ch.rid)},
		"t":   {"1"},
		"SID": {ch.sid},
		"AID": {strconv.Itoa(ch.aid)},
	}
	ch.rid++
	// Form body.
	form := url.Values{
		"count":     {"1"},
		"ofs":       {strconv.Itoa(ch.ofs)},
		"req0_data": {string(body)},
	}
	ch.ofs++
	ch.mu.Unlock()

	headers := http.Header{"Content-Type": {"application/x-www-form-urlencoded"}}
	resp, err := ch.session.FetchRaw(ctx, http.MethodPost, ch.eventsURL(params), headers, form)
	if err != nil {
		return fmt.Errorf("send stream event: %w", err)
	}
	defer resp.Body.Close()

	// FetchRaw does no status mapping of its own, so without this check any
	// rejection (400 Unknown SID, 401, 5xx) would be indistinguishable from
	// a delivered send. That matters most for the initial ping: swallowing
	// its rejection leaves the poll looking healthy while the server never
	// streams a single event.
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxReadBytes))
		return &UnexpectedStatusError{
			URL:    ch.baseURL + "events",
			Status: resp.StatusCode,
			Body:   truncateBody(body),
		}
	}
	// Drain the body to release the connection back to the pool.
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// eventsURL builds {baseURL}events?<params>. FetchRaw takes query params baked
// into the URL (params in url.Values.Encode form).
func (ch *Channel) eventsURL(params url.Values) string {
	return ch.baseURL + "events?" + params.Encode()
}

// debugDump writes raw chunks and decoded arrays under $GCHAT_DEBUG_DUMP for
// /capture-fixtures. No-op when unset.
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

// uniqueID returns base36 of 64 random bits. Used only as the zx cache-buster
// query param, so the exact encoding is not wire-critical.
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

// parseSIDResponse extracts the SID from an X-HTTP-Initial-Response header:
// parse JSON, return res[0][1][1] from a shape like [[0,["c","<sid>","",8,12]]].
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
