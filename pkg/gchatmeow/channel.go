package gchatmeow

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

	// pushTimeout is a read-idle watchdog (heartbeats every 15-30s), not a request deadline.
	pushTimeout = 60 * time.Second

	maxReadBytes = 1024 * 1024

	channelProtocolVersion = "8"

	// defaultChannelMaxAge is the 1.5h lifetime recycle.
	defaultChannelMaxAge = 90 * time.Minute
)

// Channel is the client side of Google's BrowserChannel protocol; one long-poll session.
type Channel struct {
	session *Session

	// baseURL always ends with "/".
	baseURL string

	maxAge time.Duration

	readIdleTimeout time.Duration

	// parser is only touched from Listen's goroutine.
	parser *ChunkParser

	// mu guards the session counters and connection state; the server rejects out-of-order ofs. No blocking I/O under mu.
	mu sync.Mutex

	// sendMu keeps forward-channel POSTs on the wire in ofs order, so it is held across I/O and spans adoptSID.
	// Lock ordering is sendMu -> mu; never the reverse.
	sendMu sync.Mutex

	// onConnectCalled makes OnConnect fire exactly once.
	isConnected     bool
	onConnectCalled bool

	// Guarded by mu.
	sid        string
	csessionid string // its COMPASS cookie lives in the Session jar

	aid int // last acknowledged array id
	ofs int // sent-map counter, resets on re-register
	rid int // request identifier

	// OnReceiveArray is called synchronously, in order, per data_array. Returning an error tears down the poll.
	OnReceiveArray func(ctx context.Context, arr []byte) error
	// OnConnect fires once, the first time a chunk is ever received.
	OnConnect func(ctx context.Context)
	// OnReconnect fires on the first chunk after a disconnect; events in the gap are not replayed.
	OnReconnect func(ctx context.Context)
	// OnDisconnect fires when a live poll drops with a NetworkError.
	OnDisconnect func(ctx context.Context)
}

// NewChannel creates a channel bound to session.
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

// Csessionid returns the current webchannel csessionid. Safe for concurrent use.
func (ch *Channel) Csessionid() string {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.csessionid
}

func (ch *Channel) setCsessionid(v string) {
	ch.mu.Lock()
	ch.csessionid = v
	ch.mu.Unlock()
}

// SetOnReceiveArray sets OnReceiveArray.
func (ch *Channel) SetOnReceiveArray(f func(ctx context.Context, arr []byte) error) {
	ch.OnReceiveArray = f
}

// SetOnConnect sets OnConnect.
func (ch *Channel) SetOnConnect(f func(ctx context.Context)) { ch.OnConnect = f }

// SetOnReconnect sets OnReconnect.
func (ch *Channel) SetOnReconnect(f func(ctx context.Context)) { ch.OnReconnect = f }

// SetOnDisconnect sets OnDisconnect.
func (ch *Channel) SetOnDisconnect(f func(ctx context.Context)) { ch.OnDisconnect = f }

// Listen registers and long-polls until ctx is cancelled or a terminal error, which it returns.
func (ch *Channel) Listen(ctx context.Context, maxRetries int, retryBackoffBase time.Duration) error {
	retries := 0
	skipBackoff := false
	var lastNetErr error

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

		if time.Since(start) > ch.maxAge {
			return ErrChannelLifetimeExpired
		}
		// Backoff is retryBackoffBase * 2^(retries-1).
		if retries > 0 && !skipBackoff {
			backoff := retryBackoffBase * time.Duration(1<<uint(retries-1))
			if err := sleepOrDone(ctx, backoff); err != nil {
				return err
			}
		}
		skipBackoff = false

		ch.parser = &ChunkParser{}

		err := ch.longpollRequest(ctx)
		switch {
		case err == nil:
			// Clean exit (server closed after ~1h): re-poll with the same SID.
			retries = 0
			continue
		case errors.Is(err, ErrSIDExpiring):
			// Truncated body: re-register immediately, no backoff.
			csid, rerr := ch.register(ctx)
			if rerr != nil {
				return fmt.Errorf("re-register: %w", rerr)
			}
			ch.setCsessionid(csid)
			retries++
			skipBackoff = true
			continue
		case isNetworkError(err):
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
			// SIDInvalid, UnexpectedStatusError (incl. 401), ctx and OnReceiveArray errors are terminal.
			return err
		}
	}

	// Exhausted retries: return a NetworkError so the supervisor treats it as transient.
	if lastNetErr != nil {
		return &NetworkError{Err: fmt.Errorf("ran out of retries for long-polling request: %w", lastNetErr)}
	}
	return &NetworkError{Err: errors.New("ran out of retries for long-polling request")}
}

// register does GET /register, which seeds the webchannel COMPASS cookie, resets SID/AID/OFS and returns the csessionid.
func (ch *Channel) register(ctx context.Context) (string, error) {
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

	// csessionid comes from the webchannel COMPASS cookie.
	for _, c := range resp.Cookies() {
		if c.Name == "COMPASS" {
			if strings.HasPrefix(c.Value, "dynamite-ui=") {
				return strings.TrimPrefix(c.Value, "dynamite-ui="), nil
			}
		}
	}
	return "", nil
}

// longpollRequest opens one long-poll GET and reads arrays until it ends. Returns:
// nil on clean EOF, ErrSIDExpiring on a truncated body, ErrSIDInvalid on 400 "Unknown SID", *NetworkError
// for connect/idle errors, *UnexpectedStatusError for other non-200s, or ctx.Err().
func (ch *Channel) longpollRequest(ctx context.Context) error {
	// Built under mu so rid++ doesn't race SendStreamEvent.
	ch.mu.Lock()
	params := url.Values{
		"VER": {channelProtocolVersion},
		"RID": {strconv.Itoa(ch.rid)},
		"t":   {"1"},
		"zx":  {uniqueID()},
	}
	if ch.sid == "" {
		params.Set("CVER", "22")
		params.Set("$req", "count=1&ofs=0&req0_data=%5B%5D")
		params.Set("SID", "null")
		ch.rid++
	} else {
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

		return &NetworkError{Err: err}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxReadBytes))
		if resp.StatusCode == http.StatusBadRequest {
			// "Unknown SID" can be in the status line or the body; resp.Status is the full status line.
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

	if initial := resp.Header.Get("X-HTTP-Initial-Response"); initial != "" {
		sid, err := parseSIDResponse(initial)
		if err != nil {
			return fmt.Errorf("parse SID response: %w", err)
		}
		if err := ch.adoptSID(ctx, sid); err != nil {
			return err
		}
	}

	return ch.readBody(ctx, resp)
}

// readBody streams resp.Body through the ChunkParser. A reader goroutine does the blocking reads; OnReceiveArray runs only on this one.
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

	// The idle watchdog covers only the read wait, not onPushData processing.
	idle := time.NewTimer(ch.readIdleTimeout)
	defer idle.Stop()

	for {
		select {
		case <-ctx.Done():
			resp.Body.Close()
			return ctx.Err()

		case <-idle.C:
			// Read-idle timeout: close the body to unblock the reader.
			resp.Body.Close()
			return &NetworkError{Err: errReadIdleTimeout}

		case res := <-results:
			// Disarm the watchdog while processing (drain if it already fired).
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

			idle.Reset(ch.readIdleTimeout)
		}
	}
}

var errReadIdleTimeout = errors.New("long poll read idle timeout")

// mapReadError maps io.EOF to nil (clean exit), io.ErrUnexpectedEOF to ErrSIDExpiring, and anything else to *NetworkError.
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
		// Fire OnConnect/OnReconnect without holding mu.
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

		// Dump the full wire frame with its length prefix, as /capture-fixtures expects.
		if os.Getenv("GCHAT_DEBUG_DUMP") != "" {
			frame := strconv.Itoa(len(utf16.Encode([]rune(chunk)))) + "\n" + chunk
			ch.debugDump("chunk", ".raw", []byte(frame))
		}

		// Decode errors are logged and skipped, never returned: returning would kill the channel. OnReceiveArray errors stay terminal.
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
			ch.mu.Lock()
			ch.aid = arrayID
			ch.mu.Unlock()
		}
	}
	return nil
}

// adoptSID installs a new SID and runs the required ack and initial ping, all under sendMu so the ping gets ofs=0.
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

// sendInitialPing sends the PingEvent that starts the stream. Callers must hold sendMu.
func (ch *Channel) sendInitialPing(ctx context.Context) error {
	ping := &pb.PingEvent{
		State:                      pb.PingEvent_ACTIVE.Enum(),
		ApplicationFocusState:      pb.PingEvent_FOCUS_STATE_FOREGROUND.Enum(),
		ClientInteractiveState:     pb.PingEvent_INTERACTIVE.Enum(),
		ClientNotificationsEnabled: proto.Bool(true),
	}
	return ch.sendStreamEventLocked(ctx, &pb.StreamEventsRequest{PingEvent: ping})
}

// SendStreamEvent POSTs a StreamEventsRequest on the forward channel.
func (ch *Channel) SendStreamEvent(ctx context.Context, ev *pb.StreamEventsRequest) error {
	ch.sendMu.Lock()
	defer ch.sendMu.Unlock()
	return ch.sendStreamEventLocked(ctx, ev)
}

// sendStreamEventLocked is SendStreamEvent's body; callers must hold sendMu.
func (ch *Channel) sendStreamEventLocked(ctx context.Context, ev *pb.StreamEventsRequest) error {
	// pblite.Marshal already returns JSON; do not json.Marshal it again.
	body, err := pblite.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal stream event: %w", err)
	}

	ch.mu.Lock()
	// SID is empty during re-register; a send now would be rejected.
	if ch.sid == "" {
		ch.mu.Unlock()
		return ErrChannelNotReady
	}

	params := url.Values{
		"VER": {channelProtocolVersion},
		"RID": {strconv.Itoa(ch.rid)},
		"t":   {"1"},
		"SID": {ch.sid},
		"AID": {strconv.Itoa(ch.aid)},
	}
	ch.rid++

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

	// FetchRaw doesn't map statuses; a swallowed rejection would hide a dead stream.
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxReadBytes))
		return &UnexpectedStatusError{
			URL:    ch.baseURL + "events",
			Status: resp.StatusCode,
			Body:   truncateBody(body),
		}
	}

	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// eventsURL builds {baseURL}events?<params>.
func (ch *Channel) eventsURL(params url.Values) string {
	return ch.baseURL + "events?" + params.Encode()
}

// debugDump writes raw chunks under $GCHAT_DEBUG_DUMP for /capture-fixtures. No-op when unset.
func (ch *Channel) debugDump(prefix, ext string, data []byte) {
	dir := os.Getenv("GCHAT_DEBUG_DUMP")
	if dir == "" {
		return
	}
	// Dumps hold real message content, so they are owner-only.
	if err := os.MkdirAll(dir, 0o700); err != nil {
		log.Warn().Err(err).Str("dir", dir).Msg("gchatmeow: cannot create GCHAT_DEBUG_DUMP directory, dropping dump")
		return
	}
	seq := atomic.AddInt64(&debugDumpSeq, 1)
	name := fmt.Sprintf("%s-%d-%d%s", prefix, time.Now().UnixNano(), seq, ext)
	path := filepath.Join(dir, name)

	if err := os.WriteFile(path, data, 0o600); err != nil {
		log.Warn().Err(err).Str("path", path).Msg("gchatmeow: failed to write debug dump")
	}
}

var debugDumpSeq int64

// uniqueID returns base36 of 64 random bits, for the zx cache-buster param.
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

// parseSIDResponse returns res[0][1][1] from an X-HTTP-Initial-Response like [[0,["c","<sid>","",8,12]]].
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

func isNetworkError(err error) bool {
	var ne *NetworkError
	return errors.As(err, &ne)
}
