package gchatmeow

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf16"
)

// ---------------------------------------------------------------------------
// Fake BrowserChannel server
// ---------------------------------------------------------------------------
//
// Emulates the request choreography of channel.py: GET /register (hands out a
// webchannel COMPASS cookie), then GET /events long-polls. The FIRST /events
// GET carries SID=null and its RESPONSE header X-HTTP-Initial-Response is what
// hands the client its SID (channel.py:419-425); the client then fires an ack
// GET (RID=rpc&AID=0, channel.py:429-442) and a ping POST
// (channel.py:347-360) before reading the first response's streamed body.
//
// Request classification (deterministic given the client's fixed
// choreography): the first GET /events with a non-null SID is always the ack;
// any later one is a re-poll.

type fakeReq struct {
	kind   string // register | longpoll-init | ack | longpoll | ping
	method string
	sid    string
	aid    string
	rid    string
	at     time.Time
}

type fakeChannel struct {
	srv *httptest.Server

	mu            sync.Mutex
	reqs          []fakeReq
	sidGetCount   int
	initCount     int
	registerCount int
	reRegistered  chan struct{} // closed on the 2nd register (re-register)
	reRegDone     bool

	sid string // SID handed out via X-HTTP-Initial-Response

	// Overridable handlers (defaults set in newFakeChannel).
	handleInit     func(w http.ResponseWriter, r *http.Request, f *fakeChannel)
	handleAck      func(w http.ResponseWriter, r *http.Request, f *fakeChannel)
	handleLongpoll func(w http.ResponseWriter, r *http.Request, f *fakeChannel, n int)
	handlePing     func(w http.ResponseWriter, r *http.Request, f *fakeChannel)

	pingCh  chan struct{} // closed-style signal on first ping
	pingged bool
}

func newFakeChannel(t *testing.T) *fakeChannel {
	t.Helper()
	f := &fakeChannel{
		sid:          "test-sid",
		pingCh:       make(chan struct{}, 8),
		reRegistered: make(chan struct{}),
	}
	// Default init: hand out SID, then hold the body open until the client
	// disconnects (so no re-poll happens and the ordering stays observable).
	f.handleInit = func(w http.ResponseWriter, r *http.Request, f *fakeChannel) {
		writeInitialSID(w, f.sid)
		<-r.Context().Done()
	}
	f.handleAck = func(w http.ResponseWriter, r *http.Request, f *fakeChannel) {
		w.WriteHeader(http.StatusOK)
	}
	f.handleLongpoll = func(w http.ResponseWriter, r *http.Request, f *fakeChannel, n int) {
		<-r.Context().Done()
	}
	f.handlePing = func(w http.ResponseWriter, r *http.Request, f *fakeChannel) {
		w.WriteHeader(http.StatusOK)
	}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeChannel) serve(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	rec := fakeReq{method: r.Method, sid: q.Get("SID"), aid: q.Get("AID"), rid: q.Get("RID"), at: time.Now()}

	switch {
	case strings.HasSuffix(r.URL.Path, "/register"):
		rec.kind = "register"
		f.mu.Lock()
		f.registerCount++
		if f.registerCount >= 2 && !f.reRegDone {
			f.reRegDone = true
			close(f.reRegistered)
		}
		f.mu.Unlock()
		f.record(rec)
		http.SetCookie(w, &http.Cookie{Name: "COMPASS", Value: "dynamite-ui=test-csessionid"})
		w.WriteHeader(http.StatusOK)
		return

	case strings.HasSuffix(r.URL.Path, "/events") && r.Method == http.MethodPost:
		rec.kind = "ping"
		f.record(rec)
		f.signalPing()
		f.handlePing(w, r, f)
		return

	case strings.HasSuffix(r.URL.Path, "/events") && q.Get("SID") == "null":
		rec.kind = "longpoll-init"
		f.mu.Lock()
		f.initCount++
		f.mu.Unlock()
		f.record(rec)
		f.handleInit(w, r, f)
		return

	case strings.HasSuffix(r.URL.Path, "/events"):
		f.mu.Lock()
		f.sidGetCount++
		n := f.sidGetCount
		f.mu.Unlock()
		if n == 1 {
			rec.kind = "ack"
			f.record(rec)
			f.handleAck(w, r, f)
		} else {
			rec.kind = "longpoll"
			f.record(rec)
			f.handleLongpoll(w, r, f, n)
		}
		return
	}

	http.Error(w, "unexpected request: "+r.URL.String(), http.StatusNotFound)
}

func (f *fakeChannel) record(r fakeReq) {
	f.mu.Lock()
	f.reqs = append(f.reqs, r)
	f.mu.Unlock()
}

func (f *fakeChannel) signalPing() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.pingged {
		f.pingged = true
		close(f.pingCh)
	}
}

func (f *fakeChannel) snapshot() []fakeReq {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]fakeReq, len(f.reqs))
	copy(out, f.reqs)
	return out
}

func (f *fakeChannel) initReqCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.initCount
}

func writeInitialSID(w http.ResponseWriter, sid string) {
	// Matches _parse_sid_response's expected shape: json[0][1][1] == sid
	// (channel.py:124-134): [[0,["c","<sid>","",8,12]]].
	w.Header().Set("X-HTTP-Initial-Response", fmt.Sprintf(`[[0,["c",%q,"",8,12]]]`, sid))
	w.WriteHeader(http.StatusOK)
	flush(w)
}

// writeFrame writes one BrowserChannel frame: "<utf16len>\n<payload>".
// length is the payload length in UTF-16 code units (channel.py:88-110).
func writeFrame(w http.ResponseWriter, payload string) {
	n := len(utf16.Encode([]rune(payload)))
	io.WriteString(w, fmt.Sprintf("%d\n%s", n, payload))
	flush(w)
}

func flush(w http.ResponseWriter) {
	if fl, ok := w.(http.Flusher); ok {
		fl.Flush()
	}
}

// newTestChannel builds a Channel wired to the fake server: a Session whose
// allowlist trusts the httptest host, and the unexported baseURL pointed at
// the fake server.
func newTestChannel(t *testing.T, f *fakeChannel) *Channel {
	t.Helper()
	sess, err := NewSession(nil, "")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	sess.allowedHostSuffixes = []string{testServerHost(t, f.srv.URL)}
	ch := NewChannel(sess)
	ch.baseURL = f.srv.URL + "/"
	return ch
}

func goroutineID() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	fields := strings.Fields(string(buf[:n]))
	if len(fields) < 2 {
		return -1
	}
	id, _ := strconv.ParseInt(fields[1], 10, 64)
	return id
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestRegisterExtractsSID(t *testing.T) {
	f := newFakeChannel(t)
	f.sid = "sid-XYZ-123"
	ch := newTestChannel(t, f)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- ch.Listen(ctx, 3, 20*time.Millisecond) }()

	select {
	case <-f.pingCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for initial ping (SID never acquired)")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Listen did not return after cancel")
	}

	if ch.sid != "sid-XYZ-123" {
		t.Fatalf("ch.sid = %q, want %q", ch.sid, "sid-XYZ-123")
	}
	// The ack request must carry the freshly extracted SID.
	var ack *fakeReq
	for _, r := range f.snapshot() {
		if r.kind == "ack" {
			rr := r
			ack = &rr
			break
		}
	}
	if ack == nil {
		t.Fatal("no ack request recorded")
	}
	if ack.sid != "sid-XYZ-123" {
		t.Fatalf("ack SID = %q, want %q", ack.sid, "sid-XYZ-123")
	}
}

func TestAckAndPingSentAfterRegister(t *testing.T) {
	f := newFakeChannel(t)
	ch := newTestChannel(t, f)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- ch.Listen(ctx, 3, 20*time.Millisecond) }()

	select {
	case <-f.pingCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for ping")
	}
	cancel()
	<-done

	reqs := f.snapshot()
	idxRegister, idxInit, idxAck, idxPing := -1, -1, -1, -1
	for i, r := range reqs {
		switch r.kind {
		case "register":
			if idxRegister < 0 {
				idxRegister = i
			}
		case "longpoll-init":
			if idxInit < 0 {
				idxInit = i
			}
		case "ack":
			if idxAck < 0 {
				idxAck = i
			}
		case "ping":
			if idxPing < 0 {
				idxPing = i
			}
		}
	}
	if idxRegister < 0 || idxInit < 0 || idxAck < 0 || idxPing < 0 {
		t.Fatalf("missing a request kind: register=%d init=%d ack=%d ping=%d\n%+v",
			idxRegister, idxInit, idxAck, idxPing, reqs)
	}
	// Order: register -> initial long-poll -> ack -> ping.
	if !(idxRegister < idxInit && idxInit < idxAck && idxAck < idxPing) {
		t.Fatalf("wrong request order: register=%d init=%d ack=%d ping=%d",
			idxRegister, idxInit, idxAck, idxPing)
	}
	// Ack must be a GET with RID=rpc and AID=0 (channel.py:429-431).
	ack := reqs[idxAck]
	if ack.method != http.MethodGet || ack.rid != "rpc" || ack.aid != "0" {
		t.Fatalf("ack request = %+v, want GET RID=rpc AID=0", ack)
	}
	// Ping must be a POST (forward channel, channel.py:333-339).
	if reqs[idxPing].method != http.MethodPost {
		t.Fatalf("ping method = %q, want POST", reqs[idxPing].method)
	}
}

func TestEventsDeliveredInOrder(t *testing.T) {
	f := newFakeChannel(t)
	f.handleInit = func(w http.ResponseWriter, r *http.Request, f *fakeChannel) {
		writeInitialSID(w, f.sid)
		// Three separate frames, each a container array with one inner
		// [array_id, data_array] pair.
		writeFrame(w, `[[1,["a"]]]`)
		writeFrame(w, `[[2,["b"]]]`)
		writeFrame(w, `[[3,["c"]]]`)
		<-r.Context().Done()
	}
	ch := newTestChannel(t, f)

	var (
		mu     sync.Mutex
		got    []string
		gids   []int64
		gotAll = make(chan struct{})
	)
	ch.OnReceiveArray = func(ctx context.Context, arr []byte) error {
		mu.Lock()
		got = append(got, string(arr))
		gids = append(gids, goroutineID())
		if len(got) == 3 {
			close(gotAll)
		}
		mu.Unlock()
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- ch.Listen(ctx, 3, 20*time.Millisecond) }()

	select {
	case <-gotAll:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out; received %v", got)
	}
	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	want := []string{`["a"]`, `["b"]`, `["c"]`}
	if len(got) != len(want) {
		t.Fatalf("got %d arrays %v, want %v", len(got), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("array[%d] = %q, want %q (order violated: %v)", i, got[i], want[i], got)
		}
	}
	for i := 1; i < len(gids); i++ {
		if gids[i] != gids[0] {
			t.Fatalf("OnReceiveArray called from multiple goroutines: %v", gids)
		}
	}
}

func TestCleanEOFRepolls(t *testing.T) {
	f := newFakeChannel(t)
	var (
		mu       sync.Mutex
		eofAt    time.Time
		repollAt time.Time
		repollCh = make(chan struct{})
	)
	f.handleInit = func(w http.ResponseWriter, r *http.Request, f *fakeChannel) {
		writeInitialSID(w, f.sid)
		writeFrame(w, `[[1,["x"]]]`)
		mu.Lock()
		eofAt = time.Now()
		mu.Unlock()
		// Return -> body EOFs -> clean exit -> client re-polls same SID.
	}
	f.handleLongpoll = func(w http.ResponseWriter, r *http.Request, f *fakeChannel, n int) {
		mu.Lock()
		if repollAt.IsZero() {
			repollAt = time.Now()
			close(repollCh)
		}
		mu.Unlock()
		<-r.Context().Done()
	}
	ch := newTestChannel(t, f)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	// Large backoff base: if the retry counter were NOT reset on clean EOF,
	// the re-poll would be delayed by >= 500ms.
	go func() { done <- ch.Listen(ctx, 3, 500*time.Millisecond) }()

	select {
	case <-repollCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for re-poll after clean EOF")
	}
	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	// Re-poll must use the same SID (no re-register).
	var repoll *fakeReq
	for _, r := range f.snapshot() {
		if r.kind == "longpoll" {
			rr := r
			repoll = &rr
			break
		}
	}
	if repoll == nil {
		t.Fatal("no re-poll recorded")
	}
	if repoll.sid != f.sid {
		t.Fatalf("re-poll SID = %q, want same SID %q", repoll.sid, f.sid)
	}
	// Retry counter reset => no backoff => near-immediate re-poll.
	if gap := repollAt.Sub(eofAt); gap > 250*time.Millisecond {
		t.Fatalf("re-poll gap %v too large; retry counter not reset (backoff applied)", gap)
	}
}

func TestUnknownSIDPropagates(t *testing.T) {
	f := newFakeChannel(t)
	f.handleInit = func(w http.ResponseWriter, r *http.Request, f *fakeChannel) {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, "Unknown SID")
	}
	ch := newTestChannel(t, f)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- ch.Listen(ctx, 3, 20*time.Millisecond) }()

	select {
	case err := <-done:
		if !errors.Is(err, ErrSIDInvalid) {
			t.Fatalf("Listen err = %v, want ErrSIDInvalid", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Listen did not return on 400 Unknown SID")
	}
}

func TestBackoffOnConnectError(t *testing.T) {
	f := newFakeChannel(t)
	// Disable server keep-alives so every attempt dials a FRESH connection.
	// net/http.Transport auto-retries an idempotent GET only on a *reused*
	// connection; on a fresh one it surfaces the connect error immediately.
	// Without this, a transport retry would silently swallow one of the two
	// server-side refusals and collapse the two expected backoffs into one.
	f.srv.Config.SetKeepAlivesEnabled(false)
	f.handleInit = func(w http.ResponseWriter, r *http.Request, f *fakeChannel) {
		if f.initReqCount() <= 2 {
			// Refuse: hijack and drop the connection with no response, so the
			// client's request fails at the transport layer (-> NetworkError).
			hj, ok := w.(http.Hijacker)
			if !ok {
				t.Errorf("ResponseWriter is not a Hijacker")
				return
			}
			conn, _, err := hj.Hijack()
			if err == nil {
				conn.Close()
			}
			return
		}
		writeInitialSID(w, f.sid)
		<-r.Context().Done()
	}
	ch := newTestChannel(t, f)

	const base = 30 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- ch.Listen(ctx, 3, base) }()

	select {
	case <-f.pingCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out; channel never recovered after connect errors")
	}
	elapsed := time.Since(start)
	cancel()
	<-done

	// Two failures => backoff of base (2^0) then 2*base (2^1) = 3*base = 90ms.
	// Assert clearly-nonzero cumulative backoff (slack for scheduling).
	if elapsed < 2*base {
		t.Fatalf("recovered in %v; expected >= %v of backoff between retries", elapsed, 2*base)
	}
	if ch.sid != f.sid {
		t.Fatalf("ch.sid = %q, want %q after recovery", ch.sid, f.sid)
	}
}

func TestLifetimeRecycle(t *testing.T) {
	f := newFakeChannel(t)
	ch := newTestChannel(t, f)
	// Inject a tiny lifetime via the unexported field: the loop's age check
	// (channel.py:218-219) fires on the first iteration.
	ch.maxAge = time.Nanosecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- ch.Listen(ctx, 3, 20*time.Millisecond) }()

	select {
	case err := <-done:
		if !errors.Is(err, ErrChannelLifetimeExpired) {
			t.Fatalf("Listen err = %v, want ErrChannelLifetimeExpired", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Listen did not return ErrChannelLifetimeExpired")
	}
}

// TestSIDExpiringReRegisters exercises the truncated-body ladder
// (channel.py:461-463 ClientPayloadError -> SIDExpiringError -> re-register,
// channel.py:233-240): a mid-stream truncation surfaces as io.ErrUnexpectedEOF
// -> ErrSIDExpiring, which must trigger a fresh GET /register (not a plain
// re-poll) with the retry counter incremented but backoff skipped.
func TestSIDExpiringReRegisters(t *testing.T) {
	f := newFakeChannel(t)
	f.handleInit = func(w http.ResponseWriter, r *http.Request, f *fakeChannel) {
		if f.initReqCount() == 1 {
			// Hijack and send a response that promises more bytes than it
			// delivers, then close -> the client's body read ends with
			// io.ErrUnexpectedEOF (aiohttp ClientPayloadError equivalent).
			hj, ok := w.(http.Hijacker)
			if !ok {
				t.Errorf("ResponseWriter is not a Hijacker")
				return
			}
			conn, _, err := hj.Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			frame := "11\n" + `[[1,["x"]]]` // one complete frame, then truncate
			raw := "HTTP/1.1 200 OK\r\n" +
				"X-HTTP-Initial-Response: [[0,[\"c\",\"" + f.sid + "\",\"\",8,12]]]\r\n" +
				"Content-Length: 1000000\r\n" +
				"\r\n" + frame
			_, _ = conn.Write([]byte(raw))
			// Give the client time to read the frame before we truncate.
			time.Sleep(30 * time.Millisecond)
			conn.Close()
			return
		}
		// Second registration's poll: hand out SID and hold.
		writeInitialSID(w, f.sid)
		<-r.Context().Done()
	}
	ch := newTestChannel(t, f)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	// Large backoff base: the SIDExpiring path must NOT back off, so re-register
	// happening quickly proves skipBackoff.
	go func() { done <- ch.Listen(ctx, 3, 500*time.Millisecond) }()

	select {
	case <-f.reRegistered:
	case <-time.After(3 * time.Second):
		t.Fatal("truncated body did not trigger a re-register")
	}
	cancel()
	<-done

	// At least two register requests: initial + the SIDExpiring re-register.
	registers := 0
	for _, r := range f.snapshot() {
		if r.kind == "register" {
			registers++
		}
	}
	if registers < 2 {
		t.Fatalf("register count = %d, want >= 2 (re-register on SID expiry)", registers)
	}
}

// TestReadIdleTriggersDisconnectAndReconnect exercises the 60s read-idle
// watchdog (here shortened via the unexported readIdleTimeout) and the
// connect/disconnect/reconnect event sequence (channel.py:251-253, 474-482):
// a live poll that goes silent must fire OnDisconnect, and the next poll's
// first chunk must fire OnReconnect (Task 8's gap-sync hook).
func TestReadIdleTriggersDisconnectAndReconnect(t *testing.T) {
	f := newFakeChannel(t)
	f.handleInit = func(w http.ResponseWriter, r *http.Request, f *fakeChannel) {
		writeInitialSID(w, f.sid)
		writeFrame(w, `[[1,["x"]]]`) // connect
		<-r.Context().Done()         // then go silent -> read-idle fires
	}
	f.handleLongpoll = func(w http.ResponseWriter, r *http.Request, f *fakeChannel, n int) {
		writeFrame(w, `[[2,["y"]]]`) // reconnect
		<-r.Context().Done()
	}
	ch := newTestChannel(t, f)
	ch.readIdleTimeout = 50 * time.Millisecond // inject a short idle watchdog

	var (
		mu          sync.Mutex
		connects    int
		disconnects int
		reconnects  int
	)
	reconnectCh := make(chan struct{})
	ch.OnConnect = func(ctx context.Context) { mu.Lock(); connects++; mu.Unlock() }
	ch.OnDisconnect = func(ctx context.Context) { mu.Lock(); disconnects++; mu.Unlock() }
	ch.OnReconnect = func(ctx context.Context) {
		mu.Lock()
		reconnects++
		if reconnects == 1 {
			close(reconnectCh)
		}
		mu.Unlock()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- ch.Listen(ctx, 5, 10*time.Millisecond) }()

	select {
	case <-reconnectCh:
	case <-time.After(3 * time.Second):
		t.Fatal("read-idle -> disconnect -> reconnect cycle did not complete")
	}
	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	if connects != 1 {
		t.Fatalf("OnConnect fired %d times, want exactly 1", connects)
	}
	if disconnects < 1 {
		t.Fatalf("OnDisconnect fired %d times, want >= 1", disconnects)
	}
	if reconnects < 1 {
		t.Fatalf("OnReconnect fired %d times, want >= 1", reconnects)
	}
}

// TestDebugDumpWritesFrameAndArray covers choreography point 7: with
// GCHAT_DEBUG_DUMP set, each received chunk is dumped as a self-contained WIRE
// frame ("<utf16len>\n<payload>", byte-exact + replayable per capture-fixtures
// SKILL.md), and each decoded data_array as .json.
func TestDebugDumpWritesFrameAndArray(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("GCHAT_DEBUG_DUMP", dir)

	f := newFakeChannel(t)
	gotArr := make(chan struct{})
	f.handleInit = func(w http.ResponseWriter, r *http.Request, f *fakeChannel) {
		writeInitialSID(w, f.sid)
		writeFrame(w, `[[7,["x"]]]`)
		<-r.Context().Done()
	}
	ch := newTestChannel(t, f)
	ch.OnReceiveArray = func(ctx context.Context, arr []byte) error {
		select {
		case <-gotArr:
		default:
			close(gotArr)
		}
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- ch.Listen(ctx, 3, 20*time.Millisecond) }()

	select {
	case <-gotArr:
	case <-time.After(3 * time.Second):
		t.Fatal("array never delivered")
	}
	cancel()
	<-done

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var rawBody, jsonBody string
	for _, e := range entries {
		b, _ := os.ReadFile(filepath.Join(dir, e.Name()))
		switch {
		case strings.HasPrefix(e.Name(), "chunk-") && strings.HasSuffix(e.Name(), ".raw"):
			rawBody = string(b)
		case strings.HasPrefix(e.Name(), "array-") && strings.HasSuffix(e.Name(), ".json"):
			jsonBody = string(b)
		}
	}
	// Frame must include the length prefix and be byte-exact to the wire.
	if rawBody != "11\n[[7,[\"x\"]]]" {
		t.Fatalf("chunk .raw = %q, want framed %q", rawBody, "11\n[[7,[\"x\"]]]")
	}
	if jsonBody != `["x"]` {
		t.Fatalf("array .json = %q, want %q", jsonBody, `["x"]`)
	}
}

// TestUnknownSIDInStatusLinePropagates covers the check-BOTH semantics of
// channel.py:408-411: a 400 whose reason phrase is "Unknown SID" but whose
// body does NOT contain it must still map to ErrSIDInvalid. Go's resp.Status
// is the full "400 Unknown SID" line, so this exercises the status-line branch
// (which an == "Unknown SID" equality check would silently miss).
func TestUnknownSIDInStatusLinePropagates(t *testing.T) {
	f := newFakeChannel(t)
	f.handleInit = func(w http.ResponseWriter, r *http.Request, f *fakeChannel) {
		// Hijack to control the reason phrase: httptest's WriteHeader always
		// emits Go's canonical reason ("Bad Request"), so we must write the raw
		// status line ourselves. Empty body -> only the status line carries it.
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Errorf("ResponseWriter is not a Hijacker")
			return
		}
		conn, _, err := hj.Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		_, _ = conn.Write([]byte("HTTP/1.1 400 Unknown SID\r\nContent-Length: 0\r\n\r\n"))
		conn.Close()
	}
	ch := newTestChannel(t, f)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- ch.Listen(ctx, 3, 20*time.Millisecond) }()

	select {
	case err := <-done:
		if !errors.Is(err, ErrSIDInvalid) {
			t.Fatalf("Listen err = %v, want ErrSIDInvalid (status-line Unknown SID)", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Listen did not return on 400 Unknown SID status line")
	}
}

// TestChunkedTruncationReRegisters hardens the highest-risk branch with a
// REAL wire shape: a chunked response truncated mid-stream (no terminating
// 0-length chunk) makes the client's body read end in io.ErrUnexpectedEOF,
// which must map to ErrSIDExpiring -> re-register (channel.py:461-463, 233-240).
// The Content-Length variant (TestSIDExpiringReRegisters) exercises the same
// mapping; this proves it also holds for chunked transfer-encoding, the shape
// Google actually uses.
func TestChunkedTruncationReRegisters(t *testing.T) {
	f := newFakeChannel(t)
	f.handleInit = func(w http.ResponseWriter, r *http.Request, f *fakeChannel) {
		if f.initReqCount() == 1 {
			hj, ok := w.(http.Hijacker)
			if !ok {
				t.Errorf("ResponseWriter is not a Hijacker")
				return
			}
			conn, _, err := hj.Hijack()
			if err != nil {
				t.Errorf("hijack: %v", err)
				return
			}
			frame := "11\n" + `[[1,["x"]]]` // one complete BrowserChannel frame
			// A single chunk, then close WITHOUT the terminating "0\r\n\r\n" ->
			// truncated chunked stream -> io.ErrUnexpectedEOF on the client.
			raw := "HTTP/1.1 200 OK\r\n" +
				"X-HTTP-Initial-Response: [[0,[\"c\",\"" + f.sid + "\",\"\",8,12]]]\r\n" +
				"Transfer-Encoding: chunked\r\n" +
				"\r\n" +
				fmt.Sprintf("%x\r\n%s\r\n", len(frame), frame)
			_, _ = conn.Write([]byte(raw))
			time.Sleep(30 * time.Millisecond) // let the client read the frame
			conn.Close()
			return
		}
		writeInitialSID(w, f.sid)
		<-r.Context().Done()
	}
	ch := newTestChannel(t, f)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- ch.Listen(ctx, 3, 500*time.Millisecond) }()

	select {
	case <-f.reRegistered:
	case <-time.After(3 * time.Second):
		t.Fatal("chunked truncation did not trigger a re-register")
	}
	cancel()
	<-done

	registers := 0
	for _, r := range f.snapshot() {
		if r.kind == "register" {
			registers++
		}
	}
	if registers < 2 {
		t.Fatalf("register count = %d, want >= 2 (re-register on chunked truncation)", registers)
	}
}

// TestMalformedContainerFrameSkipped: a frame whose whole container array is
// undecodable must be dropped on its own, leaving the channel live for the
// frames that follow. Returning an error here instead would propagate through
// readBody/longpollRequest into Listen's `default:` branch and tear the poll
// session down, contrary to the project's log-and-skip invariant for stream
// decode errors.
func TestMalformedContainerFrameSkipped(t *testing.T) {
	f := newFakeChannel(t)
	f.handleInit = func(w http.ResponseWriter, r *http.Request, f *fakeChannel) {
		writeInitialSID(w, f.sid)
		writeFrame(w, `[[1,["a"]]]`)
		writeFrame(w, `this is not a json array`)
		writeFrame(w, `[[2,["b"]]]`)
		<-r.Context().Done()
	}
	ch := newTestChannel(t, f)

	var (
		mu     sync.Mutex
		got    []string
		gotAll = make(chan struct{})
	)
	ch.OnReceiveArray = func(ctx context.Context, arr []byte) error {
		mu.Lock()
		got = append(got, string(arr))
		if len(got) == 2 {
			close(gotAll)
		}
		mu.Unlock()
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- ch.Listen(ctx, 3, 20*time.Millisecond) }()

	select {
	case <-gotAll:
	case err := <-done:
		t.Fatalf("Listen returned early (%v); a malformed frame killed the channel", err)
	case <-time.After(3 * time.Second):
		mu.Lock()
		defer mu.Unlock()
		t.Fatalf("timed out; received %v, want the frames either side of the bad one", got)
	}
	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	want := []string{`["a"]`, `["b"]`}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// TestMalformedInnerElementsSkipped: within ONE container, a bad inner
// element must cost only itself -- the well-formed pairs beside it, including
// those after it, must still be delivered.
func TestMalformedInnerElementsSkipped(t *testing.T) {
	f := newFakeChannel(t)
	f.handleInit = func(w http.ResponseWriter, r *http.Request, f *fakeChannel) {
		writeInitialSID(w, f.sid)
		// good pair, wrong-length inner, non-numeric array_id, good pair.
		writeFrame(w, `[[1,["a"]],[2],["x",["ignored"],9],[3,["c"]]]`)
		<-r.Context().Done()
	}
	ch := newTestChannel(t, f)

	var (
		mu     sync.Mutex
		got    []string
		gotAll = make(chan struct{})
	)
	ch.OnReceiveArray = func(ctx context.Context, arr []byte) error {
		mu.Lock()
		got = append(got, string(arr))
		if len(got) == 2 {
			close(gotAll)
		}
		mu.Unlock()
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- ch.Listen(ctx, 3, 20*time.Millisecond) }()

	select {
	case <-gotAll:
	case err := <-done:
		t.Fatalf("Listen returned early (%v); a malformed inner element killed the channel", err)
	case <-time.After(3 * time.Second):
		mu.Lock()
		defer mu.Unlock()
		t.Fatalf("timed out; received %v, want both good pairs", got)
	}
	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	want := []string{`["a"]`, `["c"]`}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("got %v, want %v", got, want)
	}
}
