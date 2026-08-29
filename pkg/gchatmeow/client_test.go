package gchatmeow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/pblite"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// ---------------------------------------------------------------------------
// splitEventBodies
// ---------------------------------------------------------------------------

// TestSplitEventBodies verifies the flattening semantics: an Event carrying
// a top-level body (field 4) plus two embedded bodies (field 8) flattens to
// three Events, in order -- parent first, then one per embedded body -- with
// the parent's fields copied and each copy's type taken from the body.
func TestSplitEventBodies(t *testing.T) {
	parent := &pb.Event{
		GroupId: &pb.GroupId{
			Id: &pb.GroupId_SpaceId{SpaceId: &pb.SpaceId{SpaceId: proto.String("space-42")}},
		},
		Type: pb.Event_MESSAGE_POSTED.Enum(),
		Body: &pb.Event_EventBody{
			EventType: pb.Event_MESSAGE_POSTED.Enum(),
			TraceId:   proto.Int64(100),
		},
		Bodies: []*pb.Event_EventBody{
			{EventType: pb.Event_MESSAGE_UPDATED.Enum(), TraceId: proto.Int64(200)},
			{EventType: pb.Event_MESSAGE_DELETED.Enum(), TraceId: proto.Int64(300)},
		},
	}

	got := splitEventBodies(parent)
	if len(got) != 3 {
		t.Fatalf("splitEventBodies returned %d events, want 3", len(got))
	}

	// Parent must have its bodies cleared.
	if len(got[0].Bodies) != 0 {
		t.Errorf("parent event still has %d bodies, want 0 (ClearField)", len(got[0].Bodies))
	}

	wantTrace := []int64{100, 200, 300}
	wantType := []pb.Event_EventType{
		pb.Event_MESSAGE_POSTED,  // parent keeps its own type
		pb.Event_MESSAGE_UPDATED, // copy: type = body.event_type
		pb.Event_MESSAGE_DELETED,
	}
	for i, ev := range got {
		if ev.GetBody().GetTraceId() != wantTrace[i] {
			t.Errorf("event[%d] body trace_id = %d, want %d", i, ev.GetBody().GetTraceId(), wantTrace[i])
		}
		if ev.GetType() != wantType[i] {
			t.Errorf("event[%d] type = %v, want %v", i, ev.GetType(), wantType[i])
		}
		// Parent fields (group_id) must be copied onto every flattened event.
		if ev.GetGroupId().GetSpaceId().GetSpaceId() != "space-42" {
			t.Errorf("event[%d] lost parent group_id: %v", i, ev.GetGroupId())
		}
	}
}

// TestSplitEventBodiesOnlyTopLevel verifies that an Event with a body but no
// embedded bodies yields exactly one event (itself).
func TestSplitEventBodiesOnlyTopLevel(t *testing.T) {
	evt := &pb.Event{
		Type: pb.Event_MESSAGE_POSTED.Enum(),
		Body: &pb.Event_EventBody{EventType: pb.Event_MESSAGE_POSTED.Enum()},
	}
	got := splitEventBodies(evt)
	if len(got) != 1 {
		t.Fatalf("splitEventBodies returned %d events, want 1", len(got))
	}
}

// TestSplitEventBodiesNoBody verifies that an Event with NO top-level body but
// two embedded bodies yields exactly two events (the parent is not yielded).
func TestSplitEventBodiesNoBody(t *testing.T) {
	evt := &pb.Event{
		Bodies: []*pb.Event_EventBody{
			{EventType: pb.Event_MESSAGE_UPDATED.Enum()},
			{EventType: pb.Event_MESSAGE_DELETED.Enum()},
		},
	}
	got := splitEventBodies(evt)
	if len(got) != 2 {
		t.Fatalf("splitEventBodies returned %d events, want 2", len(got))
	}
}

// TestOnReceiveArrayDispatch drives the full inbound path: a webchannel
// data_array whose element[0] is a StreamEventsResponse pblite array holding
// an Event with 1 body + 2 bodies must produce 3 ordered OnStreamEvent calls.
func TestOnReceiveArrayDispatch(t *testing.T) {
	ser := &pb.StreamEventsResponse{
		Event: &pb.Event{
			Type: pb.Event_MESSAGE_POSTED.Enum(),
			Body: &pb.Event_EventBody{
				EventType: pb.Event_MESSAGE_POSTED.Enum(),
				TraceId:   proto.Int64(1),
			},
			Bodies: []*pb.Event_EventBody{
				{EventType: pb.Event_MESSAGE_UPDATED.Enum(), TraceId: proto.Int64(2)},
				{EventType: pb.Event_MESSAGE_DELETED.Enum(), TraceId: proto.Int64(3)},
			},
		},
	}
	serBytes, err := pblite.Marshal(ser)
	if err != nil {
		t.Fatalf("pblite.Marshal: %v", err)
	}
	// data_array = [ <StreamEventsResponse pblite array>, ... ].
	dataArray, err := json.Marshal([]json.RawMessage{json.RawMessage(serBytes)})
	if err != nil {
		t.Fatalf("marshal data array: %v", err)
	}

	var mu sync.Mutex
	var traces []int64
	c := &Client{
		OnStreamEvent: func(ctx context.Context, ev *pb.Event) {
			mu.Lock()
			traces = append(traces, ev.GetBody().GetTraceId())
			mu.Unlock()
		},
	}

	if err := c.onReceiveArray(context.Background(), dataArray); err != nil {
		t.Fatalf("onReceiveArray: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	want := []int64{1, 2, 3}
	if len(traces) != len(want) {
		t.Fatalf("got %d stream events, want %d (traces=%v)", len(traces), len(want), traces)
	}
	for i := range want {
		if traces[i] != want[i] {
			t.Errorf("stream event[%d] trace = %d, want %d (order not preserved)", i, traces[i], want[i])
		}
	}
}

// TestOnReceiveArrayNoop verifies the "noop" keep-alive is dropped silently
// and never reaches OnStreamEvent.
func TestOnReceiveArrayNoop(t *testing.T) {
	called := false
	c := &Client{OnStreamEvent: func(ctx context.Context, ev *pb.Event) { called = true }}
	if err := c.onReceiveArray(context.Background(), []byte(`["noop"]`)); err != nil {
		t.Fatalf("onReceiveArray(noop): %v", err)
	}
	if called {
		t.Error("noop keep-alive was dispatched as a stream event")
	}
}

// ---------------------------------------------------------------------------
// Supervision ladder (research 07 §6)
// ---------------------------------------------------------------------------

// scriptedChannel is a fake channelListener whose Listen returns a scripted
// sequence of errors, one per call, so the supervision ladder can be exercised
// deterministically. Steps may invoke the captured callbacks to simulate the
// channel connecting/reconnecting/disconnecting.
type scriptedChannel struct {
	steps []func(f *scriptedChannel, ctx context.Context) error

	mu          sync.Mutex
	idx         int
	listenCalls int

	enteredOnce sync.Once
	entered     chan struct{}

	onReceiveArray func(ctx context.Context, arr []byte) error
	onConnect      func(ctx context.Context)
	onReconnect    func(ctx context.Context)
	onDisconnect   func(ctx context.Context)
}

func newScriptedChannel(steps ...func(f *scriptedChannel, ctx context.Context) error) *scriptedChannel {
	return &scriptedChannel{steps: steps, entered: make(chan struct{})}
}

func (f *scriptedChannel) Listen(ctx context.Context, maxRetries int, retryBackoffBase time.Duration) error {
	f.enteredOnce.Do(func() { close(f.entered) })
	f.mu.Lock()
	i := f.idx
	f.idx++
	f.listenCalls++
	f.mu.Unlock()
	if i < len(f.steps) {
		return f.steps[i](f, ctx)
	}
	<-ctx.Done()
	return ctx.Err()
}

func (f *scriptedChannel) SendStreamEvent(ctx context.Context, ev *pb.StreamEventsRequest) error {
	return nil
}
func (f *scriptedChannel) SetOnReceiveArray(fn func(ctx context.Context, arr []byte) error) {
	f.onReceiveArray = fn
}
func (f *scriptedChannel) SetOnConnect(fn func(ctx context.Context))    { f.onConnect = fn }
func (f *scriptedChannel) SetOnReconnect(fn func(ctx context.Context))  { f.onReconnect = fn }
func (f *scriptedChannel) SetOnDisconnect(fn func(ctx context.Context)) { f.onDisconnect = fn }

func (f *scriptedChannel) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listenCalls
}

type stateRecorder struct {
	mu     sync.Mutex
	states []ConnState
	errs   []error
}

func (r *stateRecorder) record(s ConnState, err error) {
	r.mu.Lock()
	r.states = append(r.states, s)
	r.errs = append(r.errs, err)
	r.mu.Unlock()
}

func (r *stateRecorder) snapshot() []ConnState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]ConnState(nil), r.states...)
}

// ladderClient builds a Client wired to fake and rec, with the XSRF token
// pre-seeded fresh (so the connect loop never hits the network) and all
// timers/backoffs shrunk so the test runs instantly.
func ladderClient(t *testing.T, fake channelListener, rec *stateRecorder) *Client {
	t.Helper()
	sess, err := NewSession(map[string]string{}, "")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	c := &Client{
		session:             sess,
		maxRetries:          1,
		retryBackoffBase:    time.Millisecond,
		reconnectBackoffMin: time.Millisecond,
		reconnectBackoffMax: time.Millisecond,
		xsrfRefreshInterval: time.Hour,
		sidFatalRetryFloor:  time.Millisecond,
		sidFatalRetryCap:    2 * time.Millisecond,
		newChannel:          func() channelListener { return fake },
		OnConnectionState:   rec.record,
		OnStreamEvent:       func(ctx context.Context, ev *pb.Event) {},
	}
	c.xsrfToken = "test-token"
	c.lastTokenRefresh = time.Now()
	return c
}

func statesEqual(a, b []ConnState) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func ret(err error) func(f *scriptedChannel, ctx context.Context) error {
	return func(f *scriptedChannel, ctx context.Context) error { return err }
}

func TestSupervisionLadder(t *testing.T) {
	authErr := &UnexpectedStatusError{Status: http.StatusUnauthorized}
	netErr := &NetworkError{Err: errors.New("boom")}

	tests := []struct {
		name       string
		steps      []func(f *scriptedChannel, ctx context.Context) error
		wantStates []ConnState
		wantCalls  int
		wantReturn error // sentinel to errors.Is against; nil = returns nil
	}{
		{
			name:       "lifetime expired -> silent reconnect",
			steps:      []func(*scriptedChannel, context.Context) error{ret(ErrChannelLifetimeExpired), ret(authErr)},
			wantStates: []ConnState{ConnStateBadCredentials},
			wantCalls:  2,
		},
		{
			name:       "sid expiring -> immediate reconnect",
			steps:      []func(*scriptedChannel, context.Context) error{ret(ErrSIDExpiring), ret(authErr)},
			wantStates: []ConnState{ConnStateBadCredentials},
			wantCalls:  2,
		},
		{
			name:       "network error -> transient + backoff reconnect",
			steps:      []func(*scriptedChannel, context.Context) error{ret(netErr), ret(authErr)},
			wantStates: []ConnState{ConnStateTransient, ConnStateBadCredentials},
			wantCalls:  2,
		},
		{
			// A fatal is REPORTED but is no longer the end of the account:
			// the loop waits on its own slower tier and starts over. The
			// fifth step is the terminator that proves it came back -- without
			// the retry the script would never reach it and this hangs.
			name: "four sid-invalid in a row -> fatal, then retry",
			steps: []func(*scriptedChannel, context.Context) error{
				ret(ErrSIDInvalid), ret(ErrSIDInvalid), ret(ErrSIDInvalid), ret(ErrSIDInvalid),
				ret(authErr),
			},
			wantStates: []ConnState{ConnStateFatal, ConnStateBadCredentials},
			wantCalls:  5,
		},
		{
			name: "sid-invalid interspersed with recycle -> counter resets, no fatal",
			steps: []func(*scriptedChannel, context.Context) error{
				ret(ErrSIDInvalid), ret(ErrChannelLifetimeExpired),
				ret(ErrSIDInvalid), ret(ErrChannelLifetimeExpired),
				ret(ErrSIDInvalid), ret(ErrChannelLifetimeExpired),
				ret(ErrSIDInvalid), ret(authErr),
			},
			wantStates: []ConnState{ConnStateBadCredentials},
			wantCalls:  8,
		},
		{
			name:       "auth error -> bad credentials + return",
			steps:      []func(*scriptedChannel, context.Context) error{ret(authErr)},
			wantStates: []ConnState{ConnStateBadCredentials},
			wantCalls:  1,
		},
		{
			name: "connect/disconnect/reconnect callbacks map to states",
			steps: []func(*scriptedChannel, context.Context) error{
				func(f *scriptedChannel, ctx context.Context) error {
					f.onConnect(ctx)
					f.onDisconnect(ctx)
					f.onReconnect(ctx)
					return authErr
				},
			},
			wantStates: []ConnState{ConnStateConnected, ConnStateTransient, ConnStateConnected, ConnStateBadCredentials},
			wantCalls:  1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := newScriptedChannel(tc.steps...)
			rec := &stateRecorder{}
			c := ladderClient(t, fake, rec)

			err := c.Connect(context.Background())

			if tc.wantReturn != nil {
				if !errors.Is(err, tc.wantReturn) {
					t.Errorf("Connect returned %v, want errors.Is(%v)", err, tc.wantReturn)
				}
			}
			if got := rec.snapshot(); !statesEqual(got, tc.wantStates) {
				t.Errorf("states = %v, want %v", got, tc.wantStates)
			}
			if got := fake.calls(); got != tc.wantCalls {
				t.Errorf("Listen called %d times, want %d", got, tc.wantCalls)
			}
		})
	}
}

// TestSupervisionCleanCancel verifies that cancelling the context (via
// Disconnect) while the channel is blocked returns cleanly with no error and
// no state change.
func TestSupervisionCleanCancel(t *testing.T) {
	fake := newScriptedChannel() // no steps -> Listen blocks on ctx.Done()
	rec := &stateRecorder{}
	c := ladderClient(t, fake, rec)

	done := make(chan error, 1)
	go func() { done <- c.Connect(context.Background()) }()

	select {
	case <-fake.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("Connect never entered Listen")
	}
	c.Disconnect()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Connect returned %v, want nil on clean disconnect", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Connect did not return after Disconnect")
	}
	if got := rec.snapshot(); len(got) != 0 {
		t.Errorf("states = %v, want none on clean disconnect", got)
	}
}

// hasCancel reports whether Connect has installed its cancel func yet.
// Test-only observability (defined here, not in client.go, since it exists
// solely to pin down the timing guarantee below) proving c.cancel is stored
// under mu before Connect does anything else -- ensureToken, newChannel,
// Listen, or spawning the xsrf refresh goroutine.
func (c *Client) hasCancel() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cancel != nil
}

// TestConnectInstallsCancelBeforeChannelSetup proves there is no lost-cancel
// window: Connect must store c.cancel (guarded by mu) before it ever creates
// a channel or calls Listen, i.e. before any blocking or async work. It
// blocks Connect inside newChannel (the first hook Connect calls after
// installing cancel) and, while Connect is paused there, asserts hasCancel()
// is already true and that Disconnect() takes effect immediately.
//
// The connector's wireAndStart does `go conn.Connect(ctx)`, so anything that
// deferred installing cancel until inside a nested goroutine (e.g. spawning
// the supervision loop and setting c.cancel from within it) would leave a
// window where a racing Disconnect() is silently dropped. This test would
// fail against that structuring: entered would still close (newChannel is
// called eventually), but hasCancel() would very likely still be false at
// that point, or -- pushed further -- Disconnect() would have no cancel to
// call and Connect would hang past the 2s deadline.
func TestConnectInstallsCancelBeforeChannelSetup(t *testing.T) {
	fake := newScriptedChannel() // no steps -> Listen blocks on ctx.Done()
	rec := &stateRecorder{}
	c := ladderClient(t, fake, rec)

	entered := make(chan struct{})
	release := make(chan struct{})
	c.newChannel = func() channelListener {
		close(entered)
		<-release
		return fake
	}

	done := make(chan error, 1)
	go func() { done <- c.Connect(context.Background()) }()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("Connect never reached newChannel")
	}

	if !c.hasCancel() {
		t.Fatal("cancel not installed before newChannel/Listen setup -- lost-cancel window")
	}
	c.Disconnect()
	close(release)

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Connect returned %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Connect did not return after Disconnect")
	}
}

// TestDisconnectImmediatelyAfterConnectStops is an end-to-end regression
// guard proving that a Disconnect() racing the `go conn.Connect(ctx)` call in
// the connector's wireAndStart is never PERMANENTLY lost. It spawns Connect
// and, with NO synchronization barrier at all, races a second goroutine that
// hammers Disconnect() in a tight retry loop against Connect's own goroutine
// start -- the hammering continues until Connect actually returns (not just
// for a fixed number of iterations), so this only catches a cancel that is
// never installed (or a Disconnect()/mu bug) within the 2s deadline; it does
// NOT by itself prove cancel is installed before any particular point in
// Connect's own sequence (any finite scheduling delay is absorbed by the
// retry). TestConnectInstallsCancelBeforeChannelSetup above is the
// deterministic test for that install-ordering guarantee. Repeated across
// many iterations under `-race` to make the race window meaningful.
func TestDisconnectImmediatelyAfterConnectStops(t *testing.T) {
	for i := 0; i < 50; i++ {
		fake := newScriptedChannel() // no steps -> Listen blocks on ctx.Done()
		rec := &stateRecorder{}
		c := ladderClient(t, fake, rec)

		done := make(chan error, 1)
		go func() { done <- c.Connect(context.Background()) }()

		// No barrier: race Disconnect against Connect's goroutine start,
		// retrying until it lands (Connect returns) or the deadline below
		// fires.
		stop := make(chan struct{})
		go func() {
			for {
				select {
				case <-stop:
					return
				default:
					c.Disconnect()
				}
			}
		}()

		select {
		case err := <-done:
			close(stop)
			if err != nil {
				t.Fatalf("iteration %d: Connect returned %v, want nil", i, err)
			}
		case <-time.After(2 * time.Second):
			close(stop)
			t.Fatalf("iteration %d: Connect did not return after Disconnect race", i)
		}
	}
}

// TestSIDInvalidResyncPaced verifies that consecutive SID-invalid resyncs are
// paced with a growing backoff (not fired back-to-back in milliseconds), so a
// brief transient "Unknown SID" storm backs off before the fatal cap instead of
// killing a recoverable connection. It injects sleepFn to observe the backoff
// durations deterministically without real sleeps.
func TestSIDInvalidResyncPaced(t *testing.T) {
	authErr := &UnexpectedStatusError{Status: http.StatusUnauthorized}
	fake := newScriptedChannel(
		ret(ErrSIDInvalid), ret(ErrSIDInvalid), ret(ErrSIDInvalid), ret(ErrSIDInvalid),
		// Terminator. Without the post-fatal retry the loop would return
		// before reaching this, so its absence would hang rather than fail.
		ret(authErr),
	)
	rec := &stateRecorder{}
	c := ladderClient(t, fake, rec)
	// Spread min/max so growth is observable (ladderClient pins both to 1ms).
	c.reconnectBackoffMin = 10 * time.Millisecond
	c.reconnectBackoffMax = 100 * time.Millisecond
	// Far above the ladder's ceiling, so the assertion below distinguishes the
	// two tiers rather than just observing "some sleep happened".
	c.sidFatalRetryFloor = 5 * time.Second
	c.sidFatalRetryCap = time.Minute

	var mu sync.Mutex
	var slept []time.Duration
	c.sleepFn = func(_ context.Context, d time.Duration) error {
		mu.Lock()
		slept = append(slept, d)
		mu.Unlock()
		return nil // don't actually sleep
	}

	c.Connect(context.Background())

	if got := rec.snapshot(); !statesEqual(got, []ConnState{ConnStateFatal, ConnStateBadCredentials}) {
		t.Errorf("states = %v, want [FATAL BAD_CREDENTIALS]", got)
	}
	mu.Lock()
	defer mu.Unlock()
	// resyncCount 1,2,3 each pace on the ladder; the 4th exceeds the cap, goes
	// fatal, and paces on the SLOWER tier -- so one more sleep than before.
	if len(slept) != maxSIDInvalidResyncs+1 {
		t.Fatalf("paced %d times (%v), want %d", len(slept), slept, maxSIDInvalidResyncs+1)
	}
	ladder := slept[:maxSIDInvalidResyncs]
	for i := 1; i < len(ladder); i++ {
		if ladder[i] <= ladder[i-1] {
			t.Errorf("ladder backoff did not grow: slept[%d]=%v <= slept[%d]=%v (%v)",
				i, ladder[i], i-1, ladder[i-1], slept)
		}
	}
	// THE point of this test now: the post-fatal wait is its own tier. If it
	// rejoined the ladder, a storm would be retried seconds later -- which is
	// the storm the resync cap exists to stop.
	if fatalWait := slept[maxSIDInvalidResyncs]; fatalWait != c.sidFatalRetryFloor {
		t.Errorf("post-fatal wait = %v, want the fatal floor %v (not the ladder's %v)",
			fatalWait, c.sidFatalRetryFloor, c.reconnectBackoffMax)
	}
}

// TestSIDInvalidResyncCancelDuringBackoff verifies that cancelling the context
// while a resync backoff is pending returns cleanly (nil), with no Fatal state.
func TestSIDInvalidResyncCancelDuringBackoff(t *testing.T) {
	fake := newScriptedChannel(ret(ErrSIDInvalid), ret(ErrSIDInvalid))
	rec := &stateRecorder{}
	c := ladderClient(t, fake, rec)
	// sleepFn reports ctx-done on the first resync backoff, as sleepOrDone would
	// when Disconnect cancels mid-wait.
	c.sleepFn = func(_ context.Context, _ time.Duration) error {
		return context.Canceled
	}

	if err := c.Connect(context.Background()); err != nil {
		t.Errorf("Connect returned %v, want nil when cancelled during resync backoff", err)
	}
	for _, s := range rec.snapshot() {
		if s == ConnStateFatal {
			t.Errorf("unexpected FATAL state on clean cancel: %v", rec.snapshot())
		}
	}
}

// ---------------------------------------------------------------------------
// XSRF refresh on 401 (brief's 401-retry mandate)
// ---------------------------------------------------------------------------

// TestXSRFRefreshOn401 verifies that a 401 from an /api/* RPC triggers a token
// re-fetch via /mole/world and a single retry with the fresh token.
func TestXSRFRefreshOn401(t *testing.T) {
	const oldToken = "stale-token"
	const newToken = "fresh-token"

	var mu sync.Mutex
	apiCalls := 0
	moleCalls := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/api/"):
			mu.Lock()
			apiCalls++
			mu.Unlock()
			if r.Header.Get("x-framework-xsrf-token") != newToken {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			// Fresh token accepted: return an (empty) valid proto response.
			body, _ := proto.Marshal(&pb.GetSelfUserStatusResponse{})
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(body)
		case r.URL.Path == "/mole/world":
			mu.Lock()
			moleCalls++
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprintf(w,
				`<script>window.WIZ_global_data = {"qwAQke":"NotSignIn","SMqcke":%q};</script>`, newToken)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	sess, err := NewSession(map[string]string{}, "")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	sess.moleWorldBaseURL = srv.URL

	c := &Client{
		session:             sess,
		baseURL:             srv.URL,
		xsrfRefreshInterval: time.Hour,
	}
	c.xsrfToken = oldToken

	resp, err := c.GetSelfUserStatus(context.Background(), &pb.GetSelfUserStatusRequest{})
	if err != nil {
		t.Fatalf("GetSelfUserStatus after 401 refresh: %v", err)
	}
	if resp == nil {
		t.Fatal("nil response after successful retry")
	}
	if got := c.XSRFToken(); got != newToken {
		t.Errorf("token after refresh = %q, want %q", got, newToken)
	}
	mu.Lock()
	defer mu.Unlock()
	if apiCalls != 2 {
		t.Errorf("api called %d times, want 2 (401 then retry)", apiCalls)
	}
	if moleCalls != 1 {
		t.Errorf("/mole/world called %d times, want 1", moleCalls)
	}
}

// ---------------------------------------------------------------------------
// Client.FetchXSRFToken / Client.UserAgent (Task 10: exported so the
// connector's login flow -- a different package -- can validate submitted
// cookies and persist the resolved cookie/UA fingerprint without reaching
// into Client's unexported session field.)
// ---------------------------------------------------------------------------

// TestClientFetchXSRFTokenSuccess mirrors TestFetchXSRFToken_Success
// (auth_test.go) but through the exported Client wrapper: a valid /mole/world
// response stores the scraped token on the Client.
func TestClientFetchXSRFTokenSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(loggedInWizHTML))
	}))
	defer srv.Close()

	c, err := NewClient(ClientOpts{Cookies: map[string]string{"SID": "x"}})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	c.session.moleWorldBaseURL = srv.URL

	if err := c.FetchXSRFToken(context.Background()); err != nil {
		t.Fatalf("FetchXSRFToken() error = %v", err)
	}
	if got := c.XSRFToken(); got != "the-xsrf-token-value" {
		t.Errorf("XSRFToken() = %q, want %q", got, "the-xsrf-token-value")
	}
}

// TestClientFetchXSRFTokenNotLoggedIn mirrors TestFetchXSRFToken_NotLoggedIn:
// the AccountsSignInUi signal must surface as ErrNotLoggedIn through the
// Client wrapper too, unmodified, so the connector's login flow can detect
// rejected cookies via errors.Is.
func TestClientFetchXSRFTokenNotLoggedIn(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(signInWizHTML))
	}))
	defer srv.Close()

	c, err := NewClient(ClientOpts{Cookies: map[string]string{}})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	c.session.moleWorldBaseURL = srv.URL

	err = c.FetchXSRFToken(context.Background())
	if !errors.Is(err, ErrNotLoggedIn) {
		t.Fatalf("FetchXSRFToken() error = %v, want ErrNotLoggedIn", err)
	}
	if got := c.XSRFToken(); got != "" {
		t.Errorf("XSRFToken() = %q after failed refresh, want empty", got)
	}
}

// TestClientUserAgent verifies the exported UserAgent() getter reflects the
// NORMALIZED value Session actually applies (default fallback, or a pinned
// Chrome/Firefox version on a caller-supplied UA) -- see
// normalizeUserAgent/TestUserAgentVersionRewrite. Needed so the connector can
// persist the exact fingerprint used for the validated session (doc 01 §1.1:
// "The user's real browser user-agent is stored per-user and reused").
func TestClientUserAgent(t *testing.T) {
	c, err := NewClient(ClientOpts{})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if got := c.UserAgent(); got != defaultUserAgent {
		t.Errorf("UserAgent() = %q, want default %q", got, defaultUserAgent)
	}

	c2, err := NewClient(ClientOpts{UserAgent: "Mozilla/5.0 Chrome/100.0.4321.10 Firefox/99.5"})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if got := c2.UserAgent(); !strings.Contains(got, "Chrome/"+latestChromeVersion+".0.0.0") {
		t.Errorf("UserAgent() = %q, want Chrome version rewritten to %s", got, latestChromeVersion)
	}
}

// --- xsrfRefreshLoop: noticing a Google-side logout -------------------------

// newXSRFLoopClient builds a Client whose /mole/world points at srv and whose
// refresh ticker fires immediately, so the loop can be driven synchronously.
func newXSRFLoopClient(t *testing.T, srvURL string, rec *stateRecorder) *Client {
	t.Helper()
	c, err := NewClient(ClientOpts{Cookies: map[string]string{}})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	c.session.moleWorldBaseURL = srvURL
	c.xsrfRefreshInterval = time.Millisecond
	c.OnConnectionState = rec.record
	return c
}

// TestXSRFRefreshLoopReportsAConfirmedLogout: Google answering /mole/world
// with the sign-in page means the cookies are dead -- it is the same signal
// login uses to validate them. Without this the bridge only finds out when the
// webchannel next happens to fail, which on an idle account can be a long
// time, and the user meanwhile believes they are connected.
func TestXSRFRefreshLoopReportsAConfirmedLogout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(signInWizHTML))
	}))
	defer srv.Close()

	rec := &stateRecorder{}
	c := newXSRFLoopClient(t, srv.URL, rec)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { c.xsrfRefreshLoop(ctx); close(done) }()

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("xsrfRefreshLoop never gave up on a session Google keeps rejecting")
	}

	states := rec.snapshot()
	if len(states) != 1 || states[0] != ConnStateBadCredentials {
		t.Fatalf("states = %v, want exactly one ConnStateBadCredentials", states)
	}
}

// TestXSRFRefreshLoopToleratesOneAuthFailure is the false-alarm guard, and the
// reason the threshold is two. A single spurious sign-in page must not flip
// the bridge to logged-out on a healthy session -- that would start rejecting
// the user's outbound messages for no reason.
func TestXSRFRefreshLoopToleratesOneAuthFailure(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if atomic.AddInt32(&calls, 1) == 1 {
			_, _ = w.Write([]byte(signInWizHTML)) // one bad answer...
			return
		}
		_, _ = w.Write([]byte(loggedInWizHTML)) // ...then healthy again
	}))
	defer srv.Close()

	rec := &stateRecorder{}
	c := newXSRFLoopClient(t, srv.URL, rec)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() { c.xsrfRefreshLoop(ctx); close(done) }()
	<-done

	if got := atomic.LoadInt32(&calls); got < 2 {
		t.Fatalf("the loop made %d refresh attempts, want it to have retried past the first", got)
	}
	if states := rec.snapshot(); len(states) != 0 {
		t.Errorf("states = %v, want none: one sign-in page is not a verdict", states)
	}
}

// TestXSRFRefreshLoopIgnoresOrdinaryFailures: a network blip or a 500 must not
// tear anything down, however many times it repeats. Only an authentication
// failure is a verdict.
func TestXSRFRefreshLoopIgnoresOrdinaryFailures(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	rec := &stateRecorder{}
	c := newXSRFLoopClient(t, srv.URL, rec)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() { c.xsrfRefreshLoop(ctx); close(done) }()
	<-done

	if states := rec.snapshot(); len(states) != 0 {
		t.Errorf("states = %v, want none: a server error is not a logout", states)
	}
}

// --- post-fatal retry tier --------------------------------------------------

// sleepLog records what the loop waited for without waiting.
type sleepLog struct {
	mu    sync.Mutex
	slept []time.Duration
}

func (s *sleepLog) fn(_ context.Context, d time.Duration) error {
	s.mu.Lock()
	s.slept = append(s.slept, d)
	s.mu.Unlock()
	return nil
}

func (s *sleepLog) snapshot() []time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Duration(nil), s.slept...)
}

// fatalStorm returns steps that drive the ladder to a fatal n times over,
// followed by a terminator so the loop exits rather than parking.
func fatalStorm(rounds int, terminator error) []func(*scriptedChannel, context.Context) error {
	var steps []func(*scriptedChannel, context.Context) error
	for i := 0; i < rounds; i++ {
		for j := 0; j <= maxSIDInvalidResyncs; j++ {
			steps = append(steps, ret(ErrSIDInvalid))
		}
	}
	return append(steps, ret(terminator))
}

// TestPostFatalRetryEscalates: a fatal that keeps repeating means the server is
// rejecting SIDs it has just minted, so trying again at the same rate is
// pointless. The wait has to grow, and it has to stop growing.
func TestPostFatalRetryEscalates(t *testing.T) {
	authErr := &UnexpectedStatusError{Status: http.StatusUnauthorized}
	fake := newScriptedChannel(fatalStorm(4, authErr)...)
	rec := &stateRecorder{}
	c := ladderClient(t, fake, rec)
	c.sidFatalRetryFloor = 4 * time.Second
	c.sidFatalRetryCap = 16 * time.Second
	log := &sleepLog{}
	c.sleepFn = log.fn

	c.Connect(context.Background())

	// Every sleep at or above the floor is a post-fatal wait; the ladder's own
	// pacing is pinned to 1ms by ladderClient.
	var fatalWaits []time.Duration
	for _, d := range log.snapshot() {
		if d >= c.sidFatalRetryFloor {
			fatalWaits = append(fatalWaits, d)
		}
	}
	want := []time.Duration{4 * time.Second, 8 * time.Second, 16 * time.Second, 16 * time.Second}
	if len(fatalWaits) != len(want) {
		t.Fatalf("post-fatal waits = %v, want %v", fatalWaits, want)
	}
	for i := range want {
		if fatalWaits[i] != want[i] {
			t.Errorf("post-fatal wait %d = %v, want %v (full: %v)", i, fatalWaits[i], want[i], fatalWaits)
		}
	}
}

// TestPostFatalRetryResetsAfterAHealthyChannel: an account that hits a storm
// once a month must not inherit last month's escalation. A channel that
// reached its full lifetime is the ladder's only unambiguous proof of health.
func TestPostFatalRetryResetsAfterAHealthyChannel(t *testing.T) {
	authErr := &UnexpectedStatusError{Status: http.StatusUnauthorized}
	var steps []func(*scriptedChannel, context.Context) error
	steps = append(steps, fatalStorm(2, nil)[:2*(maxSIDInvalidResyncs+1)]...)
	// A full-lifetime channel, then another storm.
	steps = append(steps, ret(ErrChannelLifetimeExpired))
	for j := 0; j <= maxSIDInvalidResyncs; j++ {
		steps = append(steps, ret(ErrSIDInvalid))
	}
	steps = append(steps, ret(authErr))

	fake := newScriptedChannel(steps...)
	rec := &stateRecorder{}
	c := ladderClient(t, fake, rec)
	c.sidFatalRetryFloor = 4 * time.Second
	c.sidFatalRetryCap = 16 * time.Second
	log := &sleepLog{}
	c.sleepFn = log.fn

	c.Connect(context.Background())

	var fatalWaits []time.Duration
	for _, d := range log.snapshot() {
		if d >= c.sidFatalRetryFloor {
			fatalWaits = append(fatalWaits, d)
		}
	}
	// 4s, 8s (escalating), then the healthy channel resets it back to 4s.
	want := []time.Duration{4 * time.Second, 8 * time.Second, 4 * time.Second}
	if len(fatalWaits) != len(want) {
		t.Fatalf("post-fatal waits = %v, want %v", fatalWaits, want)
	}
	for i := range want {
		if fatalWaits[i] != want[i] {
			t.Errorf("wait %d = %v, want %v (full: %v)", i, fatalWaits[i], want[i], fatalWaits)
		}
	}
}

// TestPostFatalRetryStopsOnDisconnect is the classic bug in this shape of
// code: a long wait that ignores cancellation. Disconnect must break out
// promptly, not after the floor -- which in production is minutes.
func TestPostFatalRetryStopsOnDisconnect(t *testing.T) {
	var steps []func(*scriptedChannel, context.Context) error
	for j := 0; j <= maxSIDInvalidResyncs; j++ {
		steps = append(steps, ret(ErrSIDInvalid))
	}
	fake := newScriptedChannel(steps...)
	rec := &stateRecorder{}
	c := ladderClient(t, fake, rec)
	// A wait far longer than the test's patience: only cancellation can end it.
	c.sidFatalRetryFloor = time.Hour
	c.sidFatalRetryCap = time.Hour

	done := make(chan error, 1)
	go func() { done <- c.Connect(context.Background()) }()

	// Wait until the fatal has been reported, i.e. the loop is in the wait.
	deadline := time.After(5 * time.Second)
	for {
		if states := rec.snapshot(); len(states) > 0 && states[len(states)-1] == ConnStateFatal {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("never reached the fatal state; got %v", rec.snapshot())
		case <-time.After(time.Millisecond):
		}
	}

	c.Disconnect()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Connect did not return after Disconnect: the post-fatal wait ignores cancellation")
	}
}

// TestPostFatalRetryDoesNotRetryADeadSession: retrying forever is right for a
// SID storm and wrong for a logged-out session. An auth error must still end
// the loop immediately.
func TestPostFatalRetryDoesNotRetryADeadSession(t *testing.T) {
	authErr := &UnexpectedStatusError{Status: http.StatusUnauthorized}
	fake := newScriptedChannel(ret(authErr), ret(ErrSIDInvalid))
	rec := &stateRecorder{}
	c := ladderClient(t, fake, rec)
	log := &sleepLog{}
	c.sleepFn = log.fn

	c.Connect(context.Background())

	if got := rec.snapshot(); !statesEqual(got, []ConnState{ConnStateBadCredentials}) {
		t.Errorf("states = %v, want [BAD_CREDENTIALS] only", got)
	}
	if got := fake.calls(); got != 1 {
		t.Errorf("Listen called %d times, want 1: a dead session must not be retried", got)
	}
}

// TestFatalRetryFloorDefaultsWhenUnset: a Client built by hand rather than by
// NewClient has a zero floor, and a zero wait would turn the post-fatal retry
// into a busy loop hammering register -- the exact storm the resync cap exists
// to prevent. The clamp is the only thing standing between the two.
func TestFatalRetryFloorDefaultsWhenUnset(t *testing.T) {
	if got := (&Client{}).fatalRetryFloor(); got != defaultSIDFatalRetryFloor {
		t.Errorf("fatalRetryFloor() = %v for an unset field, want %v", got, defaultSIDFatalRetryFloor)
	}
	if got := (&Client{sidFatalRetryFloor: time.Second}).fatalRetryFloor(); got != time.Second {
		t.Errorf("fatalRetryFloor() = %v, want the configured 1s", got)
	}
}

// TestGrowFatalBackoffCaps pins the ceiling, including for a hand-built Client
// whose cap is unset: without the fallback the doubling would run away.
func TestGrowFatalBackoffCaps(t *testing.T) {
	c := &Client{sidFatalRetryFloor: 4 * time.Second, sidFatalRetryCap: 16 * time.Second}
	got := []time.Duration{}
	d := c.fatalRetryFloor()
	for i := 0; i < 4; i++ {
		d = c.growFatalBackoff(d)
		got = append(got, d)
	}
	want := []time.Duration{8 * time.Second, 16 * time.Second, 16 * time.Second, 16 * time.Second}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("growFatalBackoff step %d = %v, want %v (%v)", i, got[i], want[i], got)
		}
	}
	if got := (&Client{}).growFatalBackoff(defaultSIDFatalRetryCap); got != defaultSIDFatalRetryCap {
		t.Errorf("growFatalBackoff with an unset cap = %v, want it clamped to %v", got, defaultSIDFatalRetryCap)
	}
}
