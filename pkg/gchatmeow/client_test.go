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
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/pblite"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// ---------------------------------------------------------------------------
// split_event_bodies (client.py:565-580)
// ---------------------------------------------------------------------------

// TestSplitEventBodies verifies the exact Python semantics: an Event carrying
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

	// Parent must have its bodies cleared (Python evt.ClearField("bodies")).
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
	// data_array = [ <StreamEventsResponse pblite array>, ... ] (client.py:550).
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
// (client.py:547-548) and never reaches OnStreamEvent.
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
// Supervision ladder (research 07 §6 / user.py:299-388)
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
			name: "four sid-invalid in a row -> fatal",
			steps: []func(*scriptedChannel, context.Context) error{
				ret(ErrSIDInvalid), ret(ErrSIDInvalid), ret(ErrSIDInvalid), ret(ErrSIDInvalid),
			},
			wantStates: []ConnState{ConnStateFatal},
			wantCalls:  4,
			wantReturn: ErrSIDInvalid,
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

// TestSIDInvalidResyncPaced verifies that consecutive SID-invalid resyncs are
// paced with a growing backoff (not fired back-to-back in milliseconds), so a
// brief transient "Unknown SID" storm backs off before the fatal cap instead of
// killing a recoverable connection. It injects sleepFn to observe the backoff
// durations deterministically without real sleeps.
func TestSIDInvalidResyncPaced(t *testing.T) {
	fake := newScriptedChannel(
		ret(ErrSIDInvalid), ret(ErrSIDInvalid), ret(ErrSIDInvalid), ret(ErrSIDInvalid),
	)
	rec := &stateRecorder{}
	c := ladderClient(t, fake, rec)
	// Spread min/max so growth is observable (ladderClient pins both to 1ms).
	c.reconnectBackoffMin = 10 * time.Millisecond
	c.reconnectBackoffMax = 100 * time.Millisecond

	var mu sync.Mutex
	var slept []time.Duration
	c.sleepFn = func(_ context.Context, d time.Duration) error {
		mu.Lock()
		slept = append(slept, d)
		mu.Unlock()
		return nil // don't actually sleep
	}

	err := c.Connect(context.Background())

	if !errors.Is(err, ErrSIDInvalid) {
		t.Errorf("Connect returned %v, want errors.Is(ErrSIDInvalid) after the cap", err)
	}
	if got := rec.snapshot(); !statesEqual(got, []ConnState{ConnStateFatal}) {
		t.Errorf("states = %v, want [FATAL]", got)
	}
	// resyncCount 1,2,3 each pace before reconnecting; the 4th exceeds the cap
	// and goes Fatal WITHOUT sleeping -> exactly maxSIDInvalidResyncs sleeps.
	mu.Lock()
	defer mu.Unlock()
	if len(slept) != maxSIDInvalidResyncs {
		t.Fatalf("paced %d times (%v), want %d", len(slept), slept, maxSIDInvalidResyncs)
	}
	for i := 1; i < len(slept); i++ {
		if slept[i] <= slept[i-1] {
			t.Errorf("backoff did not grow: slept[%d]=%v <= slept[%d]=%v (%v)",
				i, slept[i], i-1, slept[i-1], slept)
		}
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
// XSRF refresh on 401 (client.py:499-539 fetch + brief's 401-retry mandate)
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
