//go:build live

package gchatmeow

import (
	"context"
	"os"
	"testing"
	"time"
)

// TestLiveChannelProbe drives the BrowserChannel internals step-by-step to
// locate exactly where the connection stalls: register (csessionid), the first
// long-poll (SID from X-HTTP-Initial-Response), and whether any data arrays
// arrive. In-package so it can call the unexported register/longpollRequest.
func TestLiveChannelProbe(t *testing.T) {
	cookies := liveCookies(t)
	sess, err := NewSession(cookies, os.Getenv("GCHAT_LIVE_UA"))
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	ch := NewChannel(sess)

	var arrays int
	ch.OnReceiveArray = func(ctx context.Context, arr []byte) error {
		arrays++
		n := len(arr)
		if n > 80 {
			n = 80
		}
		t.Logf("      data array #%d (%d bytes): %s...", arrays, len(arr), string(arr[:n]))
		return nil
	}
	ch.OnConnect = func(ctx context.Context) { t.Log("      >>> OnConnect fired (first chunk received)") }

	// Step 1: register.
	regCtx, regCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer regCancel()
	t0 := time.Now()
	csid, err := ch.register(regCtx)
	if err != nil {
		t.Fatalf("STALL AT REGISTER after %s: %v", time.Since(t0), err)
	}
	t.Logf("PASS  register OK in %s (csessionid %q)", time.Since(t0), truncateBody([]byte(csid)))

	// Step 2: first long-poll (SID=="" → sends SID=null, reads SID from header).
	pollCtx, pollCancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer pollCancel()
	t1 := time.Now()
	pollErr := make(chan error, 1)
	go func() { pollErr <- ch.longpollRequest(pollCtx) }()

	select {
	case err := <-pollErr:
		ch.mu.Lock()
		gotSID := ch.sid
		ch.mu.Unlock()
		t.Logf("      first long-poll returned after %s: err=%v, SID-obtained=%q, arrays=%d",
			time.Since(t1), err, truncateBody([]byte(gotSID)), arrays)
		if gotSID == "" && arrays == 0 {
			t.Error("FAIL: first long-poll produced NO SID and NO data — server accepted register but starved the poll (session/webchannel issue)")
		} else {
			t.Logf("PASS  channel produced SID and/or data — the poll works; the 60s test timeout was likely just slow initial push")
		}
	case <-time.After(50 * time.Second):
		ch.mu.Lock()
		gotSID := ch.sid
		ch.mu.Unlock()
		t.Errorf("STALL IN LONG-POLL: 45s+ with no return; SID-obtained=%q, arrays=%d (server holding the connection open, sending nothing)",
			truncateBody([]byte(gotSID)), arrays)
	}
}
