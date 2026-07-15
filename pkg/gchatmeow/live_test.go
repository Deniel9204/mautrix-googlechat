//go:build live

// Package gchatmeow live-protocol validation harness (M1 Task 13 spike).
//
// This is the ONLY test that talks to real Google Chat. It is gated behind the
// `live` build tag AND the presence of real cookies in the environment, so it
// never runs in CI or the normal `go test ./...` suite.
//
// # How to run
//
//	export GCHAT_LIVE_COMPASS='...'   # the 5 cookie values from a logged-in
//	export GCHAT_LIVE_SSID='...'      # chat.google.com session (DevTools ->
//	export GCHAT_LIVE_SID='...'       # Application -> Cookies -> chat.google.com)
//	export GCHAT_LIVE_OSID='...'
//	export GCHAT_LIVE_HSID='...'
//	# optional: capture sanitizable wire fixtures for the unit suites
//	export GCHAT_DEBUG_DUMP="$PWD/.spike-fixtures"
//	go test -tags 'goolm live' -run TestLive -v -timeout 5m ./pkg/gchatmeow/
//
// The cookies are SECRETS: they are read only from the environment, never
// logged, never written to the repo. `.spike-fixtures` and `.env` are
// gitignored.
//
// What it validates (the 2026 protocol-drift risks from docs/research/07 R1 and
// the open questions in 01/02/05):
//   - the frozen API key + client_version=2440378181258 still authenticate;
//   - GET /mole/world still yields an XSRF token (not a logged-out page);
//   - alt=proto responses decode (and whether any arrive base64 — api.go's
//     dual-decode heuristic);
//   - PaginatedWorld returns the account's chats (world sync);
//   - the BrowserChannel choreography (register -> SID -> ack GET -> initial
//     ping) actually delivers events, and the $req double-encoding is accepted.
package gchatmeow

import (
	"context"
	"os"
	"testing"
	"time"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

func liveCookies(t *testing.T) map[string]string {
	t.Helper()
	env := map[string]string{
		"COMPASS": "GCHAT_LIVE_COMPASS",
		"SSID":    "GCHAT_LIVE_SSID",
		"SID":     "GCHAT_LIVE_SID",
		"OSID":    "GCHAT_LIVE_OSID",
		"HSID":    "GCHAT_LIVE_HSID",
	}
	cookies := make(map[string]string, len(env))
	var missing []string
	for name, key := range env {
		v := os.Getenv(key)
		if v == "" {
			missing = append(missing, key)
		}
		cookies[name] = v
	}
	if len(missing) > 0 {
		t.Skipf("live spike skipped: set %v (5 chat.google.com cookies) to run", missing)
	}
	return cookies
}

// TestLiveProtocol exercises the real Google Chat protocol end to end and
// prints a PASS/FAIL line per drift concern. It does not assert hard on the
// event-delivery step (that depends on live traffic) but reports what it saw.
func TestLiveProtocol(t *testing.T) {
	cookies := liveCookies(t)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	client, err := NewClient(ClientOpts{Cookies: cookies, UserAgent: os.Getenv("GCHAT_LIVE_UA")})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	// 1. XSRF bootstrap + logged-in detection (frozen API key/client_version,
	//    /mole/world scrape).
	if err := client.FetchXSRFToken(ctx); err != nil {
		t.Fatalf("FAIL FetchXSRFToken (cookies invalid / logged out / drift): %v", err)
	}
	t.Log("PASS  FetchXSRFToken: /mole/world scrape + XSRF token OK (session valid)")

	// 2. GetSelfUserStatus -> confirms the binary-proto RPC round trip + gaia id.
	self, err := client.GetSelfUserStatus(ctx, &pb.GetSelfUserStatusRequest{})
	if err != nil {
		t.Fatalf("FAIL GetSelfUserStatus (RPC wire format / base64 decode / drift): %v", err)
	}
	gaia := self.GetUserStatus().GetUserId().GetId()
	t.Logf("PASS  GetSelfUserStatus: own gaia id = %q (RPC + proto decode OK)", gaia)

	// 3. PaginatedWorld -> world sync.
	world, err := client.PaginatedWorld(ctx, &pb.PaginatedWorldRequest{
		FetchFromUserSpaces: boolPtr(true),
		FetchOptions:        []pb.PaginatedWorldRequest_FetchOptions{pb.PaginatedWorldRequest_EXCLUDE_GROUP_LITE},
	})
	if err != nil {
		t.Fatalf("FAIL PaginatedWorld (world sync / drift): %v", err)
	}
	t.Logf("PASS  PaginatedWorld: %d world items returned (chat-list sync OK)", len(world.GetWorldItems()))

	// 4. BrowserChannel: register -> SID -> ack -> ping, then observe whether
	//    any events flow. We connect for a bounded window and count.
	var events int
	firstConnected := make(chan struct{}, 1)
	client.OnConnectionState = func(state ConnState, err error) {
		if state == ConnStateConnected {
			select {
			case firstConnected <- struct{}{}:
			default:
			}
		}
		t.Logf("      channel state: %s (err=%v)", state, err)
	}
	client.OnStreamEvent = func(ctx context.Context, ev *pb.Event) {
		events++
		t.Logf("      stream event #%d: type=%s group=%v", events, ev.GetType(), ev.GetGroupId())
	}

	connCtx, connCancel := context.WithTimeout(ctx, 90*time.Second)
	defer connCancel()
	connErr := make(chan error, 1)
	go func() { connErr <- client.Connect(connCtx) }()

	select {
	case <-firstConnected:
		t.Log("PASS  BrowserChannel: register->SID->ack->ping choreography delivered a live connection")
	case err := <-connErr:
		t.Fatalf("FAIL BrowserChannel: Connect returned before connecting: %v", err)
	case <-time.After(60 * time.Second):
		t.Fatal("FAIL BrowserChannel: no Connected state within 60s (choreography/$req/ack drift?)")
	}

	// Let it run a bit to observe events (send yourself a message on the test
	// account from another device during this window to exercise receive).
	t.Log("      channel live — send a message to this account from another device now to test receive (30s window)...")
	select {
	case <-time.After(30 * time.Second):
	case <-connCtx.Done():
	}
	connCancel()
	<-connErr
	t.Logf("DONE  observed %d stream event(s) during the live window", events)
	t.Log("      (if GCHAT_DEBUG_DUMP was set, sanitize the captured frames before committing as fixtures)")
}

func boolPtr(b bool) *bool { return &b }

// TestLiveSendReceive exercises the M2 send path (create_topic) and the inbound
// MESSAGE_POSTED→text conversion against real Google Chat: it finds a
// conversation the test account can post to, sends a uniquely-marked message,
// and confirms the message comes back on the channel as a MESSAGE_POSTED echo
// carrying the same text and local_id (proving send + inbound decode + the
// local_id echo round-trip that no unit test can verify).
//
// If the test account has no existing conversation, it skips the send with a
// clear message — create one first (e.g. have another user DM the account).
func TestLiveSendReceive(t *testing.T) {
	cookies := liveCookies(t)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	client, err := NewClient(ClientOpts{Cookies: cookies, UserAgent: os.Getenv("GCHAT_LIVE_UA")})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if err := client.FetchXSRFToken(ctx); err != nil {
		t.Fatalf("FetchXSRFToken (cookies invalid / drift): %v", err)
	}

	// Find a group/DM to post into.
	world, err := client.PaginatedWorld(ctx, &pb.PaginatedWorldRequest{
		FetchFromUserSpaces: boolPtr(true),
		FetchOptions:        []pb.PaginatedWorldRequest_FetchOptions{pb.PaginatedWorldRequest_EXCLUDE_GROUP_LITE},
	})
	if err != nil {
		t.Fatalf("PaginatedWorld: %v", err)
	}
	var target *pb.GroupId
	for _, item := range world.GetWorldItems() {
		if item.GetGroupId() != nil {
			target = item.GetGroupId()
			break
		}
	}
	if target == nil {
		t.Skip("live send skipped: the test account has no conversation to post into — " +
			"have another user DM the account first, then re-run")
	}

	// Unique marker so we can recognize our own echo unambiguously.
	marker := "m2-live-" + os.Getenv("GCHAT_LIVE_MARKER")
	if marker == "m2-live-" {
		marker = "m2-live-probe" // caller can override via GCHAT_LIVE_MARKER for a fresh value
	}
	localID := "mautrix-googlechat%42424242"

	// Watch the channel for our echo.
	echoSeen := make(chan *pb.Message, 4)
	client.OnStreamEvent = func(ctx context.Context, ev *pb.Event) {
		msg := ev.GetBody().GetMessagePosted().GetMessage()
		if msg == nil {
			return
		}
		t.Logf("      inbound MESSAGE_POSTED: text=%q local_id=%q", msg.GetTextBody(), msg.GetLocalId())
		if msg.GetTextBody() == marker || msg.GetLocalId() == localID {
			select {
			case echoSeen <- msg:
			default:
			}
		}
	}
	connCtx, connCancel := context.WithTimeout(ctx, 90*time.Second)
	defer connCancel()
	connErr := make(chan error, 1)
	go func() { connErr <- client.Connect(connCtx) }()
	// Give the channel a moment to register before sending.
	select {
	case <-time.After(8 * time.Second):
	case err := <-connErr:
		t.Fatalf("Connect returned early: %v", err)
	}

	// Send via create_topic (the M2 send path; RequestHeader is stamped inside).
	resp, err := client.CreateTopic(ctx, &pb.CreateTopicRequest{
		GroupId:   target,
		LocalId:   &localID,
		TextBody:  &marker,
		HistoryV2: boolPtr(true),
	})
	if err != nil {
		connCancel()
		<-connErr
		t.Fatalf("FAIL CreateTopic (send path / drift / issue #110): %v", err)
	}
	t.Logf("PASS  CreateTopic: sent, server topic id = %q", resp.GetTopic().GetId().GetTopicId())

	select {
	case msg := <-echoSeen:
		t.Logf("PASS  echo round-trip: our message came back (text=%q local_id=%q) — send + inbound decode + local_id echo all validated",
			msg.GetTextBody(), msg.GetLocalId())
	case <-time.After(45 * time.Second):
		t.Error("FAIL: sent message did not echo back within 45s (inbound decode? local_id echo? channel?)")
	}
	connCancel()
	<-connErr
}
