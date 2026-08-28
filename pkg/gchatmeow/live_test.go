//go:build live

// Package gchatmeow live-protocol validation harness.
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
// What it validates (the 2026 protocol-drift risks):
//   - the frozen API key + client_version=2440378181258 still authenticate;
//   - GET /mole/world still yields an XSRF token (not a logged-out page);
//   - alt=proto responses decode (and whether any arrive base64 — api.go's
//     dual-decode heuristic);
//   - PaginatedWorld returns the account's chats (world sync);
//   - the BrowserChannel choreography (register -> SID -> ack GET -> initial
//     ping) actually delivers events, and the $req double-encoding is accepted.
//
// Opt-in probes for the outbound-membership/rename feature (issue #11), each
// gated on its own env var and skipped otherwise:
//   - TestLiveUpload: outbound media upload (GCHAT_LIVE_GROUP_ID).
//   - TestLiveRoomName: reversible space rename (GCHAT_LIVE_SPACE_ID).
//   - TestLiveMembershipRoundTrip: net-zero invite+remove, i.e. the
//     create_membership + remove_memberships RPCs (GCHAT_LIVE_SPACE_ID +
//     GCHAT_LIVE_INVITE_GAIA). DESTRUCTIVE -- use a throwaway target account.
package gchatmeow

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// errDetail surfaces the server's response body (first 512 bytes) from an
// *UnexpectedStatusError, which the error's own Error() omits -- for an
// opaque 500/4xx this is where Google's actual reason (e.g. invalid user id,
// permission) appears.
func errDetail(err error) string {
	var ue *UnexpectedStatusError
	if errors.As(err, &ue) && ue.Body != "" {
		return fmt.Sprintf(" [status %d body: %q]", ue.Status, ue.Body)
	}
	return ""
}

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
		FetchFromUserSpaces:  boolPtr(true),
		FetchOptions:         []pb.PaginatedWorldRequest_FetchOptions{pb.PaginatedWorldRequest_EXCLUDE_GROUP_LITE},
		WorldSectionRequests: []*pb.WorldSectionRequest{{PageSize: proto.Int32(999)}}, // required: server returns empty world without a section
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

// TestLiveSendReceive exercises the send path (create_topic) and the inbound
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
		FetchFromUserSpaces:  boolPtr(true),
		FetchOptions:         []pb.PaginatedWorldRequest_FetchOptions{pb.PaginatedWorldRequest_EXCLUDE_GROUP_LITE},
		WorldSectionRequests: []*pb.WorldSectionRequest{{PageSize: proto.Int32(999)}}, // required: server returns empty world without a section
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

	// Send via create_topic (the send path; RequestHeader is stamped inside).
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

// TestLiveUpload attempts exactly one real media upload against Google's
// /uploads endpoint -- the issue #114 risk path. It answers, definitively for
// THIS bridge's wire shape (which deliberately avoids polluting the signed
// upload URL with alt=/key= query params and sends the XSRF header on both
// hops), whether outbound media upload works today. A 500 here == #114 also
// affects our shape; a pass == #114 is caused by the signed-URL alt=/key=
// pollution this shape avoids.
//
// The account whose cookies you export MUST have at least one Google Chat
// conversation (a DM or space) — the upload is scoped to a group_id. A
// throwaway test account with no chats will skip. -count=1 is REQUIRED: Go
// caches test results on inputs it can see, but Google's server state isn't
// one of them, so without it a stale skip/pass is replayed.
//
//	go test -tags 'goolm live' -run TestLiveUpload -v -count=1 -timeout 5m ./pkg/gchatmeow/
func TestLiveUpload(t *testing.T) {
	cookies := liveCookies(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client, err := NewClient(ClientOpts{Cookies: cookies, UserAgent: os.Getenv("GCHAT_LIVE_UA")})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if err := client.FetchXSRFToken(ctx); err != nil {
		t.Fatalf("FetchXSRFToken (cookies invalid / logged out): %v", err)
	}

	// Fast path: upload into a known conversation. UploadFile only needs valid
	// cookies + XSRF and a group id -- it does NOT depend on the realtime
	// channel or on world sync, so a known-good group id skips the flaky
	// discovery below entirely. Pass the PLAIN numeric id (no dm:/space:
	// prefix), e.g. from the migrated portal ids (dm:hBEIAUAAAAE -> hBEIAUAAAAE)
	// or any group id printed in the channel logs.
	if gid := os.Getenv("GCHAT_LIVE_GROUP_ID"); gid != "" {
		runUploadProbe(t, ctx, client, gid)
		return
	}

	// Discovery path: PaginatedWorld only returns the account's chats for a
	// session with a live realtime channel, so mirror production -- connect,
	// wait for CONNECTED, then sync. (Note: this has been observed returning 0
	// items even post-connect for accounts that demonstrably have chats; if it
	// skips, use GCHAT_LIVE_GROUP_ID above instead.)
	connCtx, connCancel := context.WithCancel(ctx)
	defer connCancel()
	connected := make(chan struct{}, 1)
	client.OnConnectionState = func(state ConnState, err error) {
		if state == ConnStateConnected {
			select {
			case connected <- struct{}{}:
			default:
			}
		}
	}
	connErr := make(chan error, 1)
	go func() { connErr <- client.Connect(connCtx) }()
	select {
	case <-connected:
	case err := <-connErr:
		t.Fatalf("Connect returned before connecting: %v", err)
	case <-time.After(60 * time.Second):
		t.Fatal("no CONNECTED state within 60s (channel choreography drift?)")
	}
	// connErr is buffered(1), so the Connect goroutine can send and exit even
	// with no reader once the deferred connCancel() unblocks it -- no leak.

	world, err := client.PaginatedWorld(ctx, &pb.PaginatedWorldRequest{
		FetchFromUserSpaces:  boolPtr(true),
		FetchOptions:         []pb.PaginatedWorldRequest_FetchOptions{pb.PaginatedWorldRequest_EXCLUDE_GROUP_LITE},
		WorldSectionRequests: []*pb.WorldSectionRequest{{PageSize: proto.Int32(999)}}, // required: server returns empty world without a section
	})
	if err != nil {
		t.Fatalf("PaginatedWorld: %v", err)
	}
	items := world.GetWorldItems()
	var withGroup int
	var groupID string
	for _, item := range items {
		gid := item.GetGroupId()
		if gid == nil {
			continue
		}
		withGroup++
		if dm := gid.GetDmId().GetDmId(); dm != "" {
			groupID = dm
		} else if sp := gid.GetSpaceId().GetSpaceId(); sp != "" {
			groupID = sp
		}
		if groupID != "" {
			break
		}
	}
	if groupID == "" {
		// Distinguish "account genuinely has no chats" (0 items / 0 with a
		// group id) from a discovery bug (items exist but none yielded a
		// plain id) so a skip is diagnosable without guessing.
		t.Skipf("live upload skipped: no conversation to upload into "+
			"(world items=%d, with group id=%d) -- set GCHAT_LIVE_GROUP_ID to a "+
			"known plain group id (e.g. a migrated portal id without its dm:/"+
			"space: prefix) and re-run with -count=1", len(items), withGroup)
	}

	runUploadProbe(t, ctx, client, groupID)
}

// runUploadProbe uploads a 1x1 PNG to groupID via the #114 risk path and
// fails on any error (a 500 == #114 affects our wire shape too).
func runUploadProbe(t *testing.T, ctx context.Context, client *Client, groupID string) {
	t.Helper()
	// A 1x1 transparent PNG -- the smallest valid image, well under #114's
	// reported <500KB threshold.
	png, err := base64.StdEncoding.DecodeString(
		"iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==")
	if err != nil {
		t.Fatalf("decode probe png: %v", err)
	}

	meta, err := client.UploadFile(ctx, groupID, png, "issue114-probe.png", "image/png")
	if err != nil {
		t.Fatalf("FAIL UploadFile (group_id=%s) -- #114 DOES affect our wire shape (a 500 here confirms it): %v", groupID, err)
	}
	t.Logf("PASS UploadFile (group_id=%s) -- #114 does NOT affect our shape: attachment_token=%q content_type=%q",
		groupID, meta.GetAttachmentToken(), meta.GetContentType())
}

// TestLiveDiagnosePaginatedWorld gathers evidence for the "PaginatedWorld
// returns 0 world items even though the account has chats" bug WITHOUT any
// hypothesis baked in: it issues the exact paginated_world request
// doRequestOnce would, captures the RAW response body, and reports enough to
// tell apart three root causes at a glance:
//
//   - base64 mis-parse: the server base64-encoded the body (because of the
//     X-Goog-Encode-Response-If-Executable header), and unmarshalAPIResponse's
//     "try raw binary first" step parses that ASCII as a valid-but-EMPTY proto
//     with no error (the residual risk documented at api.go:238). Signature:
//     body is printable base64, raw-unmarshal err=nil items=0, but
//     base64-then-unmarshal yields items>0.
//   - genuinely empty: body is short binary, both paths give items=0. The
//     account really has no world items from this endpoint.
//   - proto drift: body is sizable binary, raw-unmarshal err=nil items=0, and
//     it is NOT valid base64. World data is on the wire under field numbers our
//     schema doesn't map -> inspect proto.Message unknown fields next.
//
// Run: go test -tags 'goolm live' -run TestLiveDiagnosePaginatedWorld -v -count=1 ./pkg/gchatmeow/
func TestLiveDiagnosePaginatedWorld(t *testing.T) {
	cookies := liveCookies(t)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	client, err := NewClient(ClientOpts{Cookies: cookies, UserAgent: os.Getenv("GCHAT_LIVE_UA")})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if err := client.FetchXSRFToken(ctx); err != nil {
		t.Fatalf("FetchXSRFToken (cookies invalid / logged out): %v", err)
	}

	// Controlled A/B/C: same session, three request shapes, one variable at a
	// time. Evidence so far: our shape returns a 2-byte body (no world_items).
	// The only difference from the maintained purple-googlechat client is that
	// purple always sends a world_section_requests entry with a page_size.
	//   A = our current shape (baseline; expected 0 items)
	//   B = A + one world_section_requests{page_size:999} (isolates the section)
	//   C = purple's exact shape (fetch_snippets_for_unnamed_rooms + section,
	//       no fetch_options) -- the known-working reference
	variantA := &pb.PaginatedWorldRequest{
		RequestHeader:       newRequestHeader(),
		FetchFromUserSpaces: boolPtr(true),
		FetchOptions:        []pb.PaginatedWorldRequest_FetchOptions{pb.PaginatedWorldRequest_EXCLUDE_GROUP_LITE},
	}
	variantB := &pb.PaginatedWorldRequest{
		RequestHeader:        newRequestHeader(),
		FetchFromUserSpaces:  boolPtr(true),
		FetchOptions:         []pb.PaginatedWorldRequest_FetchOptions{pb.PaginatedWorldRequest_EXCLUDE_GROUP_LITE},
		WorldSectionRequests: []*pb.WorldSectionRequest{{PageSize: proto.Int32(999)}},
	}
	variantC := &pb.PaginatedWorldRequest{
		RequestHeader:                newRequestHeader(),
		FetchFromUserSpaces:          boolPtr(true),
		FetchSnippetsForUnnamedRooms: boolPtr(true),
		WorldSectionRequests:         []*pb.WorldSectionRequest{{PageSize: proto.Int32(999)}},
	}

	probeWorld(t, ctx, client, "A (current: fetch_options, no section)", variantA)
	probeWorld(t, ctx, client, "B (current + section page_size=999)", variantB)
	probeWorld(t, ctx, client, "C (purple shape: snippets + section)", variantC)

	t.Log("VERDICT GUIDE: if A=0 and B>0 -> fix is 'add a world_section_requests entry'; " +
		"if A=0,B=0,C>0 -> fetch_options must be replaced by purple's snippet+section shape; " +
		"if all 0 -> not a request-shape issue, investigate header/session.")
}

// probeWorld issues one paginated_world request (raw, so we see the real body)
// and reports the world_items count plus body diagnostics.
func probeWorld(t *testing.T, ctx context.Context, client *Client, label string, req *pb.PaginatedWorldRequest) {
	t.Helper()
	reqBody, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("[%s] marshal request: %v", label, err)
	}

	counter := client.requestCounter.Add(1)
	params := url.Values{}
	params.Set("c", strconv.FormatInt(counter, 10))
	params.Set("rt", "b")
	params.Set("alt", "proto")
	params.Set("key", apiKey)
	base := client.baseURL
	if base == "" {
		base = apiBaseURL
	}
	reqURL := fmt.Sprintf("%s/api/paginated_world?%s", base, params.Encode())

	headers := http.Header{}
	headers.Set("Content-Type", "application/x-protobuf")
	headers.Set("X-Goog-Encode-Response-If-Executable", "base64")
	if tok := client.XSRFToken(); tok != "" {
		headers.Set("x-framework-xsrf-token", tok)
	}

	res, err := client.session.Fetch(ctx, http.MethodPost, reqURL, headers, reqBody)
	if err != nil {
		t.Fatalf("[%s] Fetch paginated_world: %v", label, err)
	}
	body := res.Body

	var resp pb.PaginatedWorldResponse
	rawErr := proto.Unmarshal(body, &resp)
	if rawErr != nil {
		// Fall back to base64 (matches unmarshalAPIResponse) just in case.
		if decoded, b64Err := base64.StdEncoding.DecodeString(string(bytes.TrimSpace(body))); b64Err == nil {
			proto.Reset(&resp)
			rawErr = proto.Unmarshal(decoded, &resp)
		}
	}
	prefixN := 24
	if len(body) < prefixN {
		prefixN = len(body)
	}
	t.Logf("VARIANT %s: status=%d body_len=%d hex[:%d]=%s unmarshal_err=%v WORLD_ITEMS=%d SECTIONS=%d",
		label, res.StatusCode, len(body), prefixN, hex.EncodeToString(body[:prefixN]),
		rawErr, len(resp.GetWorldItems()), len(resp.GetWorldSectionResponses()))
}

// TestLiveRoomName verifies the update_group rename RPC end to end and
// REVERSIBLY: it reads the space's current name, renames it to a probe value,
// asserts the RPC succeeds, then restores the original name -- net-zero.
//
// Requires GCHAT_LIVE_SPACE_ID = a plain space id (no space: prefix) the
// account can rename; renaming may require a space-manager role, in which case
// a plain member gets a clean error here (which is itself a useful result).
// Skips if unset. Optionally set GCHAT_LIVE_SPACE_ORIG_NAME to the space's
// current name to skip the get_group read-back (and thus verify update_group
// even if get_group is unavailable for the account).
//
//	export GCHAT_LIVE_SPACE_ID='AAAAxxxxxxx'
//	# optional: export GCHAT_LIVE_SPACE_ORIG_NAME='My Space'
//	go test -tags 'goolm live' -run TestLiveRoomName -v -count=1 ./pkg/gchatmeow/
func TestLiveRoomName(t *testing.T) {
	cookies := liveCookies(t)
	spaceID := os.Getenv("GCHAT_LIVE_SPACE_ID")
	if spaceID == "" {
		t.Skip("set GCHAT_LIVE_SPACE_ID to a space id the account can rename")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client, err := NewClient(ClientOpts{Cookies: cookies, UserAgent: os.Getenv("GCHAT_LIVE_UA")})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if err := client.FetchXSRFToken(ctx); err != nil {
		t.Fatalf("FetchXSRFToken (cookies invalid / logged out): %v", err)
	}

	// The original name is needed so the rename can be restored (net-zero).
	// Prefer an explicitly provided name (GCHAT_LIVE_SPACE_ORIG_NAME) so the
	// rename RPC can be verified even if get_group is unavailable; otherwise
	// read it via get_group, mirroring production's exact request shape
	// (chatinfo.go: MEMBERS + INCLUDE_DYNAMIC_GROUP_NAME + IncludeInviteDms).
	orig := os.Getenv("GCHAT_LIVE_SPACE_ORIG_NAME")
	if orig == "" {
		gg, err := client.GetGroup(ctx, &pb.GetGroupRequest{
			GroupId: PartsToGroupID(spaceID, false),
			FetchOptions: []pb.GetGroupRequest_FetchOptions{
				pb.GetGroupRequest_MEMBERS,
				pb.GetGroupRequest_INCLUDE_DYNAMIC_GROUP_NAME,
			},
			IncludeInviteDms: proto.Bool(true),
		})
		if err != nil {
			t.Fatalf("get_group (reading current name) failed: %v\n"+
				"  - check GCHAT_LIVE_SPACE_ID is a PLAIN space id (no \"space:\" prefix) for a space this account is a member of;\n"+
				"  - or set GCHAT_LIVE_SPACE_ORIG_NAME=<current name> to skip the read-back and test update_group directly.", err)
		}
		orig = gg.GetGroup().GetName()
	}
	t.Logf("original space name = %q", orig)

	rename := func(name string) error {
		_, err := client.UpdateGroup(ctx, &pb.UpdateGroupRequest{
			SpaceId:     SpaceID(spaceID),
			Name:        proto.String(name),
			UpdateMasks: []pb.UpdateGroupRequest_UpdateMask{pb.UpdateGroupRequest_NAME},
		})
		return err
	}

	probe := orig + " [rename-probe]"
	if err := rename(probe); err != nil {
		t.Fatalf("FAIL update_group rename -- #11 update_group endpoint/shape or a permission error: %v%s", err, errDetail(err))
	}
	t.Logf("PASS update_group renamed space to %q", probe)

	// Restore -- best-effort; if it fails, tell the operator to fix it manually.
	if err := rename(orig); err != nil {
		t.Errorf("could not restore original name %q (space is currently %q, rename it back manually): %v", orig, probe, err)
	} else {
		t.Logf("restored original name %q -- net-zero", orig)
	}
}

// TestLiveMembershipRoundTrip verifies create_membership + remove_memberships
// against a live space, NET-ZERO: it invites GCHAT_LIVE_INVITE_GAIA into the
// space, then removes them. The remove path is the SAME remove_memberships RPC
// a self-leave uses, so this covers invite, kick, and leave in one run.
//
// DESTRUCTIVE / opt-in: the target account receives a real invite and is then
// removed -- use a throwaway or second account you control, never a stranger.
// Requires GCHAT_LIVE_SPACE_ID + GCHAT_LIVE_INVITE_GAIA (the target's email or numeric gaia id). Skips if either is unset.
//
//	export GCHAT_LIVE_SPACE_ID='AAAAxxxxxxx' GCHAT_LIVE_INVITE_GAIA='123456789012345'
//	go test -tags 'goolm live' -run TestLiveMembershipRoundTrip -v -count=1 ./pkg/gchatmeow/
func TestLiveMembershipRoundTrip(t *testing.T) {
	cookies := liveCookies(t)
	spaceID := os.Getenv("GCHAT_LIVE_SPACE_ID")
	target := os.Getenv("GCHAT_LIVE_INVITE_GAIA")
	if spaceID == "" || target == "" {
		t.Skip("set GCHAT_LIVE_SPACE_ID and GCHAT_LIVE_INVITE_GAIA (a throwaway target's gaia id OR email) to run")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client, err := NewClient(ClientOpts{Cookies: cookies, UserAgent: os.Getenv("GCHAT_LIVE_UA")})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if err := client.FetchXSRFToken(ctx); err != nil {
		t.Fatalf("FetchXSRFToken (cookies invalid / logged out): %v", err)
	}
	groupID := PartsToGroupID(spaceID, false)

	// Build the invitee: an email goes in invitee_info.email, a numeric gaia in
	// invitee_info.user_id. The connector always has a ghost's gaia (user_id);
	// email is a live-test convenience, since a human has an email handier than
	// a raw gaia id.
	var invitee *pb.InviteeInfo
	if strings.Contains(target, "@") {
		invitee = &pb.InviteeInfo{Email: proto.String(target)}
		t.Logf("inviting by email %q", target)
	} else {
		invitee = &pb.InviteeInfo{UserId: &pb.UserId{Id: proto.String(target)}}
		t.Logf("inviting by gaia %q", target)
	}

	// Snapshot current members so an EMAIL invite (whose gaia we don't know up
	// front) can still be cleaned up net-zero via a diff. Not needed for a
	// gaia invite (we already know the id), but cheap and useful.
	before, err := spaceMemberGaias(ctx, client, spaceID)
	if err != nil {
		t.Fatalf("get_group (pre-invite snapshot): %v%s", err, errDetail(err))
	}

	// Invite. Non-fatal on error: create_membership success is proven when it
	// returns nil (logged PASS), but a re-run against a target already invited
	// from a prior run may error here -- in which case we still proceed to the
	// remove step so the stuck invite gets cleaned up and remove_memberships is
	// exercised.
	if _, err := client.CreateMembership(ctx, &pb.CreateMembershipRequest{
		GroupId: groupID,
		InviteeMemberInfos: []*pb.InviteeMemberInfo{
			{Id: &pb.InviteeMemberInfo_InviteeInfo{InviteeInfo: invitee}},
		},
	}); err != nil {
		t.Logf("create_membership returned an error (may be already-invited from a prior run): %v%s", err, errDetail(err))
	} else {
		t.Logf("PASS create_membership invited %q", target)
	}

	// Decide whom to remove. For a gaia target we know exactly who it is (no
	// need to trust a get_group diff, which can miss a pending invite); for an
	// email target, diff the membership snapshot to find the added gaia.
	var toRemove []string
	if !strings.Contains(target, "@") {
		toRemove = []string{target}
	} else {
		after, err := spaceMemberGaias(ctx, client, spaceID)
		if err != nil {
			t.Errorf("get_group (post-invite snapshot) failed -- %q may still be invited, remove manually: %v%s", target, err, errDetail(err))
			return
		}
		for g := range after {
			if !before[g] {
				toRemove = append(toRemove, g)
			}
		}
	}
	if len(toRemove) == 0 {
		t.Logf("NOTE create_membership succeeded, but no gaia to remove was resolved "+
			"(email invite still pending). If %q is now invited, remove them manually; "+
			"remove_memberships was not exercised this run.", target)
		return
	}
	for _, g := range toRemove {
		if _, err := client.RemoveMemberships(ctx, &pb.RemoveMembershipsRequest{
			GroupId:         groupID,
			MemberIds:       []*pb.MemberId{UserMemberID(g)},
			MembershipState: pb.MembershipState_MEMBER_INVITED.Enum(),
		}); err != nil {
			t.Errorf("remove_memberships cleanup for %s failed -- remove manually: %v%s", g, err, errDetail(err))
		} else {
			t.Logf("PASS remove_memberships removed %s -- net-zero (also verifies the kick/leave RPC path)", g)
		}
	}
}

// spaceMemberGaias returns the set of gaia ids currently in the space (joined
// + invited members), read via get_group with production's request shape.
func spaceMemberGaias(ctx context.Context, client *Client, spaceID string) (map[string]bool, error) {
	resp, err := client.GetGroup(ctx, &pb.GetGroupRequest{
		GroupId: PartsToGroupID(spaceID, false),
		FetchOptions: []pb.GetGroupRequest_FetchOptions{
			pb.GetGroupRequest_MEMBERS,
			pb.GetGroupRequest_INCLUDE_DYNAMIC_GROUP_NAME,
		},
		IncludeInviteDms: proto.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	set := map[string]bool{}
	for _, m := range resp.GetMemberships() {
		if id := m.GetId().GetMemberId().GetUserId().GetId(); id != "" {
			set[id] = true
		}
	}
	for _, m := range resp.GetJoinedMemberIds() {
		if id := m.GetUserId().GetId(); id != "" {
			set[id] = true
		}
	}
	for _, m := range resp.GetInvitedMemberIds() {
		if id := m.GetUserId().GetId(); id != "" {
			set[id] = true
		}
	}
	return set, nil
}

// TestLiveDumpLinkAnnotations is a diagnostic for the "inbound links are not
// links in Matrix" bug: it fetches recent messages from a conversation and
// dumps, for every message containing a URL, the raw annotation data --
// crucially whether chip_render_type is SET at all and what value it holds.
//
// gchatfmt only renders a url_metadata annotation as <a href> when
// chip_render_type == DO_NOT_RENDER; an unset field decodes as UNKNOWN(0) and
// is skipped, leaving the URL as plain text. This probe says which case real
// Google Chat traffic actually hits.
//
//	export GCHAT_LIVE_GROUP_ID='<a dm or space id with a link in it>'
//	go test -tags 'goolm live' -run TestLiveDumpLinkAnnotations -v -count=1 ./pkg/gchatmeow/
func TestLiveDumpLinkAnnotations(t *testing.T) {
	cookies := liveCookies(t)
	groupID := os.Getenv("GCHAT_LIVE_GROUP_ID")
	if groupID == "" {
		t.Skip("set GCHAT_LIVE_GROUP_ID to a conversation containing a link")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client, err := NewClient(ClientOpts{Cookies: cookies, UserAgent: os.Getenv("GCHAT_LIVE_UA")})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if err := client.FetchXSRFToken(ctx); err != nil {
		t.Fatalf("FetchXSRFToken (cookies invalid / logged out): %v", err)
	}

	// The id may be a DM or a space; try DM first, fall back to space.
	var resp *pb.ListTopicsResponse
	for _, isDM := range []bool{true, false} {
		resp, err = client.ListTopics(ctx, &pb.ListTopicsRequest{
			GroupId:           PartsToGroupID(groupID, isDM),
			PageSizeForTopics: proto.Int32(40),
		})
		if err == nil {
			t.Logf("list_topics OK (isDM=%v): %d topics", isDM, len(resp.GetTopics()))
			break
		}
		t.Logf("list_topics with isDM=%v failed: %v%s", isDM, err, errDetail(err))
	}
	if err != nil {
		t.Fatalf("list_topics failed for both DM and space forms: %v%s", err, errDetail(err))
	}

	var withURL int
	for _, topic := range resp.GetTopics() {
		for _, msg := range topic.GetReplies() {
			text := msg.GetTextBody()
			anns := msg.GetAnnotations()
			hasHTTP := strings.Contains(strings.ToLower(text), "http")
			var hasURLAnn bool
			for _, a := range anns {
				if a.GetUrlMetadata() != nil {
					hasURLAnn = true
				}
			}
			if !hasHTTP && !hasURLAnn {
				continue
			}
			withURL++
			t.Logf("--- message: text=%q  annotations=%d", text, len(anns))
			for i, a := range anns {
				t.Logf("      [%d] type=%v chip_render_type=%v (field_set=%v) start=%d len=%d url=%q",
					i, a.GetType(), a.GetChipRenderType(), a.ChipRenderType != nil,
					a.GetStartIndex(), a.GetLength(), a.GetUrlMetadata().GetUrl().GetUrl())
			}
		}
	}
	if withURL == 0 {
		t.Log("NOTE: no message with a URL found in the fetched topics -- send a link in that " +
			"conversation from Google Chat, then re-run with -count=1")
	}
	t.Logf("DONE: %d message(s) with a URL inspected", withURL)
}

// TestLiveCreateDm verifies the create_dm request shape against the real
// server. Like the membership RPCs before it, the exact shape is guesswork
// until it round-trips: this is the test that turns it into a fact.
//
// SAFE TO REPEAT: a DM is unique per pair on Google Chat, so create_dm with
// someone you already have a DM with returns THAT DM rather than making a
// second one. Running this twice does not litter the account.
//
//	export GCHAT_LIVE_INVITE_GAIA='1234567890'      # or:
//	export GCHAT_LIVE_INVITE_EMAIL='someone@example.com'
//	go test -tags 'goolm live' -run TestLiveCreateDm -v -count=1 ./pkg/gchatmeow/
func TestLiveCreateDm(t *testing.T) {
	cookies := liveCookies(t)
	gaia := os.Getenv("GCHAT_LIVE_INVITE_GAIA")
	email := os.Getenv("GCHAT_LIVE_INVITE_EMAIL")
	if gaia == "" && email == "" {
		t.Skip("set GCHAT_LIVE_INVITE_GAIA or GCHAT_LIVE_INVITE_EMAIL to someone this account may DM")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client, err := NewClient(ClientOpts{Cookies: cookies, UserAgent: os.Getenv("GCHAT_LIVE_UA")})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if err := client.FetchXSRFToken(ctx); err != nil {
		t.Fatalf("FetchXSRFToken (cookies invalid / logged out): %v", err)
	}

	req := &pb.CreateDmRequest{}
	switch {
	case gaia != "":
		req.Members = []*pb.UserId{UserID(gaia)}
		t.Logf("creating/finding a DM with gaia %s", gaia)
	default:
		req.Invitees = []*pb.InviteeInfo{EmailInvitee(email)}
		t.Logf("creating/finding a DM with email %s", email)
	}

	resp, err := client.CreateDm(ctx, req)
	if err != nil {
		t.Fatalf("create_dm failed: %v%s", err, errDetail(err))
	}
	id, isDM, ok := GroupIDToParts(resp.GetDm().GetGroupId())
	if !ok || id == "" {
		t.Fatalf("create_dm returned no usable group id: %+v", resp.GetDm())
	}
	t.Logf("PASS create_dm -> id=%s isDM=%v memberships=%d", id, isDM, len(resp.GetMemberships()))
	for _, m := range resp.GetMemberships() {
		t.Logf("  member gaia=%s", m.GetId().GetMemberId().GetUserId().GetId())
	}
	if !isDM {
		t.Errorf("create_dm returned a group id that is not a DM (id=%s)", id)
	}
}

// TestLiveCreateGroup verifies the create_group request shape.
//
// NOT SAFE TO REPEAT BLINDLY: this creates a REAL space every run, and there
// is no delete_group RPC wired in this bridge, so each run leaves a space
// behind for you to remove by hand. It is therefore gated on its own opt-in
// variable rather than running with the rest of the live suite.
//
//	export GCHAT_LIVE_CREATE_SPACE_NAME='mautrix-go live test (delete me)'
//	# optional: export GCHAT_LIVE_INVITE_GAIA='1234567890'
//	go test -tags 'goolm live' -run TestLiveCreateGroup -v -count=1 ./pkg/gchatmeow/
func TestLiveCreateGroup(t *testing.T) {
	cookies := liveCookies(t)
	name := os.Getenv("GCHAT_LIVE_CREATE_SPACE_NAME")
	if name == "" {
		t.Skip("set GCHAT_LIVE_CREATE_SPACE_NAME to opt in -- this creates a REAL space you must delete by hand")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client, err := NewClient(ClientOpts{Cookies: cookies, UserAgent: os.Getenv("GCHAT_LIVE_UA")})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if err := client.FetchXSRFToken(ctx); err != nil {
		t.Fatalf("FetchXSRFToken (cookies invalid / logged out): %v", err)
	}

	info := &pb.SpaceCreationInfo{Name: &name}
	if gaia := os.Getenv("GCHAT_LIVE_INVITE_GAIA"); gaia != "" {
		info.InviteeMemberInfos = []*pb.InviteeMemberInfo{UserInviteeMemberInfo(gaia)}
	}

	resp, err := client.CreateGroup(ctx, &pb.CreateGroupRequest{
		CreationInfo: &pb.CreateGroupRequest_Space{Space: info},
	})
	if err != nil {
		t.Fatalf("create_group failed: %v%s", err, errDetail(err))
	}
	id, isDM, ok := GroupIDToParts(resp.GetGroup().GetGroupId())
	if !ok || id == "" {
		t.Fatalf("create_group returned no usable group id: %+v", resp.GetGroup())
	}
	t.Logf("PASS create_group -> id=%s isDM=%v name=%q -- REMEMBER TO DELETE THIS SPACE", id, isDM, name)
	if isDM {
		t.Errorf("create_group returned a DM id (id=%s), want a space", id)
	}
}
