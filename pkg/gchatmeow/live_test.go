//go:build live

// Live protocol tests. Export GCHAT_LIVE_{COMPASS,SSID,SID,OSID,HSID} cookies, then run:
// go test -tags 'goolm live' -run TestLive -v -timeout 5m ./pkg/gchatmeow/
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

// errDetail returns the response body (first 512 bytes) of an *UnexpectedStatusError, where Google's reason appears.
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

// TestLiveProtocol exercises the real protocol end to end; event delivery is reported, not asserted.
func TestLiveProtocol(t *testing.T) {
	cookies := liveCookies(t)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	client, err := NewClient(ClientOpts{Cookies: cookies, UserAgent: os.Getenv("GCHAT_LIVE_UA")})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	if err := client.FetchXSRFToken(ctx); err != nil {
		t.Fatalf("FAIL FetchXSRFToken (cookies invalid / logged out / drift): %v", err)
	}
	t.Log("PASS  FetchXSRFToken: /mole/world scrape + XSRF token OK (session valid)")

	self, err := client.GetSelfUserStatus(ctx, &pb.GetSelfUserStatusRequest{})
	if err != nil {
		t.Fatalf("FAIL GetSelfUserStatus (RPC wire format / base64 decode / drift): %v", err)
	}
	gaia := self.GetUserStatus().GetUserId().GetId()
	t.Logf("PASS  GetSelfUserStatus: own gaia id = %q (RPC + proto decode OK)", gaia)

	world, err := client.PaginatedWorld(ctx, &pb.PaginatedWorldRequest{
		FetchFromUserSpaces:  boolPtr(true),
		FetchOptions:         []pb.PaginatedWorldRequest_FetchOptions{pb.PaginatedWorldRequest_EXCLUDE_GROUP_LITE},
		WorldSectionRequests: []*pb.WorldSectionRequest{{PageSize: proto.Int32(999)}}, // required: server returns empty world without a section
	})
	if err != nil {
		t.Fatalf("FAIL PaginatedWorld (world sync / drift): %v", err)
	}
	t.Logf("PASS  PaginatedWorld: %d world items returned (chat-list sync OK)", len(world.GetWorldItems()))

	// BrowserChannel: connect for a bounded window and count events.
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

// TestLiveSendReceive sends a marked message and expects its MESSAGE_POSTED echo with the same text and local_id.
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

	marker := "m2-live-" + os.Getenv("GCHAT_LIVE_MARKER")
	if marker == "m2-live-" {
		marker = "m2-live-probe"
	}
	localID := "mautrix-googlechat%42424242"

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
	select {
	case <-time.After(8 * time.Second):
	case err := <-connErr:
		t.Fatalf("Connect returned early: %v", err)
	}

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

// TestLiveUpload does one real media upload. Needs a conversation (or GCHAT_LIVE_GROUP_ID); use -count=1.
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

	if gid := os.Getenv("GCHAT_LIVE_GROUP_ID"); gid != "" {
		runUploadProbe(t, ctx, client, gid)
		return
	}

	// PaginatedWorld needs a live channel, so connect first.
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
		// Distinguish "no chats" from a discovery bug.
		t.Skipf("live upload skipped: no conversation to upload into "+
			"(world items=%d, with group id=%d) -- set GCHAT_LIVE_GROUP_ID to a "+
			"known plain group id (e.g. a migrated portal id without its dm:/"+
			"space: prefix) and re-run with -count=1", len(items), withGroup)
	}

	runUploadProbe(t, ctx, client, groupID)
}

// runUploadProbe uploads a 1x1 PNG to groupID.
func runUploadProbe(t *testing.T, ctx context.Context, client *Client, groupID string) {
	t.Helper()
	// A 1x1 transparent PNG.
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

// TestLiveDiagnosePaginatedWorld dumps raw paginated_world bodies to tell base64 mis-parse, empty and proto drift apart.
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

	// A = our shape, B = A + world_section_requests{page_size:999}, C = purple-googlechat's shape.
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

// probeWorld issues one raw paginated_world request and reports item count and body diagnostics.
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
		// Fall back to base64, as unmarshalAPIResponse does.
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

// TestLiveRoomName renames GCHAT_LIVE_SPACE_ID and restores it (optional GCHAT_LIVE_SPACE_ORIG_NAME skips get_group).
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

	if err := rename(orig); err != nil {
		t.Errorf("could not restore original name %q (space is currently %q, rename it back manually): %v", orig, probe, err)
	} else {
		t.Logf("restored original name %q -- net-zero", orig)
	}
}

// TestLiveMembershipRoundTrip invites and then removes GCHAT_LIVE_INVITE_GAIA (email or gaia) in GCHAT_LIVE_SPACE_ID.
// DESTRUCTIVE: use a throwaway target account.
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

	// An email goes in invitee_info.email, a numeric gaia in invitee_info.user_id.
	var invitee *pb.InviteeInfo
	if strings.Contains(target, "@") {
		invitee = &pb.InviteeInfo{Email: proto.String(target)}
		t.Logf("inviting by email %q", target)
	} else {
		invitee = &pb.InviteeInfo{UserId: &pb.UserId{Id: proto.String(target)}}
		t.Logf("inviting by gaia %q", target)
	}

	// Snapshot members so an email invite can be cleaned up via a diff.
	before, err := spaceMemberGaias(ctx, client, spaceID)
	if err != nil {
		t.Fatalf("get_group (pre-invite snapshot): %v%s", err, errDetail(err))
	}

	// Non-fatal: a re-run may hit an existing invite; still remove it.
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

// spaceMemberGaias returns the gaia ids of joined and invited members.
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

// TestLiveDumpLinkAnnotations dumps url_metadata annotations (notably chip_render_type) for messages in GCHAT_LIVE_GROUP_ID.
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

// TestLiveCreateDm checks create_dm via GCHAT_LIVE_INVITE_GAIA and/or GCHAT_LIVE_INVITE_EMAIL. Safe to repeat: DMs are unique per pair.
func TestLiveCreateDm(t *testing.T) {
	cookies := liveCookies(t)
	gaia := strings.TrimSpace(os.Getenv("GCHAT_LIVE_INVITE_GAIA"))
	email := strings.TrimSpace(os.Getenv("GCHAT_LIVE_INVITE_EMAIL"))

	t.Logf("env: GCHAT_LIVE_INVITE_GAIA=%s  GCHAT_LIVE_INVITE_EMAIL=%s",
		presence(gaia), presence(email))
	if gaia == "" && email == "" {
		t.Skip("set GCHAT_LIVE_INVITE_GAIA and/or GCHAT_LIVE_INVITE_EMAIL to someone this account may DM")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	client, err := NewClient(ClientOpts{Cookies: cookies, UserAgent: os.Getenv("GCHAT_LIVE_UA")})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if err := client.FetchXSRFToken(ctx); err != nil {
		t.Fatalf("FetchXSRFToken (cookies invalid / logged out): %v", err)
	}

	// The email branch learns the peer's gaia only from memberships, so assert it names someone else.
	selfResp, err := client.GetSelfUserStatus(ctx, &pb.GetSelfUserStatusRequest{})
	if err != nil {
		t.Fatalf("GetSelfUserStatus (needed to tell the peer from this account): %v", err)
	}
	self := selfResp.GetUserStatus().GetUserId().GetId()
	if self == "" {
		t.Fatal("get_self_user_status returned no gaia id")
	}

	cases := []struct {
		name string
		skip bool
		req  *pb.CreateDmRequest
	}{
		{"gaia (members)", gaia == "", &pb.CreateDmRequest{Members: []*pb.UserId{UserID(gaia)}}},
		{"email (invitees)", email == "", &pb.CreateDmRequest{Invitees: []*pb.InviteeInfo{EmailInvitee(email)}}},
	}

	ids := make(map[string]string)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skip("not configured")
			}
			resp, err := client.CreateDm(ctx, tc.req)
			if err != nil {
				t.Fatalf("create_dm failed: %v%s\n(a bare 400 here usually means the id is not someone this account can DM, not a bad request -- both envelopes were verified against a known-good id)", err, errDetail(err))
			}
			id, isDM, ok := GroupIDToParts(resp.GetDm().GetGroupId())
			if !ok || id == "" {
				t.Fatalf("create_dm returned no usable group id: %+v", resp.GetDm())
			}
			peers, blank := 0, 0
			for _, m := range resp.GetMemberships() {
				memberGaia := m.GetId().GetMemberId().GetUserId().GetId()
				switch {
				case memberGaia == "":
					blank++
				case memberGaia == self:
				default:
					peers++
				}
				// Booleans, not ids: test output gets pasted into public issues.
				t.Logf("  membership: has_user_id=%v is_self=%v state=%v",
					memberGaia != "", memberGaia == self, m.GetMembershipState())
			}
			t.Logf("PASS create_dm -> id=%s isDM=%v memberships=%d peers=%d blank=%d",
				id, isDM, len(resp.GetMemberships()), peers, blank)
			if !isDM {
				t.Errorf("create_dm returned a group id that is not a DM (id=%s)", id)
			}
			if blank > 0 {
				t.Errorf("%d membership(s) carried no user id -- connector.otherMember skips those, "+
					"so ResolveIdentifier's email branch would answer with an empty UserID", blank)
			}
			if peers == 0 {
				t.Errorf("create_dm memberships named nobody but this account (%d memberships): "+
					"ResolveIdentifier's email branch learns the peer's gaia id from exactly this "+
					"response, so it would return an empty UserID. If the target is a Chat app "+
					"rather than a person, this is expected -- bot memberships need their own "+
					"fetch option.", len(resp.GetMemberships()))
			}
			ids[tc.name] = id
		})
	}

	// Both shapes must land on the same DM.
	if len(ids) == 2 {
		var seen []string
		for _, id := range ids {
			seen = append(seen, id)
		}
		if seen[0] != seen[1] {
			t.Logf("NOTE: the two branches returned different DMs (%s vs %s) -- expected only if the gaia and the email are different people", seen[0], seen[1])
		} else {
			t.Logf("PASS both branches returned the same DM (%s): create_dm finds the existing one", seen[0])
		}
	}
}

// presence reports whether a variable is set without logging its value.
func presence(v string) string {
	if v == "" {
		return "unset"
	}
	return "set"
}

// TestLiveCreateGroup creates a REAL space every run (no delete RPC); opt in with GCHAT_LIVE_CREATE_SPACE_NAME.
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

// TestLiveCreateGroupShape tries create_group candidate shapes, stopping at the first success.
// GCHAT_LIVE_INVITE_GAIA must be a known-good id: an unreachable one gives the same bare 400.
func TestLiveCreateGroupShape(t *testing.T) {
	cookies := liveCookies(t)
	name := os.Getenv("GCHAT_LIVE_CREATE_SPACE_NAME")
	gaia := os.Getenv("GCHAT_LIVE_INVITE_GAIA")
	if name == "" || gaia == "" {
		t.Skip("set GCHAT_LIVE_CREATE_SPACE_NAME and a KNOWN-GOOD GCHAT_LIVE_INVITE_GAIA -- this creates a REAL space you must delete by hand")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	client, err := NewClient(ClientOpts{Cookies: cookies, UserAgent: os.Getenv("GCHAT_LIVE_UA")})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if err := client.FetchXSRFToken(ctx); err != nil {
		t.Fatalf("FetchXSRFToken (cookies invalid / logged out): %v", err)
	}

	invitee := func() []*pb.InviteeMemberInfo {
		return []*pb.InviteeMemberInfo{UserInviteeMemberInfo(gaia)}
	}

	candidates := []struct {
		name string
		req  *pb.CreateGroupRequest
	}{
		{
			"purple-exact (no name, explicit find=false, 1 invitee)",
			&pb.CreateGroupRequest{
				ShouldFindExistingSpace: proto.Bool(false),
				CreationInfo: &pb.CreateGroupRequest_Space{Space: &pb.SpaceCreationInfo{
					InviteeMemberInfos: invitee(),
				}},
			},
		},
		{
			"purple + name",
			&pb.CreateGroupRequest{
				ShouldFindExistingSpace: proto.Bool(false),
				CreationInfo: &pb.CreateGroupRequest_Space{Space: &pb.SpaceCreationInfo{
					Name:               proto.String(name),
					InviteeMemberInfos: invitee(),
				}},
			},
		},
		{
			"name + explicit find=false, no invitees",
			&pb.CreateGroupRequest{
				ShouldFindExistingSpace: proto.Bool(false),
				CreationInfo: &pb.CreateGroupRequest_Space{Space: &pb.SpaceCreationInfo{
					Name: proto.String(name),
				}},
			},
		},
		{
			"name + invitee + has_server_generated_name=false",
			&pb.CreateGroupRequest{
				ShouldFindExistingSpace: proto.Bool(false),
				CreationInfo: &pb.CreateGroupRequest_Space{Space: &pb.SpaceCreationInfo{
					Name:                   proto.String(name),
					HasServerGeneratedName: proto.Bool(false),
					InviteeMemberInfos:     invitee(),
				}},
			},
		},
	}

	for _, candidate := range candidates {
		resp, err := client.CreateGroup(ctx, candidate.req)
		if err != nil {
			t.Logf("  %-52s -> FAILED: %v%s", candidate.name, err, errDetail(err))
			continue
		}
		id, isDM, ok := GroupIDToParts(resp.GetGroup().GetGroupId())
		t.Logf("  %-52s -> OK id=%s isDM=%v ok=%v", candidate.name, id, isDM, ok)
		t.Logf("FIRST WORKING SHAPE: %s -- REMEMBER TO DELETE THE SPACE (id=%s)", candidate.name, id)
		return
	}
	t.Fatal("no candidate shape was accepted; see the per-candidate errors above")
}

// TestLiveSelfDm checks whether create_dm accepts a DM with yourself. Opt in with GCHAT_LIVE_SELF_DM=1.
func TestLiveSelfDm(t *testing.T) {
	cookies := liveCookies(t)
	if os.Getenv("GCHAT_LIVE_SELF_DM") == "" {
		t.Skip("set GCHAT_LIVE_SELF_DM=1 to probe whether create_dm accepts the acting account itself")
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
	selfResp, err := client.GetSelfUserStatus(ctx, &pb.GetSelfUserStatusRequest{})
	if err != nil {
		t.Fatalf("GetSelfUserStatus: %v", err)
	}
	self := selfResp.GetUserStatus().GetUserId().GetId()
	if self == "" {
		t.Fatal("get_self_user_status returned no gaia id")
	}

	// Neither outcome is a failure.
	resp, err := client.CreateDm(ctx, &pb.CreateDmRequest{Members: []*pb.UserId{UserID(self)}})
	if err != nil {
		var ue *UnexpectedStatusError
		if errors.As(err, &ue) {
			t.Logf("RESULT create_dm(self) REFUSED with status %d%s -- the connector's local "+
				"self-DM guard matches the server", ue.Status, errDetail(err))
		} else {
			t.Logf("RESULT create_dm(self) failed without a status: %v", err)
		}
		return
	}
	id, isDM, ok := GroupIDToParts(resp.GetDm().GetGroupId())
	t.Logf("RESULT create_dm(self) SUCCEEDED -> id=%s isDM=%v usable_id=%v memberships=%d",
		id, isDM, ok, len(resp.GetMemberships()))
	t.Log("ACTION: Google allows a note-to-self DM, so connector.ResolveIdentifier's " +
		"local self-DM refusal on the gaia path is blocking something that works and should " +
		"become a classification of the server's answer, as the email path already is.")
}

// TestLiveGetMembersUnknownID checks what get_members returns for GCHAT_LIVE_BOGUS_GAIA.
func TestLiveGetMembersUnknownID(t *testing.T) {
	cookies := liveCookies(t)
	bogus := strings.TrimSpace(os.Getenv("GCHAT_LIVE_BOGUS_GAIA"))
	t.Logf("env: GCHAT_LIVE_BOGUS_GAIA=%s", presence(bogus))
	if bogus == "" {
		t.Skip("set GCHAT_LIVE_BOGUS_GAIA to a numeric id that is not a real account")
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

	resp, err := client.GetMembers(ctx, &pb.GetMembersRequest{
		MemberIds: []*pb.MemberId{{Id: &pb.MemberId_UserId{UserId: UserID(bogus)}}},
	})
	if err != nil {
		var ue *UnexpectedStatusError
		if errors.As(err, &ue) {
			t.Logf("RESULT get_members(bogus) ERRORED with status %d%s", ue.Status, errDetail(err))
		} else {
			t.Logf("RESULT get_members(bogus) errored without a status: %v", err)
		}
		return
	}
	members := resp.GetMembers()
	t.Logf("RESULT get_members(bogus) SUCCEEDED -> members=%d", len(members))
	for i, m := range members {
		u := m.GetUser()
		// Shapes, never values: a bogus id could collide with a real account.
		t.Logf("  member[%d]: has_user=%v has_name=%v has_email=%v has_user_id=%v",
			i, u != nil, u.GetName() != "", u.GetEmail() != "", u.GetUserId().GetId() != "")
	}
	if len(members) == 0 {
		t.Log("ACTION: an empty member list distinguishes a non-existent id, so " +
			"connector.ResolveIdentifier could verify a gaia id before answering Found -- " +
			"but only if a real-but-invisible user gives a DIFFERENT answer. Re-run this " +
			"with GCHAT_LIVE_BOGUS_GAIA set to a real user outside your domain before " +
			"relying on it.")
	}
}

// TestLiveDumpURLMediaAnnotations dumps the annotation fields inline-media detection relies on (GCHAT_LIVE_GROUP_ID).
func TestLiveDumpURLMediaAnnotations(t *testing.T) {
	cookies := liveCookies(t)
	groupID := os.Getenv("GCHAT_LIVE_GROUP_ID")
	if groupID == "" {
		t.Skip("set GCHAT_LIVE_GROUP_ID to a conversation containing a shared GIF")
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

	var resp *pb.ListTopicsResponse
	for _, isDM := range []bool{true, false} {
		resp, err = client.ListTopics(ctx, &pb.ListTopicsRequest{
			GroupId:           PartsToGroupID(groupID, isDM),
			PageSizeForTopics: proto.Int32(40),
		})
		if err == nil {
			break
		}
	}
	if err != nil {
		t.Fatalf("list_topics failed for both DM and space forms: %v%s", err, errDetail(err))
	}

	urlAnns, uploadAnns := 0, 0
	for _, topic := range resp.GetTopics() {
		for _, msg := range topic.GetReplies() {
			for _, a := range msg.GetAnnotations() {
				if u := a.GetUploadMetadata(); u != nil {
					uploadAnns++
					// A GIF here means the attachment path already covers it.
					t.Logf("upload_metadata: content_type=%q has_name=%v",
						u.GetContentType(), u.GetContentName() != "")
					continue
				}
				m := a.GetUrlMetadata()
				if m == nil {
					continue
				}
				urlAnns++
				// Hosts, never full URLs: output gets pasted into public issues.
				t.Logf("url_metadata: type=%v length=%d empty_text_body=%v", a.GetType(), a.GetLength(), msg.GetTextBody() == "")
				t.Logf("    url_host=%s image_url_host=%s", urlHost(m.GetUrl().GetUrl()), urlHost(m.GetImageUrl()))
				t.Logf("    mime_type=%q should_not_render=%v (present=%v) dims=%dx%d",
					m.GetMimeType(), m.GetShouldNotRender(), m.ShouldNotRender != nil,
					m.GetIntImageWidth(), m.GetIntImageHeight())
				// The verdict, computed as the connector would.
				wouldInline := !m.GetShouldNotRender() &&
					(a.GetType() == pb.AnnotationType_IMAGE ||
						strings.HasPrefix(m.GetMimeType(), "image/") ||
						strings.HasPrefix(m.GetMimeType(), "video/") ||
						(m.GetImageUrl() != "" && a.GetLength() == 0))
				t.Logf("    -> inlineableURLMedia would return %v", wouldInline)
			}
		}
	}
	t.Logf("SUMMARY: %d url_metadata annotations, %d upload_metadata annotations", urlAnns, uploadAnns)
	if urlAnns == 0 {
		t.Log("NOTE: no url_metadata annotations found. If this conversation definitely " +
			"contains a GIF, then Google delivers picker GIFs as upload_metadata and the " +
			"existing attachment path already handles them.")
	}
}

// urlHost reduces a URL to its host, to avoid logging identifying URLs.
func urlHost(rawURL string) string {
	if rawURL == "" {
		return "(unset)"
	}
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" {
		return "(unparseable)"
	}
	return u.Host
}
