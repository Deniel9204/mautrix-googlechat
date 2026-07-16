package connector

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

const testDisplaynameTemplate = `{{ or .Name .Email "Unknown user" }} (Google Chat)`

func newTestConfig(t *testing.T) *Config {
	t.Helper()
	cfg := &Config{DisplaynameTemplate: testDisplaynameTemplate, InitialChatSync: 20}
	if err := cfg.PostProcess(); err != nil {
		t.Fatalf("PostProcess: %v", err)
	}
	return cfg
}

// --- displaynameParams fallback chain ------------------------------------

func TestDisplaynameParamsFallbackChain(t *testing.T) {
	cases := []struct {
		name  string
		user  *pb.User
		want  string // the .Name field FormatDisplayname's template sees
		first string
	}{
		{
			name:  "full name used as-is",
			user:  &pb.User{Name: proto.String("Ada Lovelace"), FirstName: proto.String("Ada"), Email: proto.String("ada@example.com")},
			want:  "Ada Lovelace",
			first: "Ada",
		},
		{
			name:  "first+last joined when no full name",
			user:  &pb.User{FirstName: proto.String("Grace"), LastName: proto.String("Hopper"), Email: proto.String("grace@example.com")},
			want:  "Grace Hopper",
			first: "Grace", // FirstName passes through unchanged; only Name is derived here
		},
		{
			name:  "first only, no last",
			user:  &pb.User{FirstName: proto.String("Cher"), Email: proto.String("cher@example.com")},
			want:  "Cher",
			first: "Cher",
		},
		{
			name:  "no names at all leaves Name blank (template falls back to email)",
			user:  &pb.User{Email: proto.String("noname@example.com")},
			want:  "",
			first: "",
		},
		{
			// puppet.py:194-198: full name given, but no explicit first_name
			// -> derive FirstName from the full name, stripping a trailing
			// last_name suffix.
			name:  "first name derived from full name, stripping last name suffix",
			user:  &pb.User{Name: proto.String("Ada Lovelace"), LastName: proto.String("Lovelace"), Email: proto.String("ada@example.com")},
			want:  "Ada Lovelace",
			first: "Ada",
		},
		{
			// Same derivation, but last_name is NOT a suffix of the full
			// name (e.g. a nickname/full name mismatch) -- first falls back
			// to the whole full name, matching Python's `if last and
			// first.endswith(last)` guard (only strips when it's actually a
			// suffix).
			name:  "first name derivation skips stripping when last name isn't a suffix",
			user:  &pb.User{Name: proto.String("Ada L."), LastName: proto.String("Lovelace"), Email: proto.String("ada@example.com")},
			want:  "Ada L.",
			first: "Ada L.",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := displaynameParams(tc.user)
			if got.Name != tc.want {
				t.Errorf("Name = %q, want %q", got.Name, tc.want)
			}
			if got.FirstName != tc.first {
				t.Errorf("FirstName = %q, want %q", got.FirstName, tc.first)
			}
			if got.Email != tc.user.GetEmail() {
				t.Errorf("Email = %q, want %q", got.Email, tc.user.GetEmail())
			}
		})
	}
}

// TestDisplaynameParamsThroughFormatDisplayname exercises the FULL chain
// (name -> first+last -> email -> "Unknown user") through the real
// Config.FormatDisplayname template, matching the task's stated test:
// "displayname fallback chain test (name -> first+last -> email -> default
// via FormatDisplayname)".
func TestDisplaynameParamsThroughFormatDisplayname(t *testing.T) {
	cfg := newTestConfig(t)

	cases := []struct {
		name string
		user *pb.User
		want string
	}{
		{
			name: "full name wins",
			user: &pb.User{Name: proto.String("Ada Lovelace"), Email: proto.String("ada@example.com")},
			want: "Ada Lovelace (Google Chat)",
		},
		{
			name: "first+last fallback",
			user: &pb.User{FirstName: proto.String("Grace"), LastName: proto.String("Hopper"), Email: proto.String("grace@example.com")},
			want: "Grace Hopper (Google Chat)",
		},
		{
			name: "email fallback when no names",
			user: &pb.User{Email: proto.String("noname@example.com")},
			want: "noname@example.com (Google Chat)",
		},
		{
			name: "default fallback when nothing at all",
			user: &pb.User{},
			want: `Unknown user (Google Chat)`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := cfg.FormatDisplayname(context.Background(), displaynameParams(tc.user))
			if got != tc.want {
				t.Errorf("FormatDisplayname = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestFormatDisplayname_TemplateExecutionErrorIsLogged is the M7 Task 3 item
// 8 regression test: a displayname_template that PARSES fine (PostProcess
// succeeds) but fails at EXECUTE time -- e.g. it references a field that
// doesn't exist on DisplaynameParams, which text/template only catches when
// actually rendering a value, not at Parse time -- must be logged, not
// silently discarded. FormatDisplayname still returns whatever (partial)
// text Execute produced before failing; only the "nothing was logged at
// all" behavior is being fixed here.
func TestFormatDisplayname_TemplateExecutionErrorIsLogged(t *testing.T) {
	cfg := &Config{DisplaynameTemplate: `{{ .NoSuchField }}`}
	if err := cfg.PostProcess(); err != nil {
		t.Fatalf("PostProcess: %v (template should parse fine -- the bad reference is a runtime execution error)", err)
	}

	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	ctx := logger.WithContext(context.Background())

	_ = cfg.FormatDisplayname(ctx, DisplaynameParams{Name: "Ada"})

	if buf.Len() == 0 {
		t.Fatal("no log output produced for a template execution error, want it logged instead of silently discarded")
	}
	if !strings.Contains(buf.String(), "NoSuchField") {
		t.Errorf("log output = %q, want it to mention the template execution error", buf.String())
	}
}

// TestFormatDisplayname_WorkingTemplateLogsNothing is a control case
// proving the fix doesn't spuriously log on every call -- only an actual
// Execute error should produce a log line.
func TestFormatDisplayname_WorkingTemplateLogsNothing(t *testing.T) {
	cfg := newTestConfig(t)

	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	ctx := logger.WithContext(context.Background())

	got := cfg.FormatDisplayname(ctx, DisplaynameParams{Name: "Ada"})
	if got != "Ada (Google Chat)" {
		t.Errorf("FormatDisplayname = %q, want %q", got, "Ada (Google Chat)")
	}
	if buf.Len() != 0 {
		t.Errorf("log output = %q, want none for a successfully executed template", buf.String())
	}
}

// --- wrapAvatar -----------------------------------------------------------

func TestWrapAvatarEmptyURLMeansRemove(t *testing.T) {
	a := wrapAvatar("")
	if !a.Remove {
		t.Error("Remove = false, want true for an empty avatar url")
	}
}

func TestWrapAvatarNonEmptyURLSetsIDAndGet(t *testing.T) {
	a := wrapAvatar("http://lh3.googleusercontent.com/a/foo")
	if a.Remove {
		t.Error("Remove = true, want false for a non-empty avatar url")
	}
	if string(a.ID) != "http://lh3.googleusercontent.com/a/foo" {
		t.Errorf("ID = %q, want the raw (pre-https-forcing) url", a.ID)
	}
	if a.Get == nil {
		t.Error("Get = nil, want a download function")
	}
}

// --- userInfoFromUser ------------------------------------------------------

func TestUserInfoFromUserIsBot(t *testing.T) {
	gc := &GChatClient{Main: &GChatConnector{Config: *newTestConfig(t)}}

	human := &pb.User{
		UserId: &pb.UserId{Id: proto.String("200"), Type: pb.UserType_HUMAN.Enum()},
		Name:   proto.String("A Human"),
		Email:  proto.String("human@example.com"),
	}
	bot := &pb.User{
		UserId: &pb.UserId{Id: proto.String("999"), Type: pb.UserType_BOT.Enum()},
		Name:   proto.String("A Bot"),
		Email:  proto.String("bot@example.com"),
	}

	humanInfo := gc.userInfoFromUser(context.Background(), human)
	if humanInfo.IsBot == nil || *humanInfo.IsBot {
		t.Errorf("human IsBot = %v, want false", humanInfo.IsBot)
	}
	botInfo := gc.userInfoFromUser(context.Background(), bot)
	if botInfo.IsBot == nil || !*botInfo.IsBot {
		t.Errorf("bot IsBot = %v, want true", botInfo.IsBot)
	}
}

func TestUserInfoFromUserIdentifiersAndName(t *testing.T) {
	gc := &GChatClient{Main: &GChatConnector{Config: *newTestConfig(t)}}
	user := &pb.User{
		UserId: &pb.UserId{Id: proto.String("200")},
		Name:   proto.String("Ada Lovelace"),
		Email:  proto.String("ada@example.com"),
	}

	info := gc.userInfoFromUser(context.Background(), user)

	if len(info.Identifiers) != 1 || info.Identifiers[0] != "mailto:ada@example.com" {
		t.Errorf("Identifiers = %v, want [\"mailto:ada@example.com\"]", info.Identifiers)
	}
	if info.Name == nil || *info.Name != "Ada Lovelace (Google Chat)" {
		t.Errorf("Name = %v, want \"Ada Lovelace (Google Chat)\"", info.Name)
	}
}

func TestUserInfoFromUserExtraUpdatesStoresEmail(t *testing.T) {
	gc := &GChatClient{Main: &GChatConnector{Config: *newTestConfig(t)}}
	user := &pb.User{UserId: &pb.UserId{Id: proto.String("200")}, Email: proto.String("ada@example.com")}

	info := gc.userInfoFromUser(context.Background(), user)
	if info.ExtraUpdates == nil {
		t.Fatal("ExtraUpdates = nil")
	}
	ghost := &bridgev2.Ghost{Ghost: &database.Ghost{ID: networkid.UserID("200"), Metadata: &GhostMetadata{}}}

	changed := info.ExtraUpdates(context.Background(), ghost)
	if !changed {
		t.Error("ExtraUpdates() = false on first call, want true (email newly set)")
	}
	meta := ghost.Metadata.(*GhostMetadata)
	if meta.Email != "ada@example.com" {
		t.Errorf("GhostMetadata.Email = %q, want \"ada@example.com\"", meta.Email)
	}

	// Second call with the same email should report no change.
	if changed := info.ExtraUpdates(context.Background(), ghost); changed {
		t.Error("ExtraUpdates() = true on second call with the same email, want false")
	}
}

// --- GetUserInfo: no-conn error path (RPC itself covered at Task 13) -----

func TestGetUserInfoNoConnIsError(t *testing.T) {
	gc := &GChatClient{Main: &GChatConnector{Config: *newTestConfig(t)}}
	ghost := &bridgev2.Ghost{Ghost: &database.Ghost{ID: networkid.UserID("200")}}

	_, err := gc.GetUserInfo(context.Background(), ghost)
	if err == nil {
		t.Fatal("GetUserInfo with no live conn = nil error, want non-nil")
	}
}

// TestUpdateOwnLoginProfile_SetsRemoteName pins the fix for the empty
// per-login "personal filtering space" name ("Google Chat ()"):
// updateOwnLoginProfile must resolve the logged-in account's OWN name via
// get_members on the login's own gaia, then write it (RAW, no template) to
// UserLogin.RemoteName + RemoteProfile.Name and save.
func TestUpdateOwnLoginProfile_SetsRemoteName(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{}) // ID "112233", RemoteName ""
	var saved bool
	var gotMemberID string
	c := &GChatClient{
		UserLogin: login,
		getMembersFn: func(_ context.Context, req *pb.GetMembersRequest) (*pb.GetMembersResponse, error) {
			gotMemberID = req.GetMemberIds()[0].GetUserId().GetId()
			return &pb.GetMembersResponse{
				Members: []*pb.Member{{Profile: &pb.Member_User{User: &pb.User{Name: proto.String("Ada Lovelace")}}}},
			}, nil
		},
		saveFn: func(context.Context) error { saved = true; return nil },
	}
	c.updateOwnLoginProfile(context.Background())

	if gotMemberID != "112233" {
		t.Errorf("get_members targeted %q, want the login's own gaia 112233", gotMemberID)
	}
	// RAW name, NOT "Ada Lovelace (Google Chat)": bridgev2 wraps it as
	// "Google Chat (<RemoteName>)", so a formatted name would double-wrap.
	const want = "Ada Lovelace"
	if login.RemoteName != want {
		t.Errorf("RemoteName = %q, want %q", login.RemoteName, want)
	}
	if login.RemoteProfile.Name != want {
		t.Errorf("RemoteProfile.Name = %q, want %q", login.RemoteProfile.Name, want)
	}
	if !saved {
		t.Error("expected the updated login to be saved")
	}
}

// TestUpdateOwnLoginProfile_NoChangeNoSave: when RemoteName already matches the
// resolved name, no write/save happens (avoids a redundant Save every connect).
func TestUpdateOwnLoginProfile_NoChangeNoSave(t *testing.T) {
	login := newTestUserLogin(&UserLoginMetadata{})
	login.RemoteName = "Ada Lovelace"
	login.RemoteProfile.Name = "Ada Lovelace"
	saved := false
	c := &GChatClient{
		UserLogin: login,
		getMembersFn: func(_ context.Context, _ *pb.GetMembersRequest) (*pb.GetMembersResponse, error) {
			return &pb.GetMembersResponse{Members: []*pb.Member{{Profile: &pb.Member_User{User: &pb.User{Name: proto.String("Ada Lovelace")}}}}}, nil
		},
		saveFn: func(context.Context) error { saved = true; return nil },
	}
	c.updateOwnLoginProfile(context.Background())
	if saved {
		t.Error("expected no save when RemoteName is unchanged")
	}
}
