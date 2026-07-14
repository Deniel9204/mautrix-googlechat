package gcid_test

import (
	"testing"

	"maunium.net/go/mautrix/bridgev2/networkid"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gcid"
)

func TestPortalIDRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		g    gcid.GroupID
		want networkid.PortalID
	}{
		{gcid.GroupID{ID: "AAAAdaMWXXc", IsDM: false}, "space:AAAAdaMWXXc"},
		{gcid.GroupID{ID: "gGboEmVbEyE", IsDM: true}, "dm:gGboEmVbEyE"},
	} {
		got := gcid.MakePortalID(tc.g)
		if got != tc.want {
			t.Fatalf("MakePortalID(%v) = %q, want %q", tc.g, got, tc.want)
		}
		back, err := gcid.ParsePortalID(got)
		if err != nil || back != tc.g {
			t.Fatalf("ParsePortalID(%q) = %v, %v; want %v", got, back, err, tc.g)
		}
	}
}

func TestParsePortalIDRejectsGarbage(t *testing.T) {
	for _, bad := range []networkid.PortalID{"", "AAAAdaMWXXc", "room:x", "dm:", "space:"} {
		if _, err := gcid.ParsePortalID(bad); err == nil {
			t.Fatalf("ParsePortalID(%q) should error", bad)
		}
	}
}

func TestPortalKeyReceiverScoping(t *testing.T) {
	login := networkid.UserLoginID("103432744896036786916")
	dm := gcid.MakePortalKey(gcid.GroupID{ID: "gGboEmVbEyE", IsDM: true}, login)
	if dm.Receiver != login {
		t.Fatalf("DM portals must be receiver-scoped to the owner login, got %q", dm.Receiver)
	}
	space := gcid.MakePortalKey(gcid.GroupID{ID: "AAAAdaMWXXc", IsDM: false}, login)
	if space.Receiver != "" {
		t.Fatalf("space portals must NOT be receiver-scoped, got %q", space.Receiver)
	}
}

func TestPartIDs(t *testing.T) {
	if gcid.TextPartID != networkid.PartID("") {
		t.Fatal("text part ID must be empty string")
	}
	if got := gcid.MakeAttachmentPartID(2); got != networkid.PartID("att_2") {
		t.Fatalf("MakeAttachmentPartID(2) = %q, want att_2", got)
	}
}
