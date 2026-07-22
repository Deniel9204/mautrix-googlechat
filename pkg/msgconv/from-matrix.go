package msgconv

// from-matrix.go -- Matrix -> Google Chat message conversion, the outbound
// counterpart of from-gchat.go's ToMatrix. Covers full M3 scope via
// matrixfmt.Parse (Task 2): both the plain text_body AND the HTML-derived
// annotations list Google Chat's create_topic/create_message
// text_body+annotations fields carry.
import (
	"context"

	"maunium.net/go/mautrix/event"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv/matrixfmt"
)

// FromMatrix extracts the Google Chat text_body + annotations a Matrix
// message event's content should carry, delegating entirely to
// matrixfmt.Parse (Task 2). mention is the Matrix-pill->gaiaID resolver
// matrixfmt.Parse needs to turn a mention pill into a MENTION annotation
// (fix B2's outbound half); callers pass the real one built by
// pkg/connector/mentions.go's newOutboundMentionResolver (Task 3) via
// handlematrix.go (Task 4) -- msgconv itself stays portal/network-ignorant
// (see msgconv.go's package doc comment), so mention is accepted as a plain
// function value here, not a *bridgev2.Portal.
//
// content.Body is used verbatim as the plain-text fallback only when there
// is no HTML formatting to derive text_body from (matrixfmt.Parse's own
// "no Format/empty FormattedBody, and no @room in Body" gate -- see its
// doc comment); this mirrors ToMatrix's "text_body taken verbatim" choice
// in the opposite direction (from-gchat.go). When formatted_body IS
// present, the returned text_body is instead matrixfmt.Parse's own
// annotation-stripped rendition of the HTML (e.g. a mention pill's anchor
// text becomes "@Name" in text_body, not the pill's raw inner HTML) --
// Matrix's own contract that formatted_body renders richer content than
// body, not different content, so no information is lost either way.
//
// content.FormattedBody being absent/empty (or Format != event.FormatHTML)
// produces a nil annotations slice, not an empty-but-non-nil one --
// matrixfmt.Parse's own documented contract -- so callers can use `if
// annotations != nil` as a cheap "was this formatted" check, matching this
// package's inbound gate (gchatfmt.Parse: "html is \"\" whenever
// annotations is empty").
//
// ctx is threaded through to matrixfmt.Parse's own MentionResolver calls
// (some outbound resolution paths -- e.g. bridgev2.Bridge.GetExistingUserByMXID,
// Portal.FindPreferredLogin -- are real I/O, unlike the inbound direction's
// pure ghost-MXID formula).
func (mc *MessageConverter) FromMatrix(ctx context.Context, content *event.MessageEventContent, mention matrixfmt.MentionResolver) (string, []*pb.Annotation) {
	return matrixfmt.Parse(ctx, content, mention)
}
