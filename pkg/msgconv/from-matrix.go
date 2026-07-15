package msgconv

// from-matrix.go -- Matrix -> Google Chat message conversion, the outbound
// counterpart of from-gchat.go's ToMatrix. Ports the text half of what
// portal.py's _handle_matrix_text (portal.py:1051-1079) feeds into
// maugclib/client.py's send_message: `text, annotations :=
// fmt.matrix_to_googlechat(message)` (fmt.py), restricted to M2's plain-text
// scope -- the HTML/annotation-producing half of matrix_to_googlechat is
// M3's job.
import (
	"context"

	"maunium.net/go/mautrix/event"
)

// FromMatrix extracts the plain-text body Google Chat's create_topic /
// create_message text_body field carries from a Matrix message event's
// content.
//
// M2 scope: content.Body is taken verbatim, exactly mirroring ToMatrix's
// "text_body taken verbatim as UTF-8" choice in the opposite direction (see
// from-gchat.go's doc comment) -- no UTF-16 surrogate padding/offset math
// (that only matters once annotation start_index/length slicing exists,
// M3), no trimming, no truncation. Astral-plane characters (e.g. emoji
// outside the BMP) survive untouched because Go strings are already UTF-8.
//
// content.FormattedBody is deliberately NOT read here. Porting
// matrix_to_googlechat's HTML-aware conversion (mentions, bold/italic/etc.
// -> googlechat.Annotation) is M3's job; until then, always using the plain
// Body -- even for a message that also carries a formatted_body -- matches
// the Matrix spec's own contract that body must always be a reasonable
// plain-text rendition of formatted_body (m.text's content.formatted_body
// doc, matrix spec §m.room.message msgtypes), so no information intended to
// be visible is silently dropped, and no HTML markup is ever emitted as
// literal chat text.
//
// ctx is accepted (and currently unused) to match ToMatrix's signature
// shape and leave room for M3's formatting pass, which will need it (e.g.
// resolving Matrix mentions to Google Chat user annotations requires
// network/ghost lookups a pure string-in-string-out signature can't carry).
func (mc *MessageConverter) FromMatrix(_ context.Context, content *event.MessageEventContent) string {
	return content.Body
}
