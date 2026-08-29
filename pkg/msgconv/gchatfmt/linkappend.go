package gchatfmt

import (
	"net/url"
	"strings"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// driveOpenURLBase / youtubeWatchURLBase are the fixed URL bases for Drive
// open links (https://drive.google.com/open) and YouTube watch links
// (https://www.youtube.com/watch). The query string is built with net/url
// (string construction only -- no networking, consistent with msgconv's
// "no HTTP" package doc).
const (
	driveOpenURLBase    = "https://drive.google.com/open"
	youtubeWatchURLBase = "https://www.youtube.com/watch"
)

// AppendLinkAnnotations handles the video_call_metadata / drive_metadata /
// youtube_metadata annotation branches. For each such annotation, if its URL
// (video_call_metadata's meeting_url) or id (drive_metadata / youtube_metadata)
// is not already present in the body, the corresponding link is appended:
//
//   - video_call_metadata: append the meeting_url verbatim.
//   - drive_metadata: append DRIVE_OPEN_URL?id=<id>.
//   - youtube_metadata: append YOUTUBE_URL?v=<id>.
//
// An empty body is SET to the bare URL; a non-empty body gets "\n\n" + URL
// appended.
//
// # Why url_metadata is NOT handled here (the double-render investigation)
//
// It is tempting to assume url_metadata (Drive/Meet/YouTube/generic link
// previews) is the annotation type appended to the body. It is not. The
// annotation branches split three ways:
//
//   - upload_metadata and url_metadata both build an attachment reference and
//     are downloaded over HTTP by the CALLER, becoming a SEPARATE attachment
//     message part (image/file), not a body append. url_metadata's
//     should_not_render skip and image_url-else-url.url precedence are real,
//     but they gate that download path, not a text-body append.
//   - url_metadata that covers a span of text (length > 0) is still never
//     appended here: convert.go's renderURL wraps that existing span in an
//     inline <a href>, so appending would render the same URL twice. That is
//     the double-render hazard this section was written about, and it is
//     exactly what the length check below preserves.
//   - Only video_call_metadata, drive_metadata, and youtube_metadata mutate
//     the body, unconditionally, which is what this function does.
//   - gchatfmt's OWN existing renderURL (convert.go) is a THIRD,
//     unrelated mechanism: it renders a url_metadata annotation with
//     chip_render_type == DO_NOT_RENDER as an inline `<a href>` wrapping a
//     span of text that is ALREADY part of the body (from the HTML renderer,
//     not this function). It never adds new text to the body, only HTML
//     markup around existing text.
//
// The one url_metadata case that IS appended is the complement: an annotation
// covering NO text (length == 0). Its URL appears nowhere in text_body and
// convert.go refuses to render it (inlineURLChip requires length > 0), so
// without an append the link is invisible -- and when text_body is empty too,
// ToMatrix produces zero parts and mautrix-go sends nothing at all, silently
// dropping the whole message. Because the two gates are exact complements
// (length > 0 renders inline, length == 0 appends), no annotation can ever be
// rendered by both.
//
// # Fidelity notes
//
//   - should_not_render is NEVER read for video_call_metadata,
//     drive_metadata, or youtube_metadata. Only url_metadata's branch checks
//     it (for the unrelated AttachmentURL path). So a video_call_metadata/
//     drive_metadata/youtube_metadata annotation with should_not_render=true
//     is STILL appended here; this is intentional, not an oversight.
//   - The presence check is against the RAW id (drive_metadata/
//     youtube_metadata) or the raw meeting_url (video_call_metadata) --
//     never the constructed open-URL. video_call_metadata has no derived URL
//     to build (the wire's meeting_url IS the URL appended), so its check and
//     append value are the same string.
//   - The check is evaluated against the RUNNING (already-mutated-by-this-
//     loop) text: a second annotation whose id/meeting_url was just appended
//     by an earlier one in the same message sees it already present and is
//     deduped (skipped). This is an in-place-mutation order dependency.
//   - Empty id/meeting_url needs no special-casing: strings.Contains(text, "")
//     is always true, so the append is naturally skipped when the field is
//     absent from the wire.
//   - The append-vs-set decision uses text == "" (the empty body case),
//     matching the same rule already documented in from-gchat.go for the
//     top-level text_body gate.
func AppendLinkAnnotations(text string, annotations []*pb.Annotation) string {
	for _, a := range annotations {
		var checkAgainst, appendURL string
		switch {
		case a.GetVideoCallMetadata() != nil:
			meetingURL := a.GetVideoCallMetadata().GetMeetingSpace().GetMeetingUrl()
			checkAgainst, appendURL = meetingURL, meetingURL
		case a.GetDriveMetadata() != nil:
			id := a.GetDriveMetadata().GetId()
			checkAgainst, appendURL = id, driveOpenURL(id)
		case a.GetYoutubeMetadata() != nil:
			id := a.GetYoutubeMetadata().GetId()
			checkAgainst, appendURL = id, youtubeWatchURL(id)
		case a.GetUrlMetadata() != nil && a.GetLength() == 0:
			// A link chip attached to the message rather than to a span. See
			// the doc comment: this is the only url_metadata shape appended,
			// and it is what stops a message whose sole content is such an
			// annotation from vanishing entirely.
			//
			// should_not_render is deliberately NOT consulted. It asks for the
			// preview CARD to be suppressed, and honouring it here would trade
			// a redundant link for a lost message.
			u := a.GetUrlMetadata().GetUrl().GetUrl()
			checkAgainst, appendURL = u, u
		default:
			continue
		}
		if strings.Contains(text, checkAgainst) {
			continue
		}
		if text == "" {
			text = appendURL
		} else {
			text = text + "\n\n" + appendURL
		}
	}
	return text
}

// driveOpenURL builds "https://drive.google.com/open?id=<id>",
// percent-encoded.
func driveOpenURL(id string) string {
	q := url.Values{}
	q.Set("id", id)
	return driveOpenURLBase + "?" + q.Encode()
}

// youtubeWatchURL builds "https://www.youtube.com/watch?v=<id>",
// percent-encoded.
func youtubeWatchURL(id string) string {
	q := url.Values{}
	q.Set("v", id)
	return youtubeWatchURLBase + "?" + q.Encode()
}
