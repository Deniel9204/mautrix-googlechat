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
//     but they gate that AttachmentURL/HTTP-download path (only
//     upload_metadata is handled there; a generic url_metadata attachment
//     download needs an HTTP fetch of an arbitrary external URL, out of
//     scope for this text-only, no-HTTP package). They do NOT gate a
//     text-body append, because no such append exists for url_metadata.
//   - Only video_call_metadata, drive_metadata, and youtube_metadata mutate
//     the body, unconditionally, which is what this function does.
//   - gchatfmt's OWN existing renderURL (convert.go) is a THIRD,
//     unrelated mechanism: it renders a url_metadata annotation with
//     chip_render_type == DO_NOT_RENDER as an inline `<a href>` wrapping a
//     span of text that is ALREADY part of the body (from the HTML renderer,
//     not this function). It never adds new text to the body, only HTML
//     markup around existing text.
//
// Because url_metadata's URL is never appended to the body, there is no
// double-render hazard to guard against for it: AppendLinkAnnotations simply
// never touches url_metadata (the default case below), so gchatfmt.Parse's
// inline rendering of a DO_NOT_RENDER url_metadata span is the only rendering
// url_metadata ever gets from this package.
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
