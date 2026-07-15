package gchatfmt

import (
	"net/url"
	"strings"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// driveOpenURLBase / youtubeWatchURLBase are the fixed URL bases ported from
// mautrix_googlechat/formatter/gc_url_preview.py:39,41
// (DRIVE_OPEN_URL = URL("https://drive.google.com/open"),
// YOUTUBE_URL = URL("https://www.youtube.com/watch")). Those are yarl.URL
// literals combined with .with_query({...}) in portal.py; here the query
// string is built with net/url (string construction only -- no networking,
// consistent with msgconv's "no HTTP" package doc).
const (
	driveOpenURLBase    = "https://drive.google.com/open"
	youtubeWatchURLBase = "https://www.youtube.com/watch"
)

// AppendLinkAnnotations ports the video_call_metadata / drive_metadata /
// youtube_metadata branches of portal.py's _preprocess_annotations
// (~1496-1519):
//
//	elif annotation.HasField("video_call_metadata"):
//	    if annotation.video_call_metadata.meeting_space.meeting_url not in evt.text_body:
//	        url = annotation.video_call_metadata.meeting_space.meeting_url
//	        if not evt.text_body: evt.text_body = str(url)
//	        else: evt.text_body += f"\n\n{url}"
//	    continue
//	elif annotation.HasField("drive_metadata"):
//	    if annotation.drive_metadata.id not in evt.text_body:
//	        url = fmt.DRIVE_OPEN_URL.with_query({"id": annotation.drive_metadata.id})
//	        ... same append ...
//	elif annotation.HasField("youtube_metadata"):
//	    if annotation.youtube_metadata.id not in evt.text_body:
//	        url = fmt.YOUTUBE_URL.with_query({"v": annotation.youtube_metadata.id})
//	        ... same append ...
//
// # Why url_metadata is NOT handled here (the double-render investigation)
//
// The task brief that commissioned this function assumed url_metadata
// (Drive/Meet/YouTube/generic link previews) was the annotation type
// _preprocess_annotations appends to the body. That is not what the Python
// source does. _preprocess_annotations has FIVE oneof branches
// (portal.py:1465-1523): upload_metadata and url_metadata both build an
// AttachmentURL and `continue` WITHOUT touching evt.text_body at all --
// those two are returned in a list and downloaded over HTTP by the CALLER
// (portal.py:1421's `for att in attachment_urls: ... _process_googlechat_attachment`),
// becoming a SEPARATE attachment message part (image/file), not a body
// append. Only video_call_metadata, drive_metadata, and youtube_metadata
// mutate evt.text_body in place, and they do it unconditionally in that
// same function, before it returns. So:
//
//   - url_metadata's should_not_render skip and image_url-else-url.url
//     precedence (portal.py:1487-1492) are real, but they gate the
//     AttachmentURL/HTTP-download path (M5 Task 3's territory: only
//     upload_metadata was ported there; a generic url_metadata attachment
//     download needs an HTTP fetch of an arbitrary external URL --
//     _download_external_attachment, portal.py:1456-1462 -- which is out of
//     scope for this text-only, no-HTTP task and is not assigned to any M5
//     task). They do NOT gate a text-body append, because no such append
//     exists for url_metadata in Python.
//   - gchatfmt's OWN existing renderURL (convert.go:523, from M3) is a
//     THIRD, unrelated mechanism: it renders a url_metadata annotation with
//     chip_render_type == DO_NOT_RENDER as an inline `<a href>` wrapping a
//     span of text that is ALREADY part of text_body (from
//     _gc_annotations_to_matrix / from_googlechat.py's HTML renderer, not
//     _preprocess_annotations). It never adds new text to the body, only
//     HTML markup around existing text.
//
// Because _preprocess_annotations never appends url_metadata's URL to
// text_body, there is no double-render hazard to guard against for
// url_metadata: AppendLinkAnnotations simply never touches it (the default
// case below), so gchatfmt.Parse's inline rendering of a DO_NOT_RENDER
// url_metadata span is the only rendering url_metadata ever gets from this
// package, exactly matching Python. See
// .superpowers/sdd/m5-task-4-report.md for the full investigation writeup.
//
// # Fidelity notes
//
//   - should_not_render is NEVER read for video_call_metadata,
//     drive_metadata, or youtube_metadata in _preprocess_annotations --
//     verified against the full text of all three branches (portal.py:1496-1519).
//     Only url_metadata's branch checks it (for the unrelated AttachmentURL
//     path). So a video_call_metadata/drive_metadata/youtube_metadata
//     annotation with should_not_render=true is STILL appended here; this is
//     intentional fidelity to Python, not an oversight.
//   - The presence check is against the RAW id (drive_metadata/
//     youtube_metadata) or the raw meeting_url (video_call_metadata) --
//     never the constructed open-URL -- exactly mirroring
//     `annotation.drive_metadata.id not in evt.text_body` /
//     `annotation.youtube_metadata.id not in evt.text_body` /
//     `annotation.video_call_metadata.meeting_space.meeting_url not in evt.text_body`
//     verbatim. video_call_metadata has no derived URL to build (the wire's
//     meeting_url IS the URL appended), so its check and append value are
//     the same string.
//   - The check is evaluated against the RUNNING (already-mutated-by-this-
//     loop) text, exactly like Python mutating evt.text_body in place as it
//     iterates: a second annotation whose id/meeting_url was just appended
//     by an earlier one in the same message sees it already present and is
//     deduped (skipped), matching Python's identical in-place-mutation
//     order dependency.
//   - Empty id/meeting_url needs no special-casing: strings.Contains(text, "")
//     is always true (mirroring Python's `"" in text_body` being trivially
//     true), so the append is naturally skipped when the field is absent
//     from the wire.
//   - "not evt.text_body" (append vs. set) is Python's falsy-string check --
//     ported as text == "" (the only falsy string), matching the same rule
//     already documented in from-gchat.go for the top-level text_body gate.
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

// driveOpenURL builds fmt.DRIVE_OPEN_URL.with_query({"id": id}) --
// "https://drive.google.com/open?id=<id>", percent-encoded.
func driveOpenURL(id string) string {
	q := url.Values{}
	q.Set("id", id)
	return driveOpenURLBase + "?" + q.Encode()
}

// youtubeWatchURL builds fmt.YOUTUBE_URL.with_query({"v": id}) --
// "https://www.youtube.com/watch?v=<id>", percent-encoded.
func youtubeWatchURL(id string) string {
	q := url.Values{}
	q.Set("v", id)
	return youtubeWatchURLBase + "?" + q.Encode()
}
