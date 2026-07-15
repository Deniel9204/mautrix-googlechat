package gchatfmt_test

import (
	"testing"

	"google.golang.org/protobuf/proto"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv/gchatfmt"
)

// --- annotation builders (local to this test file: these three metadata
// types have no chip_render_type/span semantics gchatfmt.Parse cares about,
// unlike the MakeXAnnotation helpers in utils.go, so a start/length of 0 is
// fine -- AppendLinkAnnotations never reads those fields). ---------------

func videoCallAnnotation(meetingURL string, shouldNotRender bool) *pb.Annotation {
	return &pb.Annotation{
		Metadata: &pb.Annotation_VideoCallMetadata{
			VideoCallMetadata: &pb.VideoCallMetadata{
				MeetingSpace:    &pb.MeetingSpace{MeetingUrl: proto.String(meetingURL)},
				ShouldNotRender: proto.Bool(shouldNotRender),
			},
		},
	}
}

func driveAnnotation(id string, shouldNotRender bool) *pb.Annotation {
	return &pb.Annotation{
		Metadata: &pb.Annotation_DriveMetadata{
			DriveMetadata: &pb.DriveMetadata{
				Id:              proto.String(id),
				ShouldNotRender: proto.Bool(shouldNotRender),
			},
		},
	}
}

func youtubeAnnotation(id string) *pb.Annotation {
	return &pb.Annotation{
		Metadata: &pb.Annotation_YoutubeMetadata{
			YoutubeMetadata: &pb.YoutubeMetadata{Id: proto.String(id)},
		},
	}
}

func urlMetadataAnnotation(rawURL, imageURL string, shouldNotRender bool) *pb.Annotation {
	return &pb.Annotation{
		Metadata: &pb.Annotation_UrlMetadata{
			UrlMetadata: &pb.UrlMetadata{
				Url:             &pb.Url{Url: proto.String(rawURL)},
				ImageUrl:        proto.String(imageURL),
				ShouldNotRender: proto.Bool(shouldNotRender),
			},
		},
	}
}

// TestAppendLinkAnnotations_VideoCallMetadata ports the video_call_metadata
// branch (portal.py:1496-1503): a Meet URL not already present in the body
// is appended with a "\n\n" separator.
func TestAppendLinkAnnotations_VideoCallMetadata(t *testing.T) {
	text := "join the call"
	annotations := []*pb.Annotation{videoCallAnnotation("https://meet.google.com/abc-defg-hij", false)}

	got := gchatfmt.AppendLinkAnnotations(text, annotations)

	want := "join the call\n\nhttps://meet.google.com/abc-defg-hij"
	if got != want {
		t.Errorf("AppendLinkAnnotations = %q, want %q", got, want)
	}
}

// TestAppendLinkAnnotations_DriveMetadata ports the drive_metadata branch
// (portal.py:1504-1511): DRIVE_OPEN_URL?id=<id> is appended when the raw id
// is not already present in the body.
func TestAppendLinkAnnotations_DriveMetadata(t *testing.T) {
	text := "check this doc"
	annotations := []*pb.Annotation{driveAnnotation("1a2b3c", false)}

	got := gchatfmt.AppendLinkAnnotations(text, annotations)

	want := "check this doc\n\nhttps://drive.google.com/open?id=1a2b3c"
	if got != want {
		t.Errorf("AppendLinkAnnotations = %q, want %q", got, want)
	}
}

// TestAppendLinkAnnotations_YoutubeMetadata ports the youtube_metadata
// branch (portal.py:1512-1519): YOUTUBE_URL?v=<id> is appended when the raw
// id is not already present in the body.
func TestAppendLinkAnnotations_YoutubeMetadata(t *testing.T) {
	text := "watch this"
	annotations := []*pb.Annotation{youtubeAnnotation("dQw4w9WgXcQ")}

	got := gchatfmt.AppendLinkAnnotations(text, annotations)

	want := "watch this\n\nhttps://www.youtube.com/watch?v=dQw4w9WgXcQ"
	if got != want {
		t.Errorf("AppendLinkAnnotations = %q, want %q", got, want)
	}
}

// TestAppendLinkAnnotations_EmptyTextBodySetNotAppended ports the
// `if not evt.text_body: evt.text_body = str(url)` branch: an empty body
// is SET to the bare URL, not prefixed with "\n\n".
func TestAppendLinkAnnotations_EmptyTextBodySetNotAppended(t *testing.T) {
	annotations := []*pb.Annotation{driveAnnotation("driveid1", false)}

	got := gchatfmt.AppendLinkAnnotations("", annotations)

	want := "https://drive.google.com/open?id=driveid1"
	if got != want {
		t.Errorf("AppendLinkAnnotations = %q, want %q (no leading separator on empty body)", got, want)
	}
}

// TestAppendLinkAnnotations_AlreadyPresentSkipsDrive covers the substring
// dedup check: if the drive id is already present anywhere in the body
// (e.g. the user pasted the link themselves), Python does not append a
// duplicate.
func TestAppendLinkAnnotations_AlreadyPresentSkipsDrive(t *testing.T) {
	text := "see https://drive.google.com/open?id=driveid1 for the doc"
	annotations := []*pb.Annotation{driveAnnotation("driveid1", false)}

	got := gchatfmt.AppendLinkAnnotations(text, annotations)

	if got != text {
		t.Errorf("AppendLinkAnnotations = %q, want unchanged %q (id already present)", got, text)
	}
}

// TestAppendLinkAnnotations_AlreadyPresentSkipsVideoCall mirrors the drive
// dedup test for video_call_metadata's own presence check.
func TestAppendLinkAnnotations_AlreadyPresentSkipsVideoCall(t *testing.T) {
	text := "https://meet.google.com/abc-defg-hij join now"
	annotations := []*pb.Annotation{videoCallAnnotation("https://meet.google.com/abc-defg-hij", false)}

	got := gchatfmt.AppendLinkAnnotations(text, annotations)

	if got != text {
		t.Errorf("AppendLinkAnnotations = %q, want unchanged %q (meeting_url already present)", got, text)
	}
}

// TestAppendLinkAnnotations_DuplicateDriveAnnotationsDedupeAgainstRunningText
// is the key double-render/dedup test for the append path itself: TWO
// drive_metadata annotations referencing the SAME id in one message. Python
// mutates evt.text_body in place as it iterates, so the SECOND annotation's
// `id not in evt.text_body` check sees the URL the FIRST one just appended
// and is skipped -- the id must appear exactly once in the result, not
// twice.
func TestAppendLinkAnnotations_DuplicateDriveAnnotationsDedupeAgainstRunningText(t *testing.T) {
	text := "shared twice"
	annotations := []*pb.Annotation{
		driveAnnotation("driveid1", false),
		driveAnnotation("driveid1", false),
	}

	got := gchatfmt.AppendLinkAnnotations(text, annotations)

	want := "shared twice\n\nhttps://drive.google.com/open?id=driveid1"
	if got != want {
		t.Errorf("AppendLinkAnnotations = %q, want %q (second duplicate annotation deduped)", got, want)
	}
}

// TestAppendLinkAnnotations_ShouldNotRenderStillAppends is a fidelity pin:
// unlike url_metadata's AttachmentURL branch, NONE of video_call_metadata /
// drive_metadata / youtube_metadata's should_not_render fields are read by
// portal.py's _preprocess_annotations (verified against the full text of
// all three branches, portal.py:1496-1519). A should_not_render=true
// annotation of these types is still appended -- this looks surprising but
// is intentional fidelity to Python, not a bug.
func TestAppendLinkAnnotations_ShouldNotRenderStillAppends(t *testing.T) {
	text := "hidden-ish"
	annotations := []*pb.Annotation{
		videoCallAnnotation("https://meet.google.com/zzz-zzzz-zzz", true),
		driveAnnotation("shoulddrive1", true),
	}

	got := gchatfmt.AppendLinkAnnotations(text, annotations)

	want := "hidden-ish\n\nhttps://meet.google.com/zzz-zzzz-zzz\n\nhttps://drive.google.com/open?id=shoulddrive1"
	if got != want {
		t.Errorf("AppendLinkAnnotations = %q, want %q (should_not_render is not read for these types)", got, want)
	}
}

// TestAppendLinkAnnotations_UrlMetadataNeverAppended is THE key
// double-render pin from the M5 Task 4 investigation: Python's
// _preprocess_annotations never appends a url_metadata annotation's URL to
// text_body at all (regardless of should_not_render or image_url) --
// url_metadata instead becomes an AttachmentURL for the (out-of-scope, HTTP)
// attachment-download path. gchatfmt.Parse (via renderURL, convert.go:523)
// already renders a DO_NOT_RENDER-chip url_metadata annotation as an inline
// <a href> wrapping EXISTING text -- if AppendLinkAnnotations also appended
// url_metadata's URL to the body, that URL would render TWICE (once as the
// inline hyperlink around the original text, once as new plain appended
// text). This test proves AppendLinkAnnotations leaves the body completely
// unchanged for url_metadata, in every combination of should_not_render and
// image_url presence.
func TestAppendLinkAnnotations_UrlMetadataNeverAppended(t *testing.T) {
	text := "check out this link"
	tests := []struct {
		name       string
		annotation *pb.Annotation
	}{
		{"should_not_render=false, no image_url", urlMetadataAnnotation("https://example.com/page", "", false)},
		{"should_not_render=true", urlMetadataAnnotation("https://example.com/page", "", true)},
		{"image_url present", urlMetadataAnnotation("https://example.com/page", "https://example.com/preview.png", false)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := gchatfmt.AppendLinkAnnotations(text, []*pb.Annotation{tc.annotation})
			if got != text {
				t.Errorf("AppendLinkAnnotations = %q, want unchanged %q (url_metadata is never appended)", got, text)
			}
		})
	}
}

// TestAppendLinkAnnotations_NoMatchingAnnotationsLeavesTextUnchanged covers
// the default/no-op case: format and mention annotations (and an empty
// annotation slice) must not perturb the text at all.
func TestAppendLinkAnnotations_NoMatchingAnnotationsLeavesTextUnchanged(t *testing.T) {
	text := "plain text, nothing special"

	t.Run("nil annotations", func(t *testing.T) {
		if got := gchatfmt.AppendLinkAnnotations(text, nil); got != text {
			t.Errorf("AppendLinkAnnotations = %q, want unchanged %q", got, text)
		}
	})

	t.Run("format annotation only", func(t *testing.T) {
		annotations := []*pb.Annotation{gchatfmt.MakeFormatAnnotation(0, 5, pb.FormatMetadata_BOLD)}
		if got := gchatfmt.AppendLinkAnnotations(text, annotations); got != text {
			t.Errorf("AppendLinkAnnotations = %q, want unchanged %q", got, text)
		}
	})

	t.Run("mention annotation only", func(t *testing.T) {
		annotations := []*pb.Annotation{gchatfmt.MakeMentionAnnotation(0, 5, "gaia1")}
		if got := gchatfmt.AppendLinkAnnotations(text, annotations); got != text {
			t.Errorf("AppendLinkAnnotations = %q, want unchanged %q", got, text)
		}
	})
}

// TestAppendLinkAnnotations_MixedOrderMatchesAnnotationOrder proves
// annotations are processed in their given order (matching Python's plain
// `for annotation in evt.annotations:` iteration, no sorting) and that
// unrelated annotation types interleaved among them are simply skipped.
func TestAppendLinkAnnotations_MixedOrderMatchesAnnotationOrder(t *testing.T) {
	text := "start"
	annotations := []*pb.Annotation{
		gchatfmt.MakeFormatAnnotation(0, 5, pb.FormatMetadata_BOLD),
		driveAnnotation("driveid1", false),
		youtubeAnnotation("yid1"),
	}

	got := gchatfmt.AppendLinkAnnotations(text, annotations)

	want := "start\n\nhttps://drive.google.com/open?id=driveid1\n\nhttps://www.youtube.com/watch?v=yid1"
	if got != want {
		t.Errorf("AppendLinkAnnotations = %q, want %q", got, want)
	}
}
