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

// urlMetadataAnnotation builds a url_metadata annotation covering length code
// units of the body. The length is load-bearing: > 0 means the annotation
// decorates a span that is already in text_body (convert.go renders it as an
// inline <a href>), == 0 means the URL appears nowhere in the body.
func urlMetadataAnnotation(rawURL, imageURL string, shouldNotRender bool, length int32) *pb.Annotation {
	return &pb.Annotation{
		Length: proto.Int32(length),
		Metadata: &pb.Annotation_UrlMetadata{
			UrlMetadata: &pb.UrlMetadata{
				Url:             &pb.Url{Url: proto.String(rawURL)},
				ImageUrl:        proto.String(imageURL),
				ShouldNotRender: proto.Bool(shouldNotRender),
			},
		},
	}
}

// TestAppendLinkAnnotations_VideoCallMetadata covers the video_call_metadata
// branch: a Meet URL not already present in the body is appended with a
// "\n\n" separator.
func TestAppendLinkAnnotations_VideoCallMetadata(t *testing.T) {
	text := "join the call"
	annotations := []*pb.Annotation{videoCallAnnotation("https://meet.google.com/abc-defg-hij", false)}

	got := gchatfmt.AppendLinkAnnotations(text, annotations)

	want := "join the call\n\nhttps://meet.google.com/abc-defg-hij"
	if got != want {
		t.Errorf("AppendLinkAnnotations = %q, want %q", got, want)
	}
}

// TestAppendLinkAnnotations_DriveMetadata covers the drive_metadata branch:
// DRIVE_OPEN_URL?id=<id> is appended when the raw id is not already present
// in the body.
func TestAppendLinkAnnotations_DriveMetadata(t *testing.T) {
	text := "check this doc"
	annotations := []*pb.Annotation{driveAnnotation("1a2b3c", false)}

	got := gchatfmt.AppendLinkAnnotations(text, annotations)

	want := "check this doc\n\nhttps://drive.google.com/open?id=1a2b3c"
	if got != want {
		t.Errorf("AppendLinkAnnotations = %q, want %q", got, want)
	}
}

// TestAppendLinkAnnotations_YoutubeMetadata covers the youtube_metadata
// branch: YOUTUBE_URL?v=<id> is appended when the raw id is not already
// present in the body.
func TestAppendLinkAnnotations_YoutubeMetadata(t *testing.T) {
	text := "watch this"
	annotations := []*pb.Annotation{youtubeAnnotation("dQw4w9WgXcQ")}

	got := gchatfmt.AppendLinkAnnotations(text, annotations)

	want := "watch this\n\nhttps://www.youtube.com/watch?v=dQw4w9WgXcQ"
	if got != want {
		t.Errorf("AppendLinkAnnotations = %q, want %q", got, want)
	}
}

// TestAppendLinkAnnotations_EmptyTextBodySetNotAppended covers the empty-body
// branch: an empty body is SET to the bare URL, not prefixed with "\n\n".
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
// (e.g. the user pasted the link themselves), no duplicate is appended.
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
// drive_metadata annotations referencing the SAME id in one message. The
// body is mutated in place as the loop iterates, so the SECOND annotation's
// presence check sees the URL the FIRST one just appended and is skipped --
// the id must appear exactly once in the result, not twice.
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
// drive_metadata / youtube_metadata read their should_not_render field. A
// should_not_render=true annotation of these types is still appended -- this
// looks surprising but is intentional, not a bug.
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

// TestAppendLinkAnnotations_UrlMetadataNeverAppended is THE double-render pin
// from the M5 Task 4 investigation, now narrowed to the case it actually
// protects: a url_metadata annotation that COVERS TEXT (length > 0).
//
// gchatfmt.Parse (via renderURL in convert.go) renders such an annotation as
// an inline <a href> wrapping text that is already in the body. If
// AppendLinkAnnotations also appended its URL, the URL would render TWICE --
// once as the hyperlink around the original text, once as new plain appended
// text. This proves the body is left completely unchanged for that shape, in
// every combination of should_not_render and image_url presence.
//
// The complement (length == 0, the URL is nowhere in the body) IS appended;
// see TestAppendLinkAnnotations_ZeroLengthUrlMetadataIsAppended.
func TestAppendLinkAnnotations_UrlMetadataNeverAppended(t *testing.T) {
	text := "check out this link"
	const covers = int32(4) // "this"
	tests := []struct {
		name       string
		annotation *pb.Annotation
	}{
		{"should_not_render=false, no image_url", urlMetadataAnnotation("https://example.com/page", "", false, covers)},
		{"should_not_render=true", urlMetadataAnnotation("https://example.com/page", "", true, covers)},
		{"image_url present", urlMetadataAnnotation("https://example.com/page", "https://example.com/preview.png", false, covers)},
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
// annotations are processed in their given order (a plain iteration, no
// sorting) and that unrelated annotation types interleaved among them are
// simply skipped.
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

// TestAppendLinkAnnotations_ZeroLengthUrlMetadataIsAppended pins the
// never-drop rule. A url_metadata annotation covering no text has its URL
// nowhere in text_body, and convert.go will not render it either
// (inlineURLChip requires length > 0). Without the append the link is
// invisible -- and when text_body is empty as well, the whole message converts
// to zero parts and mautrix-go sends nothing, so it never reaches Matrix at
// all.
func TestAppendLinkAnnotations_ZeroLengthUrlMetadataIsAppended(t *testing.T) {
	const gifURL = "https://tenor.com/view/example-gif-12345"
	tests := []struct {
		name       string
		text       string
		annotation *pb.Annotation
		want       string
	}{
		{
			// The message-loss case: nothing else in the message at all.
			name:       "empty body is set to the URL",
			text:       "",
			annotation: urlMetadataAnnotation(gifURL, "https://media.example.com/example.gif", false, 0),
			want:       gifURL,
		},
		{
			name:       "non-empty body gets the URL appended",
			text:       "look at this",
			annotation: urlMetadataAnnotation(gifURL, "", false, 0),
			want:       "look at this\n\n" + gifURL,
		},
		{
			// should_not_render asks for the preview CARD to be suppressed.
			// Honouring it here would trade a redundant link for a lost
			// message.
			name:       "should_not_render does not suppress the append",
			text:       "",
			annotation: urlMetadataAnnotation(gifURL, "", true, 0),
			want:       gifURL,
		},
		{
			// The dedup rule the other branches use applies here too.
			name:       "a URL already in the body is not appended twice",
			text:       "see " + gifURL,
			annotation: urlMetadataAnnotation(gifURL, "", false, 0),
			want:       "see " + gifURL,
		},
		{
			// strings.Contains(text, "") is always true, so an absent URL is
			// skipped without special-casing.
			name:       "an annotation with no URL appends nothing",
			text:       "hello",
			annotation: urlMetadataAnnotation("", "", false, 0),
			want:       "hello",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := gchatfmt.AppendLinkAnnotations(tc.text, []*pb.Annotation{tc.annotation}); got != tc.want {
				t.Errorf("AppendLinkAnnotations = %q, want %q", got, tc.want)
			}
		})
	}
}
