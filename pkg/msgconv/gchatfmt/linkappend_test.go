package gchatfmt_test

import (
	"testing"

	"google.golang.org/protobuf/proto"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
	"github.com/Deniel9204/mautrix-googlechat/pkg/msgconv/gchatfmt"
)

// Local builders: AppendLinkAnnotations never reads start/length for these types.

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

// urlMetadataAnnotation builds a url_metadata annotation; length > 0 means it covers body text.
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

func TestAppendLinkAnnotations_VideoCallMetadata(t *testing.T) {
	text := "join the call"
	annotations := []*pb.Annotation{videoCallAnnotation("https://meet.google.com/abc-defg-hij", false)}

	got := gchatfmt.AppendLinkAnnotations(text, annotations)

	want := "join the call\n\nhttps://meet.google.com/abc-defg-hij"
	if got != want {
		t.Errorf("AppendLinkAnnotations = %q, want %q", got, want)
	}
}

func TestAppendLinkAnnotations_DriveMetadata(t *testing.T) {
	text := "check this doc"
	annotations := []*pb.Annotation{driveAnnotation("1a2b3c", false)}

	got := gchatfmt.AppendLinkAnnotations(text, annotations)

	want := "check this doc\n\nhttps://drive.google.com/open?id=1a2b3c"
	if got != want {
		t.Errorf("AppendLinkAnnotations = %q, want %q", got, want)
	}
}

func TestAppendLinkAnnotations_YoutubeMetadata(t *testing.T) {
	text := "watch this"
	annotations := []*pb.Annotation{youtubeAnnotation("dQw4w9WgXcQ")}

	got := gchatfmt.AppendLinkAnnotations(text, annotations)

	want := "watch this\n\nhttps://www.youtube.com/watch?v=dQw4w9WgXcQ"
	if got != want {
		t.Errorf("AppendLinkAnnotations = %q, want %q", got, want)
	}
}

func TestAppendLinkAnnotations_EmptyTextBodySetNotAppended(t *testing.T) {
	annotations := []*pb.Annotation{driveAnnotation("driveid1", false)}

	got := gchatfmt.AppendLinkAnnotations("", annotations)

	want := "https://drive.google.com/open?id=driveid1"
	if got != want {
		t.Errorf("AppendLinkAnnotations = %q, want %q (no leading separator on empty body)", got, want)
	}
}

func TestAppendLinkAnnotations_AlreadyPresentSkipsDrive(t *testing.T) {
	text := "see https://drive.google.com/open?id=driveid1 for the doc"
	annotations := []*pb.Annotation{driveAnnotation("driveid1", false)}

	got := gchatfmt.AppendLinkAnnotations(text, annotations)

	if got != text {
		t.Errorf("AppendLinkAnnotations = %q, want unchanged %q (id already present)", got, text)
	}
}

func TestAppendLinkAnnotations_AlreadyPresentSkipsVideoCall(t *testing.T) {
	text := "https://meet.google.com/abc-defg-hij join now"
	annotations := []*pb.Annotation{videoCallAnnotation("https://meet.google.com/abc-defg-hij", false)}

	got := gchatfmt.AppendLinkAnnotations(text, annotations)

	if got != text {
		t.Errorf("AppendLinkAnnotations = %q, want unchanged %q (meeting_url already present)", got, text)
	}
}

// The body is mutated as the loop runs, so a repeated id is appended only once.
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

// should_not_render is intentionally ignored for these types.
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

// A url_metadata covering text renders inline, so appending would render it twice.
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

// A zero-length url_metadata appears nowhere else, so it must be appended.
func TestAppendLinkAnnotations_ZeroLengthUrlMetadataIsAppended(t *testing.T) {
	const gifURL = "https://tenor.com/view/example-gif-12345"
	tests := []struct {
		name       string
		text       string
		annotation *pb.Annotation
		want       string
	}{
		{
			// The message-loss case: nothing else in the message.
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
			// should_not_render only suppresses the preview card.
			name:       "should_not_render does not suppress the append",
			text:       "",
			annotation: urlMetadataAnnotation(gifURL, "", true, 0),
			want:       gifURL,
		},
		{
			// Same dedup rule as the other branches.
			name:       "a URL already in the body is not appended twice",
			text:       "see " + gifURL,
			annotation: urlMetadataAnnotation(gifURL, "", false, 0),
			want:       "see " + gifURL,
		},
		{
			// strings.Contains(text, "") is always true.
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
