package gchatfmt

import (
	"net/url"
	"strings"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

const (
	driveOpenURLBase    = "https://drive.google.com/open"
	youtubeWatchURLBase = "https://www.youtube.com/watch"
)

// AppendLinkAnnotations appends video_call/drive/youtube URLs, and zero-length url_metadata
// URLs, unless already in text. url_metadata covering text renders inline in convert.go instead.
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
			// should_not_render is deliberately ignored: honouring it would drop a link-only message.
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

func driveOpenURL(id string) string {
	q := url.Values{}
	q.Set("id", id)
	return driveOpenURLBase + "?" + q.Encode()
}

func youtubeWatchURL(id string) string {
	q := url.Values{}
	q.Set("v", id)
	return youtubeWatchURLBase + "?" + q.Encode()
}
