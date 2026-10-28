package gchatmeow

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"google.golang.org/protobuf/proto"

	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

// uploadURL is overridable via Client.uploadBaseURL for tests.
const uploadURL = "https://chat.google.com/uploads"

// stripWhitespace removes all embedded whitespace, which base64.StdEncoding rejects.
func stripWhitespace(data []byte) []byte {
	out := make([]byte, 0, len(data))
	for _, b := range data {
		// Only ASCII whitespace; high bytes are corruption and should fail decoding.
		switch b {
		case ' ', '\t', '\n', '\r', '\v', '\f':
			continue
		default:
			out = append(out, b)
		}
	}
	return out
}

// UploadFile performs a single-shot resumable upload and returns the UploadMetadata for an
// UPLOAD_METADATA annotation. It must not send "alt" or "key" query params.
func (c *Client) UploadFile(ctx context.Context, groupID string, data []byte, filename, mimeType string) (*pb.UploadMetadata, error) {
	xsrfToken := c.XSRFToken()

	startHeaders := http.Header{}
	startHeaders.Set("x-goog-upload-protocol", "resumable")
	startHeaders.Set("x-goog-upload-command", "start")
	startHeaders.Set("x-goog-upload-content-length", strconv.Itoa(len(data)))
	startHeaders.Set("x-goog-upload-content-type", mimeType)
	startHeaders.Set("x-goog-upload-file-name", filename)
	if xsrfToken != "" {
		startHeaders.Set("x-framework-xsrf-token", xsrfToken)
	}

	base := c.uploadBaseURL
	if base == "" {
		base = uploadURL
	}
	startQuery := url.Values{}
	startQuery.Set("group_id", groupID)
	startURL := base + "?" + startQuery.Encode()

	startResp, err := c.session.Fetch(ctx, http.MethodPost, startURL, startHeaders, nil)
	if err != nil {
		return nil, fmt.Errorf("gchatmeow: upload start request failed: %w", err)
	}

	nextURL := startResp.Header.Get("x-goog-upload-url")
	if nextURL == "" {
		return nil, fmt.Errorf("gchatmeow: upload start response missing x-goog-upload-url header")
	}

	// nextURL is server-supplied, so gate the XSRF token on the host allowlist too.
	nextParsed, err := url.Parse(nextURL)
	if err != nil {
		return nil, fmt.Errorf("gchatmeow: upload start response returned an unusable x-goog-upload-url %q: %w", nextURL, err)
	}

	finalizeHeaders := http.Header{}
	finalizeHeaders.Set("x-goog-upload-command", "upload, finalize")
	finalizeHeaders.Set("x-goog-upload-protocol", "resumable")
	finalizeHeaders.Set("x-goog-upload-offset", "0")
	if xsrfToken != "" && c.session.hostAllowed(nextParsed) {
		finalizeHeaders.Set("x-framework-xsrf-token", xsrfToken)
	}

	finalizeResp, err := c.session.Fetch(ctx, http.MethodPut, nextURL, finalizeHeaders, data)
	if err != nil {
		return nil, fmt.Errorf("gchatmeow: upload finalize request failed: %w", err)
	}

	decoded, err := base64.StdEncoding.DecodeString(string(stripWhitespace(finalizeResp.Body)))
	if err != nil {
		return nil, fmt.Errorf("gchatmeow: failed to decode base64 upload response: %w", err)
	}

	meta := &pb.UploadMetadata{}
	if err := proto.Unmarshal(decoded, meta); err != nil {
		return nil, fmt.Errorf("gchatmeow: failed to parse upload response protobuf: %w", err)
	}
	return meta, nil
}
