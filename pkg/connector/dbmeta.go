package connector

// Metadata structs stored as JSON in bridgev2's DB tables via GetDBMetaTypes.
// Field JSON names are persisted — do not rename.

type UserLoginMetadata struct {
	// The five auth cookies (COMPASS, SSID, SID, OSID, HSID), refreshed after every connect.
	Cookies   map[string]string `json:"cookies"`
	UserAgent string            `json:"user_agent,omitempty"`
	// Last fully-handled user event stream revision (catch_up_user watermark).
	Revision int64 `json:"revision,omitempty"`
}

type PortalMetadata struct {
	// Last fully-handled group revision (catch_up_group watermark).
	Revision int64 `json:"revision,omitempty"`
	// Threaded space (2023+ "threads only" model).
	ThreadsOnly bool `json:"threads_only,omitempty"`
	// Legacy threaded space (topic-based threading enabled).
	ThreadsEnabled bool `json:"threads_enabled,omitempty"`
}

type MessageMetadata struct {
	// Original Google Chat create_time in microseconds. Required to build
	// SendReplyTarget for quote-replies.
	TimestampMicro int64 `json:"ts_micro,omitempty"`
	// last_edit_time of the newest applied edit, for edit dedup.
	LastEditTime int64 `json:"last_edit_time,omitempty"`
}

type GhostMetadata struct {
	Email string `json:"email,omitempty"`
}
