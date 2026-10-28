package connector

// Metadata stored as JSON in bridgev2's DB; JSON field names are persisted, do not rename.

type UserLoginMetadata struct {
	// The five auth cookies (COMPASS, SSID, SID, OSID, HSID), refreshed after every connect.
	Cookies   map[string]string `json:"cookies"`
	UserAgent string            `json:"user_agent,omitempty"`
	// Last fully-handled user event stream revision (catch_up_user watermark).
	Revision int64 `json:"revision,omitempty"`
	// Space ids with an unanswered invite; gates whether a Matrix accept/decline is sent (invites.go).
	PendingInvites []string `json:"pending_invites,omitempty"`
}

type PortalMetadata struct {
	// Last fully-handled group revision (catch_up_group watermark).
	Revision int64 `json:"revision,omitempty"`
	// Threaded space (2023+ "threads only" model).
	ThreadsOnly bool `json:"threads_only,omitempty"`
	// Stored as flat_threads_enabled || threads_only, so a superset of ThreadsOnly.
	ThreadsEnabled bool `json:"threads_enabled,omitempty"`
}

type MessageMetadata struct {
	// Original create_time in µs; needed to build SendReplyTarget for quote-replies.
	TimestampMicro int64 `json:"ts_micro,omitempty"`
	// last_edit_time of the newest applied edit, for edit dedup.
	LastEditTime int64 `json:"last_edit_time,omitempty"`
	// Topic id this message belongs to (its own id for a topic's head message).
	TopicID string `json:"topic_id,omitempty"`
}

type GhostMetadata struct {
	Email string `json:"email,omitempty"`
}

type ReactionMetadata struct {
	// Topic id of the reacted-to message; only set for Matrix-added reactions, else looked up.
	TopicID string `json:"topic_id,omitempty"`
}
