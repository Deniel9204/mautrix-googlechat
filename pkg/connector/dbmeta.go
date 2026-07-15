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
	// Whether topic-based threading is enabled at all. Stored as
	// flat_threads_enabled || threads_only (chatinfo.go), so it's a
	// superset of ThreadsOnly -- read alongside ThreadsOnly by
	// capabilities.go, not just for legacy spaces.
	ThreadsEnabled bool `json:"threads_enabled,omitempty"`
}

type MessageMetadata struct {
	// Original Google Chat create_time in microseconds. Required to build
	// SendReplyTarget for quote-replies.
	TimestampMicro int64 `json:"ts_micro,omitempty"`
	// last_edit_time of the newest applied edit, for edit dedup.
	LastEditTime int64 `json:"last_edit_time,omitempty"`
	// Google Chat topic id this message belongs to (M3 Task 6): the
	// message's own id for the head/root message of a topic (message_id ==
	// topic_id on the wire), or the head's message id for a reply posted
	// into an existing topic. Stamped on every bridged message, both
	// directions:
	//   - inbound (msgconv_adapter.go's convertMessageToMatrix): read
	//     straight off the wire, msg.id.parent_id.topic_id.topic_id.
	//   - outbound (handlematrix.go): the id of the NEW topic a create_topic
	//     call just created, or the existing topic id a create_message
	//     reply was routed into.
	// Needed both directions: outbound reads the Matrix thread root's
	// stored topic id to route a reply into create_message's
	// parent_id.topic_id; inbound lets a later Matrix reply into this same
	// topic resolve its own ThreadRoot correctly (ToMatrix's message_id !=
	// topic_id check, pkg/msgconv/from-gchat.go).
	TopicID string `json:"topic_id,omitempty"`
}

type GhostMetadata struct {
	Email string `json:"email,omitempty"`
}
