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

type ReactionMetadata struct {
	// Google Chat topic id the reacted-to message belongs to -- the same
	// value MessageMetadata.TopicID stores on the message row itself (M3
	// Task 6). Populated ONLY when a reaction was itself added via Matrix
	// (handlereaction.go's HandleMatrixReaction, which already has the
	// target message's own resolved MessageMetadata.TopicID in hand at
	// add-time, no lookup needed) -- a fast-path optimization for a later
	// Matrix redaction of that same reaction (HandleMatrixReactionRemove)
	// to build the UpdateReaction RPC's message_id.parent_id.topic_id
	// without a DB.Message round trip. A reaction added from the Google
	// Chat side instead (queueMessageReaction, events.go, mirroring an
	// inbound MessageReactionEvent that carries no per-message payload to
	// read a topic id off directly) never populates this field, so
	// HandleMatrixReactionRemove's own reactionTopicID helper always falls
	// back to a fresh DB.Message lookup when it's empty -- exactly
	// portal.py's own handle_matrix_redaction reaction branch
	// (portal.py:816-829), which unconditionally re-fetches the target
	// DBMessage row on every removal
	// (`DBMessage.get_by_gcid(reaction.gc_msgid, ...)`) regardless of which
	// side created the reaction. See reactionTopicID's own doc comment
	// (handlereaction.go) for the full two-source resolution order; a Google
	// Chat message's topic membership is immutable once posted, so caching
	// this value here for the reactions that CAN cache it is always safe
	// (unlike last_edit_time, which genuinely changes over a message's
	// lifetime and so must stay live on the message row instead).
	TopicID string `json:"topic_id,omitempty"`
}
