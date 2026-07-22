# Features & roadmap

* Matrix → Google Chat
  * [ ] Message content
    * [x] Text
    * [ ] Media†
      * [ ] Stickers
      * [x] Files†
      * [ ] Voice messages
      * [x] Videos†
      * [x] Images†
      * [ ] Locations
    * [x] Formatting
    * [x] Mentions
    * [x] Threads
    * [x] Replies
  * [x] Message redactions
  * [x] Message reactions
  * [x] Message editing (text only)
  * [ ] Presence
  * [x] Typing notifications
  * [x] Read receipts
  * [ ] Membership actions
    * [ ] Invite
    * [ ] Join (accept invite)
    * [ ] Kick
    * [ ] Leave
  * [ ] Room metadata changes
    * [ ] Name
* Google Chat → Matrix
  * [x] Message content
    * [x] Text
    * [x] Media
      * [x] Files
      * [x] Videos
      * [x] Images
      * [x] Google Drive file links
      * [x] Google Meet / YouTube links
    * [x] Formatting
    * [x] Mentions
    * [x] Threads
    * [x] Replies
  * [x] Message deletions
  * [x] Message reactions
  * [x] Message editing (text only)
  * [x] Message history
    * [x] Initial backfill of recent history
    * [x] Missed-event catch-up after bridge downtime
  * [ ] Presence
  * [x] Typing notifications
  * [x] Read receipts
  * [x] Membership actions
    * [x] Add member
    * [x] Remove member
    * [x] Leave
  * [x] Chat metadata (title/topic) changes
  * [x] Initial chat metadata (title/topic)
  * [x] Initial user metadata
    * [x] Name
    * [x] Avatar
* Misc
  * [x] Multi-user support
  * [x] Shared group chat portals
  * [x] End-to-bridge encryption (Matrix side)
  * [x] Relay mode
  * [x] Automatic portal creation
    * [x] At startup
    * [ ] When invited to chat
    * [x] When receiving message
  * [ ] Private chat creation by inviting Matrix puppet of Google Chat user to new room
  * [x] Option to use own Matrix account for messages sent from other Google Chat clients (double puppeting)
  * [x] One-shot migration from the Python bridge's database (`--migrate-from-python`)

† Outbound media upload is fully implemented. The Python bridge's uploads have
failed with an HTTP 500 since ~Feb 2026
([mautrix/googlechat#114](https://github.com/mautrix/googlechat/issues/114)),
most plausibly because its client (maugclib) appends `alt=`/`key=` params to
the signed upload URL — a mutation this port does not perform (it sends the
[purple-googlechat](https://github.com/EionRobb/purple-googlechat) shape, which
uploads fine). A live upload from this bridge has not yet been run to confirm;
`network.disable_outbound_media` turns upload attempts into clean errors if
your account is affected.
