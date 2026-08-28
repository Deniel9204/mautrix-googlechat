# Features & roadmap

* Matrix → Google Chat
  * [ ] Message content
    * [x] Text
    * [ ] Media†
      * [ ] Stickers
      * [x] Files†
      * [x] Voice messages†
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
    * [x] Invite
    * [ ] Join (accept invite)
    * [x] Kick
    * [x] Leave
  * [x] Room metadata changes
    * [x] Name
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
    * [x] Bot/app cards (text widgets and link buttons)
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

† Outbound media upload is implemented and **verified working against Google's
live endpoint (2026-07-22)**. Some clients have hit an HTTP 500 on upload
since ~Feb 2026
([mautrix/googlechat#114](https://github.com/mautrix/googlechat/issues/114)),
but that is a client request-shape bug — the affected client appends
`alt=`/`key=` params to the signed upload URL and omits the XSRF header. This
bridge does neither (it sends the
[purple-googlechat](https://github.com/EionRobb/purple-googlechat) shape), and
a live upload confirmed it succeeds. `network.disable_outbound_media` remains
available to turn upload attempts into clean errors if a future change breaks
them for your account.
