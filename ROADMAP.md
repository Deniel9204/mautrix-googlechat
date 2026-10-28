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
  * [x] Per-chat mute (`mute` / `unmute` commands)◊
  * [x] Typing notifications
  * [x] Read receipts
  * [x] Membership actions
    * [x] Invite
    * [x] Join (accept invite)‡
    * [x] Kick
    * [x] Leave (the `leave` command¶, or a room leave with `bridge.bridge_matrix_leave: true`)
  * [x] Room metadata changes
    * [x] Name
    * [x] Topic (space description)§
    * [ ] Avatar (Google Chat space icons are emoji, not images)
* Google Chat → Matrix
  * [x] Message content
    * [x] Text
    * [x] Media
      * [x] Files
      * [x] Videos
      * [x] Images
      * [x] Google Drive file links
      * [x] Google Meet / YouTube links
      * [x] GIFs and link-preview media (downloaded and inlined)
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
  * [x] Per-chat mute◊
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
    * [x] When invited to chat‡
    * [x] When receiving message
  * [x] Private chat creation by inviting Matrix puppet of Google Chat user to new room
  * [x] Space creation from Matrix (`create-group` in an existing room)
  * [x] Option to use own Matrix account for messages sent from other Google Chat clients (double puppeting)
  * [x] One-shot migration from the Python bridge's database (`--migrate-from-python`)

† Outbound upload works. It can be turned off with
`network.disable_outbound_media` if Google ever breaks it for your account.

‡ Invites come from the live event Google sends the invitee; the chat list
never contains them. An invite sent while the bridge is down only shows up
if Google replays it on reconnect.

§ Needs the Matrix moderator level that a Google Chat space manager gets.

¶ Leaves a space with every one of your logins in it; hides a DM, since a
DM can't be left.

◊ Muting from Google Chat reaches Matrix only with double puppeting.
