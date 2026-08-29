# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project uses calendar versioning (`YY.MM`), matching the other
mautrix bridges.

## [26.08.4] - 2026-08-29

### Security

- Only the media address Google itself supplies is ever fetched. A link
  annotation carries two addresses: one Google provides (and, in the case
  observed against the live service, had already rehosted onto its own CDN),
  and one the sender's client referenced. 26.08.3 fell back to the second when
  the first was absent, so pasting a link to an image on a server you control
  was enough to make someone's bridge fetch it -- revealing the operator's IP
  address and the time the message was received, on live traffic and on
  history backfill.
  Only the Google-supplied address is now used, with no fallback, matching
  purple-googlechat -- which fetches far more link chips than this bridge does
  and still never follows the sender's address.
  Anyone running 26.08.3 with inline media enabled (the default) should
  upgrade. `network.disable_inline_url_media` also avoids it.

### Fixed

- The description of `network.disable_inline_url_media` in the example config,
  which said the fetch goes to a host chosen by whoever sent the message. That
  was describing the bug above rather than the intent.

## [26.08.3] - 2026-08-29

### Added

- Shared GIFs and link-preview media now arrive as inline images
  ([#35](https://github.com/Deniel9204/mautrix-googlechat/issues/35)). A GIF
  shared from Google Chat's own picker is not an attachment -- it is a link
  chip pointing at the media -- so it previously arrived as a bare URL to
  click, or, in the case below, not at all. The media is now downloaded and
  reuploaded as an ordinary Matrix image, alongside the link.
  Only annotations that actually look like media are fetched. Every other
  Google Chat client fetches *every* link chip, which turns a bridge into a
  link prefetcher that contacts any host a sender names; this one does not.
  The behaviour can be turned off entirely with
  `network.disable_inline_url_media`, which costs nothing but the inline
  rendering -- the link itself is always in the message either way.
- Starting a chat now tries your other logins when Google refuses
  ([#50](https://github.com/Deniel9204/mautrix-googlechat/issues/50)). If a
  person is reachable from only one of two bridged Google accounts, the
  attempt no longer stops at the first refusal.
- Typing your own email address into `start-chat` is now explained instead of
  returning a bare error. Google Chat cannot open a conversation with
  yourself, and its refusal says nothing about why; the bridge now recognises
  the address and says so. It is recognised only *after* the service refuses,
  so a stale or aliased address can never block a chat that would have worked.

### Fixed

- **Some messages were never arriving.** A message whose only content was a
  link chip -- the exact shape of a shared GIF, now confirmed against the live
  service -- converted to nothing at all and was silently dropped: no error,
  no log line, no message on Matrix. Such a message now always delivers, at
  minimum as a link.
- An attachment download that failed put the attachment's access token into
  the log. Request URLs are no longer written to error messages; what is
  written instead is the service's own explanation of the failure, which was
  previously discarded in favour of a long URL.
- `start-chat` failures now say what went wrong
  ([#49](https://github.com/Deniel9204/mautrix-googlechat/issues/49),
  [#50](https://github.com/Deniel9204/mautrix-googlechat/issues/50)). Passing
  two identifiers at once, passing a Matrix ID, or passing one of your own
  login IDs each produced an opaque HTTP status; each now explains itself.
  Deliberate rejections are also reported as client errors rather than as
  internal ones, so a mistyped identifier is no longer indistinguishable from
  a bridge fault.
- A chat between a user's two bridged Google accounts is no longer refused.
  The guard against opening a conversation with yourself was applied without
  considering that the other account might be reachable from a different
  login.
- Malformed user IDs are rejected before they cost anything. A mistyped ghost
  address previously created a database row, registered a Matrix user, and
  sent a request to Google before failing.
- A data race between the login profile write and the cookie refresh, both of
  which persist the same database row.

### Security

- Media referenced by a link chip is fetched over a separate, hardened path:
  HTTPS enforced on every redirect hop, internal and link-local addresses
  refused after DNS resolution, no proxy, capped redirects, and a size ceiling
  that cannot be disabled. The attachment path's assumptions do not apply to a
  URL the bridge did not choose, so it is deliberately not reused, and no
  Google credentials can reach the media host.

## [26.08.2] - 2026-08-28

### Added

- Starting a new DM from Matrix ([#29](https://github.com/Deniel9204/mautrix-googlechat/issues/29)). Resolving an identifier and
  opening the conversation are both wired up, so a chat can be started with
  someone who has no portal yet -- previously only conversations that already
  existed, or that a message happened to arrive in, were reachable.
  Google Chat addresses users by gaia id and its private API has no
  email-to-gaia lookup, so an email can only be acted on rather than merely
  resolved: it is accepted when starting the chat (the new DM's own
  membership list is what reveals the user), while a resolve-only request for
  an email is refused rather than silently opening a conversation nobody
  asked for. Both paths are verified against the live service, including that
  starting a chat with an existing contact returns that DM instead of
  creating a second one.
  Creating a *space* is deliberately not offered yet: every request shape
  tried is rejected by the service with a bare HTTP 400, tracked in
  [#48](https://github.com/Deniel9204/mautrix-googlechat/issues/48).
- Bot and app cards are now rendered ([#30](https://github.com/Deniel9204/mautrix-googlechat/issues/30)). An app posting a card
  puts all of its content in widgets and routinely sends no message text, and
  those attachments were never read -- so a card-only message bridged with no
  content at all and simply never appeared, which silently affected exactly
  the CI, alerting and ticketing traffic cards exist for. Card headers,
  section headers and descriptions, the text-bearing widgets and link buttons
  are now rendered; the interactive widgets (menus, pickers, form controls)
  are skipped, since Matrix cannot present them and acting on them would need
  a session this bridge does not model. Card text is escaped and button links
  carrying a script-bearing scheme are shown as plain labels, matching how
  message formatting is already handled.
- Member actions and room rename are now advertised for spaces
  ([#28](https://github.com/Deniel9204/mautrix-googlechat/issues/28)). Invite, kick, leave and renaming have worked since 26.07.5
  but were never declared, so capability-aware clients did not offer them.
  They are advertised for spaces only: both are rejected in DMs, because the
  underlying requests have no DM form, and claiming them there would offer
  actions guaranteed to fail. Only what is implemented is claimed -- not ban,
  which has no Google Chat equivalent, and not room topic or avatar.

### Fixed

- The ROADMAP's voice-message and bot-card entries, which were both stale:
  voice messages have gone through the ordinary file-upload path since 26.07.

## [26.08.1] - 2026-08-28

### Security

- Debug dumps are written owner-only
  ([GHSA-4v86-j7x4-qrg8](https://github.com/Deniel9204/mautrix-googlechat/security/advisories/GHSA-4v86-j7x4-qrg8)). With `GCHAT_DEBUG_DUMP`
  set, every wire chunk and decoded array -- raw message text, sender and room
  ids -- was written mode 0644, i.e. world-readable under a normal umask, and
  the capture-fixtures workflow points that variable at a real account. Files
  are now 0600 and the directory is created 0700; a failed write is logged
  rather than silently discarded.
- Attachment downloads refuse hops that resolve to internal addresses
  ([GHSA-3688-fgj4-649x](https://github.com/Deniel9204/mautrix-googlechat/security/advisories/GHSA-3688-fgj4-649x)). The host allowlist only
  decided whether a hop received cookies, never whether it should be made, so
  every redirect was followed wherever it pointed. Hops are now dialled
  through the same guarded dialer the avatar path uses, refusing loopback,
  link-local, private, CGNAT, multicast and unspecified addresses at connect
  time. Unlike the avatar client this one still honours `$HTTPS_PROXY`, since
  attachments are core functionality and the attachment URL is Google's own
  fixed endpoint rather than a remote-supplied one.
- The anti-CSRF token is withheld from an off-allowlist upload URL
  ([GHSA-35vq-9q47-79xv](https://github.com/Deniel9204/mautrix-googlechat/security/advisories/GHSA-35vq-9q47-79xv)). The resumable upload
  finalises against a server-supplied `x-goog-upload-url`, and every
  caller-supplied header was attached regardless of host. The token is now
  gated on the same host allowlist that gates cookies.
- pblite decoding is depth-capped
  ([GHSA-3fqp-w9q8-fwhj](https://github.com/Deniel9204/mautrix-googlechat/security/advisories/GHSA-3fqp-w9q8-fwhj)). `Message.last_reply` is
  self-referential, so a crafted payload could nest arbitrarily deep with a
  descriptor lookup and allocation per level; the codec was bounded only
  incidentally by `encoding/json`. Depth is now capped at 64 and an over-deep
  subtree is logged and skipped.

### Fixed

- Rejected forward-channel sends now surface instead of being treated as
  success ([#22](https://github.com/Deniel9204/mautrix-googlechat/issues/22)).
  `SendStreamEvent` never inspected the HTTP status, so a rejected initial
  ping left the long poll looking healthy while the server streamed nothing,
  until the 60s read-idle watchdog produced a misleading generic timeout.
  Sends are also serialized now, so the `ofs` sequence reaches the wire in the
  order it was assigned, and sending without a SID is refused outright.
- A malformed webchannel envelope no longer tears down the channel
  ([#23](https://github.com/Deniel9204/mautrix-googlechat/issues/23)). One
  undecodable element discarded the whole poll session along with the
  well-formed arrays beside it; each decode failure is now logged and skipped.
- A superseded connection can no longer steal the initial-sync latch
  ([#24](https://github.com/Deniel9204/mautrix-googlechat/issues/24)). A late
  callback from a replaced connection could consume the one-shot sync slot and
  run the chat-list sync under an already-cancelled context, leaving the
  account connected with no portals until a later reconnect.
- Rate limits are retried and message creates are not
  ([#25](https://github.com/Deniel9204/mautrix-googlechat/issues/25)). HTTP
  429 was previously not retried at all; it now is, paced by `Retry-After`.
  Conversely `create_topic`/`create_message` are no longer retried on 5xx: a
  500 can arrive after the server already accepted the message, so retrying
  risked posting it twice. **Behaviour change:** a transient 5xx on send now
  fails visibly instead of being silently retried. A visible failure you can
  retry deliberately beats a duplicate you cannot undo.
- Migration no longer aborts on an orphan reaction
  ([#26](https://github.com/Deniel9204/mautrix-googlechat/issues/26)). A
  reaction whose portal was not migrated failed a foreign key and rolled back
  the entire `--migrate-from-python` run; it now warns and skips, like every
  other dangling reference.
- Newlines inside code blocks are preserved
  ([#27](https://github.com/Deniel9204/mautrix-googlechat/issues/27)). The
  newline-to-`<br/>` pass rewrote newlines inside `<pre><code>` too, which
  renders as literal markup or doubled breaks depending on the client.

## [26.08.0] - 2026-08-28

### Security

- Avatar downloads are hardened against server-side request forgery and memory
  exhaustion
  ([GHSA-6jc3-jjwm-4px5](https://github.com/Deniel9204/mautrix-googlechat/security/advisories/GHSA-6jc3-jjwm-4px5)).
  A user's `avatar_url` is remote, untrusted input -- a Google Chat app
  supplies its own icon URL -- but was fetched with automatic
  redirect-following to any host and an uncapped body read, so a crafted URL
  could steer the bridge at its operator's own network (including the
  cloud-metadata endpoint) and have the response re-uploaded to Matrix as a
  ghost avatar, or exhaust memory. Avatar fetches now require https on the
  initial URL and every redirect hop, follow redirects manually with a cap,
  refuse to connect to loopback, link-local, private, CGNAT, multicast and
  unspecified addresses, and cap the body at 5 MiB (which also bounds a gzip
  decompression bomb). The address check runs at dial time, so a hostname that
  resolves to an internal address is caught too, and NAT64, 6to4 and
  IPv4-compatible IPv6 forms are decoded before it. `$HTTPS_PROXY` is
  deliberately no longer honoured for avatar fetches: a proxy is dialled
  instead of the target and reached via CONNECT, which would leave the address
  check inspecting only the proxy. Operators whose egress requires a proxy
  will no longer get ghost avatars.
- Relayed edits, deletions and reaction removals now require sender ownership
  ([GHSA-q7ww-q8m3-4w8f](https://github.com/Deniel9204/mautrix-googlechat/security/advisories/GHSA-q7ww-q8m3-4w8f)).
  In relay mode every relayed user's action is dispatched through one shared
  Google Chat account, so Google's per-account authorization could not stop
  one Matrix user editing or deleting another relayed user's message or
  reaction -- and sending an edit relation requires no Matrix power level at
  all. The bridge now compares the relayed sender against the sender it
  recorded for the target row and refuses a mismatch. Deployments that do not
  use relay mode are unaffected.

## [26.07.6] - 2026-07-24

### Fixed

- Links received from Google Chat are now rendered as clickable links in
  Matrix. Google Chat stamps `chip_render_type=RENDER_IF_POSSIBLE` on the URL
  annotation of an ordinary pasted link, but only `DO_NOT_RENDER` annotations
  were rendered inline, so every inbound link arrived as plain, unlinkified
  text. Sending links from Matrix was unaffected.

## [26.07.5] - 2026-07-22

### Added

- Outbound (Matrix → Google Chat) membership actions — invite, kick, and leave
  — and space rename, for spaces/group chats. Live-verified against Google
  Chat (see
  [#11](https://github.com/Deniel9204/mautrix-googlechat/issues/11)).

## [26.07.4] - 2026-07-22

### Fixed

- Chat-list sync now returns the account's conversations. The
  `paginated_world` request was missing a world-section, so the server
  returned an empty world and newly-created Google Chat conversations never
  auto-created a portal.

### Changed

- Outbound media upload confirmed working against Google's live endpoint (the
  upload 500s tracked in
  [mautrix/googlechat#114](https://github.com/mautrix/googlechat/issues/114)
  are a client request-shape bug this bridge doesn't share).
- Dependency and CI maintenance: hourly Renovate with patch automerge, and
  updated GitHub Actions plus the `tonistiigi/xx` cross-compiler.
- Project documentation consolidated into `docs/ARCHITECTURE.md`.

## [26.07.3] - 2026-07-17

### Added

- GitHub Releases are now created automatically on every release tag, titled
  with the calendar version like other mautrix bridges (e.g. `v26.07.3`) and
  carrying prebuilt static binaries (`mautrix-googlechat-amd64`, `-arm64`,
  `-darwin-arm64`) plus `sha256sums.txt`.

### Changed

- Docker version tags now use the v-prefixed calendar form matching upstream
  mautrix bridges (`:v26.07.3`, `:v26.07`); the unprefixed `:0.YYMM.P` and
  `:26.07.x` forms are no longer published.
- The Docker binary is now fully statically linked (`-linkmode external
  -extldflags -static`), same as upstream mautrix release builds.

## [26.07.2] - 2026-07-17

### Added

- Docker images are now also tagged with the calendar version (`:26.07.2`,
  `:26.07`) alongside the git-tag semver forms (`:0.2607.2`, `:0.2607`), so
  image tags match what `--version` reports.

## [26.07.1] - 2026-07-16

### Added

- Multi-arch Docker images (`linux/amd64` + `linux/arm64`) published to
  `ghcr.io/deniel9204/mautrix-googlechat` on every push and tag via GitHub
  Actions, cross-compiled with `tonistiigi/xx`.

### Fixed

- Tagged builds now report a clean calendar version (e.g. `v26.07.1`) via
  `SemCalVer`, and non-tagged (branch) Docker builds no longer panic on an
  unparseable version string.

## [26.07] - 2026-07-16

First tagged release: a Matrix–Google Chat puppeting bridge on top of the
mautrix-go bridgev2 framework (v0.29.0).

### Added

- Cookie-based login flow: paste the 5 required `chat.google.com` session
  cookies (`COMPASS`, `SSID`, `SID`, `OSID`, `HSID`) via the bridge's `login`
  command (Google Chat has no interactive OAuth login for third-party
  clients, so cookies are extracted manually from a logged-in browser).
- Real-time messaging: text messages with rich formatting, threads, replies,
  edits, and deletes, bridged bidirectionally between Matrix and Google Chat.
- Reactions, read receipts, and typing notifications, bridged
  bidirectionally.
- Inbound media: images and files sent in Google Chat are downloaded and
  bridged to Matrix as native media events.
- Outbound media support (Matrix → Google Chat uploads) is implemented using
  the purple-googlechat request shape and verified working against Google's
  live endpoint. The upload 500s tracked in
  [mautrix/googlechat#114](https://github.com/mautrix/googlechat/issues/114)
  are a client request-shape bug this bridge doesn't share; uploads can still be
  disabled via `network.disable_outbound_media`.
- Room metadata sync: room renames, topic/description changes, and
  membership changes (joins, invites, leaves, kicks) are bridged as they
  happen on Google Chat's side.
- History backfill for initial room population and catch-up after restarts,
  opt-in via the bridge's standard `backfill` config section.
- Pure-Go cryptography via the `goolm` build tag (no libolm/CGo dependency).

### Documentation

- Added `docs/authentication.md`, a step-by-step guide to extracting the
  required login cookies from Chrome or Firefox and completing the login
  flow, with a security warning about the sensitivity of those cookies.
- Rewrote `README.md` with an accurate feature matrix, quick start,
  Docker usage, and configuration pointers.
