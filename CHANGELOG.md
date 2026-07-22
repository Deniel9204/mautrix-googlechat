# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project uses calendar versioning (`YY.MM`), matching the other
mautrix bridges.

## [Unreleased]

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
