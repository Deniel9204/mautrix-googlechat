# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project uses calendar versioning (`YY.MM`), matching the other
mautrix bridges.

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

First tagged release: a complete Go rewrite of the `mautrix/googlechat`
bridge on top of the mautrix-go bridgev2 framework (v0.29.0), replacing the
original Python implementation.

### Added

- Cookie-based login flow: paste the 5 required `chat.google.com` session
  cookies (`COMPASS`, `SSID`, `SID`, `OSID`, `HSID`) via the bridge's `login`
  command, matching the Python bridge's manual cookie-extraction approach
  (Google Chat has no interactive OAuth login for third-party clients).
- Real-time messaging: text messages with rich formatting, threads, replies,
  edits, and deletes, bridged bidirectionally between Matrix and Google Chat.
- Reactions, read receipts, and typing notifications, bridged
  bidirectionally.
- Inbound media: images and files sent in Google Chat are downloaded and
  bridged to Matrix as native media events.
- Outbound media support (Matrix → Google Chat uploads) is implemented, but
  currently fails against Google's live servers due to an upstream issue
  ([mautrix/googlechat#114](https://github.com/mautrix/googlechat/issues/114));
  it can be disabled via `network.disable_outbound_media`.
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
