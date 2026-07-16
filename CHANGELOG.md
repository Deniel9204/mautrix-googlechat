# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project does not yet follow Semantic Versioning tags/releases; entries
below summarize the Go rewrite's progress at a milestone level rather than
a commit-by-commit history.

## [Unreleased]

Initial Go rewrite of the `mautrix/googlechat` bridge on top of the
mautrix-go bridgev2 framework, replacing the original Python implementation.

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
