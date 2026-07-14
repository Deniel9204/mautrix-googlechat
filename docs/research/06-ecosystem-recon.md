# Ecosystem Recon: Go Reimplementation of mautrix-googlechat

Research date: 2026-07-13. All claims cited. Note: several GitHub pages were summarized via a fetch assistant; dates were cross-checked against the GitHub API where it mattered.

---

## 1. Status of mautrix/googlechat (the Python bridge)

**Not archived, but effectively in maintenance-freeze on master.**

- GitHub API: `archived: false`, `open_issues_count: 30`, `pushed_at: 2026-04-22`, `updated_at: 2026-07-07`, default branch `master`, language Python, AGPL-3.0, 125 stars.
  Source: https://api.github.com/repos/mautrix/googlechat (verified directly via curl on 2026-07-13)
- **Last release: v0.5.2, released 2024-07-16.**
  Source: https://github.com/mautrix/googlechat (releases panel)
- **Last commits on `master`: 2025-07-16** — "Update dependencies", "Update Docker image to Alpine 3.22", "Hardcode v11 for new rooms" (room-version pin ahead of Matrix room v12). Before that, the previous burst of activity was July 2024 (the 0.5.2 release). No feature work on master since.
  Source: https://api.github.com/repos/mautrix/googlechat/commits?per_page=10
- The `pushed_at` of 2026-04-22 is more recent than the newest commit visible on any listed branch (master 2025-07-16; megabridge 2025-04-30), so it likely reflects a push to a PR/ephemeral ref — do not read it as active development.
  Sources: https://api.github.com/repos/mautrix/googlechat, https://api.github.com/repos/mautrix/googlechat/branches
- **No deprecation notice in the README** — it's the standard mautrix README (docs at docs.mau.fi, `#googlechat:maunium.net`, features in ROADMAP.md).
  Source: https://raw.githubusercontent.com/mautrix/googlechat/master/README.md
- However, Tulir's mid-2024 blog states the direction: *"Rewrites of the Python bridges (Telegram, Twitter, Google Chat, LinkedIn) are underway"* and mautrix-python's *"bridge module will likely be deprecated or even removed once the Go rewrites are ready."*
  Source: https://mau.fi/blog/2024-h1-mautrix-updates/
- matrix.org ecosystem page still lists mautrix-googlechat as the only Google Chat bridge, status **Beta**.
  Source: https://matrix.org/ecosystem/bridges/google_chat/

## 2. Existing Go implementations — CRITICAL: a Go bridgev2 port already exists

### 2a. The `megabridge` branch of mautrix/googlechat (the big one)

**There is an in-repo Go bridgev2 ("megabridge") rewrite of the googlechat bridge, written primarily by a Beeper engineer.** Any Go reimplementation plan must start from evaluating this branch rather than a green-field rebuild.

- Branch: https://github.com/mautrix/googlechat/tree/megabridge (a **protected** branch, alongside master).
  Source: https://api.github.com/repos/mautrix/googlechat/branches
- Latest commit: `c589550`, **2025-04-30**, author **Skip R (skip@beeper.com)**, message "connector: update room capabilities".
  Source: https://api.github.com/repos/mautrix/googlechat/branches/megabridge
- Structure (~20 Go files):
  - `cmd/mautrix-googlechat/main.go`
  - `pkg/connector/` — bridgev2 NetworkConnector/NetworkAPI: `client.go`, `connector.go`, `handlematrix.go`, `handlegchat.go`, `login.go`, `mapping.go`, `portal.go`, `ids.go`
  - `pkg/gchatmeow/` — Go Google Chat client library: `api.go`, `channel.go`, `client.go`, `session.go`, `cookies.go`, `event.go` (a direct structural port of the Python `maugclib`: `channel.py`, `client.py`, `event.py`, ...)
  - `pkg/gchatmeow/proto/` — `googlechat.proto` (57 KB) + generated `googlechat.pb.go` (887 KB)
  - `pkg/msgconv/` with `gchatfmt` and `matrixfmt` subpackages (incl. tests)
  Source: https://api.github.com/repos/mautrix/googlechat/git/trees/megabridge?recursive=1
- `go.mod`: module `go.mau.fi/mautrix-googlechat`, Go 1.23.0, deps `maunium.net/go/mautrix v0.23.3`, `go.mau.fi/util v0.8.6`, `google.golang.org/protobuf v1.36.6`, zerolog, x/net.
  Source: https://raw.githubusercontent.com/mautrix/googlechat/megabridge/go.mod
- Commit history: rewrite started ~**December 2024** (proto work, rich text, mentions, edits, deletions, typing, read receipts by contributor rnons), then Jan–Apr 2025 connector work by Skip R (bridge states, cookie login flow with COMPASS regex hint, timestamps/stream order, room capabilities). **Nothing since 2025-04-30.**
  Source: https://api.github.com/repos/mautrix/googlechat/commits?sha=megabridge&per_page=30
- Community assessment (issue #118 "Go Bridge?", opened 2026-05-21, still open, **no maintainer reply**): *"There is one at https://github.com/mautrix/googlechat/tree/megabridge but it is untested and hasn't been updated in a year"* (comment by NexusSfan, 2026-05-24).
  Sources: https://github.com/mautrix/googlechat/issues/118, https://api.github.com/repos/mautrix/googlechat/issues/118/comments
- Corroborating context: the mautrix April 2026 release blog discusses the Telegram Go release and says the Discord bridgev2 rewrite is still months out — googlechat isn't mentioned at all, i.e. it is not near the front of the official megabridge queue.
  Source: https://mau.fi/blog/2026-04-mautrix-release/
- Beeper's bridge tooling still ships googlechat as the **Python** bridge (`BridgeGoogleChat BridgeType = "googlechat"` with no `go` suffix, unlike `facebookgo`/`slackgo`/`discordgo` etc.).
  Sources: https://github.com/beeper/bridge-cd-tool/blob/main/bridges.go, https://github.com/beeper/bridge-manager

**Implication:** The foundation (proto definitions, pblite client port `gchatmeow`, bridgev2 connector skeleton, msgconv) exists under the same AGPL-3.0 repo. The gap is: untested, ~15 months stale, pinned to mautrix-go v0.23.3 (old), and it predates the 2026 Google-side breakages (image upload, login plugin). "Finish/rebase megabridge" is the obvious alternative to "rebuild".

### 2b. Other Go code touching the unofficial Google Chat (Dynamite) protocol

- **SnakeO/purple-googlechat-cli** ("gchat") — Go CLI for personal @gmail.com Google Chat: read/send/search/stream via *"the Dynamite protocol — the same internal protocol the Google Chat web client uses"*, with a **pblite codec in Go**, cookie auth (Chrome cookie extraction on macOS or manual DevTools paste). Explicitly built on the reverse-engineering and proto definitions of EionRobb's purple-googlechat (*"proto definitions explicitly licensed for any use"*). Created 2026-05-19, last push 2026-05-24, 0 stars. Proof that a working modern Go client against the current protocol is feasible; too new/small to depend on, but useful as a second reference implementation.
  Sources: https://api.github.com/search/repositories?q=purple-googlechat-cli, https://raw.githubusercontent.com/SnakeO/purple-googlechat-cli/master/README.md
- GitHub search `googlechat language:Go` finds nothing else relevant — only webhook senders (Zabbix alerting, message senders) and an Electron-wrapper desktop app. **No other Go Google Chat client library and no other Go Matrix bridge for Google Chat.**
  Source: https://api.github.com/search/repositories?q=googlechat+language:Go
- `pblite language:Go` search: only an unrelated, dead 2014 repo (jijinggang/pblite). The two real Go pblite implementations are in megabridge's `gchatmeow` and SnakeO's CLI.
  Source: https://api.github.com/search/repositories?q=pblite+language:Go
- Searching `googlechat` in orgs: **mautrix** org has only `mautrix/googlechat`; **beeper** org has zero googlechat repos (their Go work went into the megabridge branch upstream). No repo named `gchatmeow` exists outside that branch.
  Sources: https://api.github.com/search/repositories?q=googlechat+org:mautrix, https://api.github.com/search/repositories?q=googlechat+org:beeper, https://api.github.com/search/repositories?q=gchatmeow

## 3. Google Chat web-API changes / breakage since 0.5.2 (July 2024)

Open issues in mautrix/googlechat document three distinct post-2024 regressions (none fixed on master as of 2026-07-13):

1. **Login-cookie flow broke ~March 2026** — issue #115 "google login-cookie does not work anymore after march 2026" (opened 2026-03-05): bridge answers *"Those cookies don't seem to be valid."* Comment thread establishes the **root cause is the browser cookie-collection plugin/extension, not the protocol**: manually extracting cookies from a fresh Firefox private session and pasting them as JSON still logs in and bridges messages (comments by hary777 2026-03-06, sjcrookes 2026-05-13/26, ronnyaa 2026-05-18: *"only the plugin for the auth login-cookie is failing. doing it manually in a privacy tab...works"*). Some users (Cerothen, 2026-04-29) still fail even manually. **No maintainer response in the thread.**
   Sources: https://github.com/mautrix/googlechat/issues/115, https://api.github.com/repos/mautrix/googlechat/issues/115/comments
2. **Image/media upload broke ~February 2026** — issue #114 (opened 2026-02-06): every upload gets **HTTP 500 from Google's resumable-upload endpoint** (PUT in `upload_file()`), even <500 KB files, fresh session; text works. Reporter: *"It seems like the Google Chat API upload endpoint might have changed or is rejecting the request format."* Open, no fix.
   Source: https://github.com/mautrix/googlechat/issues/114
3. **Matrix→GChat formatting broke ~December 2025** — issue #110 (opened 2025-12-04): HTML formatting stripped Matrix→Google Chat (reverse direction fine). Also #107 (2025-10-21): DM backfill non-functional (and later Spaces too).
   Source: https://api.github.com/repos/mautrix/googlechat/issues?state=open (issues #110, #107)

Older chronic pain points that a rewrite must design around: cookie sessions expiring/logging out (#98, 2024-01-27; #93; #101 "Can't tell if I'm still logged in"), one-directional send failures (#102), Spaces support unmerged (PR #100, open since 2024-05-13).
Source: https://api.github.com/repos/mautrix/googlechat/issues?state=open

matrix.org's ecosystem page has no incident/status feed for this bridge; it just lists it as Beta (https://matrix.org/ecosystem/bridges/google_chat/).

## 4. Upstream hangups and protocol documentation

- **tdryer/hangups is dead.** Not formally archived (`archived: false`), but last push **2022-06-12**; issue #533 records that Google shut Hangouts down in November 2022, the API is gone, and the maintainer closed with *"Thank you to all hangups contributors and users for a great 8 years!"* — no Google Chat port planned.
  Sources: https://api.github.com/repos/tdryer/hangups, https://github.com/tdryer/hangups/issues/533
- The living descendants of hangups' protocol knowledge are:
  - **`maugclib`** inside mautrix/googlechat master — self-described *"'fork' of tdryer/hangups that uses Google Chat instead of Hangouts"*: `client.py`, `channel.py` (BrowserChannel long-poll), `pblite.py`, `googlechat.proto` (65 KB) + generated bindings. This is the de-facto protocol documentation for the Google Chat web-client ("Dynamite") channel: pblite-encoded protobufs over the web channel plus HTTP API calls, cookie auth (COMPASS, SSID, SID, OSID, HSID).
    Sources: https://api.github.com/repos/mautrix/googlechat/contents/maugclib, https://raw.githubusercontent.com/mautrix/googlechat/master/maugclib/README.md
  - **EionRobb/purple-googlechat** (libpurple/Pidgin plugin, GPL-3.0) — **actively maintained** (daily release "Daily 2026-06-20", 589 commits): C implementation with its own `googlechat.proto`, `googlechat_pblite.c/h`, and the same 5-cookie auth. Since it is receiving daily builds in 2026, its proto file is likely the most current public description of the protocol — useful for diffing against the (2024-era) protos in maugclib/megabridge to find what Google changed (e.g., the upload endpoint in issue #114). SnakeO's README claims its proto definitions are "explicitly licensed for any use."
    Sources: https://github.com/EionRobb/purple-googlechat, https://raw.githubusercontent.com/SnakeO/purple-googlechat-cli/master/README.md
- There is no official/other write-up of the channel protocol; hangups' own docs only ever said the API is *"undocumented and subject to change"* (https://github.com/tdryer/hangups).

## 5. Ban / crackdown risk

- **No account-ban reports attributable to the bridge were found.** A GitHub issue search in mautrix/googlechat for ban/banned/suspended/disabled returns only one unrelated Element Call issue (#109).
  Source: https://api.github.com/search/issues?q=repo:mautrix/googlechat+ban+OR+banned+OR+suspended+OR+disabled
- Web search surfaces generic Google Chat Community threads about consumer account suspensions (spam-related), none linking to unofficial clients:
  e.g. https://support.google.com/chat/thread/184081731, https://support.google.com/chat/thread/193196734 — no pattern implicating hangups/purple-googlechat/mautrix.
- The observed Google-side friction is **breakage, not enforcement**: upload endpoint 500s (#114), formatting changes (#110), and short-lived cookie sessions (#98/#101) — consistent with Google evolving the web client without deliberately hunting third-party clients. The continued daily releases of purple-googlechat through June 2026 suggest the protocol remains usable.
  Sources: https://github.com/mautrix/googlechat/issues/114, https://github.com/EionRobb/purple-googlechat

---

## Bottom line for the design decision

1. **Do not green-field.** `mautrix/googlechat@megabridge` already contains a Go bridgev2 connector + `gchatmeow` client + protos + msgconv, same repo/license, written largely by Beeper. It is untested and ~15 months stale, and there is no sign anyone is actively finishing it (no maintainer response to issue #118; not in the April 2026 roadmap blog). Finishing/rebasing it (or forking it) is the highest-leverage path — and coordinating with upstream (`#googlechat:maunium.net`) before building avoids duplicate work and AGPL friction.
2. The protocol layer needs **re-verification against 2026 Google Chat**: media upload (broken since Feb 2026) and the login-cookie flow (extension broken since Mar 2026; manual cookie paste still works). purple-googlechat's actively-maintained C code/proto is the best reference for current protocol behavior; SnakeO/purple-googlechat-cli is a small modern Go reference.
3. Ban risk appears low based on the public record, but session fragility (cookie expiry) is a recurring UX problem to design for.
