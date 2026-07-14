---
name: capture-fixtures
description: Capture and sanitize real Google Chat wire fixtures (channel chunks, pblite payloads) from the test account into pkg/gchatmeow/testdata/. Use during M1+ when tests need real frames.
---

# Capture wire fixtures

1. Requires: the owner's TEST Google account logged into the bridge (never the main
   account), bridge built from a branch with debug dump enabled.
2. Run the bridge with `GCHAT_DEBUG_DUMP=<dir>` set (implemented in
   pkg/gchatmeow/channel.go). Every received chunk is written as
   `<dir>/chunk-<timestamp>-<n>.raw`; every decoded pblite array as `.json`.
3. Trigger the traffic you need from another device on the test account
   (send message, edit, react, etc).
4. **Sanitize before committing** — mandatory, check every file:
   - gaia IDs -> sequential fakes (100000000000000000001, ...)
   - emails -> user1@example.com, ... · display names -> "Test User N"
   - space/DM/message IDs -> "AAAA-fixture-N" style · SID/session tokens -> "SID-REDACTED"
   - message text -> lorem ipsum unless the text itself is the fixture
5. Keep the frame STRUCTURE byte-exact (lengths must still match after replacement —
   recompute the `<len>\n` prefix if content length changed).
6. Store under `pkg/gchatmeow/testdata/<area>/`, add a README line per fixture:
   what it captures, date, sanitized-by.
