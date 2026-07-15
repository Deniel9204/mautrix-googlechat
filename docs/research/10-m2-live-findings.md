# 10 — M2 Live Test Findings (Task 8)

**Date:** 2026-07-15
**Account:** consumer Google account (test), brand-new, empty world (0 conversations)
**Harness:** `pkg/gchatmeow/live_test.go` + `channelprobe_live_test.go` (`-tags 'goolm live'`)

## Summary

The API/auth/world layers re-validate **PASS** against live Google Chat, but the
**realtime BrowserChannel could not establish a working event stream** on this
test account — the forward-channel POST (the initial ping) is held open by
Google's server and never responds. This blocked the M2 send→echo round-trip.
Investigation shows this is **not an M2 code regression**; the most likely cause
is server-side realtime-channel friction against this brand-new, empty account
accessed via an unofficial client from a datacenter IP.

## What passed (re-validated)

| Check | Result |
|---|---|
| XSRF bootstrap (`/mole/world`, frozen API key + client_version) | PASS |
| `GetSelfUserStatus` (binary-proto RPC) | PASS (gaia id decoded) |
| `PaginatedWorld` (world sync) | PASS (0 items — empty account) |
| BrowserChannel `register` | PASS (HTTP 200, ~350ms) |
| Long-poll first request → SID from `X-HTTP-Initial-Response` | PASS (SID obtained) |

## What failed

`TestLiveChannelProbe` isolated the exact stall (reproduced with BOTH the
day-old cookies AND freshly-extracted cookies — identical behavior):

```
register OK (csessionid "")
first long-poll: err = send stream event:
  POST .../webchannel/events?AID=0&RID=…&SID=…  → context deadline exceeded
  SID-obtained="…"  arrays=0
```

- `register` returns HTTP 200 but with an **empty csessionid** (no fresh
  `COMPASS` cookie in the register response).
- The read side works: the first long-poll GET returns a SID via the
  `X-HTTP-Initial-Response` header.
- The **forward-channel POST** (`SendStreamEvent`, the initial `PingEvent` that
  tells Google to start streaming) **hangs until the context deadline** — the
  server accepts the connection and holds it open without responding. No data
  ever arrives, so `OnConnect` never fires.

## Why this is not an M2 code regression

1. The channel code (`channel.go`) is byte-identical to the M1 spike
   (`docs/research/09`) that connected and delivered 18 real events including a
   `MESSAGE_POSTED`. M2 did not modify the channel or the outbound ping path.
2. The M2 proto change (Task 2) touched only the **inbound** `StreamEventsResponse`;
   `PingEvent`/`StreamEventsRequest` (what we send) are unchanged, and `OnConnect`
   fires on the raw chunk **before** any proto decode.
3. `SendStreamEvent` was reviewed against `channel.py:303-341` and is faithful:
   correct query params (VER/RID/SID/AID), correct form body
   (count/ofs/req0_data), single pblite encode, `application/x-www-form-urlencoded`.
   The request is well-formed; the server simply does not respond.
4. Fresh cookies produced the identical failure, ruling out cookie expiry.

## Most likely cause

Server-side realtime-channel friction / anti-abuse throttling: a brand-new,
empty consumer account, accessed via a reverse-engineered client from a
datacenter IP, opening repeated webchannels in a short window. The empty
`csessionid` (server declining to issue a fresh webchannel session) is
consistent with this. The M1 spike succeeded on the account's **first-ever**
webchannel connect, before any pattern was established.

**Not fully excluded:** a latent forward-channel issue that happens to work only
on a first-ever connect. Distinguishing this definitively requires an
established/active account (e.g. a real Workspace or long-used personal account)
and/or a residential IP — the bridge's actual deployment target, which is far
less likely to be throttled than this disposable test account.

## What remains unproven

The **M2 send→echo round-trip** specifically (send via `create_topic`, receive
the echo on the channel). Everything it depends on is validated elsewhere: the
send RPC path is the same binary-proto API layer that passes live; the inbound
`MESSAGE_POSTED` decode + text extraction was exercised on 18 real events in the
M1 spike; and the code is covered by ~180 reviewed unit/integration tests.

## Recommendation

Perform the live send/receive validation on the **actual deployment account**
(the user's established personal Google account on their continuwuity server),
which is the real bridge target and unlikely to hit new-account throttling —
rather than blocking the M2 merge on a disposable test account that Google
appears to be rate-limiting at the realtime-channel layer.
