# Authentication

Google Chat has no interactive OAuth-style login for third-party clients.
This bridge logs in the same way most unofficial Google Chat clients do: by
taking a copy of five session cookies out of a browser that's already logged
into `https://chat.google.com`, and handing them to the bridge.

> [!WARNING]
> These five cookies are **full session credentials** for your Google
> account's web session — anyone who has them can access Google Chat (and
> potentially other Google services) as you, without needing your password or
> second factor. Treat them like a password:
>
> - Never share them with anyone, paste them into a public chat, or post them
>   in an issue tracker.
> - Never commit them to a config file, script, or git repository.
> - Only ever paste them directly into the bridge's own login prompt (in a
>   private DM with the bridge bot).
> - If you think they've leaked, sign out of that browser session (or change
>   your Google account password) to invalidate them.

## The cookies you need

The bridge needs exactly these five cookies, all read from the
`chat.google.com` domain (some are actually set on the parent `.google.com`
domain — see the per-browser instructions below):

- `COMPASS`
- `SSID`
- `SID`
- `OSID`
- `HSID`

## Step 1: Extract the cookies from your browser

Use a **private/incognito window**, and close it as soon as you have the
cookies:

1. Open a private/incognito browser window.
2. Log into [https://chat.google.com](https://chat.google.com) in it.
3. Extract the five cookies (per-browser instructions below).
4. **Close the private window** once you have them.

Why this matters: a browser session that stays open keeps rotating these
cookies as you use Google, invalidating the copies you gave the bridge -- this
is the most common reason bridge logins die early. A private window that you
close afterwards leaves the bridge as the only user of that session, which is
what both reference implementations of this protocol recommend. (Closing the
window does not log the bridge out; signing out explicitly does.)

### Chrome (or other Chromium-based browsers)

1. Open `https://chat.google.com` and make sure you're logged in.
2. Open DevTools (F12, or right-click → Inspect).
3. Go to the **Application** tab.
4. In the left sidebar, expand **Storage** → **Cookies**, and select
   `https://chat.google.com`.
5. Find each of the five cookies listed above in the table and copy its
   **Value** column. Note: `SSID`, `SID`, and `HSID` may show up listed under
   a parent domain like `.google.com` instead of `chat.google.com` — copy
   them from wherever DevTools shows them; the actual account they belong to
   is what matters, not which row groups them.

### Firefox

1. Open `https://chat.google.com` and make sure you're logged in.
2. Open DevTools (F12).
3. Go to the **Storage** tab.
4. In the left sidebar, expand **Cookies**, and select
   `https://chat.google.com`.
5. Find each of the five cookies listed above and copy its **Value** column,
   same caveat as above about some of them showing up under a broader
   `.google.com` scope.

## Step 2: Start the login flow

In a direct message with the bridge bot on your Matrix homeserver, send:

```
login
```

(If you're not in the bridge bot's management room — i.e. you're sending the
command somewhere else — prefix it with the bridge's command prefix, e.g.
`!gc login`.)

The bridge will reply with the login URL and ask you to paste your cookies.

## Step 3: Paste the cookies

Reply with a single JSON object mapping each cookie name to its value:

```json
{"COMPASS": "<value>", "SSID": "<value>", "SID": "<value>", "OSID": "<value>", "HSID": "<value>"}
```

All five keys are required, spelled exactly as above — the key names are
case-sensitive and uppercase. The bridge validates the values against Google
Chat immediately; your message is automatically redacted from the room after
you send it, since it contains credentials. Stray whitespace or quotes around
a pasted value are forgiven, and if a cookie is missing the bridge names
which one rather than guessing.

Alternatively — and slightly better — paste a full `curl` command copied from
your browser's devtools (Network tab, pick any request to `chat.google.com`,
right-click → Copy → **Copy as cURL**). The bridge extracts the five cookies
from its `Cookie:` header automatically, and also picks up your browser's
`User-Agent`, so the bridge presents itself to Google as the same browser
that created the session. On Windows, make sure to pick the **bash** variant
("Copy as cURL (bash)") — the cmd variant's quoting is not understood.

If the cookies are valid, the bridge confirms which Google account you
logged in as and starts syncing your chats.

## Known caveat: cookie logins can be short-lived

Because this is a browser-cookie login rather than a first-party OAuth
session, Google may invalidate these cookies sooner than it would invalidate
a real, continuously-used browser session — for example after a period of
bridge inactivity, an account security event, or a Google-side session
rotation the bridge doesn't happen to observe in time. If the bridge reports
that your login has expired or that cookies are no longer valid, simply
repeat steps 1–3 above with a fresh private window to log in again.

The single biggest thing you can do to make a session last is the private
window discipline in step 1: extract from a private window, then close it.
Cookies copied out of your everyday browser profile are rotated out from
under the bridge as you keep using Google there.
