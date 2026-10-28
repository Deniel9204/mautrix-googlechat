# Authentication

Google Chat has no OAuth login for third-party clients, so the bridge logs in
with five cookies copied from a browser logged into `chat.google.com`:
`COMPASS`, `SSID`, `SID`, `OSID`, `HSID`.

> [!WARNING]
> These cookies are full session credentials: anyone holding them can use
> your Google account without your password or second factor. Paste them only
> into a private DM with the bridge bot, never into a public room, issue, config
> file or repository. If they leak, sign out of that browser session.

## 1. Copy the cookies

Use a private/incognito window and close it afterwards. A browser session
you keep using rotates the cookies and logs the bridge out; this is the most
common reason logins die early. Closing the window does not log the bridge
out.

1. Open a private window and log into https://chat.google.com.
2. Open DevTools (F12):
   - Chrome: **Application** → **Storage** → **Cookies** → `https://chat.google.com`
   - Firefox: **Storage** → **Cookies** → `https://chat.google.com`
3. Copy the **Value** of each of the five cookies. Some may be listed under
   `.google.com`; that's fine.
4. Close the private window.

## 2. Log in

Send `login` in a DM with the bridge bot (elsewhere use the command prefix,
e.g. `!gc login`), then reply with either:

- a JSON object (keys are case-sensitive):

  ```json
  {"COMPASS": "<value>", "SSID": "<value>", "SID": "<value>", "OSID": "<value>", "HSID": "<value>"}
  ```

- or, better, a request copied with DevTools → **Network** → any
  `chat.google.com` request → **Copy as cURL (bash)**. The bridge takes the
  cookies and your browser's User-Agent from it. On Windows pick the bash
  variant; the cmd one isn't understood.

The bridge redacts your message, checks the cookies with Google and confirms
which account you logged in as. If a cookie is missing it says which one.

## When the login expires

Cookie logins can be invalidated by Google earlier than a normal browser
session. If the bridge reports the login expired, repeat the steps above with
a fresh private window.
