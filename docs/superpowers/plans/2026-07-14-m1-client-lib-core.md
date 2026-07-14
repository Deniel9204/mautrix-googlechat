# M1 — Client Library Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. **Every porting task in this plan MUST use the `/port-module` skill and end with a `gchat-port-auditor` agent review.**

**Goal:** A working `pkg/gchatmeow` client library (auth, pblite, BrowserChannel, core RPCs) wired into the connector: cookie login succeeds, chat-list sync creates portals, the realtime channel survives disconnects — verified live against 2026-current Google Chat.

**Architecture:** Faithful Go port of `maugclib` (the Python bridge's embedded client library), structured as: session (cookies/XSRF) → api (binary-proto RPCs) → channel (pblite long-poll) → client (orchestration + supervision), with the connector layer translating client callbacks into bridgev2 events and states. Megabridge code is adopted only where `docs/research/08c` cleared it (api.go wire format, upload, login flow); everything it got wrong (channel lifecycle, cookie persistence, pblite permissiveness, event ordering) is rewritten from the Python original.

**Tech Stack:** Go stdlib `net/http` (no third-party HTTP), `google.golang.org/protobuf` (protoreflect for pblite), bridgev2 `LoginProcessCookies`, `simplevent`.

## Global Constraints

- Everything in the M0 plan's Global Constraints still applies (module path, pins, `-tags goolm`, layering, proto2, frozen IDs).
- **Protocol truth hierarchy**: (1) live behavior observed in the Task 13 spike, (2) `../_reference/googlechat-python/maugclib/` source, (3) `docs/research/01` + `02`, (4) megabridge code. When they disagree, higher wins; record disagreements in `docs/research/09-live-spike-findings.md`.
- Known megabridge defects that MUST NOT be replicated (from `docs/research/08c`): global 90s `http.Client.Timeout` killing long-polls; fire-and-forget `Listen` goroutine discarding terminal errors; chunk parser breaking on split multibyte UTF-8; all-or-nothing pblite decode; missing trailing-sparse-dict support; unordered observer goroutines; cookie rotation never persisted; `DownloadAttachment` dropping cookies on redirects.
- All Google Chat annotation/text offsets are UTF-16 code units. All GC timestamps are microseconds.
- The channel event callback must be **synchronous and ordered** — one goroutine, no fan-out.
- Every task: `gofmt`, `go vet -tags goolm`, `go test -tags goolm ./...` green before commit.

---

### Task 1: gchatmeow error taxonomy

**Files:**
- Create: `pkg/gchatmeow/errors.go`
- Test: `pkg/gchatmeow/errors_test.go`
- Port of: `$REF/googlechat-python/maugclib/exceptions.py` (85 lines — read it all first)

**Interfaces:**
- Produces (used by every later task):

```go
var (
	ErrNotLoggedIn             = errors.New("not logged in")            // /mole/world shows AccountsSignInUi
	ErrChannelLifetimeExpired  = errors.New("channel lifetime expired") // intentional 1.5h recycle
	ErrSIDExpiring             = errors.New("SID expiring")             // payload error -> re-register, no backoff
	ErrSIDInvalid              = errors.New("SID invalid")              // HTTP 400 "Unknown SID"
)

// NetworkError wraps transient transport failures (timeouts, conn resets).
type NetworkError struct{ Err error }
func (e *NetworkError) Error() string
func (e *NetworkError) Unwrap() error

// UnexpectedStatusError preserves the HTTP status (improvement over Python,
// mandated by docs/research/07 §3.3 item 6).
type UnexpectedStatusError struct {
	URL       string
	Status    int
	ErrorCode string // e.g. "invalid_grant" parsed from body if present
	Body      string // first 512 bytes
}
func (e *UnexpectedStatusError) Error() string

// IsAuthError reports whether err means the session is dead (401, or
// ErrorCode "invalid_grant", or ErrNotLoggedIn) -> connector maps to BAD_CREDENTIALS.
func IsAuthError(err error) bool
```

- [ ] **Step 1: Write failing tests** — `errors_test.go` with: `TestIsAuthError` (table: 401 UnexpectedStatusError → true; 500 → false; wrapped ErrNotLoggedIn via `fmt.Errorf("x: %w", ...)` → true; ErrorCode "invalid_grant" with status 400 → true; plain NetworkError → false) and `TestNetworkErrorUnwrap` (`errors.Is(&NetworkError{Err: context.DeadlineExceeded}, context.DeadlineExceeded)` → true).
- [ ] **Step 2: Run** `go test -tags goolm ./pkg/gchatmeow/` — expect FAIL (undefined).
- [ ] **Step 3: Implement** exactly the interface above. `IsAuthError`: `errors.Is(err, ErrNotLoggedIn)` OR (`errors.As` to `*UnexpectedStatusError` AND (`Status == 401` OR `ErrorCode == "invalid_grant"`)).
- [ ] **Step 4: Run tests** — PASS. **Step 5: Commit** `feat: gchatmeow error taxonomy`.

---

### Task 2: pblite codec

**Files:**
- Create: `pkg/gchatmeow/pblite/pblite.go`
- Test: `pkg/gchatmeow/pblite/pblite_test.go`
- Port of: `$REF/googlechat-python/maugclib/pblite.py` (176 lines) — spec in `docs/research/02` §6. Compare `$REF/gmessages/libgm/pblite/` (protoreflect prior art) and `$REF/googlechat-megabridge/pkg/gchatmeow/pblite*` before writing; adopt whichever core is closest, but the behaviors below are mandatory regardless.

**Interfaces:**

```go
package pblite

// Unmarshal decodes a pblite JSON array into msg. Permissive: unknown fields,
// nulls, and undecodable single values are skipped (debug-logged via zerolog),
// never returned as errors. Only structural failures (data isn't a JSON array)
// return an error.
func Unmarshal(data []byte, msg proto.Message) error

// Marshal encodes msg as a dense pblite JSON array (nulls for unset fields).
// int64/uint64 fields are emitted as JSON strings (matching the JS client).
func Marshal(msg proto.Message) ([]byte, error)
```

Mandatory decode behaviors (each is a test): array index i ↔ field number i+1; a trailing JSON **object** maps `"fieldNumber" -> value` for sparse high fields (hit in practice by `Message.reply_to` = field 37); int64/uint64 accept **both** JSON string and number; `bytes` fields are base64 strings; nested messages recurse; repeated fields are nested arrays; enums accept numbers; oneof = last-set-wins; `null` skips.

- [ ] **Step 1: Write failing tests** (`pblite_test.go`):

```go
package pblite_test

import (
	"strings"
	"testing"

	gproto "google.golang.org/protobuf/proto"

	"github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/pblite"
	pb "github.com/Deniel9204/mautrix-googlechat/pkg/gchatmeow/proto"
)

func s(v string) *string { return &v }
func i64(v int64) *int64 { return &v }

func TestRoundTrip(t *testing.T) {
	// GroupId with nested SpaceId — field-number independent round-trip.
	in := &pb.GroupId{SpaceId: &pb.SpaceId{SpaceId: s("AAAA-fixture-1")}}
	data, err := pblite.Marshal(in)
	if err != nil { t.Fatal(err) }
	var out pb.GroupId
	if err := pblite.Unmarshal(data, &out); err != nil { t.Fatal(err) }
	if !gproto.Equal(in, &out) { t.Fatalf("round trip mismatch: %s vs %s", in, &out) }
}

func TestInt64AsString(t *testing.T) {
	in := &pb.Message{CreateTime: i64(1700000000000000)}
	data, _ := pblite.Marshal(in)
	// Marshal must emit int64 as string (JS compat).
	if !strings.Contains(string(data), `"1700000000000000"`) {
		t.Fatalf("int64 not emitted as string: %s", data)
	}
	// Unmarshal must accept both string and number forms.
	var out pb.Message
	if err := pblite.Unmarshal(data, &out); err != nil { t.Fatal(err) }
	if out.GetCreateTime() != 1700000000000000 { t.Fatal("string int64 not decoded") }
	numeric := strings.Replace(string(data), `"1700000000000000"`, `1700000000000000`, 1)
	var out2 pb.Message
	if err := pblite.Unmarshal([]byte(numeric), &out2); err != nil { t.Fatal(err) }
	if out2.GetCreateTime() != 1700000000000000 { t.Fatal("numeric int64 not decoded") }
}

func TestTrailingSparseDict(t *testing.T) {
	// Build a message with a high-numbered field (Message.reply_to = 37),
	// marshal densely, then convert the tail to the server's sparse-dict form.
	in := &pb.Message{ReplyTo: &pb.SendReplyTarget{CreateTime: i64(42)}}
	dense, _ := pblite.Marshal(in)
	var out pb.Message
	if err := pblite.Unmarshal(dense, &out); err != nil { t.Fatal(err) }
	if out.GetReplyTo().GetCreateTime() != 42 { t.Fatal("dense high-field decode failed") }

	// Sparse form: array shorter than 37 entries, trailing object keyed by field number.
	sparse := []byte(`[null, {"37": ` + string(mustMarshalField(t, in)) + `}]`)
	var out2 pb.Message
	if err := pblite.Unmarshal(sparse, &out2); err != nil { t.Fatal(err) }
	if out2.GetReplyTo().GetCreateTime() != 42 { t.Fatal("sparse dict decode failed") }
}

// mustMarshalField marshals just the SendReplyTarget submessage as pblite.
func mustMarshalField(t *testing.T, m *pb.Message) []byte {
	data, err := pblite.Marshal(m.GetReplyTo())
	if err != nil { t.Fatal(err) }
	return data
}

func TestPermissiveness(t *testing.T) {
	// Unknown trailing fields, nulls, and garbage values must not error.
	for _, data := range []string{
		`[null, null, null, null, null, null, null, null, null, null, null, null, "unknown-tail", ["extra"]]`,
		`[]`,
		`[{"9999": "x"}]`,
	} {
		var out pb.GroupId
		if err := pblite.Unmarshal([]byte(data), &out); err != nil {
			t.Fatalf("Unmarshal(%s) errored: %v — must skip, never fail", data, err)
		}
	}
	// Structural garbage DOES error.
	if err := pblite.Unmarshal([]byte(`{"not":"array"}`), &pb.GroupId{}); err == nil {
		t.Fatal("non-array should error")
	}
}
```

Note: field names (`CreateTime`, `ReplyTo`) must match the generated code — check `pkg/gchatmeow/proto/googlechat.pb.go` and adjust accessor names if generation differs. If `Message.reply_to` is not field 37 in the generated descriptor (`proto.GetDescriptor` check), find the real number with protoreflect and fix the sparse test's key.

- [ ] **Step 2: Run** — FAIL. **Step 3: Implement** via protoreflect walk (`msg.ProtoReflect().Descriptor().Fields()`; for decode iterate array entries, map index→field, `protoreflect.Value` conversion per kind with permissive fallbacks; for the trailing dict detect `map[string]json.RawMessage` as last element; for encode iterate fields by number into a `[]any` sized to max field number, then `json.Marshal`). **Step 4: Run** — PASS. **Step 5:** `go vet`, commit `feat: permissive pblite codec`.

---

### Task 3: Cookie jar + HTTP session

**Files:**
- Create: `pkg/gchatmeow/cookies.go`, `pkg/gchatmeow/session.go`
- Test: `pkg/gchatmeow/session_test.go`
- Port of: `$REF/googlechat-python/maugclib/http_utils.py` (273 lines) — spec `docs/research/01` (session section).

**Interfaces:**

```go
// RequiredCookies are the cookies the login UI must collect, on domain chat.google.com.
var RequiredCookies = []string{"COMPASS", "SSID", "SID", "OSID", "HSID"}

// Session is the authenticated HTTP layer. It owns cookies and the UA.
type Session struct { /* http.Client with custom jar, ua string, mu */ }

func NewSession(cookies map[string]string, userAgent string) (*Session, error)

// Fetch performs a request with auth cookies; retries transient errors
// (maxRetries=3, exponential backoff) like Python's Session.fetch.
// Long-poll callers use FetchRaw with their own context instead.
func (s *Session) Fetch(ctx context.Context, method, url string, headers http.Header, body []byte) (*Response, error)

// FetchRaw performs a request and returns the raw *http.Response without
// reading the body and WITHOUT any client-level timeout — the caller owns
// cancellation via ctx. Used by the channel long-poll.
func (s *Session) FetchRaw(ctx context.Context, method, url string, headers http.Header, form url.Values) (*http.Response, error)

// Cookies returns the CURRENT values of RequiredCookies (post-rotation),
// for persistence into UserLoginMetadata.
func (s *Session) Cookies() map[string]string
```

Mandatory behaviors (each a test where feasible): cookie values sent **unquoted** even if they contain commas/spaces (custom jar — Go's `net/http` quotes some values; verify and work around by setting the `Cookie` header manually from the jar); rotated `Set-Cookie` values from any `*.google.com` response are stored and visible via `Cookies()`; cookies are **only** sent to hosts matching `*.google.com` / `*.googleusercontent.com`; `Connection: Keep-Alive` forced; **no** `http.Client.Timeout` on the client used by FetchRaw (megabridge's fatal bug); TLS verification ON (Python's `ssl=False` explicitly not replicated).

- [ ] **Step 1: Write failing tests** with `httptest.NewServer`: `TestCookiesSentUnquoted` (server echoes `Cookie` header; set a cookie value with a space, assert raw value arrives — use the test server's host mapped via a custom `http.Transport` DialContext or make host checks injectable: give `NewSession` an unexported `allowedHostSuffixes []string` field defaulted to google domains, overridable in tests); `TestCookieRotationReadback` (server sets `Set-Cookie: SID=rotated2; Domain=...`, assert `Cookies()["SID"] == "rotated2"`); `TestCookiesNotSentCrossDomain` (second httptest server outside the allowlist gets no Cookie header); `TestFetchRetries` (server fails twice with 502 then succeeds; Fetch returns success; 3 requests observed); `TestNoClientTimeout` (assert `s.pollClient.Timeout == 0` — direct field check with a comment referencing megabridge defect).
- [ ] **Step 2: Run** — FAIL. **Step 3: Implement** (port `http_utils.py` behaviors; keep two `http.Client`s: `apiClient` for Fetch, timeout-less `pollClient` for FetchRaw, sharing one jar). **Step 4: Run** — PASS. **Step 5: Audit** — dispatch `gchat-port-auditor` with (`pkg/gchatmeow/cookies.go`, `session.go`) vs `maugclib/http_utils.py`. Fix findings. **Step 6: Commit** `feat: gchatmeow session with rotating cookie jar`.

---

### Task 4: Auth bootstrap — /mole/world scrape + XSRF

**Files:**
- Create: `pkg/gchatmeow/auth.go`
- Test: `pkg/gchatmeow/auth_test.go`
- Port of: `maugclib/client.py` XSRF/wiz-scrape sections (grep `SMqcke`, `qwAQke`, `mole/world` in client.py) — spec `docs/research/01` §1.

**Interfaces:**

```go
// FetchXSRFToken GETs https://chat.google.com/u/0/mole/world, detects
// logged-out state, and extracts the XSRF token.
// Returns ErrNotLoggedIn when the WIZ data contains qwAQke == "AccountsSignInUi".
// The token is sent on API calls as the x-framework-xsrf-token header and
// must be refreshed every 24h and after any 400/401 API response.
func (s *Session) FetchXSRFToken(ctx context.Context) (string, error)
```

- [ ] **Step 1: Write failing tests** with httptest serving two captured-shape HTML bodies (inline string constants in the test, minimal WIZ blobs — copy the exact key patterns from client.py's regexes): logged-in page → token extracted; signin page → `ErrNotLoggedIn`; garbage page → descriptive error.
- [ ] **Step 2–4:** FAIL → implement (copy the regex/JSON-walk from client.py; make base URL injectable for tests) → PASS.
- [ ] **Step 5: Audit + commit** `feat: XSRF bootstrap and logged-out detection`.

---

### Task 5: API layer — binary-proto RPC plumbing + 16 wrappers

**Files:**
- Create: `pkg/gchatmeow/api.go`
- Test: `pkg/gchatmeow/api_test.go`
- Adopt from: `$REF/googlechat-megabridge/pkg/gchatmeow/api.go` (cleared by `docs/research/08c` — wire format verified). Cross-check every endpoint against `maugclib/client.py`'s 16 `proto_*` methods (table in `docs/research/01` §3).

**Interfaces:**

```go
// api.go: one exported method per RPC, all following this shape:
func (c *Client) PaginatedWorld(ctx context.Context, req *pb.PaginatedWorldRequest) (*pb.PaginatedWorldResponse, error)
func (c *Client) GetSelfUserStatus(ctx context.Context, req *pb.GetSelfUserStatusRequest) (*pb.GetSelfUserStatusResponse, error)
func (c *Client) GetGroup(ctx context.Context, req *pb.GetGroupRequest) (*pb.GetGroupResponse, error)
func (c *Client) GetMembers(ctx context.Context, req *pb.GetMembersRequest) (*pb.GetMembersResponse, error)
// ... all 12 megabridge RPCs, PLUS the 4 it lacks (needed by M6 but stubbed cheap now):
func (c *Client) ListTopics(ctx context.Context, req *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error)
func (c *Client) ListMessages(ctx context.Context, req *pb.ListMessagesRequest) (*pb.ListMessagesResponse, error)
func (c *Client) CatchUpUser(ctx context.Context, req *pb.CatchUpUserRequest) (*pb.CatchUpUserResponse, error)
func (c *Client) CatchUpGroup(ctx context.Context, req *pb.CatchUpGroupRequest) (*pb.CatchUpGroupResponse, error)
```

**This task creates the `Client` struct** in api.go with only the fields the RPC layer needs (`session *Session`, `xsrfToken string` + mutex, `requestCounter atomic.Int64`, `baseURL string` for tests). Task 8 adds the channel/callback/supervision fields to the same struct in client.go (methods across files in one package).

Wire format (verify against megabridge api.go while adopting): `POST https://chat.google.com/u/0/api/{endpoint}?c=<counter>&rt=b&alt=proto&key=<APIKey>`; request body = binary proto; headers `Content-Type: application/x-protobuf`, `x-framework-xsrf-token: <token>`, `X-Goog-Encode-Response-If-Executable: base64`; response = binary proto, but **detect base64-encoded responses** (open question from research: if the body isn't valid proto, try base64-decoding first — implement the dual path). Copy the API key and `client_version=2440378181258` constants from `maugclib/client.py` verbatim into a `constants.go` block with a comment that the spike (Task 13) validates them. Non-200 responses → `*UnexpectedStatusError` (keep the status — improvement over Python, mandated by research 07).

- [ ] **Step 1: Write failing tests**: httptest server asserting method/query/headers/body-proto round-trip for `GetSelfUserStatus` (fake response proto marshaled binary), a base64-encoded response variant, and a 401 → `IsAuthError(err) == true`.
- [ ] **Step 2–4:** FAIL → adopt/extend megabridge api.go (rename to match interface above; thread `ctx`; use `Session.Fetch`) → PASS.
- [ ] **Step 5: Audit** vs `client.py` proto_* table (all 16 present, endpoints spelled identically). **Commit** `feat: binary-proto RPC layer with 16 wrappers`.

---

### Task 6: Chunk parser — UTF-16 code-unit framing

**Files:**
- Create: `pkg/gchatmeow/chunkparser.go`
- Test: `pkg/gchatmeow/chunkparser_test.go`
- Port of: `maugclib/channel.py` `ChunkParser` — spec `docs/research/01` §2. Megabridge's version is DEFECTIVE (splits multibyte runes) — reference only the Python.

**Interfaces:**

```go
// ChunkParser incrementally decodes the channel's framing:
// "<length>\n<payload>" where length counts UTF-16 CODE UNITS of the payload
// (JS String.length semantics), and the byte stream is UTF-8 that may be
// split at arbitrary byte boundaries (including mid-rune) between Feed calls.
type ChunkParser struct{ /* buf []byte */ }

// Feed appends raw bytes and returns all complete payloads decoded so far.
func (p *ChunkParser) Feed(data []byte) []string
```

Algorithm (must match Python `get_chunks`): buffer bytes; repeatedly: find `\n` in the *decodable prefix*; parse the integer before it; then consume UTF-8 runes counting UTF-16 units (runes > 0xFFFF count as 2) until the count is reached — if the buffer ends mid-payload or mid-rune, keep remainder for the next Feed.

- [ ] **Step 1: Write failing tests**:

```go
func TestSimpleChunk(t *testing.T)        // "5\nhello" -> ["hello"]
func TestMultipleChunksOneFeed(t *testing.T) // "2\nhi3\nfoo" -> ["hi","foo"]
func TestChunkSplitAcrossFeeds(t *testing.T) // "5\nhel" + "lo" -> [], ["hello"]
func TestLengthSplitAcrossFeeds(t *testing.T) // "1" + "1\nhello world" -> [], ["hello world"]
func TestAstralCharCountsAsTwo(t *testing.T)  // payload "😀a" has UTF-16 length 3: "3\n😀a" -> ["😀a"]
func TestMultibyteSplitMidRune(t *testing.T)  // feed "2\n" + first 2 bytes of "😀"(4 bytes)... then rest:
                                              // Feed("2\n\xf0\x9f") -> []; Feed("\x98\x80") -> ["😀"]
func TestBMPNonASCII(t *testing.T)            // "é" = 1 UTF-16 unit, 2 UTF-8 bytes: "2\néa" -> ["éa"]
```

- [ ] **Step 2–4:** FAIL → implement → PASS. **Step 5: Audit + commit** `feat: UTF-16 code-unit chunk parser`.

---

### Task 7: BrowserChannel — register, long-poll, error ladder

**Files:**
- Create: `pkg/gchatmeow/channel.go`
- Test: `pkg/gchatmeow/channel_test.go`
- Port of: `maugclib/channel.py` (495 lines) **line-by-line** — this is the highest-fidelity-required module (research 07 risk #2). Spec: `docs/research/01` §2. Use `/port-module`; budget the behavior inventory before coding.

**Interfaces:**

```go
type Channel struct { /* session, sid, aid, ofs, parser, callbacks */ }

func NewChannel(session *Session) *Channel

// OnReceiveArray is called synchronously, in order, for each decoded pblite
// array from the stream. Set before Listen.
OnReceiveArray func(ctx context.Context, arr []byte) error
// OnConnect is called when the channel becomes live (first chunk received).
OnConnect func(ctx context.Context)

// Listen runs the register + long-poll loop until ctx is cancelled or a
// terminal error occurs. IT RETURNS ITS ERROR — the caller (client.go
// supervision) decides what to do. Never wrapped in a fire-and-forget goroutine.
func (ch *Channel) Listen(ctx context.Context, maxRetries int, retryBackoffBase time.Duration) error

// SendStreamEvent sends an outbound event (PingEvent) on the forward channel.
func (ch *Channel) SendStreamEvent(ctx context.Context, ev *pb.StreamEventsRequest) error
```

Choreography to port exactly (verify each against channel.py, cite lines in code comments):
1. **Register**: POST to the register endpoint (copy URL constants from channel.py) with `SID=null, CVER=22` and form `$req=count=1&ofs=0&req0_data=%5B%5D`; read new SID from the `X-HTTP-Initial-Response` header.
2. **Post-register ack**: GET with `RID=rpc&AID=0` (channel.py:429-431 — "required, unclear why").
3. **Initial ping**: send `PingEvent{state: ACTIVE, application_focus_state: FOCUS_STATE_FOREGROUND, client_interactive_state: INTERACTIVE}` — without it events never flow.
4. **Long-poll**: GET stream with SID + AID tracking (update AID from received arrays; `ofs` counter on sends); feed body bytes through ChunkParser → each chunk is a JSON array of `[aid, payload]` pairs (port `_parse_sid_response`/array handling exactly); 60-second **read-idle** timeout (no bytes for 60s → treat as NetworkError; implement by wrapping resp.Body reads with a resettable timer, NOT a request deadline).
5. **Error ladder** (Listen's internal loop):
   - payload/parse error mid-stream → return `ErrSIDExpiring` semantics: re-register immediately, retries NOT incremented (map Go-side errors: `io.ErrUnexpectedEOF` during chunked read ≈ aiohttp `ClientPayloadError`);
   - HTTP 400 with "Unknown SID" in body → `ErrSIDInvalid` → propagate to caller (client triggers resync, limit 3);
   - clean EOF after ≥1 chunk (server's ~hourly close) → re-poll with same SID, reset retry counter;
   - connect/timeout errors → exponential backoff `retryBackoffBase * 2^n` up to maxRetries, then return wrapped NetworkError;
   - 401 anywhere → return `*UnexpectedStatusError` (auth) immediately.
6. **1.5h lifetime recycle**: Listen tracks channel age; at 90min it returns `ErrChannelLifetimeExpired` (caller reconnects silently). Python has this in user.py supervision — we put the timer in Listen for cohesion; document the move.
7. **Debug dump**: if `GCHAT_DEBUG_DUMP` env var is set, write every received chunk to `$GCHAT_DEBUG_DUMP/chunk-<unixnano>.raw` and every decoded array to `.json` (feeds `/capture-fixtures`).

- [ ] **Step 1: Write failing tests** using an `httptest` fake channel server (helper in the test file: register handler returning `X-HTTP-Initial-Response`, scripted stream responses): `TestRegisterExtractsSID`, `TestAckAndPingSentAfterRegister` (server records request order: register → ack GET with RID=rpc&AID=0 → forward-channel ping), `TestEventsDeliveredInOrder` (stream 3 chunks; OnReceiveArray receives 3 in order, same goroutine — assert via goroutine ID or sequence check), `TestCleanEOFRepolls` (server closes after chunk; next poll arrives with same SID; retry counter reset — observable via no backoff delay), `TestUnknownSIDPropagates` (400 "Unknown SID" body → Listen returns error matching `ErrSIDInvalid`), `TestBackoffOnConnectError` (server refuses twice; fake clock or measure ≥ backoff; then succeeds), `TestLifetimeRecycle` (inject short lifetime via unexported field; Listen returns `ErrChannelLifetimeExpired`).
- [ ] **Step 2–4:** FAIL → port → PASS. Keep `channel.py` line references as comments on each ported behavior.
- [ ] **Step 5: Audit** (this is the audit that matters most — auditor must check all 7 choreography points + the megabridge-defect list). **Commit** `feat: BrowserChannel long-poll with full error ladder`.

---

### Task 8: Client orchestration — connect, split_event_bodies, supervision

**Files:**
- Create: `pkg/gchatmeow/client.go`
- Test: `pkg/gchatmeow/client_test.go`
- Port of: `maugclib/client.py` (connect/orchestration parts) + supervision policy from `mautrix_googlechat/user.py` (spec: research 01 §2, 03 §login/connection, 07 §6 table).

**Interfaces:**

```go
type Client struct { /* Session, Channel, xsrf token + refresh timer, counters */ }

type ClientOpts struct {
	Cookies   map[string]string
	UserAgent string
}

func NewClient(opts ClientOpts) (*Client, error)

// Callbacks (set before Connect):
OnStreamEvent   func(ctx context.Context, ev *pb.Event)        // one call per flattened body
OnConnectionState func(state ConnState, err error)             // CONNECTED / TRANSIENT / BAD_CREDENTIALS / FATAL

type ConnState int
const (
	ConnStateConnected ConnState = iota
	ConnStateTransient
	ConnStateBadCredentials
	ConnStateFatal
)

// Connect starts the supervision loop (blocking until ctx cancel or fatal):
// xsrf → channel Listen → on error consult the ladder → reconnect/backoff/stop.
func (c *Client) Connect(ctx context.Context) error
func (c *Client) Disconnect()

// Cookies returns current (rotated) cookie values for persistence.
func (c *Client) Cookies() map[string]string
```

Supervision mapping (Connect's loop, from research 07 §6 — each row a code branch):
`ErrChannelLifetimeExpired` → reconnect silently, no state change · `ErrSIDExpiring` → re-register (Channel does internally; if surfaced, reconnect immediately) · `ErrSIDInvalid` → resync counter++ (≤3) then reconnect; >3 → Fatal · `IsAuthError(err)` → OnConnectionState(BadCredentials), return · NetworkError → Transient state + backoff reconnect · ctx cancelled → clean return.

`split_event_bodies` (port from client.py): a received `Event` may carry `body` (field 4) plus repeated `bodies` (field 8); yield one `*pb.Event` per body with the parent's group_id/other fields copied — exact python semantics; unit-test with a hand-built Event containing 1 body + 2 bodies → 3 callbacks.

The received pblite arrays from Channel decode into `StreamEventsResponse` (via pblite.Unmarshal) — extract `.event`, run split, dispatch synchronously.

- [ ] **Step 1: Failing tests**: `TestSplitEventBodies` (as above), `TestSupervisionLadder` (inject fake Channel via interface: script Listen to return each ladder error; assert state callbacks + reconnect counts), `TestXSRFRefreshOn401` (script api 401 → next call refetches token).
- [ ] **Step 2–4:** FAIL → implement → PASS. **Step 5: Audit + commit** `feat: client orchestration with supervision ladder`.

---

### Task 9: GroupId/time helpers

**Files:**
- Create: `pkg/gchatmeow/ids.go`
- Test: `pkg/gchatmeow/ids_test.go`
- Port of: `maugclib/parsers.py` (49 lines — trivial).

**Interfaces:**

```go
// GroupIDToString converts *pb.GroupId to the canonical string form used by
// gcid ("dm:<id>" / "space:<id>" WITHOUT prefix here — returns id + isDM).
func GroupIDToParts(gid *pb.GroupId) (id string, isDM bool, ok bool)
func PartsToGroupID(id string, isDM bool) *pb.GroupId
// MicrosToTime / TimeToMicros: µs <-> time.Time.
```

- [ ] **Steps:** failing tests (round-trip both kinds, nil-safe: `GroupIDToParts(nil)` → ok=false) → implement → PASS → commit `feat: group ID and time helpers`.

---

### Task 10: Connector login — cookie flow + persistence

**Files:**
- Modify: `pkg/connector/connector.go` (CreateLogin), `pkg/connector/client.go` (constructor wiring)
- Create: `pkg/connector/login.go`
- Test: `pkg/connector/login_test.go`
- Adopt from: `$REF/googlechat-megabridge/pkg/connector/login.go` (cleared by 08b/08c: flow shape, COMPASS regex hint, per-cookie domains) **plus** the fix it lacks: persist cookies into `UserLoginMetadata` and re-persist after connect.

**Interfaces:**

```go
type GChatLogin struct {
	User *bridgev2.User
	Main *GChatConnector
}
var _ bridgev2.LoginProcessCookies = (*GChatLogin)(nil)

func (gl *GChatLogin) Start(ctx context.Context) (*bridgev2.LoginStep, error)
// -> LoginStep{Type: LoginStepTypeCookies, StepID: "fi.mau.googlechat.cookies", CookiesParams: &bridgev2.LoginCookiesParams{
//      URL: "https://chat.google.com/", Fields: [5 cookie fields, domain chat.google.com,
//      COMPASS with a regex hint matching the dynamite= prefix — copy exact field defs from megabridge login.go]}}
func (gl *GChatLogin) SubmitCookies(ctx context.Context, cookies map[string]string) (*bridgev2.LoginStep, error)
// -> build gchatmeow.Client; FetchXSRFToken (ErrNotLoggedIn -> user-friendly error);
//    GetSelfUserStatus -> gaia ID; user.NewLogin(ctx, &database.UserLogin{
//      ID: gcid.MakeUserLoginID(gaiaID), Metadata: &UserLoginMetadata{Cookies: client.Cookies(), UserAgent: ua}},
//      &bridgev2.NewLoginParams{DeleteOnConflict: true, DontReuseExisting: false});
//    hand the WARM client to the login's GChatClient and Connect in a goroutine;
//    return LoginStep{Type: LoginStepTypeComplete}
func (gl *GChatLogin) Cancel()
```

- [ ] **Step 1: Failing test**: `TestLoginStartStepShape` (Start returns cookies step with exactly the 5 RequiredCookies fields, correct domain+URL). SubmitCookies is integration-tested in the spike; unit-test only the error path with an unreachable base URL → error mentions login failure, no login row created (use an in-memory bridge test harness if `$REF/mautrix-go/bridgev2/bridgetest` exists; otherwise assert via mocked matrix connector — check how `$REF/meta` tests this and copy the harness approach).
- [ ] **Step 2–4:** implement → PASS. Wire `CreateLogin` to return `&GChatLogin{User: user, Main: gc}` for flow "cookies".
- [ ] **Step 5: Audit** vs megabridge login.go + research 03 (login section). **Commit** `feat: cookie login flow with metadata persistence`.

---

### Task 11: Connector client — Connect wiring + bridge states

**Files:**
- Modify: `pkg/connector/client.go` (replace all M0 stubs except HandleMatrixMessage/GetCapabilities), `pkg/connector/connector.go` (LoadUserLogin builds real client)
- Create: `pkg/connector/bridgestate.go`
- Test: `pkg/connector/bridgestate_test.go`

**Interfaces:**

```go
// bridgestate.go
const (
	GChatNotImplemented   status.BridgeStateErrorCode = "gchat-not-implemented"
	GChatBadCredentials   status.BridgeStateErrorCode = "gchat-bad-credentials"
	GChatTransientDisconnect status.BridgeStateErrorCode = "gchat-transient-disconnect"
)
// init() registers human messages via status.BridgeStateHumanErrors.update pattern —
// copy the exact mechanism from $REF/meta/pkg/connector (grep BridgeStateHumanErrors).

// client.go additions
func (c *GChatClient) Connect(ctx context.Context)  // builds gchatmeow client from
// UserLoginMetadata (cookies+UA), sets callbacks:
//   OnConnectionState -> BridgeState.Send mapping (Connected -> StateConnected,
//     Transient -> StateTransientDisconnect, BadCredentials -> StateBadCredentials + code),
//   OnStreamEvent -> handleGChatEvent (Task 12 consumes; in this task: log-only stub
//     named handleGChatEvent in events.go so Task 12 only fills the body)
// then go c.client.Connect(ctx) with the error surfaced through OnConnectionState.
// After reaching Connected: persist rotated cookies (metadata update + login.Save(ctx)).
func (c *GChatClient) Disconnect()               // client.Disconnect()
func (c *GChatClient) IsLoggedIn() bool          // cached: last conn state == Connected
func (c *GChatClient) LogoutRemote(ctx context.Context) // best-effort; clear metadata cookies
func (c *GChatClient) IsThisUser(_ context.Context, userID networkid.UserID) bool
// userID == gcid.MakeUserID(<own gaia from login ID>)
```

- [ ] **Step 1: Failing tests**: state-mapping table test (fake gchatmeow client via interface; each ConnState → expected BridgeState event + error code), cookie-persistence test (after simulated Connected callback, UserLoginMetadata.Cookies updated from client.Cookies()).
- [ ] **Step 2–4:** implement → PASS. **Step 5: Commit** `feat: connect lifecycle with bridge states and cookie persistence`.

---

### Task 12: Chat-list sync — world sync, ChatResync, chat/user info

**Files:**
- Create: `pkg/connector/sync.go`, `pkg/connector/chatinfo.go`, `pkg/connector/userinfo.go`, `pkg/connector/events.go` (fills the Task 11 stub)
- Test: `pkg/connector/chatinfo_test.go`
- Port of: `mautrix_googlechat/user.py:610` sync logic (spec: research 03 §chat-sync, 07 §1.2 rows "Chat info", "Ghost info", "Chat-list sync").

**Interfaces:**

```go
// sync.go — called from Connect after Connected state:
func (c *GChatClient) syncChats(ctx context.Context)
// PaginatedWorld RPC -> sort items by sort_timestamp desc -> for the newest
// Config.InitialChatSync items emit simplevent.ChatResync{
//   EventMeta: simplevent.EventMeta{Type: bridgev2.RemoteEventChatResync,
//     PortalKey: gcid.MakePortalKey(group, c.UserLogin.ID), CreatePortal: true},
//   ChatInfo: chatInfoFromWorldItem(item), LatestMessageTS: <from sort_timestamp>}
// -> c.UserLogin.QueueRemoteEvent. Older items: same event with CreatePortal false.

// chatinfo.go
func (c *GChatClient) GetChatInfo(ctx context.Context, portal *bridgev2.Portal) (*bridgev2.ChatInfo, error)
// ParsePortalID -> GetGroup RPC -> wrap: Name (spaces only), Members{MemberMap,
// OtherUserID for DMs from read_state/dm_members}, Type (DM/Space),
// PortalMetadata updates (ThreadsOnly/ThreadsEnabled from flat_threads_enabled etc.)
func chatInfoFromWorldItem(item *pb.WorldItemLite) *bridgev2.ChatInfo // lighter shape, same rules

// userinfo.go
func (c *GChatClient) GetUserInfo(ctx context.Context, ghost *bridgev2.Ghost) (*bridgev2.UserInfo, error)
// GetMembers RPC (batch of 1) -> UserInfo{Name: displayname template over
// (name -> first+last -> email fallback chain), Identifiers: ["mailto:<email>"],
// Avatar: wrapped avatar URL, IsBot: user type BOT}

// events.go
func (c *GChatClient) handleGChatEvent(ctx context.Context, ev *pb.Event)
// M1 scope: only GROUP_VIEWED/no-op logging + a switch skeleton with one case per
// event body type from research 02 §3, each case logging "unhandled (M2+)" at debug.
// M2 fills the cases. NO business logic here yet.
```

- [ ] **Step 1: Failing tests**: `chatInfoFromWorldItem` table test (DM with 2 members → OtherUserID set + receiver-scoped key; space → name set, no OtherUserID; threads flags land in PortalMetadata), displayname fallback chain test (name → first+last → email → "Unknown user" through Config.FormatDisplayname).
- [ ] **Step 2–4:** implement → PASS. **Step 5: Audit** vs user.py sync (initial_chat_sync cap honored, sort order, blocked/hidden groups skipped — copy the skip conditions from user.py). **Commit** `feat: chat-list sync and chat/user info`.

---

### Task 13: Live protocol validation spike (with owner)

**Files:**
- Create: `docs/research/09-live-spike-findings.md`
- Possibly modify: any client-lib file where live behavior diverges

This task needs the owner (test Google account + continuwuity). Prepare everything so their time is minimal.

- [ ] **Step 1: Prepare** — build binary, write a step-by-step runbook into the PR/message: config, registration, `login` command flow, where to paste cookies from DevTools (Application → Cookies → chat.google.com → the 5 names), how to enable `GCHAT_DEBUG_DUMP`.
- [ ] **Step 2: Execute with owner** (they run; you watch logs they paste / run on their behalf if they provide access): login with test account → expect CONNECTED state and portals for the newest 20 chats with correct names/members.
- [ ] **Step 3: Validate channel liveness** — keep running ≥2h: confirm the 1.5h recycle reconnects silently; send messages to the test account from another device and confirm `MESSAGE_POSTED` events arrive in logs (receive path only — send is M2).
- [ ] **Step 4: Validate the error ladder live** — kill network 2 min (expect TRANSIENT_DISCONNECT + recovery); restart bridge (expect cookie-rotation persistence to keep login alive).
- [ ] **Step 5: Capture fixtures** — run `/capture-fixtures`; commit sanitized frames for chunkparser/pblite tests; swap the hand-built test fixtures for real ones where they differ.
- [ ] **Step 6: Document** — write `docs/research/09-live-spike-findings.md`: every place live 2026 behavior differs from research 01/02 (constants still valid? base64 responses seen? cookie rotation cadence?); update reports 01/02 inline with `> **2026-07 spike:** ...` corrections. File follow-up issues for M2+ scope drift (formatting #110, upload #114 checks happen in their milestones).
- [ ] **Step 7: Commit** `docs: M1 live spike findings and real wire fixtures`.

---

## M1 exit checklist (maps to spec §8)

- Cookie login via bot command succeeds against live Google Chat (test account).
- Bridge state reaches CONNECTED; newest-N portals exist on Matrix with names/members/ghost info.
- Channel survives: forced disconnect (backoff + recover), 1.5h recycle, bridge restart (rotated cookies persisted).
- `logout` cleans up; re-login works (relogin/override path).
- All unit tests green incl. real-fixture chunk/pblite tests; layering greps clean; auditor PASS on tasks 3, 5, 7, 8, 10, 12.
