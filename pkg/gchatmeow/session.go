package gchatmeow

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"
)

// Constants ported from maugclib/http_utils.py (line numbers per
// docs/research/01-maugclib-client-library.md, read 2026-07-13).
const (
	// apiConnectTimeout/apiRequestTimeout mirror Python's CONNECT_TIMEOUT
	// (connect phase) and REQUEST_TIMEOUT (body-read phase),
	// http_utils.py:18-19. aiohttp applies these as two independent
	// watchdogs (aiohttp.ClientTimeout(connect=30) plus a separate
	// async_timeout.timeout(30) around res.read()); Go's http.Client.Timeout
	// is a single end-to-end budget, so apiClient uses their sum (60s) as a
	// ceiling. apiClient is ONLY used by Fetch (bounded /api/* calls) --
	// never for the long-poll channel; see pollClient below.
	apiConnectTimeout = 30 * time.Second
	apiRequestTimeout = 30 * time.Second

	// maxRetries mirrors MAX_RETRIES, http_utils.py:20 (total attempts, not
	// "retries after the first try").
	maxRetries = 3

	// retryBackoffBase has NO Python equivalent: http_utils.py's retry loop
	// (fetch, http_utils.py:136-163) has no sleep between attempts at all.
	// task-3-brief.md explicitly requires exponential backoff between
	// retries for this port; this constant and backoffDelay's doubling
	// schedule are this port's own addition (deliberate, not "invented" --
	// the brief asks for it by name), kept small so retry tests stay fast.
	retryBackoffBase = 50 * time.Millisecond

	// latestChromeVersion/latestFirefoxVersion mirror LATEST_CHROME_VERSION
	// / LATEST_FIREFOX_VERSION, http_utils.py:23-24.
	latestChromeVersion  = "114"
	latestFirefoxVersion = "114"

	// defaultUserAgent mirrors DEFAULT_USER_AGENT, http_utils.py:25-28.
	defaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/" + latestChromeVersion + ".0.0.0 Safari/537.36"

	// maxErrorBodyBytes caps how much of a non-200 response body is kept in
	// UnexpectedStatusError.Body (errors.go: "Body string // first 512
	// bytes").
	maxErrorBodyBytes = 512
)

var (
	// chromeVersionRegex/firefoxVersionRegex mirror http_utils.py:29-30
	// exactly, including the unescaped '.' in the Firefox pattern (Python:
	// r"Firefox/\d+.\d+" -- the bare '.' matches any character, not just a
	// literal dot). Kept verbatim rather than "fixed", per port-module
	// fidelity discipline; the difference is not observable for any real
	// Firefox UA string.
	chromeVersionRegex  = regexp.MustCompile(`Chrome/\d+\.\d+\.\d+\.\d+`)
	firefoxVersionRegex = regexp.MustCompile(`Firefox/\d+.\d+`)

	// defaultAllowedHostSuffixes: cookies are only ever attached to (and
	// absorbed from) requests whose host matches one of these suffixes,
	// mirroring http_utils.py:249-255's "ensure we don't accidentally send
	// the authorization header/cookies to a non-Google domain" safety rail.
	// Per task-3-brief.md this explicitly includes googleusercontent.com
	// alongside google.com. NOTE: this is broader than doc 01 §5.2's
	// description of Python's *caller-side* download-flow behavior, which
	// routes googleusercontent.com through a separate, cookie-less
	// aiohttp.ClientSession specifically so it does NOT get auth cookies --
	// flagged in the task report as a tension between the two docs for the
	// milestone owner to confirm; implemented here per this task's explicit
	// brief.
	defaultAllowedHostSuffixes = []string{"google.com", "googleusercontent.com"}
)

// Response is a fully-read HTTP response, as returned by Fetch. It mirrors
// Python's FetchResponse NamedTuple (http_utils.py:33-36: code, headers,
// body), with header stored as the idiomatic Go http.Header rather than
// Python's collapsed dict[str, str].
type Response struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

// Session is the authenticated HTTP layer for the Google Chat client. It
// owns the cookie jar and the User-Agent, and exposes two request paths:
// Fetch (bounded, retried, body-buffering -- for plain /api/* RPCs) and
// FetchRaw (unbounded, caller-cancelled, raw *http.Response -- for the
// BrowserChannel long-poll). Both share one cookie jar.
//
// Ported from maugclib/http_utils.py's Session class.
type Session struct {
	mu        sync.RWMutex
	userAgent string

	jar *cookieJar

	// apiClient is used by Fetch: a bounded per-request timeout is fine
	// because every /api/* RPC is a normal, short-lived request.
	apiClient *http.Client

	// pollClient is used by FetchRaw. Its Timeout MUST stay 0: the
	// BrowserChannel long-poll holds this connection open for up to ~1 hour
	// (doc 01 §2.2) and the caller (Task 7's channel) owns cancellation via
	// ctx/context deadline instead. googlechat-megabridge's session.go set
	// a blanket 90s http.Client.Timeout on the client shared with the
	// long-poll path; Go's Timeout covers the *entire* request including
	// body read, so every long poll there is aborted after 90s regardless
	// of heartbeats, eventually killing the channel for good
	// (docs/research/08c-megabridge-clientlib.md §1.4). Do not reintroduce
	// that here.
	pollClient *http.Client

	// allowedHostSuffixes gates both directions: cookies are only attached
	// to outgoing requests, and only absorbed from responses, when the
	// request host matches one of these suffixes (http_utils.py:249-255).
	// Defaults to defaultAllowedHostSuffixes; unexported so only this
	// package's own tests can override it.
	allowedHostSuffixes []string
}

// NewSession builds a Session preloaded with the given cookies (keys are
// upper-cased, matching http_utils.py:64-66's cookie[key.upper()] = value)
// and a User-Agent. An empty userAgent falls back to defaultUserAgent
// (http_utils.py:76-77); a non-empty one has its Chrome/Firefox version
// pinned to the latest known-good values (http_utils.py:69-77).
func NewSession(cookies map[string]string, userAgent string) (*Session, error) {
	jar := newCookieJar()
	for name, value := range cookies {
		jar.set(strings.ToUpper(name), value)
	}

	transport := &http.Transport{
		// Mirrors aiohttp's trust_env=True (http_utils.py:83), which honors
		// HTTP_PROXY/HTTPS_PROXY/NO_PROXY (doc 01 §7: "honors HTTP_PROXY env
		// var ... plus aiohttp trust_env=True").
		Proxy: http.ProxyFromEnvironment,
		// TLSClientConfig intentionally left at its zero value (nil): normal
		// certificate verification stays ON. Python passes ssl=False to
		// aiohttp (http_utils.py:264), disabling verification entirely;
		// doc 01 §7 explicitly says a Go port must NOT replicate that.

		// DialContext/TLSHandshakeTimeout bound only the CONNECT phase (TCP
		// dial + TLS handshake), mirroring http_utils.py:79-85's
		// aiohttp.ClientTimeout(connect=CONNECT_TIMEOUT) -- set once at the
		// session level and, unlike REQUEST_TIMEOUT, applied to *every*
		// request through that session, including fetch_raw's long poll
		// (aiohttp has no separate "poll" session in Python). Deliberately
		// does NOT bound the total request/response lifetime: that's exactly
		// pollClient's Timeout staying 0 (see its doc comment) -- a black-holed
		// TCP handshake still can't hang FetchRaw forever, but an
		// already-established long poll is bounded only by the caller's ctx.
		DialContext:         (&net.Dialer{Timeout: apiConnectTimeout}).DialContext,
		TLSHandshakeTimeout: apiConnectTimeout,
	}

	allowedHostSuffixes := make([]string, len(defaultAllowedHostSuffixes))
	copy(allowedHostSuffixes, defaultAllowedHostSuffixes)

	return &Session{
		userAgent: normalizeUserAgent(userAgent),
		jar:       jar,
		apiClient: &http.Client{
			Transport: transport,
			Timeout:   apiConnectTimeout + apiRequestTimeout,
		},
		pollClient: &http.Client{
			Transport: transport,
			Timeout:   0,
		},
		allowedHostSuffixes: allowedHostSuffixes,
	}, nil
}

func normalizeUserAgent(userAgent string) string {
	if userAgent == "" {
		return defaultUserAgent
	}
	userAgent = chromeVersionRegex.ReplaceAllString(userAgent, "Chrome/"+latestChromeVersion+".0.0.0")
	userAgent = firefoxVersionRegex.ReplaceAllString(userAgent, "Firefox/"+latestFirefoxVersion+".0")
	return userAgent
}

// Cookies returns the CURRENT value of each of RequiredCookies, reflecting
// any rotation absorbed from Set-Cookie responses so far. Mirrors
// Session.get_auth_cookies, http_utils.py:87-92; used to persist rotated
// cookies into UserLoginMetadata.
func (s *Session) Cookies() map[string]string {
	return s.jar.snapshot(RequiredCookies)
}

// hostAllowed reports whether u's host matches one of allowedHostSuffixes:
// either exactly, or as a dotted suffix (host ends in "."+suffix). Python's
// equivalent check (http_utils.py:251) is dot-suffix-only
// (`host.endswith(".google.com")`), which would reject a bare apex
// "google.com" -- moot in production since every real target here is a
// subdomain (chat.google.com, accounts.google.com, ...). The exact-match
// branch is intentionally broader and exists so tests can inject a bare
// httptest host/IP (e.g. "127.0.0.1") into allowedHostSuffixes, per
// task-3-brief.md's requirement that the allowlist be "injectable for
// tests via unexported field" -- an IP-literal test host has no meaningful
// "subdomain of itself" to suffix-match against.
func (s *Session) hostAllowed(u *url.URL) bool {
	host := u.Hostname()
	s.mu.RLock()
	suffixes := s.allowedHostSuffixes
	s.mu.RUnlock()
	for _, suffix := range suffixes {
		if host == suffix || strings.HasSuffix(host, "."+suffix) {
			return true
		}
	}
	return false
}

// buildRequest constructs an *http.Request with Session-level defaults
// applied: a User-Agent (unless the caller already set one), a forced
// "Connection: Keep-Alive" (http_utils.py:254-255, unconditionally
// overwritten same as Python), and -- only if the target host matches
// allowedHostSuffixes -- a hand-built, unquoted Cookie header
// (http_utils.py:61-62; see cookieJar's doc comment). It reports whether
// the host was allowed, so the caller can decide whether to also absorb
// Set-Cookie from the response.
func (s *Session) buildRequest(ctx context.Context, method, rawURL string, headers http.Header, body io.Reader) (*http.Request, bool, error) {
	req, err := http.NewRequestWithContext(ctx, method, rawURL, body)
	if err != nil {
		return nil, false, err
	}
	for k, vv := range headers {
		for _, v := range vv {
			req.Header.Add(k, v)
		}
	}
	if req.Header.Get("User-Agent") == "" {
		s.mu.RLock()
		ua := s.userAgent
		s.mu.RUnlock()
		req.Header.Set("User-Agent", ua)
	}
	// Forced unconditionally, matching http_utils.py:254-255 (headers["Connection"] = "Keep-Alive").
	req.Header.Set("Connection", "Keep-Alive")

	allowed := s.hostAllowed(req.URL)
	if allowed {
		if cookieHeader := s.jar.header(); cookieHeader != "" {
			req.Header.Set("Cookie", cookieHeader)
		}
	}
	return req, allowed, nil
}

// Fetch performs a request with auth cookies, retrying transient failures
// (maxRetries=3 total attempts, exponential backoff -- see
// retryBackoffBase) before giving up with a *NetworkError. A response is
// only ever treated as success on exact status 200, matching
// http_utils.py:165-172's `res.status != 200` check verbatim (not a
// generic 2xx range): Google Chat's /api/* endpoints always answer 200 on
// success, so anything else is an error path. A retryable 5xx status is
// retried like a transport error; anything else (including 2xx-non-200,
// and all 4xx) is returned immediately as an *UnexpectedStatusError
// carrying the status code -- an explicit improvement over Python, which
// collapses every non-200 into a bare NetworkError with no status (see
// errors.go's UnexpectedStatusError doc comment and doc 01 §6: "plain API
// calls via Session.fetch collapse non-200 into NetworkError without
// status").
func (s *Session) Fetch(ctx context.Context, method, urlStr string, headers http.Header, body []byte) (*Response, error) {
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if attempt > 0 {
			if err := sleepOrDone(ctx, backoffDelay(attempt-1)); err != nil {
				return nil, err
			}
		}

		var bodyReader io.Reader
		if body != nil {
			bodyReader = bytes.NewReader(body)
		}
		req, allowed, err := s.buildRequest(ctx, method, urlStr, headers, bodyReader)
		if err != nil {
			// Request construction (e.g. a malformed method/URL) fails the
			// same way on every attempt, so unlike a transport error it is
			// not retried -- but it's still wrapped as *NetworkError,
			// matching Python's fetch(), whose docstring promises
			// NetworkError for any failure to complete the request
			// (http_utils.py:126-127) and which -- less deliberately --
			// achieves that here by catching ValueError from URL(url)
			// inside the same retry loop (http_utils.py:156-157) and
			// burning all MAX_RETRIES attempts on a deterministically
			// unfixable input before raising.
			return nil, &NetworkError{Err: err}
		}

		resp, err := s.apiClient.Do(req)
		if err != nil {
			lastErr = err
			continue
		}

		if allowed {
			s.jar.absorb(resp)
		}

		respBody, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}

		if resp.StatusCode == http.StatusOK {
			return &Response{StatusCode: resp.StatusCode, Header: resp.Header, Body: respBody}, nil
		}

		statusErr := &UnexpectedStatusError{
			URL:       urlStr,
			Status:    resp.StatusCode,
			ErrorCode: parseErrorCode(respBody),
			Body:      truncateBody(respBody),
		}
		if isRetryableStatus(resp.StatusCode) {
			lastErr = statusErr
			continue
		}
		return nil, statusErr
	}

	return nil, &NetworkError{Err: lastErr}
}

// FetchRaw performs a request and returns the raw *http.Response without
// reading the body and WITHOUT any client-level timeout -- the caller owns
// cancellation via ctx (see pollClient's doc comment). Used by the
// BrowserChannel long-poll (Task 7). No retry loop and no status-code
// mapping happen here; the caller inspects resp.StatusCode itself (doc 01
// §2.6 documents the channel's own, more elaborate error ladder on top of
// this).
//
// form, if non-empty, is URL-encoded as the request body with a
// application/x-www-form-urlencoded Content-Type (mirrors the
// forward-channel POST body in maugclib/channel.py:303-341); most
// FetchRaw callers are plain GETs with form == nil, since the long-poll's
// query parameters are baked into urlStr already.
func (s *Session) FetchRaw(ctx context.Context, method, urlStr string, headers http.Header, form url.Values) (*http.Response, error) {
	var bodyReader io.Reader
	if len(form) > 0 {
		bodyReader = strings.NewReader(form.Encode())
		headers = cloneHeaders(headers)
		if headers.Get("Content-Type") == "" {
			headers.Set("Content-Type", "application/x-www-form-urlencoded")
		}
	}

	req, allowed, err := s.buildRequest(ctx, method, urlStr, headers, bodyReader)
	if err != nil {
		return nil, err
	}

	resp, err := s.pollClient.Do(req)
	if err != nil {
		return nil, err
	}

	if allowed {
		s.jar.absorb(resp)
	}
	return resp, nil
}

func cloneHeaders(headers http.Header) http.Header {
	if headers == nil {
		return make(http.Header)
	}
	return headers.Clone()
}

// backoffDelay returns the exponential backoff for the retryIndex-th sleep
// (0-based): retryBackoffBase * 2^retryIndex. Not present in Python (see
// retryBackoffBase's doc comment); this port's own addition, explicitly
// requested by task-3-brief.md.
func backoffDelay(retryIndex int) time.Duration {
	return retryBackoffBase * time.Duration(1<<uint(retryIndex))
}

// sleepOrDone waits for d, or returns ctx.Err() early if ctx is cancelled
// first.
func sleepOrDone(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// isRetryableStatus reports whether a non-200 status should be retried
// like a transport error rather than surfaced immediately. Deliberately
// narrow (5xx only): googlechat-megabridge's session.go retried every
// non-200 status including 401, hammering a dead session 3x and losing the
// status code in the final error
// (docs/research/08c-megabridge-clientlib.md §3, "Retries" paragraph) --
// not replicated here.
func isRetryableStatus(status int) bool {
	return status >= 500 && status <= 599
}

// parseErrorCode extracts the top-level JSON "error" field from a response
// body, if present, mirroring exceptions.py's ResponseError /
// UnexpectedStatusError parsing ("error"/"error_description" into
// .error_code/.error_desc -- only .error_code has a Go home so far, in
// errors.go's UnexpectedStatusError). Malformed/non-JSON bodies yield "".
func parseErrorCode(body []byte) string {
	var parsed struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return ""
	}
	return parsed.Error
}

// truncateBody caps body at maxErrorBodyBytes (errors.go: "Body string //
// first 512 bytes").
func truncateBody(body []byte) string {
	if len(body) > maxErrorBodyBytes {
		body = body[:maxErrorBodyBytes]
	}
	return string(body)
}
