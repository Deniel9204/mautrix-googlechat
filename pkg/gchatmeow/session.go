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
	"strconv"
	"strings"
	"sync"
	"time"
)

// Constants for the authenticated HTTP layer.
const (
	// apiConnectTimeout/apiRequestTimeout separate the connect phase from
	// the body-read phase: two independent 30s watchdogs (one on connect,
	// one around the response read). Go's http.Client.Timeout is a single
	// end-to-end budget, so apiClient uses their sum (60s) as a ceiling.
	// apiClient is ONLY used by Fetch (bounded /api/* calls) -- never for
	// the long-poll channel; see pollClient below.
	apiConnectTimeout = 30 * time.Second
	apiRequestTimeout = 30 * time.Second

	// maxRetries is the total number of attempts, not "retries after the
	// first try".
	maxRetries = 3

	// retryBackoffBase drives the exponential backoff between retries.
	// This constant and backoffDelay's doubling schedule are this port's own
	// addition, kept small so retry tests stay fast.
	retryBackoffBase = 50 * time.Millisecond

	// latestChromeVersion/latestFirefoxVersion pin the User-Agent version
	// numbers to the latest known-good values.
	latestChromeVersion  = "114"
	latestFirefoxVersion = "114"

	// defaultUserAgent is the fallback User-Agent when the caller supplies
	// none.
	defaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/" + latestChromeVersion + ".0.0.0 Safari/537.36"

	// maxRetryAfter caps how long a server-supplied Retry-After can park a
	// request. The wait is interruptible via ctx either way; the cap just
	// stops an absurd or hostile value from stalling a whole RPC.
	maxRetryAfter = 60 * time.Second

	// maxErrorBodyBytes caps how much of a non-200 response body is kept in
	// UnexpectedStatusError.Body (errors.go: "Body string // first 512
	// bytes").
	maxErrorBodyBytes = 512
)

var (
	// chromeVersionRegex/firefoxVersionRegex include an unescaped '.' in the
	// Firefox pattern (`Firefox/\d+.\d+` -- the bare '.' matches any
	// character, not just a literal dot). Kept verbatim from the reference
	// rather than "fixed", per port-module fidelity discipline; the
	// difference is not observable for any real Firefox UA string.
	chromeVersionRegex  = regexp.MustCompile(`Chrome/\d+\.\d+\.\d+\.\d+`)
	firefoxVersionRegex = regexp.MustCompile(`Firefox/\d+.\d+`)

	// defaultAllowedHostSuffixes: cookies are only ever attached to (and
	// absorbed from) requests whose host matches one of these suffixes -- a
	// safety rail so auth cookies never accidentally reach a non-Google
	// domain. google.com ONLY: googleusercontent.com is deliberately
	// excluded -- googleusercontent.com hops must be made cookie-less
	// precisely so auth cookies never reach them. The download flow must
	// make cookie-less requests for non-allowlisted hosts (this Session
	// simply won't attach cookies to them).
	defaultAllowedHostSuffixes = []string{"google.com"}
)

// Response is a fully-read HTTP response, as returned by Fetch: status
// code, headers, and body, with headers stored as the idiomatic Go
// http.Header.
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
type Session struct {
	mu        sync.RWMutex
	userAgent string

	jar *cookieJar

	// apiClient is used by Fetch: a bounded per-request timeout is fine
	// because every /api/* RPC is a normal, short-lived request.
	apiClient *http.Client

	// pollClient is used by FetchRaw. Its Timeout MUST stay 0: the
	// BrowserChannel long-poll holds this connection open for up to ~1 hour
	// and the caller (the channel) owns cancellation via ctx/context
	// deadline instead. googlechat-megabridge's session.go set a blanket 90s
	// http.Client.Timeout on the client shared with the long-poll path; Go's
	// Timeout covers the *entire* request including body read, so every long
	// poll there is aborted after 90s regardless of heartbeats, eventually
	// killing the channel for good. Do not reintroduce that here.
	pollClient *http.Client

	// allowedHostSuffixes gates both directions: cookies are only attached
	// to outgoing requests whose target host matches one of these suffixes
	// (buildRequest), and only absorbed from responses whose FINAL,
	// post-redirect origin matches (absorbCookies) -- the safety rail that
	// keeps auth off non-Google hosts. Defaults to
	// defaultAllowedHostSuffixes; unexported so only this package's own
	// tests can override it.
	allowedHostSuffixes []string

	// moleWorldBaseURL overrides the base URL FetchXSRFToken (auth.go) uses
	// for its GET to /mole/world. Empty means production
	// (defaultMoleWorldBaseURL). Unexported so only this package's own tests
	// can point it at an httptest server, same pattern as
	// allowedHostSuffixes above.
	moleWorldBaseURL string
}

// NewSession builds a Session preloaded with the given cookies (keys are
// upper-cased) and a User-Agent. An empty userAgent falls back to
// defaultUserAgent; a non-empty one has its Chrome/Firefox version pinned
// to the latest known-good values.
func NewSession(cookies map[string]string, userAgent string) (*Session, error) {
	jar := newCookieJar()
	for name, value := range cookies {
		jar.set(strings.ToUpper(name), value)
	}

	transport := &http.Transport{
		// Honors HTTP_PROXY/HTTPS_PROXY/NO_PROXY env vars.
		Proxy: http.ProxyFromEnvironment,
		// TLSClientConfig intentionally left at its zero value (nil): normal
		// certificate verification stays ON. A Go port must NOT disable
		// certificate verification.

		// DialContext/TLSHandshakeTimeout bound only the CONNECT phase (TCP
		// dial + TLS handshake): the connect timeout is applied to *every*
		// request through this session, including FetchRaw's long poll.
		// Deliberately does NOT bound the total request/response lifetime:
		// that's exactly pollClient's Timeout staying 0 (see its doc comment)
		// -- a black-holed TCP handshake still can't hang FetchRaw forever,
		// but an already-established long poll is bounded only by the
		// caller's ctx.
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
// any rotation absorbed from Set-Cookie responses so far. Used to persist
// rotated cookies into UserLoginMetadata.
func (s *Session) Cookies() map[string]string {
	return s.jar.snapshot(RequiredCookies)
}

// UserAgent returns the normalized User-Agent (see normalizeUserAgent) this
// Session applies to every request, for persistence by callers outside this
// package (mirrors the Cookies() getter's purpose above).
func (s *Session) UserAgent() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.userAgent
}

// hostAllowed reports whether u's host matches one of allowedHostSuffixes:
// either exactly, or as a dotted suffix (host ends in "."+suffix). A
// dot-suffix-only check would reject a bare apex "google.com" -- moot in
// production since every real target here is a subdomain (chat.google.com,
// accounts.google.com, ...). The exact-match branch is intentionally
// broader and exists so tests can inject a bare httptest host/IP (e.g.
// "127.0.0.1") into allowedHostSuffixes -- an IP-literal test host has
// no meaningful "subdomain of itself" to suffix-match against.
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
// "Connection: Keep-Alive" (unconditionally overwritten), and -- only if
// the target host matches allowedHostSuffixes -- a hand-built, unquoted
// Cookie header (see cookieJar's doc comment). This gates only the SEND
// direction; the absorb direction is gated separately, against the
// post-redirect response origin, in absorbCookies.
func (s *Session) buildRequest(ctx context.Context, method, rawURL string, headers http.Header, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, rawURL, body)
	if err != nil {
		return nil, err
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
	// Forced unconditionally (Connection: Keep-Alive).
	req.Header.Set("Connection", "Keep-Alive")

	if s.hostAllowed(req.URL) {
		if cookieHeader := s.jar.header(); cookieHeader != "" {
			req.Header.Set("Cookie", cookieHeader)
		}
	}
	return req, nil
}

// absorbCookies ingests Set-Cookie values from resp into the jar, but only
// when the response's ACTUAL origin is allowlisted. That origin is
// resp.Request.URL -- Go's http.Client follows redirects internally and
// sets resp.Request to the FINAL, post-redirect request on the response it
// returns -- NOT the URL the caller originally asked for. Gating on the
// pre-redirect URL would let an allowlisted origin 302 to a
// non-allowlisted host whose Set-Cookie would then be absorbed into the
// shared flat jar (e.g. an attachment redirect chain poisoning SID, which
// Cookies() would then persist and every later request replay to Google).
// An RFC 6265 domain-scoped cookie jar would need no explicit check here --
// a Set-Cookie from evil.example could never attach to a chat.google.com
// request -- but this port's deliberately-flat jar (see cookieJar's doc
// comment) moves that protection to this gate instead.
func (s *Session) absorbCookies(resp *http.Response) {
	if resp.Request == nil || !s.hostAllowed(resp.Request.URL) {
		return
	}
	s.jar.absorb(resp)
}

// Fetch performs a request with auth cookies, retrying transient failures
// (maxRetries=3 total attempts, exponential backoff -- see retryBackoffBase)
// before giving up with a *NetworkError. A response is only ever treated as
// success on exact status 200 (not a generic 2xx range): Google Chat's
// /api/* endpoints always answer 200 on success, so anything else is an
// error path. Non-retryable statuses are returned immediately as an
// *UnexpectedStatusError carrying the status code, rather than collapsing
// every non-200 into a bare status-less NetworkError.
//
// Fetch is for IDEMPOTENT requests: it retries 5xx. A 5xx can be returned
// after the origin already processed the request, so a retried create would
// post the message twice; send-path callers must use FetchNonIdempotent
// instead.
func (s *Session) Fetch(ctx context.Context, method, urlStr string, headers http.Header, body []byte) (*Response, error) {
	return s.fetch(ctx, method, urlStr, headers, body, true)
}

// FetchNonIdempotent is Fetch for a request that must not be repeated once
// the origin may have acted on it -- the message-create RPCs. It behaves
// identically except that a 5xx is surfaced rather than retried, because a
// 500/502 can arrive after the message was already accepted and a retry
// would duplicate it. A duplicate the user cannot undo is worse than a
// visible send failure they can retry deliberately.
//
// 429 is still retried here: a rate-limit response means the request was
// REJECTED rather than processed, so repeating it cannot duplicate anything.
func (s *Session) FetchNonIdempotent(ctx context.Context, method, urlStr string, headers http.Header, body []byte) (*Response, error) {
	return s.fetch(ctx, method, urlStr, headers, body, false)
}

// fetch is the shared retry loop. retryServerErrors selects the policy
// described on Fetch and FetchNonIdempotent.
func (s *Session) fetch(ctx context.Context, method, urlStr string, headers http.Header, body []byte, retryServerErrors bool) (*Response, error) {
	var lastErr error
	// retryAfter carries a server-supplied Retry-After into the next
	// iteration's wait; the server's own pacing beats our blind backoff.
	var retryAfter time.Duration

	for attempt := 0; attempt < maxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if attempt > 0 {
			delay := backoffDelay(attempt - 1)
			if retryAfter > delay {
				delay = retryAfter
			}
			retryAfter = 0
			if err := sleepOrDone(ctx, delay); err != nil {
				return nil, err
			}
		}

		var bodyReader io.Reader
		if body != nil {
			bodyReader = bytes.NewReader(body)
		}
		req, err := s.buildRequest(ctx, method, urlStr, headers, bodyReader)
		if err != nil {
			// Request construction (e.g. a malformed method/URL) fails the
			// same way on every attempt, so unlike a transport error it is
			// not retried -- but it's still wrapped as *NetworkError, keeping
			// the contract that any failure to complete the request surfaces
			// as NetworkError.
			return nil, &NetworkError{Err: err}
		}

		resp, err := s.apiClient.Do(req)
		if err != nil {
			lastErr = err
			continue
		}

		s.absorbCookies(resp)

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
		switch {
		case resp.StatusCode == http.StatusTooManyRequests:
			// Rate limited: the request was rejected, not processed, so this
			// is safe to repeat regardless of idempotency. Pace it by the
			// server's own Retry-After when it supplies one.
			retryAfter = parseRetryAfter(resp.Header.Get("Retry-After"), time.Now())
			lastErr = statusErr
			continue
		case retryServerErrors && isServerErrorStatus(resp.StatusCode):
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
// BrowserChannel long-poll. No retry loop and no status-code mapping
// happen here; the caller inspects resp.StatusCode itself (the channel
// has its own, more elaborate error ladder on top of this).
//
// form, if non-empty, is URL-encoded as the request body with a
// application/x-www-form-urlencoded Content-Type (the forward-channel POST
// body shape); most FetchRaw callers are plain GETs with form == nil, since
// the long-poll's query parameters are baked into urlStr already.
func (s *Session) FetchRaw(ctx context.Context, method, urlStr string, headers http.Header, form url.Values) (*http.Response, error) {
	var bodyReader io.Reader
	if len(form) > 0 {
		bodyReader = strings.NewReader(form.Encode())
		headers = cloneHeaders(headers)
		if headers.Get("Content-Type") == "" {
			headers.Set("Content-Type", "application/x-www-form-urlencoded")
		}
	}

	req, err := s.buildRequest(ctx, method, urlStr, headers, bodyReader)
	if err != nil {
		return nil, err
	}

	resp, err := s.pollClient.Do(req)
	if err != nil {
		return nil, err
	}

	s.absorbCookies(resp)
	return resp, nil
}

func cloneHeaders(headers http.Header) http.Header {
	if headers == nil {
		return make(http.Header)
	}
	return headers.Clone()
}

// backoffDelay returns the exponential backoff for the retryIndex-th sleep
// (0-based): retryBackoffBase * 2^retryIndex. This port's own addition
// (see retryBackoffBase's doc comment).
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

// isServerErrorStatus reports whether status is a 5xx.
//
// Only idempotent requests retry these: a 500/502 can be returned AFTER the
// origin already processed the request, so repeating a non-idempotent send
// risks a duplicate (see FetchNonIdempotent). 4xx is never retried here --
// megabridge's session.go retried every non-200 including 401, hammering a
// dead session three times and losing the status code in the final error --
// with the single exception of 429, which fetch handles separately because a
// rate-limit rejection means the request never took effect.
func isServerErrorStatus(status int) bool {
	return status >= 500 && status <= 599
}

// parseRetryAfter interprets a Retry-After header in either RFC 7231 form --
// delay-seconds, or an HTTP-date -- relative to now, returning 0 when it is
// absent, unparseable, or already in the past. The result is capped at
// maxRetryAfter.
func parseRetryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	var d time.Duration
	if secs, err := strconv.Atoi(value); err == nil {
		if secs <= 0 {
			return 0
		}
		d = time.Duration(secs) * time.Second
	} else if t, err := http.ParseTime(value); err == nil {
		d = t.Sub(now)
	}
	if d <= 0 {
		return 0
	}
	if d > maxRetryAfter {
		return maxRetryAfter
	}
	return d
}

// parseErrorCode extracts the top-level JSON "error" field from a response
// body, if present (the error response carries "error"/"error_description";
// only "error" has a Go home so far, in errors.go's UnexpectedStatusError).
// Malformed/non-JSON bodies yield "".
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
