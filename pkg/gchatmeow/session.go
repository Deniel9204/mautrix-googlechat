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

const (
	// apiClient's Timeout is the sum of these; never used for the long poll.
	apiConnectTimeout = 30 * time.Second
	apiRequestTimeout = 30 * time.Second

	// maxRetries is the total number of attempts.
	maxRetries = 3

	retryBackoffBase = 50 * time.Millisecond

	// Plausibly-current browser versions for the User-Agent; bump occasionally.
	latestChromeVersion  = "150"
	latestFirefoxVersion = "152"

	defaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/" + latestChromeVersion + ".0.0.0 Safari/537.36"

	// maxRetryAfter caps a server-supplied Retry-After.
	maxRetryAfter = 60 * time.Second

	maxErrorBodyBytes = 512
)

var (
	chromeVersionRegex  = regexp.MustCompile(`Chrome/\d+\.\d+\.\d+\.\d+`)
	firefoxVersionRegex = regexp.MustCompile(`Firefox/\d+.\d+`)

	// Auth cookies are only sent to / absorbed from these hosts.
	// googleusercontent.com is deliberately excluded: those hops must be cookie-less.
	defaultAllowedHostSuffixes = []string{"google.com"}
)

// Response is a fully-read HTTP response, as returned by Fetch.
type Response struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

// Session is the authenticated HTTP layer: cookie jar, User-Agent, and the Fetch/FetchRaw paths.
type Session struct {
	mu        sync.RWMutex
	userAgent string

	jar *cookieJar

	apiClient *http.Client

	// pollClient's Timeout MUST stay 0: the long poll lasts up to ~1h and
	// Timeout covers the body read; the caller cancels via ctx.
	pollClient *http.Client

	// allowedHostSuffixes gates cookie send and absorb; tests override it.
	allowedHostSuffixes []string

	// moleWorldBaseURL overrides FetchXSRFToken's base URL in tests; empty means production.
	moleWorldBaseURL string
}

// NewSession builds a Session from cookies (keys upper-cased) and a User-Agent (empty means default).
func NewSession(cookies map[string]string, userAgent string) (*Session, error) {
	jar := newCookieJar()
	for name, value := range cookies {
		jar.set(strings.ToUpper(name), value)
	}

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		// Bounds only the connect phase, so it also applies to FetchRaw's long poll.
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

// Cookies returns the full current jar, including rotated and Set-Cookie extras, for persistence.
func (s *Session) Cookies() map[string]string {
	return s.jar.snapshotAll()
}

// DefaultUserAgent returns the User-Agent a Session uses when the caller supplies none.
func DefaultUserAgent() string {
	return defaultUserAgent
}

// UserAgent returns the normalized User-Agent applied to every request.
func (s *Session) UserAgent() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.userAgent
}

// hostAllowed reports whether u's host equals or is a subdomain of an allowed suffix.
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

// buildRequest applies User-Agent, Keep-Alive, and (for allowed hosts only) the Cookie header.
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
	req.Header.Set("Connection", "Keep-Alive")

	if s.hostAllowed(req.URL) {
		if cookieHeader := s.jar.header(); cookieHeader != "" {
			req.Header.Set("Cookie", cookieHeader)
		}
	}
	return req, nil
}

// absorbCookies ingests Set-Cookie only if the FINAL post-redirect origin is allowlisted;
// the flat jar has no domain scoping, so gating on the original URL would let a redirect poison SID.
func (s *Session) absorbCookies(resp *http.Response) {
	if resp.Request == nil || !s.hostAllowed(resp.Request.URL) {
		return
	}
	s.jar.absorb(resp)
}

// Fetch performs an idempotent request, retrying transport errors, 429 and 5xx; only 200 is success.
// Send-path callers must use FetchNonIdempotent instead.
func (s *Session) Fetch(ctx context.Context, method, urlStr string, headers http.Header, body []byte) (*Response, error) {
	return s.fetch(ctx, method, urlStr, headers, body, true)
}

// FetchNonIdempotent is Fetch without 5xx retries (a 5xx may follow a processed send); 429 is still retried.
func (s *Session) FetchNonIdempotent(ctx context.Context, method, urlStr string, headers http.Header, body []byte) (*Response, error) {
	return s.fetch(ctx, method, urlStr, headers, body, false)
}

func (s *Session) fetch(ctx context.Context, method, urlStr string, headers http.Header, body []byte, retryServerErrors bool) (*Response, error) {
	var lastErr error
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
			// Deterministic, so not retried.
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
			// Rejected, not processed: safe to repeat regardless of idempotency.
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

// FetchRaw returns the unread *http.Response with no client timeout and no retries; the caller owns
// cancellation via ctx. A non-empty form is sent URL-encoded as the body.
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

func backoffDelay(retryIndex int) time.Duration {
	return retryBackoffBase * time.Duration(1<<uint(retryIndex))
}

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

func isServerErrorStatus(status int) bool {
	return status >= 500 && status <= 599
}

// parseRetryAfter parses delay-seconds or HTTP-date, returning 0 if absent/invalid/past; capped at maxRetryAfter.
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

// parseErrorCode extracts the top-level JSON "error" field, or "".
func parseErrorCode(body []byte) string {
	var parsed struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return ""
	}
	return parsed.Error
}

func truncateBody(body []byte) string {
	if len(body) > maxErrorBodyBytes {
		body = body[:maxErrorBodyBytes]
	}
	return string(body)
}
