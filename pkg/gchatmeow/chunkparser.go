package gchatmeow

import (
	"strconv"
	"unicode/utf16"
	"unicode/utf8"
)

// ChunkParser incrementally decodes the BrowserChannel's framing:
//
//	<length>\n<payload>
//
// repeated back to back in the response body, where <length> is the decimal
// number of UTF-16 code units in payload -- i.e. JavaScript's String.length,
// NOT the number of bytes or Unicode code points. A rune outside the Basic
// Multilingual Plane (e.g. most emoji) is a surrogate pair and counts as 2.
//
// The underlying byte stream is UTF-8 and may be split at an arbitrary byte
// boundary between Feed calls -- including in the middle of a multi-byte
// rune. ChunkParser buffers any undecodable/incomplete tail and resumes
// decoding it on the next Feed call, exactly like Python's ChunkParser
// (maugclib/channel.py:67-121), which keeps its buffer as raw bytes and only
// best-effort-decodes a *view* of it via a UTF-8 incremental decoder.
//
// Do NOT convert the whole buffer to a Go string up front: Go's []byte->string
// conversion silently replaces an incomplete trailing UTF-8 sequence with
// U+FFFD replacement runes, which permanently corrupts the data once it's
// written back into the buffer. That is the exact bug in megabridge's
// gchatmeow/channel.go GetChunks (docs/research/08c-megabridge-clientlib.md
// section 1.3): a rune split across two reads gets mangled into U+FFFD,
// which then miscounts against the UTF-16 length and desynchronizes the
// frame boundary for everything after it.
type ChunkParser struct {
	buf []byte
}

// Feed appends raw bytes and returns all complete payloads decoded so far.
// Any incomplete length prefix, incomplete payload, or trailing partial rune
// is retained in the internal buffer for the next Feed call.
func (p *ChunkParser) Feed(data []byte) []string {
	p.buf = append(p.buf, data...)

	var chunks []string
	for {
		// Decode as much of the buffer as possible as UTF-8, tolerating (and
		// leaving behind) an incomplete multi-byte sequence at the very end.
		// This mirrors Python's incremental UTF-8 decoder
		// (_best_effort_decode, channel.py:61-64).
		decoded := decodeUTF8Prefix(p.buf)

		lengthStr, rest, res := cutFirstLine(decoded)
		switch res {
		case lineIncomplete:
			// No newline in the decodable prefix yet -- can't know the
			// frame length. Wait for more data.
			return chunks
		case linePoisoned:
			// The byte at this exact position can never become part of a
			// valid "<digits>\n" length token, no matter how much more data
			// arrives (see cutFirstLine). This is unreachable with real
			// Google traffic (TLS-delivered, always well-formed), but
			// dropping one byte and resynchronizing guarantees the parser
			// always makes forward progress instead of silently stalling
			// forever with an unbounded buffer -- Python's equivalent
			// failure mode is a decode exception that tears down and
			// replaces the whole ChunkParser (channel.py:230).
			p.buf = p.buf[1:]
			continue
		}

		length, err := strconv.Atoi(lengthStr)
		if err != nil {
			// Python's LEN_REGEX (`([0-9]+)\n`) only matches if the text up
			// to the newline is all digits; if it's not, re.match returns
			// None and get_chunks stops yielding (waits for more data, same
			// as "no newline yet"). Mirror that rather than erroring.
			break
		}

		// Count UTF-16 code units of `rest` one rune at a time until we
		// reach `length` units, tracking how many *bytes* of `rest` (and
		// therefore of the original buffer) that consumed.
		payload, payloadByteLen, complete := takeUTF16Units(rest, length)
		if !complete {
			// Not enough of the payload has arrived yet (or it ends
			// mid-rune within the already-decoded prefix). Wait for more
			// data; nothing is dropped from the buffer.
			break
		}

		chunks = append(chunks, payload)

		// Drop the length prefix, its newline, and the payload from the
		// front of the buffer. Everything here was counted in bytes of the
		// original UTF-8 buffer, so this is safe even if p.buf still has an
		// undecoded/incomplete tail past what decodeUTF8Prefix decoded.
		lengthPrefixByteLen := len(lengthStr) + 1 // + "\n"
		drop := lengthPrefixByteLen + payloadByteLen
		p.buf = p.buf[drop:]
	}

	return chunks
}

// decodeUTF8Prefix decodes the longest valid-UTF-8 prefix of buf as a
// string, stopping before any trailing incomplete multi-byte sequence (it
// does NOT stop at/replace actual invalid encoding errors with U+FFFD; it
// only holds back bytes that could be the start of a truncated valid
// sequence).
func decodeUTF8Prefix(buf []byte) string {
	if utf8.Valid(buf) {
		return string(buf)
	}

	// Walk rune by rune. utf8.DecodeRune reports (RuneError, 1) both for a
	// genuinely invalid byte and for a short/incomplete-but-possibly-valid
	// sequence at the end of buf; utf8.RuneError distinguishes the latter
	// case by returning size 0 when input is empty, or by us checking
	// whether the offending bytes are a valid prefix of *some* rune (i.e.
	// we're at the tail of the buffer and could just need more bytes).
	i := 0
	for i < len(buf) {
		r, size := utf8.DecodeRune(buf[i:])
		if r == utf8.RuneError && size <= 1 {
			// Either a genuinely invalid single byte, or an incomplete
			// sequence. If it's incomplete (i.e. buf[i:] is a valid prefix
			// of some UTF-8 encoding but we ran out of bytes), stop here
			// and leave it buffered for the next Feed. Detect "incomplete"
			// vs "invalid" using utf8.FullRune.
			if !utf8.FullRune(buf[i:]) {
				break
			}
			// Genuinely invalid byte (not a truncation issue): Python's
			// codecs incremental UTF-8 decoder would raise here in strict
			// mode, but get_chunks doesn't catch that -- in practice
			// Google's channel never sends invalid UTF-8. Advance past it so
			// the truncation scan doesn't stop here; the byte itself is
			// still included verbatim in the string returned below
			// (string(buf[:i]) is a raw byte copy, not a rebuild from
			// decoded runes). cutFirstLine and takeUTF16Units handle a
			// literal invalid byte the same way: treat it as a single opaque
			// unit and keep moving, rather than mistaking it for a
			// truncated tail and waiting forever for bytes that could never
			// fix it.
			i++
			continue
		}
		i += size
	}
	return string(buf[:i])
}

// lineResult classifies the outcome of scanning the decoded buffer for a
// "<digits>\n" length token.
type lineResult int

const (
	// lineIncomplete means the buffer so far is still a valid, growable
	// prefix of "<digits>\n" (including empty) -- wait for more data.
	lineIncomplete lineResult = iota
	// lineOK means a complete "<digits>\n" token was found at position 0.
	lineOK
	// linePoisoned means the byte immediately after any leading digits is
	// neither a digit nor '\n' (or there were zero leading digits and the
	// first byte isn't '\n' either). That byte is already fixed in the
	// buffer, so no amount of future data can turn this into a valid
	// length token -- unlike lineIncomplete, this can never resolve on its
	// own and the caller must resynchronize.
	linePoisoned
)

// cutFirstLine mirrors Python's LEN_REGEX = re.compile(r"([0-9]+)\n",
// re.MULTILINE) matched at the start of the decoded buffer: it looks for a
// run of ASCII digits immediately followed by '\n' at position 0. Unlike a
// plain strings.Cut on the first '\n', this does not treat a line as a
// length unless it is anchored at the very start of the buffer, matching
// re.match (not re.search) semantics.
func cutFirstLine(decoded string) (lengthStr string, rest string, res lineResult) {
	i := 0
	for i < len(decoded) && decoded[i] >= '0' && decoded[i] <= '9' {
		i++
	}
	if i >= len(decoded) {
		// Ran out of buffer while inside (or before) a digit run. Also
		// covers the empty-buffer case (i=0, len=0).
		return "", "", lineIncomplete
	}
	if i == 0 || decoded[i] != '\n' {
		// Either the very first byte isn't a digit at all (LEN_REGEX
		// requires `[0-9]+`, i.e. at least one), or 1+ digits were followed
		// by something other than '\n'. This can only happen with
		// genuinely invalid/misdirected bytes (see decodeUTF8Prefix), which
		// real Google traffic (delivered over TLS) never produces.
		return "", "", linePoisoned
	}
	return decoded[:i], decoded[i+1:], lineOK
}

// takeUTF16Units consumes runes from s until their cumulative UTF-16 code
// unit count reaches want (runes outside the BMP count as 2 units, via a
// surrogate pair). It returns the payload string, the number of *bytes* of s
// consumed to produce it, and whether want units were actually reached.
//
// s is always a suffix of decodeUTF8Prefix's output, which by construction
// never ends mid-truncated-rune: any incomplete trailing UTF-8 sequence was
// already excluded before decodeUTF8Prefix returned. So a RuneError found
// here is never a truncation -- it can only be a genuinely invalid byte that
// decodeUTF8Prefix already decided to include verbatim. Treat it as a single
// opaque UTF-16 unit and consume exactly one byte, matching that decision
// and guaranteeing forward progress instead of waiting forever for bytes
// that could never fix it (see decodeUTF8Prefix).
func takeUTF16Units(s string, want int) (payload string, byteLen int, complete bool) {
	if want == 0 {
		return "", 0, true
	}

	units := 0
	i := 0
	for i < len(s) {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size <= 1 {
			units++
			i++
		} else {
			units += utf16RuneLen(r)
			i += size
		}

		if units >= want {
			return s[:i], i, true
		}
	}

	return "", 0, false
}

// utf16RuneLen returns how many UTF-16 code units r encodes to: 1 for BMP
// runes, 2 for astral (surrogate-pair) runes.
func utf16RuneLen(r rune) int {
	if r1, r2 := utf16.EncodeRune(r); r1 == utf8.RuneError && r2 == utf8.RuneError {
		return 1
	}
	return 2
}
