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

		lengthStr, rest, ok := cutFirstLine(decoded)
		if !ok {
			// No newline in the decodable prefix yet -- can't know the
			// frame length. Wait for more data.
			break
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
		// undecoded/incomplete tail past decodedLen.
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
			// Google's channel never sends invalid UTF-8. Skip one byte so
			// we don't infinite-loop; this only matters for malformed
			// input.
			i++
			continue
		}
		i += size
	}
	return string(buf[:i])
}

// cutFirstLine mirrors Python's LEN_REGEX = re.compile(r"([0-9]+)\n",
// re.MULTILINE) matched at the start of the decoded buffer: it looks for a
// run of ASCII digits immediately followed by '\n' at position 0. Unlike a
// plain strings.Cut on the first '\n', this does not treat a line as a
// length unless it is anchored at the very start of the buffer, matching
// re.match (not re.search) semantics.
func cutFirstLine(decoded string) (lengthStr string, rest string, ok bool) {
	i := 0
	for i < len(decoded) && decoded[i] >= '0' && decoded[i] <= '9' {
		i++
	}
	if i == 0 || i >= len(decoded) || decoded[i] != '\n' {
		return "", "", false
	}
	return decoded[:i], decoded[i+1:], true
}

// takeUTF16Units consumes runes from s until their cumulative UTF-16 code
// unit count reaches want (runes outside the BMP count as 2 units, via a
// surrogate pair). It returns the payload string, the number of *bytes* of s
// consumed to produce it, and whether want units were actually reached.
func takeUTF16Units(s string, want int) (payload string, byteLen int, complete bool) {
	if want == 0 {
		return "", 0, true
	}

	units := 0
	i := 0
	for i < len(s) {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size <= 1 {
			// Incomplete rune at the tail of the already-decoded prefix --
			// wait for more bytes.
			break
		}

		units += utf16RuneLen(r)
		i += size

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
