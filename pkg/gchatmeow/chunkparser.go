package gchatmeow

import (
	"strconv"
	"unicode/utf16"
	"unicode/utf8"
)

// ChunkParser decodes BrowserChannel "<length>\n<payload>" framing, where length counts UTF-16 code units.
// Keep the buffer as raw bytes: a []byte->string conversion would turn a rune split across reads into U+FFFD and desync framing.
type ChunkParser struct {
	buf []byte
}

// Feed appends raw bytes and returns all complete payloads; incomplete data stays buffered.
func (p *ChunkParser) Feed(data []byte) []string {
	p.buf = append(p.buf, data...)

	var chunks []string
	for {
		// Holds back an incomplete trailing UTF-8 sequence.
		decoded := decodeUTF8Prefix(p.buf)

		lengthStr, rest, res := cutFirstLine(decoded)
		switch res {
		case lineIncomplete:
			return chunks
		case linePoisoned:
			// Unrecoverable byte: drop it and resync so the parser always makes progress.
			p.buf = p.buf[1:]
			continue
		}

		length, err := strconv.Atoi(lengthStr)
		if err != nil {
			break
		}

		payload, payloadByteLen, complete := takeUTF16Units(rest, length)
		if !complete {
			break
		}

		chunks = append(chunks, payload)

		lengthPrefixByteLen := len(lengthStr) + 1
		drop := lengthPrefixByteLen + payloadByteLen
		p.buf = p.buf[drop:]
	}

	return chunks
}

// decodeUTF8Prefix decodes buf, holding back a trailing incomplete UTF-8 sequence (invalid bytes are kept verbatim).
func decodeUTF8Prefix(buf []byte) string {
	if utf8.Valid(buf) {
		return string(buf)
	}

	i := 0
	for i < len(buf) {
		r, size := utf8.DecodeRune(buf[i:])
		if r == utf8.RuneError && size <= 1 {
			// Incomplete sequence: leave it buffered for the next Feed.
			if !utf8.FullRune(buf[i:]) {
				break
			}
			// Invalid byte (not a truncation): include it verbatim and move on.
			i++
			continue
		}
		i += size
	}
	return string(buf[:i])
}

type lineResult int

const (
	// lineIncomplete: still a growable prefix of "<digits>\n"; wait for more data.
	lineIncomplete lineResult = iota
	// lineOK: a complete token at position 0.
	lineOK
	// linePoisoned: can never become a valid length token; the caller must resync.
	linePoisoned
)

// cutFirstLine matches a "<digits>\n" token anchored at position 0.
func cutFirstLine(decoded string) (lengthStr string, rest string, res lineResult) {
	i := 0
	for i < len(decoded) && decoded[i] >= '0' && decoded[i] <= '9' {
		i++
	}
	if i >= len(decoded) {
		return "", "", lineIncomplete
	}
	if i == 0 || decoded[i] != '\n' {
		// Only reachable with invalid bytes.
		return "", "", linePoisoned
	}
	return decoded[:i], decoded[i+1:], lineOK
}

// takeUTF16Units consumes runes from s until want UTF-16 units are reached, returning the payload and bytes consumed.
// A RuneError here is an invalid byte, never a truncation; it counts as one unit and one byte.
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

func utf16RuneLen(r rune) int {
	if r1, r2 := utf16.EncodeRune(r); r1 == utf8.RuneError && r2 == utf8.RuneError {
		return 1
	}
	return 2
}
