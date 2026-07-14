package gchatmeow

import (
	"reflect"
	"testing"
)

// assertChunks compares got against want, treating both nil and empty slices
// as "no chunks" (Feed legitimately returns nil when nothing completed).
func assertChunks(t *testing.T, label string, got, want []string) {
	t.Helper()
	if len(got) == 0 && len(want) == 0 {
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s: got %#v, want %#v", label, got, want)
	}
}

func TestSimpleChunk(t *testing.T) {
	p := &ChunkParser{}
	got := p.Feed([]byte("5\nhello"))
	assertChunks(t, "Feed", got, []string{"hello"})
}

func TestMultipleChunksOneFeed(t *testing.T) {
	p := &ChunkParser{}
	got := p.Feed([]byte("2\nhi3\nfoo"))
	assertChunks(t, "Feed", got, []string{"hi", "foo"})
}

func TestChunkSplitAcrossFeeds(t *testing.T) {
	p := &ChunkParser{}
	got1 := p.Feed([]byte("5\nhel"))
	assertChunks(t, "Feed 1", got1, nil)

	got2 := p.Feed([]byte("lo"))
	assertChunks(t, "Feed 2", got2, []string{"hello"})
}

func TestLengthSplitAcrossFeeds(t *testing.T) {
	p := &ChunkParser{}
	got1 := p.Feed([]byte("1"))
	assertChunks(t, "Feed 1", got1, nil)

	got2 := p.Feed([]byte("1\nhello world"))
	assertChunks(t, "Feed 2", got2, []string{"hello world"})
}

func TestAstralCharCountsAsTwo(t *testing.T) {
	// "😀a" is U+1F600 (a surrogate pair, 2 UTF-16 code units) + "a" (1 unit) = 3.
	p := &ChunkParser{}
	got := p.Feed([]byte("3\n😀a"))
	assertChunks(t, "Feed", got, []string{"😀a"})
}

func TestMultibyteSplitMidRune(t *testing.T) {
	// 😀 is 4 UTF-8 bytes (F0 9F 98 80) and 2 UTF-16 code units. Split the
	// byte stream in the middle of that 4-byte sequence, after 2 bytes.
	p := &ChunkParser{}
	got1 := p.Feed([]byte("2\n\xf0\x9f"))
	assertChunks(t, "Feed 1", got1, nil)

	got2 := p.Feed([]byte("\x98\x80"))
	assertChunks(t, "Feed 2", got2, []string{"😀"})
}

func TestBMPNonASCII(t *testing.T) {
	// "é" is 1 UTF-16 code unit (U+00E9, BMP) but 2 UTF-8 bytes (0xC3 0xA9).
	p := &ChunkParser{}
	got := p.Feed([]byte("2\néa"))
	assertChunks(t, "Feed", got, []string{"éa"})
}

// --- Additional edge cases found while reading channel.py's get_chunks ---

func TestEmptyFeed(t *testing.T) {
	p := &ChunkParser{}
	got := p.Feed(nil)
	assertChunks(t, "Feed", got, nil)
}

func TestEmptyBuffer_NoNewlineYet(t *testing.T) {
	// No newline at all yet -> LEN_REGEX doesn't match -> nothing yielded,
	// buffer just accumulates.
	p := &ChunkParser{}
	got := p.Feed([]byte("123"))
	assertChunks(t, "Feed", got, nil)
}

func TestZeroLengthChunk(t *testing.T) {
	// Python's LEN_REGEX = r"([0-9]+)\n" matches "0" just fine.
	p := &ChunkParser{}
	got := p.Feed([]byte("0\n5\nhello"))
	assertChunks(t, "Feed", got, []string{"", "hello"})
}

func TestLengthNotYetTerminatedByNewline(t *testing.T) {
	// "5" alone (no trailing \n) must not be treated as a complete length --
	// Python's regex requires the \n. Confirm nothing is yielded and the
	// eventual full frame ("5\nhello") is decoded correctly once completed.
	p := &ChunkParser{}
	got1 := p.Feed([]byte("5"))
	assertChunks(t, "Feed 1", got1, nil)

	got2 := p.Feed([]byte("\nhello"))
	assertChunks(t, "Feed 2", got2, []string{"hello"})
}

func TestPartialPayloadBuffered(t *testing.T) {
	// Length is known but fewer UTF-16 units are present than declared --
	// nothing should be yielded until the rest arrives, and multiple partial
	// feeds should accumulate correctly.
	p := &ChunkParser{}
	got1 := p.Feed([]byte("11\nhello"))
	assertChunks(t, "Feed 1", got1, nil)

	got2 := p.Feed([]byte(" wor"))
	assertChunks(t, "Feed 2", got2, nil)

	got3 := p.Feed([]byte("ld"))
	assertChunks(t, "Feed 3", got3, []string{"hello world"})
}

func TestChunkFollowedByPartialLength(t *testing.T) {
	// A complete chunk followed by the start of the next frame's length,
	// split across Feeds -- exercises the buffer-trim + resume path with a
	// non-empty remainder.
	p := &ChunkParser{}
	got1 := p.Feed([]byte("2\nhi1"))
	assertChunks(t, "Feed 1", got1, []string{"hi"})

	got2 := p.Feed([]byte("\nx"))
	assertChunks(t, "Feed 2", got2, []string{"x"})
}

func TestMultipleAstralCharsAcrossFeeds(t *testing.T) {
	// Two astral chars back to back (4 UTF-16 units total), byte stream cut
	// at an arbitrary point inside the second rune.
	p := &ChunkParser{}
	got1 := p.Feed([]byte("4\n\xf0\x9f\x98\x80\xf0\x9f"))
	assertChunks(t, "Feed 1", got1, nil)

	got2 := p.Feed([]byte("\x98\x81"))
	assertChunks(t, "Feed 2", got2, []string{"😀😁"})
}
