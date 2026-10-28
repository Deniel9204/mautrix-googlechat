package gchatmeow

import (
	"reflect"
	"testing"
)

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
	// Split the 4-byte UTF-8 sequence of 😀 after 2 bytes.
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

func TestEmptyFeed(t *testing.T) {
	p := &ChunkParser{}
	got := p.Feed(nil)
	assertChunks(t, "Feed", got, nil)
}

func TestEmptyBuffer_NoNewlineYet(t *testing.T) {
	// No newline yet: nothing yielded.
	p := &ChunkParser{}
	got := p.Feed([]byte("123"))
	assertChunks(t, "Feed", got, nil)
}

func TestZeroLengthChunk(t *testing.T) {
	// A "0" length token is valid and yields an empty payload.
	p := &ChunkParser{}
	got := p.Feed([]byte("0\n5\nhello"))
	assertChunks(t, "Feed", got, []string{"", "hello"})
}

func TestLengthNotYetTerminatedByNewline(t *testing.T) {
	// The '\n' terminator is required.
	p := &ChunkParser{}
	got1 := p.Feed([]byte("5"))
	assertChunks(t, "Feed 1", got1, nil)

	got2 := p.Feed([]byte("\nhello"))
	assertChunks(t, "Feed 2", got2, []string{"hello"})
}

func TestPartialPayloadBuffered(t *testing.T) {
	// Partial payloads accumulate until the declared length arrives.
	p := &ChunkParser{}
	got1 := p.Feed([]byte("11\nhello"))
	assertChunks(t, "Feed 1", got1, nil)

	got2 := p.Feed([]byte(" wor"))
	assertChunks(t, "Feed 2", got2, nil)

	got3 := p.Feed([]byte("ld"))
	assertChunks(t, "Feed 3", got3, []string{"hello world"})
}

func TestChunkFollowedByPartialLength(t *testing.T) {
	// Remainder after a complete chunk resumes correctly.
	p := &ChunkParser{}
	got1 := p.Feed([]byte("2\nhi1"))
	assertChunks(t, "Feed 1", got1, []string{"hi"})

	got2 := p.Feed([]byte("\nx"))
	assertChunks(t, "Feed 2", got2, []string{"x"})
}

// Invalid (non-truncated) UTF-8 must not stall the parser.

func TestInvalidByteInPayloadDoesNotStallForever(t *testing.T) {
	// Each invalid byte counts as one unit.
	p := &ChunkParser{}
	got := p.Feed([]byte("5\nab\xffcd"))
	assertChunks(t, "Feed", got, []string{"ab\xffcd"})

	got2 := p.Feed([]byte("2\nhi"))
	assertChunks(t, "Feed 2", got2, []string{"hi"})
}

func TestInvalidByteInLengthHeaderResyncs(t *testing.T) {
	// "5\xff" can never become a length token; the parser must resync.
	p := &ChunkParser{}
	got := p.Feed([]byte("5\xffgarbage2\nhi"))
	assertChunks(t, "Feed", got, []string{"hi"})

	got2 := p.Feed([]byte("3\nfoo"))
	assertChunks(t, "Feed 2", got2, []string{"foo"})
}

func TestMultipleAstralCharsAcrossFeeds(t *testing.T) {
	// Byte stream cut inside the second astral rune.
	p := &ChunkParser{}
	got1 := p.Feed([]byte("4\n\xf0\x9f\x98\x80\xf0\x9f"))
	assertChunks(t, "Feed 1", got1, nil)

	got2 := p.Feed([]byte("\x98\x81"))
	assertChunks(t, "Feed 2", got2, []string{"😀😁"})
}
