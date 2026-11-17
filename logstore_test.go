package xxljob

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryLogStore(t *testing.T) {
	// Scenario: the in-memory log store enforces a limit on total bytes
	// Given: total limit 64 bytes (at most 8 bytes per run); log 1 writes 3 lines (4 bytes each, newline included) and
	//        ends; then logs 2..9 write 2 lines each
	// Expect: log 1's 3rd line is dropped by the per-run limit and noted when the run ends; once the total is exceeded
	//         the oldest log 1 is evicted; the newest log 9 is intact; chunked reads report correct line numbers
	s := NewMemoryLogStore(64)
	ref := func(id int64) LogRef { return LogRef{LogID: id} }
	for _, line := range []string{"abc", "def", "ghi"} {
		require.NoError(t, s.Write(ref(1), line))
	}
	require.NoError(t, s.End(ref(1)))

	c1, err := s.Read(ref(1), 2)
	require.NoError(t, err)
	assert.Equal(t, LogChunk{FromLine: 2, ToLine: 3, Content: "def\n... 1 more lines dropped (log size limit 8 bytes per run)\n", End: true}, c1)

	for id := int64(2); id <= 9; id++ {
		require.NoError(t, s.Write(ref(id), "l1x"))
		require.NoError(t, s.Write(ref(id), "l2x"))
	}
	_, err = s.Read(ref(1), 1)
	require.ErrorIs(t, err, ErrLogNotFound)
	c9, err := s.Read(ref(9), 1)
	require.NoError(t, err)
	assert.Equal(t, LogChunk{FromLine: 1, ToLine: 2, Content: "l1x\nl2x\n"}, c9)
	assert.LessOrEqual(t, s.size, 64)
}

func TestMemoryLogStoreKeepsRunningJobs(t *testing.T) {
	// Scenario: several jobs run at the same time and the total log size reaches the limit
	// Given: total limit 64 bytes (at most 8 per run); log 1 is still running and log 2 has ended (8 bytes each); logs
	//        3..9 are then written to fill the store
	// Expect: the ended log 2 is evicted first; the log of the still-running log 1 is kept intact
	s := NewMemoryLogStore(64)
	ref := func(id int64) LogRef { return LogRef{LogID: id} }
	require.NoError(t, s.Write(ref(1), "run"))
	require.NoError(t, s.Write(ref(1), "nin"))
	require.NoError(t, s.Write(ref(2), "end"))
	require.NoError(t, s.Write(ref(2), "ded"))
	require.NoError(t, s.End(ref(2)))
	for id := int64(3); id <= 9; id++ {
		require.NoError(t, s.Write(ref(id), "l1x"))
		require.NoError(t, s.Write(ref(id), "l2x"))
	}

	running, err := s.Read(ref(1), 1)
	_, evicted := s.Read(ref(2), 1)

	require.NoError(t, err)
	assert.Equal(t, "run\nnin\n", running.Content)
	assert.ErrorIs(t, evicted, ErrLogNotFound)
}

func TestLogLineTruncated(t *testing.T) {
	// Scenario: a job writes an overly long log line
	// Given: a single 20KB line
	// Expect: the stored line is truncated to 8KB and marked as truncated
	s := NewMemoryLogStore(0)
	w := &lineWriter{store: s, ref: LogRef{LogID: 1}}

	_, err := w.Write([]byte(strings.Repeat("x", 20<<10) + "\n"))
	require.NoError(t, err)
	c, err := s.Read(LogRef{LogID: 1}, 1)
	require.NoError(t, err)

	assert.Len(t, c.Content, maxLogLineBytes+len("... (truncated)")+1)
	assert.True(t, strings.HasSuffix(c.Content, "... (truncated)\n"))
}

func TestFileLogStore(t *testing.T) {
	// Scenario: the file log store writes, reads, and cleans up by day
	// Given: 3-day retention; log 7 (today) gets two lines, one of which contains a newline; the directory also holds a
	//        directory from 10 days ago and a non-date directory
	// Expect: the file is at {dir}/{date}/7.log; the newline is replaced with a space; reading from line 2 returns line 2;
	//         cleanup removes only the 10-day-old directory
	dir := t.TempDir()
	s, err := NewFileLogStore(dir, 3)
	require.NoError(t, err)
	now := time.Now()
	ref := LogRef{LogID: 7, LogTime: now}
	old := filepath.Join(dir, now.AddDate(0, 0, -10).Format(time.DateOnly))
	other := filepath.Join(dir, "not-a-date")
	require.NoError(t, os.MkdirAll(old, 0o755))
	require.NoError(t, os.MkdirAll(other, 0o755))

	require.NoError(t, s.Write(ref, "first"))
	require.NoError(t, s.Write(ref, "second\nline"))
	require.NoError(t, s.End(ref))
	c, err := s.Read(ref, 2)
	require.NoError(t, err)
	_, missing := s.Read(LogRef{LogID: 8, LogTime: now}, 1)
	s.clean(now)

	assert.Equal(t, LogChunk{FromLine: 2, ToLine: 2, Content: "second line\n"}, c)
	require.ErrorIs(t, missing, ErrLogNotFound)
	assert.FileExists(t, filepath.Join(dir, now.Format(time.DateOnly), "7.log"))
	assert.NoDirExists(t, old)
	assert.DirExists(t, other)
	require.NoError(t, s.Close())
}

// readAll pages through a log the way the admin does (asking again from ToLine+1) and returns every chunk.
func readAll(t *testing.T, s LogStore, ref LogRef) []LogChunk {
	t.Helper()
	var chunks []LogChunk
	for from := 1; len(chunks) < 100; {
		c, err := s.Read(ref, from)
		require.NoError(t, err)
		if c.ToLine < c.FromLine {
			return chunks
		}
		chunks = append(chunks, c)
		from = c.ToLine + 1
	}
	t.Fatal("log never ran out")
	return nil
}

func TestLogReadIsPaged(t *testing.T) {
	// Scenario: a run writes a log larger than MaxLogChunkBytes and the admin reads it
	// Given: 2500 lines of 1000 bytes (about 2.4MB) in the memory store and in the file store
	// Expect: each read returns at most MaxLogChunkBytes; paging from ToLine+1 returns every line exactly once and in
	//         order; the memory store reports End only on the chunk that reaches the last line
	line := strings.Repeat("x", 999)
	fileStore, err := NewFileLogStore(t.TempDir(), 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = fileStore.Close() })
	stores := map[string]LogStore{"memory": NewMemoryLogStore(64 << 20), "file": fileStore}

	for name, s := range stores {
		t.Run(name, func(t *testing.T) {
			ref := LogRef{LogID: 1, LogTime: time.Now()}
			for i := range 2500 {
				require.NoError(t, s.Write(ref, fmt.Sprintf("%04d%s", i, line[4:])))
			}
			require.NoError(t, s.End(ref))

			chunks := readAll(t, s, ref)

			require.Len(t, chunks, 3)
			var got []string
			for i, c := range chunks {
				assert.LessOrEqual(t, len(c.Content), MaxLogChunkBytes)
				assert.Equal(t, name == "memory" && i == len(chunks)-1, c.End, "chunk %d", i)
				got = append(got, strings.Split(strings.TrimSuffix(c.Content, "\n"), "\n")...)
			}
			require.Len(t, got, 2500)
			for i, l := range got {
				require.Equal(t, fmt.Sprintf("%04d", i), l[:4])
			}
		})
	}
}

func TestFileLogStoreSkipsIncompleteLine(t *testing.T) {
	// Scenario: the admin reads a file log while a line is still being written
	// Given: a file with one complete line followed by a partial line without its newline
	// Expect: only the complete line is returned; once the line is finished, the next read from line 2 returns it whole
	dir := t.TempDir()
	s, err := NewFileLogStore(dir, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	ref := LogRef{LogID: 9, LogTime: time.Now()}
	require.NoError(t, s.Write(ref, "done"))
	f, err := os.OpenFile(s.path(ref), os.O_WRONLY|os.O_APPEND, 0o600)
	require.NoError(t, err)
	_, err = f.WriteString("half")
	require.NoError(t, err)

	partial, err := s.Read(ref, 1)
	require.NoError(t, err)
	_, err = f.WriteString(" of a line\n")
	require.NoError(t, err)
	require.NoError(t, f.Close())
	rest, err := s.Read(ref, partial.ToLine+1)
	require.NoError(t, err)

	assert.Equal(t, LogChunk{FromLine: 1, ToLine: 1, Content: "done\n"}, partial)
	assert.Equal(t, LogChunk{FromLine: 2, ToLine: 2, Content: "half of a line\n"}, rest)
}

func TestLogChunkOversizedLine(t *testing.T) {
	// Scenario: a single stored line is larger than MaxLogChunkBytes (possible with a custom writer)
	// Given: lines of 2MB, 1 byte
	// Expect: the big line is returned alone so reading always makes progress; the next read returns the rest
	big := strings.Repeat("y", 2<<20)

	first := chunkOf([]string{big, "z"}, 1, true)
	second := chunkOf([]string{big, "z"}, 2, true)

	assert.Equal(t, 1, first.ToLine)
	assert.False(t, first.End)
	assert.Equal(t, LogChunk{FromLine: 2, ToLine: 2, Content: "z\n", End: true}, second)
}
