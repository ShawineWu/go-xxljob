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
	// Scenario: write, read from a line, end, and read an unknown log
	// Expect: reads return the lines from fromLine to the end; End is reported once the run has ended; past the end
	//         a read is empty with ToLine = FromLine-1
	s := NewMemoryLogStore(0)
	ref := LogRef{LogID: 1}
	for _, l := range []string{"a", "b", "c"} {
		require.NoError(t, s.Write(ref, l))
	}
	running, err := s.Read(ref, 2)
	require.NoError(t, err)
	require.NoError(t, s.End(ref))
	ended, err := s.Read(ref, 1)
	require.NoError(t, err)
	past, err := s.Read(ref, 9)
	require.NoError(t, err)
	_, missing := s.Read(LogRef{LogID: 2}, 1)

	assert.Equal(t, LogChunk{FromLine: 2, ToLine: 3, Content: "b\nc\n"}, running)
	assert.Equal(t, LogChunk{FromLine: 1, ToLine: 3, Content: "a\nb\nc\n", End: true}, ended)
	assert.Equal(t, LogChunk{FromLine: 9, ToLine: 8, End: true}, past)
	require.ErrorIs(t, missing, ErrLogNotFound)
}

func TestMemoryLogStoreLimits(t *testing.T) {
	// Scenario: a store of 1KB (at most 128B per run) receives a run that logs too much, then many small runs
	// Expect: the big run keeps what fits and notes how many lines were dropped (the note is accounted too); old logs
	//         are evicted, finished ones first; the store never exceeds its size, whatever the number of runs
	s := NewMemoryLogStore(1024)
	big := LogRef{LogID: 1}
	for i := range 20 {
		require.NoError(t, s.Write(big, fmt.Sprintf("line %02d", i)))
	}
	require.NoError(t, s.End(big))
	bigLog, err := s.Read(big, 1)
	require.NoError(t, err)

	running := LogRef{LogID: 2}
	require.NoError(t, s.Write(running, "still running"))
	for id := int64(3); id < 200; id++ {
		require.NoError(t, s.Write(LogRef{LogID: id}, "x"))
		require.NoError(t, s.End(LogRef{LogID: id}))
		require.LessOrEqual(t, s.size, 1024)
	}
	_, bigErr := s.Read(big, 1)
	stillRunning, runErr := s.Read(running, 1)

	assert.Contains(t, bigLog.Content, "line 00")
	assert.Contains(t, bigLog.Content, "more lines dropped (log size limit 128 bytes per run)")
	require.ErrorIs(t, bigErr, ErrLogNotFound, "finished logs are evicted first")
	require.NoError(t, runErr, "a running log outlives finished ones")
	assert.Equal(t, "still running\n", stillRunning.Content)
	assert.Equal(t, s.size, accounted(s))
}

// accounted recomputes the store's size from its entries.
func accounted(s *MemoryLogStore) int {
	n := 0
	for _, e := range s.runs {
		n += e.Value.(*memoryLog).size
	}
	return n
}

func TestMemoryLogStoreEvictionIsCheap(t *testing.T) {
	// Scenario: a full 1MB store keeps receiving lines of new runs
	// Expect: every write stays cheap (eviction does not copy the store's index)
	s := NewMemoryLogStore(1 << 20)
	for id := range int64(20000) {
		require.NoError(t, s.Write(LogRef{LogID: id}, strings.Repeat("x", 100)))
		require.NoError(t, s.End(LogRef{LogID: id}))
	}

	allocs := testing.AllocsPerRun(1000, func() {
		id := time.Now().UnixNano()
		_ = s.Write(LogRef{LogID: id}, strings.Repeat("y", 100))
	})

	assert.Less(t, allocs, 10.0)
}

func TestFileLogStore(t *testing.T) {
	// Scenario: write a log with a multi-line record, read it from a line, end it, read an unknown log
	// Expect: {dir}/{date}/{logId}.log with newlines flattened; reads return complete lines to the end
	dir := t.TempDir()
	s, err := NewFileLogStore(dir, 0)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	now := time.Now()
	ref := LogRef{LogID: 7, LogTime: now}

	require.NoError(t, s.Write(ref, "first"))
	require.NoError(t, s.Write(ref, "second\nline"))
	require.NoError(t, s.End(ref))
	c, err := s.Read(ref, 2)
	require.NoError(t, err)
	_, missing := s.Read(LogRef{LogID: 8, LogTime: now}, 1)

	assert.Equal(t, LogChunk{FromLine: 2, ToLine: 2, Content: "second line\n"}, c)
	require.ErrorIs(t, missing, ErrLogNotFound)
	assert.FileExists(t, filepath.Join(dir, now.Format(time.DateOnly), "7.log"))
}

func TestFileLogStoreSkipsIncompleteLine(t *testing.T) {
	// Scenario: the admin reads a log while a line is being written
	// Expect: only complete lines are returned; the next read from the following line gets the finished line whole
	dir := t.TempDir()
	s, err := NewFileLogStore(dir, 0)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
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

func TestFileLogStoreRunLimit(t *testing.T) {
	// Scenario: a run writes more than DefaultFileLogRunBytes
	// Expect: the file stops growing at the limit and ends with a note of the dropped lines
	dir := t.TempDir()
	s, err := NewFileLogStore(dir, 0)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	ref := LogRef{LogID: 1, LogTime: time.Now()}
	line := strings.Repeat("z", 64<<10)

	for range DefaultFileLogRunBytes/len(line) + 10 {
		require.NoError(t, s.Write(ref, line))
	}
	require.NoError(t, s.End(ref))
	info, err := os.Stat(s.path(ref))
	require.NoError(t, err)
	data, err := os.ReadFile(s.path(ref))
	require.NoError(t, err)

	assert.LessOrEqual(t, info.Size(), int64(DefaultFileLogRunBytes+200))
	assert.Contains(t, string(data[len(data)-200:]), "more lines dropped")
}

func TestFileLogStoreRetention(t *testing.T) {
	// Scenario: 3-day retention with date directories from today back to 10 days ago, two far in the future, and a
	//           directory that is not a date
	// Expect: exactly 3 days are kept (today and the two before); old and far-future directories are removed; other
	//         directories are left alone
	dir := t.TempDir()
	s, err := NewFileLogStore(dir, 3)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()
	now := time.Now()
	day := func(offset int) string { return now.AddDate(0, 0, offset).Format(time.DateOnly) }
	for _, name := range []string{day(0), day(-1), day(-2), day(-3), day(-10), day(1), day(400), "9999-12-31", "not-a-date"} {
		require.NoError(t, os.MkdirAll(filepath.Join(dir, name), 0o750))
	}

	s.clean(now)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	var kept []string
	for _, e := range entries {
		kept = append(kept, e.Name())
	}

	assert.ElementsMatch(t, []string{day(0), day(-1), day(-2), day(1), "not-a-date"}, kept)
}
