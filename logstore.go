package xxljob

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ErrLogNotFound means the execution log does not exist: it was cleaned up, the run happened on another instance,
// or the executor restarted.
var ErrLogNotFound = errors.New("xxljob: log not found")

// LogRef identifies the execution log of one trigger.
type LogRef struct {
	LogID   int64
	LogTime time.Time
}

// LogChunk is a slice of an execution log: lines FromLine to ToLine, 1-based and inclusive.
type LogChunk struct {
	FromLine int
	ToLine   int
	Content  string
	// End reports that the run has finished and the log has been read to the end. When unknown it is false and the
	// admin decides from the run status.
	End bool
}

// MaxLogChunkBytes caps the content returned by one read of an execution log. The admin pages through longer logs
// by asking again from the next line, so a large log never has to be loaded or sent at once.
const MaxLogChunkBytes = 1 << 20

// LogStore stores execution logs. The admin reads them by logId in chunks (with an increasing fromLineNum).
// Implementations must be safe for concurrent use.
type LogStore interface {
	// Write appends a line.
	Write(ref LogRef, line string) error
	// End marks the run as finished; nothing is written afterwards.
	End(ref LogRef) error
	// Read returns the log from line fromLine (1-based), up to its current end or about MaxLogChunkBytes, whichever
	// comes first. It returns ErrLogNotFound if the run has no log.
	Read(ref LogRef, fromLine int) (LogChunk, error)
	// Close releases resources.
	Close() error
}

// ---------------------------------------------------------------------------------------------
// In-memory store
// ---------------------------------------------------------------------------------------------

// DefaultMemoryLogBytes is the default total size limit of MemoryLogStore.
const DefaultMemoryLogBytes = 16 << 20

// MemoryLogStore keeps recent execution logs in memory, up to maxBytes in total. When the limit is reached the
// oldest runs are evicted, finished runs first. A single run may use at most maxBytes/8; further lines are dropped
// and noted when the run ends. Logs are lost on restart. It suits services that prefer not to write files.
type MemoryLogStore struct {
	maxBytes, maxRunBytes int

	mu    sync.Mutex
	runs  map[int64]*memoryLog
	order []int64 // insertion order, oldest first
	size  int
}

type memoryLog struct {
	lines   []string
	size    int
	dropped int
	ended   bool
}

// NewMemoryLogStore returns a MemoryLogStore limited to maxBytes in total; a value <= 0 means
// DefaultMemoryLogBytes (16MB).
func NewMemoryLogStore(maxBytes int) *MemoryLogStore {
	if maxBytes <= 0 {
		maxBytes = DefaultMemoryLogBytes
	}
	return &MemoryLogStore{maxBytes: maxBytes, maxRunBytes: max(maxBytes/8, 1), runs: map[int64]*memoryLog{}}
}

// Write implements LogStore.
func (s *MemoryLogStore) Write(ref LogRef, line string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	l := s.runs[ref.LogID]
	if l == nil {
		l = &memoryLog{}
		s.runs[ref.LogID] = l
		s.order = append(s.order, ref.LogID)
	}
	n := len(line) + 1
	if l.size+n > s.maxRunBytes {
		l.dropped++
		return nil
	}
	for s.size+n > s.maxBytes && s.evictOldest(ref.LogID) {
	}
	if s.size+n > s.maxBytes {
		l.dropped++
		return nil
	}
	l.lines = append(l.lines, line)
	l.size += n
	s.size += n
	return nil
}

// evictOldest evicts the oldest run other than keep and reports whether it evicted one. Finished runs go first:
// when many jobs log at once and the store is full, a running job must not lose the beginning of its log.
func (s *MemoryLogStore) evictOldest(keep int64) bool {
	for _, endedOnly := range []bool{true, false} {
		for i, id := range s.order {
			if id == keep || (endedOnly && !s.runs[id].ended) {
				continue
			}
			s.size -= s.runs[id].size
			delete(s.runs, id)
			s.order = append(s.order[:i:i], s.order[i+1:]...)
			return true
		}
	}
	return false
}

// End implements LogStore.
func (s *MemoryLogStore) End(ref LogRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if l := s.runs[ref.LogID]; l != nil && !l.ended {
		l.ended = true
		if l.dropped > 0 {
			l.lines = append(l.lines, fmt.Sprintf("... %d more lines dropped (log size limit %d bytes per run)", l.dropped, s.maxRunBytes))
		}
	}
	return nil
}

// Read implements LogStore.
func (s *MemoryLogStore) Read(ref LogRef, fromLine int) (LogChunk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l := s.runs[ref.LogID]
	if l == nil {
		return LogChunk{}, ErrLogNotFound
	}
	return chunkOf(l.lines, fromLine, l.ended), nil
}

// Close implements LogStore.
func (s *MemoryLogStore) Close() error { return nil }

// chunkOf returns the lines from fromLine, stopping before the chunk would exceed MaxLogChunkBytes (it always
// contains at least one line when one is available). End is reported only when the chunk reaches the last line of an
// ended run.
func chunkOf(lines []string, fromLine int, ended bool) LogChunk {
	fromLine = max(fromLine, 1)
	var b chunkBuilder
	b.reset(fromLine)
	for _, l := range lines[min(fromLine-1, len(lines)):] {
		if !b.add(l) {
			break
		}
	}
	c := b.chunk()
	c.End = ended && c.ToLine >= len(lines)
	return c
}

// chunkBuilder accumulates lines into a LogChunk, up to MaxLogChunkBytes.
type chunkBuilder struct {
	from, to int
	sb       strings.Builder
}

func (b *chunkBuilder) reset(from int) { b.from, b.to = from, from-1 }

// add appends a line and reports whether more lines fit.
func (b *chunkBuilder) add(line string) bool {
	if b.sb.Len() > 0 && b.sb.Len()+len(line)+1 > MaxLogChunkBytes {
		return false
	}
	b.sb.WriteString(line)
	b.sb.WriteByte('\n')
	b.to++
	return b.sb.Len() < MaxLogChunkBytes
}

func (b *chunkBuilder) chunk() LogChunk {
	return LogChunk{FromLine: b.from, ToLine: b.to, Content: b.sb.String()}
}

// ---------------------------------------------------------------------------------------------
// File store
// ---------------------------------------------------------------------------------------------

// FileLogStore writes logs to {dir}/{yyyy-MM-dd}/{logId}.log (dated by the trigger's log time in the local time
// zone), the same layout as the official executor. A background goroutine removes date directories older than the
// retention period every hour.
type FileLogStore struct {
	dir       string
	retention int

	mu    sync.Mutex
	files map[int64]*os.File // open files of running jobs

	stop chan struct{}
	done chan struct{}
	once sync.Once
}

// NewFileLogStore returns a FileLogStore rooted at dir that keeps logs for retentionDays days; a value <= 0
// disables cleanup.
func NewFileLogStore(dir string, retentionDays int) (*FileLogStore, error) {
	if dir == "" {
		return nil, errors.New("xxljob: log dir is required")
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("xxljob: create log dir: %w", err)
	}
	s := &FileLogStore{dir: dir, retention: retentionDays, files: map[int64]*os.File{},
		stop: make(chan struct{}), done: make(chan struct{})}
	go s.cleanLoop()
	return s, nil
}

func (s *FileLogStore) path(ref LogRef) string {
	return filepath.Join(s.dir, ref.LogTime.Format(time.DateOnly), strconv.FormatInt(ref.LogID, 10)+".log")
}

// Write implements LogStore.
func (s *FileLogStore) Write(ref LogRef, line string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.files[ref.LogID]
	if f == nil {
		p := s.path(ref)
		if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			return fmt.Errorf("xxljob: create log dir: %w", err)
		}
		var err error
		if f, err = os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600); err != nil { //nolint:gosec // G304: path is dir/date/logId, built from trusted values
			return fmt.Errorf("xxljob: open log file: %w", err)
		}
		s.files[ref.LogID] = f
	}
	_, err := f.WriteString(strings.ReplaceAll(line, "\n", " ") + "\n")
	return err
}

// End implements LogStore.
func (s *FileLogStore) End(ref LogRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.files[ref.LogID]
	if f == nil {
		return nil
	}
	delete(s.files, ref.LogID)
	return f.Close()
}

// Read implements LogStore. It streams the file, skipping the lines before fromLine, and returns only complete
// lines, so a line that is being written is never cut in half. The file store does not record whether a run has
// finished (End is always false); the admin decides from the run status.
func (s *FileLogStore) Read(ref LogRef, fromLine int) (LogChunk, error) {
	f, err := os.Open(s.path(ref))
	if errors.Is(err, os.ErrNotExist) {
		return LogChunk{}, ErrLogNotFound
	}
	if err != nil {
		return LogChunk{}, fmt.Errorf("xxljob: open log file: %w", err)
	}
	defer func() { _ = f.Close() }()

	fromLine = max(fromLine, 1)
	var b chunkBuilder
	b.reset(fromLine)
	r := bufio.NewReader(f)
	for n := 1; ; n++ {
		line, err := r.ReadString('\n')
		if err != nil { // io.EOF: the last line is incomplete (still being written) or empty
			if errors.Is(err, io.EOF) {
				break
			}
			return LogChunk{}, fmt.Errorf("xxljob: read log file: %w", err)
		}
		if n >= fromLine && !b.add(strings.TrimSuffix(line, "\n")) {
			break
		}
	}
	return b.chunk(), nil
}

// Close implements LogStore: it stops the cleanup and closes open files.
func (s *FileLogStore) Close() error {
	s.once.Do(func() { close(s.stop) })
	<-s.done
	s.mu.Lock()
	defer s.mu.Unlock()
	var errs []error
	for id, f := range s.files {
		errs = append(errs, f.Close())
		delete(s.files, id)
	}
	return errors.Join(errs...)
}

func (s *FileLogStore) cleanLoop() {
	defer close(s.done)
	if s.retention <= 0 {
		<-s.stop
		return
	}
	tick := time.NewTicker(time.Hour)
	defer tick.Stop()
	for {
		s.clean(time.Now())
		select {
		case <-s.stop:
			return
		case <-tick.C:
		}
	}
}

// clean removes date directories older than now minus the retention period. Directories whose names are not
// dates are left alone.
func (s *FileLogStore) clean(now time.Time) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return
	}
	y, m, d := now.Date()
	cutoff := time.Date(y, m, d, 0, 0, 0, 0, time.Local).AddDate(0, 0, -s.retention)
	for _, e := range entries {
		day, err := time.ParseInLocation(time.DateOnly, e.Name(), time.Local)
		if err != nil || !e.IsDir() || !day.Before(cutoff) {
			continue
		}
		_ = os.RemoveAll(filepath.Join(s.dir, e.Name()))
	}
}
