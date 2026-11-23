package xxljob

import (
	"bufio"
	"container/list"
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

// LogChunk is a part of an execution log: lines FromLine to ToLine, 1-based and inclusive. A chunk without lines
// has ToLine == FromLine - 1.
type LogChunk struct {
	FromLine int
	ToLine   int
	Content  string
	// End reports that the run has finished and the log has been read to its end. When the store does not know,
	// it is false and the admin decides from the run's status.
	End bool
}

// LogStore stores execution logs. Every run writes its lines, then calls End once; the admin reads a log from a
// given line to its end, again and again while the run is going. Implementations must be safe for concurrent use.
// The executor never writes to a run after its End, and recovers from a panicking store.
type LogStore interface {
	// Write appends a line.
	Write(ref LogRef, line string) error
	// End marks the run as finished.
	End(ref LogRef) error
	// Read returns the log from line fromLine (1-based) to its current end, or ErrLogNotFound.
	Read(ref LogRef, fromLine int) (LogChunk, error)
	// Close releases resources.
	Close() error
}

// ---------------------------------------------------------------------------------------------
// In-memory store
// ---------------------------------------------------------------------------------------------

// DefaultMemoryLogBytes is the default total size of MemoryLogStore.
const DefaultMemoryLogBytes = 16 << 20

// logEntryOverhead is the memory accounted to every run on top of its lines, so that many tiny logs are bounded too.
const logEntryOverhead = 64

// MemoryLogStore keeps recent execution logs in memory, up to a total size. When it is full the oldest logs are
// evicted, those of finished runs first. A single run may use at most an eighth of the total; further lines are
// dropped and counted in a note at the end of its log. Logs are lost on restart.
type MemoryLogStore struct {
	maxBytes, maxRunBytes int

	mu      sync.Mutex
	runs    map[int64]*list.Element // of *memoryLog, in running or ended
	running list.List               // oldest first
	ended   list.List               // oldest first
	size    int
}

type memoryLog struct {
	id      int64
	lines   []string
	size    int // bytes accounted to this run, overhead included
	dropped int
	ended   bool
}

// NewMemoryLogStore returns a MemoryLogStore of maxBytes in total; a value <= 0 means DefaultMemoryLogBytes.
func NewMemoryLogStore(maxBytes int) *MemoryLogStore {
	if maxBytes <= 0 {
		maxBytes = DefaultMemoryLogBytes
	}
	return &MemoryLogStore{maxBytes: maxBytes, maxRunBytes: max(maxBytes/8, logEntryOverhead+1), runs: map[int64]*list.Element{}}
}

// Write implements LogStore.
func (s *MemoryLogStore) Write(ref LogRef, line string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	l := s.entry(ref.LogID)
	if l == nil {
		return nil // the store is too small to hold even an empty log
	}
	n := len(line) + 1
	if l.size+n > s.maxRunBytes || !s.makeRoom(n, ref.LogID) {
		l.dropped++
		return nil
	}
	l.lines = append(l.lines, line)
	l.size += n
	s.size += n
	return nil
}

// End implements LogStore. Writes that arrive afterwards are still accepted, but the executor never sends any.
func (s *MemoryLogStore) End(ref LogRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.runs[ref.LogID]
	if e == nil {
		return nil
	}
	l := e.Value.(*memoryLog)
	if l.ended {
		return nil
	}
	l.ended = true
	s.running.Remove(e)
	s.runs[ref.LogID] = s.ended.PushBack(l)
	if l.dropped > 0 {
		note := fmt.Sprintf("... %d more lines dropped (log size limit %d bytes per run)", l.dropped, s.maxRunBytes)
		l.lines = append(l.lines, note)
		l.size += len(note) + 1
		s.size += len(note) + 1
	}
	return nil
}

// Read implements LogStore.
func (s *MemoryLogStore) Read(ref LogRef, fromLine int) (LogChunk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.runs[ref.LogID]
	if e == nil {
		return LogChunk{}, ErrLogNotFound
	}
	l := e.Value.(*memoryLog)
	c := chunkOf(l.lines, fromLine)
	c.End = l.ended
	return c, nil
}

// Close implements LogStore.
func (s *MemoryLogStore) Close() error { return nil }

// entry returns the log of id, creating it (and making room for it) if needed. Called with s.mu held.
func (s *MemoryLogStore) entry(id int64) *memoryLog {
	if e := s.runs[id]; e != nil {
		return e.Value.(*memoryLog)
	}
	if !s.makeRoom(logEntryOverhead, id) {
		return nil
	}
	l := &memoryLog{id: id, size: logEntryOverhead}
	s.runs[id] = s.running.PushBack(l)
	s.size += logEntryOverhead
	return l
}

// makeRoom evicts the oldest logs other than keep, finished runs first, until n more bytes fit. It reports whether
// they do. Called with s.mu held.
func (s *MemoryLogStore) makeRoom(n int, keep int64) bool {
	for s.size+n > s.maxBytes {
		victim := s.ended.Front()
		if victim == nil {
			if victim = s.running.Front(); victim != nil && victim.Value.(*memoryLog).id == keep {
				victim = victim.Next()
			}
		}
		if victim == nil {
			return false
		}
		l := victim.Value.(*memoryLog)
		if l.ended {
			s.ended.Remove(victim)
		} else {
			s.running.Remove(victim)
		}
		delete(s.runs, l.id)
		s.size -= l.size
	}
	return true
}

// chunkOf returns the lines from fromLine to the end.
func chunkOf(lines []string, fromLine int) LogChunk {
	fromLine = max(fromLine, 1)
	c := LogChunk{FromLine: fromLine, ToLine: fromLine - 1}
	if fromLine <= len(lines) {
		c.Content = strings.Join(lines[fromLine-1:], "\n") + "\n"
		c.ToLine = len(lines)
	}
	return c
}

// ---------------------------------------------------------------------------------------------
// File store
// ---------------------------------------------------------------------------------------------

// DefaultFileLogRunBytes caps the execution log of a single run in FileLogStore.
const DefaultFileLogRunBytes = 16 << 20

// FileLogStore writes logs to {dir}/{yyyy-MM-dd}/{logId}.log, dated by the trigger's log time in the local time
// zone, like the official executor. A run's log is capped at DefaultFileLogRunBytes; further lines are dropped and
// counted in a note at the end. A background goroutine removes date directories older than the retention period
// every hour.
type FileLogStore struct {
	dir       string
	retention int

	mu   sync.Mutex
	open map[int64]*openLog // logs of running jobs

	stop chan struct{}
	done chan struct{}
	once sync.Once
}

type openLog struct {
	mu      sync.Mutex // serializes the writes of one run; runs do not block each other
	f       *os.File
	size    int
	dropped int
}

// NewFileLogStore returns a FileLogStore rooted at dir that keeps the logs of the last retentionDays days, today
// included; a value <= 0 disables cleanup.
func NewFileLogStore(dir string, retentionDays int) (*FileLogStore, error) {
	if dir == "" {
		return nil, errors.New("xxljob: log dir is required")
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("xxljob: create log dir: %w", err)
	}
	s := &FileLogStore{dir: dir, retention: retentionDays, open: map[int64]*openLog{},
		stop: make(chan struct{}), done: make(chan struct{})}
	go s.cleanLoop()
	return s, nil
}

func (s *FileLogStore) path(ref LogRef) string {
	return filepath.Join(s.dir, ref.LogTime.Format(time.DateOnly), strconv.FormatInt(ref.LogID, 10)+".log")
}

// Write implements LogStore.
func (s *FileLogStore) Write(ref LogRef, line string) error {
	l, err := s.get(ref)
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil { // ended concurrently
		return nil
	}
	line = strings.ReplaceAll(line, "\n", " ") + "\n"
	if l.size+len(line) > DefaultFileLogRunBytes {
		l.dropped++
		return nil
	}
	n, err := l.f.WriteString(line)
	l.size += n
	return err
}

func (s *FileLogStore) get(ref LogRef) (*openLog, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if l := s.open[ref.LogID]; l != nil {
		return l, nil
	}
	p := s.path(ref)
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		return nil, fmt.Errorf("xxljob: create log dir: %w", err)
	}
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600) //nolint:gosec // G304: dir/date/logId from trusted values
	if err != nil {
		return nil, fmt.Errorf("xxljob: open log file: %w", err)
	}
	l := &openLog{f: f}
	s.open[ref.LogID] = l
	return l, nil
}

// End implements LogStore.
func (s *FileLogStore) End(ref LogRef) error {
	s.mu.Lock()
	l := s.open[ref.LogID]
	delete(s.open, ref.LogID)
	s.mu.Unlock()
	if l == nil {
		return nil
	}
	return l.close()
}

func (l *openLog) close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return nil
	}
	var err error
	if l.dropped > 0 {
		_, err = fmt.Fprintf(l.f, "... %d more lines dropped (log size limit %d bytes per run)\n", l.dropped, DefaultFileLogRunBytes)
	}
	err = errors.Join(err, l.f.Close())
	l.f = nil
	return err
}

// Read implements LogStore. It returns only complete lines, so a line being written is never cut in half. The file
// store does not record whether a run has finished (End is always false); the admin decides from the run's status.
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
	c := LogChunk{FromLine: fromLine, ToLine: fromLine - 1}
	var sb strings.Builder
	r := bufio.NewReader(f)
	for n := 1; ; n++ {
		line, err := r.ReadString('\n')
		if errors.Is(err, io.EOF) {
			break // an incomplete last line is still being written
		}
		if err != nil {
			return LogChunk{}, fmt.Errorf("xxljob: read log file: %w", err)
		}
		if n >= fromLine {
			sb.WriteString(line)
			c.ToLine = n
		}
	}
	c.Content = sb.String()
	return c, nil
}

// Close implements LogStore: it stops the cleanup and closes open files.
func (s *FileLogStore) Close() error {
	s.once.Do(func() { close(s.stop) })
	<-s.done
	s.mu.Lock()
	open := s.open
	s.open = map[int64]*openLog{}
	s.mu.Unlock()
	var errs []error
	for _, l := range open {
		errs = append(errs, l.close())
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

// clean removes date directories outside the retention window: older than retention days (today counting as the
// first), or more than a day in the future. Other directories are left alone.
func (s *FileLogStore) clean(now time.Time) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return
	}
	y, m, d := now.Date()
	today := time.Date(y, m, d, 0, 0, 0, 0, time.Local)
	oldest, newest := today.AddDate(0, 0, 1-s.retention), today.AddDate(0, 0, 1)
	for _, e := range entries {
		day, err := time.ParseInLocation(time.DateOnly, e.Name(), time.Local)
		if err != nil || !e.IsDir() || (!day.Before(oldest) && !day.After(newest)) {
			continue
		}
		_ = os.RemoveAll(filepath.Join(s.dir, e.Name()))
	}
}
