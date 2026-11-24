package report

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ShawineWu/go-xxljob/internal/wire"
)

const (
	spoolPrefix  = "callback-"
	spoolExt     = ".jsonl"   // saved, waiting to be loaded
	claimedExt   = ".claimed" // loaded, removed once its results are delivered or saved again
	badExt       = ".bad"     // could not be read; kept for inspection, outside the size limit
	tmpExt       = ".tmp"     // being written
	maxSpoolLine = 1 << 20    // longer lines are not results this package wrote
)

// spool saves undelivered results as JSON Lines files in a directory used by one process at a time. Files are
// written to a temporary name and renamed, so a crash never leaves a half-written file that would be loaded. Loading
// a file renames it to *.claimed instead of removing it; claimed files go only once their results have been
// delivered or saved again (release), and those left by a process that crashed are loaded again by the next one.
type spool struct {
	dir      string
	maxBytes int64 // disk the files may use; 0 means no limit
	seq      atomic.Int64

	mu      sync.Mutex
	claimed []string // files claimed by this process and not yet released
	failed  bool     // a save failed since the last release
}

// record is a result as saved in a spool file, kept apart from wire.Result so that the file format, which outlives
// the process, does not change with the protocol.
type record struct {
	LogID   int64  `json:"logId"`
	LogTime int64  `json:"logTime"`
	Code    int    `json:"code"`
	Msg     string `json:"msg"`
}

func newSpool(dir string, maxBytes int64) (*spool, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create callback dir: %w", err)
	}
	return &spool{dir: dir, maxBytes: maxBytes}, nil
}

// save writes results to a new file, unless the directory's files would then use more than maxBytes.
func (s *spool) save(results []wire.Result) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	defer func() {
		if err != nil {
			s.failed = true
		}
	}()
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, r := range results {
		if err := enc.Encode(record{LogID: r.LogID, LogTime: r.LogTime, Code: r.Code, Msg: r.Msg}); err != nil {
			return err
		}
	}
	if s.maxBytes > 0 {
		used, err := s.used()
		if err != nil {
			return err
		}
		if used+int64(buf.Len()) > s.maxBytes {
			return fmt.Errorf("saving %d bytes would take the callback dir past its %d byte limit (%d used)",
				buf.Len(), s.maxBytes, used)
		}
	}
	name := fmt.Sprintf("%s%d-%d-%d", spoolPrefix, time.Now().UnixNano(), os.Getpid(), s.seq.Add(1))
	tmp := filepath.Join(s.dir, name+tmpExt)
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600) //nolint:gosec // G304: name built here
	if err != nil {
		return err
	}
	_, werr := f.Write(buf.Bytes())
	if err := errors.Join(werr, f.Sync(), f.Close()); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, filepath.Join(s.dir, name+spoolExt)); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

// adopt makes the files claimed by a process that stopped without releasing them available to claim again, and
// removes the temporary files of saves it did not finish. Call it before the first claim.
func (s *spool) adopt() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	var errs []error
	for _, e := range entries {
		name := e.Name()
		if !e.Type().IsRegular() || !strings.HasPrefix(name, spoolPrefix) {
			continue
		}
		path := filepath.Join(s.dir, name)
		switch {
		case strings.HasSuffix(name, claimedExt):
			errs = append(errs, os.Rename(path, strings.TrimSuffix(path, claimedExt)+spoolExt))
		case strings.HasSuffix(name, tmpExt):
			errs = append(errs, os.Remove(path))
		}
	}
	return errors.Join(errs...)
}

// claim loads saved files, oldest first, as long as their results fit in room; the first file is loaded even when
// it does not, so that a large file cannot block the others. A file that cannot be read is renamed to *.bad; lines
// that are not results are skipped and counted. Only regular files are read.
func (s *spool) claim(room int) (out []wire.Result, skipped int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(s.dir) // sorted by name, which starts with the time it was saved
	if err != nil {
		return nil, 0, err
	}
	var errs []error
	for _, e := range entries {
		name := e.Name()
		if len(out) >= room {
			break
		}
		if !e.Type().IsRegular() || !strings.HasPrefix(name, spoolPrefix) || !strings.HasSuffix(name, spoolExt) {
			continue
		}
		path := filepath.Join(s.dir, name)
		claimed := strings.TrimSuffix(path, spoolExt) + claimedExt
		if err := os.Rename(path, claimed); err != nil {
			errs = append(errs, err)
			continue
		}
		results, bad, err := readSpoolFile(claimed, s.maxBytes)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err), os.Rename(claimed, path+badExt))
			continue
		}
		if len(out) > 0 && len(out)+len(results) > room {
			errs = append(errs, os.Rename(claimed, path)) // for a later claim
			break
		}
		s.claimed = append(s.claimed, claimed)
		out = append(out, results...)
		skipped += bad
	}
	return out, skipped, errors.Join(errs...)
}

// release removes the claimed files, whose results have all been delivered or saved to new files. After a failed
// save it keeps them instead, since the results that could not be saved may be among theirs: the next process loads
// them again, and the admin ignores the duplicates.
func (s *spool) release() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	claimed, failed := s.claimed, s.failed
	s.claimed, s.failed = nil, false
	if failed {
		return nil
	}
	var errs []error
	for _, path := range claimed {
		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// used is the size of the files that count against maxBytes: all but the bad ones, which hold nothing loadable.
func (s *spool) used() (int64, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return 0, err
	}
	var n int64
	for _, e := range entries {
		if !e.Type().IsRegular() || !strings.HasPrefix(e.Name(), spoolPrefix) || strings.HasSuffix(e.Name(), badExt) {
			continue
		}
		if info, err := e.Info(); err == nil {
			n += info.Size()
		}
	}
	return n, nil
}

// readSpoolFile reads the results in a file, skipping and counting the lines that are not results. It reads only a
// regular file: opening does not block (a FIFO swapped in after the directory was listed), and the file is checked
// after opening (a symlink). A file larger than maxBytes (when set) is an error: this package never writes one.
// Messages are cut to the length the admin accepts.
func readSpoolFile(path string, maxBytes int64) (out []wire.Result, skipped int, err error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0) //nolint:gosec // G304: a file of our own spool directory
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	if !info.Mode().IsRegular() {
		return nil, 0, errors.New("not a regular file")
	}
	if lst, err := os.Lstat(path); err != nil || !lst.Mode().IsRegular() {
		return nil, 0, errors.New("not a regular file (a symlink?)")
	}
	if maxBytes > 0 && info.Size() > maxBytes {
		return nil, 0, fmt.Errorf("file is %d bytes, over the %d byte limit", info.Size(), maxBytes)
	}
	br := bufio.NewReader(f)
	for {
		line, tooLong, err := readLine(br, maxSpoolLine)
		if errors.Is(err, io.EOF) && len(line) == 0 && !tooLong {
			break
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, 0, err
		}
		line = bytes.TrimSpace(line)
		if len(line) == 0 && !tooLong {
			continue
		}
		var r record
		if tooLong || json.Unmarshal(line, &r) != nil || r.LogID <= 0 {
			skipped++
			continue
		}
		out = append(out, wire.Result{LogID: r.LogID, LogTime: r.LogTime, Code: r.Code, Msg: wire.TruncateMsg(r.Msg)})
	}
	return out, skipped, nil
}

// readLine reads a line of at most limit bytes. A longer one is consumed but not returned (tooLong).
func readLine(br *bufio.Reader, limit int) (line []byte, tooLong bool, err error) {
	for {
		frag, err := br.ReadSlice('\n')
		if !tooLong {
			if len(line)+len(frag) > limit {
				tooLong, line = true, nil
			} else {
				line = append(line, frag...)
			}
		}
		if !errors.Is(err, bufio.ErrBufferFull) {
			return line, tooLong, err
		}
	}
}
