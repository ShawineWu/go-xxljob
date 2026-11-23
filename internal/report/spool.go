package report

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ShawineWu/go-xxljob/internal/wire"
)

const spoolExt = ".jsonl"

// spool saves undelivered results as JSON Lines files in a directory. Files are written to a temporary name and
// renamed, so a crash never leaves a half-written file that would be loaded.
type spool struct {
	dir string
	seq atomic.Int64
	mu  sync.Mutex // serializes load against save
}

func newSpool(dir string) (*spool, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create callback dir: %w", err)
	}
	return &spool{dir: dir}, nil
}

// save writes results to a new file.
func (s *spool) save(results []wire.Result) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	name := fmt.Sprintf("callback-%d-%d-%d", time.Now().UnixNano(), os.Getpid(), s.seq.Add(1))
	tmp := filepath.Join(s.dir, name+".tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600) //nolint:gosec // G304: name built here
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = os.Remove(tmp)
		}
	}()
	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)
	for _, r := range results {
		if err := enc.Encode(r); err != nil {
			_ = f.Close()
			return err
		}
	}
	if err := errors.Join(w.Flush(), f.Sync(), f.Close()); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(s.dir, name+spoolExt))
}

// load reads and removes every saved file. A file that cannot be parsed is renamed to *.bad and skipped.
func (s *spool) load() ([]wire.Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	var out []wire.Result
	var errs []error
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), "callback-") || !strings.HasSuffix(e.Name(), spoolExt) {
			continue
		}
		path := filepath.Join(s.dir, e.Name())
		results, err := readSpoolFile(path)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", e.Name(), err), os.Rename(path, path+".bad"))
			continue
		}
		if err := os.Remove(path); err != nil {
			errs = append(errs, err)
			continue
		}
		out = append(out, results...)
	}
	return out, errors.Join(errs...)
}

func readSpoolFile(path string) ([]wire.Result, error) {
	f, err := os.Open(path) //nolint:gosec // G304: a file of our own spool directory
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	var out []wire.Result
	dec := json.NewDecoder(f)
	for dec.More() {
		var r wire.Result
		if err := dec.Decode(&r); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}
