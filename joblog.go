package xxljob

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
)

// runState is the per-run state carried in the context.
type runState struct {
	req    *Request
	logger *slog.Logger

	mu     sync.Mutex
	result string
}

type runKey struct{}

func withRun(ctx context.Context, r *runState) context.Context {
	return context.WithValue(ctx, runKey{}, r)
}

func runFrom(ctx context.Context) *runState {
	r, _ := ctx.Value(runKey{}).(*runState)
	return r
}

// Logger returns the logger of the current run. Records go to the run's execution log (visible in the admin) and
// to the executor's logger, tagged with handler, job_id and log_id. Outside a run it returns slog.Default(). Use
// the *Context methods (InfoContext, ...) so that trace-aware handlers see the run's context.
func Logger(ctx context.Context) *slog.Logger {
	if r := runFrom(ctx); r != nil {
		return r.logger
	}
	return slog.Default()
}

// SetResult sets the message reported to the admin when the run succeeds (shown as the run's note), e.g. the
// number of processed items. It is ignored if the run fails.
func SetResult(ctx context.Context, msg string) {
	if r := runFrom(ctx); r != nil {
		r.mu.Lock()
		r.result = msg
		r.mu.Unlock()
	}
}

// RequestFrom returns the request of the current run, so middlewares and deeper code can read job_id, log_id and
// so on.
func RequestFrom(ctx context.Context) (*Request, bool) {
	if r := runFrom(ctx); r != nil {
		return r.req, true
	}
	return nil, false
}

func (r *runState) resultMsg() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.result
}

// maxLogLineBytes caps a single execution log line; longer lines are truncated so one record cannot fill the store.
const maxLogLineBytes = 8 << 10

// execLog writes one run's execution log to the store. Once closed (the run has ended) it drops writes, so a
// goroutine or timer that outlives the run cannot reopen the log. A store that fails or panics is reported once to
// the application log and never brings the run down.
type execLog struct {
	store  LogStore
	ref    LogRef
	app    *slog.Logger
	failed atomic.Bool

	mu     sync.Mutex // orders writes against end
	closed bool
}

// Write stores one record of a slog.TextHandler (newline-terminated) as one log line.
func (w *execLog) Write(p []byte) (int, error) {
	line := bytes.TrimRight(p, "\n")
	if len(line) > maxLogLineBytes {
		line = append(bytes.ToValidUTF8(line[:maxLogLineBytes], nil), "... (truncated)"...)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.closed {
		w.guard("write", func() error { return w.store.Write(w.ref, string(line)) })
	}
	return len(p), nil
}

// end closes the log and tells the store the run has ended.
func (w *execLog) end() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}
	w.closed = true
	w.guard("end", func() error { return w.store.End(w.ref) })
}

func (w *execLog) guard(op string, fn func() error) {
	var err error
	func() {
		defer func() {
			if p := recover(); p != nil {
				err = panicError{fmt.Sprint(p)}
			}
		}()
		err = fn()
	}()
	if err != nil && !w.failed.Swap(true) {
		w.app.Error("xxljob: execution log "+op+" failed; further errors of this run are not logged", "err", err)
	}
}

type panicError struct{ msg string }

func (e panicError) Error() string { return "panic: " + e.msg }
