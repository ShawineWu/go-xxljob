package xxljob

import (
	"bytes"
	"context"
	"log/slog"
	"sync"
)

// runState is the per-run state carried in the context: the request, the execution-log logger and the result
// message.
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
// to the executor's logger, tagged with handler, job_id and log_id. Outside a run it returns slog.Default().
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

// lineWriter stores each record written by a slog.TextHandler (newline-terminated) as one log line.
// TextHandler serializes its writes.
type lineWriter struct {
	store LogStore
	ref   LogRef
}

// maxLogLineBytes caps a single log line; longer lines are truncated so one huge record cannot fill the store.
const maxLogLineBytes = 8 << 10

func (w *lineWriter) Write(p []byte) (int, error) {
	line := bytes.TrimRight(p, "\n")
	if len(line) > maxLogLineBytes {
		line = append(bytes.ToValidUTF8(line[:maxLogLineBytes], nil), []byte("... (truncated)")...)
	}
	if err := w.store.Write(w.ref, string(line)); err != nil {
		return 0, err
	}
	return len(p), nil
}

// teeHandler writes every record to two handlers.
type teeHandler struct {
	a, b slog.Handler
}

func (t teeHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return t.a.Enabled(ctx, l) || t.b.Enabled(ctx, l)
}

func (t teeHandler) Handle(ctx context.Context, r slog.Record) error {
	var errA, errB error
	if t.a.Enabled(ctx, r.Level) {
		errA = t.a.Handle(ctx, r.Clone())
	}
	if t.b.Enabled(ctx, r.Level) {
		errB = t.b.Handle(ctx, r)
	}
	if errA != nil {
		return errA
	}
	return errB
}

func (t teeHandler) WithAttrs(as []slog.Attr) slog.Handler {
	return teeHandler{a: t.a.WithAttrs(as), b: t.b.WithAttrs(as)}
}

func (t teeHandler) WithGroup(name string) slog.Handler {
	return teeHandler{a: t.a.WithGroup(name), b: t.b.WithGroup(name)}
}
