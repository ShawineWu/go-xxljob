package xxljob

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"
	"unicode/utf8"
)

// errGoexit is the result of a handler that called runtime.Goexit (e.g. t.FailNow in a test) instead of returning.
var errGoexit = errors.New("xxljob: handler exited its goroutine without returning (runtime.Goexit)")

// maxLoggedParams caps the job parameters written to the "job start" line of the execution log.
const maxLoggedParams = 1 << 10

// runner executes a handler once: the execution log, the logger in its context, the long-running warning, panic
// and Goexit isolation, and the classification of the result. It is shared by the executor and Invoke.
type runner struct {
	store       LogStore
	app         *slog.Logger // the executor's logger
	longRunning time.Duration
}

// run executes h for req and returns the code and message to report. The context may carry a cancellation cause
// (ErrKilled, ErrTimeout, ...) that overrides what the handler returns.
func (rn runner) run(ctx context.Context, h Handler, req Request, ref LogRef) (code int, msg string, err error) {
	app := rn.app.With("handler", req.Handler, "job_id", req.JobID, "log_id", req.LogID)
	log := &execLog{store: rn.store, ref: ref, app: app}
	defer log.end()
	text := slog.New(slog.NewTextHandler(log, &slog.HandlerOptions{Level: slog.LevelDebug}))
	rs := &runState{req: &req, logger: slog.New(slog.NewMultiHandler(text.Handler(), app.Handler()))}
	ctx = withRun(ctx, rs)

	text.Info("job start", "handler", req.Handler, "params", clip(req.Params, maxLoggedParams),
		"shard", fmt.Sprintf("%d/%d", req.ShardIndex, req.ShardTotal), "block_strategy", req.BlockStrategy,
		"timeout", req.Timeout.String())
	start := time.Now()
	if rn.longRunning > 0 {
		timer := time.AfterFunc(rn.longRunning, func() {
			app.Warn("xxljob: job is still running", "elapsed", time.Since(start).String(), "threshold", rn.longRunning.String())
			text.Warn("job is still running", "elapsed", time.Since(start).String())
		})
		defer timer.Stop()
	}

	if cause := context.Cause(ctx); cause != nil {
		err = cause // cancelled before it started
	} else {
		err = call(ctx, h, &req, app, text)
	}
	elapsed := time.Since(start)

	code, msg = classify(ctx, err, rs)
	if code == CodeSuccess {
		text.Info("job succeeded", "elapsed", elapsed.String(), "result", msg)
	} else {
		text.Error("job failed", "code", code, "elapsed", elapsed.String(), "err", msg)
	}
	return code, msg, err
}

// call runs the handler on its own goroutine, so that a panic or runtime.Goexit inside it becomes an error instead
// of taking down or wedging the job thread.
func call(ctx context.Context, h Handler, req *Request, app, text *slog.Logger) error {
	var err error
	done := make(chan struct{})
	go func() {
		defer close(done)
		returned := false
		defer func() {
			if p := recover(); p != nil {
				desc, stack := fmt.Sprint(p), string(debug.Stack())
				app.ErrorContext(ctx, "xxljob: job panicked", "panic", desc, "stack", stack)
				text.Error("job panicked", "panic", desc, "stack", stack)
				err = panicError{desc}
			} else if !returned {
				err = errGoexit
			}
		}()
		err = h.Handle(ctx, req)
		returned = true
	}()
	<-done
	return err
}

// classify turns a run's outcome into the code and message reported to the admin. A cancellation cause wins over
// whatever the handler returned: a killed or timed-out run that ignored its context does not report success.
func classify(ctx context.Context, err error, rs *runState) (int, string) {
	cause := context.Cause(ctx)
	switch {
	case isCause(cause):
		code := CodeFailure
		if errors.Is(cause, ErrTimeout) {
			code = CodeTimeout
		}
		if err != nil && !errors.Is(err, cause) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			return code, cause.Error() + ": " + err.Error()
		}
		return code, cause.Error()
	case err != nil:
		return CodeFailure, err.Error()
	}
	return CodeSuccess, rs.resultMsg()
}

func isCause(err error) bool {
	for _, c := range []error{ErrKilled, ErrCovered, ErrHandlerChanged, ErrTimeout, ErrShutdown} {
		if errors.Is(err, c) {
			return true
		}
	}
	return false
}

// clip shortens s to at most n bytes, on a rune boundary.
func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n] + "..."
}
