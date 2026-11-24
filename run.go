package xxljob

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"
	"unicode/utf8"

	"github.com/ShawineWu/go-xxljob/internal/sched"
)

// errGoexit is the result of a handler that called runtime.Goexit (e.g. t.FailNow in a test) instead of returning.
var errGoexit = errors.New("xxljob: handler exited its goroutine without returning (runtime.Goexit)")

// maxLoggedParams caps the job parameters written to the "job start" line of the execution log.
const maxLoggedParams = 1 << 10

// runner executes a handler once: the execution log, the logger in its context, the long-running warning, and the
// containment of panics and runtime.Goexit. Deciding the reported outcome is not its job (see run). It is shared by
// the executor and Invoke.
type runner struct {
	store       LogStore
	app         *slog.Logger // the executor's logger
	longRunning time.Duration
}

// run executes h for req. It returns what the handler produced, as plain data for the one decision of the outcome
// (sched.Classify, made by the scheduler or by Invoke), the handler's error itself (for Invoke's callers), and
// finish, which must then be called once with that outcome: it writes the last line of the execution log and ends
// the log, so that the log always states the outcome that was reported, even when a cancellation arrived after the
// handler returned.
func (rn runner) run(ctx context.Context, h Handler, req Request, ref LogRef) (sched.Result, func(sched.Outcome), error) {
	app := rn.app.With("handler", req.Handler, "job_id", req.JobID, "log_id", req.LogID)
	log := &execLog{store: rn.store, ref: ref, app: app}
	text := slog.New(slog.NewTextHandler(log, &slog.HandlerOptions{Level: slog.LevelDebug}))
	rs := &runState{req: &req, logger: slog.New(slog.NewMultiHandler(text.Handler(), app.Handler()))}
	ctx = withRun(ctx, rs)

	text.Info("job start", "handler", req.Handler, "params", clip(req.Params, maxLoggedParams),
		"shard", fmt.Sprintf("%d/%d", req.ShardIndex, req.ShardTotal), "block_strategy", req.BlockStrategy,
		"timeout", req.Timeout.String())
	start := time.Now()
	var timer *time.Timer
	if rn.longRunning > 0 {
		timer = time.AfterFunc(rn.longRunning, func() {
			app.Warn("xxljob: job is still running", "elapsed", time.Since(start).String(), "threshold", rn.longRunning.String())
			text.Warn("job is still running", "elapsed", time.Since(start).String())
		})
	}

	var res sched.Result
	var err error
	if cause := context.Cause(ctx); cause != nil {
		err = cause // cancelled before it started
		res = sched.Result{Failed: true, Msg: cause.Error()}
	} else {
		res, err = call(ctx, h, rs, app, text)
	}
	if timer != nil {
		timer.Stop()
	}

	finish := func(o sched.Outcome) {
		defer log.end()
		if o.Status == sched.Succeeded {
			text.Info("job succeeded", "elapsed", time.Since(start).String(), "result", o.Msg)
		} else {
			text.Error("job failed", "code", codeOf(o.Status), "elapsed", time.Since(start).String(), "err", o.Msg)
		}
	}
	return res, finish, err
}

// call runs the handler on its own goroutine and turns what it produced into plain data there, under the same
// recover: a panic, a runtime.Goexit, or an error whose Error method panics becomes a failure instead of taking down
// or wedging the job thread.
func call(ctx context.Context, h Handler, rs *runState, app, text *slog.Logger) (res sched.Result, err error) {
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
				res = sched.Result{Failed: true, Msg: err.Error()}
			} else if !returned {
				err = errGoexit
				res = sched.Result{Failed: true, Msg: err.Error()}
			}
		}()
		err = h.Handle(ctx, rs.req)
		if err != nil {
			res = sched.Result{Failed: true, Msg: err.Error()}
		} else {
			res = sched.Result{Msg: rs.resultMsg()}
		}
		returned = true
	}()
	<-done
	return res, err
}

// codeOf is the result code reported to the admin for a status.
func codeOf(s sched.Status) int {
	switch s {
	case sched.Succeeded:
		return CodeSuccess
	case sched.TimedOut:
		return CodeTimeout
	}
	return CodeFailure
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
