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

// maxHandleMsg caps the run note reported to the admin, in runes.
const maxHandleMsg = 2000

// trigger is one trigger of a job. Every trigger has its own state; nothing is shared between triggers.
type trigger struct {
	req         *Request
	logDateTime int64 // the raw millisecond timestamp sent by the admin, reported back unchanged
	handler     Handler
	ctx         context.Context
	cancel      context.CancelCauseFunc
}

func (tr *trigger) ref() LogRef { return LogRef{LogID: tr.req.LogID, LogTime: tr.req.LogTime} }

// jobThread runs the triggers of one job (jobId) in FIFO order, at most one at a time. It exists while the job
// has a running or queued trigger, and removes itself from Executor.threads when idle. Its fields are guarded by
// Executor.mu.
type jobThread struct {
	jobID   int64
	handler string
	queue   []*trigger
	running *trigger
}

func (t *jobThread) busy() bool { return t.running != nil || len(t.queue) > 0 }

func (t *jobThread) hasLog(logID int64) bool {
	if t.running != nil && t.running.req.LogID == logID {
		return true
	}
	for _, tr := range t.queue {
		if tr.req.LogID == logID {
			return true
		}
	}
	return false
}

// drain removes the queued triggers; the caller reports them as failed.
func (t *jobThread) drain() []*trigger {
	q := t.queue
	t.queue = nil
	return q
}

// abort cancels the running trigger with cause (it reports its own failure when it returns) and removes the
// queued ones.
func (t *jobThread) abort(cause error) []*trigger {
	if t.running != nil {
		t.running.cancel(cause)
	}
	return t.drain()
}

func newRequest(in runReq) *Request {
	total := in.BroadcastTotal
	if total <= 0 {
		total = 1
	}
	return &Request{
		JobID:         in.JobID,
		LogID:         in.LogID,
		LogTime:       time.UnixMilli(in.LogDateTime),
		Handler:       in.ExecutorHandler,
		Params:        in.ExecutorParams,
		BlockStrategy: in.ExecutorBlockStrategy,
		Timeout:       time.Duration(in.ExecutorTimeout) * time.Second,
		ShardIndex:    in.BroadcastIndex,
		ShardTotal:    total,
	}
}

// dispatch handles a run request: it validates it, applies the block strategy and queues it. The result is
// reported asynchronously through a callback.
//
// When the job already has a thread (a running or queued trigger):
//   - JobHandler changed: cancel the running trigger with ErrHandlerChanged, discard the queued ones, queue this one;
//   - DISCARD_LATER: reject this trigger;
//   - COVER_EARLY: cancel the running trigger with ErrCovered, discard the queued ones, queue this one (it starts
//     only after the cancelled run returns);
//   - SERIAL_EXECUTION (default): queue this trigger, or reject it when the queue is full.
func (e *Executor) dispatch(in runReq) reply {
	res := e.accept(in)
	if res.code == codeSuccess {
		e.counters.accepted.Add(1)
	} else {
		e.counters.rejected.Add(1)
	}
	return res
}

// accept validates a run request, applies the block strategy and queues it (see dispatch).
func (e *Executor) accept(in runReq) reply {
	if in.GlueType != "" && in.GlueType != glueTypeBean {
		return failure(fmt.Sprintf("glue type [%s] is not supported, only BEAN", in.GlueType))
	}

	var dropped []*trigger
	var cause error
	res := func() reply {
		e.mu.Lock()
		defer e.mu.Unlock()
		switch e.state {
		case stateNew:
			return failure("executor is not started")
		case stateClosing:
			return failure(ErrShutdown.Error())
		}
		reg, ok := e.handlers[in.ExecutorHandler]
		if !ok {
			return failure(fmt.Sprintf("job handler [%s] not found.", in.ExecutorHandler))
		}
		req := newRequest(in)
		if req.Timeout <= 0 {
			req.Timeout = e.cfg.DefaultTimeout
		}
		t := e.threads[in.JobID]
		if t != nil {
			if t.hasLog(in.LogID) {
				return failure(fmt.Sprintf("repeated trigger: log_id %d is already running or queued", in.LogID))
			}
			switch {
			case t.handler != req.Handler:
				cause, dropped = ErrHandlerChanged, t.abort(ErrHandlerChanged)
				t.handler = req.Handler
			case req.BlockStrategy == BlockDiscardLater && t.busy():
				return failure("block strategy DISCARD_LATER: job is still running")
			case req.BlockStrategy == BlockCoverEarly && t.busy():
				cause, dropped = ErrCovered, t.abort(ErrCovered)
			case len(t.queue) >= e.cfg.MaxQueueSize:
				return failure(fmt.Sprintf("job queue is full (%d triggers waiting)", len(t.queue)))
			}
		}
		mws := append(append([]Middleware(nil), e.middlewares...), reg.mws...)
		ctx, cancel := context.WithCancelCause(e.root)
		tr := &trigger{req: req, logDateTime: in.LogDateTime, handler: chain(reg.h, mws...), ctx: ctx, cancel: cancel}
		if t != nil {
			t.queue = append(t.queue, tr)
			return success("")
		}
		// An idle job gets a new thread that starts with this trigger as its running one, so the trigger is never
		// seen as queued: Shutdown waits for it like any other run instead of discarding it.
		t = &jobThread{jobID: in.JobID, handler: req.Handler, running: tr}
		e.threads[in.JobID] = t
		e.wg.Add(1)
		go e.runThread(t)
		return success("")
	}()
	for _, tr := range dropped {
		e.discard(tr, cause)
	}
	return res
}

// kill handles a kill request: it cancels the running trigger and discards the queued ones. Until the cancelled
// run returns, the job still counts as running, so later triggers never run concurrently with it.
func (e *Executor) kill(jobID int64) reply {
	e.mu.Lock()
	t := e.threads[jobID]
	if t == nil {
		e.mu.Unlock()
		return success("job thread already killed.")
	}
	dropped := t.abort(ErrKilled)
	running := t.running
	e.mu.Unlock()

	for _, tr := range dropped {
		e.discard(tr, ErrKilled)
	}
	if running != nil {
		e.log.Info("xxljob: kill requested", "job_id", jobID, "log_id", running.req.LogID)
	}
	return success("")
}

// runThread is the job thread: it runs the trigger it was created with, then the queued ones one by one, and exits
// when the queue is empty. The next trigger becomes the running one under the same lock that clears the previous
// one, so the job never looks idle between two runs.
func (e *Executor) runThread(t *jobThread) {
	defer e.wg.Done()
	e.mu.Lock()
	next := t.running
	e.mu.Unlock()
	for next != nil {
		res := e.execute(next)
		next.cancel(nil)

		e.mu.Lock()
		next = nil
		if len(t.queue) > 0 {
			next = t.queue[0]
			t.queue[0] = nil
			t.queue = t.queue[1:]
		} else if e.threads[t.jobID] == t {
			delete(e.threads, t.jobID)
		}
		t.running = next
		e.mu.Unlock()

		e.report(res)
	}
}

// discard drops a queued trigger that has not started and reports it as failed.
func (e *Executor) discard(tr *trigger, cause error) {
	tr.cancel(cause)
	_ = e.logs.Write(tr.ref(), "trigger discarded: "+cause.Error())
	_ = e.logs.End(tr.ref())
	e.report(result{LogID: tr.req.LogID, LogTime: tr.logDateTime, Code: codeFail, Msg: "trigger discarded: " + cause.Error()})
}

// execute runs one trigger (waiting for a concurrency slot first) and returns the result to report.
func (e *Executor) execute(tr *trigger) result {
	if e.sem != nil {
		select {
		case e.sem <- struct{}{}:
			defer func() { <-e.sem }()
		case <-tr.ctx.Done(): // killed, covered or shut down while waiting: runOnce treats it as cancelled before start
		}
	}
	req := tr.req
	ref := tr.ref()
	code, msg, _ := runOnce(tr.ctx, tr.handler, req, runOpts{
		store:       e.logs,
		ref:         ref,
		base:        e.log.With("handler", req.Handler, "job_id", req.JobID, "log_id", req.LogID),
		longRunning: e.cfg.LongRunningThreshold,
	})
	_ = e.logs.End(ref)
	return result{LogID: req.LogID, LogTime: tr.logDateTime, Code: code, Msg: truncate(msg)}
}

type runOpts struct {
	store LogStore
	ref   LogRef
	base  *slog.Logger
	// longRunning logs a warning (application log and execution log) once a run exceeds it; <= 0 disables it.
	longRunning time.Duration
}

// runOnce runs a handler once, including its middleware chain: it writes the start and end lines of the execution
// log, applies the timeout, recovers panics and warns about long runs. It returns the code to report
// (200 / 500 / 502), the run note and the handler's error. Shared by the executor and Invoke.
func runOnce(parent context.Context, h Handler, req *Request, o runOpts) (code int, msg string, err error) {
	text := slog.New(slog.NewTextHandler(&lineWriter{store: o.store, ref: o.ref}, &slog.HandlerOptions{Level: slog.LevelDebug}))
	rs := &runState{req: req, logger: slog.New(teeHandler{a: text.Handler(), b: o.base.Handler()})}

	ctx := parent
	if req.Timeout > 0 {
		var stop context.CancelFunc
		ctx, stop = context.WithTimeoutCause(ctx, req.Timeout, ErrTimeout)
		defer stop()
	}
	ctx = withRun(ctx, rs)

	text.Info("job start", "handler", req.Handler, "params", req.Params, "shard", fmt.Sprintf("%d/%d", req.ShardIndex, req.ShardTotal),
		"block_strategy", req.BlockStrategy, "timeout", req.Timeout.String())
	start := time.Now()
	if o.longRunning > 0 {
		timer := time.AfterFunc(o.longRunning, func() {
			o.base.Warn("xxljob: job is still running", "elapsed", time.Since(start).String(), "threshold", o.longRunning.String())
			text.Warn("job is still running", "elapsed", time.Since(start).String())
		})
		defer timer.Stop()
	}
	if cause := context.Cause(parent); cause != nil {
		err = cause // cancelled before it started (killed while waiting for a slot, shutdown, ...)
	} else {
		err = invoke(ctx, h, req, o.base, text)
	}
	elapsed := time.Since(start)

	code, msg = codeSuccess, rs.resultMsg()
	if err != nil {
		cause := context.Cause(ctx)
		code = codeFail
		if errors.Is(cause, ErrTimeout) {
			code = codeTimeout
		}
		msg = failureMsg(cause, err)
		text.Error("job failed", "code", code, "elapsed", elapsed.String(), "err", msg)
	} else {
		text.Info("job succeeded", "elapsed", elapsed.String(), "result", msg)
	}
	return code, msg, err
}

// invoke calls the handler (with its middleware chain) and turns a panic into an error, so a bug in one job
// cannot take the executor down.
func invoke(ctx context.Context, h Handler, req *Request, base, text *slog.Logger) (err error) {
	defer func() {
		if p := recover(); p != nil {
			stack := string(debug.Stack())
			base.ErrorContext(ctx, "xxljob: job panicked", "panic", fmt.Sprint(p), "stack", stack)
			text.Error("job panicked", "panic", fmt.Sprint(p), "stack", stack)
			err = fmt.Errorf("panic: %v", p)
		}
	}()
	return h.Handle(ctx, req)
}

// failureMsg builds the note of a failed run. It is prefixed with the cancellation cause (killed, covered,
// timeout, shutdown) so the admin's log shows why the run stopped.
func failureMsg(cause, err error) string {
	if cause != nil && !errors.Is(err, cause) {
		return cause.Error() + ": " + err.Error()
	}
	return err.Error()
}

func truncate(msg string) string {
	if utf8.RuneCountInString(msg) <= maxHandleMsg {
		return msg
	}
	return string([]rune(msg)[:maxHandleMsg]) + "..."
}
