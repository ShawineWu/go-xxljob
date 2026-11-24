package xxljob

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/ShawineWu/go-xxljob/internal/sched"
	"github.com/ShawineWu/go-xxljob/internal/wire"
)

// InvokeResult is the outcome of Invoke.
type InvokeResult struct {
	// Err is why the run failed: the cancellation cause (such as ErrTimeout) when the run was cancelled, whatever the
	// handler returned; otherwise the handler's error, including a recovered panic. nil on success.
	Err error
	// Code is the code the executor would report to the admin: CodeSuccess, CodeFailure or CodeTimeout.
	Code int
	// Msg is the run's note as the admin would show it: the SetResult message on success, the error otherwise.
	Msg string
	// Log is the execution log (what the admin shows), one entry per line.
	Log []string
}

// Invoke runs a handler once with ctx, without an admin, exactly as the executor would, and waits for it to return:
// Logger, SetResult and RequestFrom work, req.Timeout is enforced, and a panic or runtime.Goexit becomes a failure. Middlewares
// registered with an Executor are not applied: wrap h yourself if needed. Use it in unit tests, for manual runs and
// in maintenance commands. A nil req means empty parameters; ShardTotal 0 means 1.
//
//	res := xxljob.Invoke(ctx, NewSyncTask(db), &xxljob.Request{Params: `{"batchSize":10}`})
//	if res.Err != nil { ... }
func Invoke(ctx context.Context, h Handler, req *Request) InvokeResult {
	var r Request
	if req != nil {
		r = *req
	}
	r.ShardTotal = max(r.ShardTotal, 1)
	if r.LogTime.IsZero() {
		r.LogTime = time.Now()
	}
	if t, ok := h.(Task); ok && r.Handler == "" {
		r.Handler = t.Name()
	}
	if r.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeoutCause(ctx, r.Timeout, ErrTimeout)
		defer cancel()
	}
	store := NewMemoryLogStore(0)
	ref := LogRef{LogID: r.LogID, LogTime: r.LogTime}
	run, finish, err := runner{store: store, app: slog.Default()}.run(ctx, h, r, ref)
	cause := context.Cause(ctx)
	o := sched.Classify(cause, run)
	finish(o)
	if sched.IsCause(cause) {
		err = cause // the cancellation decided the outcome, whatever the handler returned
	}
	res := InvokeResult{Err: err, Code: codeOf(o.Status), Msg: wire.TruncateMsg(o.Msg)}
	if chunk, rerr := store.Read(ref, 1); rerr == nil && chunk.Content != "" {
		res.Log = strings.Split(strings.TrimSuffix(chunk.Content, "\n"), "\n")
	}
	return res
}
