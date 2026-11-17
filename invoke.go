package xxljob

import (
	"context"
	"log/slog"
	"strings"
	"time"
)

// Result is the outcome of Invoke.
type Result struct {
	// Err is the error returned by the handler (including a recovered panic or a timeout); nil on success.
	Err error
	// Code is the code the executor would report to the admin: 200 success, 500 failure, 502 timeout.
	Code int
	// Msg is the run's note: the SetResult message on success, the error message on failure.
	Msg string
	// Log is the execution log (what the admin shows), one entry per line.
	Log []string
}

// Invoke runs a handler once in the current goroutine, without an admin. It applies the handler's own middleware
// but not the executor's global middlewares, and behaves like a real run: Logger, SetResult and RequestFrom work,
// req.Timeout is enforced, and a panic becomes a failure. Use it in unit tests, for local runs and in maintenance
// scripts. A nil req means empty parameters; ShardTotal 0 is treated as 1.
//
//	res := xxljob.Invoke(ctx, NewSyncTask(db), &xxljob.Request{Params: `{"batchSize":10}`})
//	if res.Err != nil { ... }
func Invoke(ctx context.Context, h Handler, req *Request) Result {
	r := Request{}
	if req != nil {
		r = *req
	}
	if r.ShardTotal <= 0 {
		r.ShardTotal = 1
	}
	if r.LogTime.IsZero() {
		r.LogTime = time.Now()
	}
	if r.Handler == "" {
		if t, ok := h.(Task); ok {
			r.Handler = t.Name()
		}
	}
	store := NewMemoryLogStore(0)
	ref := LogRef{LogID: r.LogID, LogTime: r.LogTime}
	code, msg, err := runOnce(ctx, h, &r, runOpts{
		store: store,
		ref:   ref,
		base:  slog.Default().With("handler", r.Handler, "job_id", r.JobID, "log_id", r.LogID),
	})
	_ = store.End(ref)
	res := Result{Err: err, Code: code, Msg: msg}
	if chunk, rerr := store.Read(ref, 1); rerr == nil && chunk.Content != "" {
		res.Log = strings.Split(strings.TrimSuffix(chunk.Content, "\n"), "\n")
	}
	return res
}
