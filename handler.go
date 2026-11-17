package xxljob

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Reasons a run's context is cancelled. Check them with context.Cause(ctx).
var (
	// ErrKilled means the admin killed the job.
	ErrKilled = errors.New("xxljob: job killed by the scheduling center")
	// ErrCovered means a newer trigger replaced this run (block strategy COVER_EARLY).
	ErrCovered = errors.New("xxljob: job covered by a newer trigger")
	// ErrHandlerChanged means the job was switched to another JobHandler in the admin.
	ErrHandlerChanged = errors.New("xxljob: job handler changed")
	// ErrTimeout means the run exceeded its timeout.
	ErrTimeout = errors.New("xxljob: job execution timeout")
	// ErrShutdown means the executor is shutting down and stopped waiting for the run.
	ErrShutdown = errors.New("xxljob: executor is shutting down")
)

// Request is one trigger of a job.
type Request struct {
	JobID int64
	LogID int64
	// LogTime is the log time of this trigger, assigned by the admin.
	LogTime time.Time
	// Handler is the JobHandler configured in the admin.
	Handler string
	// Params are the job parameters configured in the admin (executorParams). Use BindJSON when they are JSON.
	Params string
	// BlockStrategy is the block strategy (one of the Block* constants).
	BlockStrategy string
	// Timeout is the run timeout, 0 for none. When it expires the context is cancelled with ErrTimeout.
	Timeout time.Duration
	// ShardIndex and ShardTotal describe sharding broadcast: the zero-based shard of this run and the number of
	// shards. They are 0 and 1 for jobs that are not sharded.
	ShardIndex int
	ShardTotal int
}

// BindJSON decodes Params into v. When Params is blank it does nothing, so v keeps the defaults set by the caller.
func (r *Request) BindJSON(v any) error {
	if strings.TrimSpace(r.Params) == "" {
		return nil
	}
	if err := json.Unmarshal([]byte(r.Params), v); err != nil {
		return fmt.Errorf("xxljob: invalid job params %q: %w", r.Params, err)
	}
	return nil
}

// Handler runs one trigger of a job.
//
// Returning an error fails the run: the admin shows it as failed and applies its alarm and retry settings. A panic
// is reported as a failure too. Returning nil succeeds; use SetResult to attach a message. The context is cancelled
// on timeout, kill, COVER_EARLY and executor shutdown (see context.Cause). Long-running handlers must watch it:
// until a handler returns, later triggers of the same job are only queued or discarded.
type Handler interface {
	Handle(ctx context.Context, req *Request) error
}

// HandlerFunc adapts a function to the Handler interface.
type HandlerFunc func(ctx context.Context, req *Request) error

// Handle implements Handler.
func (f HandlerFunc) Handle(ctx context.Context, req *Request) error { return f(ctx, req) }

// Task is a Handler that carries its own name: one type per job, named after the JobHandler configured in the
// admin. Register tasks in bulk with Executor.RegisterTasks.
//
//	type SyncItemsTask struct{ db *sql.DB }
//
//	func (t *SyncItemsTask) Name() string { return "SyncItems" }
//	func (t *SyncItemsTask) Handle(ctx context.Context, req *xxljob.Request) error { ... }
//
//	exec.MustRegisterTasks(&SyncItemsTask{db}, &CleanupTask{db})
type Task interface {
	Handler
	// Name returns the JobHandler name, unique within the executor.
	Name() string
}

// HandleJSON decodes the job parameters (JSON) into P before calling fn, saving a BindJSON call in every handler.
// Blank parameters leave p as the zero value; invalid JSON fails the run without calling fn.
//
//	exec.MustRegister("Cleanup", xxljob.HandleJSON(func(ctx context.Context, req *xxljob.Request, p struct{ Days int }) error { ... }))
func HandleJSON[P any](fn func(ctx context.Context, req *Request, params P) error) Handler {
	return HandlerFunc(func(ctx context.Context, req *Request) error {
		var p P
		if err := req.BindJSON(&p); err != nil {
			return err
		}
		return fn(ctx, req, p)
	})
}

// Middleware wraps a Handler, e.g. for logging, tracing, metrics or rate limiting.
type Middleware func(next Handler) Handler

// chain wraps h with mws, outermost first: chain(h, a, b) calls a, then b, then h.
func chain(h Handler, mws ...Middleware) Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}
