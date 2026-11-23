package xxljob

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ShawineWu/go-xxljob/internal/sched"
	"github.com/ShawineWu/go-xxljob/internal/wire"
)

// Causes a run's context is cancelled with; check them with context.Cause(ctx). The run is reported to the admin as
// failed (timed out for ErrTimeout) the moment its context is cancelled, whatever the handler returns afterwards.
var (
	// ErrKilled means the admin killed the job.
	ErrKilled = sched.ErrKilled
	// ErrCovered means a newer trigger replaced this run (block strategy COVER_EARLY).
	ErrCovered = sched.ErrCovered
	// ErrHandlerChanged means the job was switched to another JobHandler in the admin.
	ErrHandlerChanged = sched.ErrHandlerChanged
	// ErrTimeout means the run exceeded its timeout.
	ErrTimeout = sched.ErrTimeout
	// ErrShutdown means the executor is shutting down and stopped waiting for the run.
	ErrShutdown = sched.ErrShutdown
)

// Block strategies: what happens to a trigger that arrives while the previous run of the same job is still going.
// They are the values of Request.BlockStrategy.
const (
	// BlockSerial queues the trigger and runs it after the previous one (the default).
	BlockSerial = sched.Serial
	// BlockDiscardLater rejects the new trigger.
	BlockDiscardLater = sched.DiscardLater
	// BlockCoverEarly cancels the running trigger, discards queued ones, and runs the new one once the cancelled run
	// has returned.
	BlockCoverEarly = sched.CoverEarly
)

// Result codes reported to the admin.
const (
	CodeSuccess = wire.CodeSuccess
	CodeFailure = wire.CodeFail
	CodeTimeout = wire.CodeTimeout
)

// Request is one trigger of a job. Handlers receive their own copy: changing it affects nothing else.
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
// The error does not include the parameters, which may hold secrets.
func (r *Request) BindJSON(v any) error {
	if strings.TrimSpace(r.Params) == "" {
		return nil
	}
	if err := json.Unmarshal([]byte(r.Params), v); err != nil {
		return fmt.Errorf("xxljob: invalid job params (%d bytes): %w", len(r.Params), err)
	}
	return nil
}

// Handler runs one trigger of a job.
//
// Returning an error fails the run: the admin shows it as failed and applies its alarm and retry settings. A panic
// is reported as a failure too. Returning nil succeeds; use SetResult to attach a message. The context is cancelled
// on timeout, kill, COVER_EARLY, handler change and executor shutdown (see context.Cause). Long-running handlers must
// watch it: the run is reported as failed when it is cancelled, but later triggers of the same job wait until the
// handler returns, so that a job never runs twice at once.
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

// HandleJSON decodes the job parameters (JSON) into P before calling fn. Blank parameters leave the value at its
// zero value; invalid JSON fails the run without calling fn.
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

// Middleware wraps a Handler, e.g. for logging, tracing, metrics or rate limiting. The chain is built when a trigger
// is accepted, outside the executor's locks: a middleware may call the Executor (Stats, ...). A middleware that
// panics while building fails that trigger.
type Middleware func(next Handler) Handler
