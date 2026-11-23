package xxljob

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"slices"
	"sync/atomic"
	"time"

	"github.com/ShawineWu/go-xxljob/internal/ratelog"
	"github.com/ShawineWu/go-xxljob/internal/report"
	"github.com/ShawineWu/go-xxljob/internal/sched"
	"github.com/ShawineWu/go-xxljob/internal/wire"
)

// maxClockSkew bounds how far a trigger's log time may be from the executor's clock. It keeps log directories
// (named after that time) within reach of the retention cleanup.
const maxClockSkew = 7 * 24 * time.Hour

// Executor is an XXL-JOB executor. All methods are safe for concurrent use.
//
// It composes independent parts: the handlers (handlers.go), the scheduler of job threads (internal/sched), the run
// of one trigger (run.go), the admin client with its callbacks and registry heartbeat (internal/report), the wire
// protocol (internal/wire), the execution log store, and the HTTP endpoints (server.go). The Executor itself only
// wires them together; its state and shutdown budget belong to its lifecycle (lifecycle.go).
type Executor struct {
	cfg      Config
	opts     options
	log      *slog.Logger
	proto    wire.Protocol
	handlers *handlerSet
	sched    *sched.Scheduler
	runner   runner
	logs     LogStore

	callbacks *report.Callbacks
	registry  *report.Registry
	server    *http.Server
	rejectLog *ratelog.Limiter

	rejected atomic.Int64 // triggers refused before reaching the scheduler

	life lifecycle
}

// New validates the configuration and creates an executor. It neither listens nor registers; see Start and Run.
func New(c Config, opts ...Option) (*Executor, error) {
	cfg, err := c.normalize()
	if err != nil {
		return nil, err
	}
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	o.logger = cmp.Or(o.logger, slog.Default())
	if o.logStore == nil {
		o.logStore = NewMemoryLogStore(0)
	}
	proto, err := wire.New(cfg.AdminVersion, cfg.AccessToken, cfg.AppName)
	if err != nil {
		return nil, fmt.Errorf("xxljob: %w", err)
	}
	handlers, err := newHandlerSet(o.middlewares)
	if err != nil {
		return nil, err
	}
	client := report.NewClient(cfg.AdminAddresses, proto, o.httpClient, cfg.RequestTimeout)
	callbacks, err := report.NewCallbacks(client, o.logger, report.CallbackConfig{
		RetryInterval: cfg.CallbackRetryInterval, MaxPending: cfg.MaxPendingCallbacks, MaxBytes: maxMessageBytes,
		Dir: cfg.CallbackDir,
	})
	if err != nil {
		return nil, fmt.Errorf("xxljob: %w", err)
	}
	e := &Executor{
		cfg:       cfg,
		opts:      o,
		log:       o.logger,
		proto:     proto,
		handlers:  handlers,
		runner:    runner{store: o.logStore, app: o.logger, longRunning: cfg.LongRunningThreshold},
		logs:      o.logStore,
		callbacks: callbacks,
		registry:  report.NewRegistry(client, o.logger, cfg.AppName, cfg.Address, cfg.RegistryInterval),
		rejectLog: ratelog.New(time.Minute),
		life:      newLifecycle(),
	}
	e.sched = sched.New(sched.Config{MaxQueue: cfg.MaxQueueSize, MaxActive: cfg.MaxActiveTriggers,
		MaxConcurrency: cfg.MaxConcurrency}, e.report)
	return e, nil
}

// Config returns the effective configuration, including defaults and the derived Address.
func (e *Executor) Config() Config {
	c := e.cfg
	c.AdminAddresses = slices.Clone(c.AdminAddresses)
	return c
}

// Register registers a JobHandler. mws apply to this handler only, inside the global middlewares. It returns an
// error for an empty or duplicate name, or a nil handler or middleware.
func (e *Executor) Register(name string, h Handler, mws ...Middleware) error {
	return e.handlers.add(map[string]registered{name: {h: h, mws: slices.Clone(mws)}})
}

// MustRegister is like Register but panics on error.
func (e *Executor) MustRegister(name string, h Handler, mws ...Middleware) {
	if err := e.Register(name, h, mws...); err != nil {
		panic(err)
	}
}

// RegisterTasks registers tasks by Task.Name. If any task is nil or has an empty or duplicate name, it registers
// none of them.
func (e *Executor) RegisterTasks(tasks ...Task) error {
	entries := make(map[string]registered, len(tasks))
	for _, t := range tasks {
		if t == nil {
			return errors.New("xxljob: nil task")
		}
		name := t.Name()
		if _, dup := entries[name]; dup {
			return fmt.Errorf("xxljob: duplicate handler %q (task %T)", name, t)
		}
		entries[name] = registered{h: t}
	}
	return e.handlers.add(entries)
}

// MustRegisterTasks is like RegisterTasks but panics on error.
func (e *Executor) MustRegisterTasks(tasks ...Task) {
	if err := e.RegisterTasks(tasks...); err != nil {
		panic(err)
	}
}

// Use appends global middlewares. They apply to triggers accepted afterwards. It panics on a nil middleware, like
// http.Handle does on a nil handler.
func (e *Executor) Use(mws ...Middleware) {
	if err := e.handlers.use(mws); err != nil {
		panic(err)
	}
}

// Stats is a snapshot of the executor's activity, for metrics and alerts.
//
// Running, Queued and PendingCallbacks are current values (gauges). Accepted through TimedOut are totals since the
// executor was created (counters); every accepted trigger ends up in exactly one of Succeeded, Failed and TimedOut,
// so Accepted - Succeeded - Failed - TimedOut is the number of triggers in flight.
type Stats struct {
	// Running is the number of jobs with a running trigger, including triggers waiting for a concurrency slot and
	// cancelled runs whose handler has not returned yet.
	Running int
	// Queued is the number of queued triggers (SERIAL_EXECUTION).
	Queued int
	// PendingCallbacks is the number of results not yet delivered to the admin, including the batch being sent.
	// A steady rise means the admin is unreachable.
	PendingCallbacks int
	// Registered reports whether the last registry heartbeat succeeded. It follows the admin's availability, so
	// alert on it rather than using it as a readiness probe: an admin outage would take every instance out of
	// service.
	Registered bool

	// Accepted counts triggers the executor accepted.
	Accepted int64
	// Rejected counts triggers it refused: unknown handler, DISCARD_LATER while busy, full queue, repeated log_id,
	// unsupported glue type, invalid parameters, not started or shutting down.
	Rejected int64
	// Succeeded, Failed and TimedOut count the outcomes of accepted triggers (codes 200, 500 and 502): failures
	// include errors, panics, kills, covered or discarded triggers and shutdown.
	Succeeded, Failed, TimedOut int64
}

// Stats returns a snapshot of the executor's activity.
func (e *Executor) Stats() Stats {
	st := e.sched.Stats()
	return Stats{
		Running: st.Running, Queued: st.Queued,
		PendingCallbacks: e.callbacks.Pending(),
		Registered:       e.registry.Registered(),
		Accepted:         st.Accepted, Rejected: st.Rejected + e.rejected.Load(),
		Succeeded: st.Succeeded, Failed: st.Failed, TimedOut: st.TimedOut,
	}
}

// ---------------------------------------------------------------------------------------------
// Triggers
// ---------------------------------------------------------------------------------------------

// trigger validates a run request from the admin and hands it to the scheduler.
func (e *Executor) trigger(in wire.TriggerRequest) wire.Reply {
	req, err := e.validate(in)
	if err == nil {
		var h Handler
		if h, err = e.handlers.resolve(in.ExecutorHandler); err == nil {
			err = e.sched.Submit(&sched.Trigger{
				JobID: in.JobID, LogID: in.LogID, LogTime: in.LogDateTime, Handler: in.ExecutorHandler,
				Strategy: in.ExecutorBlockStrategy, Timeout: req.Timeout,
				Run: func(ctx context.Context) sched.Outcome {
					code, msg, _ := e.runner.run(ctx, h, req, LogRef{LogID: req.LogID, LogTime: req.LogTime})
					return sched.Outcome{Code: code, Msg: msg}
				},
			})
			if err == nil {
				return wire.OK()
			}
			return wire.Fail("%s", err) // counted by the scheduler
		}
	}
	e.rejected.Add(1)
	return wire.Fail("%s", err)
}

// validate checks what the scheduler cannot: the executor's state, the glue type and the values sent by the admin.
func (e *Executor) validate(in wire.TriggerRequest) (Request, error) {
	switch e.life.current() {
	case stateNew:
		return Request{}, errors.New("executor is not started")
	case stateClosing:
		return Request{}, ErrShutdown
	}
	switch {
	case in.GlueType != "" && in.GlueType != wire.GlueBean:
		return Request{}, fmt.Errorf("glue type [%s] is not supported, only BEAN", in.GlueType)
	case in.JobID <= 0 || in.LogID <= 0:
		return Request{}, fmt.Errorf("invalid trigger: jobId %d, logId %d", in.JobID, in.LogID)
	case in.ExecutorTimeout < 0 || in.ExecutorTimeout > math.MaxInt64/int64(time.Second):
		return Request{}, fmt.Errorf("invalid executorTimeout %d", in.ExecutorTimeout)
	}
	logTime := time.UnixMilli(in.LogDateTime)
	if d := time.Since(logTime); d > maxClockSkew || d < -maxClockSkew {
		return Request{}, fmt.Errorf("invalid logDateTime %d: more than %v away from the executor's clock", in.LogDateTime, maxClockSkew)
	}
	timeout := time.Duration(in.ExecutorTimeout) * time.Second
	if timeout == 0 {
		timeout = e.cfg.DefaultTimeout
	}
	return Request{
		JobID: in.JobID, LogID: in.LogID, LogTime: logTime, Handler: in.ExecutorHandler, Params: in.ExecutorParams,
		BlockStrategy: in.ExecutorBlockStrategy, Timeout: timeout, ShardIndex: in.BroadcastIndex,
		ShardTotal: max(in.BroadcastTotal, 1),
	}, nil
}

// report delivers a trigger's outcome to the admin. It is the scheduler's Reporter.
func (e *Executor) report(tr *sched.Trigger, o sched.Outcome) {
	e.callbacks.Push(wire.Result{LogID: tr.LogID, LogTime: tr.LogTime, Code: o.Code, Msg: wire.TruncateMsg(o.Msg)})
}
