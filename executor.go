// Package xxljob is an XXL-JOB executor for Go.
//
// It implements the executor side of XXL-JOB (https://github.com/xuxueli/xxl-job): it registers with the admin,
// runs triggered jobs, reports results, serves execution logs and shuts down gracefully. It supports admin
// 2.3.0 ~ 3.5.x and depends only on the standard library.
//
// Basic usage:
//
//	exec, err := xxljob.New(xxljob.Config{
//		AdminAddresses: []string{"http://xxl-job-admin:8080/xxl-job-admin"},
//		AccessToken:    "default_token",
//		AppName:        "demo-executor",
//	})
//	if err != nil { ... }
//	exec.MustRegister("DemoJobHandler", xxljob.HandlerFunc(func(ctx context.Context, req *xxljob.Request) error {
//		xxljob.Logger(ctx).Info("hello", "params", req.Params)
//		return nil
//	}))
//	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
//	defer stop()
//	if err := exec.Run(ctx); err != nil { ... } // start, then shut down gracefully when ctx ends
//
// See the README for the execution model, block strategies, callbacks and shutdown.
package xxljob

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type state int

const (
	stateNew state = iota
	stateRunning
	stateClosing
)

// Executor is an XXL-JOB executor. All methods are safe for concurrent use.
type Executor struct {
	cfg   Config
	proto protocol // admin protocol, selected by Config.AdminVersion
	opts  options
	log   *slog.Logger
	admin *adminClient
	cb    *callbacker
	logs  LogStore

	// root is the parent context of every run; Shutdown cancels it with ErrShutdown when its deadline passes.
	root       context.Context
	cancelRoot context.CancelCauseFunc

	mu          sync.Mutex
	state       state
	handlers    map[string]registered
	middlewares []Middleware
	threads     map[int64]*jobThread // jobId → job thread (exists while the job has a running or queued trigger)
	wg          sync.WaitGroup       // job threads
	sem         chan struct{}        // global concurrency limit (when MaxConcurrency > 0)

	counters   counters
	registered atomic.Bool // the last registry heartbeat succeeded

	server       *http.Server
	stopRegistry chan struct{}
	registryDone chan struct{}
	shutdownOnce sync.Once
	shutdownErr  error
}

type registered struct {
	h   Handler
	mws []Middleware
}

// New validates the configuration and creates an executor. It neither listens nor registers; see Start.
func New(c Config, opts ...Option) (*Executor, error) {
	cfg, err := c.normalize()
	if err != nil {
		return nil, err
	}
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	if o.logger == nil {
		o.logger = slog.Default()
	}
	if o.logStore == nil {
		o.logStore = NewMemoryLogStore(0)
	}
	proto, err := newProtocol(cfg)
	if err != nil {
		return nil, err
	}
	admin := newAdminClient(cfg, proto, o.httpClient)
	root, cancel := context.WithCancelCause(context.Background())
	var sem chan struct{}
	if cfg.MaxConcurrency > 0 {
		sem = make(chan struct{}, cfg.MaxConcurrency)
	}
	return &Executor{
		cfg:         cfg,
		proto:       proto,
		opts:        o,
		log:         o.logger,
		admin:       admin,
		cb:          newCallbacker(admin, o.logger, cfg),
		logs:        o.logStore,
		root:        root,
		cancelRoot:  cancel,
		handlers:    map[string]registered{},
		middlewares: o.middlewares,
		threads:     map[int64]*jobThread{},
		sem:         sem,
	}, nil
}

// Config returns the effective configuration, including defaults and the derived Address.
func (e *Executor) Config() Config { return e.cfg }

// Register registers a JobHandler. mws apply to this handler only, inside the global middlewares.
// It returns an error for an empty or duplicate name.
func (e *Executor) Register(name string, h Handler, mws ...Middleware) error {
	if name == "" {
		return errors.New("xxljob: empty handler name")
	}
	if h == nil {
		return fmt.Errorf("xxljob: nil handler %q", name)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, dup := e.handlers[name]; dup {
		return fmt.Errorf("xxljob: duplicate handler %q", name)
	}
	e.handlers[name] = registered{h: h, mws: mws}
	return nil
}

// MustRegister is like Register but panics on error.
func (e *Executor) MustRegister(name string, h Handler, mws ...Middleware) {
	if err := e.Register(name, h, mws...); err != nil {
		panic(err)
	}
}

// RegisterTasks registers tasks by Task.Name. If any task is nil or has an empty or duplicate name, it returns an
// error and registers none of them.
func (e *Executor) RegisterTasks(tasks ...Task) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	seen := make(map[string]bool, len(tasks))
	for _, t := range tasks {
		if t == nil {
			return errors.New("xxljob: nil task")
		}
		name := t.Name()
		switch {
		case name == "":
			return fmt.Errorf("xxljob: task %T has an empty name", t)
		case seen[name]:
			return fmt.Errorf("xxljob: duplicate handler %q (task %T)", name, t)
		}
		if _, dup := e.handlers[name]; dup {
			return fmt.Errorf("xxljob: duplicate handler %q (task %T)", name, t)
		}
		seen[name] = true
	}
	for _, t := range tasks {
		e.handlers[t.Name()] = registered{h: t}
	}
	return nil
}

// MustRegisterTasks is like RegisterTasks but panics on error.
func (e *Executor) MustRegisterTasks(tasks ...Task) {
	if err := e.RegisterTasks(tasks...); err != nil {
		panic(err)
	}
}

// Use appends global middlewares; they apply to runs that start afterwards.
func (e *Executor) Use(mws ...Middleware) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.middlewares = append(e.middlewares, mws...)
}

// Start listens on the executor port (unless WithoutServer is set), then starts the registry heartbeat and the
// result callbacks. It does not block. It listens before registering, so a port that is already taken fails Start
// instead of registering an executor that cannot receive triggers. Start can be called only once.
func (e *Executor) Start() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state != stateNew {
		return errors.New("xxljob: executor already started or shut down")
	}
	if !e.opts.withoutServer {
		ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", ":"+strconv.Itoa(e.cfg.Port))
		if err != nil {
			return fmt.Errorf("xxljob: listen executor port %d: %w", e.cfg.Port, err)
		}
		e.server = &http.Server{
			Handler:           e.Handler(),
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       10 * time.Second,
			WriteTimeout:      10 * time.Second,
			IdleTimeout:       time.Minute,
		}
		go func() {
			if err := e.server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				e.log.Error("xxljob: executor server stopped unexpectedly", "err", err)
			}
		}()
	}
	e.state = stateRunning
	e.cb.start()
	e.stopRegistry = make(chan struct{})
	e.registryDone = make(chan struct{})
	go e.registryLoop()

	if e.cfg.AccessToken == "" {
		e.log.Warn("xxljob: AccessToken is empty, requests to the executor are not authenticated")
	}
	e.log.Info("xxljob: executor started", "app_name", e.cfg.AppName, "address", e.cfg.Address, "handlers", len(e.handlers))
	return nil
}

// Run starts the executor, blocks until ctx is done, then shuts it down gracefully, giving running jobs
// Config.ShutdownTimeout to finish. It is the usual way to run an executor from main:
//
//	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
//	defer stop()
//	if err := exec.Run(ctx); err != nil { ... }
//
// It returns Start's error if the executor cannot start, and Shutdown's error (context.DeadlineExceeded) if running
// jobs had to be cancelled; the end of ctx itself is a normal stop and returns nil.
func (e *Executor) Run(ctx context.Context) error {
	if err := e.Start(); err != nil {
		return err
	}
	<-ctx.Done()
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), e.cfg.ShutdownTimeout)
	defer cancel()
	return e.Shutdown(sctx)
}

// Shutdown stops the executor gracefully:
//
//  1. reject new triggers (run, beat and idleBeat fail, so failover routing sends work to other instances);
//  2. stop the registry heartbeat and deregister; report queued triggers as failed;
//  3. wait for running jobs until ctx is done; then cancel them with ErrShutdown and wait Config.CancelWait more;
//  4. deliver the remaining results; close the executor port (last, so the admin can still kill jobs while
//     waiting); close the log store.
//
// It returns ctx's error if running jobs had to be cancelled. It may be called more than once; concurrent calls
// wait for the first one to finish.
func (e *Executor) Shutdown(ctx context.Context) error {
	e.shutdownOnce.Do(func() { e.shutdownErr = e.shutdown(ctx) })
	return e.shutdownErr
}

func (e *Executor) shutdown(ctx context.Context) error {
	e.mu.Lock()
	started := e.state == stateRunning
	e.state = stateClosing
	var dropped []*trigger
	for _, t := range e.threads {
		dropped = append(dropped, t.drain()...)
	}
	e.mu.Unlock()

	for _, tr := range dropped {
		e.discard(tr, ErrShutdown)
	}
	if started {
		close(e.stopRegistry)
		<-e.registryDone
		e.deregister(context.Background())
	}

	var err error
	if !waitGroup(ctx, &e.wg) {
		err = ctx.Err()
		e.log.Warn("xxljob: jobs still running at shutdown deadline, cancelling them")
		e.cancelRoot(ErrShutdown)
		cctx, cancel := context.WithTimeout(context.Background(), e.cfg.CancelWait)
		if !waitGroup(cctx, &e.wg) {
			e.log.Error("xxljob: jobs did not return after cancellation", "wait", e.cfg.CancelWait.String())
		}
		cancel()
	}

	if started {
		fctx, cancel := ctx, context.CancelFunc(func() {})
		if ctx.Err() != nil {
			fctx, cancel = context.WithTimeout(context.Background(), e.cfg.CancelWait)
		}
		e.cb.close(fctx)
		cancel()
	}
	if e.server != nil {
		sctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_ = e.server.Shutdown(sctx)
		cancel()
	}
	if cerr := e.logs.Close(); cerr != nil {
		e.log.Error("xxljob: close log store", "err", cerr)
	}
	e.log.Info("xxljob: executor stopped", "app_name", e.cfg.AppName)
	return err
}

// Stats is a snapshot of the executor's activity, for metrics and health checks.
//
// Running, Queued and PendingCallbacks are current values (gauges). Accepted through TimedOut are totals since the
// executor was created (counters). Every accepted trigger ends up in exactly one of Succeeded, Failed or TimedOut,
// so Accepted - Succeeded - Failed - TimedOut is the number of triggers still in flight.
type Stats struct {
	// Running is the number of running triggers, including those waiting for a concurrency slot and those killed
	// but not yet returned.
	Running int
	// Queued is the number of queued triggers (SERIAL_EXECUTION).
	Queued int
	// PendingCallbacks is the number of results waiting to be reported, including retries.
	PendingCallbacks int
	// Registered reports whether the last registry heartbeat succeeded. It is false before the first heartbeat,
	// while the admin is unreachable, and after Shutdown; use it for readiness checks.
	Registered bool

	// Accepted counts triggers the executor accepted.
	Accepted int64
	// Rejected counts triggers the executor refused: unknown handler, DISCARD_LATER while busy, full queue,
	// repeated log_id, unsupported glue type, not started or shutting down.
	Rejected int64
	// Succeeded counts runs reported as successful (200).
	Succeeded int64
	// Failed counts runs reported as failed (500): errors, panics, kills, covered or discarded triggers, shutdown.
	Failed int64
	// TimedOut counts runs reported as timed out (502).
	TimedOut int64
}

// counters are the cumulative counts reported by Stats.
type counters struct {
	accepted, rejected, succeeded, failed, timedOut atomic.Int64
}

// Stats returns a snapshot of the executor's activity, e.g. for metrics and readiness checks.
func (e *Executor) Stats() Stats {
	st := Stats{
		Registered: e.registered.Load(),
		Accepted:   e.counters.accepted.Load(),
		Rejected:   e.counters.rejected.Load(),
		Succeeded:  e.counters.succeeded.Load(),
		Failed:     e.counters.failed.Load(),
		TimedOut:   e.counters.timedOut.Load(),
	}
	e.mu.Lock()
	for _, t := range e.threads {
		if t.running != nil {
			st.Running++
		}
		st.Queued += len(t.queue)
	}
	e.mu.Unlock()
	st.PendingCallbacks = e.cb.pendingCount()
	return st
}

// report counts a result and queues it for the admin.
func (e *Executor) report(res result) {
	switch res.Code {
	case codeSuccess:
		e.counters.succeeded.Add(1)
	case codeTimeout:
		e.counters.timedOut.Add(1)
	default:
		e.counters.failed.Add(1)
	}
	e.cb.push(res)
}

// waitGroup waits for wg or ctx and reports whether wg finished.
func waitGroup(ctx context.Context, wg *sync.WaitGroup) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}
