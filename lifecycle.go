package xxljob

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type state int

const (
	stateNew state = iota
	stateRunning
	stateClosing
	stateClosed
)

// lifecycle owns the executor's state (new → running → closing → closed) and runs its shutdown, once.
type lifecycle struct {
	mu       sync.Mutex
	state    state
	stopping chan struct{} // closed when shutdown begins
	serveErr chan error    // the executor port stopped unexpectedly

	once sync.Once
	err  error
}

func newLifecycle() lifecycle {
	return lifecycle{stopping: make(chan struct{}), serveErr: make(chan error, 1)}
}

func (l *lifecycle) current() state {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.state
}

// Start listens on the executor port (unless WithoutServer is set), then starts the registry heartbeat and the
// result callbacks. It does not block. It listens before registering, so a port that is already taken fails Start
// instead of registering an executor that cannot receive triggers. Start can be called only once.
func (e *Executor) Start() error {
	if err := e.start(); err != nil {
		return err
	}
	if e.cfg.AccessToken == "" {
		e.log.Warn("xxljob: AccessToken is empty: anyone who can reach the executor port can trigger and kill jobs and read their logs")
	}
	e.log.Info("xxljob: executor started", "app_name", e.cfg.AppName, "address", e.cfg.Address, "admin_version", e.cfg.AdminVersion)
	return nil
}

func (e *Executor) start() error {
	e.life.mu.Lock()
	defer e.life.mu.Unlock()
	if e.life.state != stateNew {
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
			WriteTimeout:      30 * time.Second, // a log read of a long run
			IdleTimeout:       time.Minute,
		}
		go func() {
			if err := e.server.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				e.life.serveErr <- fmt.Errorf("xxljob: executor port stopped: %w", err)
			}
		}()
	}
	e.life.state = stateRunning
	e.callbacks.Start()
	e.registry.Start()
	return nil
}

// Run starts the executor and blocks until ctx is done, Shutdown is called, or the executor port fails. It then
// shuts down gracefully, giving running jobs Config.ShutdownTimeout to finish:
//
//	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
//	defer stop()
//	if err := exec.Run(ctx); err != nil { ... }
//
// It returns Start's error, the port failure, or Shutdown's error (context.DeadlineExceeded when running jobs had
// to be cancelled). The end of ctx itself is a normal stop and returns nil.
func (e *Executor) Run(ctx context.Context) error {
	if err := e.Start(); err != nil {
		return err
	}
	var cause error
	select {
	case <-ctx.Done():
	case <-e.life.stopping:
	case cause = <-e.life.serveErr:
		e.log.Error("xxljob: executor port failed, shutting down", "err", cause)
	}
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), e.cfg.ShutdownTimeout)
	defer cancel()
	return errors.Join(cause, e.Shutdown(sctx))
}

// Shutdown stops the executor gracefully, following a time budget fixed when it begins (see shutdownPlan):
//
//  1. reject new triggers (beat and idleBeat fail too, so failover routing sends work elsewhere);
//  2. stop the registry heartbeat and deregister: at most RequestTimeout;
//  3. discard queued triggers and triggers waiting for a concurrency slot; wait for running jobs until ctx is done,
//     then cancel them with ErrShutdown and wait at most CancelWait more;
//  4. deliver the remaining results: at least CancelWait, longer if ctx allows, plus the request in progress (it is
//     not cut short, since the admin may already have processed it); with CallbackDir set, results still undelivered
//     are saved for the next process, otherwise they are logged as lost;
//  5. close the executor port (last, so the admin can still kill jobs while waiting): at most 1s; close the log store.
//
// With one admin address, it returns at most 2×RequestTimeout + 2×CancelWait + 1s after ctx's deadline (each
// further address can add a RequestTimeout to the requests that fail over). Without a deadline it waits for running
// jobs indefinitely; every other step stays bounded. It returns ctx's error if running jobs had to be cancelled.
// Repeated and concurrent calls wait for the first one and return its result.
func (e *Executor) Shutdown(ctx context.Context) error {
	e.life.once.Do(func() { e.life.err = e.shutdown(newShutdownPlan(ctx, e.cfg)) })
	return e.life.err
}

func (e *Executor) shutdown(plan shutdownPlan) error {
	e.life.mu.Lock()
	started := e.life.state == stateRunning
	e.life.state = stateClosing
	close(e.life.stopping)
	e.life.mu.Unlock()

	if started {
		ctx, cancel := plan.deregister()
		e.registry.Close(ctx)
		cancel()
	}
	err := e.sched.Close(plan.jobs, plan.cancelWait)
	if err != nil {
		e.log.Warn("xxljob: jobs were still running at the shutdown deadline and were cancelled")
	}
	if started {
		ctx, cancel := plan.callbacks()
		e.callbacks.Close(ctx)
		cancel()
	}
	if e.server != nil {
		ctx, cancel := plan.port()
		_ = e.server.Shutdown(ctx)
		cancel()
	}
	if cerr := e.logs.Close(); cerr != nil {
		e.log.Error("xxljob: close log store", "err", cerr)
	}
	e.life.mu.Lock()
	e.life.state = stateClosed
	e.life.mu.Unlock()
	e.log.Info("xxljob: executor stopped", "app_name", e.cfg.AppName)
	return err
}

// shutdownPlan is the time budget of one shutdown, fixed when it begins. Every step takes its deadline from it,
// instead of choosing its own.
type shutdownPlan struct {
	jobs       context.Context // running jobs may finish until it is done (the caller's context)
	cancelWait time.Duration   // how long cancelled jobs get to return
	request    time.Duration   // the deregistration
	flush      time.Duration   // the least time the last callbacks get
}

// portWait bounds closing the executor port.
const portWait = time.Second

func newShutdownPlan(ctx context.Context, cfg Config) shutdownPlan {
	return shutdownPlan{jobs: ctx, cancelWait: cfg.CancelWait, request: cfg.RequestTimeout, flush: cfg.CancelWait}
}

// deregister bounds the deregistration. It does not depend on the caller's context, which may already be done:
// telling the admin to stop routing here matters most when time is short.
func (p shutdownPlan) deregister() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), p.request)
}

// callbacks bounds the delivery of the last results: at least flush, or until the caller's deadline if that is
// later.
func (p shutdownPlan) callbacks() (context.Context, context.CancelFunc) {
	deadline := time.Now().Add(p.flush)
	if d, ok := p.jobs.Deadline(); ok && d.After(deadline) {
		deadline = d
	}
	return context.WithDeadline(context.Background(), deadline)
}

func (p shutdownPlan) port() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), portWait)
}
