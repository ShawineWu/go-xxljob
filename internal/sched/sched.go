// Package sched runs job triggers. Each job (jobId) has a job thread that runs its triggers one at a time, in order;
// block strategies decide what happens to a trigger that arrives while the job is busy. The scheduler knows nothing
// about HTTP, the wire protocol or the admin: it receives triggers and reports exactly one outcome for each accepted
// one.
//
// Every accepted trigger is a run with an explicit phase:
//
//		queued ──► ready ──► waiting ──► running ──► cancelling
//		   │         │          │           │            │
//		   └─────────┴──────────┴───────────┴────────────┴──► finished
//
//	  - queued: in its job's queue;
//	  - ready: its job's current run, about to take a concurrency slot;
//	  - waiting: blocked on a concurrency slot (discarded by Close like a queued run);
//	  - running: Run has been called;
//	  - cancelling: cancelled (kill, cover, handler change, timeout, shutdown) and already reported, Run not returned;
//	  - finished: done; its resources are released.
//
// Phases change only through to. Outcomes leave only through settle, once per run; resources are released only
// through retire, once per run. The number of runs not finished is capped (Config.MaxActive), which bounds the job
// goroutines and everything a run holds.
package sched

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"
)

// Causes a run's context is cancelled with. They are reported to the admin as the run's result.
var (
	ErrKilled         = errors.New("xxljob: job killed by the scheduling center")
	ErrCovered        = errors.New("xxljob: job covered by a newer trigger")
	ErrHandlerChanged = errors.New("xxljob: job handler changed")
	ErrTimeout        = errors.New("xxljob: job execution timeout")
	ErrShutdown       = errors.New("xxljob: executor is shutting down")
)

// Reasons a trigger is rejected.
var (
	ErrDiscardLater = errors.New("block strategy DISCARD_LATER: job is still running")
	ErrQueueFull    = errors.New("job queue is full")
	ErrBusy         = errors.New("executor is busy")
	ErrRepeated     = errors.New("repeated trigger: log_id is already running or queued")
)

// Block strategies (the admin's executorBlockStrategy).
const (
	Serial       = "SERIAL_EXECUTION"
	DiscardLater = "DISCARD_LATER"
	CoverEarly   = "COVER_EARLY"
)

// NormalizeStrategy returns s if it is a block strategy, Serial otherwise (the admin may send none).
func NormalizeStrategy(s string) string {
	if s == DiscardLater || s == CoverEarly {
		return s
	}
	return Serial
}

// Trigger is one trigger of a job.
type Trigger struct {
	JobID    int64
	LogID    int64
	LogTime  int64  // opaque to the scheduler; carried for the reporter
	Handler  string // a change of handler cancels the job's running trigger
	Strategy string // one of the block strategies; anything else means Serial
	Timeout  time.Duration
	// Run executes the trigger. It is called at most once, with a context that is cancelled with one of the causes
	// above, and it must return (it may return after the trigger's outcome was already reported).
	Run func(ctx context.Context) Result
	// Done, if set, is called after Run has returned, on the same goroutine, with the outcome that was reported: the
	// trigger's one decision, which may come from a cancellation rather than from what Run returned.
	Done func(Outcome)
}

// Result is what a run produced: a failure with its message, or a success with an optional note. It is plain data,
// so that classifying it never calls into user code.
type Result struct {
	Failed bool
	Msg    string
}

// Status is how a trigger ended.
type Status uint8

// Statuses, reported to the admin as codes 200, 500 and 502.
const (
	Succeeded Status = iota
	Failed
	TimedOut
)

// Outcome is the result reported for a trigger.
type Outcome struct {
	Status Status
	Msg    string
}

// Classify decides the outcome of a run. It is the only place that does: a cancellation cause wins over what the
// run returned, so a killed or timed-out run that ignored its cancellation is never reported as a success.
func Classify(cause error, r Result) Outcome {
	switch {
	case errors.Is(cause, ErrTimeout):
		return Outcome{TimedOut, cause.Error()}
	case IsCause(cause):
		return Outcome{Failed, cause.Error()}
	case r.Failed:
		return Outcome{Failed, r.Msg}
	}
	return Outcome{Succeeded, r.Msg}
}

// IsCause reports whether err is (or wraps) one of the scheduler's cancellation causes.
func IsCause(err error) bool {
	for _, c := range []error{ErrKilled, ErrCovered, ErrHandlerChanged, ErrTimeout, ErrShutdown} {
		if errors.Is(err, c) {
			return true
		}
	}
	return false
}

// Reporter receives the outcome of every accepted trigger, exactly once. It is called without the scheduler's lock
// held, possibly from several goroutines at once.
type Reporter func(tr *Trigger, o Outcome)

// Config limits the scheduler. Zero values mean no limit.
type Config struct {
	MaxQueue       int // queued triggers per job
	MaxActive      int // accepted triggers not finished yet (running, waiting or queued), across all jobs
	MaxConcurrency int // runs executing at once across all jobs
}

// Stats is a consistent snapshot of the scheduler: Accepted - Succeeded - Failed - TimedOut triggers have not been
// reported yet.
type Stats struct {
	Running   int // jobs whose current run is ready, waiting, running or cancelling
	Queued    int
	Active    int // runs not finished
	Accepted  int64
	Rejected  int64
	Succeeded int64
	Failed    int64
	TimedOut  int64
}

type phase uint8

const (
	queued phase = iota
	ready
	waiting
	running
	cancelling
	finished
)

var phaseNames = [...]string{"queued", "ready", "waiting", "running", "cancelling", "finished"}

func (p phase) String() string { return phaseNames[p] }

// next lists the phases each phase may move to.
var next = [...][]phase{
	queued:     {ready, finished},
	ready:      {waiting, running, finished},
	waiting:    {running, finished},
	running:    {cancelling, finished},
	cancelling: {finished},
	finished:   nil,
}

// Scheduler runs triggers. It is safe for concurrent use.
type Scheduler struct {
	cfg    Config
	report Reporter
	sem    chan struct{} // nil without a concurrency limit

	mu      sync.Mutex
	closing bool
	threads map[int64]*thread
	logIDs  map[int64]struct{} // log IDs of every run not finished, across jobs
	changed chan struct{}      // closed and replaced whenever a run finishes
	stats   Stats              // counters, Queued and Active; Running is computed

	wg sync.WaitGroup // job thread goroutines
}

// thread is the state of one job. It exists while the job has a current or queued run.
type thread struct {
	jobID   int64
	handler string
	current *run
	queue   []*run
}

// run is one accepted trigger. phase and reported are guarded by Scheduler.mu.
type run struct {
	tr        *Trigger
	ctx       context.Context
	cancel    context.CancelCauseFunc
	phase     phase
	reported  bool
	outcome   Outcome       // the one reported; set with reported
	delivered chan struct{} // closed once the Reporter has returned for this run
}

// New returns a scheduler that reports outcomes to report.
func New(cfg Config, report Reporter) *Scheduler {
	s := &Scheduler{cfg: cfg, report: report, threads: map[int64]*thread{}, logIDs: map[int64]struct{}{},
		changed: make(chan struct{})}
	if cfg.MaxConcurrency > 0 {
		s.sem = make(chan struct{}, cfg.MaxConcurrency)
	}
	return s
}

// Submit accepts a trigger or rejects it with an error that explains why (shown in the admin's trigger log).
func (s *Scheduler) Submit(tr *Trigger) error {
	dropped, cause, err := s.submit(tr)
	s.discard(dropped, cause)
	return err
}

func (s *Scheduler) submit(tr *Trigger) (dropped []*run, cause, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	defer func() {
		if err != nil {
			s.stats.Rejected++
		}
	}()
	_, dup := s.logIDs[tr.LogID] // log IDs key execution logs and results, across jobs
	switch {
	case s.closing:
		return nil, nil, ErrShutdown
	case dup:
		return nil, nil, fmt.Errorf("%w (log_id %d)", ErrRepeated, tr.LogID)
	case s.cfg.MaxActive > 0 && s.stats.Active >= s.cfg.MaxActive:
		return nil, nil, fmt.Errorf("%w (%d triggers running or queued)", ErrBusy, s.stats.Active)
	}
	t := s.threads[tr.JobID]
	if t == nil {
		r := s.admit(tr)
		s.to(r, ready)
		t = &thread{jobID: tr.JobID, handler: tr.Handler, current: r}
		s.threads[tr.JobID] = t
		s.wg.Go(func() { s.loop(t) })
		return nil, nil, nil
	}
	switch {
	case t.handler != tr.Handler:
		cause = ErrHandlerChanged
		dropped = s.abort(t, cause)
		t.handler = tr.Handler
	case tr.Strategy == DiscardLater:
		return nil, nil, ErrDiscardLater
	case tr.Strategy == CoverEarly:
		cause = ErrCovered
		dropped = s.abort(t, cause)
	case s.cfg.MaxQueue > 0 && len(t.queue) >= s.cfg.MaxQueue:
		return nil, nil, fmt.Errorf("%w (%d triggers of this job waiting)", ErrQueueFull, len(t.queue))
	}
	t.queue = append(t.queue, s.admit(tr))
	s.stats.Queued++
	return dropped, cause, nil
}

// admit records a newly accepted trigger as a queued run. Called with s.mu held.
func (s *Scheduler) admit(tr *Trigger) *run {
	ctx, cancel := context.WithCancelCause(context.Background())
	s.logIDs[tr.LogID] = struct{}{}
	s.stats.Accepted++
	s.stats.Active++
	return &run{tr: tr, ctx: ctx, cancel: cancel, phase: queued, delivered: make(chan struct{})}
}

// to moves r to phase p. Called with s.mu held; an illegal move is a bug.
func (s *Scheduler) to(r *run, p phase) {
	if slices.Contains(next[r.phase], p) {
		r.phase = p
		return
	}
	panic(fmt.Sprintf("sched: illegal phase change %s -> %s (log_id %d)", r.phase, p, r.tr.LogID))
}

// settle reports r's outcome. It is the only way an outcome leaves the scheduler, and it reports once per run; the
// run cannot finish before the report has been delivered (see retire).
func (s *Scheduler) settle(r *run, o Outcome) {
	s.mu.Lock()
	if r.reported {
		s.mu.Unlock()
		return
	}
	r.reported, r.outcome = true, o
	switch o.Status {
	case Succeeded:
		s.stats.Succeeded++
	case TimedOut:
		s.stats.TimedOut++
	default:
		s.stats.Failed++
	}
	s.mu.Unlock()
	defer close(r.delivered)
	s.report(r.tr, o)
}

// retire finishes r and releases what it holds. It is called once per run, after settle; it waits until the
// outcome has been delivered, which may happen on another goroutine (a cancellation), so that a finished run never
// has a report still in progress.
func (s *Scheduler) retire(r *run) {
	<-r.delivered
	r.cancel(nil)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.to(r, finished)
	delete(s.logIDs, r.tr.LogID)
	s.stats.Active--
	close(s.changed)
	s.changed = make(chan struct{})
}

// Kill cancels the job's current run and discards its queued ones. It reports whether the job was busy.
func (s *Scheduler) Kill(jobID int64) bool {
	s.mu.Lock()
	t := s.threads[jobID]
	var dropped []*run
	if t != nil {
		dropped = s.abort(t, ErrKilled)
	}
	s.mu.Unlock()
	s.discard(dropped, ErrKilled)
	return t != nil
}

// Busy reports whether the job has a current or queued run.
func (s *Scheduler) Busy(jobID int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.threads[jobID] != nil
}

// Stats returns a consistent snapshot.
func (s *Scheduler) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.stats
	for _, t := range s.threads {
		if t.current != nil {
			st.Running++
		}
	}
	return st
}

// Close stops accepting triggers and discards the queued ones, as well as runs waiting for a concurrency slot. It
// waits for the other runs until ctx is done, then cancels them with ErrShutdown and waits grace more. It returns
// ctx's error if runs had to be cancelled, nil otherwise.
func (s *Scheduler) Close(ctx context.Context, grace time.Duration) error {
	s.mu.Lock()
	s.closing = true
	var dropped []*run
	for _, t := range s.threads {
		dropped = append(dropped, t.queue...)
		s.stats.Queued -= len(t.queue)
		t.queue = nil
		if r := t.current; r != nil && r.phase == waiting {
			r.cancel(ErrShutdown)
		}
	}
	s.mu.Unlock()
	s.discard(dropped, ErrShutdown)

	if s.waitIdle(ctx) {
		s.wg.Wait()
		return nil
	}
	s.mu.Lock()
	var cancelled []*run
	for _, t := range s.threads {
		if t.current != nil {
			t.current.cancel(ErrShutdown)
			cancelled = append(cancelled, t.current)
		}
	}
	s.mu.Unlock()
	gctx, cancel := context.WithTimeout(context.Background(), grace)
	defer cancel()
	// A cancellation is reported at once, from the cancelled context's AfterFunc, and reporting never blocks for
	// long: wait for those reports, whatever the grace, so that none arrives after Close has returned.
	for _, r := range cancelled {
		<-r.delivered
	}
	if s.waitIdle(gctx) {
		s.wg.Wait()
	}
	return ctx.Err()
}

// waitIdle waits until every run has finished, or ctx is done. Being idle wins over an expired ctx.
func (s *Scheduler) waitIdle(ctx context.Context) bool {
	for {
		s.mu.Lock()
		idle := s.stats.Active == 0
		changed := s.changed
		s.mu.Unlock()
		if idle {
			return true
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return false
		}
	}
}

// abort cancels the job's current run with cause and removes the queued ones, which the caller discards. Called
// with s.mu held.
func (s *Scheduler) abort(t *thread, cause error) []*run {
	if t.current != nil {
		t.current.cancel(cause)
	}
	dropped := t.queue
	t.queue = nil
	s.stats.Queued -= len(dropped)
	return dropped
}

// discard settles and retires runs removed from a queue before they started.
func (s *Scheduler) discard(dropped []*run, cause error) {
	for _, r := range dropped {
		s.settle(r, Outcome{Failed, "trigger discarded: " + cause.Error()})
		s.retire(r)
	}
}

// loop is the job thread: it executes the job's runs one by one and removes the thread when the queue is empty. The
// next run becomes current under the same lock that ends the previous one, so the job never looks idle in between.
func (s *Scheduler) loop(t *thread) {
	s.mu.Lock()
	r := t.current
	s.mu.Unlock()
	for r != nil {
		s.execute(r)
		s.mu.Lock()
		r = nil
		if len(t.queue) > 0 {
			r = t.queue[0]
			t.queue[0] = nil
			t.queue = t.queue[1:]
			s.stats.Queued--
			s.to(r, ready)
		} else {
			delete(s.threads, t.jobID)
		}
		t.current = r
		s.mu.Unlock()
	}
}

// execute takes a concurrency slot, then runs r. A cancellation is reported the moment it happens, even if Run has
// not returned; the job stays busy until it does, so its runs never overlap.
func (s *Scheduler) execute(r *run) {
	defer s.retire(r)
	if !s.acquire(r) {
		s.settle(r, Classify(context.Cause(r.ctx), Result{}))
		return
	}
	if s.sem != nil {
		defer func() { <-s.sem }()
	}

	ctx := r.ctx
	if r.tr.Timeout > 0 {
		var stop context.CancelFunc
		ctx, stop = context.WithTimeoutCause(ctx, r.tr.Timeout, ErrTimeout)
		defer stop()
	}
	stopWatch := context.AfterFunc(ctx, func() {
		if cause := context.Cause(ctx); IsCause(cause) {
			s.mu.Lock()
			if r.phase == running {
				s.to(r, cancelling)
			}
			s.mu.Unlock()
			s.settle(r, Classify(cause, Result{}))
		}
	})
	res := s.call(ctx, r.tr)
	stopWatch()
	s.settle(r, Classify(context.Cause(ctx), res))
	if r.tr.Done != nil {
		s.mu.Lock()
		o := r.outcome
		s.mu.Unlock()
		s.done(r.tr, o)
	}
}

// done calls the trigger's Done. A panic there is contained like one in Run: the run has been reported already.
func (s *Scheduler) done(tr *Trigger, o Outcome) {
	defer func() { _ = recover() }()
	tr.Done(o)
}

// call runs the trigger. Run is not expected to panic; if it does anyway, the run fails instead of leaving its job
// busy forever.
func (s *Scheduler) call(ctx context.Context, tr *Trigger) (res Result) {
	defer func() {
		if p := recover(); p != nil {
			res = Result{Failed: true, Msg: fmt.Sprint("xxljob: run panicked: ", p)}
		}
	}()
	return tr.Run(ctx)
}

// acquire moves r from ready to running, taking a concurrency slot first. A run that has to wait for a slot is
// discarded by Close like a queued one. It reports false when r was cancelled before it could run.
func (s *Scheduler) acquire(r *run) bool {
	if s.sem != nil {
		select {
		case s.sem <- struct{}{}:
		default:
			if !s.wait(r) {
				return false
			}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.ctx.Err() != nil {
		if s.sem != nil {
			<-s.sem
		}
		return false
	}
	s.to(r, running)
	return true
}

// wait blocks until r gets a concurrency slot. It reports false when r was cancelled meanwhile.
func (s *Scheduler) wait(r *run) bool {
	s.mu.Lock()
	s.to(r, waiting)
	if s.closing {
		r.cancel(ErrShutdown)
	}
	s.mu.Unlock()
	select {
	case s.sem <- struct{}{}:
		return true
	case <-r.ctx.Done():
		return false
	}
}
