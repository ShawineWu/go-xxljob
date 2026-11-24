package xxljob

import (
	"context"
	"errors"
	"math/rand/v2"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ShawineWu/go-xxljob/internal/wire"
)

// Stress tests for many concurrent jobs. Scenarios are random (the seed is logged; reproduce a failure with
// XXLJOB_STRESS_SEED). They assert no specific outcome, only invariants that must hold under any interleaving.

// guard checks, from inside the handlers, that no job runs twice at once and that MaxConcurrency holds.
type guard struct {
	limit int

	mu       sync.Mutex
	perJob   map[int64]int
	global   int
	violated []string
}

func newGuard(limit int) *guard { return &guard{limit: limit, perJob: map[int64]int{}} }

func (g *guard) middleware(next Handler) Handler {
	return HandlerFunc(func(ctx context.Context, req *Request) error {
		g.mu.Lock()
		g.perJob[req.JobID]++
		g.global++
		if g.perJob[req.JobID] > 1 {
			g.violated = append(g.violated, "job ran concurrently")
		}
		if g.limit > 0 && g.global > g.limit {
			g.violated = append(g.violated, "MaxConcurrency exceeded")
		}
		g.mu.Unlock()
		defer func() {
			g.mu.Lock()
			g.perJob[req.JobID]--
			g.global--
			g.mu.Unlock()
		}()
		return next.Handle(ctx, req)
	})
}

func (g *guard) violations() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.violated...)
}

// stressHandlers mix every behaviour: success, failure, panic, timeouts, and jobs that end only when cancelled or
// that ignore cancellation for a while.
func stressHandlers() map[string]Handler {
	sleep := func(ctx context.Context, d time.Duration) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(d):
			return nil
		}
	}
	return map[string]Handler{
		"ok": HandlerFunc(func(ctx context.Context, _ *Request) error {
			Logger(ctx).Info("working")
			SetResult(ctx, "done")
			return sleep(ctx, time.Duration(rand.IntN(3))*time.Millisecond)
		}),
		"fail":  HandlerFunc(func(context.Context, *Request) error { return errors.New("business failed") }),
		"panic": HandlerFunc(func(context.Context, *Request) error { panic("boom") }),
		"slow": HandlerFunc(func(ctx context.Context, _ *Request) error {
			return sleep(ctx, time.Duration(5+rand.IntN(60))*time.Millisecond) // some exceed DefaultTimeout
		}),
		"stuck": HandlerFunc(func(ctx context.Context, _ *Request) error {
			<-ctx.Done()
			time.Sleep(3 * time.Millisecond) // cleanup takes a moment: the job's next trigger must not start meanwhile
			return ctx.Err()
		}),
		"stubborn": HandlerFunc(func(context.Context, *Request) error {
			time.Sleep(time.Duration(rand.IntN(20)) * time.Millisecond) // ignores cancellation entirely
			return nil
		}),
	}
}

func stressSeed(t *testing.T) uint64 {
	t.Helper()
	seed := uint64(time.Now().UnixNano())
	if v, err := strconv.ParseUint(os.Getenv("XXLJOB_STRESS_SEED"), 10, 64); err == nil {
		seed = v
	}
	t.Logf("seed=%d (rerun with XXLJOB_STRESS_SEED=%d)", seed, seed)
	return seed
}

// checkCallbacks asserts that every accepted trigger is called back exactly once and no rejected one is.
func checkCallbacks(t *testing.T, admin *fakeAdmin, accepted map[int64]bool) {
	t.Helper()
	require.Eventually(t, func() bool { return len(admin.callbackCount()) >= len(accepted) },
		20*time.Second, 10*time.Millisecond, "callbacks missing")
	time.Sleep(100 * time.Millisecond) // leave time for duplicates to surface
	counts := admin.callbackCount()
	for logID, n := range counts {
		assert.True(t, accepted[logID], "callback for a rejected trigger: log_id %d", logID)
		assert.Equal(t, 1, n, "log_id %d called back %d times", logID, n)
	}
	assert.Len(t, counts, len(accepted))
}

func TestMultiJobStress(t *testing.T) {
	// Scenario: many jobs are triggered at once with every block strategy and behaviour, with random kills, busy
	//           checks, log reads and stats reads, from 16 goroutines
	// Given: 40 jobs, 800 operations; concurrency limit 6, per-job queue limit 5, default timeout 50ms; the admin
	//        fails for a while
	// Expect: every accepted trigger is called back exactly once and rejected ones never; no job overlaps itself;
	//         the concurrency limit holds; the counters balance; nothing is left running and no goroutine leaks
	rng := rand.New(rand.NewPCG(stressSeed(t), 0))
	const jobs, ops, workers, maxConc = 40, 800, 16, 6
	g := newGuard(maxConc)
	baseline := runtime.NumGoroutine()
	h := newHarness(t, stressHandlers(), func(c *Config) {
		c.MaxConcurrency = maxConc
		c.MaxQueueSize = 5
		c.DefaultTimeout = 50 * time.Millisecond
	})
	h.e.Use(g.middleware)
	h.admin.down.Store(true)
	time.AfterFunc(100*time.Millisecond, func() { h.admin.down.Store(false) })

	handlers := []string{"ok", "ok", "fail", "panic", "slow", "stuck", "stubborn"}
	strategies := []string{BlockSerial, BlockSerial, BlockDiscardLater, BlockCoverEarly}
	type jobDef struct{ handler, strategy string }
	defs := make([]jobDef, jobs)
	for i := range defs {
		defs[i] = jobDef{handlers[rng.IntN(len(handlers))], strategies[rng.IntN(len(strategies))]}
	}
	type op struct {
		kind  int // 0 trigger, 1 kill, 2 busy, 3 read log, 4 stats
		job   int64
		logID int64
	}
	plan := make([]op, ops) // generated up front: the generator must not be shared across goroutines
	triggers := 0
	for i := range plan {
		kind := 0
		if r := rng.IntN(10); r >= 6 {
			kind = min(r-5, 4)
		}
		plan[i] = op{kind: kind, job: int64(rng.IntN(jobs) + 1), logID: int64(i + 1)}
		if kind == 0 {
			triggers++
		}
	}

	var mu sync.Mutex
	accepted := map[int64]bool{}
	work := make(chan op)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for o := range work {
				switch o.kind {
				case 0:
					d := defs[o.job-1]
					res := h.e.trigger(wire.TriggerRequest{JobID: o.job, LogID: o.logID, LogDateTime: time.Now().UnixMilli(),
						ExecutorHandler: d.handler, ExecutorBlockStrategy: d.strategy})
					if res.Code == CodeSuccess {
						mu.Lock()
						accepted[o.logID] = true
						mu.Unlock()
					}
				case 1:
					h.e.sched.Kill(o.job)
				case 2:
					h.e.sched.Busy(o.job)
				case 3:
					h.e.readLog(wire.LogRequest{LogID: o.logID, LogTime: time.Now().UnixMilli(), FromLine: 1})
				case 4:
					st := h.e.Stats()
					if st.Succeeded+st.Failed+st.TimedOut > st.Accepted {
						t.Errorf("more outcomes than accepted triggers: %+v", st)
					}
				}
			}
		})
	}
	for _, o := range plan {
		work <- o
	}
	close(work)
	wg.Wait()

	checkCallbacks(t, h.admin, accepted)
	require.Eventually(t, func() bool { st := h.e.Stats(); return st.Running == 0 && st.Queued == 0 }, 5*time.Second, 10*time.Millisecond)
	st := h.e.Stats()
	assert.Empty(t, g.violations())
	assert.Equal(t, int64(len(accepted)), st.Accepted)
	assert.Equal(t, st.Accepted, st.Succeeded+st.Failed+st.TimedOut)
	assert.Equal(t, int64(triggers), st.Accepted+st.Rejected, "every trigger is accepted or rejected")
	t.Logf("accepted %d of %d triggers", len(accepted), triggers)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	require.NoError(t, h.e.Shutdown(ctx))
	h.srv.Close()
	h.admin.srv.Close()
	assert.Eventually(t, func() bool { return runtime.NumGoroutine() <= baseline+2 }, 5*time.Second, 50*time.Millisecond,
		"goroutines leaked: %d now, %d before", runtime.NumGoroutine(), baseline)
}

func TestShutdownUnderLoad(t *testing.T) {
	// Scenario: the executor shuts down while triggers keep arriving
	// Given: 20 jobs (half end only when cancelled), 8 goroutines triggering continuously; Shutdown with a 100ms
	//        deadline
	// Expect: every trigger that arrives once shutdown has begun is rejected; every accepted trigger is called back
	//         exactly once (completed, discarded or cancelled); no job overlaps itself; nothing is left running
	g := newGuard(0)
	h := newHarness(t, stressHandlers())
	h.e.Use(g.middleware)

	var mu sync.Mutex
	accepted := map[int64]bool{}
	var nextLog atomic.Int64
	var closing atomic.Bool
	var acceptedAfterClose atomic.Int32
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Go(func() {
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				job := int64((w*7+i)%20 + 1)
				handler := "ok"
				if job%2 == 0 {
					handler = "stuck"
				}
				logID := nextLog.Add(1)
				wasClosing := closing.Load()
				res := h.e.trigger(wire.TriggerRequest{JobID: job, LogID: logID, LogDateTime: time.Now().UnixMilli(), ExecutorHandler: handler})
				if res.Code == CodeSuccess {
					mu.Lock()
					accepted[logID] = true
					mu.Unlock()
					if wasClosing {
						acceptedAfterClose.Add(1)
					}
				}
				time.Sleep(time.Millisecond)
			}
		})
	}
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() {
		_ = h.e.Shutdown(ctx)
		close(done)
	}()
	// Set only after observing the closing state: any trigger that then reads closing came after shutdown began.
	require.Eventually(t, func() bool { return h.e.life.accepting() != nil }, time.Second, time.Millisecond)
	closing.Store(true)
	<-done
	time.Sleep(20 * time.Millisecond) // keep triggering for a while after shutdown completes
	close(stop)
	wg.Wait()

	checkCallbacks(t, h.admin, accepted)
	assert.Zero(t, acceptedAfterClose.Load(), "triggers accepted after shutdown began")
	assert.Empty(t, g.violations())
	assert.Zero(t, h.e.Stats().Running)
}
