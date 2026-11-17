package xxljob

import (
	"context"
	"errors"
	"math/rand/v2"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Stress tests for many concurrent jobs. Trigger scenarios are generated randomly (the seed is logged; reproduce a
// failure with XXLJOB_STRESS_SEED). They assert no specific outcomes, only invariants that must hold under any
// interleaving.

// concurrencyGuard tracks per-job and global concurrent executions and records a violation when a limit is exceeded.
type concurrencyGuard struct {
	maxGlobal int

	mu       sync.Mutex
	perJob   map[int64]int
	global   int
	violated []string
}

func newConcurrencyGuard(maxGlobal int) *concurrencyGuard {
	return &concurrencyGuard{maxGlobal: maxGlobal, perJob: map[int64]int{}}
}

// middleware wraps the handler (inside the concurrency slot) and counts the executions actually in progress.
func (g *concurrencyGuard) middleware(next Handler) Handler {
	return HandlerFunc(func(ctx context.Context, req *Request) error {
		g.mu.Lock()
		g.perJob[req.JobID]++
		g.global++
		if g.perJob[req.JobID] > 1 {
			g.violated = append(g.violated, "job ran concurrently")
		}
		if g.maxGlobal > 0 && g.global > g.maxGlobal {
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

func (g *concurrencyGuard) violations() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.violated...)
}

// stressHandlers returns jobs with a mix of behaviors: success, failure, panic, occasional timeout, and jobs that end
// only when killed, covered, or timed out.
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
			time.Sleep(3 * time.Millisecond) // like a real job, cleanup takes a moment; a new trigger for the same job must not start meanwhile
			return ctx.Err()
		}),
	}
}

func stressSeed(t *testing.T) uint64 {
	seed := uint64(time.Now().UnixNano())
	if v := envUint("XXLJOB_STRESS_SEED"); v != 0 {
		seed = v
	}
	t.Logf("seed=%d (rerun with XXLJOB_STRESS_SEED=%d)", seed, seed)
	return seed
}

func envUint(key string) uint64 {
	var v uint64
	for _, c := range []byte(getenv(key)) {
		if c < '0' || c > '9' {
			return 0
		}
		v = v*10 + uint64(c-'0')
	}
	return v
}

// checkCallbacks asserts that every accepted trigger is called back exactly once and no rejected trigger is called
// back.
func checkCallbacks(t *testing.T, admin *fakeAdmin, accepted map[int64]bool) {
	t.Helper()
	require.Eventually(t, func() bool {
		admin.mu.Lock()
		defer admin.mu.Unlock()
		return len(admin.counts) >= len(accepted)
	}, 20*time.Second, 10*time.Millisecond, "callbacks missing: got %d of %d", len(admin.counts), len(accepted))
	time.Sleep(100 * time.Millisecond) // leave time for any duplicate callbacks to surface
	admin.mu.Lock()
	defer admin.mu.Unlock()
	for logID, n := range admin.counts {
		assert.True(t, accepted[logID], "callback for a rejected trigger: log_id %d", logID)
		assert.Equal(t, 1, n, "log_id %d called back %d times", logID, n)
	}
	assert.Len(t, admin.counts, len(accepted))
}

func TestMultiJobStress(t *testing.T) {
	// Scenario: many jobs are triggered at once with a mix of block strategies and job behaviors, with random kills
	//           and concurrent state reads along the way
	// Given: 40 jobs (random handler and block strategy) and 800 random operations (trigger / kill / idle beat /
	//        read log / Stats) run concurrently by 16 goroutines; global concurrency limit 6, per-job queue limit 5,
	//        default timeout 50ms; the admin fails the first few callbacks
	// Expect: every accepted trigger is called back exactly once and rejected ones never are; the same job never runs
	//         concurrently; global concurrency never exceeds the limit; no job threads or goroutines remain afterwards
	rng := rand.New(rand.NewPCG(stressSeed(t), 0))
	const jobs, ops, workers, maxConc = 40, 800, 16, 6
	guard := newConcurrencyGuard(maxConc)
	baseline := runtime.NumGoroutine()
	h := newHarness(t, stressHandlers(), func(c *Config) {
		c.MaxConcurrency = maxConc
		c.MaxQueueSize = 5
		c.DefaultTimeout = 50 * time.Millisecond
	})
	h.e.Use(guard.middleware)
	h.admin.failCallbacks.Store(3)

	handlers := []string{"ok", "ok", "fail", "panic", "slow", "stuck"}
	strategies := []string{BlockSerial, BlockSerial, BlockDiscardLater, BlockCoverEarly}
	type jobDef struct{ handler, strategy string }
	defs := make([]jobDef, jobs)
	for i := range defs {
		defs[i] = jobDef{handlers[rng.IntN(len(handlers))], strategies[rng.IntN(len(strategies))]}
	}
	type op struct {
		kind   int // 0 trigger, 1 kill, 2 idle beat, 3 read log, 4 Stats
		job    int
		logID  int64
		target int64 // log_id to read (generated up front: rng must not be shared across goroutines)
	}
	plan := make([]op, ops)
	for i := range plan {
		kind := 0
		if r := rng.IntN(10); r >= 7 {
			kind = r - 6
		}
		plan[i] = op{kind: kind, job: rng.IntN(jobs), logID: int64(i + 1), target: int64(rng.IntN(ops) + 1)}
	}

	var mu sync.Mutex
	accepted := map[int64]bool{}
	var nonTriggers atomic.Int64 // operations that are not triggers
	work := make(chan op)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for o := range work {
				switch o.kind {
				case 0:
					d := defs[o.job]
					res := h.e.dispatch(runReq{JobID: int64(o.job), LogID: o.logID, LogDateTime: time.Now().UnixMilli(),
						ExecutorHandler: d.handler, ExecutorBlockStrategy: d.strategy})
					if res.code == codeSuccess {
						mu.Lock()
						accepted[o.logID] = true
						mu.Unlock()
					}
				case 1:
					nonTriggers.Add(1)
					h.e.kill(int64(o.job))
				case 2:
					nonTriggers.Add(1)
					h.e.idleBeat(int64(o.job))
				case 3:
					nonTriggers.Add(1)
					h.e.readLog(logQuery{LogID: o.target, FromLine: 1})
				case 4:
					nonTriggers.Add(1)
					_ = h.e.Stats()
				}
			}
		}()
	}
	for _, o := range plan {
		work <- o
	}
	close(work)
	wg.Wait()

	checkCallbacks(t, h.admin, accepted)
	require.Eventually(t, func() bool { return idle(h.e.Stats()) }, 5*time.Second, 10*time.Millisecond)
	st := h.e.Stats()
	assert.Equal(t, int64(len(accepted)), st.Accepted)
	assert.Equal(t, int64(ops)-st.Accepted, st.Rejected+nonTriggers.Load(), "every trigger is either accepted or rejected")
	assert.Equal(t, st.Accepted, st.Succeeded+st.Failed+st.TimedOut, "every accepted trigger has exactly one outcome")
	assert.Empty(t, guard.violations())
	assert.NotEmpty(t, accepted)
	t.Logf("accepted %d triggers", len(accepted))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, h.e.Shutdown(ctx))
	h.srv.Close()
	h.admin.srv.Close()
	assert.Eventually(t, func() bool { return runtime.NumGoroutine() <= baseline+2 }, 5*time.Second, 50*time.Millisecond,
		"goroutines leaked: %d now, %d before", runtime.NumGoroutine(), baseline)
}

func TestShutdownUnderLoad(t *testing.T) {
	// Scenario: the executor shuts down while triggers keep arriving
	// Given: 20 jobs (half end only when canceled), 8 goroutines triggering continuously; Shutdown mid-run (100ms
	//        deadline)
	// Expect: Shutdown returns; every trigger after shutdown starts is rejected; every accepted trigger is called back
	//         exactly once (completed, discarded, or canceled); the same job never runs concurrently; no job threads
	//         remain
	guard := newConcurrencyGuard(0)
	h := newHarness(t, stressHandlers())
	h.e.Use(guard.middleware)

	var mu sync.Mutex
	accepted := map[int64]bool{}
	var nextLog atomic.Int64
	var closing atomic.Bool
	var acceptedAfterClose atomic.Int32
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				job := (w*7 + i) % 20
				handler := "ok"
				if job%2 == 0 {
					handler = "stuck"
				}
				logID := nextLog.Add(1)
				wasClosing := closing.Load()
				res := h.e.dispatch(runReq{JobID: int64(job), LogID: logID, LogDateTime: time.Now().UnixMilli(), ExecutorHandler: handler})
				if res.code == codeSuccess {
					mu.Lock()
					accepted[logID] = true
					mu.Unlock()
					if wasClosing {
						acceptedAfterClose.Add(1)
					}
				}
				time.Sleep(time.Millisecond)
			}
		}()
	}
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	shutdownDone := make(chan struct{})
	go func() {
		_ = h.e.Shutdown(ctx)
		close(shutdownDone)
	}()
	// Set only after observing the executor in the closing state: any trigger that then reads closing happens after
	// shutdown started and must be rejected.
	require.Eventually(t, func() bool {
		h.e.mu.Lock()
		defer h.e.mu.Unlock()
		return h.e.state == stateClosing
	}, time.Second, time.Millisecond)
	closing.Store(true)
	<-shutdownDone
	time.Sleep(20 * time.Millisecond) // keep triggering for a while after shutdown completes
	close(stop)
	wg.Wait()

	checkCallbacks(t, h.admin, accepted)
	assert.Zero(t, acceptedAfterClose.Load(), "triggers accepted after shutdown started")
	assert.Empty(t, guard.violations())
	h.e.mu.Lock()
	assert.Empty(t, h.e.threads)
	h.e.mu.Unlock()
}

func getenv(key string) string { return os.Getenv(key) }
