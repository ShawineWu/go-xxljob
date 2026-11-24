package sched

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recorder collects reported outcomes and checks that no job ever runs two triggers at once.
type recorder struct {
	mu       sync.Mutex
	outcomes map[int64][]Outcome // by log ID
	order    []int64             // log IDs in the order their Run started
	active   map[int64]int       // running Run calls per job
	overlaps int
}

func newRecorder() *recorder {
	return &recorder{outcomes: map[int64][]Outcome{}, active: map[int64]int{}}
}

func (r *recorder) report(tr *Trigger, o Outcome) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.outcomes[tr.LogID] = append(r.outcomes[tr.LogID], o)
}

func (r *recorder) outcome(t *testing.T, logID int64) Outcome {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	require.Len(t, r.outcomes[logID], 1, "log %d must be reported exactly once", logID)
	return r.outcomes[logID][0]
}

func (r *recorder) reported(logID int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.outcomes[logID]) > 0
}

// trigger builds a trigger whose Run is wrapped to record start order and overlap.
func (r *recorder) trigger(jobID, logID int64, strategy string, run func(ctx context.Context) Result) *Trigger {
	tr := &Trigger{JobID: jobID, LogID: logID, Handler: "h", Strategy: strategy}
	tr.Run = func(ctx context.Context) Result {
		r.mu.Lock()
		r.order = append(r.order, logID)
		r.active[jobID]++
		if r.active[jobID] > 1 {
			r.overlaps++
		}
		r.mu.Unlock()
		defer func() {
			r.mu.Lock()
			r.active[jobID]--
			r.mu.Unlock()
		}()
		return run(ctx)
	}
	return tr
}

func succeed(context.Context) Result { return Result{} }

// sleep runs for d unless cancelled.
func sleep(d time.Duration) func(context.Context) Result {
	return func(ctx context.Context) Result {
		select {
		case <-time.After(d):
			return Result{Msg: "done"}
		case <-ctx.Done():
			return Result{Failed: true, Msg: "cancelled"}
		}
	}
}

// stubborn runs for d and ignores cancellation, then claims success.
func stubborn(d time.Duration) func(context.Context) Result {
	return func(context.Context) Result {
		time.Sleep(d)
		return Result{Msg: "ignored cancellation"}
	}
}

func TestSerialRunsInOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Scenario: three triggers of one job arrive while it is busy (SERIAL_EXECUTION)
		// Expect: they run one at a time in arrival order, each reported once as success
		rec := newRecorder()
		s := New(Config{}, rec.report)

		for i := int64(1); i <= 3; i++ {
			require.NoError(t, s.Submit(rec.trigger(1, i, Serial, sleep(time.Second))))
		}
		busy := s.Stats()
		time.Sleep(10 * time.Second)
		synctest.Wait()

		assert.Equal(t, Stats{Running: 1, Queued: 2, Active: 3, Accepted: 3}, busy)
		assert.Equal(t, []int64{1, 2, 3}, rec.order)
		for i := int64(1); i <= 3; i++ {
			assert.Equal(t, Outcome{Succeeded, "done"}, rec.outcome(t, i))
		}
		assert.Zero(t, rec.overlaps)
		assert.False(t, s.Busy(1))
		assert.Equal(t, Stats{Accepted: 3, Succeeded: 3}, s.Stats())
	})
}

func TestDiscardLater(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Scenario: DISCARD_LATER while the job runs, then again once it is idle
		// Expect: rejected while busy (never reported), accepted when idle
		rec := newRecorder()
		s := New(Config{}, rec.report)

		require.NoError(t, s.Submit(rec.trigger(1, 1, Serial, sleep(time.Second))))
		busyErr := s.Submit(rec.trigger(1, 2, DiscardLater, succeed))
		time.Sleep(2 * time.Second)
		synctest.Wait()
		idleErr := s.Submit(rec.trigger(1, 3, DiscardLater, succeed))
		synctest.Wait()

		require.ErrorIs(t, busyErr, ErrDiscardLater)
		require.NoError(t, idleErr)
		assert.False(t, rec.reported(2))
		assert.Equal(t, Succeeded, rec.outcome(t, 3).Status)
		assert.Equal(t, int64(1), s.Stats().Rejected)
	})
}

func TestCoverEarly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Scenario: COVER_EARLY arrives while trigger 1 runs (ignoring cancellation for 5s) and trigger 2 is queued
		// Expect: 1 is reported as covered at once, 2 as discarded; 3 starts only after 1 returns, so they never
		//         overlap
		rec := newRecorder()
		s := New(Config{}, rec.report)

		require.NoError(t, s.Submit(rec.trigger(1, 1, Serial, stubborn(5*time.Second))))
		require.NoError(t, s.Submit(rec.trigger(1, 2, Serial, succeed)))
		synctest.Wait()
		require.NoError(t, s.Submit(rec.trigger(1, 3, CoverEarly, succeed)))
		synctest.Wait()
		coveredAt := rec.reported(1)
		startedEarly := rec.reported(3)
		time.Sleep(10 * time.Second)
		synctest.Wait()

		assert.True(t, coveredAt, "covered run reported immediately")
		assert.False(t, startedEarly, "new trigger waits for the covered run to return")
		assert.Equal(t, Outcome{Failed, ErrCovered.Error()}, rec.outcome(t, 1))
		assert.Equal(t, Outcome{Failed, "trigger discarded: " + ErrCovered.Error()}, rec.outcome(t, 2))
		assert.Equal(t, Succeeded, rec.outcome(t, 3).Status)
		assert.Equal(t, []int64{1, 3}, rec.order)
		assert.Zero(t, rec.overlaps)
	})
}

func TestHandlerChange(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Scenario: the job is switched to another JobHandler while a trigger runs and one is queued
		// Expect: the running one is cancelled with ErrHandlerChanged, the queued one discarded, the new one runs
		rec := newRecorder()
		s := New(Config{}, rec.report)

		require.NoError(t, s.Submit(rec.trigger(1, 1, Serial, sleep(time.Minute))))
		require.NoError(t, s.Submit(rec.trigger(1, 2, Serial, succeed)))
		synctest.Wait()
		changed := rec.trigger(1, 3, Serial, succeed)
		changed.Handler = "other"
		require.NoError(t, s.Submit(changed))
		synctest.Wait()

		assert.Equal(t, ErrHandlerChanged.Error(), rec.outcome(t, 1).Msg)
		assert.Equal(t, "trigger discarded: "+ErrHandlerChanged.Error(), rec.outcome(t, 2).Msg)
		assert.Equal(t, Succeeded, rec.outcome(t, 3).Status)
	})
}

func TestKill(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Scenario: kill a job whose running trigger ignores cancellation for 3s, with one trigger queued; then
		//           trigger it again right away; then kill an idle job
		// Expect: the running trigger is reported killed at once (not as the success it returns later), the queued one
		//         discarded; the job stays busy until the run returns, so the new trigger never overlaps it
		rec := newRecorder()
		s := New(Config{}, rec.report)

		require.NoError(t, s.Submit(rec.trigger(1, 1, Serial, stubborn(3*time.Second))))
		require.NoError(t, s.Submit(rec.trigger(1, 2, Serial, succeed)))
		synctest.Wait()
		killed := s.Kill(1)
		synctest.Wait()
		reportedAt := rec.reported(1)
		require.NoError(t, s.Submit(rec.trigger(1, 3, Serial, succeed)))
		stillBusy := s.Stats()
		time.Sleep(5 * time.Second)
		synctest.Wait()

		assert.True(t, killed)
		assert.True(t, reportedAt, "killed run reported immediately")
		assert.Equal(t, Outcome{Failed, ErrKilled.Error()}, rec.outcome(t, 1))
		assert.Equal(t, "trigger discarded: "+ErrKilled.Error(), rec.outcome(t, 2).Msg)
		assert.Equal(t, Succeeded, rec.outcome(t, 3).Status)
		assert.Equal(t, 1, stillBusy.Running)
		assert.Zero(t, rec.overlaps)
		assert.False(t, s.Kill(1))
	})
}

func TestTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Scenario: a 2s timeout; one run honours cancellation, another ignores it and returns success after 10s
		// Expect: both are reported as timeouts (502) exactly at the deadline; the stubborn one keeps its job busy
		//         until it returns
		rec := newRecorder()
		s := New(Config{}, rec.report)
		polite := rec.trigger(1, 1, Serial, sleep(time.Minute))
		polite.Timeout = 2 * time.Second
		stub := rec.trigger(2, 2, Serial, stubborn(10*time.Second))
		stub.Timeout = 2 * time.Second

		require.NoError(t, s.Submit(polite))
		require.NoError(t, s.Submit(stub))
		time.Sleep(2*time.Second - time.Millisecond)
		synctest.Wait()
		early := rec.reported(1) || rec.reported(2)
		time.Sleep(time.Millisecond)
		synctest.Wait()
		atDeadline := s.Stats()

		assert.False(t, early)
		assert.Equal(t, Outcome{TimedOut, ErrTimeout.Error()}, rec.outcome(t, 1))
		assert.Equal(t, Outcome{TimedOut, ErrTimeout.Error()}, rec.outcome(t, 2))
		assert.Equal(t, 1, atDeadline.Running, "the stubborn run still occupies its job")
		assert.Equal(t, int64(2), atDeadline.TimedOut)
		time.Sleep(10 * time.Second)
		synctest.Wait()
		assert.False(t, s.Busy(2))
	})
}

func TestMaxConcurrency(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Scenario: concurrency limit 2 with 4 jobs triggered at once; job 3 is killed while waiting for a slot
		// Expect: never more than 2 runs at once; waiting triggers count as running; the killed one never runs
		var mu sync.Mutex
		active, peak := 0, 0
		track := func(ctx context.Context) Result {
			mu.Lock()
			active++
			peak = max(peak, active)
			mu.Unlock()
			defer func() { mu.Lock(); active--; mu.Unlock() }()
			return sleep(time.Second)(ctx)
		}
		rec := newRecorder()
		s := New(Config{MaxConcurrency: 2}, rec.report)

		for job := int64(1); job <= 4; job++ {
			require.NoError(t, s.Submit(rec.trigger(job, job, Serial, track)))
		}
		synctest.Wait()
		waiting := s.Stats()
		rec.mu.Lock()
		started := append([]int64(nil), rec.order...)
		rec.mu.Unlock()
		var victim int64 // a job still waiting for a slot (which two got one first is up to the runtime)
		for job := int64(1); job <= 4; job++ {
			if !slices.Contains(started, job) {
				victim = job
				break
			}
		}
		s.Kill(victim)
		time.Sleep(5 * time.Second)
		synctest.Wait()

		assert.Len(t, started, 2)
		assert.Equal(t, 4, waiting.Running)
		assert.Equal(t, 2, peak)
		assert.Equal(t, Outcome{Failed, ErrKilled.Error()}, rec.outcome(t, victim))
		assert.NotContains(t, rec.order, victim)
		assert.Equal(t, int64(3), s.Stats().Succeeded)
	})
}

func TestRejections(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Scenario: per-job queue limit 1, at most 5 active triggers in total, and repeated log IDs (same job and
		//           another job)
		// Expect: each limit rejects with an explanation; the total counts running and queued triggers alike; a log ID
		//         is unique across jobs while in flight
		rec := newRecorder()
		s := New(Config{MaxQueue: 1, MaxActive: 5}, rec.report)
		long := sleep(time.Minute)

		require.NoError(t, s.Submit(rec.trigger(1, 1, Serial, long)))
		require.NoError(t, s.Submit(rec.trigger(1, 2, Serial, long)))
		perJob := s.Submit(rec.trigger(1, 3, Serial, long))
		require.NoError(t, s.Submit(rec.trigger(2, 4, Serial, long)))
		require.NoError(t, s.Submit(rec.trigger(2, 5, Serial, long)))
		require.NoError(t, s.Submit(rec.trigger(3, 6, Serial, long)))
		total := s.Submit(rec.trigger(4, 7, Serial, long))
		sameJob := s.Submit(rec.trigger(1, 2, Serial, long))
		st := s.Stats()
		s.Kill(1)
		synctest.Wait()
		otherJob := s.Submit(rec.trigger(9, 4, Serial, long)) // log 4 is still running in job 2
		s.Kill(2)
		s.Kill(3)
		s.Kill(9)
		synctest.Wait()

		require.ErrorIs(t, perJob, ErrQueueFull)
		require.ErrorContains(t, perJob, "1 triggers of this job waiting")
		require.ErrorIs(t, total, ErrBusy)
		require.ErrorContains(t, total, "5 triggers running or queued")
		require.ErrorIs(t, sameJob, ErrRepeated)
		require.ErrorIs(t, otherJob, ErrRepeated)
		assert.Equal(t, int64(3), st.Rejected)
		assert.Equal(t, 2, st.Queued)
		assert.Equal(t, 5, st.Active)
		assert.Zero(t, s.Stats().Active)
	})
}

func TestClose(t *testing.T) {
	cases := []struct {
		name     string
		run      func(context.Context) Result
		deadline time.Duration
		wantErr  error
		want     Outcome
	}{
		{"running trigger finishes in time", sleep(time.Second), 5 * time.Second, nil,
			Outcome{Succeeded, "done"}},
		{"running trigger is cancelled at the deadline", sleep(time.Minute), time.Second, context.DeadlineExceeded,
			Outcome{Failed, ErrShutdown.Error()}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				// Scenario: Close while one trigger runs and one is queued
				// Expect: the queued one is discarded; the running one either finishes or is cancelled at the
				//         deadline; later triggers are rejected
				rec := newRecorder()
				s := New(Config{}, rec.report)
				require.NoError(t, s.Submit(rec.trigger(1, 1, Serial, c.run)))
				require.NoError(t, s.Submit(rec.trigger(1, 2, Serial, succeed)))
				synctest.Wait()
				ctx, cancel := context.WithTimeout(t.Context(), c.deadline)
				defer cancel()

				err := s.Close(ctx, time.Second)
				late := s.Submit(rec.trigger(2, 3, Serial, succeed))

				if c.wantErr == nil {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, c.wantErr)
				}
				assert.Equal(t, c.want, rec.outcome(t, 1))
				assert.Equal(t, "trigger discarded: "+ErrShutdown.Error(), rec.outcome(t, 2).Msg)
				require.ErrorIs(t, late, ErrShutdown)
			})
		})
	}
}

func TestCloseIdleWithExpiredContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Scenario: Close an idle scheduler with a context that has already expired
		// Expect: nil, since nothing had to be cancelled
		s := New(Config{}, newRecorder().report)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		assert.NoError(t, s.Close(ctx, time.Second))
	})
}

func TestCloseRightAfterSubmit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Scenario: Close right after an idle job accepted a trigger, before its goroutine had a chance to start it
		// Expect: the trigger runs and succeeds; it is never mistaken for a queued one
		rec := newRecorder()
		s := New(Config{}, rec.report)

		require.NoError(t, s.Submit(rec.trigger(1, 1, Serial, sleep(time.Second))))
		err := s.Close(t.Context(), time.Second)

		require.NoError(t, err)
		assert.Equal(t, Succeeded, rec.outcome(t, 1).Status)
	})
}

func TestCloseDiscardsTriggersWaitingForASlot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Scenario: concurrency limit 1; job 1 runs, job 2 waits for the slot; Close is called
		// Expect: job 2 is cancelled with ErrShutdown and never starts, even though the slot frees up
		rec := newRecorder()
		s := New(Config{MaxConcurrency: 1}, rec.report)
		require.NoError(t, s.Submit(rec.trigger(1, 1, Serial, sleep(time.Second))))
		synctest.Wait() // job 1 holds the slot
		require.NoError(t, s.Submit(rec.trigger(2, 2, Serial, succeed)))
		synctest.Wait()

		err := s.Close(t.Context(), time.Second)

		require.NoError(t, err)
		assert.Equal(t, Succeeded, rec.outcome(t, 1).Status)
		assert.Equal(t, Outcome{Failed, ErrShutdown.Error()}, rec.outcome(t, 2))
		assert.Equal(t, []int64{1}, rec.order)
	})
}

func TestCountersBalance(t *testing.T) {
	// Scenario: many goroutines submit, kill and read stats concurrently with every strategy (real time, -race)
	// Expect: every accepted trigger is reported exactly once, rejected ones never; no job overlaps itself; once
	//         idle, Accepted equals Succeeded + Failed + TimedOut and every snapshot has outcomes <= accepted
	rec := newRecorder()
	s := New(Config{MaxQueue: 3, MaxConcurrency: 4}, rec.report)
	strategies := []string{Serial, DiscardLater, CoverEarly}
	runs := []func(context.Context) Result{succeed, sleep(time.Millisecond), stubborn(time.Millisecond)}
	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted := map[int64]bool{}
	for w := range 8 {
		wg.Go(func() {
			for i := range 200 {
				logID := int64(w*1000 + i)
				job := int64(i % 5)
				tr := rec.trigger(job, logID, strategies[i%3], runs[(i+w)%3])
				tr.Timeout = time.Duration(i%2) * time.Millisecond
				if s.Submit(tr) == nil {
					mu.Lock()
					accepted[logID] = true
					mu.Unlock()
				}
				if i%17 == 0 {
					s.Kill(job)
				}
				st := s.Stats()
				if st.Succeeded+st.Failed+st.TimedOut > st.Accepted {
					t.Errorf("snapshot with more outcomes than accepted triggers: %+v", st)
				}
			}
		})
	}
	wg.Wait()
	require.NoError(t, s.Close(t.Context(), time.Second))

	st := s.Stats()
	assert.Equal(t, int64(len(accepted)), st.Accepted)
	assert.Equal(t, st.Accepted, st.Succeeded+st.Failed+st.TimedOut)
	rec.mu.Lock()
	defer rec.mu.Unlock()
	for logID, outs := range rec.outcomes {
		assert.True(t, accepted[logID], "outcome for rejected log %d", logID)
		assert.Len(t, outs, 1, "log %d", logID)
	}
	assert.Len(t, rec.outcomes, len(accepted))
	assert.Zero(t, rec.overlaps)
}

func TestClassify(t *testing.T) {
	// Scenario: classify runs by cancellation cause and result
	// Expect: a cause (possibly wrapped) wins over the result; otherwise an error fails and nil succeeds with the note
	assert.Equal(t, Outcome{TimedOut, ErrTimeout.Error()}, Classify(ErrTimeout, Result{Msg: "ok"}))
	assert.Equal(t, Outcome{Failed, "x\n" + ErrCovered.Error()}, Classify(errors.Join(errors.New("x"), ErrCovered), Result{}))
	assert.Equal(t, Outcome{Failed, "bad"}, Classify(context.Canceled, Result{Failed: true, Msg: "bad"}))
	assert.Equal(t, Outcome{Succeeded, "ok"}, Classify(nil, Result{Msg: "ok"}))
	assert.False(t, IsCause(context.Canceled))
}

func TestPanickingRunFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Scenario: a Run function panics (a bug outside the handler's containment)
		// Expect: the run fails with the panic as its message and its job is free again
		rec := newRecorder()
		s := New(Config{}, rec.report)

		require.NoError(t, s.Submit(rec.trigger(1, 1, Serial, func(context.Context) Result { panic("boom") })))
		synctest.Wait()

		assert.Equal(t, Outcome{Failed, "xxljob: run panicked: boom"}, rec.outcome(t, 1))
		assert.False(t, s.Busy(1))
	})
}

func TestMaxActiveBoundsJobThreads(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Scenario: 500 distinct jobs are triggered while every run blocks, with at most 50 active triggers
		// Expect: only 50 are accepted, so only 50 job goroutines exist; once they finish new triggers are accepted
		rec := newRecorder()
		s := New(Config{MaxActive: 50}, rec.report)
		release := make(chan struct{})
		block := func(context.Context) Result { <-release; return Result{} }

		accepted := 0
		for job := int64(1); job <= 500; job++ {
			if s.Submit(rec.trigger(job, job, Serial, block)) == nil {
				accepted++
			}
		}
		synctest.Wait()
		busy := s.Stats()
		close(release)
		synctest.Wait()
		after := s.Submit(rec.trigger(1000, 1000, Serial, succeed))
		synctest.Wait()

		assert.Equal(t, 50, accepted)
		assert.Equal(t, 50, busy.Running)
		assert.Equal(t, int64(450), busy.Rejected)
		require.NoError(t, after)
		assert.Zero(t, s.Stats().Active)
	})
}

func TestIllegalPhaseChangePanics(t *testing.T) {
	// Scenario: a bug moves a finished run back to running
	// Expect: the scheduler refuses loudly instead of corrupting its bookkeeping
	s := New(Config{}, newRecorder().report)
	r := &run{tr: &Trigger{LogID: 1}, phase: finished}

	assert.PanicsWithValue(t, "sched: illegal phase change finished -> running (log_id 1)", func() { s.to(r, running) })
}

func TestRunFinishesOnlyAfterItsReportIsDelivered(t *testing.T) {
	// Scenario: a run is killed; the kill is reported from the cancellation's goroutine and delivering it is slow; the
	//           handler returns while that report is still in progress
	// Expect: the scheduler does not consider the run finished (Close does not return) until the report has been
	//         delivered, so nothing can be reported after the reporter has been shut down
	var mu sync.Mutex
	delivered := false
	reporting, unblock := make(chan struct{}), make(chan struct{})
	s := New(Config{}, func(*Trigger, Outcome) {
		close(reporting)
		<-unblock
		mu.Lock()
		delivered = true
		mu.Unlock()
	})
	started := make(chan struct{})
	require.NoError(t, s.Submit(&Trigger{JobID: 1, LogID: 1, Run: func(ctx context.Context) Result {
		close(started)
		<-ctx.Done()
		<-reporting // the kill report is being delivered by the cancellation's goroutine
		return Result{}
	}}))
	<-started
	s.Kill(1)
	closed := make(chan error)
	go func() { closed <- s.Close(context.Background(), time.Second) }()

	select {
	case <-closed:
		t.Fatal("Close returned while a report was still being delivered")
	case <-time.After(100 * time.Millisecond):
	}
	close(unblock)
	require.NoError(t, <-closed)
	mu.Lock()
	defer mu.Unlock()
	assert.True(t, delivered)
}

func TestCloseWaitsForCancellationReports(t *testing.T) {
	// Scenario: Close with an expired context and almost no grace, while a run ignores its cancellation
	// Expect: Close still returns only after the cancellation has been reported, so nothing is reported after the
	//         caller has shut its reporting down
	for i := range 50 {
		var closed, late atomic.Bool
		started, release := make(chan struct{}), make(chan struct{})
		s := New(Config{}, func(*Trigger, Outcome) {
			if closed.Load() {
				late.Store(true)
			}
		})
		require.NoError(t, s.Submit(&Trigger{JobID: 1, LogID: int64(i + 1), Run: func(context.Context) Result {
			close(started)
			<-release // ignores its context
			return Result{}
		}}))
		<-started
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		require.ErrorIs(t, s.Close(ctx, time.Nanosecond), context.Canceled)
		closed.Store(true)
		close(release)

		require.False(t, late.Load(), "run %d: reported after Close returned", i)
	}
}
