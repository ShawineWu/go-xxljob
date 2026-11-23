package xxljob

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigNormalize(t *testing.T) {
	// Scenario: configurations with missing, invalid and defaulted fields
	// Expect: required fields and invalid values are rejected with a clear error; zero values become the defaults;
	//         the token is trimmed and Address gets a trailing slash
	base := Config{AdminAddresses: []string{" http://admin:8080/xxl-job-admin/ "}, AppName: " demo ", IP: "10.0.0.8",
		AccessToken: " t "}
	cases := []struct {
		name   string
		mutate func(*Config)
		err    string
	}{
		{"no admin", func(c *Config) { c.AdminAddresses = []string{" "} }, "at least one admin address is required"},
		{"admin not http", func(c *Config) { c.AdminAddresses = []string{"ftp://x"} }, "must be an http(s) URL"},
		{"admin with credentials", func(c *Config) { c.AdminAddresses = []string{"http://u:p@admin"} }, "must not contain credentials"},
		{"admin with query", func(c *Config) { c.AdminAddresses = []string{"http://admin?x=1"} }, "must not contain credentials, a query"},
		{"admin api path", func(c *Config) { c.AdminAddresses = []string{"http://admin/api/registry"} }, "without /api/"},
		{"no app name", func(c *Config) { c.AppName = "" }, "AppName is required"},
		{"bad port", func(c *Config) { c.Port = 70000 }, "invalid port"},
		{"negative interval", func(c *Config) { c.RegistryInterval = -time.Second }, "RegistryInterval must not be negative"},
		{"negative queue", func(c *Config) { c.MaxQueueSize = -1 }, "MaxQueueSize must not be negative"},
		{"bad address", func(c *Config) { c.Address = "not a url" }, "invalid executor address"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := base
			c.mutate(&cfg)

			_, err := cfg.normalize()

			require.ErrorContains(t, err, c.err)
		})
	}

	got, err := base.normalize()

	require.NoError(t, err)
	assert.Equal(t, []string{"http://admin:8080/xxl-job-admin"}, got.AdminAddresses)
	assert.Equal(t, "demo", got.AppName)
	assert.Equal(t, "t", got.AccessToken)
	assert.Equal(t, AdminVersion2, got.AdminVersion)
	assert.Equal(t, "http://10.0.0.8:9999/", got.Address)
	assert.Equal(t, DefaultShutdownTimeout, got.ShutdownTimeout)
	assert.Equal(t, DefaultMaxActiveTriggers, got.MaxActiveTriggers)
	assert.Equal(t, DefaultLongRunningThreshold, got.LongRunningThreshold)
}

func TestPodIPFromEnv(t *testing.T) {
	// Scenario: Kubernetes injects POD_IP through the Downward API and no IP or Address is configured
	// Expect: the executor registers the pod IP
	t.Setenv(EnvPodIP, "10.1.2.3")

	got, err := Config{AdminAddresses: []string{"http://admin"}, AppName: "demo"}.normalize()

	require.NoError(t, err)
	assert.Equal(t, "http://10.1.2.3:9999/", got.Address)
}

func TestConfigReturnsACopy(t *testing.T) {
	// Scenario: a caller modifies the slice returned by Config()
	// Expect: the executor keeps its own admin addresses
	e, err := New(testConfig("http://admin"), WithLogger(quiet))
	require.NoError(t, err)

	e.Config().AdminAddresses[0] = "http://evil"

	assert.Equal(t, "http://admin", e.Config().AdminAddresses[0])
}

func TestRegister(t *testing.T) {
	// Scenario: invalid and duplicate registrations, and tasks registered in bulk
	// Expect: errors name the problem; a failed bulk registration registers nothing; a nil middleware is refused
	e, err := New(testConfig("http://admin"), WithLogger(quiet))
	require.NoError(t, err)
	task := func(name string) Task { return namedTask{name} }

	require.ErrorContains(t, e.Register("", HandlerFunc(noop)), "empty handler name")
	require.ErrorContains(t, e.Register("A", nil), `nil handler "A"`)
	require.ErrorContains(t, e.Register("A", HandlerFunc(noop), nil), "nil middleware")
	require.NoError(t, e.Register("A", HandlerFunc(noop)))
	require.ErrorContains(t, e.Register("A", HandlerFunc(noop)), `duplicate handler "A"`)
	require.ErrorContains(t, e.RegisterTasks(task("B"), task("B")), `duplicate handler "B"`)
	require.ErrorContains(t, e.RegisterTasks(task("C"), task("A")), `duplicate handler "A"`)
	require.ErrorContains(t, e.RegisterTasks(task("D"), nil), "nil task")
	require.NoError(t, e.RegisterTasks(task("E"), task("F")))
	assert.Panics(t, func() { e.Use(nil) })
	assert.Panics(t, func() { e.MustRegister("A", HandlerFunc(noop)) })
	_, errC := e.handlers.resolve("C")
	_, errF := e.handlers.resolve("F")

	require.Error(t, errC, "a failed bulk registration registers nothing")
	require.NoError(t, errF)
}

type namedTask struct{ name string }

func (t namedTask) Name() string                         { return t.name }
func (namedTask) Handle(context.Context, *Request) error { return nil }

func TestMiddleware(t *testing.T) {
	// Scenario: global middlewares (option and Use) and a handler's own; one middleware constructor calls back into
	//           the executor; another handler's middleware panics while being built
	// Expect: they run outermost first (option, Use, own); calling the executor from a constructor does not deadlock;
	//         a panicking constructor fails that trigger instead of the process
	var order []string
	mw := func(name string) Middleware {
		return func(next Handler) Handler {
			return HandlerFunc(func(ctx context.Context, req *Request) error {
				order = append(order, name)
				return next.Handle(ctx, req)
			})
		}
	}
	admin := newFakeAdmin(t)
	var e *Executor
	reentrant := func(next Handler) Handler {
		_ = e.Stats() // calls back into the executor while building the chain
		return next
	}
	h := startHarness(t, admin, testConfig(admin.srv.URL), nil, WithLogger(quiet), WithMiddleware(mw("option"), reentrant))
	e = h.e
	e.Use(mw("use"))
	require.NoError(t, e.Register("T", HandlerFunc(noop), mw("own")))
	require.NoError(t, e.Register("Broken", HandlerFunc(noop), func(Handler) Handler { panic("bad middleware") }))

	ok := h.run(t, trigger{JobID: 1, LogID: 1, ExecutorHandler: "T"})
	h.admin.callback(t, 1)
	broken := h.run(t, trigger{JobID: 2, LogID: 2, ExecutorHandler: "Broken"})

	assert.Equal(t, CodeSuccess, ok.Code)
	assert.Equal(t, []string{"option", "use", "own"}, order)
	assert.Equal(t, CodeFailure, broken.Code)
	assert.Contains(t, broken.Msg, `middleware for handler "Broken" panicked: bad middleware`)
}

func TestHandlerCannotRedirectItsResult(t *testing.T) {
	// Scenario: a handler changes the LogID and Handler of the request it received
	// Expect: the result is still reported for the original trigger
	h := newHarness(t, map[string]Handler{"T": HandlerFunc(func(_ context.Context, req *Request) error {
		req.LogID, req.Handler = 999, "Other"
		return nil
	})})

	h.run(t, trigger{JobID: 1, LogID: 7, ExecutorHandler: "T"})

	assert.Equal(t, CodeSuccess, h.admin.callback(t, 7).Code)
	assert.NotContains(t, h.admin.callbackCount(), int64(999))
}

func TestStartErrors(t *testing.T) {
	// Scenario: the executor port is taken; Start is called twice
	// Expect: Start fails before registering anything; a second Start is refused
	busy, err := net.Listen("tcp", ":0") // all interfaces, like the executor
	require.NoError(t, err)
	defer func() { _ = busy.Close() }()
	admin := newFakeAdmin(t)
	cfg := testConfig(admin.srv.URL)
	cfg.Port = busy.Addr().(*net.TCPAddr).Port
	taken, err := New(cfg, WithLogger(quiet))
	require.NoError(t, err)
	h := newHarness(t, nil)

	errTaken := taken.Start()
	errTwice := h.e.Start()

	require.ErrorContains(t, errTaken, fmt.Sprintf("listen executor port %d", cfg.Port))
	require.ErrorContains(t, errTwice, "already started")
	assert.Zero(t, admin.registry.Load())
}

func TestRun(t *testing.T) {
	// Scenario: Run drives the executor; it ends because its context ends, because Shutdown is called elsewhere, or
	//           with a job still running past ShutdownTimeout
	// Expect: a normal stop returns nil after the running job finished and was reported, and deregisters; an external
	//         Shutdown makes Run return too; a job outliving ShutdownTimeout is cancelled and Run reports the deadline
	cases := []struct {
		name    string
		stop    func(cancel context.CancelFunc, e *Executor)
		job     time.Duration
		timeout time.Duration
		wantErr error
		code    int
	}{
		{"context ends", func(cancel context.CancelFunc, _ *Executor) { cancel() }, 200 * time.Millisecond, 0, nil, CodeSuccess},
		{"Shutdown elsewhere", func(_ context.CancelFunc, e *Executor) { go func() { _ = e.Shutdown(context.Background()) }() },
			200 * time.Millisecond, 0, nil, CodeSuccess},
		{"job outlives ShutdownTimeout", func(cancel context.CancelFunc, _ *Executor) { cancel() }, time.Minute,
			100 * time.Millisecond, context.DeadlineExceeded, CodeFailure},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			admin := newFakeAdmin(t)
			cfg := testConfig(admin.srv.URL)
			cfg.ShutdownTimeout = c.timeout
			e, err := New(cfg, WithoutServer(), WithLogger(quiet))
			require.NoError(t, err)
			started := make(chan struct{})
			require.NoError(t, e.Register("T", HandlerFunc(func(ctx context.Context, _ *Request) error {
				close(started)
				select {
				case <-time.After(c.job):
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})))
			srv := httptest.NewServer(e.Handler())
			defer srv.Close()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- e.Run(ctx) }()
			require.Eventually(t, func() bool { return e.Stats().Registered }, 2*time.Second, 5*time.Millisecond)
			h := &harness{e: e, admin: admin, srv: srv}
			h.run(t, trigger{JobID: 1, LogID: 1, ExecutorHandler: "T"})
			<-started

			c.stop(cancel, e)
			var runErr error
			select {
			case runErr = <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("Run did not return")
			}

			if c.wantErr == nil {
				require.NoError(t, runErr)
			} else {
				require.ErrorIs(t, runErr, c.wantErr)
			}
			assert.Equal(t, c.code, admin.callback(t, 1).Code)
			assert.Positive(t, admin.removed.Load())
			assert.False(t, e.Stats().Registered)
		})
	}
}

func TestShutdownIsBounded(t *testing.T) {
	// Scenario: the admin is unreachable and a result is pending; Shutdown is called with a context that has no
	//           deadline (no job is running)
	// Expect: Shutdown returns within its bounded steps instead of retrying forever; the result is logged as lost
	logs, logger := capture()
	admin := newFakeAdmin(t)
	cfg := testConfig(admin.srv.URL)
	cfg.RequestTimeout = 200 * time.Millisecond
	cfg.CancelWait = 300 * time.Millisecond
	h := startHarness(t, admin, cfg, map[string]Handler{"T": HandlerFunc(noop)}, WithLogger(logger))
	require.Eventually(t, func() bool { return h.e.Stats().Registered }, time.Second, 5*time.Millisecond)
	admin.down.Store(true)
	h.run(t, trigger{JobID: 1, LogID: 1, ExecutorHandler: "T"})
	require.Eventually(t, func() bool { return h.e.Stats().PendingCallbacks == 1 }, time.Second, 5*time.Millisecond)

	start := time.Now()
	err := h.e.Shutdown(context.Background())

	require.NoError(t, err)
	assert.Less(t, time.Since(start), 2*time.Second) // RequestTimeout + CancelWait + port, with slack
	assert.Contains(t, logs.String(), "results not delivered before shutdown, results lost")
}

func TestShutdownWithExpiredContextWhenIdle(t *testing.T) {
	// Scenario: Shutdown with a context that has already expired, while nothing runs
	// Expect: nil, without claiming that jobs were cancelled
	logs, logger := capture()
	admin := newFakeAdmin(t)
	h := startHarness(t, admin, testConfig(admin.srv.URL), nil, WithLogger(logger))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := h.e.Shutdown(ctx)

	require.NoError(t, err)
	assert.NotContains(t, logs.String(), "were cancelled")
}

func TestShutdownRightAfterTrigger(t *testing.T) {
	// Scenario: Shutdown immediately after the executor accepted a trigger for an idle job
	// Expect: the trigger runs and succeeds; it is never mistaken for a queued one
	h := newHarness(t, map[string]Handler{"T": HandlerFunc(func(context.Context, *Request) error {
		time.Sleep(20 * time.Millisecond)
		return nil
	})})

	h.run(t, trigger{JobID: 1, LogID: 1, ExecutorHandler: "T"})
	err := h.e.Shutdown(t.Context())

	require.NoError(t, err)
	assert.Equal(t, CodeSuccess, h.admin.callback(t, 1).Code)
}

func TestCallbackDirSurvivesRestart(t *testing.T) {
	// Scenario: with CallbackDir, the admin is down when the executor shuts down with an undelivered result; a new
	//           executor starts with the same directory once the admin is back
	// Expect: the result is saved, then delivered by the new executor
	dir := t.TempDir()
	admin := newFakeAdmin(t)
	cfg := testConfig(admin.srv.URL)
	cfg.CallbackDir = dir
	cfg.CancelWait = 100 * time.Millisecond
	first := startHarness(t, admin, cfg, map[string]Handler{"T": HandlerFunc(noop)}, WithLogger(quiet))
	admin.down.Store(true)
	first.run(t, trigger{JobID: 1, LogID: 1, ExecutorHandler: "T"})
	require.Eventually(t, func() bool { return first.e.Stats().PendingCallbacks == 1 }, time.Second, 5*time.Millisecond)
	require.NoError(t, first.e.Shutdown(t.Context()))
	saved, _ := filepath.Glob(filepath.Join(dir, "*.jsonl"))

	admin.down.Store(false)
	startHarness(t, admin, cfg, nil, WithLogger(quiet))

	assert.Len(t, saved, 1)
	assert.Equal(t, CodeSuccess, admin.callback(t, 1).Code)
}

func TestStats(t *testing.T) {
	// Scenario: monitoring reads the executor's stats through a mix of outcomes and rejections
	// Expect: gauges reflect running and queued triggers; counters count every trigger exactly once, including
	//         those rejected before reaching the scheduler; Registered follows the heartbeat
	g := newGate()
	h := newHarness(t, map[string]Handler{"G": g, "Fail": HandlerFunc(func(context.Context, *Request) error { return errors.New("x") })})
	require.Eventually(t, func() bool { return h.e.Stats().Registered }, time.Second, 5*time.Millisecond)

	h.run(t, trigger{JobID: 1, LogID: 1, ExecutorHandler: "G"})
	g.waitStart(t)
	h.run(t, trigger{JobID: 1, LogID: 2, ExecutorHandler: "G"})
	h.run(t, trigger{JobID: 2, LogID: 3, ExecutorHandler: "Fail"})
	h.run(t, trigger{JobID: 3, LogID: 4, ExecutorHandler: "Missing"})
	h.admin.callback(t, 3)
	busy := h.e.Stats()
	close(g.release)
	h.admin.callback(t, 2)
	require.Eventually(t, func() bool { st := h.e.Stats(); return st.Running == 0 && st.PendingCallbacks == 0 }, time.Second, 5*time.Millisecond)
	idle := h.e.Stats()

	assert.Equal(t, 1, busy.Running)
	assert.Equal(t, 1, busy.Queued)
	assert.Equal(t, Stats{Registered: true, Accepted: 3, Rejected: 1, Succeeded: 2, Failed: 1}, idle)
}

func TestManyJobsAreBounded(t *testing.T) {
	// Scenario: 300 distinct jobs are triggered while every run blocks, with MaxActiveTriggers 20
	// Expect: only 20 are accepted (and so only 20 job goroutines exist); the rest are refused as busy
	g := newGate()
	h := newHarness(t, map[string]Handler{"G": g}, func(c *Config) { c.MaxActiveTriggers = 20 })

	var accepted atomic.Int32
	var busyMsg string
	for job := int64(1); job <= 300; job++ {
		r := h.run(t, trigger{JobID: job, LogID: job, ExecutorHandler: "G"})
		if r.Code == CodeSuccess {
			accepted.Add(1)
		} else {
			busyMsg = r.Msg
		}
	}
	close(g.release)

	assert.Equal(t, int32(20), accepted.Load())
	assert.Contains(t, busyMsg, "executor is busy (20 triggers running or queued)")
}

func TestFileLogStoreClosesFiles(t *testing.T) {
	// Scenario: many runs with a FileLogStore, each with a goroutine that keeps logging after the run returned
	// Expect: late lines are dropped instead of reopening the log, so no file stays open after the runs ended
	dir := t.TempDir()
	store, err := NewFileLogStore(dir, 0)
	require.NoError(t, err)
	admin := newFakeAdmin(t)
	release := make(chan struct{})
	h := startHarness(t, admin, testConfig(admin.srv.URL), map[string]Handler{"T": HandlerFunc(func(ctx context.Context, _ *Request) error {
		go func() {
			<-release
			Logger(ctx).Info("late line")
		}()
		return nil
	})}, WithLogger(quiet), WithLogStore(store))

	for i := int64(1); i <= 20; i++ {
		h.run(t, trigger{JobID: i, LogID: i, ExecutorHandler: "T"})
		h.admin.callback(t, i)
	}
	time.Sleep(50 * time.Millisecond) // every run has ended its log
	close(release)
	time.Sleep(50 * time.Millisecond)
	store.mu.Lock()
	open := len(store.open)
	store.mu.Unlock()
	data, err := os.ReadFile(filepath.Join(dir, time.Now().Format(time.DateOnly), "1.log"))
	require.NoError(t, err)

	assert.Zero(t, open)
	assert.NotContains(t, string(data), "late line")
	assert.True(t, strings.HasSuffix(strings.TrimSpace(string(data)), `result=""`) || strings.Contains(string(data), "job succeeded"))
}
