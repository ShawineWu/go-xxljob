package xxljob

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testToken = "secret-token"

// fakeAdmin simulates the admin. It records registrations, deregistrations, callbacks and received tokens; the next
// failCallbacks callbacks return a failure.
type fakeAdmin struct {
	srv           *httptest.Server
	registry      atomic.Int32
	removed       atomic.Int32
	failCallbacks atomic.Int32
	callbackReqs  atomic.Int32

	mu        sync.Mutex
	callbacks map[int64]result
	counts    map[int64]int // callbacks received per log_id (to detect duplicates)
	order     []int64
	tokens    map[string]bool
	appNames  map[string]bool
	bodies    map[string]bool // callback request body format: "array" (2.x) / "callbackList" (3.5+)
}

func newFakeAdmin(t testing.TB) *fakeAdmin {
	a := &fakeAdmin{callbacks: map[int64]result{}, counts: map[int64]int{}, tokens: map[string]bool{}, appNames: map[string]bool{}, bodies: map[string]bool{}}
	a.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a.mu.Lock()
		a.tokens[r.Header.Get(tokenHeader)] = true
		a.appNames[r.Header.Get(appNameHeader)] = true
		a.mu.Unlock()
		res := map[string]any{"code": codeSuccess}
		switch r.URL.Path {
		case "/api/registry":
			a.registry.Add(1)
		case "/api/registryRemove":
			a.removed.Add(1)
		case "/api/callback":
			a.callbackReqs.Add(1)
			if a.failCallbacks.Add(-1) >= 0 {
				res = map[string]any{"code": codeFail, "msg": "db busy"}
				break
			}
			ps, form := decodeCallback(r.Body)
			a.mu.Lock()
			a.bodies[form] = true
			for _, p := range ps {
				a.callbacks[p.LogID] = p
				a.counts[p.LogID]++
				a.order = append(a.order, p.LogID)
			}
			a.mu.Unlock()
		default:
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(res)
	}))
	t.Cleanup(a.srv.Close)
	return a
}

// decodeCallback parses a callback request body in either admin protocol generation: 2.x sends an array, 3.5+ sends
// {"callbackList": [...]} (the time field name differs too).
func decodeCallback(r io.Reader) ([]result, string) {
	raw, _ := io.ReadAll(r)
	var v2 []struct {
		LogID      int64  `json:"logId"`
		LogDateTim int64  `json:"logDateTim"`
		HandleCode int    `json:"handleCode"`
		HandleMsg  string `json:"handleMsg"`
	}
	if json.Unmarshal(raw, &v2) == nil {
		out := make([]result, len(v2))
		for i, p := range v2 {
			out[i] = result{LogID: p.LogID, LogTime: p.LogDateTim, Code: p.HandleCode, Msg: p.HandleMsg}
		}
		return out, "array"
	}
	var v35 struct {
		CallbackList []struct {
			LogID       int64  `json:"logId"`
			LogDateTime int64  `json:"logDateTime"`
			HandleCode  int    `json:"handleCode"`
			HandleMsg   string `json:"handleMsg"`
		} `json:"callbackList"`
	}
	_ = json.Unmarshal(raw, &v35)
	out := make([]result, len(v35.CallbackList))
	for i, p := range v35.CallbackList {
		out[i] = result{LogID: p.LogID, LogTime: p.LogDateTime, Code: p.HandleCode, Msg: p.HandleMsg}
	}
	return out, "callbackList"
}

func (a *fakeAdmin) callback(t *testing.T, logID int64) result {
	t.Helper()
	var p result
	require.Eventually(t, func() bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		var ok bool
		p, ok = a.callbacks[logID]
		return ok
	}, 5*time.Second, 5*time.Millisecond, "no callback for log_id %d", logID)
	return p
}

func (a *fakeAdmin) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.order)
}

type harness struct {
	e       *Executor
	admin   *fakeAdmin
	srv     *httptest.Server
	appName string // when non-empty, requests carry XXL-JOB-APPNAME (simulating a 3.5+ admin)
}

func testConfig(adminURL string) Config {
	return Config{
		AdminAddresses:        []string{adminURL},
		AccessToken:           testToken,
		AppName:               "demo",
		Address:               "http://127.0.0.1:9999/",
		RegistryInterval:      20 * time.Millisecond,
		RequestTimeout:        time.Second,
		CallbackRetryInterval: 50 * time.Millisecond,
		CancelWait:            time.Second,
	}
}

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// newHarness mounts the executor on an httptest server (WithoutServer), with fakeAdmin as the admin.
func newHarness(t testing.TB, handlers map[string]Handler, mutate ...func(*Config)) *harness {
	t.Helper()
	admin := newFakeAdmin(t)
	cfg := testConfig(admin.srv.URL)
	for _, m := range mutate {
		m(&cfg)
	}
	e, err := New(cfg, WithoutServer(), WithLogger(quiet))
	require.NoError(t, err)
	for name, h := range handlers {
		require.NoError(t, e.Register(name, h))
	}
	require.NoError(t, e.Start())
	srv := httptest.NewServer(e.Handler())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		_ = e.Shutdown(ctx)
		srv.Close()
	})
	return &harness{e: e, admin: admin, srv: srv}
}

// post calls the executor as the admin and returns the decoded reply (with the log payload in payload).
// The 2.x protocol must put the log payload in both content and data (covering 3.3 - 3.4); 3.5 puts it only in data.
func (h *harness) post(t testing.TB, path, token string, body any) reply {
	t.Helper()
	data, err := json.Marshal(body)
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, h.srv.URL+path, bytes.NewReader(data))
	require.NoError(t, err)
	req.Header.Set(tokenHeader, token)
	if h.appName != "" {
		req.Header.Set(appNameHeader, h.appName)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	var res struct {
		Code    int             `json:"code"`
		Msg     string          `json:"msg"`
		Content json.RawMessage `json:"content"`
		Data    json.RawMessage `json:"data"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&res))
	out := reply{code: res.Code, msg: res.Msg}
	if len(res.Data) > 0 {
		var lr logResult
		require.NoError(t, json.Unmarshal(res.Data, &lr))
		out.payload = lr
		if h.e.cfg.AdminVersion == AdminVersion2 {
			require.JSONEq(t, string(res.Data), string(res.Content), "2.x protocol must put the log payload in both content and data")
		} else {
			require.Empty(t, res.Content)
		}
	}
	return out
}

func (h *harness) run(t testing.TB, req runReq) reply {
	t.Helper()
	if req.LogDateTime == 0 {
		req.LogDateTime = time.Now().UnixMilli()
	}
	return h.post(t, h.e.proto.runPath(), testToken, req)
}

func noop(context.Context, *Request) error { return nil }

// gate sends the log_id on started when a job starts, then waits for release or ctx cancellation; once ctx is
// canceled it waits exitDelay before returning.
type gate struct {
	started   chan int64
	release   chan struct{}
	exitDelay time.Duration
	active    atomic.Int32 // current concurrency
	maxActive atomic.Int32
}

func newGate() *gate {
	return &gate{started: make(chan int64, 64), release: make(chan struct{})}
}

func (g *gate) Handle(ctx context.Context, req *Request) error {
	n := g.active.Add(1)
	defer g.active.Add(-1)
	for {
		m := g.maxActive.Load()
		if n <= m || g.maxActive.CompareAndSwap(m, n) {
			break
		}
	}
	g.started <- req.LogID
	select {
	case <-g.release:
		return nil
	case <-ctx.Done():
		time.Sleep(g.exitDelay)
		return ctx.Err()
	}
}

func (g *gate) waitStart(t *testing.T) int64 {
	t.Helper()
	select {
	case id := <-g.started:
		return id
	case <-time.After(5 * time.Second):
		t.Fatal("job did not start")
		return 0
	}
}

// ---------------------------------------------------------------------------------------------
// Protocol and results
// ---------------------------------------------------------------------------------------------

func TestRunReportsResult(t *testing.T) {
	// Scenario: the admin triggers jobs whose handler succeeds (with a result message) / returns an error / panics
	// Given: three handlers; trigger parameters include params, timeout, shard, and log time
	// Expect: /run replies success immediately; callbacks report per log_id: success 200 + the SetResult message,
	//         error 500 + the error message, panic 500 "panic: ..."; the handler receives the full Request; callbacks
	//         carry the token and echo logDateTim unchanged
	var got *Request
	h := newHarness(t, map[string]Handler{
		"OK": HandlerFunc(func(ctx context.Context, req *Request) error {
			got = req
			SetResult(ctx, "processed 3 rows")
			return nil
		}),
		"Err":   HandlerFunc(func(context.Context, *Request) error { return errors.New("boom") }),
		"Panic": HandlerFunc(func(context.Context, *Request) error { panic("nil map") }),
	})
	logTime := time.UnixMilli(1_700_000_000_123)

	res := h.run(t, runReq{JobID: 1, LogID: 11, LogDateTime: logTime.UnixMilli(), ExecutorHandler: "OK", ExecutorParams: `{"a":1}`,
		ExecutorBlockStrategy: BlockSerial, ExecutorTimeout: 60, BroadcastIndex: 1, BroadcastTotal: 3})
	h.run(t, runReq{JobID: 2, LogID: 12, ExecutorHandler: "Err"})
	h.run(t, runReq{JobID: 3, LogID: 13, ExecutorHandler: "Panic"})

	assert.Equal(t, success(""), res)
	assert.Equal(t, result{LogID: 11, LogTime: logTime.UnixMilli(), Code: 200, Msg: "processed 3 rows"}, h.admin.callback(t, 11))
	errCb := h.admin.callback(t, 12)
	assert.Equal(t, 500, errCb.Code)
	assert.Equal(t, "boom", errCb.Msg)
	panicCb := h.admin.callback(t, 13)
	assert.Equal(t, 500, panicCb.Code)
	assert.Equal(t, "panic: nil map", panicCb.Msg)
	assert.Equal(t, &Request{JobID: 1, LogID: 11, LogTime: logTime, Handler: "OK", Params: `{"a":1}`, BlockStrategy: BlockSerial,
		Timeout: time.Minute, ShardIndex: 1, ShardTotal: 3}, got)
	assert.Equal(t, map[string]bool{testToken: true}, h.admin.tokens)
}

func TestAuthentication(t *testing.T) {
	// Scenario: requests with no token, a wrong token, or a non-POST method hit the executor port; an executor with no
	//           token configured
	// Given: an executor configured with AccessToken; another executor without AccessToken
	// Expect: the former rejects all of them ("The access token is wrong." / 405) and the handler never runs; the
	//         latter runs jobs without a token
	var ran atomic.Bool
	handlers := map[string]Handler{"T": HandlerFunc(func(context.Context, *Request) error { ran.Store(true); return nil })}
	h := newHarness(t, handlers)

	noToken := h.post(t, "/run", "", runReq{JobID: 1, LogID: 1, ExecutorHandler: "T"})
	wrong := h.post(t, "/run", "guess", runReq{JobID: 1, LogID: 1, ExecutorHandler: "T"})
	kill := h.post(t, "/kill", "", killReq{JobID: 1})
	resp, err := http.Get(h.srv.URL + "/beat") //nolint:noctx // test
	require.NoError(t, err)
	_ = resp.Body.Close()
	time.Sleep(50 * time.Millisecond)

	assert.Equal(t, failure("The access token is wrong."), noToken)
	assert.Equal(t, failure("The access token is wrong."), wrong)
	assert.Equal(t, failure("The access token is wrong."), kill)
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	assert.False(t, ran.Load())

	open := newHarness(t, handlers, func(c *Config) { c.AccessToken = "" })
	assert.Equal(t, success(""), open.post(t, "/run", "", runReq{JobID: 1, LogID: 2, ExecutorHandler: "T"}))
	assert.Equal(t, 200, open.admin.callback(t, 2).Code)
}

func TestRunValidation(t *testing.T) {
	// Scenario: invalid trigger requests
	// Given: only T is registered; T is running log 1
	// Expect: an unknown handler, GLUE mode, and a repeated log_id each fail without running
	g := newGate()
	h := newHarness(t, map[string]Handler{"T": g})
	h.run(t, runReq{JobID: 1, LogID: 1, ExecutorHandler: "T"})
	g.waitStart(t)

	assert.Equal(t, failure("job handler [Missing] not found."), h.run(t, runReq{JobID: 2, LogID: 2, ExecutorHandler: "Missing"}))
	assert.Equal(t, failure("glue type [GLUE_SHELL] is not supported, only BEAN"),
		h.run(t, runReq{JobID: 3, LogID: 3, ExecutorHandler: "T", GlueType: "GLUE_SHELL"}))
	assert.Equal(t, failure("repeated trigger: log_id 1 is already running or queued"), h.run(t, runReq{JobID: 1, LogID: 1, ExecutorHandler: "T"}))
	close(g.release)
}

func TestNotStarted(t *testing.T) {
	// Scenario: the executor receives a trigger before Start
	// Given: New has been called but Start has not
	// Expect: /run fails with "executor is not started"
	admin := newFakeAdmin(t)
	e, err := New(testConfig(admin.srv.URL), WithoutServer(), WithLogger(quiet))
	require.NoError(t, err)
	require.NoError(t, e.Register("T", HandlerFunc(noop)))

	assert.Equal(t, failure("executor is not started"), e.dispatch(runReq{JobID: 1, LogID: 1, ExecutorHandler: "T"}))
}

// ---------------------------------------------------------------------------------------------
// Block strategies, kill, timeout
// ---------------------------------------------------------------------------------------------

func TestSerialExecutionQueues(t *testing.T) {
	// Scenario: serial execution; two more triggers arrive while the job is running; queue limit 2
	// Given: log 1 is running; logs 2 and 3 are triggered in turn, then log 4
	// Expect: 2 and 3 are queued (idle beat reports busy) and log 4 fails because the queue is full; after release they
	//         run in order 1 → 2 → 3, never more than one at a time
	g := newGate()
	h := newHarness(t, map[string]Handler{"T": g}, func(c *Config) { c.MaxQueueSize = 2 })
	h.run(t, runReq{JobID: 1, LogID: 1, ExecutorHandler: "T"})
	g.waitStart(t)

	r2 := h.run(t, runReq{JobID: 1, LogID: 2, ExecutorHandler: "T"})
	r3 := h.run(t, runReq{JobID: 1, LogID: 3, ExecutorHandler: "T", ExecutorBlockStrategy: BlockSerial})
	r4 := h.run(t, runReq{JobID: 1, LogID: 4, ExecutorHandler: "T"})
	busy := h.post(t, "/idleBeat", testToken, idleBeatReq{JobID: 1})
	idle := h.post(t, "/idleBeat", testToken, idleBeatReq{JobID: 99})
	close(g.release)
	order := []int64{g.waitStart(t), g.waitStart(t)}
	for _, id := range []int64{1, 2, 3} {
		assert.Equal(t, 200, h.admin.callback(t, id).Code)
	}

	assert.Equal(t, success(""), r2)
	assert.Equal(t, success(""), r3)
	assert.Equal(t, failure("job queue is full (2 triggers waiting)"), r4)
	assert.Equal(t, failure("job thread is running or has trigger queue."), busy)
	assert.Equal(t, success(""), idle)
	assert.Equal(t, []int64{2, 3}, order)
	assert.Equal(t, int32(1), g.maxActive.Load())
}

func TestDiscardLater(t *testing.T) {
	// Scenario: DISCARD_LATER block strategy; another trigger arrives while the job is running
	// Given: log 1 is running
	// Expect: log 2 fails immediately; log 1 is unaffected
	g := newGate()
	h := newHarness(t, map[string]Handler{"T": g})
	h.run(t, runReq{JobID: 1, LogID: 1, ExecutorHandler: "T"})
	g.waitStart(t)

	res := h.run(t, runReq{JobID: 1, LogID: 2, ExecutorHandler: "T", ExecutorBlockStrategy: BlockDiscardLater})
	close(g.release)

	assert.Equal(t, failure("block strategy DISCARD_LATER: job is still running"), res)
	assert.Equal(t, 200, h.admin.callback(t, 1).Code)
}

func TestCoverEarly(t *testing.T) {
	// Scenario: COVER_EARLY block strategy; the job is running and has a queued trigger
	// Given: log 1 is running (returns 100ms after ctx is canceled), log 2 is queued; log 3 is triggered with
	//        COVER_EARLY
	// Expect: log 1 is canceled with ErrCovered and reports failure; log 2 is discarded and reports failure; log 3
	//         starts only after log 1 returns, never concurrently
	g := newGate()
	g.exitDelay = 100 * time.Millisecond
	h := newHarness(t, map[string]Handler{"T": g})
	h.run(t, runReq{JobID: 1, LogID: 1, ExecutorHandler: "T"})
	g.waitStart(t)
	h.run(t, runReq{JobID: 1, LogID: 2, ExecutorHandler: "T"})

	res := h.run(t, runReq{JobID: 1, LogID: 3, ExecutorHandler: "T", ExecutorBlockStrategy: BlockCoverEarly})
	third := g.waitStart(t)
	close(g.release)

	assert.Equal(t, success(""), res)
	assert.Equal(t, int64(3), third)
	assert.Equal(t, result{LogID: 1, LogTime: h.admin.callback(t, 1).LogTime, Code: 500,
		Msg: "xxljob: job covered by a newer trigger: context canceled"}, h.admin.callback(t, 1))
	assert.Equal(t, "trigger discarded: xxljob: job covered by a newer trigger", h.admin.callback(t, 2).Msg)
	assert.Equal(t, 200, h.admin.callback(t, 3).Code)
	assert.Equal(t, int32(1), g.maxActive.Load())
}

func TestKill(t *testing.T) {
	// Scenario: the admin kills a job that is slow to clean up, then triggers it again right away
	// Given: log 1 is running (returns 150ms after ctx is canceled), log 2 is queued
	// Expect: /kill succeeds; log 1 is canceled with ErrKilled and log 2 is discarded, both reporting failure; the
	//         job stays busy until log 1 returns, so the new trigger log 3 queues and starts only after log 1 returns
	//         (no overlap); /kill with nothing running replies that the job is already killed
	g := newGate()
	g.exitDelay = 150 * time.Millisecond
	h := newHarness(t, map[string]Handler{"T": g})
	h.run(t, runReq{JobID: 7, LogID: 1, ExecutorHandler: "T"})
	g.waitStart(t)
	h.run(t, runReq{JobID: 7, LogID: 2, ExecutorHandler: "T"})

	kill := h.post(t, "/kill", testToken, killReq{JobID: 7})
	busy := h.post(t, "/idleBeat", testToken, idleBeatReq{JobID: 7})
	again := h.run(t, runReq{JobID: 7, LogID: 3, ExecutorHandler: "T"})
	third := g.waitStart(t)
	close(g.release)
	h.admin.callback(t, 3)
	notRunning := h.post(t, "/kill", testToken, killReq{JobID: 8})

	assert.Equal(t, success(""), kill)
	assert.Equal(t, failure("job thread is running or has trigger queue."), busy)
	assert.Equal(t, success(""), again)
	assert.Equal(t, int64(3), third)
	assert.Equal(t, "xxljob: job killed by the scheduling center: context canceled", h.admin.callback(t, 1).Msg)
	assert.Equal(t, "trigger discarded: xxljob: job killed by the scheduling center", h.admin.callback(t, 2).Msg)
	assert.Equal(t, int32(1), g.maxActive.Load())
	assert.Equal(t, success("job thread already killed."), notRunning)
}

func TestHandlerChanged(t *testing.T) {
	// Scenario: the admin switches a running job to a different JobHandler and triggers it again
	// Given: job 1 is running log 1 with A
	// Expect: log 1 is canceled with ErrHandlerChanged and reports failure; log 2 runs successfully with B
	g := newGate()
	h := newHarness(t, map[string]Handler{"A": g, "B": HandlerFunc(noop)})
	h.run(t, runReq{JobID: 1, LogID: 1, ExecutorHandler: "A"})
	g.waitStart(t)

	h.run(t, runReq{JobID: 1, LogID: 2, ExecutorHandler: "B"})

	assert.Equal(t, "xxljob: job handler changed: context canceled", h.admin.callback(t, 1).Msg)
	assert.Equal(t, 200, h.admin.callback(t, 2).Code)
}

func TestTimeout(t *testing.T) {
	// Scenario: execution exceeds the timeout set by the admin
	// Given: executorTimeout=1 (second); the job waits until ctx is canceled
	// Expect: after about 1 second ctx is canceled with ErrTimeout and 502 is reported (the admin shows a timeout)
	g := newGate()
	h := newHarness(t, map[string]Handler{"T": g})

	h.run(t, runReq{JobID: 1, LogID: 1, ExecutorHandler: "T", ExecutorTimeout: 1})
	cb := h.admin.callback(t, 1)

	assert.Equal(t, 502, cb.Code)
	assert.Equal(t, "xxljob: job execution timeout: context deadline exceeded", cb.Msg)
}

// ---------------------------------------------------------------------------------------------
// Callbacks, registration, admin failover
// ---------------------------------------------------------------------------------------------

func TestCallbackRetry(t *testing.T) {
	// Scenario: the admin fails two callbacks in a row
	// Given: fakeAdmin fails the first two callbacks; retry interval 50ms
	// Expect: the callback is resent at the retry interval and the result is eventually delivered, not lost
	h := newHarness(t, map[string]Handler{"T": HandlerFunc(noop)})
	h.admin.failCallbacks.Store(2)

	h.run(t, runReq{JobID: 1, LogID: 1, ExecutorHandler: "T"})

	assert.Equal(t, 200, h.admin.callback(t, 1).Code)
	assert.GreaterOrEqual(t, h.admin.callbackReqs.Load(), int32(3))
}

func TestAdminFailover(t *testing.T) {
	// Scenario: two admin addresses are configured and the first is unavailable
	// Given: the first address points to a closed server
	// Expect: both registration and callbacks go to the second address
	down := httptest.NewServer(http.NotFoundHandler())
	down.Close()
	g := newGate()
	h := newHarness(t, map[string]Handler{"T": g}, func(c *Config) {
		c.AdminAddresses = append([]string{down.URL}, c.AdminAddresses...)
	})

	h.run(t, runReq{JobID: 1, LogID: 1, ExecutorHandler: "T"})
	close(g.release)

	assert.Equal(t, 200, h.admin.callback(t, 1).Code)
	assert.Positive(t, h.admin.registry.Load())
}

func TestRegistryHeartbeat(t *testing.T) {
	// Scenario: the executor keeps renewing its registration while running
	// Given: heartbeat interval 20ms
	// Expect: registry requests keep being sent, with the configured executor address as the registry value
	h := newHarness(t, nil)

	require.Eventually(t, func() bool { return h.admin.registry.Load() >= 3 }, 2*time.Second, 10*time.Millisecond)
	assert.Equal(t, registryParam{RegistryGroup: "EXECUTOR", RegistryKey: "demo", RegistryValue: "http://127.0.0.1:9999/"}, h.e.registryParam())
}

// ---------------------------------------------------------------------------------------------
// Shutdown
// ---------------------------------------------------------------------------------------------

func TestShutdownCancelsSlowJobs(t *testing.T) {
	// Scenario: shutdown with running and queued triggers, where the running one does not finish before the deadline
	// Given: log 1 is running (returns only after ctx is canceled), log 2 is queued; Shutdown deadline 100ms
	// Expect: the heartbeat stops and the executor deregisters; during shutdown both the liveness check and new
	//         triggers fail; log 2 is discarded; at the deadline log 1 is canceled with ErrShutdown; both results are
	//         delivered before Shutdown returns; Shutdown returns a deadline error; no heartbeats are sent afterwards
	g := newGate()
	h := newHarness(t, map[string]Handler{"T": g})
	h.run(t, runReq{JobID: 1, LogID: 1, ExecutorHandler: "T"})
	g.waitStart(t)
	h.run(t, runReq{JobID: 1, LogID: 2, ExecutorHandler: "T"})

	errc := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		errc <- h.e.Shutdown(ctx)
	}()
	require.Eventually(t, func() bool { return h.admin.removed.Load() == 1 }, time.Second, 5*time.Millisecond)
	beat := h.post(t, "/beat", testToken, struct{}{})
	newRun := h.run(t, runReq{JobID: 2, LogID: 3, ExecutorHandler: "T"})
	err := <-errc
	registered := h.admin.registry.Load()
	time.Sleep(60 * time.Millisecond)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, failure(ErrShutdown.Error()), beat)
	assert.Equal(t, failure(ErrShutdown.Error()), newRun)
	assert.Equal(t, "xxljob: executor is shutting down: context canceled", h.admin.callback(t, 1).Msg)
	assert.Equal(t, "trigger discarded: xxljob: executor is shutting down", h.admin.callback(t, 2).Msg)
	assert.Equal(t, registered, h.admin.registry.Load())
}

func TestShutdownWaitsForJobs(t *testing.T) {
	// Scenario: on shutdown, the running job finishes within the deadline
	// Given: the job finishes on its own after 50ms; deadline 2s
	// Expect: the job is not canceled; Shutdown returns nil after the result is delivered; calling Shutdown again has
	//         no side effects
	h := newHarness(t, map[string]Handler{"T": HandlerFunc(func(ctx context.Context, _ *Request) error {
		time.Sleep(50 * time.Millisecond)
		return ctx.Err()
	})})
	h.run(t, runReq{JobID: 1, LogID: 1, ExecutorHandler: "T"})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := h.e.Shutdown(ctx)
	again := h.e.Shutdown(ctx)

	assert.NoError(t, err)
	assert.NoError(t, again)
	assert.Equal(t, 200, h.admin.callback(t, 1).Code)
}

// ---------------------------------------------------------------------------------------------
// Execution log
// ---------------------------------------------------------------------------------------------

func TestExecutionLog(t *testing.T) {
	// Scenario: the admin's execution log view reads the log written by a job in segments
	// Given: the job writes two lines via Logger(ctx); read from fromLineNum=1, then from past the end; read a
	//        nonexistent log_id
	// Expect: the first segment contains the start line, the job's log lines and the end line, with isEnd set once
	//         execution has finished; reading past the end returns nothing new; a nonexistent log_id returns a notice
	//         with isEnd set
	h := newHarness(t, map[string]Handler{"T": HandlerFunc(func(ctx context.Context, _ *Request) error {
		Logger(ctx).Info("step one", "rows", 3)
		Logger(ctx).Warn("step two")
		return nil
	})})
	now := time.Now().UnixMilli()
	h.run(t, runReq{JobID: 1, LogID: 1, LogDateTime: now, ExecutorHandler: "T", ExecutorParams: "p"})
	h.admin.callback(t, 1)

	first := h.post(t, "/log", testToken, map[string]any{"logId": 1, "logDateTim": now, "fromLineNum": 1}).payload.(logResult)
	next := h.post(t, "/log", testToken, map[string]any{"logId": 1, "logDateTim": now, "fromLineNum": first.ToLineNum + 1}).payload.(logResult)
	missing := h.post(t, "/log", testToken, map[string]any{"logId": 404, "logDateTim": now, "fromLineNum": 1}).payload.(logResult)

	assert.Equal(t, 1, first.FromLineNum)
	assert.Equal(t, 4, first.ToLineNum)
	assert.True(t, first.IsEnd)
	lines := strings.Split(strings.TrimSpace(first.LogContent), "\n")
	require.Len(t, lines, 4)
	assert.Contains(t, lines[0], `msg="job start" handler=T params=p`)
	assert.Contains(t, lines[1], `level=INFO msg="step one" rows=3`)
	assert.Contains(t, lines[2], `level=WARN msg="step two"`)
	assert.Contains(t, lines[3], `msg="job succeeded"`)
	assert.Equal(t, logResult{FromLineNum: 5, ToLineNum: 4, IsEnd: true}, next)
	assert.True(t, missing.IsEnd)
	assert.Contains(t, missing.LogContent, "log not found")
}

func TestLoggerOutsideJob(t *testing.T) {
	// Scenario: Logger / SetResult / RequestFrom are called outside job execution
	// Given: a plain ctx
	// Expect: Logger returns slog.Default(), SetResult has no effect, RequestFrom returns false
	ctx := context.Background()
	SetResult(ctx, "x")
	_, ok := RequestFrom(ctx)

	assert.Same(t, slog.Default(), Logger(ctx))
	assert.False(t, ok)
}

// ---------------------------------------------------------------------------------------------
// Middleware, config, startup
// ---------------------------------------------------------------------------------------------

func TestMiddlewareOrder(t *testing.T) {
	// Scenario: execution order of global middleware (WithMiddleware, Use) and per-handler middleware
	// Given: WithMiddleware(a), Use(b), Register(..., c)
	// Expect: a → b → c → handler, and the handler can get the request via RequestFrom
	var mu sync.Mutex
	var calls []string
	mw := func(name string) Middleware {
		return func(next Handler) Handler {
			return HandlerFunc(func(ctx context.Context, req *Request) error {
				mu.Lock()
				calls = append(calls, name)
				mu.Unlock()
				return next.Handle(ctx, req)
			})
		}
	}
	admin := newFakeAdmin(t)
	e, err := New(testConfig(admin.srv.URL), WithoutServer(), WithLogger(quiet), WithMiddleware(mw("a")))
	require.NoError(t, err)
	e.Use(mw("b"))
	var fromCtx *Request
	require.NoError(t, e.Register("T", HandlerFunc(func(ctx context.Context, _ *Request) error {
		fromCtx, _ = RequestFrom(ctx)
		return nil
	}), mw("c")))
	require.NoError(t, e.Start())
	defer func() { _ = e.Shutdown(context.Background()) }()

	e.dispatch(runReq{JobID: 1, LogID: 1, ExecutorHandler: "T"})
	admin.callback(t, 1)

	assert.Equal(t, []string{"a", "b", "c"}, calls)
	require.NotNil(t, fromCtx)
	assert.Equal(t, int64(1), fromCtx.LogID)
}

func TestConfigNormalize(t *testing.T) {
	// Scenario: config validation and defaults
	// Given: missing address, invalid address, missing AppName, out-of-range port; a valid config (addresses with
	//        whitespace and a trailing /, an empty entry, Address unset)
	// Expect: the first four return errors; the valid config gets defaults filled in, with Address set to
	//         http://{IP}:{Port}/
	_, err := Config{AppName: "a"}.normalize()
	require.ErrorContains(t, err, "at least one admin address")
	_, err = Config{AdminAddresses: []string{"xxl-job-admin:8080"}, AppName: "a"}.normalize()
	require.ErrorContains(t, err, "invalid admin address")
	_, err = Config{AdminAddresses: []string{"http://admin"}}.normalize()
	require.ErrorContains(t, err, "AppName is required")
	_, err = Config{AdminAddresses: []string{"http://admin"}, AppName: "a", Port: 70000}.normalize()
	require.ErrorContains(t, err, "invalid port")

	c, err := Config{AdminAddresses: []string{" http://a/xxl-job-admin/ ", "", "https://b"}, AppName: " demo ", IP: "10.0.0.8"}.normalize()
	require.NoError(t, err)
	assert.Equal(t, []string{"http://a/xxl-job-admin", "https://b"}, c.AdminAddresses)
	assert.Equal(t, "demo", c.AppName)
	assert.Equal(t, "http://10.0.0.8:9999/", c.Address)
	assert.Equal(t, DefaultRegistryInterval, c.RegistryInterval)
	assert.Equal(t, DefaultMaxQueueSize, c.MaxQueueSize)

	c, err = Config{AdminAddresses: []string{"http://a"}, AppName: "x", Address: "http://10.0.0.8:8080/xxl-job"}.normalize()
	require.NoError(t, err)
	assert.Equal(t, "http://10.0.0.8:8080/xxl-job/", c.Address)
}

func TestRegisterAndStart(t *testing.T) {
	// Scenario: validation of handler registration and startup
	// Given: empty name, nil handler, duplicate registration; executor port already in use; Start called twice
	// Expect: each registration returns an error; Start fails while the port is taken (without registering); after
	//         starting on a free port, calling Start again returns an error
	admin := newFakeAdmin(t)
	ln, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	busyPort := ln.Addr().(*net.TCPAddr).Port
	cfg := testConfig(admin.srv.URL)
	cfg.Port = busyPort
	e, err := New(cfg, WithLogger(quiet))
	require.NoError(t, err)

	require.ErrorContains(t, e.Register("", HandlerFunc(noop)), "empty handler name")
	require.ErrorContains(t, e.Register("A", nil), "nil handler")
	require.NoError(t, e.Register("A", HandlerFunc(noop)))
	require.ErrorContains(t, e.Register("A", HandlerFunc(noop)), `duplicate handler "A"`)
	assert.Panics(t, func() { e.MustRegister("A", HandlerFunc(noop)) })
	require.ErrorContains(t, e.Start(), fmt.Sprintf("listen executor port %d", busyPort))
	assert.Zero(t, admin.registry.Load())

	require.NoError(t, ln.Close())
	require.NoError(t, e.Start())
	defer func() { _ = e.Shutdown(context.Background()) }()
	require.ErrorContains(t, e.Start(), "already started")
	beat := (&harness{srv: &httptest.Server{URL: fmt.Sprintf("http://127.0.0.1:%d", busyPort)}}).post(t, "/beat", testToken, struct{}{})
	assert.Equal(t, success(""), beat)
}

// ---------------------------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------------------------

func TestConcurrentTriggers(t *testing.T) {
	// Scenario: many jobs, each triggered many times concurrently (run with -race)
	// Given: 20 jobs × 10 triggers, serial execution; the handler tracks per-job concurrency
	// Expect: every trigger is reported exactly once and succeeds; executions of the same job never overlap
	var mu sync.Mutex
	active := map[int64]int{}
	var violated atomic.Bool
	h := newHarness(t, map[string]Handler{"T": HandlerFunc(func(_ context.Context, req *Request) error {
		mu.Lock()
		active[req.JobID]++
		if active[req.JobID] > 1 {
			violated.Store(true)
		}
		mu.Unlock()
		time.Sleep(time.Millisecond)
		mu.Lock()
		active[req.JobID]--
		mu.Unlock()
		return nil
	})})

	var wg sync.WaitGroup
	for job := range 20 {
		for n := range 10 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				logID := int64(job*100 + n + 1)
				res := h.e.dispatch(runReq{JobID: int64(job), LogID: logID, ExecutorHandler: "T", LogDateTime: time.Now().UnixMilli()})
				assert.Equal(t, success(""), res)
			}()
		}
	}
	wg.Wait()

	require.Eventually(t, func() bool { return h.admin.count() == 200 }, 10*time.Second, 10*time.Millisecond)
	assert.False(t, violated.Load())
	h.admin.mu.Lock()
	defer h.admin.mu.Unlock()
	for _, p := range h.admin.callbacks {
		assert.Equal(t, 200, p.Code)
	}
}

// ---------------------------------------------------------------------------------------------
// Task、HandleJSON
// ---------------------------------------------------------------------------------------------

type namedTask struct {
	name string
	ran  *atomic.Int32
}

func (t namedTask) Name() string { return t.name }
func (t namedTask) Handle(context.Context, *Request) error {
	t.ran.Add(1)
	return nil
}

func TestRegisterTasks(t *testing.T) {
	// Scenario: registering jobs in bulk by Task.Name()
	// Given: handler "Existing" is already registered; a valid group of jobs A, B; several invalid groups
	//        (duplicate within the group, duplicate of an existing name, empty name, nil)
	// Expect: A and B can be triggered after registration; invalid groups return an error and register nothing (not
	//         even the valid C in the group)
	var ran atomic.Int32
	h := newHarness(t, map[string]Handler{"Existing": HandlerFunc(noop)})

	require.NoError(t, h.e.RegisterTasks(namedTask{"A", &ran}, namedTask{"B", &ran}))
	dupInGroup := h.e.RegisterTasks(namedTask{"C", &ran}, namedTask{"D", &ran}, namedTask{"D", &ran})
	dupExisting := h.e.RegisterTasks(namedTask{"C", &ran}, namedTask{"Existing", &ran})
	empty := h.e.RegisterTasks(namedTask{"", &ran})
	nilTask := h.e.RegisterTasks(nil)
	h.run(t, runReq{JobID: 1, LogID: 1, ExecutorHandler: "A"})
	h.run(t, runReq{JobID: 2, LogID: 2, ExecutorHandler: "B"})
	h.admin.callback(t, 1)
	h.admin.callback(t, 2)
	notRegistered := h.run(t, runReq{JobID: 3, LogID: 3, ExecutorHandler: "C"})

	require.ErrorContains(t, dupInGroup, `duplicate handler "D"`)
	require.ErrorContains(t, dupExisting, `duplicate handler "Existing"`)
	require.ErrorContains(t, empty, "empty name")
	require.ErrorContains(t, nilTask, "nil task")
	assert.Equal(t, int32(2), ran.Load())
	assert.Equal(t, failure("job handler [C] not found."), notRegistered)
	assert.Panics(t, func() { h.e.MustRegisterTasks(namedTask{"A", &ran}) })
}

func TestHandleJSON(t *testing.T) {
	// Scenario: HandleJSON delivers the trigger params already decoded
	// Given: params {"days":7}, blank, and invalid JSON
	// Expect: the first two calls pass days=7 and the zero value to fn and succeed; invalid JSON reports failure
	//         without calling fn
	type params struct {
		Days int `json:"days"`
	}
	var mu sync.Mutex
	var got []params
	h := newHarness(t, map[string]Handler{"T": HandleJSON(func(_ context.Context, _ *Request, p params) error {
		mu.Lock()
		got = append(got, p)
		mu.Unlock()
		return nil
	})})

	h.run(t, runReq{JobID: 1, LogID: 1, ExecutorHandler: "T", ExecutorParams: `{"days":7}`})
	h.admin.callback(t, 1)
	h.run(t, runReq{JobID: 1, LogID: 2, ExecutorHandler: "T", ExecutorParams: "  "})
	h.admin.callback(t, 2)
	h.run(t, runReq{JobID: 1, LogID: 3, ExecutorHandler: "T", ExecutorParams: `{bad`})
	bad := h.admin.callback(t, 3)

	assert.Equal(t, []params{{Days: 7}, {}}, got)
	assert.Equal(t, 500, bad.Code)
	assert.Contains(t, bad.Msg, "invalid job params")
}

// ---------------------------------------------------------------------------------------------
// Default timeout, long-running warning, concurrency limit, runtime stats, results after shutdown
// ---------------------------------------------------------------------------------------------

func TestDefaultTimeout(t *testing.T) {
	// Scenario: the admin sets no timeout and the executor has a default timeout configured
	// Given: DefaultTimeout=300ms; executorTimeout=0; the job waits until ctx is canceled
	// Expect: after 300ms ctx is canceled with ErrTimeout and 502 is reported
	g := newGate()
	h := newHarness(t, map[string]Handler{"T": g}, func(c *Config) { c.DefaultTimeout = 300 * time.Millisecond })

	h.run(t, runReq{JobID: 1, LogID: 1, ExecutorHandler: "T"})

	assert.Equal(t, 502, h.admin.callback(t, 1).Code)
}

// syncBuffer is a concurrency-safe log buffer.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func TestLongRunningWarning(t *testing.T) {
	// Scenario: an execution exceeds the long-running warning threshold
	// Given: warning threshold 50ms; the job runs for 150ms
	// Expect: the application log and the execution log each record "job is still running" exactly once; the job
	//         still succeeds
	var app syncBuffer
	store := NewMemoryLogStore(0)
	ref := LogRef{LogID: 1}

	code, _, err := runOnce(context.Background(), HandlerFunc(func(context.Context, *Request) error {
		time.Sleep(150 * time.Millisecond)
		return nil
	}), &Request{Handler: "T", ShardTotal: 1}, runOpts{store: store, ref: ref, base: slog.New(slog.NewTextHandler(&app, nil)), longRunning: 50 * time.Millisecond})
	jobLog, rerr := store.Read(ref, 1)
	require.NoError(t, rerr)

	require.NoError(t, err)
	assert.Equal(t, 200, code)
	assert.Equal(t, 1, strings.Count(app.String(), "xxljob: job is still running"))
	assert.Equal(t, 1, strings.Count(jobLog.Content, `level=WARN msg="job is still running"`))
}

func TestMaxConcurrency(t *testing.T) {
	// Scenario: global concurrency limit 1 with two different jobs triggered at once; the second is killed while
	//           waiting for a slot
	// Given: job 1 and job 2 are each triggered once; job 3 is triggered while job 1 is running
	// Expect: only one runs at a time; triggers waiting for a slot count as running (Stats.Running=3); job 2 is
	//         killed while waiting → reports failure and never starts; job 3 starts only after job 1 is released
	g := newGate()
	h := newHarness(t, map[string]Handler{"T": g}, func(c *Config) { c.MaxConcurrency = 1 })
	h.run(t, runReq{JobID: 1, LogID: 1, ExecutorHandler: "T"})
	g.waitStart(t)
	h.run(t, runReq{JobID: 2, LogID: 2, ExecutorHandler: "T"})
	h.run(t, runReq{JobID: 3, LogID: 3, ExecutorHandler: "T"})
	require.Eventually(t, func() bool { return h.e.Stats().Running == 3 }, time.Second, 5*time.Millisecond)

	h.post(t, "/kill", testToken, killReq{JobID: 2})
	killed := h.admin.callback(t, 2)
	select {
	case id := <-g.started:
		t.Fatalf("log %d started while job 1 holds the only slot", id)
	case <-time.After(100 * time.Millisecond):
	}
	g.release <- struct{}{}
	third := g.waitStart(t)
	close(g.release)

	assert.Equal(t, "xxljob: job killed by the scheduling center", killed.Msg)
	assert.Equal(t, int64(3), third)
	assert.Equal(t, int32(1), g.maxActive.Load())
}

func TestStats(t *testing.T) {
	// Scenario: monitoring reads the runtime stats
	// Given: job 1 is running with two more triggers queued
	// Expect: Running=1, Queued=2; after release all finish and Running=0, Queued=0
	g := newGate()
	h := newHarness(t, map[string]Handler{"T": g})
	h.run(t, runReq{JobID: 1, LogID: 1, ExecutorHandler: "T"})
	g.waitStart(t)
	h.run(t, runReq{JobID: 1, LogID: 2, ExecutorHandler: "T"})
	h.run(t, runReq{JobID: 1, LogID: 3, ExecutorHandler: "T"})

	busy := h.e.Stats()
	close(g.release)
	h.admin.callback(t, 3)
	require.Eventually(t, func() bool { return h.e.Stats() == Stats{} }, time.Second, 5*time.Millisecond)

	assert.Equal(t, 1, busy.Running)
	assert.Equal(t, 2, busy.Queued)
}

func TestResultAfterShutdownIsLogged(t *testing.T) {
	// Scenario: on shutdown a job does not return within the post-cancel wait and returns its result later
	// Given: the callbacker is already closed
	// Expect: the result is not sent, but an Error (with log_id) is logged so it is not silently lost
	var buf syncBuffer
	admin := newFakeAdmin(t)
	cfg, err := testConfig(admin.srv.URL).normalize()
	require.NoError(t, err)
	cb := newCallbacker(newAdminClient(cfg, protocolV2{}, nil), slog.New(slog.NewTextHandler(&buf, nil)), cfg)
	cb.start()
	cb.close(context.Background())

	cb.push(result{LogID: 42, Code: 500, Msg: "late"})

	assert.Contains(t, buf.String(), `msg="xxljob: result arrived after shutdown, not delivered" log_id=42`)
	assert.Zero(t, admin.count())
}

func TestPodIPFromEnv(t *testing.T) {
	// Scenario: Kubernetes injects POD_IP via the Downward API, with no IP / Address configured
	// Given: POD_IP=10.1.2.3
	// Expect: the registry address uses POD_IP; an explicitly configured IP still takes precedence; a negative
	//         DefaultTimeout is an error
	t.Setenv(EnvPodIP, "10.1.2.3")

	c, err := Config{AdminAddresses: []string{"http://admin"}, AppName: "a"}.normalize()
	require.NoError(t, err)
	explicit, err := Config{AdminAddresses: []string{"http://admin"}, AppName: "a", IP: "10.9.9.9"}.normalize()
	require.NoError(t, err)
	_, negative := Config{AdminAddresses: []string{"http://admin"}, AppName: "a", DefaultTimeout: -time.Second}.normalize()

	assert.Equal(t, "http://10.1.2.3:9999/", c.Address)
	assert.Equal(t, "http://10.9.9.9:9999/", explicit.Address)
	assert.Equal(t, DefaultLongRunningThreshold, c.LongRunningThreshold)
	assert.ErrorContains(t, negative, "must not be negative")
}

// ---------------------------------------------------------------------------------------------
// Admin protocol versions
// ---------------------------------------------------------------------------------------------

func TestAdminVersion35(t *testing.T) {
	// Scenario: integration with a 3.5+ admin
	// Given: AdminVersion=3.5; admin requests carry the token and XXL-JOB-APPNAME, triggers go to /trigger, and
	//        logs are read by logDateTime
	// Expect: /trigger runs successfully and /run does not exist; requests with the wrong AppName are rejected; the
	//         callback body is {"callbackList": [...]}; requests to the admin carry the token and XXL-JOB-APPNAME;
	//         logs are read by logDateTime with the payload only in data
	h := newHarness(t, map[string]Handler{"T": HandlerFunc(func(ctx context.Context, _ *Request) error {
		Logger(ctx).Info("v35")
		return nil
	})}, func(c *Config) { c.AdminVersion = AdminVersion35 })
	h.appName = "demo"
	now := time.Now().UnixMilli()

	trig := h.post(t, "/trigger", testToken, runReq{JobID: 1, LogID: 1, LogDateTime: now, ExecutorHandler: "T"})
	cb := h.admin.callback(t, 1)
	resp, err := http.Post(h.srv.URL+"/run", "application/json", strings.NewReader("{}")) //nolint:noctx // test
	require.NoError(t, err)
	_ = resp.Body.Close()
	h.appName = "other-app"
	wrongApp := h.post(t, "/beat", testToken, struct{}{})
	h.appName = "demo"
	logRes := h.post(t, "/log", testToken, map[string]any{"logId": 1, "logDateTime": now, "fromLineNum": 1}).payload.(logResult)

	assert.Equal(t, success(""), trig)
	assert.Equal(t, result{LogID: 1, LogTime: now, Code: 200}, cb)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Equal(t, failure("The appname is wrong."), wrongApp)
	assert.Equal(t, map[string]bool{"callbackList": true}, h.admin.bodies)
	assert.Equal(t, map[string]bool{testToken: true}, h.admin.tokens)
	assert.Equal(t, map[string]bool{"demo": true}, h.admin.appNames)
	assert.Contains(t, logRes.LogContent, "msg=v35")
}

func TestAdminVersion2Default(t *testing.T) {
	// Scenario: the default admin (2.3 - 3.4)
	// Given: AdminVersion unset; requests carry no XXL-JOB-APPNAME
	// Expect: /run works as usual and /trigger does not exist; the callback body is an array; requests to the admin
	//         carry only the token, not XXL-JOB-APPNAME
	h := newHarness(t, map[string]Handler{"T": HandlerFunc(noop)})

	h.run(t, runReq{JobID: 1, LogID: 1, ExecutorHandler: "T"})
	h.admin.callback(t, 1)
	resp, err := http.Post(h.srv.URL+"/trigger", "application/json", strings.NewReader("{}")) //nolint:noctx // test
	require.NoError(t, err)
	_ = resp.Body.Close()

	assert.Equal(t, AdminVersion2, h.e.Config().AdminVersion)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Equal(t, map[string]bool{"array": true}, h.admin.bodies)
	assert.Equal(t, map[string]bool{"": true}, h.admin.appNames)
}

func TestAdminVersionConfig(t *testing.T) {
	// Scenario: AdminVersion config validation
	// Given: an unsupported version; 3.5 without an AccessToken
	// Expect: New returns an error in both cases, listing the supported values / stating that the token is required
	base := Config{AdminAddresses: []string{"http://admin"}, AppName: "a", IP: "10.0.0.1"}
	bad, v35NoToken := base, base
	bad.AdminVersion = "4"
	v35NoToken.AdminVersion = AdminVersion35

	_, errBad := New(bad)
	_, errToken := New(v35NoToken)

	require.ErrorContains(t, errBad, `unsupported AdminVersion "4"`)
	assert.ErrorContains(t, errToken, "AccessToken is required for xxl-job-admin 3.5+")
}

// ---------------------------------------------------------------------------------------------
// Edge cases
// ---------------------------------------------------------------------------------------------

func TestLongResultMessageIsTruncated(t *testing.T) {
	// Scenario: a job fails with an error message far longer than the admin can store
	// Given: an error of 3000 multi-byte runes
	// Expect: the callback carries the first maxHandleMsg runes plus "...", still valid UTF-8
	long := strings.Repeat("错", 3000)
	h := newHarness(t, map[string]Handler{"T": HandlerFunc(func(context.Context, *Request) error { return errors.New(long) })})

	h.run(t, runReq{JobID: 1, LogID: 1, ExecutorHandler: "T"})
	res := h.admin.callback(t, 1)

	assert.Equal(t, strings.Repeat("错", maxHandleMsg)+"...", res.Msg)
}

func TestLoggerWithAttrsAndGroup(t *testing.T) {
	// Scenario: a job derives loggers with With and WithGroup
	// Given: Logger(ctx).With("tenant", "a").WithGroup("db").Info("query", "rows", 2)
	// Expect: the attributes and group appear in both the execution log and the application log
	var app syncBuffer
	store := NewMemoryLogStore(0)
	ref := LogRef{LogID: 1}

	_, _, err := runOnce(context.Background(), HandlerFunc(func(ctx context.Context, _ *Request) error {
		Logger(ctx).With("tenant", "a").WithGroup("db").Info("query", "rows", 2)
		return nil
	}), &Request{Handler: "T", ShardTotal: 1}, runOpts{store: store, ref: ref, base: slog.New(slog.NewTextHandler(&app, nil))})
	jobLog, rerr := store.Read(ref, 1)
	require.NoError(t, rerr)

	require.NoError(t, err)
	assert.Contains(t, jobLog.Content, `msg=query tenant=a db.rows=2`)
	assert.Contains(t, app.String(), `msg=query tenant=a db.rows=2`)
}

func TestShutdownWithUnreachableAdmin(t *testing.T) {
	// Scenario: the admin goes away while the executor still has a result to report, then the executor shuts down
	// Given: the admin is closed after registration; a job finishes, so its callback can never be delivered; shutdown
	//        has 300ms
	// Expect: Shutdown returns within its budget (plus CancelWait) instead of hanging; deregistration and the lost
	//         result are both logged as errors
	var buf syncBuffer
	admin := newFakeAdmin(t)
	cfg := testConfig(admin.srv.URL)
	cfg.CancelWait = 200 * time.Millisecond
	cfg.RequestTimeout = 100 * time.Millisecond
	e, err := New(cfg, WithoutServer(), WithLogger(slog.New(slog.NewTextHandler(&buf, nil))))
	require.NoError(t, err)
	require.NoError(t, e.Register("T", HandlerFunc(noop)))
	require.NoError(t, e.Start())
	srv := httptest.NewServer(e.Handler())
	defer srv.Close()
	h := &harness{e: e, admin: admin, srv: srv}
	require.Eventually(t, func() bool { return admin.registry.Load() > 0 }, time.Second, 5*time.Millisecond)
	admin.srv.Close()
	h.run(t, runReq{JobID: 1, LogID: 1, ExecutorHandler: "T"})
	require.Eventually(t, func() bool { return e.Stats().PendingCallbacks == 1 }, 2*time.Second, 5*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = e.Shutdown(ctx)
	elapsed := time.Since(start)

	require.NoError(t, err, "no job was running, so nothing had to be cancelled")
	assert.Less(t, elapsed, 2*time.Second)
	assert.Contains(t, buf.String(), "xxljob: registryRemove failed")
	assert.Contains(t, buf.String(), `msg="xxljob: results not delivered before shutdown" count=1`)
}

// countingTransport counts the requests sent to the admin.
type countingTransport struct{ n atomic.Int32 }

func (c *countingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	c.n.Add(1)
	return http.DefaultTransport.RoundTrip(r)
}

// recordingStore wraps a MemoryLogStore and records the runs written to it.
type recordingStore struct {
	*MemoryLogStore
	mu     sync.Mutex
	writes map[int64]int
	closed bool
}

func (s *recordingStore) Write(ref LogRef, line string) error {
	s.mu.Lock()
	s.writes[ref.LogID]++
	s.mu.Unlock()
	return s.MemoryLogStore.Write(ref, line)
}

func (s *recordingStore) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}

func TestCustomHTTPClientAndLogStore(t *testing.T) {
	// Scenario: the caller supplies its own HTTP client for the admin and its own execution log store
	// Given: WithHTTPClient with a counting transport; WithLogStore with a recording store
	// Expect: registration and callbacks go through the client; the run's log is written to the store and served by
	//         /log; Shutdown closes the store
	admin := newFakeAdmin(t)
	transport := &countingTransport{}
	store := &recordingStore{MemoryLogStore: NewMemoryLogStore(0), writes: map[int64]int{}}
	e, err := New(testConfig(admin.srv.URL), WithoutServer(), WithLogger(quiet),
		WithHTTPClient(&http.Client{Transport: transport}), WithLogStore(store))
	require.NoError(t, err)
	require.NoError(t, e.Register("T", HandlerFunc(func(ctx context.Context, _ *Request) error {
		Logger(ctx).Info("custom store")
		return nil
	})))
	require.NoError(t, e.Start())
	srv := httptest.NewServer(e.Handler())
	defer srv.Close()
	h := &harness{e: e, admin: admin, srv: srv}

	h.run(t, runReq{JobID: 1, LogID: 7, ExecutorHandler: "T"})
	h.admin.callback(t, 7)
	served := h.post(t, "/log", testToken, map[string]any{"logId": 7, "fromLineNum": 1}).payload.(logResult)
	require.NoError(t, e.Shutdown(context.Background()))

	assert.GreaterOrEqual(t, transport.n.Load(), int32(2), "registry and callback use the custom client")
	assert.Contains(t, served.LogContent, `msg="custom store"`)
	store.mu.Lock()
	defer store.mu.Unlock()
	assert.Equal(t, 3, store.writes[7], "start, job line, end")
	assert.True(t, store.closed)
}
