package xxljob

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const testToken = "secret-token"

// fakeAdmin simulates the admin: it records registrations, deregistrations and callbacks (in either protocol
// generation). While down is set it answers every request with a failure.
type fakeAdmin struct {
	srv      *httptest.Server
	down     atomic.Bool // answer every request with a failure
	hang     atomic.Bool // hold every request until the executor gives up on it
	registry atomic.Int32
	removed  atomic.Int32

	mu        sync.Mutex
	callbacks map[int64][]callback // by log ID, in arrival order
	headers   []http.Header
}

type callback struct {
	LogTime int64
	Code    int
	Msg     string
	Form    string // "array" (2.x) or "callbackList" (3.5+)
}

func newFakeAdmin(t testing.TB) *fakeAdmin {
	a := &fakeAdmin{callbacks: map[int64][]callback{}}
	a.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		a.mu.Lock()
		a.headers = append(a.headers, r.Header.Clone())
		a.mu.Unlock()
		if a.hang.Load() {
			<-r.Context().Done()
			return
		}
		if a.down.Load() {
			_, _ = w.Write([]byte(`{"code":500,"msg":"down"}`))
			return
		}
		switch r.URL.Path {
		case "/api/registry":
			a.registry.Add(1)
		case "/api/registryRemove":
			a.removed.Add(1)
		case "/api/callback":
			a.record(body)
		default:
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte(`{"code":200}`))
	}))
	t.Cleanup(a.srv.Close)
	return a
}

// record parses a callback body in either generation: 2.x sends an array with logDateTim, 3.5+ sends
// {"callbackList": [...]} with logDateTime.
func (a *fakeAdmin) record(body []byte) {
	type item struct {
		LogID       int64  `json:"logId"`
		LogDateTim  int64  `json:"logDateTim"`
		LogDateTime int64  `json:"logDateTime"`
		HandleCode  int    `json:"handleCode"`
		HandleMsg   string `json:"handleMsg"`
	}
	var items []item
	form := "array"
	if json.Unmarshal(body, &items) != nil {
		var v35 struct {
			CallbackList []item `json:"callbackList"`
		}
		_ = json.Unmarshal(body, &v35)
		items, form = v35.CallbackList, "callbackList"
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, it := range items {
		a.callbacks[it.LogID] = append(a.callbacks[it.LogID], callback{it.LogDateTim + it.LogDateTime, it.HandleCode, it.HandleMsg, form})
	}
}

// callback waits for the result of logID and returns it.
func (a *fakeAdmin) callback(t testing.TB, logID int64) callback {
	t.Helper()
	var cb callback
	require.Eventually(t, func() bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		if cbs := a.callbacks[logID]; len(cbs) > 0 {
			cb = cbs[0]
			return true
		}
		return false
	}, 5*time.Second, 2*time.Millisecond, "no callback for log_id %d", logID)
	return cb
}

// callbackCount returns how many callbacks arrived for each log ID.
func (a *fakeAdmin) callbackCount() map[int64]int {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make(map[int64]int, len(a.callbacks))
	for id, cbs := range a.callbacks {
		out[id] = len(cbs)
	}
	return out
}

// harness is an executor mounted on an httptest server (WithoutServer), with fakeAdmin as its admin.
type harness struct {
	e     *Executor
	admin *fakeAdmin
	srv   *httptest.Server
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
		CancelWait:            500 * time.Millisecond,
	}
}

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// newHarness starts an executor with the given handlers. mutate adjusts the configuration.
func newHarness(t testing.TB, handlers map[string]Handler, mutate ...func(*Config)) *harness {
	t.Helper()
	admin := newFakeAdmin(t)
	cfg := testConfig(admin.srv.URL)
	for _, m := range mutate {
		m(&cfg)
	}
	return startHarness(t, admin, cfg, handlers, WithLogger(quiet))
}

func startHarness(t testing.TB, admin *fakeAdmin, cfg Config, handlers map[string]Handler, opts ...Option) *harness {
	t.Helper()
	e, err := New(cfg, append([]Option{WithoutServer()}, opts...)...)
	require.NoError(t, err)
	for name, h := range handlers {
		require.NoError(t, e.Register(name, h))
	}
	srv := httptest.NewServer(e.Handler())
	require.NoError(t, e.Start())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = e.Shutdown(ctx)
		srv.Close()
	})
	return &harness{e: e, admin: admin, srv: srv}
}

// reply is the decoded answer of the executor.
type reply struct {
	Code int
	Msg  string
	Log  *logPayload
}

type logPayload struct {
	FromLineNum int    `json:"fromLineNum"`
	ToLineNum   int    `json:"toLineNum"`
	LogContent  string `json:"logContent"`
	IsEnd       bool   `json:"isEnd"`
}

// post calls the executor as the admin would. headers are sent as given (use authHeaders for the right ones).
func (h *harness) post(t testing.TB, path string, headers http.Header, body any) reply {
	t.Helper()
	data, err := json.Marshal(body)
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, h.srv.URL+path, bytes.NewReader(data))
	require.NoError(t, err)
	maps.Copy(req.Header, headers)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var res struct {
		Code    int         `json:"code"`
		Msg     string      `json:"msg"`
		Content *logPayload `json:"content"`
		Data    *logPayload `json:"data"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&res))
	if h.e.cfg.AdminVersion == AdminVersion2 && res.Data != nil {
		require.Equal(t, res.Data, res.Content, "the 2.x protocol puts the payload in both content and data")
	}
	return reply{Code: res.Code, Msg: res.Msg, Log: res.Data}
}

// authHeaders are the headers the admin sends.
func (h *harness) authHeaders() http.Header {
	hd := http.Header{"Xxl-Job-Access-Token": {h.e.cfg.AccessToken}}
	if h.e.cfg.AdminVersion == AdminVersion35 {
		hd.Set("Xxl-Job-Appname", h.e.cfg.AppName)
	}
	return hd
}

// trigger is a run request; zero fields get usable defaults.
type trigger struct {
	JobID                 int64  `json:"jobId"`
	LogID                 int64  `json:"logId"`
	LogDateTime           int64  `json:"logDateTime"`
	ExecutorHandler       string `json:"executorHandler"`
	ExecutorParams        string `json:"executorParams,omitempty"`
	ExecutorBlockStrategy string `json:"executorBlockStrategy,omitempty"`
	ExecutorTimeout       int64  `json:"executorTimeout,omitempty"`
	GlueType              string `json:"glueType,omitempty"`
	BroadcastIndex        int    `json:"broadcastIndex,omitempty"`
	BroadcastTotal        int    `json:"broadcastTotal,omitempty"`
}

func (h *harness) run(t testing.TB, tr trigger) reply {
	t.Helper()
	if tr.LogDateTime == 0 {
		tr.LogDateTime = time.Now().UnixMilli()
	}
	return h.post(t, h.e.proto.RunPath(), h.authHeaders(), tr)
}

func noop(context.Context, *Request) error { return nil }

// gate is a handler that reports when it starts and then waits for release or cancellation.
type gate struct {
	started chan int64
	release chan struct{}
	active  atomic.Int32
	peak    atomic.Int32
}

func newGate() *gate { return &gate{started: make(chan int64, 64), release: make(chan struct{})} }

func (g *gate) Handle(ctx context.Context, req *Request) error {
	n := g.active.Add(1)
	defer g.active.Add(-1)
	for m := g.peak.Load(); n > m && !g.peak.CompareAndSwap(m, n); m = g.peak.Load() {
	}
	g.started <- req.LogID
	select {
	case <-g.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *gate) waitStart(t testing.TB) int64 {
	t.Helper()
	select {
	case id := <-g.started:
		return id
	case <-time.After(5 * time.Second):
		t.Fatal("job did not start")
		return 0
	}
}

// syncBuffer is a goroutine-safe log sink.
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

func capture() (*syncBuffer, *slog.Logger) {
	var buf syncBuffer
	return &buf, slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}
