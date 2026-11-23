package report

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ShawineWu/go-xxljob/internal/wire"
)

// admin is a fake admin. Its behaviour per request is set by fail/hang; it records delivered callbacks.
type admin struct {
	srv    *httptest.Server
	fail   atomic.Bool // answer code 500
	hang   atomic.Bool // block until the request is cancelled
	hits   atomic.Int32
	tokens sync.Map // received token headers

	mu        sync.Mutex
	delivered []int64        // log IDs, in delivery order
	batches   []int          // callback batch sizes
	paths     map[string]int // requests per path
}

func newAdmin(t *testing.T) *admin {
	a := &admin{paths: map[string]int{}}
	a.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a.hits.Add(1)
		a.tokens.Store(r.Header.Get("XXL-JOB-ACCESS-TOKEN"), true)
		a.mu.Lock()
		a.paths[r.URL.Path]++
		a.mu.Unlock()
		if a.hang.Load() {
			// Read the body first: only then does net/http watch the connection and cancel r.Context() when the
			// client gives up.
			_, _ = io.Copy(io.Discard, r.Body)
			<-r.Context().Done()
			return
		}
		if a.fail.Load() {
			_, _ = w.Write([]byte(`{"code":500,"msg":"db busy"}`))
			return
		}
		if r.URL.Path == "/api/callback" {
			var items []struct {
				LogID int64 `json:"logId"`
			}
			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &items)
			a.mu.Lock()
			a.batches = append(a.batches, len(items))
			for _, it := range items {
				a.delivered = append(a.delivered, it.LogID)
			}
			a.mu.Unlock()
		}
		_, _ = w.Write([]byte(`{"code":200}`))
	}))
	t.Cleanup(a.srv.Close)
	return a
}

func (a *admin) deliveredIDs() []int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]int64(nil), a.delivered...)
}

func (a *admin) count(path string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.paths[path]
}

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func v2(t *testing.T, token string) wire.Protocol {
	t.Helper()
	p, err := wire.New(wire.Version2, token, "app")
	require.NoError(t, err)
	return p
}

func callbacks(t *testing.T, a *admin, cfg CallbackConfig, log *slog.Logger) *Callbacks {
	t.Helper()
	cfg.RetryInterval = cmp.Or(cfg.RetryInterval, 20*time.Millisecond)
	cfg.MaxPending = cmp.Or(cfg.MaxPending, 10000)
	cfg.MaxBytes = cmp.Or(cfg.MaxBytes, 1<<20)
	c, err := NewCallbacks(NewClient([]string{a.srv.URL}, v2(t, ""), nil, 200*time.Millisecond), log, cfg)
	require.NoError(t, err)
	return c
}

func results(from, to int64) []wire.Result {
	var out []wire.Result
	for id := from; id <= to; id++ {
		out = append(out, wire.Result{LogID: id, LogTime: 1, Code: wire.CodeSuccess})
	}
	return out
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

func TestClientFailoverIsSticky(t *testing.T) {
	// Scenario: two admin addresses, the first one dead
	// Expect: the first request fails over to the second; later requests start with the second, so the dead one costs
	//         a single timeout instead of one per request
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer dead.Close()
	live := newAdmin(t)
	var deadHits atomic.Int32
	dead.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		deadHits.Add(1)
		w.WriteHeader(http.StatusBadGateway)
	})
	c := NewClient([]string{dead.URL, live.srv.URL}, v2(t, ""), nil, time.Second)

	for range 5 {
		require.NoError(t, c.Post(t.Context(), "registry", wire.NewRegistryParam("app", "http://x/")))
	}

	assert.Equal(t, int32(1), deadHits.Load())
	assert.Equal(t, 5, live.count("/api/registry"))
}

func TestClientDoesNotFollowRedirects(t *testing.T) {
	// Scenario: the admin answers with a redirect to another host
	// Expect: the request fails and the other host never receives the access token
	var stolen atomic.Bool
	evil := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { stolen.Store(true) }))
	defer evil.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, evil.URL+"/steal", http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	c := NewClient([]string{redirect.URL}, v2(t, "secret"), &http.Client{}, time.Second)

	err := c.Post(t.Context(), "registry", wire.NewRegistryParam("app", "http://x/"))

	require.ErrorContains(t, err, "http status 307")
	assert.False(t, stolen.Load())
}

func TestClientErrors(t *testing.T) {
	// Scenario: the admin answers with a failure code, and an always sends the token header (even empty, as admin
	//           3.3.0/3.3.1 require it)
	a := newAdmin(t)
	a.fail.Store(true)
	c := NewClient([]string{a.srv.URL}, v2(t, ""), nil, time.Second)

	err := c.Post(t.Context(), "registry", wire.NewRegistryParam("app", "http://x/"))

	require.ErrorContains(t, err, "code 500: db busy")
	_, sentEmpty := a.tokens.Load("")
	assert.True(t, sentEmpty)
}

func TestCallbacksBatchAndRetry(t *testing.T) {
	// Scenario: 250 results while the admin fails, then the admin recovers
	// Expect: nothing is lost; delivery happens in order, in batches of at most 100, after the retry interval
	a := newAdmin(t)
	a.fail.Store(true)
	c := callbacks(t, a, CallbackConfig{}, quiet)
	c.Start()
	defer c.Close(t.Context())

	for _, r := range results(1, 250) {
		c.Push(r)
	}
	require.Eventually(t, func() bool { return a.hits.Load() > 0 }, time.Second, time.Millisecond)
	a.fail.Store(false)
	require.Eventually(t, func() bool { return len(a.deliveredIDs()) == 250 }, 2*time.Second, 5*time.Millisecond)

	ids := a.deliveredIDs()
	for i, id := range ids {
		require.Equal(t, int64(i+1), id)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, n := range a.batches {
		assert.LessOrEqual(t, n, batchSize)
	}
	assert.Zero(t, c.Pending())
}

func TestCallbacksPendingCountsInflight(t *testing.T) {
	// Scenario: a callback request hangs at the admin
	// Expect: the batch being sent still counts as pending (monitoring must not see 0 while results are unsent)
	a := newAdmin(t)
	a.hang.Store(true)
	c := callbacks(t, a, CallbackConfig{}, quiet)
	c.Start()

	c.Push(wire.Result{LogID: 1})
	require.Eventually(t, func() bool { return a.hits.Load() > 0 }, time.Second, time.Millisecond)
	inflight := c.Pending()
	a.hang.Store(false)
	c.Close(t.Context())

	assert.Equal(t, 1, inflight)
	assert.Equal(t, []int64{1}, a.deliveredIDs())
}

func TestCallbacksOverflow(t *testing.T) {
	// Scenario: the admin is down and more results arrive than the queue holds (count limit 10, then size limit 1KB)
	// Expect: without Dir the oldest are dropped in chunks and logged as lost; the queue stays within its limits
	var logs syncBuffer
	a := newAdmin(t)
	a.fail.Store(true)
	byCount := callbacks(t, a, CallbackConfig{MaxPending: 10}, slog.New(slog.NewTextHandler(&logs, nil)))
	bySize, err := NewCallbacks(NewClient([]string{a.srv.URL}, v2(t, ""), nil, time.Second), quiet,
		CallbackConfig{RetryInterval: time.Hour, MaxPending: 10000, MaxBytes: 1024})
	require.NoError(t, err)

	for _, r := range results(1, 25) {
		byCount.Push(r)
	}
	for i := range 50 {
		bySize.Push(wire.Result{LogID: int64(i), Msg: strings.Repeat("x", 100)})
	}

	assert.LessOrEqual(t, byCount.Pending(), 10)
	assert.Contains(t, logs.String(), "callback queue full, results lost")
	bySize.mu.Lock()
	defer bySize.mu.Unlock()
	assert.LessOrEqual(t, bySize.bytes, 1024)
	assert.Equal(t, len(bySize.pending)*(64+100), bySize.bytes)
}

func TestCallbacksCloseIsBounded(t *testing.T) {
	// Scenario: Close while a callback request hangs at the admin (request timeout 300ms), with a 100ms context
	// Expect: the request in progress is not cut short (the admin may have processed it) but ends at its timeout;
	//         Close then gives up and logs the result as lost, returning within one request timeout of its context
	var logs syncBuffer
	a := newAdmin(t)
	a.hang.Store(true)
	c, err := NewCallbacks(NewClient([]string{a.srv.URL}, v2(t, ""), nil, 300*time.Millisecond),
		slog.New(slog.NewTextHandler(&logs, nil)), CallbackConfig{RetryInterval: time.Second, MaxPending: 100, MaxBytes: 1 << 20})
	require.NoError(t, err)
	c.Start()
	c.Push(wire.Result{LogID: 1})
	require.Eventually(t, func() bool { return a.hits.Load() > 0 }, time.Second, time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	c.Close(ctx)

	assert.Less(t, time.Since(start), 900*time.Millisecond)
	assert.Contains(t, logs.String(), "results not delivered before shutdown, results lost")
}

func TestCallbacksSurviveRestart(t *testing.T) {
	// Scenario: with a callback Dir, the admin is down at shutdown; a result also arrives after Close; a new process
	//           starts with the same Dir once the admin is back
	// Expect: the undelivered results are saved, then delivered by the new process; the files are removed
	dir := t.TempDir()
	a := newAdmin(t)
	a.fail.Store(true)
	first := callbacks(t, a, CallbackConfig{Dir: dir}, quiet)
	first.Start()
	for _, r := range results(1, 3) {
		first.Push(r)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	first.Close(ctx)
	cancel()
	first.Push(wire.Result{LogID: 4})
	saved, _ := filepath.Glob(filepath.Join(dir, "*.jsonl"))

	a.fail.Store(false)
	second := callbacks(t, a, CallbackConfig{Dir: dir}, quiet)
	second.Start()
	defer second.Close(t.Context())
	require.Eventually(t, func() bool { return len(a.deliveredIDs()) == 4 }, 2*time.Second, 5*time.Millisecond)
	left, _ := filepath.Glob(filepath.Join(dir, "*"))

	assert.NotEmpty(t, saved)
	assert.ElementsMatch(t, []int64{1, 2, 3, 4}, a.deliveredIDs())
	assert.Empty(t, left)
}

func TestSpoolSkipsCorruptFiles(t *testing.T) {
	// Scenario: the callback Dir holds a valid file, a corrupt one and unrelated files
	// Expect: the valid results load; the corrupt file is renamed to .bad; unrelated files are left alone
	dir := t.TempDir()
	sp, err := newSpool(dir)
	require.NoError(t, err)
	require.NoError(t, sp.save(results(1, 2)))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "callback-1-1-1.jsonl"), []byte("{not json"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("keep"), 0o600))

	got, err := sp.load()

	require.Error(t, err)
	assert.Equal(t, results(1, 2), got)
	assert.FileExists(t, filepath.Join(dir, "callback-1-1-1.jsonl.bad"))
	assert.FileExists(t, filepath.Join(dir, "notes.txt"))
}

func TestRegistry(t *testing.T) {
	// Scenario: the heartbeat runs while the admin is up, then down; then Close
	// Expect: Registered follows the last heartbeat; Close deregisters and Registered stays false
	a := newAdmin(t)
	r := NewRegistry(NewClient([]string{a.srv.URL}, v2(t, ""), nil, time.Second), quiet, "app", "http://x/", 20*time.Millisecond)
	r.Start()

	require.Eventually(t, r.Registered, time.Second, time.Millisecond)
	a.fail.Store(true)
	require.Eventually(t, func() bool { return !r.Registered() }, time.Second, time.Millisecond)
	a.fail.Store(false)
	r.Close(t.Context())

	assert.False(t, r.Registered())
	assert.Equal(t, 1, a.count("/api/registryRemove"))
}

func TestRegistryCloseInterruptsHeartbeat(t *testing.T) {
	// Scenario: Close while a heartbeat request hangs (10s request timeout), with a 200ms context
	// Expect: Close returns promptly instead of waiting for the heartbeat or the deregistration to time out
	a := newAdmin(t)
	a.hang.Store(true)
	r := NewRegistry(NewClient([]string{a.srv.URL}, v2(t, ""), nil, 10*time.Second), quiet, "app", "http://x/", time.Hour)
	r.Start()
	require.Eventually(t, func() bool { return a.hits.Load() > 0 }, time.Second, time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	r.Close(ctx)

	assert.Less(t, time.Since(start), time.Second)
}
