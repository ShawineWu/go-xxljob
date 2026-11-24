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
	// The admin records a batch before answering: the last one is still in flight until its answer is read.
	require.Eventually(t, func() bool { return c.Pending() == 0 }, time.Second, time.Millisecond)

	ids := a.deliveredIDs()
	for i, id := range ids {
		require.Equal(t, int64(i+1), id)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, n := range a.batches {
		assert.LessOrEqual(t, n, batchSize)
	}
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
	// The files go once the answer to the last batch has been read, a moment after the admin recorded it.
	require.Eventually(t, func() bool {
		left, _ := filepath.Glob(filepath.Join(dir, "*"))
		return len(left) == 0
	}, time.Second, 5*time.Millisecond)

	assert.NotEmpty(t, saved)
	assert.ElementsMatch(t, []int64{1, 2, 3, 4}, a.deliveredIDs())
}

func TestCallbacksLoadSavedResultsOnlyWhenStarted(t *testing.T) {
	// Scenario: the callback Dir holds saved results; an executor is created but never started
	// Expect: the files stay where they are, for the next process to send
	dir := t.TempDir()
	sp, err := newSpool(dir, 0)
	require.NoError(t, err)
	require.NoError(t, sp.save(results(1, 3)))
	a := newAdmin(t)

	_ = callbacks(t, a, CallbackConfig{Dir: dir}, quiet)

	saved, _ := filepath.Glob(filepath.Join(dir, "*.jsonl"))
	assert.Len(t, saved, 1)
	assert.Zero(t, a.hits.Load())
}

func TestCallbacksSurviveCrashAfterLoading(t *testing.T) {
	// Scenario: a process loads saved results while the admin is down, then crashes (no Close); the next process
	//           starts with the same Dir once the admin is back
	// Expect: the loaded files were kept (claimed) and the next process delivers their results, then removes them
	dir := t.TempDir()
	sp, err := newSpool(dir, 0)
	require.NoError(t, err)
	require.NoError(t, sp.save(results(1, 3)))
	a := newAdmin(t)
	a.fail.Store(true)
	crashed := callbacks(t, a, CallbackConfig{Dir: dir}, quiet)
	crashed.Start()
	t.Cleanup(func() { crashed.cancel(); <-crashed.done }) // stand-in for the process exiting
	require.Eventually(t, func() bool { return a.hits.Load() > 0 }, time.Second, time.Millisecond)
	claimed, _ := filepath.Glob(filepath.Join(dir, "*"+claimedExt))

	a.fail.Store(false)
	next := callbacks(t, a, CallbackConfig{Dir: dir}, quiet)
	next.Start()
	defer next.Close(t.Context())
	require.Eventually(t, func() bool { return len(a.deliveredIDs()) >= 3 }, 2*time.Second, 5*time.Millisecond)
	require.Eventually(t, func() bool {
		left, _ := filepath.Glob(filepath.Join(dir, "*"))
		return len(left) == 0
	}, time.Second, 5*time.Millisecond)

	assert.Len(t, claimed, 1)
	assert.Subset(t, a.deliveredIDs(), []int64{1, 2, 3})
}

func TestCallbacksLoadSavedResultsAsRoomAllows(t *testing.T) {
	// Scenario: the Dir holds 5 files of 4 results; the queue holds 10
	// Expect: only the files that fit are loaded, leaving the others saved (nothing is pushed out of the queue)
	dir := t.TempDir()
	sp, err := newSpool(dir, 0)
	require.NoError(t, err)
	for i := range int64(5) {
		require.NoError(t, sp.save(results(i*4+1, i*4+4)))
	}
	a := newAdmin(t)
	a.hang.Store(true)
	c, err := NewCallbacks(NewClient([]string{a.srv.URL}, v2(t, ""), nil, time.Second), quiet,
		CallbackConfig{RetryInterval: time.Hour, MaxPending: 10, MaxBytes: 1 << 20, Dir: dir})
	require.NoError(t, err)

	c.reload()

	saved, _ := filepath.Glob(filepath.Join(dir, "*.jsonl"))
	assert.Equal(t, 8, c.Pending())
	assert.Len(t, saved, 3)
}

func TestCallbacksQueueOfOneKeepsTheNewest(t *testing.T) {
	// Scenario: MaxPending 1 while the admin is down
	// Expect: each push keeps the newest result and drops the older one, instead of dropping everything
	a := newAdmin(t)
	a.fail.Store(true)
	c, err := NewCallbacks(NewClient([]string{a.srv.URL}, v2(t, ""), nil, time.Second), quiet,
		CallbackConfig{RetryInterval: time.Hour, MaxPending: 1, MaxBytes: 1 << 20})
	require.NoError(t, err)

	c.Push(results(1, 1)[0])
	c.Push(results(2, 2)[0])

	c.mu.Lock()
	defer c.mu.Unlock()
	assert.Equal(t, results(2, 2), c.pending)
}

func TestCallbacksCloseTriesOnceEvenWithoutTime(t *testing.T) {
	// Scenario: Close with a context that is already done, the admin up and results queued
	// Expect: one more request is still attempted, so the results are delivered rather than dropped
	a := newAdmin(t)
	c, err := NewCallbacks(NewClient([]string{a.srv.URL}, v2(t, ""), nil, time.Second), quiet,
		CallbackConfig{RetryInterval: time.Hour, MaxPending: 100, MaxBytes: 1 << 20})
	require.NoError(t, err)
	c.Start()
	c.mu.Lock() // queue without notifying the sender
	c.pending = results(1, 5)
	c.mu.Unlock()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	c.Close(ctx)

	assert.Equal(t, []int64{1, 2, 3, 4, 5}, a.deliveredIDs())
}

func TestSpoolToleratesBadContent(t *testing.T) {
	// Scenario: the Dir holds a valid file, one with a corrupt line, an oversized one, a symlink and unrelated files
	// Expect: valid lines load and corrupt ones are counted; the oversized file is renamed to .bad; the symlink and
	//         unrelated files are left alone
	dir := t.TempDir()
	outside := filepath.Join(t.TempDir(), "secret")
	require.NoError(t, os.WriteFile(outside, []byte(`{"logId":99}`+"\n"), 0o600))
	sp, err := newSpool(dir, 1024)
	require.NoError(t, err)
	require.NoError(t, sp.save(results(1, 2)))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "callback-2-1-1.jsonl"), []byte("{not json\n{\"logId\":3}\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "callback-3-1-1.jsonl"), bytes.Repeat([]byte("x"), 2048), 0o600))
	require.NoError(t, os.Symlink(outside, filepath.Join(dir, "callback-4-1-1.jsonl")))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("keep"), 0o600))

	got, skipped, err := sp.claim(100)

	require.Error(t, err)
	assert.ElementsMatch(t, []int64{1, 2, 3}, logIDs(got))
	assert.Equal(t, 1, skipped)
	assert.FileExists(t, filepath.Join(dir, "callback-3-1-1.jsonl.bad"))
	assert.FileExists(t, filepath.Join(dir, "callback-4-1-1.jsonl"))
	assert.FileExists(t, filepath.Join(dir, "notes.txt"))
}

func TestSpoolIsBounded(t *testing.T) {
	// Scenario: saving to a spool limited to 200 bytes
	// Expect: saves fail once the files would exceed the limit, and the claimed files are then kept on release
	sp, err := newSpool(t.TempDir(), 200)
	require.NoError(t, err)
	require.NoError(t, sp.save(results(1, 2)))
	_, _, err = sp.claim(10)
	require.NoError(t, err)

	err = sp.save(results(3, 10))
	require.NoError(t, sp.release())

	require.ErrorContains(t, err, "byte limit")
	left, _ := filepath.Glob(filepath.Join(sp.dir, "*"+claimedExt))
	assert.Len(t, left, 1)
}

func logIDs(rs []wire.Result) []int64 {
	out := make([]int64, 0, len(rs))
	for _, r := range rs {
		out = append(out, r.LogID)
	}
	return out
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

func TestRegistryCloseIsBounded(t *testing.T) {
	// Scenario: Close while a heartbeat request hangs (1s request timeout), with a 200ms context
	// Expect: Close stops waiting for the heartbeat when its context is done, and still sends the deregistration,
	//         bounded by the request timeout
	a := newAdmin(t)
	a.hang.Store(true)
	r := NewRegistry(NewClient([]string{a.srv.URL}, v2(t, ""), nil, time.Second), quiet, "app", "http://x/", time.Hour)
	r.Start()
	require.Eventually(t, func() bool { return a.hits.Load() > 0 }, time.Second, time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	r.Close(ctx)

	assert.Less(t, time.Since(start), 1600*time.Millisecond)
	assert.Equal(t, 1, a.count("/api/registryRemove"))
}

func TestRegistryDeregistersAfterTheHeartbeat(t *testing.T) {
	// Scenario: Close while a heartbeat request is in progress at the admin
	// Expect: the deregistration is sent only once the heartbeat has finished, so the heartbeat cannot register the
	//         executor again after it
	var mu sync.Mutex
	var order []string
	inHeartbeat := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/registry" {
			inHeartbeat <- struct{}{}
			time.Sleep(100 * time.Millisecond)
		}
		mu.Lock()
		order = append(order, r.URL.Path)
		mu.Unlock()
		_, _ = w.Write([]byte(`{"code":200}`))
	}))
	defer srv.Close()
	r := NewRegistry(NewClient([]string{srv.URL}, v2(t, ""), nil, time.Second), quiet, "app", "http://x/", time.Hour)
	r.Start()
	<-inHeartbeat

	r.Close(t.Context())

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"/api/registry", "/api/registryRemove"}, order)
}

func TestCallbacksCloseStopsTheSender(t *testing.T) {
	// Scenario: Close with no time left while a callback request hangs and the sender is woken every 5ms
	// Expect: after the request in progress, Close makes exactly one more attempt; the sender does not start others
	//         (select picks at random among ready cases, so it must check for Close itself)
	const timeout = 200 * time.Millisecond
	for range 10 {
		a := newAdmin(t)
		a.hang.Store(true)
		c, err := NewCallbacks(NewClient([]string{a.srv.URL}, v2(t, ""), nil, timeout), quiet,
			CallbackConfig{RetryInterval: 5 * time.Millisecond, MaxPending: 100, MaxBytes: 1 << 20})
		require.NoError(t, err)
		c.Start()
		c.Push(wire.Result{LogID: 1})
		require.Eventually(t, func() bool { return a.hits.Load() > 0 }, time.Second, time.Millisecond)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		before, start := a.hits.Load(), time.Now()

		c.Close(ctx)

		assert.LessOrEqual(t, a.hits.Load()-before, int32(1))
		assert.Less(t, time.Since(start), 5*timeout, "two requests of 200ms, with slack for a loaded machine")
	}
}

func TestCallbacksKeepClaimedFilesUntilOverflowIsSaved(t *testing.T) {
	// Scenario: results loaded from a saved file overflow the queue; before the overflow is saved, the sender empties
	//           the queue; then saving the overflow fails
	// Expect: the claimed file is not removed while the overflow is unsaved, nor after the save failed, so the
	//         overflowing results stay on disk instead of being lost
	dir := t.TempDir()
	sp, err := newSpool(dir, 0)
	require.NoError(t, err)
	require.NoError(t, sp.save(results(1, 5)))
	a := newAdmin(t)
	c := callbacks(t, a, CallbackConfig{Dir: dir, MaxPending: 5}, quiet)
	c.reload()
	r := wire.Result{LogID: 6, LogTime: 1, Code: wire.CodeSuccess}
	c.mu.Lock() // the first half of Push(6): the oldest (claimed) results are trimmed out
	c.pending = append(c.pending, r)
	c.bytes += size(r)
	overflow := c.trim()
	c.mu.Unlock()
	require.NotEmpty(t, overflow)

	c.flush(t.Context()) // the sender delivers the rest and finds the queue empty
	afterFlush, _ := filepath.Glob(filepath.Join(dir, "*"+claimedExt))
	require.NoError(t, os.Chmod(dir, 0o500)) // the second half of Push(6): saving fails
	c.saveOverflow(overflow)
	require.NoError(t, os.Chmod(dir, 0o700))
	c.flush(t.Context())
	left, _ := filepath.Glob(filepath.Join(dir, "*"+claimedExt))

	assert.Len(t, afterFlush, 1)
	assert.Len(t, left, 1)
}

func TestRegistryDeregistersEvenWhenTheHeartbeatOutlastsTheBudget(t *testing.T) {
	// Scenario: three admin addresses, the first two hanging, so the heartbeat in progress outlasts Close's context
	// Expect: Close still sends the deregistration once its context is done (it reaches the live admin)
	const timeout = 200 * time.Millisecond
	dead, dead2 := newAdmin(t), newAdmin(t)
	dead.hang.Store(true)
	dead2.hang.Store(true)
	var removed atomic.Int32
	live := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if r.URL.Path == "/api/registryRemove" {
			removed.Add(1)
		}
		_, _ = w.Write([]byte(`{"code":200}`))
	}))
	defer live.Close()
	r := NewRegistry(NewClient([]string{dead.srv.URL, dead2.srv.URL, live.URL}, v2(t, ""), nil, timeout), quiet, "app",
		"http://e/", time.Hour)
	r.Start()
	require.Eventually(t, func() bool { return dead.hits.Load() > 0 }, time.Second, time.Millisecond)
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()

	r.Close(ctx)

	assert.Equal(t, int32(1), removed.Load())
}
