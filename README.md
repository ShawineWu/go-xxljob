# go-xxljob

[![CI](https://github.com/ShawineWu/go-xxljob/actions/workflows/ci.yml/badge.svg)](https://github.com/ShawineWu/go-xxljob/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/ShawineWu/go-xxljob.svg)](https://pkg.go.dev/github.com/ShawineWu/go-xxljob)
[![Go Report Card](https://goreportcard.com/badge/github.com/ShawineWu/go-xxljob)](https://goreportcard.com/report/github.com/ShawineWu/go-xxljob)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

English | [简体中文](README_zh.md)

A robust [XXL-JOB](https://github.com/xuxueli/xxl-job) executor for Go.

XXL-JOB is a widely used distributed task scheduler: the **admin** (scheduling center) decides when jobs run, and
**executors** run them. This library is the executor side for Go services.

- **Admin 2.3.0 – 3.5.x.** Both protocol generations are supported; switch with one setting.
- **Failures are failures.** Return an `error` and the admin shows the run as failed and fires its alarms and retries.
  Panics are recovered and reported as failures too.
- **A job never runs concurrently with itself.** The three block strategies (serial, discard later, cover early)
  behave like the official Java executor, including across kills.
- **Complete:** kill, timeouts, execution logs in the admin UI, sharding broadcast, retried result callbacks,
  graceful shutdown.
- **Secure by default:** access-token check on every request, body limits, timeouts on every connection.
- **Easy to test:** `xxljob.Invoke` runs a handler without an admin, exactly as the executor would.
- **Zero dependencies** beyond the Go standard library.

## Contents

- [Install](#install)
- [Quick start](#quick-start)
- [Writing jobs](#writing-jobs)
- [Admin versions](#admin-versions)
- [Configuration](#configuration)
- [Deployment](#deployment)
- [Advanced usage](#advanced-usage)
- [How it works](#how-it-works)
- [Testing](#testing)
- [Contributing](#contributing) · [License](#license)

## Install

```bash
go get github.com/ShawineWu/go-xxljob
```

Requires Go 1.24 or later.

## Quick start

```go
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ShawineWu/go-xxljob"
)

func main() {
	exec, err := xxljob.New(xxljob.Config{
		AdminAddresses: []string{"http://xxl-job-admin:8080/xxl-job-admin"},
		AccessToken:    "default_token",
		AppName:        "demo-executor", // the AppName of the executor in the admin
	})
	if err != nil {
		panic(err)
	}

	exec.MustRegister("Hello", xxljob.HandlerFunc(func(ctx context.Context, req *xxljob.Request) error {
		xxljob.Logger(ctx).Info("hello", "params", req.Params) // shown in the admin's execution log
		return nil
	}))

	if err := exec.Start(); err != nil { // listens on :9999 and registers with the admin
		panic(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_ = exec.Shutdown(shutdownCtx) // waits for running jobs, then deregisters
}
```

Then, in the admin:

1. **Executor Management** → add an executor with AppName `demo-executor` and automatic registration.
2. **Job Management** → add a job for that executor with run mode **BEAN** and JobHandler `Hello`.

More complete programs are in [`examples/`](examples): a standalone executor with several jobs, and an executor
mounted on an existing HTTP server.

## Writing jobs

### One type per job

For services with more than a couple of jobs, give each job its own type. `Name` returns the JobHandler configured
in the admin, and dependencies come in through the constructor:

```go
type SyncItemsTask struct{ db *sql.DB }

func NewSyncItemsTask(db *sql.DB) *SyncItemsTask { return &SyncItemsTask{db: db} }

// Name is the JobHandler configured in the admin.
func (t *SyncItemsTask) Name() string { return "SyncItems" }

func (t *SyncItemsTask) Handle(ctx context.Context, req *xxljob.Request) error {
	p := struct {
		BatchSize int `json:"batchSize"`
	}{BatchSize: 500} // kept when the job has no parameters
	if err := req.BindJSON(&p); err != nil {
		return err // returning an error fails the run
	}

	for _, batch := range batches {
		if err := ctx.Err(); err != nil {
			return err // timed out, killed, or the executor is shutting down
		}
		xxljob.Logger(ctx).Info("synced batch", "size", len(batch))
	}

	xxljob.SetResult(ctx, "synced 1200 items") // optional note shown on success
	return nil
}
```

Register them in one call; nothing is registered if any name is empty or duplicated:

```go
exec.MustRegisterTasks(
	NewSyncItemsTask(db),
	NewDailyReportTask(db, mailer),
)
```

### Function handlers

Small jobs can be plain functions. `HandleJSON` decodes the parameters for you:

```go
exec.MustRegister("Cleanup", xxljob.HandleJSON(func(ctx context.Context, req *xxljob.Request, p struct{ Days int }) error {
	...
}))
```

### What a handler gets

| | |
|---|---|
| `req.Params`, `req.BindJSON(&v)` | Job parameters from the admin |
| `req.ShardIndex`, `req.ShardTotal` | Sharding broadcast: this instance's shard (0-based) and the shard count |
| `req.JobID`, `req.LogID`, `req.Timeout`, … | Trigger metadata |
| `xxljob.Logger(ctx)` | A `*slog.Logger` that writes to the admin's execution log *and* your application log |
| `xxljob.SetResult(ctx, msg)` | The note reported with a successful run |
| `ctx` | Cancelled on timeout, kill, cover-early and shutdown; `context.Cause(ctx)` tells you which |

Long-running handlers must watch `ctx`. Until a handler returns, later triggers of the same job are only queued or
discarded, and a handler that ignores `ctx` cannot be killed.

### Unit tests

`xxljob.Invoke` runs a handler once without an admin. Logging, `SetResult`, timeouts and panic recovery behave exactly
as in a real run:

```go
func TestSyncItemsTask(t *testing.T) {
	res := xxljob.Invoke(context.Background(), NewSyncItemsTask(db), &xxljob.Request{Params: `{"batchSize":10}`})

	require.NoError(t, res.Err)
	assert.Equal(t, "synced 1200 items", res.Msg)
	t.Log(strings.Join(res.Log, "\n")) // what the admin's execution log would show
}
```

`Invoke` is also handy for "run this job now" admin endpoints and maintenance commands.

## Admin versions

XXL-JOB 3.5.0 changed the executor protocol in incompatible ways, so set `AdminVersion` to match your admin:

| Admin version | `AdminVersion` | `AccessToken` |
|---|---|---|
| 2.3.0 – 3.4.x | leave empty (default `"2"`) | the admin's global `xxl.job.accessToken`; optional |
| 3.5.0 and later | `"3.5"` | the token of **this executor** in Executor Management; required |

When you upgrade the admin to 3.5, change these two settings; job code stays the same. Note that the admin dropped
the `/xxl-job-admin` context path in 3.4.1, so update `AdminAddresses` too.

## Configuration

Only `AdminAddresses` and `AppName` are required.

| Field | Default | Description |
|---|---|---|
| `AdminAddresses` | required | Admin URLs; with several, they are tried in order |
| `AppName` | required | The executor's AppName in the admin |
| `AdminVersion` | `"2"` | `"2"` for admin 2.3.0 – 3.4.x, `"3.5"` for 3.5.0+ |
| `AccessToken` | empty | See [Admin versions](#admin-versions) |
| `IP` | `$POD_IP`, else auto-detected | IP registered with the admin |
| `Port` | `9999` | Executor port |
| `Address` | `http://{IP}:{Port}/` | Full URL registered with the admin; set it behind NAT or when mounted on a sub-path |
| `DefaultTimeout` | `0` (none) | Timeout for jobs configured without one |
| `MaxConcurrency` | `0` (none) | Cap on runs executing at once across all jobs; extra triggers wait for a slot |
| `MaxQueueSize` | `1000` | Max queued triggers per job (serial execution) |
| `LongRunningThreshold` | `1h` | Log a warning when a run exceeds this |
| `RegistryInterval` | `30s` | Registry heartbeat interval |
| `RequestTimeout` | `3s` | Timeout of each request to the admin |
| `CallbackRetryInterval` | `10s` | Delay before retrying failed result callbacks |
| `MaxPendingCallbacks` | `10000` | Cap on undelivered results; the oldest are dropped beyond it |
| `CancelWait` | `3s` | How long `Shutdown` waits for runs after cancelling them |

Options passed to `New`:

| Option | Default | |
|---|---|---|
| `WithLogger(*slog.Logger)` | `slog.Default()` | Logger for the executor and the application side of job logs |
| `WithLogStore(LogStore)` | in memory, 16 MB | Where execution logs are kept |
| `WithMiddleware(...)` | none | Middleware applied to every job |
| `WithHTTPClient(*http.Client)` | `&http.Client{}` | Client for requests to the admin |
| `WithoutServer()` | off | Don't listen; mount `exec.Handler()` yourself |

## Deployment

### Graceful shutdown

On `Shutdown(ctx)` the executor stops accepting triggers, deregisters, waits for running jobs until `ctx` expires,
cancels whatever is left, and delivers the remaining results. Make sure the platform gives it the time:

```
shutdown ctx timeout + ~10s  <  time before the process is killed
```

On Kubernetes that is `terminationGracePeriodSeconds` (30s by default). The extra ~10s covers deregistration,
`CancelWait` and the final callbacks.

### Fallback timeout

A job configured without a timeout that hangs and ignores `ctx` blocks every later trigger of that job. Setting
`DefaultTimeout` to the duration of your longest job bounds that.

### Executor IP

Without `IP` or `Address`, the executor registers the first up, non-loopback IPv4 address. In a standard Kubernetes
pod that is the pod IP. On hosts or pods with several interfaces (for example Multus), set `IP`, or expose the pod
IP through the Downward API; `POD_IP` takes precedence over auto-detection:

```yaml
env:
  - name: POD_IP
    valueFrom:
      fieldRef:
        fieldPath: status.podIP
```

## Advanced usage

### Middleware

Middleware wraps every run, for tracing, metrics, logging fields, rate limiting and the like:

```go
exec.Use(func(next xxljob.Handler) xxljob.Handler {
	return xxljob.HandlerFunc(func(ctx context.Context, req *xxljob.Request) error {
		start := time.Now()
		err := next.Handle(ctx, req)
		jobDuration.WithLabelValues(req.Handler).Observe(time.Since(start).Seconds())
		return err
	})
})
```

Middleware for a single job goes in `Register(name, handler, middlewares...)`. Code deeper in the call stack can
read the current trigger with `xxljob.RequestFrom(ctx)`.

### Metrics

`exec.Stats()` returns the number of running and queued triggers and pending callbacks; export it as gauges. A
steadily growing `PendingCallbacks` means the admin is unreachable.

### Execution log storage

| Store | |
|---|---|
| `NewMemoryLogStore(maxBytes)` (default) | 16 MB total, oldest finished runs evicted first, at most 1/8 per run; lost on restart |
| `NewFileLogStore(dir, retentionDays)` | `{dir}/{yyyy-MM-dd}/{logId}.log`, cleaned up after `retentionDays` |
| your own | implement `LogStore` and pass it with `WithLogStore` |

Lines longer than 8 KB are truncated. The admin reads a log in chunks of at most 1 MB (`MaxLogChunkBytes`), so
large logs are paged rather than loaded at once.

### Mounting on an existing HTTP server

When the service may expose only one port:

```go
exec, _ := xxljob.New(xxljob.Config{
	// ...
	Address: "http://10.0.0.8:8080/xxl-job/", // the URL the admin uses to reach this executor
}, xxljob.WithoutServer())
mux.Handle("/xxl-job/", http.StripPrefix("/xxl-job", exec.Handler()))
_ = exec.Start() // registry and callbacks only
```

## How it works

### Execution model

Each job (jobId) gets a **job thread**, a goroutine that runs that job's triggers in order, one at a time. It exits
when idle and is recreated by the next trigger.

```
trigger ─► validate (token, handler, duplicate logId) ─► block strategy ─► queue ─► job thread
                                                                                     │
                 callback queue ◄── result ◄── panic recovery ◄── middleware ◄── job ◄┘
                       │
                       └─► batched callbacks to the admin (retries, failover)
```

Every trigger has its own state. When its context is cancelled, `context.Cause(ctx)` tells you why:

| Cause | When | Reported as |
|---|---|---|
| `ErrKilled` | Killed in the admin | failure (500) |
| `ErrCovered` | Replaced by a newer trigger (cover early) | failure (500) |
| `ErrHandlerChanged` | The job was switched to another JobHandler | failure (500) |
| `ErrTimeout` | Exceeded the job's timeout | timeout (502) |
| `ErrShutdown` | The executor shut down and stopped waiting | failure (500) |

### Block strategies

What happens when a trigger arrives while the previous run of the same job is still going:

| Strategy | Behaviour |
|---|---|
| Serial execution (`SERIAL_EXECUTION`) | Queue it; run after the previous one. Rejected beyond `MaxQueueSize` |
| Discard later (`DISCARD_LATER`) | Reject the new trigger |
| Cover early (`COVER_EARLY`) | Cancel the running one, drop the queued ones, and run the new one **after the cancelled run returns** |

A **kill** cancels the running trigger and drops the queued ones. Until the cancelled run actually returns, the job
still counts as running, so triggering it again right after a kill never runs two copies at once.

### Result callbacks

Results are queued and sent to the admin in batches by a background goroutine, trying each admin address in turn.
Failed batches are retried every `CallbackRetryInterval`; the queue is bounded. On shutdown the remaining results
are flushed. Results not delivered before a restart are lost, and the admin marks those runs "lost" after about 10
minutes.

### Registration and shutdown

`Start` listens before registering, so a port that is already taken fails `Start` instead of registering an executor
that cannot receive triggers. The registration is renewed every 30s.

`Shutdown(ctx)`:

1. rejects new triggers (liveness checks fail too, so failover routing moves work to other instances);
2. stops the heartbeat and deregisters;
3. reports queued triggers as failed;
4. waits for running jobs until `ctx` expires, then cancels them and waits `CancelWait` more;
5. delivers the remaining results;
6. closes the executor port last, so the admin can still kill jobs while it waits.

### Protocol versions

Everything that differs between admin generations (trigger path, auth headers, field names, response envelope,
callback body) lives behind the internal `protocol` interface, one file per version. The rest of the executor works
with version-neutral types, so supporting a new generation means adding one implementation.

### Security

- With `AccessToken` set, every request to the executor is authenticated (constant-time comparison) before anything
  runs; the 3.5 protocol also checks `XXL-JOB-APPNAME`.
- Request bodies are capped at 1 MB; the HTTP server has read/write timeouts; every request to the admin has a
  timeout.
- Only BEAN mode is supported. GLUE scripts sent by the admin are rejected, never executed.

## Testing

```bash
make test       # unit tests with -race
make lint       # golangci-lint
make e2e        # end-to-end against a real admin in Docker (default 2.4.1)
XXL_JOB_VERSION=3.5.0 make e2e
make e2e-all    # 2.4.1, 3.4.2 and 3.5.0
KEEP=1 make e2e # keep the containers to look around in the admin UI (admin / 123456)
XXLJOB_E2E_VERBOSE=1 make e2e # include all executor logs (warnings and errors are always shown)
make fuzz       # run each fuzz target for 30s (FUZZTIME=5m for longer)
make bench      # benchmarks
```

- **Unit tests** use a fake admin to cover the protocols, block strategies, kill, timeouts, callbacks, shutdown and
  concurrency.
- **Stress tests**: `TestMultiJobStress` fires 800 random operations (trigger, kill, idle check, log read) at 40 jobs
  with mixed block strategies and handlers that succeed, fail, panic, time out or only stop when killed.
  `TestShutdownUnderLoad` shuts down while triggers keep coming. Both check invariants: every accepted trigger gets
  exactly one callback, rejected ones get none, a job never overlaps itself, global concurrency stays within its cap,
  and no goroutines leak. Scenarios are random; reproduce a failure with the seed it logs:
  `XXLJOB_STRESS_SEED=<seed> go test -run TestMultiJobStress .`
- **Fuzz tests** feed arbitrary requests to every endpoint under both protocols, and arbitrary logs to the log
  paging and line truncation code, checking that nothing panics and every invariant holds.
- **End-to-end tests** run against the official `xuxueli/xxl-job-admin` image for each supported version, including
  cron jobs that the admin triggers every second while earlier runs are still going. They need Docker with
  compose v2. The admin's schema SQL is downloaded at test time and checked against a SHA-256 (XXL-JOB is GPL
  licensed, so it is not vendored here).

CI runs lint, unit tests on Go 1.24 and the two latest Go releases, a short fuzzing pass, `govulncheck`, CodeQL, and the end-to-end tests
on all three admin versions.

## Contributing

Issues and pull requests are welcome; see [CONTRIBUTING.md](CONTRIBUTING.md). Please report security issues
privately as described in [SECURITY.md](SECURITY.md).

## License

[MIT](LICENSE). XXL-JOB itself is a separate project by xuxueli, licensed under GPL-3.0; this library implements its
executor protocol and contains none of its code.
