# go-xxljob

[![CI](https://github.com/ShawineWu/go-xxljob/actions/workflows/ci.yml/badge.svg)](https://github.com/ShawineWu/go-xxljob/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/ShawineWu/go-xxljob.svg)](https://pkg.go.dev/github.com/ShawineWu/go-xxljob)
[![Go Report Card](https://goreportcard.com/badge/github.com/ShawineWu/go-xxljob)](https://goreportcard.com/report/github.com/ShawineWu/go-xxljob)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

English | [简体中文](README_zh.md)

A robust [XXL-JOB](https://github.com/xuxueli/xxl-job) executor for Go.

XXL-JOB is a widely used distributed task scheduler: the **admin** (scheduling center) decides when jobs run, and
**executors** run them. This library is the executor side for Go services.

- **Admin 2.3.0 – 3.5.x.** Both protocol generations are supported; switch with one setting. Tested end to end
  against admin 2.4.1, 3.3.1, 3.4.2 and 3.5.0.
- **Results you can trust.** A failing handler is reported as failed, so the admin's alarms and retries fire. A run
  that is killed or times out is reported as such the moment it happens, even if its handler ignores the
  cancellation and returns nil later.
- **A job never runs twice at once.** The three block strategies (serial, discard later, cover early) behave like
  the official executor, and the next run of a job only starts once the previous handler has returned.
- **Bounded.** Admission, queues, logs and callbacks have limits, and shutdown has a time budget; see
  [Resource limits](#resource-limits).
- **Complete:** kill, timeouts, execution logs in the admin UI, sharding broadcast, result callbacks with retries
  (and optionally saved across restarts), graceful shutdown, OpenTelemetry ([`otelxxljob`](#tracing-and-metrics-opentelemetry)).
- **Easy to test:** `xxljob.Invoke` runs a handler without an admin, exactly as the executor would.
- **Zero dependencies** beyond the Go standard library (the OpenTelemetry integration is a separate module).

## Contents

- [Install](#install)
- [Quick start](#quick-start)
- [Writing jobs](#writing-jobs)
- [Admin versions](#admin-versions)
- [Configuration](#configuration)
- [Deployment](#deployment)
- [Resource limits](#resource-limits)
- [Advanced usage](#advanced-usage)
- [How it works](#how-it-works)
- [Testing](#testing)
- [Contributing](#contributing) · [License](#license)

## Install

```bash
go get github.com/ShawineWu/go-xxljob
```

Requires Go 1.26 or later (the two Go releases the Go team supports).

## Quick start

```go
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

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
		xxljob.Logger(ctx).InfoContext(ctx, "hello", "params", req.Params) // shown in the admin's execution log
		return nil
	}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := exec.Run(ctx); err != nil { // starts; on SIGINT/SIGTERM waits for running jobs and deregisters
		panic(err)
	}
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
		xxljob.Logger(ctx).InfoContext(ctx, "synced batch", "size", len(batch))
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
| `req.JobID`, `req.LogID`, `req.Timeout`, … | Trigger metadata (the handler's own copy) |
| `xxljob.Logger(ctx)` | A `*slog.Logger` that writes to the admin's execution log *and* your application log |
| `xxljob.SetResult(ctx, msg)` | The note reported with a successful run |
| `ctx` | Cancelled on timeout, kill, cover-early, handler change and shutdown; `context.Cause(ctx)` tells which |

When the context is cancelled, the run is reported as failed (timed out for a timeout) right away, whatever the
handler returns afterwards. Long-running handlers should still watch `ctx`: until the handler returns, its job stays
busy, so later triggers of that job wait (or are discarded, depending on the block strategy).

### Unit tests

`xxljob.Invoke` runs a handler once without an admin. Logging, `SetResult`, timeouts and panic recovery behave exactly
as in a real run:

```go
func TestSyncItemsTask(t *testing.T) {
	res := xxljob.Invoke(t.Context(), NewSyncItemsTask(db), &xxljob.Request{Params: `{"batchSize":10}`})

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

When you upgrade the admin to 3.5, change these two settings; job code stays the same. The admin dropped the
`/xxl-job-admin` context path in 3.4.1, so update `AdminAddresses` too. If the admin calls a path the executor does
not serve (typically `/trigger` on an executor configured for `"2"`), its trigger log says so and points at
`AdminVersion`.

## Configuration

Only `AdminAddresses` and `AppName` are required. Zero values mean the defaults; negative values are rejected.

| Field | Default | Description |
|---|---|---|
| `AdminAddresses` | required | Admin base URLs; with several, requests go to the one that answered last and fail over to the others |
| `AppName` | required | The executor's AppName in the admin |
| `AdminVersion` | `"2"` | `"2"` for admin 2.3.0 – 3.4.x, `"3.5"` for 3.5.0+ |
| `AccessToken` | empty | See [Admin versions](#admin-versions). Empty means the executor port is not authenticated |
| `IP` | `$POD_IP`, else auto-detected | IP registered with the admin |
| `Port` | `9999` | Executor port |
| `Address` | `http://{IP}:{Port}/` | Full URL registered with the admin; set it behind NAT or when mounted on a sub-path |
| `DefaultTimeout` | `0` (none) | Timeout for jobs configured without one |
| `MaxConcurrency` | `0` (none) | Runs executing at once across all jobs; extra triggers wait for a slot |
| `MaxQueueSize` | `1000` | Queued triggers per job (serial execution) |
| `MaxActiveTriggers` | `2000` | Triggers running, waiting or queued across all jobs; see [Resource limits](#resource-limits) |
| `ShutdownTimeout` | `15s` | How long `Run` lets running jobs finish when its context ends |
| `CancelWait` | `3s` | How long shutdown waits for cancelled jobs, and the least time it gives the last callbacks |
| `CallbackDir` | empty | Directory where undeliverable results are saved and sent later, even by the next process |
| `MaxPendingCallbacks` | `10000` | Results waiting to be delivered (also capped at 64 MiB of messages) |
| `LongRunningThreshold` | `1h` | Log a warning when a run exceeds this; negative disables it |
| `RegistryInterval` | `30s` | Registry heartbeat interval |
| `RequestTimeout` | `3s` | Timeout of each request to the admin |
| `CallbackRetryInterval` | `10s` | Delay before retrying failed callbacks |

Options passed to `New`:

| Option | Default | |
|---|---|---|
| `WithLogger(*slog.Logger)` | `slog.Default()` | Logger for the executor and the application side of job logs |
| `WithLogStore(LogStore)` | in memory, 16 MiB | Where execution logs are kept |
| `WithMiddleware(...)` | none | Middleware applied to every job |
| `WithHTTPClient(*http.Client)` | `&http.Client{}` | Client for requests to the admin (copied; never follows redirects) |
| `WithoutServer()` | off | Don't listen; mount `exec.Handler()` yourself before starting |

## Deployment

### Graceful shutdown

When `Run`'s context ends (or on `Shutdown(ctx)`), the executor stops accepting triggers, deregisters, waits for
running jobs for `ShutdownTimeout` (or until `ctx` expires), cancels whatever is left, and delivers the remaining
results. Every step is bounded: with one admin address, shutdown takes at most `ShutdownTimeout + RequestTimeout +
2×CancelWait + 1s` (when `RequestTimeout ≤ CancelWait` and `ShutdownTimeout ≥ 2×RequestTimeout`), 25s with the defaults, within Kubernetes' default `terminationGracePeriodSeconds` of 30s. If you
raise `ShutdownTimeout` for longer jobs, raise the grace period with it.

### Keeping results across restarts

Results that cannot be delivered at shutdown (the admin is down, or slow) are lost by default, and the admin then
shows those runs as running. Set `CallbackDir` to a persistent directory (a volume that survives the pod) to have them
saved and sent by the next process.

### Fallback timeout

A job configured without a timeout can run forever. Set `DefaultTimeout` to the duration of your longest job: at
the timeout the run is reported as timed out and its context is cancelled. A handler that ignores its context keeps
its job busy until it returns, so make long-running loops check `ctx`.

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

## Resource limits

Everything the executor holds is bounded:

| Resource | Bound |
|---|---|
| Job goroutines | `MaxActiveTriggers` (one per job with a running or queued trigger), plus one per handler being run |
| Triggers in memory | `MaxActiveTriggers` in total, `MaxQueueSize` per job; each request body is at most 64 KiB |
| Concurrent handler runs | `MaxConcurrency` when set |
| Execution logs in memory | 16 MiB (`NewMemoryLogStore`), at most 1/8 per run, oldest finished runs evicted first |
| Execution logs on disk | 16 MiB per run (`FileLogStore`), kept for the retention period; one open file per running run |
| Undelivered results | `MaxPendingCallbacks` and 64 MiB; beyond that, saved to `CallbackDir` or dropped and logged |
| Shutdown time | see [Graceful shutdown](#graceful-shutdown) |

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

Middleware for a single job goes in `Register(name, handler, middlewares...)`. Chains are built when a trigger is
accepted, so `Use` applies to triggers accepted afterwards. Code deeper in the call stack can read the current
trigger with `xxljob.RequestFrom(ctx)`.

### Tracing and metrics (OpenTelemetry)

The [`otelxxljob`](otelxxljob) module (separate, so the executor stays dependency-free) traces every run and
exports the executor's state:

```bash
go get github.com/ShawineWu/go-xxljob/otelxxljob
```

```go
if _, err := otelxxljob.Instrument(exec); err != nil { ... } // before Start
```

- **Spans:** one per run (`xxljob {handler}`, with job ID, log ID, block strategy and shard), with errors and panics
  recorded, and a result (`xxljob.result`) that tells failures, timeouts, kills and cover-early runs apart. The
  trace ID is written to the execution log, so a run in the admin's log view leads to its trace.
- **Metrics:** the `xxljob.job.duration` histogram by handler and result; `xxljob.executor.running`, `.queued` and
  `.pending_callbacks` up-down counters; the `.registered` gauge; `xxljob.executor.triggers` (accepted / rejected)
  and `xxljob.executor.results` (success / failure / timeout) counters. Everything is tagged with the AppName.

It uses the global OpenTelemetry providers unless `WithTracerProvider` / `WithMeterProvider` is given. The admin does
not propagate trace context, so each run starts a new trace; spans created inside the job become its children.

### Stats and alerts

Without OpenTelemetry, `exec.Stats()` is a cheap snapshot for metrics:

| Field | Kind | |
|---|---|---|
| `Running`, `Queued` | gauge | Jobs with a running trigger (including triggers waiting for a slot) and queued triggers |
| `PendingCallbacks` | gauge | Results not yet delivered; a steady rise means the admin is unreachable |
| `Registered` | state | The last registry heartbeat succeeded. Alert on it; don't use it as a readiness probe, or an admin outage takes every instance out of service |
| `Accepted`, `Rejected` | counter | Triggers accepted, and refused (unknown handler, discard later, queue full, busy, …) |
| `Succeeded`, `Failed`, `TimedOut` | counter | Outcomes; every accepted trigger ends in exactly one of them |

### Execution log storage

| Store | |
|---|---|
| `NewMemoryLogStore(maxBytes)` (default) | 16 MiB total, at most 1/8 per run, oldest finished runs evicted first; lost on restart |
| `NewFileLogStore(FileLogStoreConfig{...})` | `{Dir}/{yyyy-MM-dd}/{logId}.log`, at most `MaxRunBytes` (default 16 MiB) per run, keeps the last `RetentionDays` days |
| your own | implement `LogStore` and pass it with `WithLogStore` |

Lines longer than 8 KiB are truncated. The admin reads a log from a line to its end, like with the official
executor. A store that fails or panics never fails the run; the error is logged once.

### Mounting on an existing HTTP server

When the service may expose only one port:

```go
exec, _ := xxljob.New(xxljob.Config{
	// ...
	Address: "http://10.0.0.8:8080/xxl-job/", // the URL the admin uses to reach this executor
}, xxljob.WithoutServer())
mux.Handle("/xxl-job/", http.StripPrefix("/xxl-job", exec.Handler())) // mount before starting
go server.ListenAndServe()
err := exec.Run(ctx) // registration and callbacks; your server serves the admin's requests
```

## How it works

[ARCHITECTURE.md](ARCHITECTURE.md) describes the design: each invariant below has a single owner.

### Execution model

Each job (jobId) gets a **job thread**, a goroutine that runs that job's triggers in order, one at a time. Every
accepted trigger is a run with an explicit phase (queued, ready, waiting for a slot, running, cancelling,
finished), and reports exactly one result.

When a run's context is cancelled, `context.Cause(ctx)` tells why, and the result is reported at once:

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

A **kill** cancels the running trigger and drops the queued ones. Unlike the official executor, which starts the
next run while a cancelled handler may still be running, the next run here starts only once the cancelled handler
has returned: a job never overlaps itself.

### Result callbacks

Results are queued and sent to the admin in batches, retried every `CallbackRetryInterval`, to the admin address
that answered last (failing over to the others). A request in progress is never cut short, since the admin may
already have processed it. The queue is bounded; with `CallbackDir`, results that do not fit or cannot be delivered
by shutdown are saved and sent later, even by the next process. Delivery is at least once: after a lost response a
batch may be sent again, and the admin ignores the repeat.

### Registration and shutdown

`Start` listens before registering, so a port that is already taken fails `Start` instead of registering an executor
that cannot receive triggers. The registration is renewed every `RegistryInterval` (default 30s).

`Shutdown(ctx)` follows a plan fixed when it begins:

1. reject new triggers (liveness checks fail too, so failover routing moves work to other instances);
2. stop the heartbeat, wait for the one in progress, and deregister (at most `2×RequestTimeout`);
3. discard queued triggers and triggers waiting for a slot; wait for running jobs until `ctx` expires, then cancel
   them and wait at most `CancelWait` more;
4. deliver the remaining results (at least `CancelWait`, and always one more attempt); save the rest to
   `CallbackDir` (up to 512 MiB), or log them as lost;
5. close the executor port last, so the admin can still kill jobs while it waits.

### Protocol versions

Everything that differs between admin generations (trigger path, auth headers and messages, field names, response
envelope, callback body) lives in `internal/wire`, one implementation per version. Like the official executor, the
executor port always answers HTTP 200 with a JSON body whose code tells success from failure.

### Security

- With `AccessToken` set, every request to the executor is authenticated (constant-time, length-independent
  comparison) before anything runs; the 3.5 protocol also checks `XXL-JOB-APPNAME`. **Without it, anyone who can
  reach the port can trigger and kill jobs and read their logs**: the executor logs a warning at startup.
- Request bodies and headers are capped at 64 KiB; the HTTP server has timeouts; every request to the admin has a timeout and
  never follows redirects (which would carry the token elsewhere).
- Job parameters are never included in error messages, and are capped in the execution log.
- Only BEAN mode is supported. GLUE scripts sent by the admin are rejected, never executed.

See [SECURITY.md](SECURITY.md) to report a vulnerability.

## Testing

```bash
make test       # unit tests with -race, every module
make lint       # golangci-lint
make e2e        # end-to-end against a real admin in Docker (default 2.4.1)
XXL_JOB_VERSION=3.5.0 make e2e
make e2e-all    # 2.4.1, 3.3.1, 3.4.2 and 3.5.0
KEEP=1 make e2e # keep the containers to look around in the admin UI (admin / 123456)
make fuzz       # run each fuzz target for 30s (FUZZTIME=5m for longer)
make stress     # repeat the concurrency stress tests
make bench      # benchmarks
```

- **Scheduler tests** run on a fake clock (`testing/synctest`): timeouts, kills, cover-early and shutdown are
  checked at exact instants, without sleeps or flakiness.
- **Unit tests** cover the protocols, the HTTP endpoints, callbacks (failover, retries, bounds, spool), the log
  stores and the lifecycle against a fake admin.
- **Stress tests** fire hundreds of random operations at many jobs and check invariants: every accepted trigger is
  called back exactly once, rejected ones never; a job never overlaps itself; the concurrency limit holds; the
  counters balance; no goroutine leaks. Reproduce a failure with the seed it logs:
  `XXLJOB_STRESS_SEED=<seed> go test -run TestMultiJobStress .`
- **Fuzz tests** feed arbitrary requests to every endpoint under both protocols, and arbitrary logs to the log code.
- **End-to-end tests** run against the official `xuxueli/xxl-job-admin` image for 2.4.1, 3.3.1 (without a token),
  3.4.2 and 3.5.0, including cron jobs the admin triggers every second. They need Docker with compose v2. The admin's
  schema SQL is downloaded at test time and checked against a SHA-256 (XXL-JOB is GPL licensed, so it is not vendored
  here).

CI runs lint, the unit tests on the two supported Go releases, fuzzing, `govulncheck`, CodeQL, and the end-to-end
tests on all four admin versions.

## Contributing

Issues and pull requests are welcome; see [CONTRIBUTING.md](CONTRIBUTING.md) and [ARCHITECTURE.md](ARCHITECTURE.md).
Please report security issues privately as described in [SECURITY.md](SECURITY.md).

## License

[MIT](LICENSE). XXL-JOB itself is a separate project by xuxueli, licensed under GPL-3.0; this library implements its
executor protocol and contains none of its code.
