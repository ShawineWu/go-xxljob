# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/). Until v1.0.0, minor versions may contain breaking API changes.

The executor (`github.com/ShawineWu/go-xxljob`) and the OpenTelemetry integration
(`github.com/ShawineWu/go-xxljob/otelxxljob`) are released together with the same version number.

## [Unreleased]

A redesign around invariants with a single owner each (see [ARCHITECTURE.md](ARCHITECTURE.md)): the scheduler,
the delivery of results, the wire protocol and the lifecycle are separate parts, and every limit and timeout is
explicit. It fixes the problems found by an audit of v0.1.0.

### Changed (breaking)

- Go 1.26 or later is required (the two Go releases the Go team supports).
- A run cancelled by a kill, a newer trigger, a handler change, a timeout or shutdown is reported as failed (or
  timed out) the moment it is cancelled, whatever its handler returns afterwards. In v0.1 a handler that ignored the
  cancellation and returned nil was reported as successful.
- `MaxActiveTriggers` (default 2000) replaces the global queue limit: it counts every trigger running, waiting for a
  slot or queued, so distinct jobs can no longer create goroutines without bound. Rejected triggers say
  "executor is busy".
- A log ID may be running or queued in only one job at a time; a repeat is rejected.
- Invalid trigger values are rejected: non-positive job or log IDs, out-of-range `executorTimeout`, and a
  `logDateTime` more than 7 days from the executor's clock.
- The executor port answers every request with HTTP 200 and a JSON body, like the official executor, including
  unknown paths (which now hint at `Config.AdminVersion`) and methods other than POST.
- Authentication messages of the 3.5 protocol match the official executor.
- Configuration: negative values are rejected instead of becoming defaults; admin addresses must not contain
  credentials, a query or a fragment; the token is trimmed. `ShutdownTimeout` defaults to 15s (was 20s), so that a
  whole shutdown fits Kubernetes' default 30s grace period.
- `Executor.Use` and `Register` reject nil middlewares. Middleware chains are built outside the executor's locks.
- Execution logs are returned from the requested line to the end, as by the official executor (the admin's log view
  of a finished run reads only one page). `FileLogStore` caps each run at 16 MiB; its retention keeps exactly the last
  `retentionDays` days and also removes directories dated more than a day ahead.
- `Request.BindJSON` errors no longer include the job parameters.
- otelxxljob: `Instrument(exec)` sets everything up and tags it with the AppName; running, queued and pending
  callbacks are up-down counters; requires OpenTelemetry v1.47.

### Added

- `Config.CallbackDir`: results that cannot be delivered (admin unreachable at shutdown, queue full) are saved and
  sent later, including by the next process using the same directory.
- `CodeSuccess`, `CodeFailure`, `CodeTimeout`.
- `ARCHITECTURE.md`, and the "Resource limits" section of the README.
- End-to-end tests against admin 3.3.1 without a token.

### Fixed

- Admin 3.3.0 and 3.3.1 rejected the executor's registration and callbacks when no token was configured: the token
  header is now always sent.
- Shutdown could hang forever (no deadline and an unreachable admin) or overrun its deadline by a lot (a heartbeat,
  deregistration or callback batch in progress). Every step now has a bound; see `Executor.Shutdown`.
- Results of jobs that finished just before the shutdown deadline could be lost; the last callbacks always get at
  least `CancelWait`.
- A result reported from a cancellation could still be in delivery when shutdown closed the callback queue.
- `Shutdown` with an expired context returned an error and claimed jobs were cancelled even when none were running.
- `Run` did not return when `Shutdown` was called elsewhere, or when the executor port failed.
- The default memory log store copied its whole index on every eviction once full (about 1000× slower writes).
- `FileLogStore` reopened (and leaked) a run's file when something logged after the run ended; writes after a run
  has ended are now dropped. A failing or panicking log store no longer crashes the executor.
- A handler calling `runtime.Goexit` (for example `t.FailNow`) left its job busy forever.
- A middleware constructor that called the executor deadlocked it; one that panicked lost queued triggers.
- A handler changing its `Request` could redirect its result to another log ID.
- `Config()` returned a slice shared with the executor.
- The "log not found" notice never showed in the admin's log view.
- Result messages were cut at 2000 characters; the limit is now 50000, as in the official executor.
- The admin client followed redirects, sending the access token to the redirect target; it always started with the
  first admin address, so a dead first address delayed every request.
- `Stats.PendingCallbacks` did not include the batch being sent; counters could briefly show more outcomes than
  accepted triggers.
- CI: documentation-only pull requests could never pass the required checks; `otelxxljob` tags were not released.

### Removed

- `MaxLogChunkBytes`, `MaxQueuedTriggers` and `DefaultMaxQueuedTriggers` (see above).

## [0.1.0] - 2026-11-17

First release.

### Added

- XXL-JOB executor supporting admin 2.3.0 – 3.4.x (`AdminVersion` "2") and 3.5.0+ (`AdminVersion` "3.5").
- Per-job execution threads with the `SERIAL_EXECUTION`, `DISCARD_LATER` and `COVER_EARLY` block strategies;
  kill, timeouts (reported as 502) and cancellation causes via `context.Cause`.
- `Handler`, `HandlerFunc`, `Task` / `RegisterTasks`, `HandleJSON` and middleware (global and per job).
- Execution logs visible in the admin via `Logger(ctx)`, with in-memory (default) and file `LogStore`s. Logs are
  read in chunks of at most `MaxLogChunkBytes` (1 MB); the file store streams the file and never returns a line
  that is still being written.
- Asynchronous, batched result callbacks with retries and admin failover.
- `Run(ctx)`: start, then shut down gracefully within `ShutdownTimeout` when the context ends; and `Shutdown` for
  finer control. `MaxConcurrency`, `DefaultTimeout`, `LongRunningThreshold`.
- `Stats` with current gauges (running, queued, pending callbacks), the registration state for readiness probes,
  and cumulative counters (accepted, rejected, succeeded, failed, timed out).
- `Invoke` for running a handler without an admin (unit tests, manual runs).
- `WithoutServer` for mounting the executor on an existing HTTP server.
- `otelxxljob` module: OpenTelemetry `Middleware` (a span and a duration measurement per run, trace ID in the
  execution log) and `ObserveStats` (executor gauges and counters).
- Unit, stress, fuzz and end-to-end tests (admin 2.4.1, 3.4.2, 3.5.0), and benchmarks.

[Unreleased]: https://github.com/ShawineWu/go-xxljob/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/ShawineWu/go-xxljob/releases/tag/v0.1.0
