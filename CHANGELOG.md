# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/). Until v1.0.0, minor versions may contain breaking API changes.

The executor (`github.com/ShawineWu/go-xxljob`) and the OpenTelemetry integration
(`github.com/ShawineWu/go-xxljob/otelxxljob`) are released together with the same version number.

## [Unreleased]

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
