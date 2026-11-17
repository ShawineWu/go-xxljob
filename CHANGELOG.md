# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/). Until v1.0.0, minor versions may contain breaking API changes.

## [Unreleased]

## [0.1.0] - 2026-11-17

First release.

### Added

- XXL-JOB executor supporting admin 2.3.0 – 3.4.x (`AdminVersion` "2") and 3.5.0+ (`AdminVersion` "3.5").
- Per-job execution threads with the `SERIAL_EXECUTION`, `DISCARD_LATER` and `COVER_EARLY` block strategies;
  kill, timeouts (reported as 502) and cancellation causes via `context.Cause`.
- `Handler`, `HandlerFunc`, `Task` / `RegisterTasks`, `HandleJSON` and middleware (global and per job).
- Execution logs visible in the admin via `Logger(ctx)`, with in-memory (default) and file `LogStore`s.
- Asynchronous, batched result callbacks with retries and admin failover.
- Graceful `Shutdown`, `Stats`, `MaxConcurrency`, `DefaultTimeout`, `LongRunningThreshold`.
- `Invoke` for running a handler without an admin (unit tests, manual runs).
- `WithoutServer` for mounting the executor on an existing HTTP server.
- Execution logs are read in chunks of at most `MaxLogChunkBytes` (1 MB); the file store streams the file and never
  returns a line that is still being written.
- Unit, stress, fuzz and end-to-end tests (admin 2.4.1, 3.4.2, 3.5.0), and benchmarks.

[Unreleased]: https://github.com/ShawineWu/go-xxljob/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/ShawineWu/go-xxljob/releases/tag/v0.1.0
