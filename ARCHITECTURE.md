# Architecture

go-xxljob is organised around the invariants an executor must keep, each with exactly one owner. A rule that spans
several mechanisms (for example "a run reports exactly one result") lives in one place instead of being spread over
the files that touch it.

```
                         ┌──────────────────────────── Executor (executor.go) ───────────────────────────┐
 admin ──HTTP──► server.go ──► trigger/validate ──► internal/sched ──► run.go ──► Handler + middlewares
   ▲                │                                   │   (job threads,        (one run: context,
   │                │ internal/wire                     │    block strategies,    execution log,
   │                │ (protocol versions)               │    admission, phases)   panic/Goexit)
   │                ▼                                   ▼
   └──── internal/report ◄──────────── outcomes ◄───────┘
         (admin client, callbacks + spool, registry heartbeat)
                                   lifecycle.go: state and shutdown plan
```

| Invariant | Owner | How it is enforced |
|---|---|---|
| A run reports exactly one result, and a cancelled run is never reported as a success | `internal/sched` | Every accepted trigger is a run with an explicit phase (`queued → ready → waiting → running → cancelling → finished`). Phases change only through one function that rejects illegal moves; outcomes leave only through `settle`, once per run; resources are released only through `retire`, after the outcome has been delivered. A cancellation (kill, cover, handler change, timeout, shutdown) is reported the moment it happens and overrides whatever the handler returns later. One function, `sched.Classify`, turns the cancellation cause and what the handler produced into the outcome; the executor, the execution log and `Invoke` all use it. |
| A job never runs twice at once | `internal/sched` | One job thread per job; the next run becomes current under the same lock that ends the previous one. A cancelled run keeps its job busy until its handler returns. |
| Resources are bounded | `internal/sched` (admission), `internal/report`, log stores | One admission limit, `MaxActiveTriggers`, counts every run that is not finished; it bounds the job goroutines and the log files open at once. The callback queue is bounded by count and bytes; log stores by bytes per run and in total. The bounds are listed in the README. |
| The executor answers only while it is running | `lifecycle.go` | The lifecycle owns the state (`new → running → closing → closed`); every check of it (triggers, `beat`, `idleBeat`) asks `accepting()`, so nothing is accepted before `Start` or after shutdown begins. |
| Shutdown is bounded | `lifecycle.go` | A shutdown plan, fixed when shutdown begins, gives every step its deadline: deregistration, waiting for jobs, cancelling them, delivering the last results, closing the port. |
| Every result reaches the admin, or is accounted for | `internal/report` | Results are queued, sent in batches, retried, and failed over to the admin address that answered last. With `CallbackDir`, results that cannot be delivered are saved and sent by the next process; otherwise they are logged as lost. Saved files are loaded only by `Start`, as far as the queue has room, and removed only once their results are delivered or saved again, so a crash in between sends them twice rather than losing them. A heartbeat in progress is never interrupted, and the deregistration waits for it. |
| Protocol differences between admin versions stay in one place | `internal/wire` | One implementation per protocol generation (2.3 – 3.4, 3.5+): paths, authentication, field names, envelopes, callback bodies. Nothing else branches on the version. The paths, admin actions, admin reply format and the official executor's messages are defined there too; the callback spool has its own record format, independent of the protocol. |
| What a handler receives cannot corrupt the executor | `run.go`, `joblog.go` | The handler gets its own copy of the request; results are reported from the scheduler's record of the trigger. A handler's panic or `runtime.Goexit` is contained in its own goroutine. Execution log writes after a run has ended are dropped, and a failing or panicking log store never brings the run down. |

## Packages

- **`xxljob`** (root): the public API and the glue. `executor.go` wires the parts together and turns admin requests
  into triggers; `lifecycle.go` owns the state and the shutdown plan; `server.go` maps HTTP requests to executor
  operations; `run.go` runs one trigger; `handlers.go` holds handlers and middlewares (chains are built outside any
  lock); `logstore.go` holds the execution log stores.
- **`internal/sched`**: job threads, block strategies, kill, concurrency limit, admission, run phases. It knows
  nothing about HTTP, the protocol or the admin, and is tested deterministically with `testing/synctest`.
- **`internal/report`**: the admin client (sticky failover, no redirects), result callbacks (batching, retries,
  bounds, spool) and the registry heartbeat.
- **`internal/wire`**: the wire protocol.
- **`internal/ratelog`**: rate limiting for log messages that could otherwise flood (rejected requests, failing
  callbacks).
- **`otelxxljob`** (separate module): OpenTelemetry tracing and metrics.

## Design choices

- **A cancelled run is reported at once, but its job stays busy until the handler returns.** The official Java
  executor reports a timeout at the deadline and starts the next trigger right away, so a handler that ignores its
  interruption can overlap the next run. Here the report is just as prompt, and runs of one job never overlap.
- **The shutdown budget follows the caller.** Running jobs get as long as the caller's context allows; the other
  steps have fixed, documented bounds, so `Shutdown` returns at most `2×RequestTimeout + 2×CancelWait + 1s` after
  the context's deadline (with one admin address).
- **Bounds are per component, defined in one place.** Logs and callbacks have different lifetimes, so they do not
  share a single byte pool; the README lists every bound and what it limits.
