package xxljob

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ShawineWu/go-xxljob/internal/sched"
)

// runOnce runs h with the runner against a fresh memory store and returns the code and message the admin would get.
func runOnce(t *testing.T, ctx context.Context, h Handler) (code int, msg string, err error) {
	t.Helper()
	_, app := capture()
	res, finish, err := runner{store: NewMemoryLogStore(0), app: app}.run(ctx, h, Request{Handler: "T", ShardTotal: 1},
		LogRef{LogID: 1, LogTime: time.Now()})
	o := sched.Classify(context.Cause(ctx), res)
	finish(o)
	return codeOf(o.Status), o.Msg, err
}

func TestRunOutcome(t *testing.T) {
	// Scenario: handlers end in every way, with and without a cancellation cause on their context
	// Expect: a cancellation cause wins over what the handler returned (a killed or timed-out handler that returns nil
	//         is not a success); otherwise errors fail and nil succeeds with the SetResult message
	note := func(ctx context.Context, _ *Request) error { SetResult(ctx, "note"); return nil }
	fail := func(context.Context, *Request) error { return errors.New("bad") }
	cases := []struct {
		name  string
		cause error
		h     HandlerFunc
		code  int
		msg   string
	}{
		{"success", nil, note, CodeSuccess, "note"},
		{"error", nil, fail, CodeFailure, "bad"},
		{"killed, returned nil", ErrKilled, note, CodeFailure, ErrKilled.Error()},
		{"timed out, returned an error", ErrTimeout, fail, CodeTimeout, ErrTimeout.Error()},
		{"plain cancel is not a cause", context.Canceled, note, CodeSuccess, "note"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			h := HandlerFunc(func(ctx context.Context, req *Request) error {
				if c.cause != nil {
					cancel(c.cause) // cancelled while running
				}
				return c.h(ctx, req)
			})

			code, msg, _ := runOnce(t, ctx, h)

			assert.Equal(t, c.code, code)
			assert.Equal(t, c.msg, msg)
		})
	}
}

// nilErr is an error whose Error method dereferences its receiver: returned as a typed nil, it panics when formatted.
type nilErr struct{ msg string }

func (e *nilErr) Error() string { return e.msg }

func TestRunIsolatesPanicsAndGoexit(t *testing.T) {
	// Scenario: a handler panics with a value whose String method panics too; another calls runtime.Goexit (as
	//           t.FailNow does)
	// Expect: both become failures with a clear message; the caller's goroutine is unaffected
	cases := []struct {
		name string
		h    HandlerFunc
		msg  string
	}{
		{"panic", func(context.Context, *Request) error { panic(badStringer{}) }, "panic: %!v(PANIC=String method: String exploded)"},
		{"goexit", func(context.Context, *Request) error { runtime.Goexit(); return nil }, errGoexit.Error()},
		{"error whose Error panics", func(context.Context, *Request) error { var e *nilErr; return e },
			"panic: runtime error: invalid memory address or nil pointer dereference"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			code, msg, err := runOnce(t, t.Context(), c.h)

			require.Error(t, err)
			assert.Equal(t, CodeFailure, code)
			assert.Equal(t, c.msg, msg)
		})
	}
}

type badStringer struct{}

func (badStringer) String() string { panic("String exploded") }

func TestExecutionLogLines(t *testing.T) {
	// Scenario: a handler logs through Logger(ctx) with derived loggers, and a very long line
	// Expect: lines reach the execution log with their attributes and groups, between the start and end lines; a long
	//         line is capped and stays valid UTF-8; the application log gets the handler's lines too
	appLog, app := capture()
	store := NewMemoryLogStore(0)
	ref := LogRef{LogID: 1, LogTime: time.Now()}
	h := HandlerFunc(func(ctx context.Context, _ *Request) error {
		Logger(ctx).With("tenant", "a").WithGroup("db").Info("query", "rows", 2)
		Logger(ctx).Info(strings.Repeat("错", 4000))
		return nil
	})

	res, finish, err := runner{store: store, app: app}.run(t.Context(), h, Request{Handler: "T", Params: "p", ShardTotal: 1}, ref)
	finish(sched.Classify(nil, res))
	chunk, _ := store.Read(ref, 1)
	lines := strings.Split(strings.TrimSuffix(chunk.Content, "\n"), "\n")

	require.NoError(t, err)
	require.Len(t, lines, 4)
	assert.Contains(t, lines[0], `msg="job start" handler=T params=p shard=0/1`)
	assert.Contains(t, lines[1], `msg=query tenant=a db.rows=2`)
	assert.True(t, strings.HasSuffix(lines[2], "... (truncated)"))
	assert.LessOrEqual(t, len(lines[2]), maxLogLineBytes+len("... (truncated)"))
	assert.Contains(t, lines[3], `msg="job succeeded"`)
	assert.Contains(t, appLog.String(), `msg=query handler=T job_id=0 log_id=0 tenant=a db.rows=2`)
}

func TestLongRunningWarning(t *testing.T) {
	// Scenario: a run exceeds the long-running threshold
	// Expect: one warning in the application log and one in the execution log; the run still succeeds
	appLog, app := capture()
	store := NewMemoryLogStore(0)
	ref := LogRef{LogID: 1, LogTime: time.Now()}

	res, finish, _ := runner{store: store, app: app, longRunning: 30 * time.Millisecond}.run(t.Context(),
		HandlerFunc(func(context.Context, *Request) error { time.Sleep(100 * time.Millisecond); return nil }),
		Request{Handler: "T", ShardTotal: 1}, ref)
	finish(sched.Classify(nil, res))
	chunk, _ := store.Read(ref, 1)

	assert.False(t, res.Failed)
	assert.Equal(t, 1, strings.Count(appLog.String(), "xxljob: job is still running"))
	assert.Equal(t, 1, strings.Count(chunk.Content, `level=WARN msg="job is still running"`))
}

// faultyStore fails or panics on every call.
type faultyStore struct{ panics bool }

func (s faultyStore) fail() error {
	if s.panics {
		panic("store exploded")
	}
	return errors.New("disk full")
}
func (s faultyStore) Write(LogRef, string) error         { return s.fail() }
func (s faultyStore) End(LogRef) error                   { return s.fail() }
func (s faultyStore) Read(LogRef, int) (LogChunk, error) { return LogChunk{}, ErrLogNotFound }
func (s faultyStore) Close() error                       { return nil }

func TestFaultyLogStore(t *testing.T) {
	for _, panics := range []bool{false, true} {
		t.Run(fmt.Sprintf("panics=%v", panics), func(t *testing.T) {
			// Scenario: the log store fails (or panics) on every write
			// Expect: the run itself is unaffected; the failure is reported once to the application log
			appLog, app := capture()

			res, finish, err := runner{store: faultyStore{panics}, app: app}.run(t.Context(), HandlerFunc(func(ctx context.Context, _ *Request) error {
				Logger(ctx).Info("one")
				Logger(ctx).Info("two")
				return nil
			}), Request{Handler: "T", ShardTotal: 1}, LogRef{LogID: 1})
			finish(sched.Classify(nil, res))

			require.NoError(t, err)
			assert.False(t, res.Failed)
			assert.Equal(t, 1, strings.Count(appLog.String(), "execution log write failed"))
		})
	}
}

func TestInvoke(t *testing.T) {
	// Scenario: run handlers with Invoke, as a unit test would: a Task with JSON params, a failing one, a panicking
	//           one, and one that outlives its timeout while ignoring its context
	// Expect: the same results as a real run: the task's name, the log, SetResult, failures, recovered panics and a
	//         timeout reported as such
	task := countTask{}
	ok := Invoke(context.Background(), task, &Request{Params: `{"n":2}`})
	failed := Invoke(context.Background(), HandlerFunc(func(context.Context, *Request) error { return errors.New("bad") }), nil)
	panicked := Invoke(context.Background(), HandlerFunc(func(context.Context, *Request) error { panic("boom") }), nil)
	timedOut := Invoke(context.Background(), HandlerFunc(func(context.Context, *Request) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	}), &Request{Timeout: 10 * time.Millisecond})

	require.NoError(t, ok.Err)
	assert.Equal(t, CodeSuccess, ok.Code)
	assert.Equal(t, "counted 2", ok.Msg)
	assert.Contains(t, ok.Log[0], "handler=Count")
	assert.Contains(t, ok.Log[2], "i=1")
	assert.Equal(t, InvokeResult{Err: failed.Err, Code: CodeFailure, Msg: "bad", Log: failed.Log}, failed)
	assert.Equal(t, "panic: boom", panicked.Msg)
	assert.Equal(t, CodeTimeout, timedOut.Code)
	assert.Equal(t, ErrTimeout.Error(), timedOut.Msg)
}

func TestInvokeErrIsTheCause(t *testing.T) {
	// Scenario: two handlers outlive a 10ms timeout: one returns ctx.Err() as it should, one ignores it and returns nil
	// Expect: both report ErrTimeout as Err, whatever the handler returned
	wellBehaved := Invoke(t.Context(), HandlerFunc(func(ctx context.Context, _ *Request) error {
		<-ctx.Done()
		return ctx.Err()
	}), &Request{Timeout: 10 * time.Millisecond})
	ignoring := Invoke(t.Context(), HandlerFunc(func(context.Context, *Request) error {
		time.Sleep(30 * time.Millisecond)
		return nil
	}), &Request{Timeout: 10 * time.Millisecond})

	require.ErrorIs(t, wellBehaved.Err, ErrTimeout)
	require.ErrorIs(t, ignoring.Err, ErrTimeout)
	assert.Equal(t, CodeTimeout, wellBehaved.Code)
	assert.Equal(t, CodeTimeout, ignoring.Code)
}

type countTask struct{}

func (countTask) Name() string { return "Count" }
func (countTask) Handle(ctx context.Context, req *Request) error {
	var p struct{ N int }
	if err := req.BindJSON(&p); err != nil {
		return err
	}
	for i := range p.N {
		Logger(ctx).Info("item", "i", i)
	}
	SetResult(ctx, fmt.Sprintf("counted %d", p.N))
	return nil
}

func TestBindJSON(t *testing.T) {
	// Scenario: decode job params: blank, valid, invalid; the invalid params contain a secret
	// Expect: blank keeps the defaults; the error never includes the params
	type params struct{ Size int }
	p := params{Size: 5}

	blank := (&Request{Params: "  "}).BindJSON(&p)
	err := (&Request{Params: `{"password":"hunter2"`}).BindJSON(&p)

	require.NoError(t, blank)
	assert.Equal(t, 5, p.Size)
	require.ErrorContains(t, err, "invalid job params (21 bytes)")
	assert.NotContains(t, err.Error(), "hunter2")
}

func TestHandleJSON(t *testing.T) {
	// Scenario: a function handler with typed parameters
	got := Invoke(context.Background(), HandleJSON(func(ctx context.Context, _ *Request, p struct{ Days int }) error {
		SetResult(ctx, fmt.Sprint(p.Days))
		return nil
	}), &Request{Params: `{"days":3}`})

	assert.Equal(t, "3", got.Msg)
}

func TestLoggerOutsideRun(t *testing.T) {
	// Scenario: Logger, SetResult and RequestFrom outside a run
	// Expect: the default logger, no effect, no request
	ctx := context.Background()
	SetResult(ctx, "x")
	_, ok := RequestFrom(ctx)

	assert.Same(t, slog.Default(), Logger(ctx))
	assert.False(t, ok)
}
