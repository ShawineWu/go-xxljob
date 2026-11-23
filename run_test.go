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
)

// runOnce runs h with the runner against a fresh memory store.
func runOnce(t *testing.T, h Handler) (code int, msg string, err error) {
	t.Helper()
	_, app := capture()
	return runner{store: NewMemoryLogStore(0), app: app}.run(context.Background(), h, Request{Handler: "T", ShardTotal: 1},
		LogRef{LogID: 1, LogTime: time.Now()})
}

func TestClassify(t *testing.T) {
	// Scenario: handlers end in every way, with and without a cancellation cause on their context
	// Expect: a cancellation cause wins over what the handler returned (a killed or timed-out handler that returns nil
	//         is not a success); otherwise errors fail and nil succeeds with the SetResult message
	cancelled := func(cause error) context.Context {
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(cause)
		return ctx
	}
	cases := []struct {
		name string
		ctx  context.Context
		err  error
		code int
		msg  string
	}{
		{"success", context.Background(), nil, CodeSuccess, "note"},
		{"error", context.Background(), errors.New("bad"), CodeFailure, "bad"},
		{"killed, returned nil", cancelled(ErrKilled), nil, CodeFailure, ErrKilled.Error()},
		{"timed out, returned nil", cancelled(ErrTimeout), nil, CodeTimeout, ErrTimeout.Error()},
		{"timed out, returned ctx.Err", cancelled(ErrTimeout), context.Canceled, CodeTimeout, ErrTimeout.Error()},
		{"covered, returned own error", cancelled(ErrCovered), errors.New("rollback failed"), CodeFailure, ErrCovered.Error() + ": rollback failed"},
		{"plain cancel is not a cause", cancelled(context.Canceled), nil, CodeSuccess, "note"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rs := &runState{result: "note"}

			code, msg := classify(c.ctx, c.err, rs)

			assert.Equal(t, c.code, code)
			assert.Equal(t, c.msg, msg)
		})
	}
}

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
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			code, msg, err := runOnce(t, c.h)

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

	_, _, err := runner{store: store, app: app}.run(context.Background(), h, Request{Handler: "T", Params: "p", ShardTotal: 1}, ref)
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

	code, _, _ := runner{store: store, app: app, longRunning: 30 * time.Millisecond}.run(context.Background(),
		HandlerFunc(func(context.Context, *Request) error { time.Sleep(100 * time.Millisecond); return nil }),
		Request{Handler: "T", ShardTotal: 1}, ref)
	chunk, _ := store.Read(ref, 1)

	assert.Equal(t, CodeSuccess, code)
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

			code, _, err := runner{store: faultyStore{panics}, app: app}.run(context.Background(), HandlerFunc(func(ctx context.Context, _ *Request) error {
				Logger(ctx).Info("one")
				Logger(ctx).Info("two")
				return nil
			}), Request{Handler: "T", ShardTotal: 1}, LogRef{LogID: 1})

			require.NoError(t, err)
			assert.Equal(t, CodeSuccess, code)
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
	assert.Equal(t, Result{Err: failed.Err, Code: CodeFailure, Msg: "bad", Log: failed.Log}, failed)
	assert.Equal(t, "panic: boom", panicked.Msg)
	assert.Equal(t, CodeTimeout, timedOut.Code)
	assert.Equal(t, ErrTimeout.Error(), timedOut.Msg)
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
