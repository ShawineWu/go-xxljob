package xxljob

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type countTask struct{}

func (countTask) Name() string { return "Count" }
func (countTask) Handle(ctx context.Context, req *Request) error {
	p := struct {
		N int `json:"n"`
	}{N: 1}
	if err := req.BindJSON(&p); err != nil {
		return err
	}
	for i := range p.N {
		Logger(ctx).Info("item", "i", i)
	}
	SetResult(ctx, "counted")
	return nil
}

func TestInvoke(t *testing.T) {
	// Scenario: run a job directly in a unit test, without the admin
	// Given: a Task with params {"n":2}; plus handlers that return an error, panic, and time out
	// Expect: on success Code is 200, Msg is the SetResult value, Log holds the start line / two job log lines / the end
	//         line, and Handler defaults to Task.Name(); error -> 500 with the error message; panic -> 500 "panic: ...";
	//         timeout -> 502 with Err set to the ctx error
	ok := Invoke(context.Background(), countTask{}, &Request{Params: `{"n":2}`})
	boom := errors.New("boom")
	failed := Invoke(context.Background(), HandlerFunc(func(context.Context, *Request) error { return boom }), nil)
	panicked := Invoke(context.Background(), HandlerFunc(func(context.Context, *Request) error { panic("oops") }), nil)
	timedOut := Invoke(context.Background(), HandlerFunc(func(ctx context.Context, _ *Request) error {
		<-ctx.Done()
		return ctx.Err()
	}), &Request{Timeout: 50 * time.Millisecond})

	require.NoError(t, ok.Err)
	assert.Equal(t, 200, ok.Code)
	assert.Equal(t, "counted", ok.Msg)
	require.Len(t, ok.Log, 4)
	assert.Contains(t, ok.Log[0], `msg="job start" handler=Count params="{\"n\":2}" shard=0/1`)
	assert.Contains(t, ok.Log[1], `msg=item i=0`)
	assert.Contains(t, ok.Log[3], `msg="job succeeded"`)

	require.ErrorIs(t, failed.Err, boom)
	assert.Equal(t, Result{Err: boom, Code: 500, Msg: "boom", Log: failed.Log}, failed)
	assert.Equal(t, 500, panicked.Code)
	assert.Equal(t, "panic: oops", panicked.Msg)
	assert.Equal(t, 502, timedOut.Code)
	assert.ErrorIs(t, timedOut.Err, context.DeadlineExceeded)
}
