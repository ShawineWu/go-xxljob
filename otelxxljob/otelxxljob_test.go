package otelxxljob

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/ShawineWu/go-xxljob"
)

type telemetry struct {
	spans  *tracetest.SpanRecorder
	reader *sdkmetric.ManualReader
	opts   []Option
}

func newTelemetry(extra ...Option) *telemetry {
	spans := tracetest.NewSpanRecorder()
	reader := sdkmetric.NewManualReader()
	return &telemetry{
		spans:  spans,
		reader: reader,
		opts: append([]Option{
			WithTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans))),
			WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))),
		}, extra...),
	}
}

// metrics collects the current metrics by name.
func (tm *telemetry) metrics(t *testing.T) map[string]metricdata.Aggregation {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, tm.reader.Collect(context.Background(), &rm))
	out := map[string]metricdata.Aggregation{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			out[m.Name] = m.Data
		}
	}
	return out
}

func attrOf(set attribute.Set, key attribute.Key) string {
	v, _ := set.Value(key)
	return v.String()
}

func spanAttr(s sdktrace.ReadOnlySpan, key attribute.Key) string {
	for _, kv := range s.Attributes() {
		if kv.Key == key {
			return kv.Value.String()
		}
	}
	return ""
}

func TestMiddlewareRecordsRuns(t *testing.T) {
	// Scenario: runs end in every way: success, failure, timeout, kill, cover, panic, and a killed handler that
	//           returns nil anyway
	// Expect: one span per run named "xxljob {handler}" with the job's attributes and its result (the cancellation
	//         cause wins over a nil return, as it does for the executor's report); non-successful runs have status
	//         Error and a recorded error; one duration point per handler and result, labelled with both
	tm := newTelemetry(WithAttributes(AttrAppName.String("demo")))
	mw := Middleware(tm.opts...)
	cancelledWith := func(cause error) context.Context {
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(cause)
		return ctx
	}
	cases := []struct {
		handler string
		ctx     context.Context
		timeout time.Duration
		h       xxljob.HandlerFunc
		result  string
	}{
		{"Ok", context.Background(), 0, func(context.Context, *xxljob.Request) error { return nil }, ResultSuccess},
		{"Fail", context.Background(), 0, func(context.Context, *xxljob.Request) error { return errors.New("bad input") }, ResultFailure},
		{"Slow", context.Background(), 20 * time.Millisecond, func(ctx context.Context, _ *xxljob.Request) error {
			<-ctx.Done()
			return ctx.Err()
		}, ResultTimeout},
		{"Killed", cancelledWith(xxljob.ErrKilled), 0, func(context.Context, *xxljob.Request) error { return nil }, ResultKilled},
		{"Covered", cancelledWith(xxljob.ErrCovered), 0, func(ctx context.Context, _ *xxljob.Request) error { return ctx.Err() }, ResultCovered},
		{"Panic", context.Background(), 0, func(context.Context, *xxljob.Request) error { panic("boom") }, ResultPanic},
	}

	for i, c := range cases {
		run := mw(c.h)
		// Invoke skips handlers whose context is already cancelled, so call the middleware directly for those.
		if c.ctx.Err() != nil {
			_ = run.Handle(c.ctx, &xxljob.Request{Handler: c.handler, JobID: int64(i + 1), ShardIndex: 1, ShardTotal: 3,
				BlockStrategy: xxljob.BlockSerial})
			continue
		}
		xxljob.Invoke(c.ctx, run, &xxljob.Request{Handler: c.handler, JobID: int64(i + 1), Timeout: c.timeout,
			BlockStrategy: xxljob.BlockSerial, ShardIndex: 1, ShardTotal: 3})
	}
	spans := tm.spans.Ended()
	durations := tm.metrics(t)["xxljob.job.duration"].(metricdata.Histogram[float64])

	require.Len(t, spans, len(cases))
	points := map[string]string{}
	for _, dp := range durations.DataPoints {
		assert.Equal(t, uint64(1), dp.Count)
		assert.Equal(t, "demo", attrOf(dp.Attributes, AttrAppName))
		points[attrOf(dp.Attributes, AttrHandler)] = attrOf(dp.Attributes, AttrResult)
	}
	for i, c := range cases {
		s := spans[i]
		assert.Equal(t, "xxljob "+c.handler, s.Name())
		assert.Equal(t, c.result, spanAttr(s, AttrResult), c.handler)
		assert.Equal(t, c.result, points[c.handler], "duration label of %s", c.handler)
		assert.Equal(t, "1", spanAttr(s, AttrShardIndex))
		assert.Equal(t, "3", spanAttr(s, AttrShardTotal))
		assert.Equal(t, "demo", spanAttr(s, AttrAppName))
		if c.result == ResultSuccess || c.result == ResultKilled {
			assert.Empty(t, s.Events(), c.handler) // no error was returned
		} else {
			assert.Equal(t, codes.Error, s.Status().Code, c.handler)
			require.Len(t, s.Events(), 1, c.handler)
		}
	}
	assert.Equal(t, "panic: boom", spans[5].Status().Description)
}

func TestSpanStatusIsClipped(t *testing.T) {
	// Scenario: a handler fails with a very long error
	// Expect: the span's status description is capped
	tm := newTelemetry()

	xxljob.Invoke(context.Background(), Middleware(tm.opts...)(xxljob.HandlerFunc(func(context.Context, *xxljob.Request) error {
		return errors.New(strings.Repeat("x", 10000))
	})), &xxljob.Request{Handler: "T"})

	assert.Len(t, tm.spans.Ended()[0].Status().Description, maxStatusDescription+len("..."))
}

func TestMiddlewareLogsTraceID(t *testing.T) {
	// Scenario: the trace ID of a run is written to its execution log, so it can be found from the admin
	// Given: the default options, and WithLogTraceID(false)
	// Expect: by default the log has a "trace" line with the span's trace ID; with the option it has none
	tm := newTelemetry()
	off := newTelemetry(WithLogTraceID(false))
	h := xxljob.HandlerFunc(func(context.Context, *xxljob.Request) error { return nil })

	on := xxljob.Invoke(context.Background(), Middleware(tm.opts...)(h), &xxljob.Request{Handler: "T"})
	silent := xxljob.Invoke(context.Background(), Middleware(off.opts...)(h), &xxljob.Request{Handler: "T"})
	traceID := tm.spans.Ended()[0].SpanContext().TraceID().String()

	assert.Contains(t, strings.Join(on.Log, "\n"), `msg=trace trace_id=`+traceID)
	assert.NotContains(t, strings.Join(silent.Log, "\n"), "trace_id")
}

// fakeAdmin accepts every request from the executor.
func fakeAdmin(t *testing.T) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"code":200}`))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func trigger(t *testing.T, h http.Handler, jobID, logID int64, handler string) {
	t.Helper()
	data, err := json.Marshal(map[string]any{"jobId": jobID, "logId": logID, "logDateTime": time.Now().UnixMilli(), "executorHandler": handler})
	require.NoError(t, err)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/run", bytes.NewReader(data)))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), `"code"`)
}

func TestInstrument(t *testing.T) {
	// Scenario: an instrumented executor runs one successful and one failed trigger, rejects one for an unknown
	//           handler, and has a job blocked with another trigger queued behind it
	// Expect: the counters report 3 accepted and 1 rejected triggers, 1 success and 1 failure; the up-down counters
	//         report 1 running and 1 queued (not just zeros); the executor is registered; everything carries the
	//         AppName; the runs were traced
	tm := newTelemetry()
	exec, err := xxljob.New(xxljob.Config{
		AdminAddresses:   []string{fakeAdmin(t).URL},
		AppName:          "otel-demo",
		Address:          "http://127.0.0.1:9999/",
		RegistryInterval: 20 * time.Millisecond,
	}, xxljob.WithoutServer(), xxljob.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	require.NoError(t, err)
	release := make(chan struct{})
	exec.MustRegister("Ok", xxljob.HandlerFunc(func(context.Context, *xxljob.Request) error { return nil }))
	exec.MustRegister("Fail", xxljob.HandlerFunc(func(context.Context, *xxljob.Request) error { return errors.New("x") }))
	exec.MustRegister("Block", xxljob.HandlerFunc(func(context.Context, *xxljob.Request) error { <-release; return nil }))
	reg, err := Instrument(exec, tm.opts...)
	require.NoError(t, err)
	require.NoError(t, exec.Start())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = exec.Shutdown(ctx)
	})

	trigger(t, exec.Handler(), 1, 1, "Ok")
	trigger(t, exec.Handler(), 2, 2, "Fail")
	trigger(t, exec.Handler(), 3, 3, "Missing")
	trigger(t, exec.Handler(), 4, 4, "Block")
	trigger(t, exec.Handler(), 4, 5, "Block")
	require.Eventually(t, func() bool {
		st := exec.Stats()
		return st.Registered && st.Succeeded+st.Failed == 2 && st.PendingCallbacks == 0 && st.Queued == 1
	}, 2*time.Second, 5*time.Millisecond)
	m := tm.metrics(t)
	close(release)
	require.NoError(t, reg.Unregister())

	sums := func(name string, key attribute.Key) map[string]int64 {
		out := map[string]int64{}
		for _, dp := range m[name].(metricdata.Sum[int64]).DataPoints {
			assert.Equal(t, "otel-demo", attrOf(dp.Attributes, AttrAppName))
			out[attrOf(dp.Attributes, key)] = dp.Value
		}
		return out
	}
	single := func(name string) metricdata.Sum[int64] {
		s := m[name].(metricdata.Sum[int64])
		require.Len(t, s.DataPoints, 1, name)
		assert.Equal(t, "otel-demo", attrOf(s.DataPoints[0].Attributes, AttrAppName))
		return s
	}

	assert.Equal(t, map[string]int64{"accepted": 4, "rejected": 1}, sums("xxljob.executor.triggers", AttrTrigger))
	assert.Equal(t, map[string]int64{ResultSuccess: 1, ResultFailure: 1, ResultTimeout: 0}, sums("xxljob.executor.results", AttrResult))
	assert.True(t, m["xxljob.executor.triggers"].(metricdata.Sum[int64]).IsMonotonic)
	running := single("xxljob.executor.running")
	assert.False(t, running.IsMonotonic, "an up-down counter")
	assert.Equal(t, int64(1), running.DataPoints[0].Value)
	assert.Equal(t, int64(1), single("xxljob.executor.queued").DataPoints[0].Value)
	assert.Equal(t, int64(0), single("xxljob.executor.pending_callbacks").DataPoints[0].Value)
	registered := m["xxljob.executor.registered"].(metricdata.Gauge[int64]).DataPoints
	require.Len(t, registered, 1)
	assert.Equal(t, int64(1), registered[0].Value)
	var names []string
	for _, s := range tm.spans.Ended() {
		names = append(names, s.Name())
	}
	assert.Subset(t, names, []string{"xxljob Ok", "xxljob Fail"})
}

func TestResultOf(t *testing.T) {
	// Scenario: classify runs by cause and error, including a handler change and a shutdown
	cause := func(c error) context.Context {
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(c)
		return ctx
	}

	assert.Equal(t, ResultHandlerChanged, resultOf(cause(xxljob.ErrHandlerChanged), nil))
	assert.Equal(t, ResultShutdown, resultOf(cause(xxljob.ErrShutdown), context.Canceled))
	assert.Equal(t, ResultFailure, resultOf(cause(context.Canceled), context.Canceled))
	assert.Equal(t, ResultSuccess, resultOf(context.Background(), nil))
}
