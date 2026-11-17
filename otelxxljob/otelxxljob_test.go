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
	return v.Emit()
}

func spanAttr(s sdktrace.ReadOnlySpan, key attribute.Key) string {
	for _, kv := range s.Attributes() {
		if kv.Key == key {
			return kv.Value.Emit()
		}
	}
	return ""
}

func TestMiddlewareRecordsRuns(t *testing.T) {
	// Scenario: runs with every kind of outcome go through the middleware
	// Given: handlers that succeed, fail, time out, get killed and panic
	// Expect: one span per run named "xxljob {handler}", with the job's attributes and its result; non-successful runs
	//         have status Error and a recorded error; a panic is re-raised (Invoke reports it as a failure); the
	//         duration histogram has one data point per handler and result
	tm := newTelemetry()
	mw := Middleware(tm.opts...)
	killCtx, kill := context.WithCancelCause(context.Background())
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
		{"Killed", killCtx, 0, func(ctx context.Context, _ *xxljob.Request) error {
			kill(xxljob.ErrKilled)
			return ctx.Err()
		}, ResultKilled},
		{"Panic", context.Background(), 0, func(context.Context, *xxljob.Request) error { panic("boom") }, ResultPanic},
	}

	for i, c := range cases {
		res := xxljob.Invoke(c.ctx, mw(c.h), &xxljob.Request{Handler: c.handler, JobID: int64(i + 1), LogID: int64(100 + i),
			Timeout: c.timeout, BlockStrategy: xxljob.BlockSerial, ShardIndex: 1, ShardTotal: 3})
		assert.Equal(t, c.result == ResultSuccess, res.Err == nil, c.handler)
	}
	spans := tm.spans.Ended()
	durations := tm.metrics(t)["xxljob.job.duration"].(metricdata.Histogram[float64])

	require.Len(t, spans, len(cases))
	for i, c := range cases {
		s := spans[i]
		assert.Equal(t, "xxljob "+c.handler, s.Name())
		assert.Equal(t, c.result, spanAttr(s, AttrResult), c.handler)
		assert.Equal(t, c.handler, spanAttr(s, AttrHandler))
		assert.Equal(t, string(rune('1'+i)), spanAttr(s, AttrJobID))
		assert.Equal(t, "1", spanAttr(s, AttrShardIndex))
		assert.Equal(t, "3", spanAttr(s, AttrShardTotal))
		assert.Equal(t, xxljob.BlockSerial, spanAttr(s, AttrBlockStrategy))
		if c.result == ResultSuccess {
			assert.Equal(t, codes.Unset, s.Status().Code)
			assert.Empty(t, s.Events())
		} else {
			assert.Equal(t, codes.Error, s.Status().Code, c.handler)
			require.Len(t, s.Events(), 1, c.handler)
			assert.Equal(t, "exception", s.Events()[0].Name)
		}
	}
	assert.Equal(t, "panic: boom", spans[4].Status().Description)
	require.Len(t, durations.DataPoints, len(cases))
	for _, dp := range durations.DataPoints {
		assert.Equal(t, uint64(1), dp.Count)
		assert.Contains(t, []string{"Ok", "Fail", "Slow", "Killed", "Panic"}, attrOf(dp.Attributes, AttrHandler))
	}
}

func TestMiddlewareLogsTraceID(t *testing.T) {
	// Scenario: the trace ID of a run is written to its execution log so it can be found from the admin
	// Given: the default options, and WithLogTraceID(false)
	// Expect: by default the execution log has a "trace" line with the span's trace ID; with the option it has none
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

func trigger(t *testing.T, h http.Handler, body map[string]any) {
	t.Helper()
	data, err := json.Marshal(body)
	require.NoError(t, err)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/run", bytes.NewReader(data)))
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestObserveStats(t *testing.T) {
	// Scenario: executor gauges and counters are exported through a meter
	// Given: an executor with the middleware installed; one successful run, one failed run and one trigger rejected
	//        for an unknown handler
	// Expect: the counters report 2 accepted and 1 rejected triggers, 1 success and 1 failure; the gauges report the
	//         executor as registered and idle; all carry the AppName; the middleware traced both runs
	tm := newTelemetry()
	exec, err := xxljob.New(xxljob.Config{
		AdminAddresses:   []string{fakeAdmin(t).URL},
		AppName:          "otel-demo",
		Address:          "http://127.0.0.1:9999/",
		RegistryInterval: 20 * time.Millisecond,
	}, xxljob.WithoutServer(), xxljob.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
		xxljob.WithMiddleware(Middleware(tm.opts...)))
	require.NoError(t, err)
	exec.MustRegister("Ok", xxljob.HandlerFunc(func(context.Context, *xxljob.Request) error { return nil }))
	exec.MustRegister("Fail", xxljob.HandlerFunc(func(context.Context, *xxljob.Request) error { return errors.New("x") }))
	reg, err := ObserveStats(exec, tm.opts...)
	require.NoError(t, err)
	require.NoError(t, exec.Start())
	t.Cleanup(func() { _ = exec.Shutdown(context.Background()) })

	now := time.Now().UnixMilli()
	trigger(t, exec.Handler(), map[string]any{"jobId": 1, "logId": 1, "logDateTime": now, "executorHandler": "Ok"})
	trigger(t, exec.Handler(), map[string]any{"jobId": 2, "logId": 2, "logDateTime": now, "executorHandler": "Fail"})
	trigger(t, exec.Handler(), map[string]any{"jobId": 3, "logId": 3, "logDateTime": now, "executorHandler": "Missing"})
	require.Eventually(t, func() bool {
		st := exec.Stats()
		return st.Registered && st.Succeeded+st.Failed == 2 && st.PendingCallbacks == 0
	}, 2*time.Second, 5*time.Millisecond)
	m := tm.metrics(t)
	require.NoError(t, reg.Unregister())

	sums := func(name string, key attribute.Key) map[string]int64 {
		out := map[string]int64{}
		for _, dp := range m[name].(metricdata.Sum[int64]).DataPoints {
			assert.Equal(t, "otel-demo", attrOf(dp.Attributes, AttrAppName))
			out[attrOf(dp.Attributes, key)] = dp.Value
		}
		return out
	}
	gauge := func(name string) int64 {
		dps := m[name].(metricdata.Gauge[int64]).DataPoints
		require.Len(t, dps, 1, name)
		assert.Equal(t, "otel-demo", attrOf(dps[0].Attributes, AttrAppName))
		return dps[0].Value
	}

	assert.Equal(t, map[string]int64{"accepted": 2, "rejected": 1}, sums("xxljob.executor.triggers", AttrTrigger))
	assert.Equal(t, map[string]int64{ResultSuccess: 1, ResultFailure: 1, ResultTimeout: 0}, sums("xxljob.executor.results", AttrResult))
	assert.True(t, m["xxljob.executor.triggers"].(metricdata.Sum[int64]).IsMonotonic)
	assert.Equal(t, int64(1), gauge("xxljob.executor.registered"))
	assert.Equal(t, int64(0), gauge("xxljob.executor.running"))
	assert.Equal(t, int64(0), gauge("xxljob.executor.queued"))
	assert.Equal(t, int64(0), gauge("xxljob.executor.pending_callbacks"))
	assert.Len(t, tm.spans.Ended(), 2)
}
