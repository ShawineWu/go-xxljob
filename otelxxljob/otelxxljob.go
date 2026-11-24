// Package otelxxljob adds OpenTelemetry tracing and metrics to a go-xxljob executor.
//
// It lives in its own module so the executor itself keeps depending on the standard library only. One call
// instruments an executor (before it starts):
//
//	if _, err := otelxxljob.Instrument(exec); err != nil { ... }
//
// Instrument installs Middleware (a span and a duration measurement per run) and ObserveStats (the executor's
// gauges and counters), both tagged with the executor's AppName. They use the global providers
// (otel.GetTracerProvider, otel.GetMeterProvider) unless WithTracerProvider or WithMeterProvider is given.
//
// The admin does not propagate trace context when it triggers a job, so every run starts a new trace. Spans created
// inside the job (database, HTTP, RPC clients) become its children. The run's trace ID is also written to the
// execution log, so a run in the admin's log view leads to its trace.
package otelxxljob

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/ShawineWu/go-xxljob"
)

// ScopeName is the instrumentation scope of the tracer and meter.
const ScopeName = "github.com/ShawineWu/go-xxljob/otelxxljob"

// Attribute keys.
const (
	AttrHandler       = attribute.Key("xxljob.handler")
	AttrJobID         = attribute.Key("xxljob.job_id")
	AttrLogID         = attribute.Key("xxljob.log_id")
	AttrBlockStrategy = attribute.Key("xxljob.block_strategy")
	AttrShardIndex    = attribute.Key("xxljob.shard.index")
	AttrShardTotal    = attribute.Key("xxljob.shard.total")
	AttrResult        = attribute.Key("xxljob.result")
	AttrAppName       = attribute.Key("xxljob.app_name")
	AttrTrigger       = attribute.Key("xxljob.trigger")
)

// Values of AttrResult on spans and the duration histogram.
const (
	ResultSuccess        = "success"
	ResultFailure        = "failure"
	ResultPanic          = "panic"
	ResultTimeout        = "timeout"
	ResultKilled         = "killed"
	ResultCovered        = "covered"
	ResultHandlerChanged = "handler_changed"
	ResultShutdown       = "shutdown"
)

// maxStatusDescription caps the error text put on a span: errors can be long and may carry data from job parameters.
const maxStatusDescription = 512

// Option configures Instrument, Middleware and ObserveStats.
type Option func(*config)

type config struct {
	tp         trace.TracerProvider
	mp         metric.MeterProvider
	attrs      []attribute.KeyValue
	logTraceID bool
}

func newConfig(opts []Option) config {
	c := config{logTraceID: true}
	for _, o := range opts {
		o(&c)
	}
	if c.tp == nil {
		c.tp = otel.GetTracerProvider()
	}
	if c.mp == nil {
		c.mp = otel.GetMeterProvider()
	}
	return c
}

// WithTracerProvider sets the tracer provider. Default otel.GetTracerProvider().
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(c *config) { c.tp = tp }
}

// WithMeterProvider sets the meter provider. Default otel.GetMeterProvider().
func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(c *config) { c.mp = mp }
}

// WithAttributes adds attributes to every span and measurement. Keep them low-cardinality.
func WithAttributes(attrs ...attribute.KeyValue) Option {
	return func(c *config) { c.attrs = append(c.attrs, attrs...) }
}

// WithLogTraceID controls whether Middleware writes the run's trace ID to the execution log (and the application
// log) when a run starts. Default true.
func WithLogTraceID(enabled bool) Option {
	return func(c *config) { c.logTraceID = enabled }
}

// Instrument installs Middleware on exec and registers ObserveStats, both tagged with exec's AppName. Call it before
// Start, and before adding other middlewares with Use if the span should cover them: the middleware applies to
// runs that start afterwards, around the middlewares added after it.
func Instrument(exec *xxljob.Executor, opts ...Option) (metric.Registration, error) {
	opts = append(slices.Clone(opts), WithAttributes(AttrAppName.String(exec.Config().AppName)))
	reg, err := ObserveStats(exec, opts...)
	if err != nil {
		return nil, err
	}
	exec.Use(Middleware(opts...))
	return reg, nil
}

// Middleware returns an xxljob.Middleware that, for every run:
//
//   - starts a span named "xxljob {handler}" (kind consumer) with the job's attributes, recording the error and
//     setting the status to Error when the run does not succeed;
//   - records the run's duration in the xxljob.job.duration histogram (seconds), by handler and result;
//   - writes the trace ID to the execution log (see WithLogTraceID).
//
// The result (AttrResult) tells plain failures from panics, timeouts, kills and the other cancellation causes.
// Panics are recorded and then re-raised for the executor to report. Prefer Instrument, which also tags everything
// with the executor's AppName.
func Middleware(opts ...Option) xxljob.Middleware {
	c := newConfig(opts)
	tracer := c.tp.Tracer(ScopeName)
	duration, err := c.mp.Meter(ScopeName).Float64Histogram("xxljob.job.duration",
		metric.WithDescription("Duration of job runs."),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 300, 600, 1800, 3600))
	if err != nil {
		otel.Handle(err)
	}

	return func(next xxljob.Handler) xxljob.Handler {
		return xxljob.HandlerFunc(func(ctx context.Context, req *xxljob.Request) (err error) {
			ctx, span := tracer.Start(ctx, "xxljob "+req.Handler,
				trace.WithSpanKind(trace.SpanKindConsumer), trace.WithAttributes(requestAttrs(req, c.attrs)...))
			if sc := span.SpanContext(); c.logTraceID && sc.IsValid() {
				xxljob.Logger(ctx).InfoContext(ctx, "trace", "trace_id", sc.TraceID().String(), "span_id", sc.SpanID().String())
			}
			start := time.Now()

			finish := func(result string, err error) {
				span.SetAttributes(AttrResult.String(result))
				if err != nil {
					span.RecordError(err)
					span.SetStatus(codes.Error, clip(err.Error(), maxStatusDescription))
				}
				span.End()
				if duration != nil {
					attrs := append([]attribute.KeyValue{AttrHandler.String(req.Handler), AttrResult.String(result)}, c.attrs...)
					duration.Record(context.WithoutCancel(ctx), time.Since(start).Seconds(), metric.WithAttributes(attrs...))
				}
			}
			defer func() {
				if p := recover(); p != nil {
					finish(ResultPanic, fmt.Errorf("panic: %v", p))
					panic(p)
				}
			}()

			err = next.Handle(ctx, req)
			finish(resultOf(ctx, err), err)
			return err
		})
	}
}

func requestAttrs(req *xxljob.Request, extra []attribute.KeyValue) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		AttrHandler.String(req.Handler),
		AttrJobID.Int64(req.JobID),
		AttrLogID.Int64(req.LogID),
	}
	if req.BlockStrategy != "" {
		attrs = append(attrs, AttrBlockStrategy.String(req.BlockStrategy))
	}
	if req.ShardTotal > 1 {
		attrs = append(attrs, AttrShardIndex.Int(req.ShardIndex), AttrShardTotal.Int(req.ShardTotal))
	}
	return append(attrs, extra...)
}

// resultOf classifies a finished run by the cancellation cause of its context, which wins over what the handler
// returned (as it does for the result the executor reports), then by its error. It sees the run as the handler
// returned: a cancellation arriving after that, which the executor may still report, is not reflected.
func resultOf(ctx context.Context, err error) string {
	switch cause := context.Cause(ctx); {
	case errors.Is(cause, xxljob.ErrTimeout):
		return ResultTimeout
	case errors.Is(cause, xxljob.ErrKilled):
		return ResultKilled
	case errors.Is(cause, xxljob.ErrCovered):
		return ResultCovered
	case errors.Is(cause, xxljob.ErrHandlerChanged):
		return ResultHandlerChanged
	case errors.Is(cause, xxljob.ErrShutdown):
		return ResultShutdown
	case err != nil:
		return ResultFailure
	}
	return ResultSuccess
}

// ObserveStats registers instruments that report exec.Stats() on every collection:
//
//	xxljob.executor.running            up-down counter  jobs with a running trigger
//	xxljob.executor.queued             up-down counter  queued triggers
//	xxljob.executor.pending_callbacks  up-down counter  results not yet delivered (rising: the admin is unreachable)
//	xxljob.executor.registered         gauge            1 while the registry heartbeat succeeds, else 0
//	xxljob.executor.triggers           counter          triggers by xxljob.trigger: accepted / rejected
//	xxljob.executor.results            counter          reported results by xxljob.result: success / failure / timeout
//
// Unlike Middleware, the counters also see triggers that never run: rejected ones and queued ones discarded by a
// kill, a newer trigger or shutdown. Call Unregister on the returned registration to stop reporting.
func ObserveStats(exec *xxljob.Executor, opts ...Option) (metric.Registration, error) {
	c := newConfig(opts)
	meter := c.mp.Meter(ScopeName)
	with := func(extra ...attribute.KeyValue) metric.ObserveOption {
		return metric.WithAttributes(append(slices.Clone(c.attrs), extra...)...)
	}

	var errs []error
	upDown := func(name, desc, unit string) metric.Int64ObservableUpDownCounter {
		i, err := meter.Int64ObservableUpDownCounter(name, metric.WithDescription(desc), metric.WithUnit(unit))
		errs = append(errs, err)
		return i
	}
	running := upDown("xxljob.executor.running", "Jobs with a running trigger, including triggers waiting for a slot.", "{trigger}")
	queued := upDown("xxljob.executor.queued", "Queued triggers (serial execution).", "{trigger}")
	pending := upDown("xxljob.executor.pending_callbacks", "Results not yet delivered to the admin.", "{result}")
	registered, err := meter.Int64ObservableGauge("xxljob.executor.registered",
		metric.WithDescription("1 while the executor is registered with the admin."), metric.WithUnit("1"))
	errs = append(errs, err)
	triggers, err := meter.Int64ObservableCounter("xxljob.executor.triggers",
		metric.WithDescription("Triggers received, by whether they were accepted."), metric.WithUnit("{trigger}"))
	errs = append(errs, err)
	results, err := meter.Int64ObservableCounter("xxljob.executor.results",
		metric.WithDescription("Results reported to the admin, by outcome."), metric.WithUnit("{result}"))
	errs = append(errs, err)
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	plain := with()
	accepted, rejected := with(AttrTrigger.String("accepted")), with(AttrTrigger.String("rejected"))
	success, failure := with(AttrResult.String(ResultSuccess)), with(AttrResult.String(ResultFailure))
	timeout := with(AttrResult.String(ResultTimeout))

	return meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		st := exec.Stats()
		o.ObserveInt64(running, int64(st.Running), plain)
		o.ObserveInt64(queued, int64(st.Queued), plain)
		o.ObserveInt64(pending, int64(st.PendingCallbacks), plain)
		o.ObserveInt64(registered, boolToInt(st.Registered), plain)
		o.ObserveInt64(triggers, st.Accepted, accepted)
		o.ObserveInt64(triggers, st.Rejected, rejected)
		o.ObserveInt64(results, st.Succeeded, success)
		o.ObserveInt64(results, st.Failed, failure)
		o.ObserveInt64(results, st.TimedOut, timeout)
		return nil
	}, running, queued, pending, registered, triggers, results)
}

func boolToInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// clip shortens s to at most n bytes, on a rune boundary.
func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n] + "..."
}
