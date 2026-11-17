// Package otelxxljob adds OpenTelemetry tracing and metrics to a go-xxljob executor.
//
// It lives in its own module so the executor itself keeps depending on the standard library only.
//
//	exec.Use(otelxxljob.Middleware())                 // a span and a duration measurement per run
//	if _, err := otelxxljob.ObserveStats(exec); err != nil { ... } // executor gauges and counters
//
// Both use the global providers (otel.GetTracerProvider, otel.GetMeterProvider) unless WithTracerProvider or
// WithMeterProvider is given.
//
// The admin does not propagate trace context when it triggers a job, so every run starts a new trace. Spans created
// inside the job (database, HTTP, RPC clients) become its children. The run's trace ID is also written to the
// execution log, so it can be found from the admin's log view.
package otelxxljob

import (
	"context"
	"errors"
	"fmt"
	"time"

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

// Option configures Middleware and ObserveStats.
type Option func(*config)

type config struct {
	tp         trace.TracerProvider
	mp         metric.MeterProvider
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

// WithLogTraceID controls whether Middleware writes the run's trace ID to the execution log (and the application
// log) when a run starts. Default true.
func WithLogTraceID(enabled bool) Option {
	return func(c *config) { c.logTraceID = enabled }
}

// Middleware returns an xxljob.Middleware that, for every run:
//
//   - starts a span named "xxljob {handler}" (kind consumer) with the job's attributes, recording the error and
//     setting the status to Error when the run does not succeed;
//   - records the run's duration in the xxljob.job.duration histogram (seconds), by handler and result;
//   - writes the trace ID to the execution log (see WithLogTraceID).
//
// The result (AttrResult) distinguishes plain failures from panics, timeouts, kills and the other cancellation
// causes. Install it with Executor.Use or WithMiddleware; it should be the outermost middleware so the span covers
// the others. Panics are recorded and then re-raised for the executor to report.
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
				trace.WithSpanKind(trace.SpanKindConsumer), trace.WithAttributes(requestAttrs(req)...))
			if sc := span.SpanContext(); c.logTraceID && sc.IsValid() {
				xxljob.Logger(ctx).InfoContext(ctx, "trace", "trace_id", sc.TraceID().String(), "span_id", sc.SpanID().String())
			}
			start := time.Now()

			finish := func(result string, err error) {
				span.SetAttributes(AttrResult.String(result))
				if err != nil {
					span.RecordError(err)
					span.SetStatus(codes.Error, err.Error())
				}
				span.End()
				if duration != nil {
					duration.Record(context.WithoutCancel(ctx), time.Since(start).Seconds(),
						metric.WithAttributes(AttrHandler.String(req.Handler), AttrResult.String(result)))
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

func requestAttrs(req *xxljob.Request) []attribute.KeyValue {
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
	return attrs
}

// resultOf classifies a finished run by its error and the cancellation cause of its context.
func resultOf(ctx context.Context, err error) string {
	if err == nil {
		return ResultSuccess
	}
	cause := context.Cause(ctx)
	switch {
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
	}
	return ResultFailure
}

// ObserveStats registers instruments that report exec.Stats() on every collection, tagged with the executor's
// AppName:
//
//	xxljob.executor.running            gauge    running triggers
//	xxljob.executor.queued             gauge    queued triggers
//	xxljob.executor.pending_callbacks  gauge    results waiting to be reported (rising: the admin is unreachable)
//	xxljob.executor.registered         gauge    1 while the registry heartbeat succeeds, else 0
//	xxljob.executor.triggers           counter  triggers by xxljob.trigger: accepted / rejected
//	xxljob.executor.results            counter  reported results by xxljob.result: success / failure / timeout
//
// Unlike Middleware, the counters also see triggers that never run: rejected ones and queued ones discarded by a
// kill, a newer trigger or shutdown. Call Unregister on the returned registration to stop reporting.
func ObserveStats(exec *xxljob.Executor, opts ...Option) (metric.Registration, error) {
	c := newConfig(opts)
	meter := c.mp.Meter(ScopeName)
	appName := AttrAppName.String(exec.Config().AppName)
	app := metric.WithAttributes(appName)

	var errs []error
	gauge := func(name, desc, unit string) metric.Int64ObservableGauge {
		g, err := meter.Int64ObservableGauge(name, metric.WithDescription(desc), metric.WithUnit(unit))
		errs = append(errs, err)
		return g
	}
	counter := func(name, desc, unit string) metric.Int64ObservableCounter {
		g, err := meter.Int64ObservableCounter(name, metric.WithDescription(desc), metric.WithUnit(unit))
		errs = append(errs, err)
		return g
	}
	running := gauge("xxljob.executor.running", "Running triggers, including those waiting for a concurrency slot.", "{trigger}")
	queued := gauge("xxljob.executor.queued", "Queued triggers (serial execution).", "{trigger}")
	pending := gauge("xxljob.executor.pending_callbacks", "Results waiting to be reported to the admin.", "{result}")
	registered := gauge("xxljob.executor.registered", "1 while the executor is registered with the admin.", "1")
	triggers := counter("xxljob.executor.triggers", "Triggers received, by whether they were accepted.", "{trigger}")
	results := counter("xxljob.executor.results", "Results reported to the admin, by outcome.", "{result}")
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	accepted := metric.WithAttributes(appName, AttrTrigger.String("accepted"))
	rejected := metric.WithAttributes(appName, AttrTrigger.String("rejected"))
	outcome := func(result string) metric.ObserveOption {
		return metric.WithAttributes(appName, AttrResult.String(result))
	}
	success, failure, timeout := outcome(ResultSuccess), outcome(ResultFailure), outcome(ResultTimeout)

	return meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		st := exec.Stats()
		o.ObserveInt64(running, int64(st.Running), app)
		o.ObserveInt64(queued, int64(st.Queued), app)
		o.ObserveInt64(pending, int64(st.PendingCallbacks), app)
		o.ObserveInt64(registered, boolToInt(st.Registered), app)
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
