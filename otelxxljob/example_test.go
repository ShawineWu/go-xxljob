package otelxxljob_test

import (
	"github.com/ShawineWu/go-xxljob"
	"github.com/ShawineWu/go-xxljob/otelxxljob"
)

// This example traces every run and exports the executor's gauges and counters through the global OpenTelemetry
// providers (set them up with your exporter of choice before creating the executor).
func Example() {
	exec, err := xxljob.New(xxljob.Config{
		AdminAddresses: []string{"http://xxl-job-admin:8080/xxl-job-admin"},
		AppName:        "demo-executor",
	}, xxljob.WithMiddleware(otelxxljob.Middleware())) // outermost, so the span covers other middleware
	if err != nil {
		panic(err)
	}
	if _, err := otelxxljob.ObserveStats(exec); err != nil {
		panic(err)
	}
	// register jobs, Start, Shutdown as usual
}
