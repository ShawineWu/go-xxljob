package otelxxljob_test

import (
	"github.com/ShawineWu/go-xxljob"
	"github.com/ShawineWu/go-xxljob/otelxxljob"
)

// This example traces every run and exports the executor's metrics through the global OpenTelemetry providers (set
// them up with your exporter of choice first).
func Example() {
	exec, err := xxljob.New(xxljob.Config{
		AdminAddresses: []string{"http://xxl-job-admin:8080/xxl-job-admin"},
		AccessToken:    "default_token",
		AppName:        "demo-executor",
	})
	if err != nil {
		panic(err)
	}
	if _, err := otelxxljob.Instrument(exec); err != nil { // before Start and before other middlewares
		panic(err)
	}
	// register jobs, then exec.Run(ctx) as usual
}
