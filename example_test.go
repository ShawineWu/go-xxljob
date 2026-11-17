package xxljob_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/ShawineWu/go-xxljob"
)

// This example creates an executor, registers a handler that decodes JSON parameters, writes to the execution log and
// honors cancellation, then starts the executor and shuts it down gracefully.
func Example() {
	exec, err := xxljob.New(xxljob.Config{
		AdminAddresses: []string{"http://xxl-job-admin:8080/xxl-job-admin"},
		AccessToken:    "default_token",
		AppName:        "demo-executor",
	}, xxljob.WithLogger(slog.Default()))
	if err != nil {
		panic(err)
	}

	exec.MustRegister("DemoJobHandler", xxljob.HandlerFunc(func(ctx context.Context, req *xxljob.Request) error {
		var p struct {
			Limit int `json:"limit"`
		}
		if err := req.BindJSON(&p); err != nil {
			return err // the admin marks the run as failed, raising alarms and retrying on failure
		}
		xxljob.Logger(ctx).Info("processing", "limit", p.Limit) // visible in the admin's execution log
		for i := range p.Limit {
			if err := ctx.Err(); err != nil {
				return err // timed out, killed, or the executor is shutting down
			}
			_ = i
		}
		xxljob.SetResult(ctx, "done")
		return nil
	}))

	if err := exec.Start(); err != nil {
		panic(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_ = exec.Shutdown(ctx)
}

// This example mounts the executor on a subpath of an existing HTTP server instead of listening on a port of its own.
func ExampleWithoutServer() {
	exec, err := xxljob.New(xxljob.Config{
		AdminAddresses: []string{"http://xxl-job-admin:8080/xxl-job-admin"},
		AppName:        "demo-executor",
		Address:        "http://10.0.0.8:8080/xxl-job/", // full URL the admin uses to reach the executor
	}, xxljob.WithoutServer())
	if err != nil {
		panic(err)
	}
	mux := http.NewServeMux()
	mux.Handle("/xxl-job/", http.StripPrefix("/xxl-job", exec.Handler()))
	if err := exec.Start(); err != nil { // starts only registration and callbacks; mux serves the requests
		panic(err)
	}
}

// This example applies middleware globally (WithMiddleware or Executor.Use) and to a single handler (the variadic
// arguments of Register).
func ExampleMiddleware() {
	timing := func(next xxljob.Handler) xxljob.Handler {
		return xxljob.HandlerFunc(func(ctx context.Context, req *xxljob.Request) error {
			start := time.Now()
			err := next.Handle(ctx, req)
			slog.Info("job done", "handler", req.Handler, "elapsed", time.Since(start), "err", err)
			return err
		})
	}
	exec, err := xxljob.New(xxljob.Config{
		AdminAddresses: []string{"http://xxl-job-admin:8080/xxl-job-admin"},
		AppName:        "demo-executor",
	}, xxljob.WithMiddleware(timing))
	if err != nil {
		panic(err)
	}
	onlyWeekdays := func(next xxljob.Handler) xxljob.Handler {
		return xxljob.HandlerFunc(func(ctx context.Context, req *xxljob.Request) error {
			if d := time.Now().Weekday(); d == time.Saturday || d == time.Sunday {
				return errors.New("skipped on weekends")
			}
			return next.Handle(ctx, req)
		})
	}
	exec.MustRegister("WeekdayJob", xxljob.HandlerFunc(func(context.Context, *xxljob.Request) error { return nil }), onlyWeekdays)
}

// SyncItemsTask illustrates one type per job: dependencies are injected through the constructor, and Name returns the
// JobHandler configured in the admin.
type SyncItemsTask struct {
	batchSize int
}

func (t *SyncItemsTask) Name() string { return "SyncItems" }

func (t *SyncItemsTask) Handle(ctx context.Context, req *xxljob.Request) error {
	p := struct {
		BatchSize int `json:"batchSize"`
	}{BatchSize: t.batchSize} // keeps the default when params are empty
	if err := req.BindJSON(&p); err != nil {
		return err
	}
	xxljob.Logger(ctx).Info("syncing", "batch_size", p.BatchSize, "shard", req.ShardIndex)
	return nil
}

// This example organizes jobs as Task types and registers them in bulk, which suits services with many jobs.
func ExampleTask() {
	exec, err := xxljob.New(xxljob.Config{
		AdminAddresses: []string{"http://xxl-job-admin:8080/xxl-job-admin"},
		AppName:        "demo-executor",
	})
	if err != nil {
		panic(err)
	}
	tasks := []xxljob.Task{
		&SyncItemsTask{batchSize: 500},
	}
	exec.MustRegisterTasks(tasks...)
}

// This example uses HandleJSON so that a function handler receives its parameters already decoded.
func ExampleHandleJSON() {
	exec, err := xxljob.New(xxljob.Config{
		AdminAddresses: []string{"http://xxl-job-admin:8080/xxl-job-admin"},
		AppName:        "demo-executor",
	})
	if err != nil {
		panic(err)
	}
	type cleanupParams struct {
		Days int `json:"days"`
	}
	exec.MustRegister("Cleanup", xxljob.HandleJSON(func(ctx context.Context, req *xxljob.Request, p cleanupParams) error {
		if p.Days <= 0 {
			return errors.New("days must be positive")
		}
		xxljob.SetResult(ctx, "cleaned")
		return nil
	}))
}

// This example runs a handler once without an admin, as a unit test or a maintenance command would.
func ExampleInvoke() {
	type params struct {
		Name string `json:"name"`
	}
	greet := xxljob.HandleJSON(func(ctx context.Context, _ *xxljob.Request, p params) error {
		if p.Name == "" {
			return errors.New("name is required")
		}
		xxljob.SetResult(ctx, "hello, "+p.Name)
		return nil
	})

	ok := xxljob.Invoke(context.Background(), greet, &xxljob.Request{Params: `{"name":"gopher"}`})
	failed := xxljob.Invoke(context.Background(), greet, nil)

	fmt.Println(ok.Code, ok.Msg)
	fmt.Println(failed.Code, failed.Err)
	// Output:
	// 200 hello, gopher
	// 500 name is required
}
