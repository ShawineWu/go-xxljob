package xxljob_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ShawineWu/go-xxljob"
)

// This example creates an executor with a handler that decodes its parameters, logs to the execution log and honors
// cancellation, then runs it until the process is asked to stop.
func Example() {
	exec, err := xxljob.New(xxljob.Config{
		AdminAddresses: []string{"http://xxl-job-admin:8080/xxl-job-admin"},
		AccessToken:    "default_token",
		AppName:        "demo-executor",
	}, xxljob.WithLogger(slog.Default()))
	if err != nil {
		panic(err)
	}

	exec.MustRegister("Demo", xxljob.HandleJSON(func(ctx context.Context, _ *xxljob.Request, p struct{ Limit int }) error {
		xxljob.Logger(ctx).InfoContext(ctx, "processing", "limit", p.Limit) // visible in the admin's execution log
		for range p.Limit {
			if err := ctx.Err(); err != nil {
				return err // timed out, killed, or the executor is shutting down
			}
		}
		xxljob.SetResult(ctx, "done")
		return nil
	}))

	// Run until SIGINT/SIGTERM, then shut down gracefully within Config.ShutdownTimeout.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := exec.Run(ctx); err != nil {
		panic(err)
	}
}

// This example mounts the executor on a sub-path of an existing HTTP server instead of a port of its own.
func ExampleWithoutServer() {
	exec, err := xxljob.New(xxljob.Config{
		AdminAddresses: []string{"http://xxl-job-admin:8080/xxl-job-admin"},
		AccessToken:    "default_token",
		AppName:        "demo-executor",
		Address:        "http://10.0.0.8:8080/xxl-job/", // the URL the admin uses to reach this executor
	}, xxljob.WithoutServer())
	if err != nil {
		panic(err)
	}
	mux := http.NewServeMux()
	mux.Handle("/xxl-job/", http.StripPrefix("/xxl-job", exec.HTTPHandler())) // mount before starting
	srv := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.ListenAndServe() }()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	_ = exec.Run(ctx) // registration and callbacks; srv serves the admin's requests
	_ = srv.Shutdown(context.Background())
}

// This example applies middleware to every handler (Use) and to a single one (the arguments of Register).
func ExampleExecutor_Use() {
	exec, err := xxljob.New(xxljob.Config{
		AdminAddresses: []string{"http://xxl-job-admin:8080/xxl-job-admin"},
		AppName:        "demo-executor",
	})
	if err != nil {
		panic(err)
	}
	exec.Use(func(next xxljob.Handler) xxljob.Handler {
		return xxljob.HandlerFunc(func(ctx context.Context, req *xxljob.Request) error {
			start := time.Now()
			err := next.Handle(ctx, req)
			slog.InfoContext(ctx, "job done", "handler", req.Handler, "elapsed", time.Since(start), "err", err)
			return err
		})
	})
	weekdaysOnly := func(next xxljob.Handler) xxljob.Handler {
		return xxljob.HandlerFunc(func(ctx context.Context, req *xxljob.Request) error {
			if d := time.Now().Weekday(); d == time.Saturday || d == time.Sunday {
				return errors.New("skipped on weekends")
			}
			return next.Handle(ctx, req)
		})
	}
	exec.MustRegister("Report", xxljob.HandlerFunc(func(context.Context, *xxljob.Request) error { return nil }), weekdaysOnly)
}

// SyncItemsTask shows one type per job: dependencies come in through the struct, and Name returns the JobHandler
// configured in the admin.
type SyncItemsTask struct{ batchSize int }

// Name implements xxljob.Task.
func (t *SyncItemsTask) Name() string { return "SyncItems" }

// Handle implements xxljob.Handler.
func (t *SyncItemsTask) Handle(ctx context.Context, req *xxljob.Request) error {
	p := struct {
		BatchSize int `json:"batchSize"`
	}{BatchSize: t.batchSize} // kept when the job has no parameters
	if err := req.BindJSON(&p); err != nil {
		return err
	}
	xxljob.Logger(ctx).InfoContext(ctx, "syncing", "batch_size", p.BatchSize, "shard", req.ShardIndex)
	return nil
}

// This example registers jobs as Task types, in bulk.
func ExampleExecutor_RegisterTasks() {
	exec, err := xxljob.New(xxljob.Config{
		AdminAddresses: []string{"http://xxl-job-admin:8080/xxl-job-admin"},
		AppName:        "demo-executor",
	})
	if err != nil {
		panic(err)
	}
	exec.MustRegisterTasks(&SyncItemsTask{batchSize: 500})
}

// This example runs a handler once without an admin, as a unit test or a maintenance command would.
func ExampleInvoke() {
	greet := xxljob.HandleJSON(func(ctx context.Context, _ *xxljob.Request, p struct{ Name string }) error {
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
