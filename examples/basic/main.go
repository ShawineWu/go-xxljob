// Command basic runs a standalone XXL-JOB executor with a few demo jobs.
//
//	XXL_JOB_ADMIN=http://127.0.0.1:8080/xxl-job-admin XXL_JOB_TOKEN=default_token go run ./examples/basic
//
// In the admin, create an executor with AppName "go-demo" (auto registration), then add jobs that use the BEAN
// glue type and one of the JobHandlers below: Hello, SyncItems (accepts {"batchSize":100}) or Shard (route with
// SHARDING_BROADCAST).
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/ShawineWu/go-xxljob"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(logger); err != nil {
		logger.Error("executor failed", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	dataDir := envOr("XXL_JOB_DATA_DIR", filepath.Join(os.TempDir(), "go-xxljob"))
	logs, err := xxljob.NewFileLogStore(xxljob.FileLogStoreConfig{Dir: filepath.Join(dataDir, "logs"), RetentionDays: 7})
	if err != nil {
		return err
	}
	exec, err := xxljob.New(xxljob.Config{
		AdminAddresses: []string{envOr("XXL_JOB_ADMIN", "http://127.0.0.1:8080/xxl-job-admin")},
		AdminVersion:   envOr("XXL_JOB_ADMIN_VERSION", xxljob.AdminVersion2), // "3.5" for admin 3.5.0+
		AccessToken:    os.Getenv("XXL_JOB_TOKEN"),
		AppName:        envOr("XXL_JOB_APPNAME", "go-demo"),
		DefaultTimeout: 10 * time.Minute,                    // bounds jobs configured without a timeout
		CallbackDir:    filepath.Join(dataDir, "callbacks"), // results undelivered at shutdown are sent by the next run
	}, xxljob.WithLogger(logger), xxljob.WithLogStore(logs), xxljob.WithMiddleware(recordDuration(logger)))
	if err != nil {
		return err
	}

	exec.MustRegister("Hello", xxljob.HandlerFunc(hello))
	exec.MustRegisterTasks(&SyncItemsTask{batchSize: 500}, ShardTask{})

	// Run until SIGINT/SIGTERM, then give running jobs ShutdownTimeout (15s by default) to finish; after that they
	// are cancelled with xxljob.ErrShutdown.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return exec.Run(ctx)
}

// hello is the smallest possible handler: everything logged through xxljob.Logger shows up in the admin's
// execution log, and SetResult becomes the run's note.
func hello(ctx context.Context, req *xxljob.Request) error {
	xxljob.Logger(ctx).InfoContext(ctx, "hello from Go", "params", req.Params)
	xxljob.SetResult(ctx, "said hello")
	return nil
}

// SyncItemsTask shows the one-type-per-job style: dependencies come in through the struct, defaults can be
// overridden by JSON parameters, and the loop stops as soon as the run is cancelled.
type SyncItemsTask struct {
	batchSize int
}

// Name implements xxljob.Task.
func (t *SyncItemsTask) Name() string { return "SyncItems" }

// Handle implements xxljob.Handler.
func (t *SyncItemsTask) Handle(ctx context.Context, req *xxljob.Request) error {
	p := struct {
		BatchSize int `json:"batchSize"`
	}{BatchSize: t.batchSize}
	if err := req.BindJSON(&p); err != nil {
		return err
	}
	log := xxljob.Logger(ctx)
	for batch := 1; batch <= 5; batch++ {
		select {
		case <-ctx.Done():
			// killed, timed out, covered by a newer trigger or shutting down
			return fmt.Errorf("stopped at batch %d: %w", batch, context.Cause(ctx))
		case <-time.After(time.Second):
		}
		log.InfoContext(ctx, "synced batch", "batch", batch, "size", p.BatchSize)
	}
	xxljob.SetResult(ctx, fmt.Sprintf("synced %d items", 5*p.BatchSize))
	return nil
}

// ShardTask processes its slice of the data when the job is routed with SHARDING_BROADCAST.
type ShardTask struct{}

// Name implements xxljob.Task.
func (ShardTask) Name() string { return "Shard" }

// Handle implements xxljob.Handler.
func (ShardTask) Handle(ctx context.Context, req *xxljob.Request) error {
	if req.ShardTotal <= 0 {
		return errors.New("invalid shard total")
	}
	xxljob.Logger(ctx).InfoContext(ctx, "processing ids where id % total == index", "index", req.ShardIndex, "total", req.ShardTotal)
	return nil
}

// recordDuration is a global middleware; replace the log line with a metrics call in a real service.
func recordDuration(logger *slog.Logger) xxljob.Middleware {
	return func(next xxljob.Handler) xxljob.Handler {
		return xxljob.HandlerFunc(func(ctx context.Context, req *xxljob.Request) error {
			start := time.Now()
			err := next.Handle(ctx, req)
			logger.InfoContext(ctx, "job finished", "handler", req.Handler, "log_id", req.LogID, "elapsed", time.Since(start), "err", err)
			return err
		})
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
