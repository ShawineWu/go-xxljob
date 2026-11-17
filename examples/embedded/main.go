// Command embedded serves the executor from an existing HTTP server instead of a dedicated port, which is useful
// when a service may expose only one port.
//
//	XXL_JOB_ADMIN=http://127.0.0.1:8080/xxl-job-admin EXECUTOR_ADDRESS=http://<this-host-ip>:8081/xxl-job/ go run ./examples/embedded
//
// EXECUTOR_ADDRESS must be reachable from the admin; the admin appends run, kill, beat, ... to it.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ShawineWu/go-xxljob"
)

func main() {
	if err := run(); err != nil {
		slog.Error("server failed", "err", err)
		os.Exit(1)
	}
}

func run() error {
	exec, err := xxljob.New(xxljob.Config{
		AdminAddresses: []string{envOr("XXL_JOB_ADMIN", "http://127.0.0.1:8080/xxl-job-admin")},
		AccessToken:    os.Getenv("XXL_JOB_TOKEN"),
		AppName:        envOr("XXL_JOB_APPNAME", "go-demo"),
		Address:        envOr("EXECUTOR_ADDRESS", "http://127.0.0.1:8081/xxl-job/"),
	}, xxljob.WithoutServer())
	if err != nil {
		return err
	}
	exec.MustRegister("Hello", xxljob.HandlerFunc(func(ctx context.Context, req *xxljob.Request) error {
		xxljob.Logger(ctx).Info("hello from an embedded executor")
		return nil
	}))

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	mux.Handle("/xxl-job/", http.StripPrefix("/xxl-job", exec.Handler()))
	srv := &http.Server{Addr: ":8081", Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	if err := exec.Start(); err != nil { // registry and callbacks only; srv serves the admin's requests
		return err
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("http server stopped", "err", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Stop the executor first (it still needs the HTTP server while jobs finish), then the server.
	err = exec.Shutdown(shutdownCtx)
	return errors.Join(err, srv.Shutdown(shutdownCtx))
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
