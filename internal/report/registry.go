package report

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/ShawineWu/go-xxljob/internal/wire"
)

// Registry keeps the executor registered with the admin: it registers at once, renews every interval, and
// deregisters on Close.
type Registry struct {
	client   *Client
	log      *slog.Logger
	param    wire.RegistryParam
	interval time.Duration

	registered atomic.Bool
	ctx        context.Context // cancelled by Close: interrupts an in-flight heartbeat
	cancel     context.CancelFunc
	done       chan struct{}
}

// NewRegistry returns the heartbeat for appName at address.
func NewRegistry(client *Client, log *slog.Logger, appName, address string, interval time.Duration) *Registry {
	ctx, cancel := context.WithCancel(context.Background())
	return &Registry{client: client, log: log, param: wire.NewRegistryParam(appName, address), interval: interval,
		ctx: ctx, cancel: cancel, done: make(chan struct{})}
}

// Start starts the heartbeat.
func (r *Registry) Start() { go r.loop() }

// Registered reports whether the last heartbeat succeeded.
func (r *Registry) Registered() bool { return r.registered.Load() }

// Close stops the heartbeat and deregisters, giving up when ctx is done.
func (r *Registry) Close(ctx context.Context) {
	r.cancel()
	<-r.done
	r.registered.Store(false)
	if err := r.client.Post(ctx, "registryRemove", r.param); err != nil {
		r.log.Error("xxljob: registryRemove failed", "app_name", r.param.RegistryKey, "address", r.param.RegistryValue, "err", err)
	}
}

// loop logs only state changes (failing ↔ recovered), so an admin outage does not flood the log.
func (r *Registry) loop() {
	defer close(r.done)
	tick := time.NewTicker(r.interval)
	defer tick.Stop()
	healthy := true
	for {
		err := r.client.Post(r.ctx, "registry", r.param)
		if r.ctx.Err() != nil {
			return
		}
		switch {
		case err != nil && healthy:
			r.log.Error("xxljob: registry failed", "app_name", r.param.RegistryKey, "address", r.param.RegistryValue, "err", err)
		case err == nil && !healthy:
			r.log.Info("xxljob: registry recovered", "app_name", r.param.RegistryKey, "address", r.param.RegistryValue)
		}
		healthy = err == nil
		r.registered.Store(healthy)
		select {
		case <-r.ctx.Done():
			return
		case <-tick.C:
		}
	}
}
