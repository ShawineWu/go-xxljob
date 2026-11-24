package report

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/ShawineWu/go-xxljob/internal/wire"
)

// Registry keeps the executor registered with the admin: it registers at once, renews every interval, and
// deregisters on Close. A heartbeat in progress is never interrupted: the admin may already be processing it, and
// one that landed after the deregistration would register the executor again until it expires.
type Registry struct {
	client   *Client
	log      *slog.Logger
	param    wire.RegistryParam
	interval time.Duration

	registered atomic.Bool
	stop       chan struct{} // closed by Close: stops the heartbeat between requests
	done       chan struct{}
}

// NewRegistry returns the heartbeat for appName at address.
func NewRegistry(client *Client, log *slog.Logger, appName, address string, interval time.Duration) *Registry {
	return &Registry{client: client, log: log, param: wire.NewRegistryParam(appName, address), interval: interval,
		stop: make(chan struct{}), done: make(chan struct{})}
}

// Start starts the heartbeat.
func (r *Registry) Start() { go r.loop() }

// Registered reports whether the last heartbeat succeeded.
func (r *Registry) Registered() bool { return r.registered.Load() }

// Close stops the heartbeat, waits for the one in progress (it ends within its request timeout, one per admin
// address), then deregisters. It gives up when ctx is done.
func (r *Registry) Close(ctx context.Context) {
	close(r.stop)
	select {
	case <-r.done:
	case <-ctx.Done():
	}
	r.registered.Store(false)
	if err := r.client.Post(ctx, wire.ActionRegistryRemove, r.param); err != nil {
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
		err := r.client.Post(context.Background(), wire.ActionRegistry, r.param)
		select {
		case <-r.stop:
			return
		default:
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
		case <-r.stop:
			return
		case <-tick.C:
		}
	}
}
