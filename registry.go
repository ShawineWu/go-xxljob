package xxljob

import (
	"context"
	"time"
)

func (e *Executor) registryParam() registryParam {
	return registryParam{RegistryGroup: registryGroupExecutor, RegistryKey: e.cfg.AppName, RegistryValue: e.cfg.Address}
}

// registryLoop registers immediately and then renews every RegistryInterval until stopRegistry is closed.
// It only logs when the registry state changes (failing ↔ recovered), so an admin outage does not flood the log.
func (e *Executor) registryLoop() {
	defer close(e.registryDone)
	tick := time.NewTicker(e.cfg.RegistryInterval)
	defer tick.Stop()
	healthy := true
	for {
		err := e.admin.post(context.Background(), "registry", e.registryParam())
		switch {
		case err != nil && healthy:
			e.log.Error("xxljob: registry failed", "app_name", e.cfg.AppName, "address", e.cfg.Address, "err", err)
		case err == nil && !healthy:
			e.log.Info("xxljob: registry recovered", "app_name", e.cfg.AppName, "address", e.cfg.Address)
		}
		healthy = err == nil
		select {
		case <-e.stopRegistry:
			return
		case <-tick.C:
		}
	}
}

// deregister removes the executor from the admin, so no more triggers are routed to this instance.
func (e *Executor) deregister(ctx context.Context) {
	if err := e.admin.post(ctx, "registryRemove", e.registryParam()); err != nil {
		e.log.Error("xxljob: registryRemove failed", "app_name", e.cfg.AppName, "address", e.cfg.Address, "err", err)
	}
}
