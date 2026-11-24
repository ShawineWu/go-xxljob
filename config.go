package xxljob

import (
	"cmp"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ShawineWu/go-xxljob/internal/wire"
)

// Default values.
const (
	DefaultPort                  = 9999
	DefaultRegistryInterval      = 30 * time.Second
	DefaultRequestTimeout        = 3 * time.Second
	DefaultCallbackRetryInterval = 10 * time.Second
	DefaultMaxPendingCallbacks   = 10000
	DefaultMaxQueueSize          = 1000
	DefaultMaxActiveTriggers     = 2000
	DefaultCancelWait            = 3 * time.Second
	DefaultShutdownTimeout       = 15 * time.Second
	DefaultLongRunningThreshold  = time.Hour
)

// Admin protocol versions (Config.AdminVersion).
const (
	// AdminVersion2 is the protocol of XXL-JOB admin 2.3.0 – 3.4.x (the default).
	AdminVersion2 = wire.Version2
	// AdminVersion35 is the protocol of XXL-JOB admin 3.5.0 and later.
	AdminVersion35 = wire.Version35
)

// EnvPodIP is the environment variable read when Config.IP is empty (the conventional name for the Kubernetes
// Downward API). It is rarely needed: a standard pod has a single interface, which auto-detection picks.
const EnvPodIP = "POD_IP"

// Config configures an Executor. Zero values mean the defaults; negative values are rejected, except for
// LongRunningThreshold, where a negative value disables the warning.
type Config struct {
	// AdminAddresses are the admin URLs, e.g. http://xxl-job-admin:8080/xxl-job-admin. At least one is required.
	// With several, requests go to the one that answered last and fail over to the others.
	AdminAddresses []string
	// AdminVersion selects the admin protocol: "2" (default, admin 2.3.0 – 3.4.x) or "3.5" (admin 3.5.0+).
	AdminVersion string
	// AccessToken must match the admin. For admin 2.3 – 3.4 it is the global xxl.job.accessToken and may be empty,
	// in which case nothing on the executor port is authenticated. For admin 3.5+ it is this executor's token in the
	// admin console and is required.
	AccessToken string
	// AppName is the executor's AppName in the admin ("Executor Management"). Required.
	AppName string

	// IP is the IP registered with the admin. When empty, the POD_IP environment variable is used, then the first
	// up, non-loopback IPv4 address of this host.
	IP string
	// Port is the port the executor listens on. Default 9999.
	Port int
	// Address is the full URL registered with the admin, e.g. http://10.0.0.8:9999/. When empty it is
	// http://{IP}:{Port}/. Set it when the executor is mounted under a path (WithoutServer) or reached through NAT.
	Address string

	// RegistryInterval is the registry heartbeat interval. Default 30s; the admin drops executors silent for 90s.
	RegistryInterval time.Duration
	// RequestTimeout bounds each request to the admin (registry, deregistry, callback). Default 3s.
	RequestTimeout time.Duration
	// CallbackRetryInterval is the delay before retrying failed callbacks. Default 10s.
	CallbackRetryInterval time.Duration
	// MaxPendingCallbacks caps results waiting to be reported. Default 10000; beyond it (or beyond 64 MiB of
	// messages) the oldest are saved to CallbackDir, or dropped and logged when it is not set.
	MaxPendingCallbacks int
	// CallbackDir, when set, is where results that cannot be delivered (admin unreachable at shutdown, queue full)
	// are saved, up to 512 MiB; they are sent when the admin is reachable again, including by the next process using
	// the same directory. Use one directory per executor process. Without it they are lost, and the admin shows those
	// runs as running.
	CallbackDir string

	// MaxQueueSize caps how many triggers of one job may wait (SERIAL_EXECUTION). Default 1000.
	MaxQueueSize int
	// MaxActiveTriggers caps the triggers accepted but not finished (running, waiting for a slot or queued) across
	// all jobs. Default 2000. It bounds the executor's job goroutines and the log files open at once; see the
	// README's "Resource limits".
	MaxActiveTriggers int
	// MaxConcurrency caps runs executing at once across all jobs. 0 means no limit. Triggers waiting for a slot
	// count as running and can be killed; at shutdown they fail with ErrShutdown without running, like queued ones.
	MaxConcurrency int

	// DefaultTimeout applies to runs the admin sends without a timeout. 0 means no limit. At the timeout the run is
	// reported as timed out and its context is cancelled; a handler that ignores its context keeps its job busy.
	DefaultTimeout time.Duration
	// LongRunningThreshold logs a warning (application log and execution log) once a run exceeds it.
	// Default 1h; a negative value disables it.
	LongRunningThreshold time.Duration
	// ShutdownTimeout is how long Run lets running jobs finish when its context ends. Default 15s: with the other
	// defaults the whole shutdown then takes at most 25s, within Kubernetes' default 30s termination grace period.
	// It does not apply to Shutdown, which uses its own context.
	ShutdownTimeout time.Duration
	// CancelWait is how long shutdown waits for runs after cancelling them, and the least time it gives the last
	// callbacks. Default 3s.
	CancelWait time.Duration
}

// maxMessageBytes caps the memory held by pending callbacks, and maxSpoolBytes the disk used in CallbackDir.
const (
	maxMessageBytes = 64 << 20
	maxSpoolBytes   = 512 << 20
)

// normalize validates the configuration and fills in defaults.
func (c Config) normalize() (Config, error) {
	addrs := make([]string, 0, len(c.AdminAddresses))
	for _, a := range c.AdminAddresses {
		a = strings.TrimRight(strings.TrimSpace(a), "/")
		if a == "" {
			continue
		}
		if err := checkURL(a, false); err != nil {
			return c, fmt.Errorf("xxljob: invalid admin address %q: %w", a, err)
		}
		addrs = append(addrs, a)
	}
	if len(addrs) == 0 {
		return c, errors.New("xxljob: at least one admin address is required")
	}
	c.AdminAddresses = addrs
	if c.AppName = strings.TrimSpace(c.AppName); c.AppName == "" {
		return c, errors.New("xxljob: AppName is required")
	}
	c.AccessToken = strings.TrimSpace(c.AccessToken)
	c.AdminVersion = cmp.Or(strings.TrimSpace(c.AdminVersion), AdminVersion2)

	for _, f := range []struct {
		name string
		v    int64
	}{
		{"Port", int64(c.Port)}, {"MaxPendingCallbacks", int64(c.MaxPendingCallbacks)},
		{"MaxQueueSize", int64(c.MaxQueueSize)}, {"MaxActiveTriggers", int64(c.MaxActiveTriggers)},
		{"MaxConcurrency", int64(c.MaxConcurrency)}, {"RegistryInterval", int64(c.RegistryInterval)},
		{"RequestTimeout", int64(c.RequestTimeout)}, {"CallbackRetryInterval", int64(c.CallbackRetryInterval)},
		{"DefaultTimeout", int64(c.DefaultTimeout)}, {"ShutdownTimeout", int64(c.ShutdownTimeout)},
		{"CancelWait", int64(c.CancelWait)},
	} {
		if f.v < 0 {
			return c, fmt.Errorf("xxljob: %s must not be negative", f.name)
		}
	}
	c.Port = cmp.Or(c.Port, DefaultPort)
	if c.Port > 65535 {
		return c, fmt.Errorf("xxljob: invalid port %d", c.Port)
	}
	c.RegistryInterval = cmp.Or(c.RegistryInterval, DefaultRegistryInterval)
	c.RequestTimeout = cmp.Or(c.RequestTimeout, DefaultRequestTimeout)
	c.CallbackRetryInterval = cmp.Or(c.CallbackRetryInterval, DefaultCallbackRetryInterval)
	c.MaxPendingCallbacks = cmp.Or(c.MaxPendingCallbacks, DefaultMaxPendingCallbacks)
	c.MaxQueueSize = cmp.Or(c.MaxQueueSize, DefaultMaxQueueSize)
	c.MaxActiveTriggers = cmp.Or(c.MaxActiveTriggers, DefaultMaxActiveTriggers)
	c.ShutdownTimeout = cmp.Or(c.ShutdownTimeout, DefaultShutdownTimeout)
	c.CancelWait = cmp.Or(c.CancelWait, DefaultCancelWait)
	c.LongRunningThreshold = cmp.Or(c.LongRunningThreshold, DefaultLongRunningThreshold)

	if c.Address == "" {
		ip := cmp.Or(c.IP, strings.TrimSpace(os.Getenv(EnvPodIP)))
		if ip == "" {
			var err error
			if ip, err = localIPv4(); err != nil {
				return c, fmt.Errorf("xxljob: detect executor IP (set IP or Address): %w", err)
			}
		}
		c.Address = "http://" + net.JoinHostPort(ip, strconv.Itoa(c.Port)) + "/"
	}
	if err := checkURL(c.Address, true); err != nil {
		return c, fmt.Errorf("xxljob: invalid executor address %q: %w", c.Address, err)
	}
	if !strings.HasSuffix(c.Address, "/") {
		c.Address += "/" // the admin appends the action name to the address
	}
	return c, nil
}

// checkURL accepts plain http(s) URLs with a host: no credentials, query or fragment, which would end up in logs or
// break the paths appended to them.
func checkURL(raw string, allowPath bool) error {
	u, err := url.Parse(raw)
	switch {
	case err != nil:
		return err
	case (u.Scheme != "http" && u.Scheme != "https") || u.Host == "":
		return errors.New("must be an http(s) URL with a host")
	case u.User != nil || u.RawQuery != "" || u.Fragment != "":
		return errors.New("must not contain credentials, a query or a fragment")
	case !allowPath && strings.Contains(u.Path, "/api/"):
		return errors.New("must be the admin's base URL, without /api/")
	}
	return nil
}

// Option configures dependencies that do not belong in a serializable Config.
type Option func(*options)

type options struct {
	logger        *slog.Logger
	logStore      LogStore
	httpClient    *http.Client
	withoutServer bool
}

// WithLogger sets the logger for the executor itself and for the application side of job logs.
// Default slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(o *options) { o.logger = l }
}

// WithLogStore sets where execution logs are stored. Default NewMemoryLogStore(0) (in memory, 16 MiB). The
// executor closes the store on shutdown.
func WithLogStore(s LogStore) Option {
	return func(o *options) { o.logStore = s }
}

// WithHTTPClient sets the client used for requests to the admin. It is copied; the copy never follows redirects.
// Each request is bounded by Config.RequestTimeout.
func WithHTTPClient(c *http.Client) Option {
	return func(o *options) { o.httpClient = c }
}

// WithoutServer makes the executor skip listening on Config.Port: mount Executor.HTTPHandler on your own HTTP
// server, before calling Start, and set Config.Address (required) to the URL it is reachable at.
func WithoutServer() Option {
	return func(o *options) { o.withoutServer = true }
}

// localIPv4 returns the first IPv4 address of an up, non-loopback, non-link-local interface.
func localIPv4() (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for _, ifc := range ifaces {
		if ifc.Flags&net.FlagUp == 0 || ifc.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := ifc.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			if ipn, ok := a.(*net.IPNet); ok && ipn.IP.To4() != nil && !ipn.IP.IsLinkLocalUnicast() {
				return ipn.IP.String(), nil
			}
		}
	}
	return "", errors.New("no non-loopback IPv4 address found")
}
