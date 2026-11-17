package xxljob

import (
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
)

// Default values.
const (
	DefaultPort                  = 9999
	DefaultRegistryInterval      = 30 * time.Second
	DefaultRequestTimeout        = 3 * time.Second
	DefaultCallbackRetryInterval = 10 * time.Second
	DefaultMaxPendingCallbacks   = 10000
	DefaultMaxQueueSize          = 1000
	DefaultCancelWait            = 3 * time.Second
	DefaultLongRunningThreshold  = time.Hour
)

// EnvPodIP is the environment variable read when Config.IP is empty (the conventional name for the Kubernetes
// Downward API). It is rarely needed: a standard pod has a single interface, which auto-detection picks. Set it
// when a pod has several interfaces (e.g. Multus).
const EnvPodIP = "POD_IP"

// Config configures an Executor. Zero values fall back to the defaults.
type Config struct {
	// AdminAddresses are the admin URLs, e.g. http://xxl-job-admin:8080/xxl-job-admin. At least one is required;
	// with several, they are tried in order (failover).
	AdminAddresses []string
	// AdminVersion selects the admin protocol: "2" (default, admin 2.3.0 ~ 3.4.x) or "3.5" (admin 3.5.0+). The two
	// protocols are incompatible, so switch it together with the admin upgrade.
	AdminVersion string
	// AccessToken must match the admin. For admin 2.3 ~ 3.4 it is the global xxl.job.accessToken and may be empty
	// (no authentication). For admin 3.5+ it is this executor's token in the admin console and is required. When
	// set, every request to the executor must carry it.
	AccessToken string
	// AppName is the executor's AppName in the admin ("Executor Management"), i.e. the registry key. Required.
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
	// MaxPendingCallbacks caps results waiting to be reported (including retries). Default 10000; beyond it the
	// oldest are dropped and an error is logged.
	MaxPendingCallbacks int
	// MaxQueueSize caps how many triggers of one job may wait in its queue (SERIAL_EXECUTION). Default 1000; when
	// full, new triggers are rejected.
	MaxQueueSize int
	// CancelWait is how long Shutdown waits for runs to return after cancelling them. Default 3s.
	CancelWait time.Duration

	// DefaultTimeout applies to runs the admin sends without a timeout (executorTimeout=0). 0 means no limit.
	// A stuck handler that ignores its context blocks later triggers of the same job; a fallback timeout bounds it.
	DefaultTimeout time.Duration
	// LongRunningThreshold logs a warning (application log and execution log) once a run exceeds it.
	// Default 1h; a negative value disables it.
	LongRunningThreshold time.Duration
	// MaxConcurrency caps runs executing at once across all jobs. 0 means no limit. Excess triggers wait for a
	// slot; while waiting they count as running and can be killed.
	MaxConcurrency int
}

// normalize validates the configuration and fills in defaults.
func (c Config) normalize() (Config, error) {
	addrs := make([]string, 0, len(c.AdminAddresses))
	for _, a := range c.AdminAddresses {
		a = strings.TrimRight(strings.TrimSpace(a), "/")
		if a == "" {
			continue
		}
		if err := checkHTTPURL(a); err != nil {
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
	if c.AdminVersion = strings.TrimSpace(c.AdminVersion); c.AdminVersion == "" {
		c.AdminVersion = AdminVersion2
	}
	if c.Port == 0 {
		c.Port = DefaultPort
	}
	if c.Port < 0 || c.Port > 65535 {
		return c, fmt.Errorf("xxljob: invalid port %d", c.Port)
	}
	if c.Address == "" {
		ip := c.IP
		if ip == "" {
			ip = strings.TrimSpace(os.Getenv(EnvPodIP))
		}
		if ip == "" {
			var err error
			if ip, err = localIPv4(); err != nil {
				return c, fmt.Errorf("xxljob: detect executor IP (set IP or Address): %w", err)
			}
		}
		c.Address = "http://" + net.JoinHostPort(ip, strconv.Itoa(c.Port)) + "/"
	}
	if err := checkHTTPURL(c.Address); err != nil {
		return c, fmt.Errorf("xxljob: invalid executor address %q: %w", c.Address, err)
	}
	if !strings.HasSuffix(c.Address, "/") {
		c.Address += "/" // the admin appends the action name to the address
	}
	c.RegistryInterval = orDefault(c.RegistryInterval, DefaultRegistryInterval)
	c.RequestTimeout = orDefault(c.RequestTimeout, DefaultRequestTimeout)
	c.CallbackRetryInterval = orDefault(c.CallbackRetryInterval, DefaultCallbackRetryInterval)
	c.CancelWait = orDefault(c.CancelWait, DefaultCancelWait)
	if c.MaxPendingCallbacks <= 0 {
		c.MaxPendingCallbacks = DefaultMaxPendingCallbacks
	}
	if c.MaxQueueSize <= 0 {
		c.MaxQueueSize = DefaultMaxQueueSize
	}
	if c.LongRunningThreshold == 0 {
		c.LongRunningThreshold = DefaultLongRunningThreshold
	}
	if c.DefaultTimeout < 0 || c.MaxConcurrency < 0 {
		return c, errors.New("xxljob: DefaultTimeout and MaxConcurrency must not be negative")
	}
	return c, nil
}

func orDefault(d, def time.Duration) time.Duration {
	if d <= 0 {
		return def
	}
	return d
}

func checkHTTPURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return err
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return errors.New("must be an http(s) URL with a host")
	}
	return nil
}

// Option configures dependencies that do not belong in a serializable Config.
type Option func(*options)

type options struct {
	logger        *slog.Logger
	logStore      LogStore
	httpClient    *http.Client
	middlewares   []Middleware
	withoutServer bool
}

// WithLogger sets the logger for the executor itself and for the application-log side of job logs.
// Default slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(o *options) { o.logger = l }
}

// WithLogStore sets where execution logs are stored. Default NewMemoryLogStore(0) (in memory, 16MB).
// The store is closed by Shutdown.
func WithLogStore(s LogStore) Option {
	return func(o *options) { o.logStore = s }
}

// WithHTTPClient sets the client used for requests to the admin. Default &http.Client{}. Each request is also
// bounded by Config.RequestTimeout.
func WithHTTPClient(c *http.Client) Option {
	return func(o *options) { o.httpClient = c }
}

// WithMiddleware adds middlewares applied to every handler, outermost first.
func WithMiddleware(mws ...Middleware) Option {
	return func(o *options) { o.middlewares = append(o.middlewares, mws...) }
}

// WithoutServer makes Start skip listening on the executor port: mount Executor.Handler on your own HTTP server
// and set Config.Address to the matching URL.
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
