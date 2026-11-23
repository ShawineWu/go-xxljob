package report

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/ShawineWu/go-xxljob/internal/ratelog"
	"github.com/ShawineWu/go-xxljob/internal/wire"
)

// batchSize caps the results sent in one callback request.
const batchSize = 100

// CallbackConfig configures Callbacks.
type CallbackConfig struct {
	RetryInterval time.Duration // delay before retrying after a failed request
	MaxPending    int           // results held in memory
	MaxBytes      int           // approximate memory held by pending results
	Dir           string        // when set, undeliverable results are saved here and sent after a restart
}

// Callbacks reports run results to the admin. Results are queued and sent in batches by a background goroutine;
// failed batches are retried. The queue is bounded by count and size: beyond either limit the oldest results are
// saved to Dir (or dropped when there is none). Results still undelivered at Close are saved too, and loaded again
// by the next process using the same Dir.
type Callbacks struct {
	client *Client
	log    *slog.Logger
	cfg    CallbackConfig
	spool  *spool // nil without Dir
	warn   *ratelog.Limiter

	mu          sync.Mutex
	pending     []wire.Result
	bytes       int
	inflight    int
	lastFailure time.Time
	closed      bool

	notify chan struct{}
	ctx    context.Context // cancelled by Close: stops the sender between requests
	cancel context.CancelFunc
	done   chan struct{}
}

// NewCallbacks returns the callback sender. With a Dir, results saved by a previous process are queued.
func NewCallbacks(client *Client, log *slog.Logger, cfg CallbackConfig) (*Callbacks, error) {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Callbacks{client: client, log: log, cfg: cfg, warn: ratelog.New(time.Minute),
		notify: make(chan struct{}, 1), ctx: ctx, cancel: cancel, done: make(chan struct{})}
	if cfg.Dir != "" {
		sp, err := newSpool(cfg.Dir)
		if err != nil {
			cancel()
			return nil, err
		}
		c.spool = sp
		c.reload()
	}
	return c, nil
}

// Start starts the background sender.
func (c *Callbacks) Start() { go c.loop() }

// Push queues a result. It never blocks on the network.
func (c *Callbacks) Push(r wire.Result) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		c.lose([]wire.Result{r}, "result arrived after shutdown")
		return
	}
	c.pending = append(c.pending, r)
	c.bytes += size(r)
	overflow := c.trim()
	c.mu.Unlock()
	if len(overflow) > 0 {
		c.lose(overflow, "callback queue full")
	}
	select {
	case c.notify <- struct{}{}:
	default:
	}
}

// Pending is the number of results not yet delivered, including the batch being sent.
func (c *Callbacks) Pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pending) + c.inflight
}

// Close stops the sender (a request in progress finishes within its timeout), then delivers the remaining results
// until ctx is done; the last request may outlast ctx by one request timeout per admin address. Whatever is left is
// saved to Dir (or logged as lost). Results pushed afterwards are saved or logged the same way. ctx must have a
// deadline: with an unreachable admin, Close retries until it is done.
func (c *Callbacks) Close(ctx context.Context) {
	c.cancel()
	<-c.done
	for {
		c.flush(ctx)
		c.mu.Lock()
		if len(c.pending) == 0 {
			c.closed = true
			c.mu.Unlock()
			return
		}
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			c.mu.Lock()
			left := c.pending
			c.pending, c.bytes, c.closed = nil, 0, true
			c.mu.Unlock()
			c.lose(left, "results not delivered before shutdown")
			return
		case <-time.After(min(c.cfg.RetryInterval, time.Second)):
		}
	}
}

func (c *Callbacks) loop() {
	defer close(c.done)
	tick := time.NewTicker(c.cfg.RetryInterval)
	defer tick.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.notify:
			// After a failure, wait for the retry interval instead of hitting an unavailable admin on every result.
			c.mu.Lock()
			backoff := time.Since(c.lastFailure) < c.cfg.RetryInterval
			c.mu.Unlock()
			if !backoff {
				c.flush(c.ctx)
			}
		case <-tick.C:
			c.flush(c.ctx)
			c.reload()
		}
	}
}

// flush sends the queued results in batches until the queue is empty, a request fails or ctx is done. ctx is
// checked between batches only: a request in progress is bounded by its own timeout rather than interrupted, since
// the admin may already have processed it and a retry would report the results twice. A failed batch goes back to
// the head of the queue.
func (c *Callbacks) flush(ctx context.Context) {
	for ctx.Err() == nil {
		c.mu.Lock()
		n := min(len(c.pending), batchSize)
		if n == 0 {
			c.mu.Unlock()
			return
		}
		batch := c.pending[:n:n]
		c.pending = c.pending[n:]
		c.inflight = n
		c.mu.Unlock()

		err := c.client.Post(context.WithoutCancel(ctx), "callback", c.client.proto.CallbackBody(batch))

		c.mu.Lock()
		c.inflight = 0
		if err != nil {
			c.pending = append(batch, c.pending...)
			c.lastFailure = time.Now()
			pending := len(c.pending)
			c.mu.Unlock()
			if ok, n := c.warn.Allow(); ok && ctx.Err() == nil {
				c.log.Error("xxljob: callback failed, will retry", "pending", pending, "err", err, "suppressed", n)
			}
			return
		}
		for _, r := range batch {
			c.bytes -= size(r)
		}
		c.mu.Unlock()
	}
}

// trim removes the oldest results beyond the limits, a tenth of the limit at a time so that a full queue does not
// copy itself on every push. Called with c.mu held.
func (c *Callbacks) trim() []wire.Result {
	if len(c.pending) <= c.cfg.MaxPending && c.bytes <= c.cfg.MaxBytes {
		return nil
	}
	n := 0
	for len(c.pending)-n > c.cfg.MaxPending*9/10 || c.bytes > c.cfg.MaxBytes*9/10 {
		if n == len(c.pending) {
			break
		}
		c.bytes -= size(c.pending[n])
		n++
	}
	out := append([]wire.Result(nil), c.pending[:n]...)
	c.pending = append([]wire.Result(nil), c.pending[n:]...)
	return out
}

// reload queues results saved by an earlier process (or overflow) once the queue has drained.
func (c *Callbacks) reload() {
	if c.spool == nil {
		return
	}
	c.mu.Lock()
	busy := len(c.pending) > 0 || c.closed
	c.mu.Unlock()
	if busy {
		return
	}
	results, err := c.spool.load()
	if err != nil {
		c.log.Error("xxljob: load saved callbacks", "dir", c.cfg.Dir, "err", err)
	}
	if len(results) == 0 {
		return
	}
	c.log.Info("xxljob: sending saved callbacks", "count", len(results))
	for _, r := range results {
		c.Push(r)
	}
}

// lose saves results to the spool, or logs them as lost when there is none (or saving fails).
func (c *Callbacks) lose(results []wire.Result, why string) {
	if c.spool != nil {
		err := c.spool.save(results)
		if err == nil {
			c.log.Warn("xxljob: "+why+", results saved for later delivery", "count", len(results), "dir", c.cfg.Dir)
			return
		}
		c.log.Error("xxljob: save callbacks", "dir", c.cfg.Dir, "err", err)
	}
	c.log.Error("xxljob: "+why+", results lost (the admin keeps these runs as running)", "count", len(results),
		"first_log_id", results[0].LogID, "last_log_id", results[len(results)-1].LogID)
}

// size approximates the memory a result holds.
func size(r wire.Result) int { return 64 + len(r.Msg) }
