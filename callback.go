package xxljob

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// callbackBatch caps the results sent in one callback request.
const callbackBatch = 100

// callbacker reports run results. Results are queued when a run ends and sent in batches by a background
// goroutine; failed batches stay queued and are retried after the retry interval. The queue is bounded: beyond the
// limit the oldest results are dropped (the admin marks them as lost after about 10 minutes).
type callbacker struct {
	admin         *adminClient
	log           *slog.Logger
	retryInterval time.Duration
	max           int

	mu          sync.Mutex
	pending     []result
	lastFailure time.Time
	closed      bool

	notify chan struct{}
	stop   chan struct{}
	done   chan struct{}
}

func newCallbacker(admin *adminClient, log *slog.Logger, c Config) *callbacker {
	return &callbacker{
		admin:         admin,
		log:           log,
		retryInterval: c.CallbackRetryInterval,
		max:           c.MaxPendingCallbacks,
		notify:        make(chan struct{}, 1),
		stop:          make(chan struct{}),
		done:          make(chan struct{}),
	}
}

// push queues a result and wakes the sender. It never blocks.
func (c *callbacker) push(p result) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		// A run that outlived the shutdown grace period: its result can no longer be delivered, and the admin will
		// mark it as lost after about 10 minutes.
		c.log.Error("xxljob: result arrived after shutdown, not delivered", "log_id", p.LogID, "code", p.Code, "msg", p.Msg)
		return
	}
	c.pending = append(c.pending, p)
	if over := len(c.pending) - c.max; over > 0 {
		dropped := c.pending[:over]
		c.pending = append([]result(nil), c.pending[over:]...)
		c.log.Error("xxljob: callback queue full, dropping oldest results",
			"dropped", over, "first_log_id", dropped[0].LogID, "last_log_id", dropped[over-1].LogID)
	}
	c.mu.Unlock()
	select {
	case c.notify <- struct{}{}:
	default:
	}
}

func (c *callbacker) start() { go c.loop() }

func (c *callbacker) loop() {
	defer close(c.done)
	tick := time.NewTicker(c.retryInterval)
	defer tick.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-c.notify:
			// After a failure, wait for the retry interval instead of hitting an unavailable admin on every result.
			c.mu.Lock()
			backoff := time.Since(c.lastFailure) < c.retryInterval
			c.mu.Unlock()
			if !backoff {
				c.flush(context.Background())
			}
		case <-tick.C:
			c.flush(context.Background())
		}
	}
}

// flush sends the queued results in batches. It stops at the first failed batch and puts that batch and the rest
// back at the head of the queue.
func (c *callbacker) flush(ctx context.Context) {
	c.mu.Lock()
	batch := c.pending
	c.pending = nil
	c.mu.Unlock()
	for len(batch) > 0 {
		n := min(len(batch), callbackBatch)
		if err := c.admin.callback(ctx, batch[:n]); err != nil {
			c.mu.Lock()
			c.pending = append(batch, c.pending...)
			c.lastFailure = time.Now()
			pending := len(c.pending)
			c.mu.Unlock()
			c.log.Error("xxljob: callback failed, will retry", "pending", pending, "err", err)
			return
		}
		batch = batch[n:]
	}
}

func (c *callbacker) pendingCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pending)
}

// close stops the background sender and tries to deliver the remaining results until ctx ends. "Queue empty" and
// "closed" are decided under the same lock: every result pushed before closing is sent, and push logs any result
// that arrives afterwards.
func (c *callbacker) close(ctx context.Context) {
	close(c.stop)
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
			lost := len(c.pending)
			c.closed = true
			c.mu.Unlock()
			c.log.Error("xxljob: results not delivered before shutdown", "count", lost)
			return
		case <-time.After(min(c.retryInterval, time.Second)):
		}
	}
}
