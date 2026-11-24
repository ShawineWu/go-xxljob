// Package ratelog limits how often a recurring event is logged.
package ratelog

import (
	"sync"
	"time"
)

// Limiter allows an event through at most once per interval and counts the ones it suppressed. The zero value is
// not usable; use New.
type Limiter struct {
	every time.Duration

	mu         sync.Mutex
	last       time.Time
	suppressed int
}

// New returns a limiter that allows one event per interval.
func New(every time.Duration) *Limiter { return &Limiter{every: every} }

// Allow reports whether the event should be logged now and, if so, how many were suppressed since the last one.
func (l *Limiter) Allow() (ok bool, suppressed int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	if !l.last.IsZero() && now.Sub(l.last) < l.every {
		l.suppressed++
		return false, 0
	}
	l.last, suppressed, l.suppressed = now, l.suppressed, 0
	return true, suppressed
}
