// Package circuit implements a per-node circuit breaker with the standard
// closed -> open -> half-open lifecycle.
package circuit

import (
	"sync"
	"time"
)

// State is a breaker state.
type State int

const (
	Closed State = iota
	Open
	HalfOpen
)

func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case Open:
		return "open"
	case HalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// Breaker is the per-node breaker state machine.
type Breaker struct {
	mu             sync.Mutex
	state          State
	failures       int
	threshold      int
	openedAt       time.Time
	cooldown       time.Duration
	halfOpenTried  bool
	now            func() time.Time
}

// New creates a closed breaker.
func New(threshold int, cooldown time.Duration) *Breaker {
	if threshold <= 0 {
		threshold = 5
	}
	if cooldown <= 0 {
		cooldown = 10 * time.Second
	}
	return &Breaker{state: Closed, threshold: threshold, cooldown: cooldown, now: time.Now}
}

// State returns the current breaker state.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// Allow reports whether a request may proceed.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case Open:
		if b.now().Sub(b.openedAt) >= b.cooldown {
			b.state = HalfOpen
			b.halfOpenTried = false
			return true
		}
		return false
	case HalfOpen:
		if b.halfOpenTried {
			return false
		}
		b.halfOpenTried = true
		return true
	default:
		return true
	}
}

// RecordSuccess closes an open/half-open breaker.
func (b *Breaker) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = 0
	b.halfOpenTried = false
	b.state = Closed
}

// RecordFailure counts a failure and opens the breaker past the threshold.
func (b *Breaker) RecordFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures++
	if b.state == HalfOpen || b.failures >= b.threshold {
		b.state = Open
		b.openedAt = b.now()
	}
}
