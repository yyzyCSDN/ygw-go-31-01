// Package control exposes operator operations: draining, eviction, restore and
// the shared retry budget.
package control

import (
	"sync"

	"poolroute/internal/model"
	"poolroute/internal/registry"
)

// RetryBudget bounds how many retries the whole control plane will spend.
type RetryBudget struct {
	mu     sync.Mutex
	limit  int
	spent  int
}

// NewRetryBudget creates a budget with the given limit.
func NewRetryBudget(limit int) *RetryBudget {
	if limit <= 0 {
		limit = 100
	}
	return &RetryBudget{limit: limit}
}

// Try spends one retry if budget remains.
func (b *RetryBudget) Try() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.spent >= b.limit {
		return false
	}
	b.spent++
	return true
}

// Spent returns the number of retries used.
func (b *RetryBudget) Spent() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.spent
}

// Ops performs operator actions against the registry and pool.
type Ops struct {
	reg   *registry.Registry
	drain func(nodeID string)
}

// NewOps wires operator actions to a registry and drain callback.
func NewOps(reg *registry.Registry, drain func(nodeID string)) *Ops {
	return &Ops{reg: reg, drain: drain}
}

// Drain marks a node draining and drains its connections.
func (o *Ops) Drain(nodeID string) error {
	if err := o.reg.Transition(nodeID, model.Draining); err != nil {
		return err
	}
	if o.drain != nil {
		o.drain(nodeID)
	}
	return nil
}

// Evict removes a node entirely.
func (o *Ops) Evict(nodeID string) error {
	return o.reg.Remove(nodeID)
}

// Restore marks a node healthy again.
func (o *Ops) Restore(nodeID string) error {
	return o.reg.Transition(nodeID, model.Healthy)
}
