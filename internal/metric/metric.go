// Package metric maintains small rolling windows of per-node request outcomes
// used by the passive failure and retry-budget logic.
package metric

import (
	"sync"
	"time"
)

// Window holds a fixed-size ring of booleans plus a recent count.
type Window struct {
	mu     sync.Mutex
	size   int
	ok     int
	total  int
	events []bool
}

// NewWindow creates a rolling window of the given size.
func NewWindow(size int) *Window {
	if size <= 0 {
		size = 20
	}
	return &Window{size: size}
}

// Record appends one outcome, evicting the oldest when full.
func (w *Window) Record(success bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.events) == w.size {
		old := w.events[0]
		w.events = w.events[1:]
		if old {
			w.ok--
		}
		w.total--
	}
	w.events = append(w.events, success)
	w.total++
	if success {
		w.ok++
	}
}

// SuccessRate returns the success rate in [0,1].
func (w *Window) SuccessRate() float64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.total == 0 {
		return 1
	}
	return float64(w.ok) / float64(w.total)
}

// Failures returns the number of failures in the window.
func (w *Window) Failures() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.total - w.ok
}

// Total returns the number of recorded outcomes.
func (w *Window) Total() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.total
}

// Collector tracks one window per node.
type Collector struct {
	mu     sync.Mutex
	window *Window
	nodes  map[string]*Window
}

// NewCollector creates a collector.
func NewCollector(size int) *Collector {
	return &Collector{window: NewWindow(size), nodes: make(map[string]*Window)}
}

// Record records an outcome for a node.
func (c *Collector) Record(nodeID string, success bool) {
	c.window.Record(success)
	c.mu.Lock()
	w, ok := c.nodes[nodeID]
	if !ok {
		w = NewWindow(c.window.size)
		c.nodes[nodeID] = w
	}
	c.mu.Unlock()
	w.Record(success)
}

// NodeRate returns a node's success rate.
func (c *Collector) NodeRate(nodeID string) float64 {
	c.mu.Lock()
	w, ok := c.nodes[nodeID]
	c.mu.Unlock()
	if !ok {
		return 1
	}
	return w.SuccessRate()
}

// Snapshot returns rates for all nodes.
func (c *Collector) Snapshot() map[string]float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]float64, len(c.nodes))
	for id, w := range c.nodes {
		out[id] = w.SuccessRate()
	}
	return out
}

// Now is exposed for tests that need to advance time deterministically.
var Now = time.Now
