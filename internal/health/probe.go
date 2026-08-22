package health

import (
	"context"
	"errors"
	"sync"
	"time"

	"poolroute/internal/model"
)

// ProbeOptions configures the active probe loop.
type ProbeOptions struct {
	Interval time.Duration
	Timeout  time.Duration
	Dialer   Dialer
}

// Prober runs active health probes for every node.
type Prober struct {
	opts ProbeOptions
	mu   sync.Mutex
	// pending counts in-flight probes so tests can detect leaked goroutines.
	pending int
}

// NewProber creates a prober with the given options.
func NewProber(opts ProbeOptions) *Prober {
	if opts.Interval <= 0 {
		opts.Interval = 5 * time.Second
	}
	if opts.Timeout <= 0 {
		opts.Timeout = time.Second
	}
	return &Prober{opts: opts}
}

// Probe runs one probe against a node and returns the result.
func (p *Prober) Probe(ctx context.Context, node *model.Node) model.ProbeResult {
	p.mu.Lock()
	p.pending++
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		p.pending--
		p.mu.Unlock()
	}()

	start := time.Now()
	conn, err := p.opts.Dialer.DialContext(ctx, "tcp", node.Addr)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return model.ProbeResult{NodeID: node.ID, OK: false, Stale: true, Err: err}
		}
		return model.ProbeResult{NodeID: node.ID, OK: false, Latency: time.Since(start), Err: err}
	}
	_ = conn.Close()
	if ctx.Err() != nil {
		// The probe was cancelled while the dial was in flight; a late
		// "successful" dial must not be treated as a fresh result.
		return model.ProbeResult{NodeID: node.ID, OK: false, Stale: true, Err: ctx.Err()}
	}
	return model.ProbeResult{NodeID: node.ID, OK: true, Latency: time.Since(start)}
}

// Pending returns the number of in-flight probes.
func (p *Prober) Pending() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pending
}
