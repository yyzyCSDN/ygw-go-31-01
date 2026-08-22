package health

import (
	"context"
	"time"

	"poolroute/internal/model"
)

// ProbeLoop continuously probes every node returned by nodes() and forwards
// results to onResult until the context is cancelled.
func (p *Prober) ProbeLoop(ctx context.Context, nodes func() []*model.Node, onResult func(model.ProbeResult)) {
	ticker := time.NewTicker(p.opts.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, n := range nodes() {
				res := p.Probe(ctx, n)
				if onResult != nil {
					onResult(res)
				}
			}
		}
	}
}
