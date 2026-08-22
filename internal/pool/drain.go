package pool

import (
	"poolroute/internal/model"
)

// drain closes all idle connections and marks the pool as draining.
func (p *NodePool) drain() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.draining = true
	for _, conn := range p.idle {
		conn.SetState(model.ConnDraining)
		_ = conn.Close()
	}
	p.idle = p.idle[:0]
}
