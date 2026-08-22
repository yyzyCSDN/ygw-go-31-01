package pool

import (
	"errors"

	"poolroute/internal/model"
)

// release returns a connection to the pool of the node that served the request.
// Connections belonging to a draining node are closed instead of reused.
func (p *NodePool) release(conn *model.Conn, servedBy *model.Node) error {
	if conn.State() != model.ConnInUse {
		// Defensive: only connections that were actually acquired may be
		// released; anything else indicates a double release or a leak.
		return errors.New("pool: releasing a connection that is not in use")
	}
	conn.SetState(model.ConnIdle)
	p.mu.Lock()
	defer p.mu.Unlock()

	p.inUse--
	if p.inUse < 0 {
		p.inUse = 0
	}

	if p.draining || (servedBy != nil && servedBy.State() == model.Draining) {
		conn.SetState(model.ConnDraining)
		_ = conn.Close()
		return nil
	}
	if len(p.idle) >= p.maxIdle {
		_ = conn.Close()
		return nil
	}
	p.idle = append(p.idle, conn)
	return nil
}
