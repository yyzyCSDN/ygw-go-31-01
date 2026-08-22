package pool

import (
	"context"

	"poolroute/internal/model"
)

// acquire returns an idle connection or dials a fresh one.
func (p *NodePool) acquire(ctx context.Context, node *model.Node) (*model.Conn, error) {
	p.mu.Lock()
	if len(p.idle) > 0 {
		conn := p.idle[len(p.idle)-1]
		p.idle = p.idle[:len(p.idle)-1]
		p.inUse++
		p.mu.Unlock()
		conn.SetState(model.ConnInUse)
		return conn, nil
	}
	if p.inUse >= p.maxTotal {
		p.mu.Unlock()
		return nil, ErrNoConnection
	}
	p.inUse++
	p.mu.Unlock()

	raw, err := p.dialer.DialContext(ctx, node.Addr)
	if err != nil {
		p.mu.Lock()
		p.inUse--
		p.mu.Unlock()
		return nil, err
	}
	conn := model.NewConn(raw, node.ID)
	conn.SetState(model.ConnInUse)
	return conn, nil
}
