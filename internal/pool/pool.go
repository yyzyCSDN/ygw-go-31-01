// Package pool manages per-node connection pools. Connections are acquired for
// a request, released back to the node that actually served the request, and
// drained when a node goes offline.
package pool

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	"poolroute/internal/model"
)

// Defaults for pool sizing.
const (
	DefaultMaxIdlePerNode = 8
	DefaultMaxTotalPerNode = 32
)

// ErrNodeDraining is returned when a request tries to acquire from a draining node.
var ErrNodeDraining = errors.New("pool: node is draining")

// ErrNoConnection is returned when a dial attempt fails.
var ErrNoConnection = errors.New("pool: no connection available")

// Dialer establishes a raw connection to a node address.
type Dialer interface {
	DialContext(ctx context.Context, addr string) (net.Conn, error)
}

// NodePool is the connection pool for a single node.
type NodePool struct {
	nodeID string

	mu       sync.Mutex
	idle     []*model.Conn
	inUse    int
	draining bool

	maxIdle  int
	maxTotal int
	dialer   Dialer
}

// PoolManager owns one NodePool per upstream node.
type PoolManager struct {
	mu       sync.Mutex
	pools    map[string]*NodePool
	draining map[string]bool
	dialer   Dialer
	maxIdle  int
	maxTotal int
}

// NewManager creates a pool manager with the given dialer.
func NewManager(dialer Dialer) *PoolManager {
	return &PoolManager{
		pools:    make(map[string]*NodePool),
		draining: make(map[string]bool),
		dialer:   dialer,
		maxIdle:  DefaultMaxIdlePerNode,
		maxTotal: DefaultMaxTotalPerNode,
	}
}

// poolFor returns the pool for a node, creating it on first use.
func (m *PoolManager) poolFor(nodeID string) *NodePool {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.pools[nodeID]
	if !ok {
		p = &NodePool{
			nodeID:   nodeID,
			maxIdle:  m.maxIdle,
			maxTotal: m.maxTotal,
			dialer:   m.dialer,
		}
		m.pools[nodeID] = p
	}
	return p
}

// Acquire gets an idle connection for the node or dials a new one.
func (m *PoolManager) Acquire(ctx context.Context, node *model.Node) (*model.Conn, error) {
	if !node.TakesTraffic() {
		if node.State() == model.Draining {
			return nil, ErrNodeDraining
		}
		return nil, fmt.Errorf("pool: node %s not taking traffic (%s)", node.ID, node.State())
	}
	return m.poolFor(node.ID).acquire(ctx, node)
}

// Release returns a connection to the pool of the node that owns it. A retried
// request may have been served by a different node, but the connection always
// belongs to the pool it was acquired from; returning it anywhere else would
// corrupt both pools' accounting.
func (m *PoolManager) Release(conn *model.Conn, servedBy *model.Node) error {
	if conn == nil {
		return errors.New("pool: release of nil connection")
	}
	owner := conn.NodeID
	if owner == "" {
		return errors.New("pool: connection is missing its owning node")
	}
	return m.poolFor(owner).release(conn, servedBy)
}

// Drain marks a node draining, records it as draining in the manager, and
// closes its idle connections. In-flight connections are closed as they are
// released so no new request can borrow a draining node's connection.
func (m *PoolManager) Drain(nodeID string) {
	m.mu.Lock()
	m.draining[nodeID] = true
	m.mu.Unlock()
	m.poolFor(nodeID).drain()
}

// IsDraining reports whether a node was marked draining through the manager.
func (m *PoolManager) IsDraining(nodeID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.draining[nodeID]
}

// Stats returns per-node idle/in-use counters for reports and tests.
func (m *PoolManager) Stats() map[string][2]int {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string][2]int, len(m.pools))
	for id, p := range m.pools {
		p.mu.Lock()
		out[id] = [2]int{len(p.idle), p.inUse}
		p.mu.Unlock()
	}
	return out
}
