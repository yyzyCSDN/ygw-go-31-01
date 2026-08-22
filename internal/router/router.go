// Package router picks a node for a key using the consistent-hash ring,
// honours sticky session bindings, and performs bounded failure retries.
package router

import (
	"context"
	"errors"
	"fmt"

	"poolroute/internal/model"
	"poolroute/internal/pool"
	"poolroute/internal/ring"
	"poolroute/internal/session"
)

// ErrNoHealthyNode is returned when no node can serve a request.
var ErrNoHealthyNode = errors.New("router: no healthy node")

// RoundTripper executes a single request against a node and reports success.
type RoundTripper func(ctx context.Context, node *model.Node, key string) error

// Router routes keys to nodes with sticky sessions and retries.
type Router struct {
	ring     *ring.Ring
	sessions *session.Store
	pools    *pool.PoolManager
	lookup   NodeLookup
	ids      func() []string
	maxRetry int
}

// NodeLookup resolves a node id to a node, or returns an error when absent.
type NodeLookup func(nodeID string) (*model.Node, error)

// New creates a router.
func New(r *ring.Ring, s *session.Store, p *pool.PoolManager, lookup NodeLookup, ids func() []string, maxRetry int) *Router {
	if maxRetry < 0 {
		maxRetry = 0
	}
	return &Router{ring: r, sessions: s, pools: p, lookup: lookup, ids: ids, maxRetry: maxRetry}
}

// Route resolves the preferred node for a key without executing it.
func (rt *Router) Route(key string) (*model.Node, error) {
	if bound := rt.sessions.Bound(key); bound != "" {
		if n, err := rt.lookup(bound); err == nil && n.TakesTraffic() {
			return n, nil
		}
	}
	return rt.pickHealthy(key)
}

// pickHealthy resolves the ring owner for key and falls back to the next
// healthy candidate when the ring is stale (for example right after a node has
// been evicted but before the ring is rebuilt).
func (rt *Router) pickHealthy(key string) (*model.Node, error) {
	nodeID, err := rt.ring.Pick(key)
	if err != nil {
		return nil, err
	}
	if n, err := rt.lookup(nodeID); err == nil && n.TakesTraffic() {
		return n, nil
	}
	if ids, err := rt.ring.Candidates(key, 4); err == nil {
		for _, id := range ids {
			if id == nodeID {
				continue
			}
			if n, err := rt.lookup(id); err == nil && n.TakesTraffic() {
				return n, nil
			}
		}
	}
	return nil, ErrNoHealthyNode
}

// Serve routes a key and runs the round tripper with bounded retries. The
// sticky binding is only set on the first successful attempt.
func (rt *Router) Serve(ctx context.Context, key string, rt2 RoundTripper) error {
	attempted := 0
	var node *model.Node
	for {
		var err error
		if attempted == 0 {
			node, err = rt.Route(key)
		} else {
			node, err = rt.fallbackNode(key, node.ID)
		}
		if err != nil {
			return err
		}
		conn, err := rt.pools.Acquire(ctx, node)
		if err != nil {
			return err
		}
		serveErr := rt2(ctx, node, key)
		_ = rt.pools.Release(conn, node)
		if serveErr == nil {
			if attempted == 0 {
				// Sticky binding is recorded only for the original route so a
				// single retry can never rewrite long-term routing state.
				rt.sessions.BindIfAbsent(key, node.ID)
			}
			return nil
		}
		// The preferred node failed; drop a stale binding to it so the next
		// attempt can fall back to a healthy node instead of looping.
		if rt.sessions.Bound(key) == node.ID {
			rt.sessions.Invalidate(key)
		}
		attempted++
		if attempted > rt.maxRetry {
			return fmt.Errorf("router: request for key %s failed after %d attempts: %w", key, attempted, serveErr)
		}
	}
}
