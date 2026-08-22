package router

import (
	"context"
	"errors"
	"net"
	"testing"

	"poolroute/internal/model"
	"poolroute/internal/pool"
	"poolroute/internal/ring"
	"poolroute/internal/session"
)

// pipeDialer hands out in-memory connections so the pool can acquire without a
// real network. It is only used to satisfy the Router constructor.
type pipeDialer struct{}

func (pipeDialer) DialContext(_ context.Context, _ string) (net.Conn, error) {
	c, s := net.Pipe()
	go s.Close()
	return c, nil
}

// registryStub backs the router's lookup/ids without a full registry.
type registryStub struct {
	nodes map[string]*model.Node
	order []string
}

func (s *registryStub) lookup(id string) (*model.Node, error) {
	n, ok := s.nodes[id]
	if !ok {
		return nil, errors.New("registryStub: not found")
	}
	return n, nil
}

func (s *registryStub) ids() []string {
	out := make([]string, len(s.order))
	copy(out, s.order)
	return out
}

func newTestRouter(t *testing.T) (*Router, *ring.Ring, map[string]*model.Node, *registryStub) {
	t.Helper()
	r := ring.New()
	ids := []string{"node-A", "node-B", "node-C"}
	byID := make(map[string]*model.Node, len(ids))
	nodes := make([]*model.Node, 0, len(ids))
	for _, id := range ids {
		n := model.NewNode(id, "127.0.0.1:0", "v1", 1)
		byID[id] = n
		nodes = append(nodes, n)
	}
	r.Rebuild(nodes)
	reg := &registryStub{nodes: byID, order: ids}
	rt := New(r, session.New(), pool.NewManager(pipeDialer{}), reg.lookup, reg.ids, 2)
	return rt, r, byID, reg
}

func keyFor(i int) string {
	return "usr-" + string(rune('a'+i%26)) + string(rune('a'+i/26%26))
}

// After a node is evicted from the ring without a full rebuild, Route must
// never return it; it must walk the ring to a healthy node.
func TestRouteSkipsEvictedNode(t *testing.T) {
	rt, r, byID, _ := newTestRouter(t)

	r.RemoveNode("node-B")
	delete(byID, "node-B")

	leaks := 0
	for i := 0; i < 10000; i++ {
		key := keyFor(i)
		n, err := rt.Route(key)
		if err != nil {
			t.Fatalf("Route(%q) error: %v", key, err)
		}
		if n.ID == "node-B" {
			leaks++
		}
	}
	if leaks != 0 {
		t.Fatalf("Route returned evicted node-B for %d keys", leaks)
	}
}

// When the primary ring owner is unhealthy but still on the ring, Route must
// fall through to the next healthy node instead of pinning on the dead owner.
func TestRouteWalksPastUnhealthyOwner(t *testing.T) {
	rt, _, byID, _ := newTestRouter(t)

	// Mark every node unhealthy except node-B; Route must land on node-B.
	byID["node-A"].SetState(model.Unhealthy)
	byID["node-C"].SetState(model.Unhealthy)

	for i := 0; i < 1000; i++ {
		key := keyFor(i)
		n, err := rt.Route(key)
		if err != nil {
			t.Fatalf("Route(%q) error: %v", key, err)
		}
		if n.ID != "node-B" {
			t.Fatalf("Route(%q) = %s, want node-B (only healthy node)", key, n.ID)
		}
	}
}

// With no node taking traffic, Route must surface ErrNoHealthyNode rather than
// silently returning an unhealthy node.
func TestRouteErrNoHealthyWhenAllUnhealthy(t *testing.T) {
	rt, _, byID, _ := newTestRouter(t)
	for _, n := range byID {
		n.SetState(model.Unhealthy)
	}
	if _, err := rt.Route(keyFor(0)); !errors.Is(err, ErrNoHealthyNode) {
		t.Fatalf("Route = %v, want ErrNoHealthyNode", err)
	}
}
