// Package registry maintains the upstream node registry: membership, lifecycle
// state transitions, weights and versions. It is the single authority for which
// nodes exist and what state they are in.
package registry

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"poolroute/internal/model"
)

// ErrNotFound is returned when a node id is not present in the registry.
var ErrNotFound = errors.New("registry: node not found")

// Registry is a concurrency-safe map of nodes plus transition bookkeeping.
type Registry struct {
	mu    sync.RWMutex
	nodes map[string]*model.Node
	order []string
}

// New creates an empty registry.
func New() *Registry {
	return &Registry{nodes: make(map[string]*model.Node)}
}

// Add registers a new healthy node.
func (r *Registry) Add(n *model.Node) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.nodes[n.ID]; ok {
		return fmt.Errorf("registry: node %s already registered", n.ID)
	}
	n.SetState(model.Healthy)
	r.nodes[n.ID] = n
	r.order = append(r.order, n.ID)
	sort.Strings(r.order)
	return nil
}

// Remove deletes a node from the registry.
func (r *Registry) Remove(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.nodes[id]; !ok {
		return ErrNotFound
	}
	delete(r.nodes, id)
	r.rebuildOrderLocked()
	return nil
}

// Get returns a node by id.
func (r *Registry) Get(id string) (*model.Node, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if id == "" {
		return nil, ErrNotFound
	}
	n, ok := r.nodes[id]
	if !ok {
		// A removed node must surface as an error so callers (for example the
		// health receipt handler) can detect the deletion instead of assuming
		// the node still exists.
		return nil, ErrNotFound
	}
	return n, nil
}

// Nodes returns all registered nodes sorted by id.
func (r *Registry) Nodes() []*model.Node {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*model.Node, 0, len(r.nodes))
	for _, id := range r.order {
		out = append(out, r.nodes[id])
	}
	return out
}

// HealthyNodes returns nodes whose state allows new traffic.
func (r *Registry) HealthyNodes() []*model.Node {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*model.Node, 0, len(r.nodes))
	for _, id := range r.order {
		if n := r.nodes[id]; n.TakesTraffic() {
			out = append(out, n)
		}
	}
	return out
}

// SetState transitions a node to the given state.
func (r *Registry) SetState(id string, s model.NodeState) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, ok := r.nodes[id]
	if !ok {
		return ErrNotFound
	}
	n.SetState(s)
	return nil
}

// SetWeight changes a node's weight.
func (r *Registry) SetWeight(id string, weight int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, ok := r.nodes[id]
	if !ok {
		return ErrNotFound
	}
	if weight < 0 {
		return fmt.Errorf("registry: negative weight for %s", id)
	}
	n.Weight = weight
	// Weight changes alter the ring layout; consumers (the ring) must rebuild
	// so a zero weight keeps a placeholder instead of remapping the space.
	return nil
}

// SetVersion changes a node's version tag.
func (r *Registry) SetVersion(id, version string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, ok := r.nodes[id]
	if !ok {
		return ErrNotFound
	}
	n.Version = version
	return nil
}

// Len returns the number of registered nodes.
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodes)
}

// rebuildOrderLocked recomputes the stable sorted id order.
func (r *Registry) rebuildOrderLocked() {
	r.order = r.order[:0]
	for id := range r.nodes {
		r.order = append(r.order, id)
	}
	sort.Strings(r.order)
}
