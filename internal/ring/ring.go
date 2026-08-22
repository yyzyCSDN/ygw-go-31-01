// Package ring implements a consistent-hash ring over upstream nodes using
// murmur3. Each node contributes virtual nodes proportional to its weight, and
// keys are routed to the first clockwise virtual node.
package ring

import (
	"fmt"
	"sort"
	"sync"

	"poolroute/internal/model"
)

// DefaultVNodesPerUnit is how many virtual nodes each unit of weight gets.
const DefaultVNodesPerUnit = 16

// vnode is a single point on the hash ring.
type vnode struct {
	hash   uint32
	nodeID string
}

// Ring is a concurrency-safe consistent hash ring.
type Ring struct {
	mu        sync.RWMutex
	vnodes    []vnode
	weights   map[string]int
	perUnit   int
}

// New creates an empty ring.
func New() *Ring {
	return &Ring{weights: make(map[string]int), perUnit: DefaultVNodesPerUnit}
}

// Len returns the number of virtual nodes currently on the ring.
func (r *Ring) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.vnodes)
}

// Pick returns the node id responsible for key.
func (r *Ring) Pick(key string) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.vnodes) == 0 {
		return "", fmt.Errorf("ring: empty ring")
	}
	h := hashKey(key)
	idx := sort.Search(len(r.vnodes), func(i int) bool {
		return r.vnodes[i].hash >= h
	})
	if idx == len(r.vnodes) {
		idx = 0
	}
	return r.vnodes[idx].nodeID, nil
}

// Rebuild replaces the ring from a set of nodes and their weights.
func (r *Ring) Rebuild(nodes []*model.Node) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.weights = make(map[string]int, len(nodes))
	built := make([]vnode, 0)
	for _, n := range nodes {
		if n.Weight < 0 {
			continue
		}
		r.weights[n.ID] = n.Weight
		built = appendVNodes(built, n.ID, n.Weight, r.perUnit)
	}
	sort.Slice(built, func(i, j int) bool { return built[i].hash < built[j].hash })
	r.vnodes = built
}

// RemoveNode drops a node from the ring without full rebuild.
func (r *Ring) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.weights, nodeID)
	kept := r.vnodes[:0]
	for _, v := range r.vnodes {
		if v.nodeID != nodeID {
			kept = append(kept, v)
		}
	}
	r.vnodes = kept
}
