package ring

import (
	"fmt"
	"sort"
)

// Candidates returns up to n distinct node ids responsible for key, walking the
// ring clockwise from the primary owner. It is used by failover paths that need
// an ordered list of backup nodes.
func (r *Ring) Candidates(key string, n int) ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if n <= 0 {
		n = 1
	}
	if len(r.vnodes) == 0 {
		return nil, fmt.Errorf("ring: empty ring")
	}
	h := hashKey(key)
	idx := sort.Search(len(r.vnodes), func(i int) bool {
		return r.vnodes[i].hash >= h
	})
	if idx == len(r.vnodes) {
		idx = 0
	}
	out := make([]string, 0, n)
	seen := make(map[string]bool)
	for step := 0; step < len(r.vnodes) && len(out) < n; step++ {
		pos := (idx + step) % len(r.vnodes)
		id := r.vnodes[pos].nodeID
		if seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, id)
	}
	return out, nil
}

// Distribution returns how many virtual nodes each node owns on the ring.
func (r *Ring) Distribution() map[string]int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]int)
	for _, v := range r.vnodes {
		out[v.nodeID]++
	}
	return out
}

// NodeCount returns the number of distinct nodes represented on the ring.
func (r *Ring) NodeCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	seen := make(map[string]bool)
	for _, v := range r.vnodes {
		seen[v.nodeID] = true
	}
	return len(seen)
}
