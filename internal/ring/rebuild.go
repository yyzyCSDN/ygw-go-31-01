package ring

import (
	"sort"

	"poolroute/internal/model"
)

// RebuildFromRegistry reconstructs the ring using every node currently in the
// registry, including zero-weight nodes which are kept as placeholders.
func (r *Ring) RebuildFromRegistry(nodes []*model.Node) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.weights = make(map[string]int, len(nodes))
	built := make([]vnode, 0)
	for _, n := range nodes {
		if n.Weight < 0 {
			continue
		}
		r.weights[n.ID] = n.Weight
		if n.Weight == 0 {
			// BUG(07): zero-weight nodes are dropped entirely, remapping the
			// hash space and drifting a large number of keys.
			continue
		}
		built = appendVNodes(built, n.ID, n.Weight, r.perUnit)
	}
	sort.Slice(built, func(i, j int) bool { return built[i].hash < built[j].hash })
	r.vnodes = built
}

// nodeIDs returns the sorted set of node ids currently represented on the ring.
func (r *Ring) nodeIDs() []string {
	seen := make(map[string]bool)
	for _, v := range r.vnodes {
		seen[v.nodeID] = true
	}
	out := make([]string, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}
