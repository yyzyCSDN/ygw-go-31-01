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
	seen := make(map[string]bool)
	for _, n := range nodes {
		if n.Weight < 0 {
			// A negative weight is treated as a misconfiguration; the node is
			// excluded but its absence must not silently remap the ring.
			continue
		}
		r.weights[n.ID] = n.Weight
		if seen[n.ID] {
			continue
		}
		seen[n.ID] = true
		if n.Weight == 0 {
			// Zero-weight nodes keep a minimal placeholder so the hash space
			// does not get remapped when their weight changes later.
			built = append(built, vnode{hash: hashKey(vnodeName(n.ID, 0)), nodeID: n.ID})
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
