package router

import (
	"sort"

	"poolroute/internal/model"
)

// fallbackNode returns the ring's next candidate for a key after the preferred
// node failed. It is used by retry paths that need an alternative node.
// fallbackNode returns a healthy node other than avoidID, preferring the ring
// candidate and otherwise the lowest-id healthy node.
func (rt *Router) fallbackNode(key, avoidID string) (*model.Node, error) {
	if id, err := rt.ring.Pick(key); err == nil && id != avoidID {
		if n, err := rt.lookup(id); err == nil && n.TakesTraffic() {
			return n, nil
		}
	}
	// Deterministic fallback: the lowest healthy node id that is not avoided.
	ids := make([]string, 0)
	if rt.ids != nil {
		for _, id := range rt.ids() {
			if id != avoidID {
				if n, err := rt.lookup(id); err == nil && n.TakesTraffic() {
					ids = append(ids, id)
				}
			}
		}
	}
	if len(ids) == 0 {
		return nil, ErrNoHealthyNode
	}
	sort.Strings(ids)
	return rt.lookup(ids[0])
}

// Fallback returns a healthy backup node for key, avoiding avoidID.
func (rt *Router) Fallback(key, avoidID string) (*model.Node, error) {
	return rt.fallbackNode(key, avoidID)
}
