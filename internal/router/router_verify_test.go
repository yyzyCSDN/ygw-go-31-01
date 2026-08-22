package router

import (
	"fmt"
	"testing"

	"poolroute/internal/model"
	"poolroute/internal/registry"
	"poolroute/internal/ring"
	"poolroute/internal/session"
)

func TestRouteUsesUpdatedRingAfterEviction(t *testing.T) {
	reg := registry.New()
	r := ring.New()
	ids := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("node-%02d", i+1)
		if err := reg.Add(model.NewNode(id, "127.0.0.1:0", "v1", 100)); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	r.Rebuild(reg.Nodes())
	rt := New(r, session.New(), nil,
		func(id string) (*model.Node, error) { return reg.Get(id) },
		func() []string { return ids },
		2)

	// Find a key whose ring owner is node-01.
	key := ""
	for i := 0; i < 200000 && key == ""; i++ {
		k := fmt.Sprintf("key-%d", i)
		if id, err := r.Pick(k); err == nil && id == "node-01" {
			key = k
		}
	}
	if key == "" {
		t.Fatal("no key found for node-01")
	}

	// Evict node-01 from the registry. The ring is intentionally left stale so
	// the router must fall back to a healthy candidate.
	if err := reg.Remove("node-01"); err != nil {
		t.Fatal(err)
	}

	node, err := rt.Route(key)
	if err != nil {
		t.Fatalf("route after eviction returned error: %v", err)
	}
	if node.ID == "node-01" {
		t.Fatalf("route still returns evicted node %s", node.ID)
	}
	if !node.TakesTraffic() {
		t.Fatalf("route returned non-traffic node %s", node.ID)
	}
}
