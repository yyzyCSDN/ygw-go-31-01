package ring

import (
	"fmt"
	"testing"

	"poolroute/internal/model"
)

func buildRing(t *testing.T, r *Ring, ids ...string) []*model.Node {
	t.Helper()
	nodes := make([]*model.Node, 0, len(ids))
	for _, id := range ids {
		nodes = append(nodes, model.NewNode(id, "", "v1", 100))
	}
	r.Rebuild(nodes)
	return nodes
}

// RemoveNode must physically drop a node's virtual nodes so Pick/Candidates
// never return the evicted id again — not just until the next Rebuild.
func TestRingRemoveNodeDropsVirtualNodes(t *testing.T) {
	r := New()
	buildRing(t, r, "node-A", "node-B", "node-C")

	if got := r.NodeCount(); got != 3 {
		t.Fatalf("NodeCount before evict = %d, want 3", got)
	}
	before := r.Distribution()["node-B"]
	if before == 0 {
		t.Fatalf("node-B had no vnodes before evict")
	}

	r.RemoveNode("node-B")

	if got := r.NodeCount(); got != 2 {
		t.Fatalf("NodeCount after evict = %d, want 2", got)
	}
	if got := r.Distribution()["node-B"]; got != 0 {
		t.Fatalf("node-B still owns %d vnodes after RemoveNode", got)
	}

	// No key may resolve to the evicted node.
	leaks := 0
	for i := 0; i < 100000; i++ {
		id, err := r.Pick(fmt.Sprintf("key-%d", i))
		if err != nil {
			t.Fatalf("Pick error: %v", err)
		}
		if id == "node-B" {
			leaks++
		}
	}
	if leaks != 0 {
		t.Fatalf("RemoveNode leaked %d keys to evicted node-B", leaks)
	}
}

// Candidates must no longer list the evicted node as a backup.
func TestRingCandidatesExcludesRemovedNode(t *testing.T) {
	r := New()
	buildRing(t, r, "node-A", "node-B", "node-C")

	r.RemoveNode("node-B")

	for i := 0; i < 1000; i++ {
		ids, err := r.Candidates(fmt.Sprintf("key-%d", i), 5)
		if err != nil {
			t.Fatalf("Candidates error: %v", err)
		}
		for _, id := range ids {
			if id == "node-B" {
				t.Fatalf("Candidates returned evicted node-B for key-%d", i)
			}
		}
	}
}

// Removing a node that has no vnodes (already removed / never added) is a no-op.
func TestRingRemoveNodeAbsentIsNoOp(t *testing.T) {
	r := New()
	buildRing(t, r, "node-A", "node-B")
	n := r.Len()
	r.RemoveNode("node-ghost")
	if got := r.Len(); got != n {
		t.Fatalf("Len after removing absent node = %d, want %d", got, n)
	}
}
