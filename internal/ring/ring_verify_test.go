package ring

import (
	"fmt"
	"testing"

	"poolroute/internal/model"
)

func rnode(id string, w int) *model.Node {
	return model.NewNode(id, "addr-"+id, "v1", w)
}

func TestRebuildKeepsZeroWeightVnodes(t *testing.T) {
	r := New()
	r.RebuildFromRegistry([]*model.Node{rnode("A", 100), rnode("B", 0), rnode("C", 100)})

	// Find a key that maps to the zero-weight node B (its placeholder vnode).
	key := ""
	for i := 0; i < 300000 && key == ""; i++ {
		k := fmt.Sprintf("k-%d", i)
		if id, err := r.Pick(k); err == nil && id == "B" {
			key = k
		}
	}
	if key == "" {
		t.Fatal("no key maps to zero-weight node B")
	}

	// Restore B's weight; the key owned by B must stay with B.
	r.RebuildFromRegistry([]*model.Node{rnode("A", 100), rnode("B", 100), rnode("C", 100)})
	got, err := r.Pick(key)
	if err != nil {
		t.Fatal(err)
	}
	if got != "B" {
		t.Fatalf("key drifted after weight restore: want B, got %s", got)
	}
}
