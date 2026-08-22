package pool

import (
	"context"
	"net"
	"testing"

	"poolroute/internal/model"
)

type fakeDialer struct{}

func (fakeDialer) DialContext(ctx context.Context, addr string) (net.Conn, error) {
	a, _ := net.Pipe()
	return a, nil
}

func TestReleaseReturnsConnectionToServedNode(t *testing.T) {
	m := NewManager(fakeDialer{})
	nodeA := model.NewNode("A", "addr-a", "v1", 100)
	nodeB := model.NewNode("B", "addr-b", "v1", 100)

	conn, err := m.Acquire(context.Background(), nodeA)
	if err != nil {
		t.Fatal(err)
	}
	if conn.NodeID != "A" {
		t.Fatalf("conn belongs to %s, want A", conn.NodeID)
	}
	// The request was actually served by B after a retry; the connection must
	// still return to the pool that owns it (A), not to B.
	if err := m.Release(conn, nodeB); err != nil {
		t.Fatal(err)
	}
	stats := m.Stats()
	if stats["A"][0] != 1 {
		t.Fatalf("node A idle=%d, want 1 (connection must return to its owning pool)", stats["A"][0])
	}
	if stats["B"][0] != 0 {
		t.Fatalf("node B idle=%d, want 0", stats["B"][0])
	}
}
