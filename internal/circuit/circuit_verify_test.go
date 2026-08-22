package circuit

import (
	"context"
	"net"
	"testing"
	"time"

	"poolroute/internal/model"
)

type recAcquirer struct {
	acquired int
	released int
}

func (a *recAcquirer) Acquire(ctx context.Context, node *model.Node) (*model.Conn, error) {
	a.acquired++
	c, _ := net.Pipe()
	return model.NewConn(c, node.ID), nil
}

func (a *recAcquirer) Release(conn *model.Conn, servedBy *model.Node) error {
	a.released++
	return conn.Close()
}

func TestHalfOpenProbeAcquiresFromPool(t *testing.T) {
	b := New(5, 10*time.Millisecond)
	for i := 0; i < 5; i++ {
		b.RecordFailure()
	}
	if b.State() != Open {
		t.Fatalf("breaker state=%s, want open", b.State())
	}
	time.Sleep(15 * time.Millisecond)
	if !b.Allow() {
		t.Fatal("breaker did not transition to half-open")
	}
	if b.State() != HalfOpen {
		t.Fatalf("breaker state=%s, want half-open", b.State())
	}

	node := model.NewNode("n", "addr", "v1", 100)
	pool := &recAcquirer{}
	err := b.HalfOpenTrial(context.Background(), node, pool, func(raw any) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if pool.acquired != 1 {
		t.Fatalf("pool.acquired=%d, want 1 (half-open probe must acquire from the pool)", pool.acquired)
	}
	if pool.released != 1 {
		t.Fatalf("pool.released=%d, want 1 (half-open probe must return the connection)", pool.released)
	}
}
