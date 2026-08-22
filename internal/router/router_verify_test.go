package router

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"

	"poolroute/internal/model"
	"poolroute/internal/pool"
	"poolroute/internal/registry"
	"poolroute/internal/ring"
	"poolroute/internal/session"
)

type okDialer struct{}

func (okDialer) DialContext(ctx context.Context, addr string) (net.Conn, error) {
	a, _ := net.Pipe()
	return a, nil
}

func TestRetryDoesNotRewriteStickyBinding(t *testing.T) {
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
	sess := session.New()
	pools := pool.NewManager(okDialer{})
	rt := New(r, sess, pools,
		func(id string) (*model.Node, error) { return reg.Get(id) },
		func() []string { return ids },
		1)

	// Pick a key whose ring owner is node-01, then make node-01 fail once so
	// the request retries on a fallback node.
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

	err := rt.Serve(context.Background(), key, func(ctx context.Context, n *model.Node, k string) error {
		if n.ID == "node-01" {
			return errors.New("upstream failure")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := sess.Bound(key); got != "" {
		t.Fatalf("retry rewrote sticky binding to %q; want no binding", got)
	}
}
