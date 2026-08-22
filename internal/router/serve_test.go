package router

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"poolroute/internal/model"
	"poolroute/internal/pool"
	"poolroute/internal/ring"
	"poolroute/internal/session"
	"poolroute/internal/traffic"
)

// testDialer adapts a net.Dialer to the pool.Dialer surface for tests.
type testDialer struct{ timeout time.Duration }

func (d testDialer) DialContext(ctx context.Context, addr string) (net.Conn, error) {
	var zero net.Dialer
	zero.Timeout = d.timeout
	return zero.DialContext(ctx, "tcp", addr)
}

// newTestRouter builds a router over n real TCP upstreams and returns it with
// the session store so tests can inspect sticky bindings.
func newTestRouter(t *testing.T, n int) (*Router, *session.Store) {
	t.Helper()
	nodes := make([]*model.Node, 0, n)
	for i := 0; i < n; i++ {
		u, err := traffic.StartUpstream()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = u.Close() })
		nodes = append(nodes, model.NewNode(fmt.Sprintf("node-%02d", i+1), u.Addr(), "v1.0.0", 100))
	}
	r := ring.New()
	r.Rebuild(nodes)
	sess := session.New()
	pools := pool.NewManager(testDialer{timeout: 2 * time.Second})
	lookup := func(id string) (*model.Node, error) {
		for _, n := range nodes {
			if n.ID == id {
				return n, nil
			}
		}
		return nil, fmt.Errorf("not found: %s", id)
	}
	ids := func() []string {
		out := make([]string, 0, len(nodes))
		for _, n := range nodes {
			out = append(out, n.ID)
		}
		return out
	}
	return New(r, sess, pools, lookup, ids, 2), sess
}

// TestServe_FirstAttemptSuccessBindsToPreferred confirms the positive case: a
// request that succeeds on the first (preferred) node pins the key to it.
func TestServe_FirstAttemptSuccessBindsToPreferred(t *testing.T) {
	rt, sess := newTestRouter(t, 3)

	key := "usr-1"
	preferred, err := rt.Route(key)
	if err != nil {
		t.Fatalf("Route: %v", err)
	}

	success := func(ctx context.Context, node *model.Node, k string) error { return nil }
	if err := rt.Serve(context.Background(), key, success); err != nil {
		t.Fatalf("Serve: %v", err)
	}
	if bound := sess.Bound(key); bound != preferred.ID {
		t.Fatalf("sticky binding = %q; want preferred %s", bound, preferred.ID)
	}
}

// TestServe_RetrySuccessDoesNotRebindSticky is the regression for the bug where
// a request that fails on the preferred node and succeeds on a retry fallback
// rewrote the sticky binding to the fallback, permanently pinning the key away
// from its ring owner. A retry must serve only the current request; the binding
// target is decided by the ring owner, not by which node absorbed a failure.
func TestServe_RetrySuccessDoesNotRebindSticky(t *testing.T) {
	rt, sess := newTestRouter(t, 3)

	key := "usr-1"
	preferred, err := rt.Route(key)
	if err != nil {
		t.Fatalf("Route: %v", err)
	}

	// Fail the preferred node so Serve retries onto a fallback, which succeeds.
	var servedBy []string
	failPreferred := func(ctx context.Context, node *model.Node, k string) error {
		servedBy = append(servedBy, node.ID)
		if node.ID == preferred.ID {
			return fmt.Errorf("injected failure on %s", node.ID)
		}
		return nil
	}
	if err := rt.Serve(context.Background(), key, failPreferred); err != nil {
		t.Fatalf("Serve after retry: %v", err)
	}

	// A retry must actually have happened: preferred failed, then a different
	// node served the request.
	if len(servedBy) < 2 || servedBy[0] != preferred.ID || servedBy[1] == preferred.ID {
		t.Fatalf("expected retry from %s to a fallback, got %v", preferred.ID, servedBy)
	}

	// The sticky binding must NOT have been rewritten to the fallback node.
	if bound := sess.Bound(key); bound != "" {
		t.Fatalf("retry rewrote sticky binding to %s; want unset so the key re-resolves via the ring", bound)
	}

	// The next request must re-resolve to the ring owner instead of staying
	// pinned to the fallback that absorbed the retry.
	again, err := rt.Route(key)
	if err != nil {
		t.Fatalf("Route again: %v", err)
	}
	if again.ID != preferred.ID {
		t.Fatalf("after retry, key routes to %s; want ring owner %s", again.ID, preferred.ID)
	}
}

// TestServe_RetrySuccessAfterExistingBinding preserves an existing binding to
// the preferred node across a transient failure: the retry must not rebind to
// the fallback, and once the preferred recovers the key returns to it.
func TestServe_RetrySuccessAfterExistingBinding(t *testing.T) {
	rt, sess := newTestRouter(t, 3)

	key := "usr-1"
	preferred, err := rt.Route(key)
	if err != nil {
		t.Fatalf("Route: %v", err)
	}

	// Establish a binding to the preferred node with a successful first request.
	if err := rt.Serve(context.Background(), key, func(ctx context.Context, node *model.Node, k string) error {
		return nil
	}); err != nil {
		t.Fatalf("Serve initial: %v", err)
	}
	if sess.Bound(key) != preferred.ID {
		t.Fatalf("expected binding to %s, got %q", preferred.ID, sess.Bound(key))
	}

	// Now the preferred node fails once; the retry succeeds on a fallback.
	if err := rt.Serve(context.Background(), key, func(ctx context.Context, node *model.Node, k string) error {
		if node.ID == preferred.ID {
			return fmt.Errorf("injected failure on %s", node.ID)
		}
		return nil
	}); err != nil {
		t.Fatalf("Serve retry: %v", err)
	}

	// The binding must not have migrated to the fallback. It is either cleared
	// (so the next request re-resolves) or still the preferred node — but never
	// a different node.
	if bound := sess.Bound(key); bound != "" && bound != preferred.ID {
		t.Fatalf("retry migrated sticky binding to %s; want unset or %s", bound, preferred.ID)
	}

	// After the transient failure, the key must still route back to the ring
	// owner rather than being pinned to the fallback.
	again, err := rt.Route(key)
	if err != nil {
		t.Fatalf("Route again: %v", err)
	}
	if again.ID != preferred.ID {
		t.Fatalf("after retry, key routes to %s; want ring owner %s", again.ID, preferred.ID)
	}
}
