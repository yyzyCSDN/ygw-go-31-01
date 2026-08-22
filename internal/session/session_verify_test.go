package session

import "testing"

func TestStickyBindingInvalidatedOnEviction(t *testing.T) {
	s := New()
	s.Bind("k", "A")
	if n := s.OnNodeEvicted("A"); n == 0 {
		t.Fatal("expected eviction to invalidate the sticky binding")
	}
	if got := s.Bound("k"); got != "" {
		t.Fatalf("stale binding survived eviction: %q", got)
	}
}
