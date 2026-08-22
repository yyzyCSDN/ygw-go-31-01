// Package session keeps sticky key-to-node bindings for the router.
package session

import "sync"

// Store is a concurrency-safe sticky binding map.
type Store struct {
	mu    sync.RWMutex
	binds map[string]string
}

// New creates an empty store.
func New() *Store {
	return &Store{binds: make(map[string]string)}
}

// Bound returns the node a key is pinned to, or "".
func (s *Store) Bound(key string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.binds[key]
}

// Bind pins a key to a node.
func (s *Store) Bind(key, nodeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.binds[key] = nodeID
}

// Invalidate removes the binding for a key.
func (s *Store) Invalidate(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.binds, key)
}

// InvalidateNode removes every binding that points at a node.
func (s *Store) InvalidateNode(nodeID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for k, v := range s.binds {
		if v == nodeID {
			delete(s.binds, k)
			n++
		}
	}
	return n
}

// Keys returns all bound keys.
func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.binds))
	for k := range s.binds {
		out = append(out, k)
	}
	return out
}

// Count returns the number of active bindings.
func (s *Store) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.binds)
}

// BoundTo returns every key currently bound to a node.
func (s *Store) BoundTo(nodeID string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0)
	for k, v := range s.binds {
		if v == nodeID {
			out = append(out, k)
		}
	}
	return out
}
