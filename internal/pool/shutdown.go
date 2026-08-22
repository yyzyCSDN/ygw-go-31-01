package pool

// Shutdown closes every pool in the manager, releasing all idle connections.
func (m *PoolManager) Shutdown() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, p := range m.pools {
		p.drain()
	}
}

// TotalIdle returns the sum of idle connections across all pools.
func (m *PoolManager) TotalIdle() int {
	stats := m.Stats()
	total := 0
	for _, pair := range stats {
		total += pair[0]
	}
	return total
}
