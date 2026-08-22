package health

import (
	"sync"
	"time"

	"poolroute/internal/model"
)

// Status holds the merged active/passive health view per node.
type Status struct {
	mu      sync.Mutex
	records map[string]*record
	now     func() time.Time
}

type record struct {
	lastProbeOK   bool
	passiveCount  int
	passiveWindow time.Time
	updatedAt     time.Time
}

// NewStatus creates a health status store.
func NewStatus() *Status {
	return &Status{records: make(map[string]*record), now: time.Now}
}

// RecordProbe merges an active probe result into the node status.
func (s *Status) RecordProbe(result model.ProbeResult) {
	if result.Stale {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.ensure(result.NodeID)
	r.lastProbeOK = result.OK
	r.updatedAt = s.now()
}

// RecordPassiveFailure adds one passive failure to a node's window.
func (s *Status) RecordPassiveFailure(nodeID string, window time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.ensure(nodeID)
	now := s.now()
	if now.Sub(r.passiveWindow) > window {
		r.passiveCount = 0
		r.passiveWindow = now
	}
	r.passiveCount++
	r.updatedAt = now
}

// ResetPassive clears the passive failure counter for a node.
func (s *Status) ResetPassive(nodeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.records[nodeID]; ok {
		r.passiveCount = 0
		r.passiveWindow = s.now()
	}
}

// Snapshot returns the merged health view for a node.
func (s *Status) Snapshot(nodeID string) model.HealthSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.records[nodeID]
	if !ok {
		return model.HealthSnapshot{NodeID: nodeID, State: model.Healthy, UpdatedAt: s.now()}
	}
	return mergeHealth(nodeID, r, s.now())
}

func (s *Status) ensure(nodeID string) *record {
	r, ok := s.records[nodeID]
	if !ok {
		r = &record{passiveWindow: s.now()}
		s.records[nodeID] = r
	}
	return r
}

// mergeHealth combines the active probe signal and the passive failure count
// into a single health snapshot. Passive failures are sticky within their
// window: a single successful active probe must not clear an accumulated
// business-failure signal, otherwise a degraded node keeps taking traffic
// until it fails completely.
func mergeHealth(nodeID string, r *record, now time.Time) model.HealthSnapshot {
	state := model.Healthy
	if r.passiveCount > 0 {
		state = model.Suspected
	} else if !r.lastProbeOK {
		state = model.Unhealthy
	}
	return model.HealthSnapshot{
		NodeID:       nodeID,
		State:        state,
		LastProbeOK:  r.lastProbeOK,
		PassiveCount: r.passiveCount,
		UpdatedAt:    r.updatedAt,
	}
}
