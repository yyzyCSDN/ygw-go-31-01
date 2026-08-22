package model

import "time"

// ProbeResult is the outcome of one active health probe against a node.
type ProbeResult struct {
	NodeID  string
	OK      bool
	Latency time.Duration
	Err     error
	Stale   bool
}

// PassiveFailure accumulates business-level failures observed on a node.
type PassiveFailure struct {
	NodeID     string
	Count      int
	WindowStart time.Time
}

// HealthSnapshot is the merged view of active and passive health signals.
type HealthSnapshot struct {
	NodeID       string
	State        NodeState
	LastProbeOK  bool
	PassiveCount int
	UpdatedAt    time.Time
}
