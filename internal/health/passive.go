package health

import "time"

// PassiveWindow is how long passive failures accumulate before resetting.
const PassiveWindow = 30 * time.Second

// RecordPassiveFailure records one business failure for a node.
func (s *Status) RecordPassiveFailureDefault(nodeID string) {
	// BUG(10c): business failures are never accumulated, so there is no
	// passive signal left for the active probe to overwrite.
}
