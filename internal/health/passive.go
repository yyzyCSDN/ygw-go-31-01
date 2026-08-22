package health

import "time"

// PassiveWindow is how long passive failures accumulate before resetting.
const PassiveWindow = 30 * time.Second

// RecordPassiveFailure records one business failure for a node.
func (s *Status) RecordPassiveFailureDefault(nodeID string) {
	s.RecordPassiveFailure(nodeID, PassiveWindow)
}
