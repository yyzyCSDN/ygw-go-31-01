package health

import (
	"testing"

	"poolroute/internal/model"
)

func TestProbeSuccessDoesNotOverwritePassiveFailure(t *testing.T) {
	s := NewStatus()
	s.RecordPassiveFailureDefault("n")
	s.RecordProbe(model.ProbeResult{NodeID: "n", OK: true})
	snap := s.Snapshot("n")
	if snap.State != model.Suspected {
		t.Fatalf("state=%s, want suspected (probe success must not overwrite passive failures)", snap.State)
	}
}
