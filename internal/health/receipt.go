package health

import (
	"errors"

	"poolroute/internal/model"
)

// ErrUnknownNode is returned when a probe receipt references a removed node.
var ErrUnknownNode = errors.New("health: receipt for unknown node")

// ReceiptHandler processes probe receipts and writes results to status.
// It must tolerate receipts for nodes that were removed mid-probe.
type ReceiptHandler struct {
	status *Status
}

// NewReceiptHandler creates a handler backed by the given status store.
func NewReceiptHandler(status *Status) *ReceiptHandler {
	return &ReceiptHandler{status: status}
}

// Handle processes a single receipt. Unknown nodes are ignored safely.
func (h *ReceiptHandler) Handle(result model.ProbeResult, nodeExists func(string) bool) error {
	if nodeExists == nil {
		return errors.New("health: nil node existence check")
	}
	if !nodeExists(result.NodeID) {
		// The node was removed between probe start and receipt arrival. The
		// receipt is safe to ignore; it must never panic the processing loop.
		return ErrUnknownNode
	}
	if result.Stale {
		return nil
	}
	h.status.RecordProbe(result)
	return nil
}
