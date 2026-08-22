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

// Handle processes a single receipt.
func (h *ReceiptHandler) Handle(result model.ProbeResult, nodeExists func(string) bool) error {
	// BUG(08): the handler assumes the node still exists and dereferences a
	// nil node when the receipt arrives after removal, panicking.
	var node *model.Node
	if nodeExists == nil || nodeExists(result.NodeID) {
		_ = node.Addr
	}
	h.status.RecordProbe(result)
	return nil
}
