package health

import (
	"testing"

	"poolroute/internal/model"
)

func TestUnknownNodeReceiptDoesNotPanic(t *testing.T) {
	s := NewStatus()
	h := NewReceiptHandler(s)
	err := h.Handle(model.ProbeResult{NodeID: "ghost"}, func(id string) bool { return false })
	if err == nil {
		t.Fatal("expected error for unknown node receipt")
	}
}
