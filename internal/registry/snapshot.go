package registry

import (
	"time"

	"poolroute/internal/model"
)

// Entry is an immutable registry snapshot row.
type Entry struct {
	ID      string
	Addr    string
	State   string
	Weight  int
	Version string
}

// Snapshot returns an immutable copy of the registry.
func (r *Registry) Snapshot() []Entry {
	nodes := r.Nodes()
	out := make([]Entry, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, Entry{
			ID:      n.ID,
			Addr:    n.Addr,
			State:   n.State().String(),
			Weight:  n.Weight,
			Version: n.Version,
		})
	}
	return out
}

// ChangeHook receives notifications about registry mutations.
type ChangeHook func(entries []Entry)

// Registrar wraps a registry with a change hook so consumers (for example the
// ring) can react to membership and state changes.
type Registrar struct {
	reg  *Registry
	hook ChangeHook
}

// NewRegistrar creates a registrar that calls hook on every mutation.
func NewRegistrar(reg *Registry, hook ChangeHook) *Registrar {
	return &Registrar{reg: reg, hook: hook}
}

// Add registers a node and notifies the hook.
func (r *Registrar) Add(n *model.Node) error {
	if err := r.reg.Add(n); err != nil {
		return err
	}
	r.hook(r.reg.Snapshot())
	return nil
}

// Remove deletes a node and notifies the hook.
func (r *Registrar) Remove(id string) error {
	if err := r.reg.Remove(id); err != nil {
		return err
	}
	r.hook(r.reg.Snapshot())
	return nil
}

// Transition changes a node state and notifies the hook.
func (r *Registrar) Transition(id string, next model.NodeState) error {
	if err := r.reg.Transition(id, next); err != nil {
		return err
	}
	r.hook(r.reg.Snapshot())
	return nil
}

// SetWeight changes a node weight and notifies the hook.
func (r *Registrar) SetWeight(id string, weight int) error {
	if err := r.reg.SetWeight(id, weight); err != nil {
		return err
	}
	r.hook(r.reg.Snapshot())
	return nil
}

// TimeNow is exposed for deterministic snapshot timestamps in tests.
var TimeNow = time.Now
