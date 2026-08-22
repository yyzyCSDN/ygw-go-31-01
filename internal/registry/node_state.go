package registry

import (
	"errors"

	"poolroute/internal/model"
)

// ErrInvalidTransition is returned when a lifecycle transition is not allowed.
var ErrInvalidTransition = errors.New("registry: invalid node state transition")

// allowedTransitions lists legal lifecycle edges for the node state machine.
var allowedTransitions = map[model.NodeState]map[model.NodeState]bool{
	model.Healthy: {
		model.Suspected: true,
		model.Unhealthy: true,
		model.Draining:  true,
		model.Offline:   true,
	},
	model.Suspected: {
		model.Healthy:   true,
		model.Unhealthy: true,
		model.Draining:  true,
		model.Offline:   true,
	},
	model.Unhealthy: {
		model.Healthy:   true,
		model.Suspected: true,
		model.Draining:  true,
		model.Offline:   true,
	},
	model.Draining: {
		model.Offline: true,
	},
	model.Offline: {},
}

// Transition moves a node from its current state to next, validating the edge.
func (r *Registry) Transition(id string, next model.NodeState) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	n, ok := r.nodes[id]
	if !ok {
		return ErrNotFound
	}
	cur := n.State()
	if cur == next {
		return nil
	}
	if !allowedTransitions[cur][next] {
		return ErrInvalidTransition
	}
	n.SetState(next)
	return nil
}
