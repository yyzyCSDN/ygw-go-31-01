// Package model defines the core domain types shared by the control plane:
// upstream nodes, connections, sticky sessions and health records.
package model

import (
	"fmt"
	"sync/atomic"
)

// NodeState is the lifecycle state of an upstream node.
type NodeState int

const (
	// Healthy nodes take traffic normally.
	Healthy NodeState = iota
	// Suspected nodes are marked by passive failures but still take traffic.
	Suspected
	// Unhealthy nodes are excluded from routing.
	Unhealthy
	// Draining nodes finish in-flight connections and stop accepting new ones.
	Draining
	// Offline nodes are removed from the registry and ring.
	Offline
)

func (s NodeState) String() string {
	switch s {
	case Healthy:
		return "healthy"
	case Suspected:
		return "suspected"
	case Unhealthy:
		return "unhealthy"
	case Draining:
		return "draining"
	case Offline:
		return "offline"
	default:
		return "unknown"
	}
}

// TakesTraffic reports whether a node in this state may receive new requests.
func (s NodeState) TakesTraffic() bool {
	return s == Healthy || s == Suspected
}

// Node is an upstream service instance registered in the control plane.
type Node struct {
	ID      string
	Addr    string
	Weight  int
	Version string
	state   atomic.Int32
}

// NewNode creates a node in the Healthy state with the given weight.
func NewNode(id, addr, version string, weight int) *Node {
	n := &Node{ID: id, Addr: addr, Weight: weight, Version: version}
	n.state.Store(int32(Healthy))
	return n
}

// State returns the current lifecycle state.
func (n *Node) State() NodeState {
	return NodeState(n.state.Load())
}

// SetState transitions the node state and returns the new state.
func (n *Node) SetState(s NodeState) NodeState {
	n.state.Store(int32(s))
	return s
}

// TakesTraffic reports whether this node may take new requests right now.
func (n *Node) TakesTraffic() bool {
	return n.State().TakesTraffic()
}

// String renders a stable identity for logs and reports.
func (n *Node) String() string {
	return fmt.Sprintf("%s(%s/%s)", n.ID, n.Addr, n.State())
}
