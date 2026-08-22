package model

import (
	"net"
	"sync/atomic"
)

// ConnState is the lifecycle state of a pooled connection.
type ConnState int

const (
	// ConnIdle connections sit in a pool and can be acquired.
	ConnIdle ConnState = iota
	// ConnInUse connections are currently serving a request.
	ConnInUse
	// ConnDraining connections belong to a draining node and will be closed.
	ConnDraining
	// ConnClosed connections are no longer usable.
	ConnClosed
)

func (s ConnState) String() string {
	switch s {
	case ConnIdle:
		return "idle"
	case ConnInUse:
		return "in-use"
	case ConnDraining:
		return "draining"
	case ConnClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// Conn wraps a net.Conn with the identity of the node that owns it.
type Conn struct {
	Raw    net.Conn
	NodeID string
	state  atomic.Int32
}

// NewConn wraps a raw connection for a node.
func NewConn(raw net.Conn, nodeID string) *Conn {
	c := &Conn{Raw: raw, NodeID: nodeID}
	c.state.Store(int32(ConnIdle))
	return c
}

// State returns the current connection state.
func (c *Conn) State() ConnState {
	return ConnState(c.state.Load())
}

// SetState marks the connection with a new state.
func (c *Conn) SetState(s ConnState) {
	c.state.Store(int32(s))
}

// Close closes the underlying raw connection and marks the wrapper closed.
func (c *Conn) Close() error {
	c.state.Store(int32(ConnClosed))
	if c.Raw != nil {
		return c.Raw.Close()
	}
	return nil
}
