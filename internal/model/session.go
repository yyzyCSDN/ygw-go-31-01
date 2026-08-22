package model

// StickyBinding pins a routing key to a specific upstream node.
type StickyBinding struct {
	Key    string
	NodeID string
}
