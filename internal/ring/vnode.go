package ring

import (
	"fmt"

	"github.com/spaolacci/murmur3"
)

// hashKey hashes a string key to a 32-bit ring position.
func hashKey(key string) uint32 {
	return murmur3.Sum32([]byte(key))
}

// vnodeName produces a stable per-vnode key so the same vnode maps to the same
// hash across rebuilds.
func vnodeName(nodeID string, index int) string {
	return fmt.Sprintf("%s#%d", nodeID, index)
}

// appendVNodes adds weight*vnodesPerUnit virtual nodes for a node.
func appendVNodes(dst []vnode, nodeID string, weight, perUnit int) []vnode {
	total := weight * perUnit
	if total < 0 {
		return dst
	}
	for i := 0; i < total; i++ {
		dst = append(dst, vnode{
			hash:   hashKey(vnodeName(nodeID, i)),
			nodeID: nodeID,
		})
	}
	return dst
}
