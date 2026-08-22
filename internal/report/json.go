package report

import "encoding/json"

// JSON renders the snapshot as an indented JSON document.
func (s *Snapshot) JSON() ([]byte, error) {
	return json.MarshalIndent(struct {
		Entries []Entry `json:"entries"`
	}{Entries: s.Entries}, "", "  ")
}

// AddRingVNodes fills the RingVNodes column from a vnode distribution map.
func (s *Snapshot) AddRingVNodes(distribution map[string]int) {
	for i := range s.Entries {
		s.Entries[i].RingVNodes = distribution[s.Entries[i].NodeID]
	}
}
