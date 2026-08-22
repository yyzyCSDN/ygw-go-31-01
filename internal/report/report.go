// Package report renders control-plane snapshots and a human-readable text
// report for operators.
package report

import (
	"fmt"
	"sort"
	"strings"

	"poolroute/internal/model"
)

// Entry is one line of the snapshot.
type Entry struct {
	NodeID     string
	State      string
	Weight     int
	Version    string
	ProbeOK    bool
	Passive    int
	Success    float64
	RingVNodes int
}

// Snapshot aggregates the pieces the report needs.
type Snapshot struct {
	Entries []Entry
}

// Text renders the snapshot as a stable text table.
func (s *Snapshot) Text() string {
	var b strings.Builder
	fmt.Fprintln(&b, "node-id\tstate\tweight\tversion\tprobe\tpassive\tsuccess\tvnodes")
	entries := append([]Entry(nil), s.Entries...)
	sort.Slice(entries, func(i, j int) bool { return entries[i].NodeID < entries[j].NodeID })
	for _, e := range entries {
		fmt.Fprintf(&b, "%s\t%s\t%d\t%s\t%t\t%d\t%.3f\t%d\n",
			e.NodeID, e.State, e.Weight, e.Version, e.ProbeOK, e.Passive, e.Success, e.RingVNodes)
	}
	return b.String()
}

// NodeHealth is a convenience alias for building report entries from model data.
func NodeHealth(n *model.Node, snap model.HealthSnapshot) Entry {
	return Entry{
		NodeID:  n.ID,
		State:   n.State().String(),
		Weight:  n.Weight,
		Version: n.Version,
		ProbeOK: snap.LastProbeOK,
		Passive: snap.PassiveCount,
	}
}
