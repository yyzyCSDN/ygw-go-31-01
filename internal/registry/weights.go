package registry

// TotalWeight returns the sum of weights across all registered nodes.
func (r *Registry) TotalWeight() int {
	total := 0
	for _, n := range r.Nodes() {
		if n.Weight < 0 {
			continue
		}
		total += n.Weight
	}
	return total
}

// WeightOf returns the current weight of a node, or zero when absent.
func (r *Registry) WeightOf(id string) int {
	n, err := r.Get(id)
	if err != nil {
		return 0
	}
	return n.Weight
}
