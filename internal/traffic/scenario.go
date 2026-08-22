package traffic

import (
	"context"
	"fmt"
	"sync"
)

// Outcome is one request result recorded by a Scenario.
type Outcome struct {
	Key     string
	NodeID  string
	OK      bool
	Retries int
}

// Scenario drives a deterministic sequence of keys through a serve function and
// records per-key outcomes.
type Scenario struct {
	Serve func(ctx context.Context, key string) (nodeID string, retries int, err error)
}

// Run sends requests for keys key0..key0+count and returns all outcomes.
func (s *Scenario) Run(ctx context.Context, keyPrefix string, count int) []Outcome {
	out := make([]Outcome, 0, count)
	for i := 0; i < count; i++ {
		key := Key(keyPrefix, i+1)
		nodeID, retries, err := s.Serve(ctx, key)
		out = append(out, Outcome{
			Key:     key,
			NodeID:  nodeID,
			OK:      err == nil,
			Retries: retries,
		})
	}
	return out
}

// ParallelRun drives several workers through the scenario concurrently.
func (s *Scenario) ParallelRun(ctx context.Context, keyPrefix string, count, workers int) []Outcome {
	if workers <= 0 {
		workers = 1
	}
	results := make([]Outcome, count)
	var wg sync.WaitGroup
	next := make(chan int)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range next {
				key := Key(keyPrefix, i+1)
				nodeID, retries, err := s.Serve(ctx, key)
				results[i] = Outcome{Key: key, NodeID: nodeID, OK: err == nil, Retries: retries}
			}
		}()
	}
	for i := 0; i < count; i++ {
		next <- i
	}
	close(next)
	wg.Wait()
	return results
}

// Summary aggregates scenario outcomes.
func Summary(outcomes []Outcome) (total, ok, failed, retried int) {
	total = len(outcomes)
	for _, o := range outcomes {
		if o.OK {
			ok++
		} else {
			failed++
		}
		if o.Retries > 0 {
			retried++
		}
	}
	return
}

// Describe renders a one-line scenario summary.
func Describe(outcomes []Outcome) string {
	total, ok, failed, retried := Summary(outcomes)
	return fmt.Sprintf("total=%d ok=%d failed=%d retried=%d", total, ok, failed, retried)
}
