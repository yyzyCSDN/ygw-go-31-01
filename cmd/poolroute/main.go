// Command poolroute starts a deterministic demo of the upstream routing
// control plane: it registers upstream nodes, routes synthetic requests with
// retries, runs active probes, exercises drain/restore and prints a report.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"poolroute/internal/circuit"
	"poolroute/internal/control"
	"poolroute/internal/health"
	"poolroute/internal/metric"
	"poolroute/internal/model"
	"poolroute/internal/pool"
	"poolroute/internal/registry"
	"poolroute/internal/report"
	"poolroute/internal/ring"
	"poolroute/internal/router"
	"poolroute/internal/session"
	"poolroute/internal/traffic"
)

func main() {
	requests := flag.Int("requests", 120, "number of deterministic requests to send")
	nodes := flag.Int("nodes", 3, "number of upstream nodes")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	reg := registry.New()
	r := ring.New()
	sess := session.New()
	metrics := metric.NewCollector(20)
	status := health.NewStatus()
	receipts := health.NewReceiptHandler(status)
	prober := health.NewProber(health.ProbeOptions{
		Interval: time.Second,
		Timeout:  500 * time.Millisecond,
		Dialer:   health.NetDialer{Timeout: 500 * time.Millisecond},
	})

	// The registrar rebuilds the hash ring whenever the registry changes so the
	// ring view always matches the set of healthy nodes.
	rebuild := func() {
		healthy := make([]*model.Node, 0)
		for _, n := range reg.Nodes() {
			if n.TakesTraffic() {
				healthy = append(healthy, n)
			}
		}
		r.Rebuild(healthy)
	}
	registrar := registry.NewRegistrar(reg, func([]registry.Entry) { rebuild() })

	upstreams := make(map[string]*traffic.Upstream)
	for i := 0; i < *nodes; i++ {
		u, err := traffic.StartUpstream()
		if err != nil {
			log.Fatal(err)
		}
		defer u.Close()
		id := fmt.Sprintf("node-%02d", i+1)
		upstreams[id] = u
		if err := registrar.Add(model.NewNode(id, u.Addr(), "v1.0.0", 100)); err != nil {
			log.Fatal(err)
		}
	}

	pools := pool.NewManager(netDialer{timeout: 2 * time.Second})
	rt := router.New(r, sess, pools,
		func(id string) (*model.Node, error) { return reg.Get(id) },
		func() []string { return nodeIDs(reg) },
		2)
	ops := control.NewOps(reg, pools.Drain)

	breakers := make(map[string]*circuit.Breaker)
	for _, n := range reg.Nodes() {
		breakers[n.ID] = circuit.New(5, 2*time.Second)
	}

	// Active probe loop feeds the merged health status.
	go prober.ProbeLoop(ctx, func() []*model.Node { return reg.Nodes() }, func(res model.ProbeResult) {
		receipts.Handle(res, func(id string) bool {
			_, err := reg.Get(id)
			return err == nil
		})
	})

	client := traffic.NewClient(2 * time.Second)
	scenario := &traffic.Scenario{
		Serve: func(sctx context.Context, key string) (string, int, error) {
			node, err := rt.Route(key)
			if err != nil {
				return "", 0, err
			}
			brk := breakers[node.ID]
			if !brk.Allow() {
				metrics.Record(node.ID, false)
				return node.ID, 0, fmt.Errorf("circuit open for %s", node.ID)
			}
			attempts := 0
			for {
				err = client.RoundTrip(sctx, node.Addr, key)
				attempts++
				if err == nil {
					brk.RecordSuccess()
					metrics.Record(node.ID, true)
					return node.ID, attempts - 1, nil
				}
				brk.RecordFailure()
				metrics.Record(node.ID, false)
				status.RecordPassiveFailureDefault(node.ID)
				if attempts > 2 {
					return node.ID, attempts - 1, err
				}
				fallback, ferr := rt.Fallback(key, node.ID)
				if ferr != nil {
					return node.ID, attempts - 1, err
				}
				node = fallback
			}
		},
	}

	outcomes := scenario.Run(ctx, "usr-", *requests)
	fmt.Println("scenario:", traffic.Describe(outcomes))

	// Demonstrate the drain/evict lifecycle and ring reconciliation.
	if len(reg.Nodes()) > 1 {
		if err := ops.Drain("node-01"); err == nil {
			rebuild()
			fmt.Println("drained node-01; ring nodes:", len(r.Distribution()))
		}
		if err := ops.Evict("node-01"); err == nil {
			rebuild()
			fmt.Println("evicted node-01; ring nodes:", len(r.Distribution()))
		}
	}

	entries := make([]report.Entry, 0, len(reg.Nodes()))
	for _, n := range reg.Nodes() {
		e := report.NodeHealth(n, status.Snapshot(n.ID))
		e.Success = metrics.NodeRate(n.ID)
		entries = append(entries, e)
	}
	snap := &report.Snapshot{Entries: entries}
	snap.AddRingVNodes(r.Distribution())
	if _, err := os.Stdout.WriteString(snap.Text()); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("total upstream served: %d\n", totalServed(upstreams))
}

func nodeIDs(reg *registry.Registry) []string {
	nodes := reg.Nodes()
	out := make([]string, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, n.ID)
	}
	return out
}

func totalServed(m map[string]*traffic.Upstream) int {
	total := 0
	for _, u := range m {
		total += u.Served()
	}
	return total
}

// netDialer adapts the health dialer to the pool dialer surface.
type netDialer struct {
	timeout time.Duration
}

func (d netDialer) DialContext(ctx context.Context, addr string) (net.Conn, error) {
	var zero net.Dialer
	zero.Timeout = d.timeout
	return zero.DialContext(ctx, "tcp", addr)
}
