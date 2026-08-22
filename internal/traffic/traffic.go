// Package traffic runs a deterministic TCP load generator against a pool of
// upstream listeners with pluggable fault injection, so routing behaviour can
// be exercised with real network round trips.
package traffic

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"
)

// Upstream is a fake upstream service that counts served requests.
type Upstream struct {
	ln     net.Listener
	addr   string
	mu     sync.Mutex
	served int
	fail   bool
	delay  time.Duration
}

// StartUpstream starts a TCP echo-style upstream on 127.0.0.1:0.
func StartUpstream() (*Upstream, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	u := &Upstream{ln: ln, addr: ln.Addr().String()}
	go u.serve()
	return u, nil
}

// Addr returns the upstream listen address.
func (u *Upstream) Addr() string { return u.addr }

// Served returns how many requests the upstream completed.
func (u *Upstream) Served() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.served
}

// SetFault toggles whether the upstream fails every request.
func (u *Upstream) SetFault(fail bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.fail = fail
}

// SetDelay sets a per-request processing delay.
func (u *Upstream) SetDelay(d time.Duration) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.delay = d
}

func (u *Upstream) serve() {
	for {
		conn, err := u.ln.Accept()
		if err != nil {
			return
		}
		go u.handle(conn)
	}
}

func (u *Upstream) handle(conn net.Conn) {
	defer conn.Close()
	u.mu.Lock()
	fail, delay := u.fail, u.delay
	u.mu.Unlock()
	if delay > 0 {
		time.Sleep(delay)
	}
	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil && err != io.EOF {
		return
	}
	if fail {
		_, _ = conn.Write([]byte("ERR\n"))
		return
	}
	u.mu.Lock()
	u.served++
	u.mu.Unlock()
	_, _ = conn.Write([]byte("OK " + line))
}

// Close stops the upstream listener.
func (u *Upstream) Close() error { return u.ln.Close() }

// Client sends deterministic load to an upstream through the router.
type Client struct {
	timeout time.Duration
}

// NewClient creates a load client.
func NewClient(timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return &Client{timeout: timeout}
}

// RoundTrip sends one request to a node address and reports success.
func (c *Client) RoundTrip(ctx context.Context, addr, key string) error {
	d := net.Dialer{Timeout: c.timeout}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(c.timeout))
	if _, err := fmt.Fprintf(conn, "GET %s\n", key); err != nil {
		return err
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return err
	}
	if len(line) >= 3 && line[:3] == "ERR" {
		return fmt.Errorf("upstream error for %s", key)
	}
	return nil
}

// Key builds a deterministic routing key.
func Key(prefix string, seq int) string {
	return prefix + strconv.Itoa(seq)
}
