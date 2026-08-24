package health

import (
	"context"
	"errors"
	"sync"
	"time"

	"poolroute/internal/model"
)

// ProbeOptions configures the active probe loop.
type ProbeOptions struct {
	Interval time.Duration
	Timeout  time.Duration
	Dialer   Dialer
}

// Prober runs active health probes for every node.
type Prober struct {
	opts ProbeOptions
	mu   sync.Mutex
	// pending counts in-flight probes so tests can detect leaked goroutines.
	pending int
}

// NewProber creates a prober with the given options.
func NewProber(opts ProbeOptions) *Prober {
	if opts.Interval <= 0 {
		opts.Interval = 5 * time.Second
	}
	if opts.Timeout <= 0 {
		opts.Timeout = time.Second
	}
	return &Prober{opts: opts}
}

// Probe runs one probe against a node and returns the result. It enforces
// p.opts.Timeout via a child context so that a timeout cancels the in-flight
// dial all the way down rather than leaving it dangling. A result produced
// after the deadline is marked stale so callers can drop it instead of letting
// a late response overwrite fresh node state.
func (p *Prober) Probe(ctx context.Context, node *model.Node) model.ProbeResult {
	p.mu.Lock()
	p.pending++
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		p.pending--
		p.mu.Unlock()
	}()

	timeout := p.opts.Timeout
	if timeout <= 0 {
		timeout = time.Second
	}
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	start := time.Now()
	conn, err := p.opts.Dialer.DialContext(probeCtx, "tcp", node.Addr)
	latency := time.Since(start)
	if err != nil {
		stale := errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
		return model.ProbeResult{NodeID: node.ID, OK: false, Latency: latency, Err: err, Stale: stale}
	}
	_ = conn.Close()
	return model.ProbeResult{NodeID: node.ID, OK: true, Latency: latency}
}

// Pending returns the number of in-flight probes.
func (p *Prober) Pending() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pending
}
