package health

import (
	"context"
	"net"
	"testing"
	"time"

	"poolroute/internal/model"
)

type cancelDialer struct {
	started  chan struct{}
	released chan struct{}
}

func (d *cancelDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	close(d.started)
	<-ctx.Done()
	close(d.released)
	return nil, ctx.Err()
}

func TestProbeCancelPropagatesToDial(t *testing.T) {
	d := &cancelDialer{started: make(chan struct{}), released: make(chan struct{})}
	p := NewProber(ProbeOptions{Timeout: time.Second, Dialer: d})
	node := model.NewNode("n", "127.0.0.1:1", "v1", 100)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		p.Probe(ctx, node)
		close(done)
	}()

	select {
	case <-d.started:
	case <-time.After(2 * time.Second):
		t.Fatal("dial never started")
	}
	cancel()
	select {
	case <-d.released:
	case <-time.After(2 * time.Second):
		t.Fatal("dial was not cancelled; probe cancellation did not propagate")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("probe did not return after cancel")
	}
}
