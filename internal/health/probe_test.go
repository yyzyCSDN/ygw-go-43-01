package health

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"poolroute/internal/model"
)

// stubDialer lets a test observe how DialContext is driven: it records the
// context passed in, optionally blocks until that context is cancelled, and
// reports whether the dial goroutine actually exited.
type stubDialer struct {
	gotCtx    context.Context
	block     chan struct{}
	goroutine atomic.Int32
}

func (d *stubDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	d.gotCtx = ctx
	d.goroutine.Add(1)
	defer d.goroutine.Add(-1)
	// Hold the dial open until the caller releases the gate or the context
	// expires. If the context never propagates a timeout/cancel, this blocks
	// forever and the test's deadline catches it.
	select {
	case <-d.block:
		return nil, errors.New("dial closed by test")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// TestProbeTimeoutPropagatesToDial proves the per-probe timeout reaches the
// dial: a dial that never returns on its own must be cancelled by Probe, the
// dial goroutine must exit promptly, and the resulting ProbeResult must be
// marked stale so it is not mistaken for live node state.
func TestProbeTimeoutPropagatesToDial(t *testing.T) {
	d := &stubDialer{block: make(chan struct{})}
	p := NewProber(ProbeOptions{
		Interval: time.Second,
		Timeout:  20 * time.Millisecond,
		Dialer:   d,
	})

	done := make(chan model.ProbeResult, 1)
	go func() { done <- p.Probe(context.Background(), model.NewNode("n1", "127.0.0.1:1", "v1", 1)) }()

	select {
	case res := <-done:
		if !res.Stale {
			t.Fatalf("timed-out probe must be stale, got %+v", res)
		}
		if res.OK {
			t.Fatalf("timed-out probe must not report OK")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Probe did not return after its timeout; timeout never reached the dial")
	}

	// The dial goroutine must have been released by the context cancellation
	// rather than left dangling.
	deadlineCh := time.After(time.Second)
	for {
		if d.goroutine.Load() == 0 {
			break
		}
		select {
		case <-deadlineCh:
			t.Fatal("dial goroutine leaked: context cancellation did not propagate to the dial")
		default:
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// cancelOnceDialer blocks the dial until the probe context is cancelled and
// then records the cancellation cause path it observed.
type cancelOnceDialer struct {
	goroutine atomic.Int32
	sawCancel  atomic.Bool
}

func (d *cancelOnceDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	d.goroutine.Add(1)
	defer d.goroutine.Add(-1)
	<-ctx.Done()
	d.sawCancel.Store(true)
	return nil, ctx.Err()
}

// TestProbeParentCancelPropagates proves that cancelling the parent context
// (e.g. loop shutdown) also tears down the in-flight dial, not only the
// per-probe timeout.
func TestProbeParentCancelPropagates(t *testing.T) {
	d := &cancelOnceDialer{}
	p := NewProber(ProbeOptions{
		Interval: time.Second,
		Timeout:  30 * time.Minute, // long: only parent cancel should fire
		Dialer:   d,
	})

	parentCtx, parentCancel := context.WithCancel(context.Background())
	done := make(chan model.ProbeResult, 1)
	go func() { done <- p.Probe(parentCtx, model.NewNode("n1", "127.0.0.1:1", "v1", 1)) }()

	// Give the dial time to start, then cancel the parent.
	time.Sleep(20 * time.Millisecond)
	parentCancel()

	select {
	case res := <-done:
		if !res.Stale {
			t.Fatalf("parent-cancelled probe must be stale, got %+v", res)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Probe did not return after parent cancel")
	}

	deadlineCh := time.After(time.Second)
	for {
		if d.goroutine.Load() == 0 {
			break
		}
		select {
		case <-deadlineCh:
			t.Fatal("dial goroutine leaked after parent cancel")
		default:
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !d.sawCancel.Load() {
		t.Fatal("dial never observed context cancellation")
	}
}

// fastDialer succeeds immediately. Its results must not be marked stale.
func fastDialer() Dialer {
	return &instantDialer{}
}

type instantDialer struct{}

func (d *instantDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	c, _ := net.Pipe()
	return c, nil
}

// TestProbeSuccessNotStale proves the stale flag is not set on a normal
// in-time probe, so genuine results still flow into node state.
func TestProbeSuccessNotStale(t *testing.T) {
	p := NewProber(ProbeOptions{
		Interval: time.Second,
		Timeout:  time.Second,
		Dialer:   fastDialer(),
	})
	res := p.Probe(context.Background(), model.NewNode("n1", "127.0.0.1:1", "v1", 1))
	if res.Stale {
		t.Fatalf("in-time probe must not be stale, got %+v", res)
	}
	if !res.OK {
		t.Fatalf("fast probe must report OK")
	}
}

// TestNetDialerHonorsContextCancellation checks the real NetDialer aborts a
// dial when its context is already cancelled, rather than ignoring the context
// and proceeding to a real connect (regression for the "_ = ctx" bug). With a
// pre-cancelled context a context-aware dialer must report context.Canceled;
// the buggy Dial() path dials on context.Background() and would never surface
// the cancellation.
func TestNetDialerHonorsContextCancellation(t *testing.T) {
	d := NetDialer{Timeout: 30 * time.Minute}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	go func() {
		_, err := d.DialContext(ctx, "tcp", "240.0.0.1:80")
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("NetDialer ignored context cancellation and kept dialing")
	}
}
