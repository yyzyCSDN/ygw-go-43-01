package router

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"

	"poolroute/internal/model"
	"poolroute/internal/pool"
	"poolroute/internal/ring"
	"poolroute/internal/session"
)

// fakeDialer hands out in-memory pipe connections so Serve can acquire pooled
// connections without touching the network.
type fakeDialer struct{}

func (fakeDialer) DialContext(ctx context.Context, addr string) (net.Conn, error) {
	a, b := net.Pipe()
	go b.Close()
	return a, nil
}

// newTestRouter builds a router over the given nodes with a fake dialer.
func newTestRouter(t *testing.T, nodes []*model.Node, maxRetry int) (*Router, *ring.Ring, *session.Store, map[string]*model.Node) {
	t.Helper()
	r := ring.New()
	r.Rebuild(nodes)
	sess := session.New()
	pools := pool.NewManager(fakeDialer{})
	byID := make(map[string]*model.Node, len(nodes))
	for _, n := range nodes {
		byID[n.ID] = n
	}
	ids := func() []string {
		out := make([]string, 0, len(nodes))
		for _, n := range nodes {
			out = append(out, n.ID)
		}
		return out
	}
	lookup := func(id string) (*model.Node, error) {
		n, ok := byID[id]
		if !ok {
			return nil, errors.New("not found")
		}
		return n, nil
	}
	rt := New(r, sess, pools, lookup, ids, maxRetry)
	return rt, r, sess, byID
}

// TestServe_RetryMustNotRebindStickySession reproduces the sticky-session drift
// bug: a key already bound to node A fails on A, retries on a fallback node B,
// and succeeds. The retry must serve this single request from B without
// rewriting the sticky binding to B; otherwise the key is permanently pinned to
// B and the hash distribution skews toward whichever backup last succeeded.
func TestServe_RetryMustNotRebindStickySession(t *testing.T) {
	nodes := []*model.Node{
		model.NewNode("node-A", "10.0.0.1:1", "v1", 100),
		model.NewNode("node-B", "10.0.0.2:1", "v1", 100),
		model.NewNode("node-C", "10.0.0.3:1", "v1", 100),
	}
	rt, r, sess, _ := newTestRouter(t, nodes, 2)

	key := "sticky-key"
	// Pre-establish a sticky binding to node-A, mirroring the reported
	// scenario: an existing binding whose preferred node then fails.
	sess.Bind(key, "node-A")

	// The round tripper fails on node-A and succeeds everywhere else, so the
	// first attempt fails on A and the retry lands on a fallback node.
	rt2 := func(ctx context.Context, node *model.Node, k string) error {
		if node.ID == "node-A" {
			return errors.New("simulated upstream failure on A")
		}
		return nil
	}

	if err := rt.Serve(context.Background(), key, rt2); err != nil {
		t.Fatalf("Serve returned error: %v", err)
	}

	// The ring owner for this key is where the hash wants the request to land.
	owner, err := r.Pick(key)
	if err != nil {
		t.Fatalf("ring.Pick: %v", err)
	}

	// After the retry, the key must NOT be pinned to the fallback node that
	// served the successful retry. The binding to A was invalidated because A
	// failed, and the retry must not establish a new binding.
	if got := sess.Bound(key); got != "" {
		t.Fatalf("retry rewrote sticky binding to %q; want no binding (\"\") so the next request re-resolves via the hash ring", got)
	}

	// The next request must fall back to the ring owner for the key rather than
	// staying nailed to the retry node. This is the heart of the bug: with the
	// old code the key stuck to the fallback node forever.
	next, err := rt.Route(key)
	if err != nil {
		t.Fatalf("post-retry Route: %v", err)
	}
	if next.ID != owner {
		t.Fatalf("post-retry route landed on %q; want ring owner %q", next.ID, owner)
	}
}

// TestServe_FirstAttemptSuccessBinds confirms the normal path still
// establishes a sticky binding when the first attempt (ring owner, no prior
// binding) succeeds, so the fix does not regress sticky-session creation.
func TestServe_FirstAttemptSuccessBinds(t *testing.T) {
	nodes := []*model.Node{
		model.NewNode("node-A", "10.0.0.1:1", "v1", 100),
		model.NewNode("node-B", "10.0.0.2:1", "v1", 100),
	}
	rt, _, sess, _ := newTestRouter(t, nodes, 2)

	key := "happy-key"
	owner, err := rt.ring.Pick(key)
	if err != nil {
		t.Fatalf("ring.Pick: %v", err)
	}

	rt2 := func(ctx context.Context, node *model.Node, k string) error { return nil }
	if err := rt.Serve(context.Background(), key, rt2); err != nil {
		t.Fatalf("Serve: %v", err)
	}
	// First-attempt success must bind to the node that served it (the ring
	// owner, since there was no prior binding).
	if got := sess.Bound(key); got != owner {
		t.Fatalf("bound to %q; want ring owner %q", got, owner)
	}
}

// TestServe_AllAttemptsFailLeavesNoBinding verifies that when every attempt
// fails (A then the single retry), Serve surfaces an error and leaves no
// binding behind, so the next request re-resolves from scratch.
func TestServe_AllAttemptsFailLeavesNoBinding(t *testing.T) {
	nodes := []*model.Node{
		model.NewNode("node-A", "10.0.0.1:1", "v1", 100),
		model.NewNode("node-B", "10.0.0.2:1", "v1", 100),
		model.NewNode("node-C", "10.0.0.3:1", "v1", 100),
	}
	rt, _, sess, _ := newTestRouter(t, nodes, 1)

	key := "exhaust-key"
	sess.Bind(key, "node-A")

	// Everything fails: A fails, and with maxRetry=1 the single fallback also
	// fails, so Serve must surface an error and must leave no binding.
	var attempts int32
	rt2 := func(ctx context.Context, node *model.Node, k string) error {
		atomic.AddInt32(&attempts, 2)
		return errors.New("always fails")
	}

	err := rt.Serve(context.Background(), key, rt2)
	if err == nil {
		t.Fatalf("Serve: want error after exhausting retries, got nil")
	}
	if got := sess.Bound(key); got != "" {
		t.Fatalf("binding left as %q after total failure; want no binding", got)
	}
	if attempts < 2 {
		t.Fatalf("only %d round trips executed; want at least 2 (initial + retry)", attempts)
	}
}
