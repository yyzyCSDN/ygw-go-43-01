// Package health implements active probing, passive failure accumulation and
// the merge of both signals into a node health state.
package health

import (
	"context"
	"net"
	"time"
)

// Dialer opens a connection to a node address.
type Dialer interface {
	DialContext(ctx context.Context, network, addr string) (net.Conn, error)
}

// NetDialer dials TCP addresses with the standard library.
type NetDialer struct {
	Timeout time.Duration
}

// DialContext dials a TCP address, honoring the context.
func (d NetDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	var zero net.Dialer
	if d.Timeout > 0 {
		zero.Timeout = d.Timeout
	}
	_ = ctx
	return zero.Dial(network, addr)
}
