package circuit

import (
	"context"
	"errors"

	"poolroute/internal/model"
)

// Acquirer is the connection-pool surface the half-open trial needs.
type Acquirer interface {
	Acquire(ctx context.Context, node *model.Node) (*model.Conn, error)
	Release(conn *model.Conn, servedBy *model.Node) error
}

// RoundTripper executes one trial request over a raw connection.
type RoundTripper func(raw any) error

// ErrNoTrial is returned when the breaker is not in half-open state.
var ErrNoTrial = errors.New("circuit: not half-open")

// HalfOpenTrial runs exactly one trial request through the connection pool so
// a half-open probe never leaks connections by dialing upstream directly.
func (b *Breaker) HalfOpenTrial(ctx context.Context, node *model.Node, pool Acquirer, rt RoundTripper) error {
	if b.State() != HalfOpen {
		return ErrNoTrial
	}
	conn, err := pool.Acquire(ctx, node)
	if err != nil {
		return err
	}
	// Return the trial connection to the pool once the probe finishes so the
	// half-open path reuses pooled connections instead of dialling upstream
	// directly and abandoning the connection to leak the pool one probe at a
	// time.
	defer pool.Release(conn, node)
	return rt(conn.Raw)
}
