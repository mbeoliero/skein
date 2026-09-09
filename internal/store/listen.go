package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Listener is the one dedicated LISTEN connection of a process (§2.7). It is taken
// out of the pool for good: a pooled connection would be handed to other callers
// between notifications, and the pool's health checks would race WaitForNotification.
type Listener struct {
	conn *pgx.Conn
}

// Listen hijacks a pool connection and subscribes to the schema's channel. The
// channel name is the schema name itself (the trigger sends on TG_TABLE_SCHEMA);
// LISTEN takes an identifier, so like CREATE SCHEMA it is the one statement built
// from the quoted name instead of a query parameter.
func (s *Store) Listen(ctx context.Context) (*Listener, error) {
	c, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	conn := c.Hijack()
	if _, err := conn.Exec(ctx, "LISTEN "+s.path); err != nil {
		cctx, cancel := cleanupCtx(ctx)
		defer cancel()
		_ = conn.Close(cctx)
		return nil, fmt.Errorf("store: listen: %w", err)
	}
	return &Listener{conn: conn}, nil
}

// Wait blocks for the next notification and returns its payload. Any error ends the
// subscription: the caller closes and reconnects.
func (l *Listener) Wait(ctx context.Context) (string, error) {
	n, err := l.conn.WaitForNotification(ctx)
	if err != nil {
		return "", err
	}
	return n.Payload, nil
}

// Close ends the connection on the bounded cleanup deadline: the caller's ctx is
// usually already cancelled when it gets here.
func (l *Listener) Close() {
	ctx, cancel := cleanupCtx(context.Background())
	defer cancel()
	_ = l.conn.Close(ctx)
}
