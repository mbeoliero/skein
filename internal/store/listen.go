package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Listener owns a connection removed from the pool: reuse and health checks
// would race WaitForNotification (§2.7).
type Listener struct {
	conn *pgx.Conn
}

// Listen subscribes on a hijacked connection. The channel is the schema name
// (TG_TABLE_SCHEMA in the trigger); LISTEN requires a quoted identifier.
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

// Wait errors require the caller to close the subscription and reconnect.
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
