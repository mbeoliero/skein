package store

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// ───────────── retention and checks (§6.10) ─────────────

type Retention struct {
	Succeeded time.Duration
	Failed    time.Duration
	Batch     int
}

// MaintenanceReport is what one holder did; a step that failed is in Failures.
type MaintenanceReport struct {
	Skipped       bool // another instance holds the lock
	Deleted       map[string]int64
	StaleWorkflow int
	StaleJob      int
	Failures      map[string]error
}

// Maintain runs the retention steps under a session advisory lock on one dedicated
// connection, each step in its own short transaction so no long transaction holds
// the xmin horizon back (which would defeat HOT pruning for the heartbeat).
func (s *Store) Maintain(ctx context.Context, r Retention) (rep MaintenanceReport, err error) {
	rep.Deleted, rep.Failures = map[string]int64{}, map[string]error{}
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return rep, err
	}
	defer conn.Release()
	var locked bool
	if err := s.txOn(ctx, conn.Conn(), func(ctx context.Context, tx pgx.Tx) error {
		var err error
		locked, err = s.q.TryMaintenanceLock(ctx, tx)
		return err
	}); err != nil {
		return rep, err
	}
	if !locked {
		rep.Skipped = true
		return rep, nil
	}
	defer func() {
		// A session lock outlives a failed statement. On failure the session is closed
		// so the lock dies with it: Release destroys a closed connection instead of
		// returning one that still holds the lock to the pool.
		uctx, cancel := cleanupCtx(ctx)
		defer cancel()
		if uerr := s.txOn(uctx, conn.Conn(), func(ctx context.Context, tx pgx.Tx) error {
			return s.q.MaintenanceUnlock(ctx, tx)
		}); uerr != nil {
			_ = conn.Conn().Close(uctx)
			rep.Failures["unlock"] = uerr
		}
	}()

	terminalFailed := []string{"failed", "cancelled"}
	steps := []struct {
		name string
		fn   func(ctx context.Context, tx pgx.Tx) (int64, error)
	}{
		{"job_run_succeeded", func(ctx context.Context, tx pgx.Tx) (int64, error) {
			return s.q.DeleteOldJobRuns(ctx, tx, DeleteOldJobRunsParams{States: []string{"succeeded"}, Age: r.Succeeded, Lim: int32(r.Batch)})
		}},
		{"job_run_failed", func(ctx context.Context, tx pgx.Tx) (int64, error) {
			return s.q.DeleteOldJobRuns(ctx, tx, DeleteOldJobRunsParams{States: terminalFailed, Age: r.Failed, Lim: int32(r.Batch)})
		}},
		{"workflow_run_succeeded", func(ctx context.Context, tx pgx.Tx) (int64, error) {
			return s.q.DeleteOldWorkflowRuns(ctx, tx, DeleteOldWorkflowRunsParams{States: []string{"succeeded"}, Age: r.Succeeded, Lim: int32(r.Batch)})
		}},
		{"workflow_run_failed", func(ctx context.Context, tx pgx.Tx) (int64, error) {
			return s.q.DeleteOldWorkflowRuns(ctx, tx, DeleteOldWorkflowRunsParams{States: terminalFailed, Age: r.Failed, Lim: int32(r.Batch)})
		}},
	}
	for _, step := range steps {
		for {
			var n int64
			err := s.txOn(ctx, conn.Conn(), func(ctx context.Context, tx pgx.Tx) error {
				var err error
				n, err = step.fn(ctx, tx)
				return err
			})
			if err != nil {
				rep.Failures[step.name] = err
				break
			}
			rep.Deleted[step.name] += n
			if int(n) < r.Batch {
				break
			}
		}
	}
	if err := s.txOn(ctx, conn.Conn(), func(ctx context.Context, tx pgx.Tx) error {
		wf, err := s.q.StaleWorkflowRuns(ctx, tx, r.Failed)
		if err != nil {
			return err
		}
		jr, err := s.q.StaleJobRuns(ctx, tx, r.Failed)
		rep.StaleWorkflow, rep.StaleJob = int(wf), int(jr)
		return err
	}); err != nil {
		rep.Failures["stale_check"] = err
	}
	return rep, nil
}

func (s *Store) Stats(ctx context.Context, registered []string) (row StatsRow, err error) {
	err = s.tx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		row, err = s.q.Stats(ctx, tx, registered)
		return err
	})
	return row, err
}

func (s *Store) ListJobRuns(ctx context.Context, p ListJobRunsParams) (rows []JobRun, err error) {
	err = s.tx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		rows, err = s.q.ListJobRuns(ctx, tx, p)
		return err
	})
	return rows, err
}
