package skein

import (
	"context"
	"time"

	"github.com/mbeoliero/skein/internal/store"
)

const (
	maintenanceTimeout = 10 * time.Minute
	retentionBatch     = 5000
)

func (e *Engine) maintenanceLoop(ctx context.Context) {
	t := time.NewTicker(e.cfg.MaintenanceInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		e.maintainOnce(ctx)
	}
}

// maintainOnce is §2.8: retention plus the stale-active check, on whichever instance
// wins the advisory lock; skipped reports that another holder had it. A silent failure
// is this job's biggest risk, so every step that fails is logged at error level and counted.
// It runs under the loop ctx: a batch is its own short transaction, so Shutdown cancels
// the one in flight instead of waiting for it (§2.6 step 1), unlike a claim.
func (e *Engine) maintainOnce(ctx context.Context) (skipped bool) {
	mctx, cancel := context.WithTimeout(ctx, maintenanceTimeout)
	defer cancel()
	rep, err := e.st.Maintain(mctx, store.Retention{Succeeded: e.cfg.RetentionSucceeded, Failed: e.cfg.RetentionFailed, Batch: retentionBatch})
	if ctx.Err() != nil && (err != nil || len(rep.Failures) > 0) {
		e.log.Info("maintenance interrupted by shutdown")
		return false
	}
	if err != nil {
		e.metrics.Count("maintenance_failures_total", 1, "step", "lock")
		e.log.Error("maintenance failed", "err", err)
		return false
	}
	if rep.Skipped {
		return true
	}
	for table, n := range rep.Deleted {
		e.metrics.Count("retention_deleted_total", int(n), "table", table)
	}
	for step, err := range rep.Failures {
		e.metrics.Count("maintenance_failures_total", 1, "step", step)
		e.log.Error("maintenance step failed", "step", step, "err", err)
	}
	stale := rep.StaleWorkflow + rep.StaleJob
	e.metrics.Gauge("stale_active_total", float64(stale))
	if stale > 0 {
		e.log.Warn("runs in flight longer than the failed-retention window", "workflow_runs", rep.StaleWorkflow, "job_runs", rep.StaleJob)
	}
	e.log.Info("maintenance done", "deleted", rep.Deleted)
	return false
}

// Stats is the §3.3 one-statement snapshot the host can scrape.
type Stats struct {
	PendingDue       int           // pending rows whose run_at has passed
	Running          int           // rows holding a lease
	OldestPendingAge time.Duration // now − run_at of the oldest due pending row
	UnregisteredDue  int           // due rows whose executor type this process does not register
}

func (e *Engine) Stats(ctx context.Context) (Stats, error) {
	row, err := e.st.Stats(ctx, e.registeredTypes())
	if err != nil {
		return Stats{}, err
	}
	return Stats{
		PendingDue: int(row.PendingDue), Running: int(row.Running),
		OldestPendingAge: time.Duration(row.OldestPendingSec * float64(time.Second)),
		UnregisteredDue:  int(row.UnregisteredDue),
	}, nil
}
