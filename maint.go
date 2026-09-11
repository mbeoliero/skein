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

// Retention batches follow the loop ctx so Shutdown cancels in-flight work (§2.6/§2.8).
// skipped means another instance holds the advisory lock.
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
	PendingDue       int                      // pending rows whose run_at has passed
	Running          int                      // rows holding a lease
	OldestPendingAge time.Duration            // now − run_at of the oldest due pending row
	UnregisteredDue  int                      // due rows whose executor type this process does not register
	ByExecutor       map[string]ExecutorStats // only types with pending or running rows
}

// ExecutorStats counts one type across this schema. Registered describes this
// Engine's registry, not every process sharing the queue.
type ExecutorStats struct {
	PendingDue       int
	Running          int
	OldestPendingAge time.Duration
	Registered       bool
}

func (e *Engine) Stats(ctx context.Context) (Stats, error) {
	rows, err := e.st.Stats(ctx, e.registeredTypes())
	if err != nil {
		return Stats{}, err
	}
	stats := Stats{ByExecutor: make(map[string]ExecutorStats, len(rows))}
	for _, row := range rows {
		counts := ExecutorStats{
			PendingDue: int(row.PendingDue), Running: int(row.Running),
			OldestPendingAge: time.Duration(row.OldestPendingSec * float64(time.Second)),
			Registered:       row.Registered,
		}
		stats.ByExecutor[row.ExecutorType] = counts
		stats.PendingDue += counts.PendingDue
		stats.Running += counts.Running
		stats.OldestPendingAge = max(stats.OldestPendingAge, counts.OldestPendingAge)
		if !counts.Registered {
			stats.UnregisteredDue += counts.PendingDue
		}
	}
	return stats, nil
}
