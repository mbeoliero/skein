package skein

import (
	"context"
	"time"
)

func (e *Engine) scheduleLoop(ctx context.Context) {
	for {
		wait := e.scanOnce()
		select {
		case <-ctx.Done():
			return
		case <-e.wakeSched:
		case <-time.After(wait):
		}
	}
}

// scanOnce is one §6.2 tick. Like a claim it runs on its own bounded ctx so a tick
// that already committed is always accounted for. It returns how long the loop waits
// unless woken: until the nearest next_run_at, or nothing at all after a full batch.
func (e *Engine) scanOnce() time.Duration {
	ctx, cancel := context.WithTimeout(context.Background(), claimTimeout)
	defer cancel()
	sc, err := e.st.ScanDue(ctx, nextRun)
	sampled := time.Now()
	if err != nil {
		e.log.Error("schedule scan failed", "err", err)
		return jitter(e.cfg.PollInterval)
	}
	created := 0
	for _, f := range sc.Fired {
		if f.Disabled != nil {
			e.metrics.Count("schedule_skipped_total", 1, "reason", "no_next")
			e.log.Error("schedule disabled: no next fire time", "schedule", f.Schedule, "err", f.Disabled)
			continue
		}
		if f.Skipped {
			e.metrics.Count("schedule_skipped_total", 1, "reason", "overlap")
			e.log.Info("schedule beat skipped: previous run still in flight", "schedule", f.Schedule, "scheduled_at", f.ScheduledAt)
			continue
		}
		created++
		e.metrics.Observe("schedule_lag", f.Lag.Seconds())
		e.log.Info("schedule fired", "schedule", f.Schedule, "scheduled_at", f.ScheduledAt, "run_id", f.RunId, "workflow", f.IsWorkflow, "lag", f.Lag)
	}
	if created > 0 {
		e.wakeClaimer()
	}
	if sc.Full {
		return 0
	}
	return e.untilDue(sc.NextDue, sc.DbNow, sampled)
}
