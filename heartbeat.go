package skein

import (
	"context"
	"time"

	"github.com/mbeoliero/skein/internal/store"
)

func (e *Engine) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(e.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		e.heartbeatOnce(ctx)
	}
}

// heartbeatOnce renews every lease this process holds in one statement and brings
// back cancel signals (§2.3). A lease the database no longer attributes to us is
// dropped: its ctx is cancelled and its result will not be reported. The statement is
// bounded by HeartbeatInterval and derives from hbCtx, not the host's ctx: only
// Shutdown step 6 cancels it, when inflight is already empty and nothing is left to
// renew, so hb.Wait cannot outlast the host's budget by a blocked statement.
func (e *Engine) heartbeatOnce(ctx context.Context) {
	ids, tokens, infs := e.inflight.snapshot()
	if len(ids) == 0 {
		e.lastOK = time.Now()
		return
	}
	started := time.Now()
	hctx, cancel := context.WithTimeout(ctx, e.cfg.HeartbeatInterval)
	rows, err := e.st.Heartbeat(hctx, ids, tokens, e.cfg.LeaseTTL, e.cfg.CancelTimeout)
	cancel()
	if err != nil {
		if ctx.Err() != nil {
			e.log.Info("heartbeat cancelled by shutdown", "leases", len(ids))
			return
		}
		e.log.Error("heartbeat failed", "err", err, "leases", len(ids))
		// local deadline on the monotonic clock: never assume a lease we could not renew.
		// Only a heartbeat moves it: a claim succeeding meanwhile proves the database is
		// reachable, not that the older leases were renewed.
		if time.Since(e.lastOK) > e.cfg.LeaseTTL-e.cfg.HeartbeatInterval {
			for _, inf := range infs {
				e.drop(inf)
			}
		}
		return
	}
	e.lastOK = started
	alive := make(map[int64]store.HeartbeatRow, len(rows))
	for _, r := range rows {
		alive[r.Id] = r
	}
	for _, inf := range infs {
		r, ok := alive[inf.id]
		if !ok {
			e.drop(inf)
			continue
		}
		if r.CancelRequested || r.WfCancelling {
			inf.cancelRequested.Store(true)
			inf.cancel(errCancelRequested)
		}
	}
}

func (e *Engine) drop(inf *inflight) {
	if !e.inflight.removeIf(inf, true) {
		return // already settled between the snapshot and now
	}
	inf.cancel(errLeaseLost)
	e.metrics.Count("lease_lost_total", 1)
	e.log.Warn("lease lost", "run_id", inf.id)
}
