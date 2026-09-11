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

// Heartbeat I/O uses hbCtx and is bounded by HeartbeatInterval. Shutdown cancels it
// only after draining inflight, so blocked I/O cannot extend hb.Wait (§2.3/§2.6).
func (e *Engine) heartbeatOnce(ctx context.Context) {
	e.heartbeatHealth.tick()
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
		e.heartbeatHealth.finish(err)
		// Only a successful heartbeat resets the monotonic deadline; a successful
		// claim does not prove older leases were renewed.
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
	if ctx.Err() == nil {
		e.heartbeatHealth.finish(nil)
	}
}

func (e *Engine) drop(inf *inflight) {
	if !e.inflight.removeIf(inf, true) {
		return // already settled between the snapshot and now
	}
	inf.cancel(errLeaseLost)
	e.metrics.Count("lease_lost_total", 1)
	e.log.Warn("lease lost", "run_id", inf.id, "execution_id", executionId(inf.token))
}
