package skein

import (
	"context"
	"slices"
	"strings"
	"time"
)

// listenLoop keeps the process's LISTEN connection alive (§2.7). Each payload is
// routed to a loop: "run:<executor_type>" wakes the claimer when this process
// registered that type, "schedule" wakes the scheduler, anything else wakes both.
// Notifications only shorten a wait; a lost one costs at most a PollInterval, so an
// error here is logged, counted, and retried after a jittered poll period.
func (e *Engine) listenLoop(ctx context.Context) {
	for {
		l, err := e.st.Listen(ctx)
		if err == nil {
			// a notification sent before LISTEN took effect was missed: look once now
			e.wakeClaimer()
			e.wakeScheduler()
			for {
				payload, werr := l.Wait(ctx)
				if werr != nil {
					err = werr
					break
				}
				e.route(payload)
			}
			l.Close()
		}
		if ctx.Err() != nil {
			return
		}
		e.metrics.Count("listener_reconnect_total", 1)
		e.log.Warn("listener disconnected, polling until it reconnects", "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(jitter(e.cfg.PollInterval)):
		}
	}
}

func (e *Engine) route(payload string) {
	switch typ, ok := strings.CutPrefix(payload, "run:"); {
	case ok:
		if _, found := slices.BinarySearch(e.registeredTypes(), typ); found {
			e.wakeClaimer()
		}
	case payload == "schedule":
		e.wakeScheduler()
	default:
		e.wakeClaimer()
		e.wakeScheduler()
	}
}
