package skein

import (
	"context"
	"slices"
	"strings"
	"time"
)

// Polling remains the fallback if LISTEN disconnects or loses notifications (§2.7).
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
