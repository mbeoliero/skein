package skein

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/mbeoliero/skein/internal/store"
)

// claimTimeout bounds one claim transaction; it is not tied to the loop ctx so a
// claim that already committed is always dispatched (§2.6 step 1).
const claimTimeout = 10 * time.Second

// wakeFloor bounds how soon a loop may run again after a timer wake: a row another
// instance is claiming or firing right now is skipped, not waited for, so the next
// read only makes sense once that transaction had a chance to commit (§2.1 / §2.2).
const wakeFloor = 20 * time.Millisecond

func (e *Engine) claimLoop(ctx context.Context) {
	for ctx.Err() == nil {
		wait := e.claimOnce()
		select {
		case <-ctx.Done():
			return
		case <-e.wake:
		case <-time.After(wait):
		}
	}
}

// jitter spreads polling across instances: d ± 20%.
func jitter(d time.Duration) time.Duration {
	return time.Duration(float64(d) * (0.8 + 0.4*rand.Float64()))
}

// untilDue is how long a loop sleeps: to the next due moment, never past the poll
// backstop, never under wakeFloor. next and dbNow come from the same statement and
// sampled is the local monotonic time that statement returned, so the two clocks are
// never compared: the local one only measures how much of the wait is already spent
// (§2.7). A next at or before dbNow is a due row nobody claimed yet: wakeFloor.
func (e *Engine) untilDue(d store.Due) time.Duration {
	poll := jitter(e.cfg.PollInterval)
	if d.Next == nil {
		return poll
	}
	return min(poll, max(d.Next.Sub(d.DbNow)-time.Since(d.Sampled), wakeFloor))
}

// claimOnce takes as many rows as this process has free slots: branch one (due
// pending) first, branch two (expired leases) only for what is left (§2.2). It
// returns how long the loop waits unless woken: a full round means a slot release
// will wake it; a round with room left reads the nearest future run_at.
func (e *Engine) claimOnce() time.Duration {
	free := cap(e.slots) - len(e.slots)
	types := e.registeredTypes()
	if free <= 0 || len(types) == 0 {
		return jitter(e.cfg.PollInterval)
	}
	ctx, cancel := context.WithTimeout(context.Background(), claimTimeout)
	defer cancel()
	rows, err := e.st.ClaimPending(ctx, types, free, e.owner, e.cfg.LeaseTTL)
	if err != nil {
		e.log.Error("claim pending failed", "err", err)
		return jitter(e.cfg.PollInterval)
	}
	wait := func() time.Duration { return jitter(e.cfg.PollInterval) }
	if len(rows) < free {
		more, err := e.st.ClaimExpired(ctx, types, free-len(rows), e.owner, e.cfg.LeaseTTL)
		if err != nil {
			e.log.Error("claim expired failed", "err", err)
		} else {
			for _, c := range more {
				e.log.Warn("reclaimed expired lease", "run_id", c.Id, "job_name", c.JobName, "attempt", int(c.Attempt)+1)
			}
			e.metrics.Count("reclaim_total", len(more))
			rows = append(rows, more...)
		}
		// read last, and turn it into a wait only when the round is over: everything
		// this round still does after the sample counts against the wait (§2.2)
		if due, err := e.st.NextPendingAt(ctx, types); err != nil {
			e.log.Error("next due read failed", "err", err)
		} else {
			wait = func() time.Duration { return e.untilDue(due) }
		}
	}
	for _, c := range rows {
		e.metrics.Observe("claim_latency", c.WaitedSec)
		e.dispatch(c)
	}
	return wait()
}
