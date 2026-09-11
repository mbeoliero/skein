package skein

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"time"

	"github.com/mbeoliero/skein/internal/store"
)

// claimTimeout bounds one claim transaction; it is not tied to the loop ctx so a
// claim that already committed is always dispatched (§2.6 step 1).
const claimTimeout = 10 * time.Second

// wakeFloor avoids busy retries while SKIP LOCKED candidates await another
// transaction's commit (§2.1/§2.2).
const wakeFloor = 20 * time.Millisecond

// One recovery-first round in eight bounds branch starvation even with one slot.
const reclaimEvery = 8

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
	return boundedDuration(float64(d) * (0.8 + 0.4*rand.Float64()))
}

// A floating-point product may exceed Duration even when its inputs fit.
func boundedDuration(nanos float64) time.Duration {
	if nanos >= float64(math.MaxInt64) {
		return time.Duration(math.MaxInt64)
	}
	return max(time.Nanosecond, time.Duration(nanos))
}

// Next and DbNow come from one statement; Sampled is the local monotonic time it
// returned. Compare DB times and subtract only local elapsed time (§2.7).
func (e *Engine) untilDue(d store.Due) time.Duration {
	poll := jitter(e.cfg.PollInterval)
	if d.Next == nil {
		return poll
	}
	return min(poll, max(d.Next.Sub(d.DbNow)-time.Since(d.Sampled), wakeFloor))
}

// Branches commit independently; failures must not discard committed claims (§2.2).
// Full rounds rely on slot-release wakeups; others also probe the next run_at.
func (e *Engine) claimOnce() time.Duration {
	e.claimHealth.tick()
	free := cap(e.slots) - len(e.slots)
	types := e.registeredTypes()
	if free <= 0 || len(types) == 0 {
		return jitter(e.cfg.PollInterval)
	}
	e.claimTurn = (e.claimTurn + 1) % reclaimEvery
	branches := [2]struct {
		name  string
		claim func(context.Context, []string, int, string, time.Duration) ([]store.Claimed, error)
	}{
		{name: "pending", claim: e.st.ClaimPending},
		{name: "expired", claim: e.st.ClaimExpired},
	}
	if e.claimTurn == 0 {
		branches[0], branches[1] = branches[1], branches[0]
	}
	ctx, cancel := context.WithTimeout(context.Background(), claimTimeout)
	defer cancel()
	rows := []store.Claimed{}
	var roundErr error
	for _, branch := range branches {
		if len(rows) == free || ctx.Err() != nil {
			break
		}
		more, err := branch.claim(ctx, types, free-len(rows), e.owner, e.cfg.LeaseTTL)
		if err != nil {
			e.log.Error("claim "+branch.name+" failed", "err", err)
			roundErr = errors.Join(roundErr, err)
			continue
		}
		rows = append(rows, more...)
		if branch.name == "expired" {
			for _, c := range more {
				e.log.Warn("reclaimed expired lease", "run_id", c.Id, "job_name", c.JobName,
					"attempt", int(c.Attempt)+1, "execution_id", executionId(c.LeaseToken))
			}
			e.metrics.Count("reclaim_total", len(more))
		}
	}
	var due *store.Due
	if len(rows) < free {
		if next, err := e.st.NextPendingAt(ctx, types); err != nil {
			e.log.Error("next due read failed", "err", err)
			roundErr = errors.Join(roundErr, err)
		} else {
			due = &next
		}
	}
	e.claimHealth.finish(roundErr)
	for _, c := range rows {
		e.metrics.Observe("claim_latency", c.WaitedSec)
		e.dispatch(c)
	}
	// Account for all work since the database clock sample, including dispatch.
	if due != nil {
		return e.untilDue(*due)
	}
	return jitter(e.cfg.PollInterval)
}
