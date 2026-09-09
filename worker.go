package skein

import (
	"context"
	"fmt"

	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"uuid"

	"github.com/mbeoliero/skein/internal/store"
)

// inflight is one executor this process is responsible for renewing (§2.3).
type inflight struct {
	id              int64
	token           uuid.UUID
	cancel          context.CancelCauseFunc
	dropped         atomic.Bool // lease lost: the result must not be reported
	cancelRequested atomic.Bool // context cause may already be fixed by timeout or shutdown
}

type inflightSet struct {
	mu sync.Mutex
	m  map[int64]*inflight
}

// add checks admission and registers the lease under the same lock Shutdown uses
// to stop the loops, so cancellation cannot pass between those two operations.
func (s *inflightSet) add(ctx context.Context, inf *inflight) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ctx.Err() != nil {
		return false
	}
	s.m[inf.id] = inf
	return true
}

// removeIf untracks inf and reports whether it was still tracked. It matches by
// identity: the same run id can already belong to a later attempt. A lease-loss
// removal publishes dropped before unlocking, so completion cannot miss that decision.
func (s *inflightSet) removeIf(inf *inflight, drop bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m[inf.id] != inf {
		return false
	}
	if drop {
		inf.dropped.Store(true)
	}
	delete(s.m, inf.id)
	return true
}

func (s *inflightSet) snapshot() (ids []int64, tokens []uuid.UUID, infs []*inflight) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, inf := range s.m {
		ids = append(ids, inf.id)
		tokens = append(tokens, inf.token)
		infs = append(infs, inf)
	}
	return ids, tokens, infs
}

func (s *inflightSet) cancelAll(cause error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, inf := range s.m {
		inf.cancel(cause)
	}
}

// drain stops tracking everything: Shutdown no longer renews what is left.
func (s *inflightSet) drain() {
	s.mu.Lock()
	defer s.mu.Unlock()
	clear(s.m)
}

// dispatch takes a slot (never blocks: claimOnce only asked for free slots) and runs
// the claimed row on its own goroutine. active tracks executor goroutines, which is
// not the same set as inflight: a lease stops being renewed before its executor
// necessarily returns (§2.3), and Shutdown reports the executors (§2.6 step 5). It is
// keyed by lease token, not run id: an old attempt that ignores cancellation and the
// new attempt this process reclaimed after its lease expired share the id.
func (e *Engine) dispatch(c store.Claimed) {
	e.slots <- struct{}{}
	e.active.Store(c.LeaseToken, c.Id)
	e.execs.Go(func() {
		defer func() {
			e.active.Delete(c.LeaseToken)
			<-e.slots
			e.wakeClaimer() // a free slot is a wake source (§2.7): throughput is not capped by the poll
		}()
		e.run(c)
	})
}

func (e *Engine) activeIds() []int64 {
	var ids []int64
	e.active.Range(func(_, id any) bool {
		ids = append(ids, id.(int64))
		return true
	})
	return ids
}

func (e *Engine) run(c store.Claimed) {
	log := e.log.With("run_id", c.Id, "job_name", c.JobName, "attempt", int(c.Attempt)+1)
	policy := parseRetry(c.RetryPolicy)
	req := &Request{
		RunId: c.Id, JobName: c.JobName, Attempt: int(c.Attempt) + 1, Params: RawJSON(c.Params),
		WorkflowRunId: c.WorkflowRunId, IdempotencyKey: "run:" + strconv.FormatInt(c.Id, 10),
	}

	// §2.2 checks after the claim, before execution
	if c.CancelRequested {
		e.settle(log, settlementFor(c, store.Cancelled))
		return
	}
	if c.WorkflowRunId != nil {
		nctx, cancelNc := context.WithTimeout(context.Background(), claimTimeout)
		nc, err := e.st.NodeContext(nctx, *c.WorkflowRunId, c.JobName)
		cancelNc()
		if err != nil {
			log.Error("read workflow run failed; leaving the lease to expire", "err", err)
			return
		}
		if nc.Cancelling {
			e.settle(log, settlementFor(c, store.Cancelled))
			return
		}
		req.Input = RawJSON(nc.Input)
		req.Deps = make(map[string]RawJSON, len(nc.Outputs))
		for name, out := range nc.Outputs {
			req.Deps[name] = RawJSON(out)
		}
		req.IdempotencyKey = "wf:" + strconv.FormatInt(*c.WorkflowRunId, 10) + "/" + c.JobName
	}
	if int(c.Attempt) >= policy.MaxAttempts {
		e.settle(log, settlementFor(c, store.Interrupted))
		return
	}
	fn := e.executors[c.ExecutorType]
	ctx, cancel := context.WithCancelCause(context.Background())
	ctx, cancelTimeout := context.WithTimeoutCause(ctx, time.Duration(c.Timeout)*time.Second, errTimeout)
	inf := &inflight{id: c.Id, token: c.LeaseToken, cancel: cancel}
	if !e.inflight.add(e.loopCtx, inf) {
		cancel(errShuttingDown)
		cancelTimeout()
		st := settlementFor(c, store.Released)
		st.Err = encodeErr(errEntry{Attempt: int(c.Attempt) + 1, At: time.Now().UTC(), Kind: "released", Message: "shutdown"})
		e.settle(log, st)
		return
	}
	// once the ctx ends, an executor that keeps running past CancelTimeout stops being renewed (§2.3)
	var untrack atomic.Pointer[time.Timer]
	stopAfter := context.AfterFunc(ctx, func() {
		untrack.Store(time.AfterFunc(e.cfg.CancelTimeout, func() {
			if e.inflight.removeIf(inf, false) {
				log.Warn("executor ignored cancellation; lease no longer renewed")
			}
		}))
	})

	started := time.Now()
	out, err := callExecutor(ctx, fn, req)
	cause := context.Cause(ctx) // nil unless the ctx ended before the executor returned
	stopAfter()
	if t := untrack.Load(); t != nil {
		t.Stop()
	}
	cancelTimeout()
	cancel(nil)
	e.inflight.removeIf(inf, false)
	if inf.dropped.Load() {
		log.Warn("result dropped: lease lost", "duration", time.Since(started))
		return
	}
	if inf.cancelRequested.Load() {
		cause = errCancelRequested
	}
	e.settleResult(log, c, policy, out, err, cause, time.Since(started))
}

// settlementFor starts a settlement for a claimed row; the node fields make Settle
// lock the parent first and propagate afterwards.
func settlementFor(c store.Claimed, outcome store.Outcome) store.Settlement {
	return store.Settlement{Id: c.Id, Token: c.LeaseToken, WorkflowRunId: c.WorkflowRunId, JobName: c.JobName, Outcome: outcome}
}

type panicError struct {
	value any
	stack []byte
}

func (p *panicError) Error() string { return fmt.Sprintf("panic: %v", p.value) }

// callExecutor recovers a panic on the executor's own goroutine only.
func callExecutor(ctx context.Context, fn Executor, req *Request) (out RawJSON, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &panicError{value: r, stack: debug.Stack()}
		}
	}()
	return fn(ctx, req)
}
