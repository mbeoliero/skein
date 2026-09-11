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

func (s *inflightSet) drain() {
	s.mu.Lock()
	defer s.mu.Unlock()
	clear(s.m)
}

// Taking a slot cannot block: claimOnce requests only free slots. active tracks
// executor lifetimes beyond lease renewal for Shutdown (§2.3/§2.6). It is keyed by token:
// an old executor and a reclaimed execution can share the same run id.
func (e *Engine) dispatch(c store.Claimed) {
	e.slots <- struct{}{}
	e.active.Store(c.LeaseToken, c.Id)
	e.execs.Go(func() {
		defer func() {
			e.active.Delete(c.LeaseToken)
			<-e.slots
			e.wakeClaimer()
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
	execId := executionId(c.LeaseToken)
	log := e.log.With("run_id", c.Id, "job_name", c.JobName, "attempt", int(c.Attempt)+1, "execution_id", execId)
	baseCtx := restoreTrace(c.Extra)
	if id := traceId(baseCtx); id != "" {
		log = log.With("trace_id", id)
	}
	policy := parseRetry(c.RetryPolicy)
	extra, workflowName := c.Extra, ""
	var nc store.NodeContext
	// Direct settlement has no lease renewal; commit before callbacks can delay it.
	settleClaim := func(st store.Settlement) {
		res, err := e.commitSettlement(log, st)
		e.observeReclaim(c, extra, workflowName)
		if err == nil {
			e.observe(res.Changes)
		}
	}

	// Cancellation precedes budget checks (§2.2).
	if c.CancelRequested {
		settleClaim(settlementFor(c, store.Cancelled))
		return
	}
	if c.WorkflowRunId != nil {
		nctx, cancelNc := context.WithTimeout(context.Background(), claimTimeout)
		var err error
		nc, err = e.st.NodeContext(nctx, *c.WorkflowRunId, c.JobName)
		cancelNc()
		if err != nil {
			log.Error("read workflow run failed; leaving the lease to expire", "err", err)
			e.observeReclaim(c, extra, workflowName)
			return
		}
		extra, workflowName = nc.Extra, nc.WorkflowName
		baseCtx = restoreTrace(nc.Extra)
		if id := traceId(baseCtx); id != "" {
			log = log.With("trace_id", id)
		}
		if nc.Cancelling {
			settleClaim(settlementFor(c, store.Cancelled))
			return
		}
	}
	if int(c.Attempt) >= policy.MaxAttempts {
		settleClaim(settlementFor(c, store.Interrupted))
		return
	}
	req := requestFor(c, nc, execId)
	req.TraceId = traceId(baseCtx)
	fn := e.executors[c.ExecutorType]
	ctx, cancel := context.WithCancelCause(baseCtx)
	ctx, cancelTimeout := context.WithTimeoutCause(ctx, time.Duration(c.Timeout)*time.Second, errTimeout)
	inf := &inflight{id: c.Id, token: c.LeaseToken, cancel: cancel}
	if !e.inflight.add(e.loopCtx, inf) {
		cancel(errShuttingDown)
		cancelTimeout()
		st := settlementFor(c, store.Released)
		st.Error = "shutdown"
		st.Err = encodeErr(errEntry{Attempt: int(c.Attempt) + 1, At: time.Now().UTC(), Kind: "released", Message: "shutdown"})
		settleClaim(st)
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

	// A slow observer must remain inside lease tracking, and cannot admit an
	// executor after heartbeat or shutdown cancelled this holder while it waited.
	e.observeReclaim(c, extra, workflowName)
	var out RawJSON
	var took time.Duration
	err := ctx.Err()
	if err == nil && !inf.dropped.Load() {
		started := time.Now()
		out, err = callExecutor(ctx, fn, req)
		took = time.Since(started)
	}
	cause := context.Cause(ctx)
	stopAfter()
	if t := untrack.Load(); t != nil {
		t.Stop()
	}
	cancelTimeout()
	cancel(nil)
	e.inflight.removeIf(inf, false)
	if inf.dropped.Load() {
		log.Warn("result dropped: lease lost", "duration", took)
		return
	}
	if inf.cancelRequested.Load() {
		cause = errCancelRequested
	}
	e.settleResult(log, c, policy, out, err, cause, took)
}

// Request identity is owned by the executor; settlement must keep its original
// parent even if the executor writes through Request.WorkflowRunId (§3.1).
func requestFor(c store.Claimed, nc store.NodeContext, execId string) *Request {
	req := &Request{
		RunId: c.Id, ExecutionId: execId, JobName: c.JobName,
		Attempt: int(c.Attempt) + 1, Params: RawJSON(c.Params),
		IdempotencyKey: "run:" + strconv.FormatInt(c.Id, 10),
	}
	if c.WorkflowRunId == nil {
		return req
	}
	req.WorkflowRunId = new(*c.WorkflowRunId)
	req.IdempotencyKey = "wf:" + strconv.FormatInt(*c.WorkflowRunId, 10) + "/" + c.JobName
	req.Input = RawJSON(nc.Input)
	req.Deps = make(map[string]RawJSON, len(nc.Outputs))
	for name, out := range nc.Outputs {
		req.Deps[name] = RawJSON(out)
	}
	return req
}

// Parent identity makes Settle lock the workflow before updating and propagating.
func settlementFor(c store.Claimed, outcome store.Outcome) store.Settlement {
	return store.Settlement{Id: c.Id, Token: c.LeaseToken, WorkflowRunId: c.WorkflowRunId, Outcome: outcome}
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
