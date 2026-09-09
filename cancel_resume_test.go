package skein

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mbeoliero/skein/internal/store"
)

func TestExecutorCancelPrecedence(t *testing.T) {
	if Cancel(nil) != nil {
		t.Fatal("Cancel(nil) must be nil")
	}
	reason := errors.New("stop\x00\xff" + strings.Repeat("x", errMessageMax))
	wrapped := fmt.Errorf("wrapped: %w", Cancel(reason))
	if !errors.Is(wrapped, reason) {
		t.Fatal("Cancel must unwrap")
	}
	for _, tc := range []struct {
		name    string
		err     error
		cause   error
		state   RunState
		kind    string
		attempt int
	}{
		{name: "wrapped", err: wrapped, state: StateCancelled, kind: "cancelled"},
		{name: "cancel_before_snooze", err: Cancel(Snooze(time.Hour)), state: StateCancelled, kind: "cancelled"},
		{name: "permanent_outside", err: Permanent(wrapped), state: StateFailed, kind: "business", attempt: 1},
		{name: "permanent_inside", err: Cancel(Permanent(reason)), state: StateFailed, kind: "business", attempt: 1},
		{name: "cancel_request", err: wrapped, cause: errCancelRequested, state: StateCancelled},
		{name: "shutdown", err: wrapped, cause: errShuttingDown, state: StatePending, kind: "released"},
		{name: "timeout", err: wrapped, cause: errTimeout, state: StateFailed, kind: "timeout", attempt: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool, schema := freshSchema(t)
			e := startEngine(t, pool, submitOnly(schema), nil)
			declare(t, e, JobSpec{Name: "j", ExecutorType: "x", Retry: RetryPolicy{MaxAttempts: 1}})
			id := trigger(t, e, "j", "")
			c := claimAsDeadHolder(t, pool, schema, "x")
			e.settleResult(e.log, c, parseRetry(c.RetryPolicy), RawJSON("not json"), tc.err, tc.cause, 0)
			r := waitRun(t, e, id, tc.state)
			if r.Attempt != tc.attempt || len(r.Output) != 0 || r.LeaseExpiresAt != nil {
				t.Fatalf("unexpected settlement: %+v", r)
			}
			es := errorsOf(t, r)
			if tc.kind == "" {
				if len(es) != 0 {
					t.Fatalf("reasonless cancel: %+v", es)
				}
				return
			}
			if len(es) != 1 || es[0].Kind != tc.kind || es[0].Attempt != 1 {
				t.Fatalf("errors: %+v", es)
			}
			if tc.name == "wrapped" && es[0].Message != cleanMessage(wrapped.Error()) {
				t.Fatalf("reason not sanitized: %q", es[0].Message)
			}
		})
	}
}

func TestExecutorCancelFence(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	e := startEngine(t, pool, submitOnly(schema), nil)
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
	id := trigger(t, e, "j", "")
	c := claimAsDeadHolder(t, pool, schema, "x")
	st := settlementFor(c, store.Succeeded)
	if _, err := e.st.Settle(t.Context(), st); err != nil {
		t.Fatal(err)
	}
	st.Outcome, st.Err = store.Cancelled, encodeErr(errEntry{Kind: "cancelled", Message: "stale"})
	if _, err := e.st.Settle(t.Context(), st); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("stale cancel: %v", err)
	}
	r := waitRun(t, e, id, StateSucceeded)
	if len(errorsOf(t, r)) != 0 {
		t.Fatal("stale cancel wrote history")
	}
}

func TestExecutorCancelWorkflowResume(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	var resumed atomic.Bool
	var calls recorder
	sibling := make(chan struct{})
	e := startEngine(t, pool, fastConfig(schema), func(e *Engine) {
		e.Register("x", func(ctx context.Context, req *Request) (RawJSON, error) {
			calls.add(req)
			switch req.JobName {
			case "submit":
				return RawJSON(`{"task":42}`), nil
			case "cancel":
				if !resumed.Load() {
					select {
					case <-sibling:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
					return RawJSON(`{"ignored":true}`), Cancel(errors.New("provider stopped"))
				}
			case "sibling":
				if !resumed.Load() {
					close(sibling)
					<-ctx.Done()
					return nil, ctx.Err()
				}
			}
			return nil, nil
		})
	})
	for _, name := range []string{"submit", "cancel", "sibling", "child"} {
		declare(t, e, JobSpec{Name: name, ExecutorType: "x"})
	}
	declareWorkflow(t, e, WorkflowSpec{Name: "wf", Nodes: []Node{
		{Job: "submit"}, {Job: "cancel", Deps: []string{"submit"}},
		{Job: "sibling", Deps: []string{"submit"}}, {Job: "child", Deps: []string{"cancel"}},
	}})
	id := triggerWorkflow(t, e, "wf", "")
	w := waitWorkflow(t, e, id, WorkflowCancelled)
	if calls.calls("child") != 0 || nodeOf(t, w, "sibling").State != StateCancelled {
		t.Fatal("cancellation did not propagate")
	}
	cancelled := nodeOf(t, w, "cancel")
	if es := errorsOf(t, cancelled); len(es) != 1 || es[0].Message != "provider stopped" || cancelled.Attempt != 0 {
		t.Fatalf("cancel reason: %+v", cancelled)
	}
	if es := errorsOf(t, nodeOf(t, w, "child")); len(es) != 1 || es[0].Kind != "upstream_cancelled" {
		t.Fatalf("descendant reason: %+v", es)
	}
	if err := e.Runs().Resume(t.Context(), cancelled.Id); err == nil || !strings.Contains(err.Error(), "workflow") {
		t.Fatalf("node resume: %v", err)
	}
	resumed.Store(true)
	if err := e.Workflows().Resume(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	w = waitWorkflow(t, e, id, WorkflowSucceeded)
	if calls.calls("submit") != 1 || string(nodeOf(t, w, "submit").Output) != `{"task": 42}` {
		t.Fatal("resume lost successful output")
	}
	if string(calls.last("cancel").Deps["submit"]) != `{"task": 42}` {
		t.Fatal("resume lost predecessor output")
	}
}

func TestRunResumeBudgetAndWake(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	var calls recorder
	var succeed atomic.Bool
	cfg := slowPoll(schema)
	e := startEngine(t, pool, cfg, func(e *Engine) {
		e.Register("x", func(_ context.Context, req *Request) (RawJSON, error) {
			calls.add(req)
			if succeed.Load() {
				return RawJSON(`{"ok":true}`), nil
			}
			return nil, errors.New("retry exhausted")
		})
	})
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x", Retry: RetryPolicy{MaxAttempts: 2}})
	id := trigger(t, e, "j", `{"original":true}`, DedupKey("key"))
	before := waitRun(t, e, id, StateFailed)
	if before.Attempt != 2 || calls.calls("j") != 2 {
		t.Fatalf("not exhausted: %+v", before)
	}
	declare(t, e, JobSpec{Name: "j", ExecutorType: "changed", Retry: RetryPolicy{MaxAttempts: 1}})
	if err := e.Runs().Resume(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	before = waitRun(t, e, id, StateFailed)
	if before.Attempt != 2 || calls.calls("j") != 4 || len(errorsOf(t, before)) != 4 {
		t.Fatalf("resume did not renew full budget: %+v", before)
	}
	succeed.Store(true)
	started := time.Now()
	if err := e.Runs().Resume(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	r := waitRun(t, e, id, StateSucceeded)
	if time.Since(started) >= time.Second {
		t.Fatal("resume waited for poll")
	}
	req := calls.last("j")
	if req.Attempt != 1 || req.RunId != id || req.IdempotencyKey != fmt.Sprintf("run:%d", id) {
		t.Fatalf("identity/budget: %+v", req)
	}
	if r.Attempt != 0 || string(r.Errors) != string(before.Errors) || string(r.Params) != string(before.Params) || r.ExecutorType != "x" || r.RetryPolicy.MaxAttempts != 2 || r.DedupKey != "key" {
		t.Fatalf("resume changed snapshot/history: %+v", r)
	}
}

func TestRunResumeValidationDedupAndConcurrent(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	e := startEngine(t, pool, submitOnly(schema), nil)
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
	for _, id := range []int64{0, -1} {
		if err := e.Runs().Resume(t.Context(), id); err == nil {
			t.Fatalf("accepted id %d", id)
		}
	}
	if err := e.Runs().Resume(t.Context(), 999999); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
	id := trigger(t, e, "j", "", DedupKey("key"))
	if err := e.Runs().Resume(t.Context(), id); !errors.Is(err, ErrNotResumable) {
		t.Fatalf("pending: %v", err)
	}
	c := claimAsDeadHolder(t, pool, schema, "x")
	if err := e.Runs().Resume(t.Context(), id); !errors.Is(err, ErrNotResumable) {
		t.Fatalf("running: %v", err)
	}
	if _, err := e.st.Settle(t.Context(), settlementFor(c, store.Succeeded)); err != nil {
		t.Fatal(err)
	}
	if err := e.Runs().Resume(t.Context(), id); !errors.Is(err, ErrNotResumable) {
		t.Fatalf("succeeded: %v", err)
	}
	if dup, err := e.Jobs().Trigger(t.Context(), "j", nil, DedupKey("key")); !errors.Is(err, ErrDuplicate) || dup != id {
		t.Fatalf("succeeded run keeps its key: id %d err %v, want %d ErrDuplicate", dup, err, id)
	}
	old := trigger(t, e, "j", "", DedupKey("key2"))
	if err := e.Runs().Cancel(t.Context(), old); err != nil {
		t.Fatal(err)
	}
	waitRun(t, e, old, StateCancelled)
	if dup, err := e.Jobs().Trigger(t.Context(), "j", nil, DedupKey("key2")); !errors.Is(err, ErrDuplicate) || dup != old {
		t.Fatalf("cancelled run keeps its key: id %d err %v, want %d ErrDuplicate", dup, err, old)
	}
	resume := func() error { return e.Runs().Resume(t.Context(), old) }
	var ok, rejected int
	for _, err := range parallel(resume, resume) {
		switch {
		case err == nil:
			ok++
		case errors.Is(err, ErrNotResumable):
			rejected++
		default:
			t.Fatal(err)
		}
	}
	if ok != 1 || rejected != 1 {
		t.Fatalf("concurrent resume: success=%d rejected=%d", ok, rejected)
	}
	r := waitRun(t, e, old, StatePending)
	if r.Attempt != 0 || r.CancelRequested || r.StartedAt != nil || r.FinishedAt != nil || r.LeaseExpiresAt != nil || r.LeaseOwner != "" || len(r.Output) != 0 {
		t.Fatalf("resume retained execution fields: %+v", r)
	}
}
