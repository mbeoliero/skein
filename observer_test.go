package skein

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/mbeoliero/skein/internal/store"
)

type observerRecorder struct {
	mu     sync.Mutex
	events []Event
}

func (r *observerRecorder) Observe(_ context.Context, event Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
}

func (r *observerRecorder) snapshot() []Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.events)
}

func (r *observerRecorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = r.events[:0]
}

func TestObserverSeesCommitWithoutHoldingRowLock(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	defer release()
	type report struct {
		event Event
		err   error
	}
	observed := make(chan report, 1)
	cfg := submitOnly(schema)
	cfg.DisableScheduler = true
	cfg.Observer = ObserverFunc(func(ctx context.Context, event Event) {
		checkCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		tx, err := pool.Begin(checkCtx)
		if err == nil {
			var state string
			err = tx.QueryRow(checkCtx,
				"SELECT state FROM "+qualified(schema, "job_run")+" WHERE id=$1 FOR UPDATE NOWAIT",
				event.RunId,
			).Scan(&state)
			if err == nil && state != event.State {
				err = fmt.Errorf("committed state %q differs from event %q", state, event.State)
			}
			err = errors.Join(err, tx.Rollback(checkCtx))
		}
		observed <- report{event: event, err: err}
		<-gate
	})
	e := startEngine(t, pool, cfg, nil)
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
	id := trigger(t, e, "j", "")
	c := claimAsDeadHolder(t, pool, schema, "x")
	finished := make(chan error, 1)
	go func() {
		_, err := e.settle(e.log, settlementFor(c, store.Succeeded))
		finished <- err
	}()
	select {
	case got := <-observed:
		if got.err != nil {
			t.Fatal(got.err)
		}
		if got.event.Type != EventRunSucceeded || got.event.RunId != id || got.event.EventId == "" {
			t.Fatalf("unexpected committed event: %+v", got.event)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("observer did not run after settlement")
	}
	select {
	case err := <-finished:
		t.Fatalf("synchronous settlement returned while callback was blocked: %v", err)
	default:
	}
	if run := waitRun(t, e, id, StateSucceeded); run.LeaseOwner != "" {
		t.Errorf("committed settlement kept lease owner %q", run.LeaseOwner)
	}
	release()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("settlement did not return after observer returned")
	}
}

func TestObserverPanicDoesNotChangeCommittedResult(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	events := []Event{}
	cfg := submitOnly(schema)
	cfg.DisableScheduler = true
	cfg.Observer = ObserverFunc(func(_ context.Context, event Event) {
		events = append(events, event)
		if len(events) == 1 {
			panic("host observer failed")
		}
	})
	e := startEngine(t, pool, cfg, nil)
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
	for range 2 {
		id := trigger(t, e, "j", "")
		c := claimAsDeadHolder(t, pool, schema, "x")
		if _, err := e.settle(e.log, settlementFor(c, store.Succeeded)); err != nil {
			t.Fatalf("observer panic changed settlement result: %v", err)
		}
		waitRun(t, e, id, StateSucceeded)
	}
	if len(events) != 2 {
		t.Fatalf("received %d callbacks after first callback panicked", len(events))
	}
	if events[0].EventId == "" || events[0].EventId == events[1].EventId {
		t.Error("distinct committed changes did not get distinct notification ids")
	}
}

func TestObserverSettlementUsesActualState(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		out    RawJSON
		err    error
		cancel bool
		state  RunState
		event  EventType
	}{
		{name: "retry", err: errors.New("temporary failure"), state: StatePending, event: EventRunRetry},
		{name: "permanent", err: Permanent(errors.New("bad request")), state: StateFailed, event: EventRunFailed},
		{name: "invalid_jsonb", out: RawJSON(`{"bad":"\u0000"}`), state: StateFailed, event: EventRunFailed},
		{name: "cancel_over_retry", err: errors.New("temporary failure"), cancel: true,
			state: StateCancelled, event: EventRunCancelled},
		{name: "cancel_over_snooze", err: Snooze(time.Hour), cancel: true,
			state: StateCancelled, event: EventRunCancelled},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool, schema := freshSchema(t)
			rec := &observerRecorder{}
			metrics := &metricsRec{}
			cfg := submitOnly(schema)
			cfg.DisableScheduler, cfg.Observer, cfg.Metrics = true, rec, metrics
			e := startEngine(t, pool, cfg, nil)
			declare(t, e, JobSpec{Name: "j", ExecutorType: "x", Retry: RetryPolicy{MaxAttempts: 3}})
			submitCtx, parent := testTraceContext(t, 90)
			id, err := e.Jobs().Trigger(submitCtx, "j", nil)
			if err != nil {
				t.Fatal(err)
			}
			c := claimAsDeadHolder(t, pool, schema, "x")
			if tc.cancel {
				if err := e.Runs().Cancel(t.Context(), id); err != nil {
					t.Fatal(err)
				}
				rec.reset() // Only the later, fenced settlement is under test here.
			}
			const took = 17 * time.Millisecond
			e.settleResult(e.log, c, parseRetry(c.RetryPolicy), tc.out, tc.err, nil, took)
			waitRun(t, e, id, tc.state)
			// Metrics, like events, must follow the state chosen by the DB cancellation guard.
			if tc.cancel {
				for _, outcome := range []string{"cancelled", "failed", "snoozed"} {
					want := 0
					if outcome == "cancelled" {
						want = 1
					}
					if got := metrics.get("exec_duration", "executor_type", "x", "outcome", outcome); got != want {
						t.Errorf("%s observations: %d, want %d", outcome, got, want)
					}
				}
			}
			events := rec.snapshot()
			if len(events) != 1 {
				t.Fatalf("settlement emitted %d events: %+v", len(events), events)
			}
			got := events[0]
			if got.Type != tc.event || got.State != string(tc.state) {
				t.Errorf("event %+v, want %s/%s", got, tc.event, tc.state)
			}
			if got.ExecutionId != executionId(c.LeaseToken) || got.Duration != took {
				t.Errorf("settlement lost execution identity or duration: %+v", got)
			}
			if got.JobName != "j" || got.ExecutorType != "x" || got.RunId != id || got.WorkflowRunId != nil {
				t.Errorf("settlement identified the wrong run: %+v", got)
			}
			if tc.event == EventRunFailed && got.Error == "" {
				t.Error("failed settlement omitted its error")
			}
			if got.TraceId != parent.TraceID().String() {
				t.Errorf("settlement lost submission trace: %+v", got)
			}
		})
	}
}

func TestObserverCommitFailureDoesNotPublish(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	rec := &observerRecorder{}
	cfg := submitOnly(schema)
	cfg.DisableScheduler, cfg.Observer = true, rec
	e := startEngine(t, pool, cfg, nil)
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
	id := trigger(t, e, "j", "")
	c := claimAsDeadHolder(t, pool, schema, "x")
	jr, reject := qualified(schema, "job_run"), qualified(schema, "reject_commit")
	// The UPDATE returns its row first; this failure occurs only at Commit.
	execSql(t, pool, "CREATE FUNCTION "+reject+`() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'forced commit rollback' USING ERRCODE = '40001'; END $$;
		CREATE CONSTRAINT TRIGGER reject_commit AFTER UPDATE ON `+jr+`
		DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
		WHEN (OLD.state = 'running' AND NEW.state = 'succeeded') EXECUTE FUNCTION `+reject+"()")
	if _, err := e.settle(e.log, settlementFor(c, store.Succeeded)); err == nil {
		t.Fatal("settlement unexpectedly committed")
	}
	if events := rec.snapshot(); len(events) != 0 {
		t.Errorf("failed commit emitted events: %+v", events)
	}
	waitRun(t, e, id, StateRunning)
	if leaseToken(t, pool, schema, id) != c.LeaseToken {
		t.Error("rollback changed the valid lease")
	}
	execSql(t, pool, "DROP TRIGGER reject_commit ON "+jr)
	if _, err := e.settle(e.log, settlementFor(c, store.Succeeded)); err != nil {
		t.Fatal(err)
	}
	if events := rec.snapshot(); len(events) != 1 || events[0].Type != EventRunSucceeded {
		t.Errorf("later committed settlement events: %+v", events)
	}
}

func TestObserverStaleLeaseDoesNotPublish(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	rec := &observerRecorder{}
	cfg := submitOnly(schema)
	cfg.DisableScheduler, cfg.Observer = true, rec
	e := startEngine(t, pool, cfg, nil)
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
	id := trigger(t, e, "j", "")
	c := claimAsDeadHolder(t, pool, schema, "x")
	stealLease(t, pool, schema, id, "new-owner")
	if _, err := e.settle(e.log, settlementFor(c, store.Succeeded)); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("stale settlement: %v", err)
	}
	if events := rec.snapshot(); len(events) != 0 {
		t.Errorf("stale lease emitted events: %+v", events)
	}
	waitRun(t, e, id, StateRunning)
}

func TestObserverLocallyDroppedResultDoesNotPublish(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	rec := &observerRecorder{}
	entered, gate := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	defer release()
	cfg := submitOnly(schema)
	cfg.DisableScheduler, cfg.Observer = true, rec
	e := startEngine(t, pool, cfg, func(e *Engine) {
		e.Register("x", func(context.Context, *Request) (RawJSON, error) {
			close(entered)
			<-gate
			return nil, nil
		})
	})
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
	id := trigger(t, e, "j", "")
	c := claimAsDeadHolder(t, pool, schema, "x")
	e.dispatch(c)
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("executor did not enter")
	}
	_, _, leases := e.inflight.snapshot()
	if len(leases) != 1 {
		t.Fatalf("inflight leases: %d", len(leases))
	}
	e.drop(leases[0])
	release()
	if !waitGroup(t.Context(), &e.execs, 5*time.Second) {
		t.Fatal("dropped executor did not finish")
	}
	if events := rec.snapshot(); len(events) != 0 {
		t.Errorf("locally dropped result emitted events: %+v", events)
	}
	waitRun(t, e, id, StateRunning)
}

func TestObserverReexecutionKeepsTraceAndChangesIdentity(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	ctx, parent := testTraceContext(t, 70)
	ctx = context.WithValue(ctx, traceRequestValueKey{}, "request-only")
	ctx, cancel := context.WithTimeout(ctx, time.Hour)
	defer cancel()
	events := []Event{}
	observerContexts := []context.Context{}
	requests := []*Request{}
	cfg := submitOnly(schema)
	cfg.DisableScheduler = true
	cfg.Observer = ObserverFunc(func(ctx context.Context, event Event) {
		events = append(events, event)
		observerContexts = append(observerContexts, ctx)
	})
	e := startEngine(t, pool, cfg, func(e *Engine) {
		e.Register("x", func(_ context.Context, req *Request) (RawJSON, error) {
			requests = append(requests, req)
			switch len(requests) {
			case 1:
				return nil, Snooze(time.Hour)
			case 2:
				return nil, errors.New("retry")
			default:
				return nil, nil
			}
		})
	})
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x", Retry: RetryPolicy{MaxAttempts: 3}})
	id, err := e.Jobs().Trigger(ctx, "j", nil)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	for round := range 3 {
		if round > 0 {
			execSql(t, pool, "UPDATE "+qualified(schema, "job_run")+" SET run_at=now() WHERE id="+itoa(id))
		}
		e.run(claimAsDeadHolder(t, pool, schema, "x"))
	}
	if len(events) != 3 || len(requests) != 3 {
		t.Fatalf("events=%d executions=%d", len(events), len(requests))
	}
	ids := map[string]bool{}
	for i, wantType := range []EventType{EventRunSnoozed, EventRunRetry, EventRunSucceeded} {
		got, req, observedCtx := events[i], requests[i], observerContexts[i]
		if got.Type != wantType || got.ExecutionId != req.ExecutionId || got.Duration <= 0 {
			t.Errorf("execution %d correlation: %+v", i+1, got)
		}
		if got.TraceId != parent.TraceID().String() || req.IdempotencyKey != requests[0].IdempotencyKey {
			t.Errorf("execution %d changed submission identity", i+1)
		}
		if ids[got.ExecutionId] || got.ExecutionId == "" {
			t.Errorf("execution %d reused an identity", i+1)
		}
		ids[got.ExecutionId] = true
		if sc := trace.SpanContextFromContext(observedCtx); !sc.Equal(parent.WithRemote(true)) {
			t.Errorf("observer %d lost the original remote parent: %v", i+1, sc)
		}
		if observedCtx.Err() != nil || observedCtx.Value(traceRequestValueKey{}) != nil {
			t.Errorf("observer %d inherited request/executor cancellation or values", i+1)
		}
		if _, hasDeadline := observedCtx.Deadline(); hasDeadline {
			t.Errorf("observer %d inherited request/executor deadline", i+1)
		}
	}
}
