package skein

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mbeoliero/skein/internal/store"
)

func TestClassifyResultPrecedence(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		result    executionResult
		outcome   store.Outcome
		reason    string
		retryable bool
	}{
		{
			name:    "completed_after_timeout",
			result:  executionResult{output: RawJSON(`true`), cause: errTimeout},
			outcome: store.Succeeded, reason: "completed",
		},
		{
			name:    "completed_after_shutdown",
			result:  executionResult{output: RawJSON(`true`), cause: errShuttingDown},
			outcome: store.Succeeded, reason: "completed",
		},
		{
			name:    "known_cancel_before_output_validation",
			result:  executionResult{output: RawJSON(`invalid`), cause: errCancelRequested},
			outcome: store.Cancelled, reason: "cancel_requested",
		},
		{
			name:    "invalid_success_output",
			result:  executionResult{output: RawJSON(`invalid`)},
			outcome: store.Failed, reason: "invalid_output",
		},
		{
			name:    "shutdown_before_permanent",
			result:  executionResult{err: Permanent(errors.New("bad input")), cause: errShuttingDown},
			outcome: store.Released, reason: "shutdown",
		},
		{
			name:    "timeout_before_permanent",
			result:  executionResult{err: Permanent(errors.New("bad input")), cause: errTimeout},
			outcome: store.Failed, reason: "timeout", retryable: true,
		},
		{
			name:    "permanent_before_cancel",
			result:  executionResult{err: Cancel(Permanent(errors.New("stop")))},
			outcome: store.Failed, reason: "business",
		},
		{
			name:    "cancel_before_snooze",
			result:  executionResult{err: Cancel(Snooze(time.Hour))},
			outcome: store.Cancelled, reason: "executor_cancelled",
		},
		{
			name:    "wrapped_snooze_ignores_output",
			result:  executionResult{output: RawJSON(`invalid`), err: fmt.Errorf("waiting: %w", Snooze(time.Hour))},
			outcome: store.Snoozed, reason: "snoozed",
		},
		{
			name:    "panic_retries",
			result:  executionResult{err: &panicError{value: "broken executor"}},
			outcome: store.Failed, reason: "panic", retryable: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := classifyResult(tc.result, 256)
			if got.outcome != tc.outcome || got.reason != tc.reason {
				t.Fatalf("outcome %v/%s, want %v/%s", got.outcome, got.reason, tc.outcome, tc.reason)
			}
			if got.outcome == store.Failed && got.retryable != tc.retryable {
				t.Errorf("retryable = %v, want %v", got.retryable, tc.retryable)
			}
			if got.outcome == store.Snoozed && (got.delay != time.Hour || len(got.output) != 0) {
				t.Errorf("snooze classification: delay %s, output %s", got.delay, got.output)
			}
		})
	}
}

// Pause after sampling the shutdown state, before admission can register its lease.
type admissionContext struct {
	context.Context
	t       *testing.T
	entered chan struct{}
	release chan struct{}
	once    atomic.Bool
}

func (c *admissionContext) Err() error {
	err := c.Context.Err()
	if c.once.CompareAndSwap(false, true) {
		close(c.entered)
		select {
		case <-c.release:
		case <-time.After(5 * time.Second):
			c.t.Error("admission barrier was not released")
		}
	}
	return err
}

func TestShutdownSerializesAdmission(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	e, err := New(pool, fastConfig(schema))
	if err != nil {
		t.Fatal(err)
	}
	exit := make(chan struct{})
	stopExecutor := sync.OnceFunc(func() { close(exit) })
	cause := make(chan error, 1)
	var calls atomic.Int32
	e.Register("x", func(ctx context.Context, _ *Request) (RawJSON, error) {
		calls.Add(1)
		select {
		case <-ctx.Done():
		case <-exit:
		case <-time.After(5 * time.Second):
			t.Error("executor was not cancelled or released")
		}
		cause <- context.Cause(ctx)
		return nil, ctx.Err()
	})
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
	trigger(t, e, "j", "")
	c := claimAsDeadHolder(t, pool, schema, "x")
	gate := &admissionContext{Context: e.loopCtx, t: t, entered: make(chan struct{}), release: make(chan struct{})}
	e.loopCtx = gate
	release := sync.OnceFunc(func() { close(gate.release) })
	defer func() {
		release()
		stopExecutor()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := e.Shutdown(ctx); err != nil && !errors.Is(err, ErrNotDrained) {
			t.Errorf("cleanup shutdown: %v", err)
		}
		if !waitGroup(ctx, &e.execs, 5*time.Second) {
			t.Error("cleanup did not drain executor")
		}
	}()
	e.dispatch(c)
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("run did not reach admission")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	shut := make(chan error, 1)
	shutdownStarted := make(chan struct{})
	go func() {
		close(shutdownStarted)
		shut <- e.Shutdown(ctx)
	}()
	select {
	case <-shutdownStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown goroutine did not start")
	}
	var shutErr error
	overtook := false
	select {
	case shutErr = <-shut:
		overtook = true
	case <-time.After(100 * time.Millisecond): // shutdown must wait for the admission critical section
	}
	release()
	if !overtook {
		select {
		case shutErr = <-shut:
		case <-time.After(5 * time.Second):
			t.Fatal("Shutdown did not finish after admission was released")
		}
	}
	stopExecutor()
	if !waitGroup(t.Context(), &e.execs, 5*time.Second) {
		t.Fatal("executor did not finish")
	}
	if overtook {
		t.Fatal("Shutdown finished between checking its state and registering the executor")
	}
	if !errors.Is(shutErr, ErrNotDrained) && shutErr != nil {
		t.Fatal(shutErr)
	}
	assertShutdownReleased(t, e, c)
	switch got := calls.Load(); got {
	case 0:
		// Shutdown may cancel the admitted holder before the executor-entry guard.
		if len(cause) != 0 {
			t.Fatal("unstarted executor reported a cancellation cause")
		}
	case 1:
		select {
		case got := <-cause:
			if !errors.Is(got, errShuttingDown) {
				t.Fatalf("executor missed shutdown cancellation: %v", got)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("finished executor did not report its cancellation cause")
		}
	default:
		t.Fatalf("executor called %d times", got)
	}
}

func TestShutdownAroundExecutorEntry(t *testing.T) {
	t.Parallel()
	for _, beforeEntry := range []bool{true, false} {
		name := "after"
		if beforeEntry {
			name = "before"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			pool, schema := freshSchema(t)
			entered := make(chan struct{})
			gate := make(chan struct{})
			release := sync.OnceFunc(func() { close(gate) })
			waitRelease := func() {
				select {
				case <-gate:
				case <-time.After(5 * time.Second):
					t.Error("executor-entry barrier was not released")
				}
			}
			cfg := fastConfig(schema)
			cfg.CancelTimeout = time.Minute
			if beforeEntry {
				cfg.Observer = ObserverFunc(func(_ context.Context, event Event) {
					if event.Type == EventRunReclaimed {
						close(entered)
						waitRelease()
					}
				})
			}
			e, err := New(pool, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				release()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := e.Shutdown(ctx); err != nil && !errors.Is(err, ErrNotDrained) {
					t.Errorf("cleanup shutdown: %v", err)
				}
				if !waitGroup(ctx, &e.execs, 5*time.Second) {
					t.Error("cleanup did not drain executor")
				}
			}()
			var calls atomic.Int32
			cause := make(chan error, 1)
			e.Register("x", func(ctx context.Context, _ *Request) (RawJSON, error) {
				calls.Add(1)
				if !beforeEntry {
					close(entered)
					waitRelease()
				}
				cause <- context.Cause(ctx)
				return RawJSON(`true`), ctx.Err()
			})
			declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
			trigger(t, e, "j", "")
			c := claimAsDeadHolder(t, pool, schema, "x")
			if beforeEntry {
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				_, err := pool.Exec(ctx, "UPDATE "+qualified(schema, "job_run")+
					" SET lease_expires_at=now()-interval '1 second' WHERE id=$1", c.Id)
				if err != nil {
					t.Fatal(err)
				}
				claimed, err := e.st.ClaimExpired(ctx, []string{"x"}, 1, "recovered", time.Minute)
				if err != nil || len(claimed) != 1 {
					t.Fatalf("reclaim: %+v, %v", claimed, err)
				}
				c = claimed[0]
			}
			e.dispatch(c)
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("run did not reach executor-entry barrier")
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if err := e.Shutdown(ctx); !errors.Is(err, ErrNotDrained) {
				t.Fatalf("shutdown with blocked holder: %v", err)
			}
			release()
			if !waitGroup(t.Context(), &e.execs, 5*time.Second) {
				t.Fatal("holder did not finish after releasing executor-entry barrier")
			}
			assertShutdownReleased(t, e, c)
			wantCalls := int32(1)
			if beforeEntry {
				wantCalls = 0
			}
			if got := calls.Load(); got != wantCalls {
				t.Fatalf("executor called %d times, want %d", got, wantCalls)
			}
			if !beforeEntry {
				select {
				case got := <-cause:
					if !errors.Is(got, errShuttingDown) {
						t.Fatalf("executor missed shutdown cancellation: %v", got)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("finished executor did not report its cancellation cause")
				}
			}
		})
	}
}

func assertShutdownReleased(t *testing.T, e *Engine, claimed store.Claimed) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	run, err := e.Runs().Get(ctx, claimed.Id)
	if err != nil {
		t.Fatal(err)
	}
	if run.State != StatePending || run.Attempt != int(claimed.Attempt) {
		t.Fatalf("released run state/attempt = %s/%d, want pending/%d", run.State, run.Attempt, claimed.Attempt)
	}
	if len(run.Output) != 0 || run.FinishedAt != nil || run.LeaseExpiresAt != nil || run.LeaseOwner != "" {
		t.Fatalf("released run retained output, completion or lease: %+v", run)
	}
	entries := errorsOf(t, run)
	wantEntries := 1
	if claimed.Reclaimed {
		wantEntries++
	}
	if len(entries) != wantEntries {
		t.Fatalf("shutdown errors = %+v, want %d entries", entries, wantEntries)
	}
	if claimed.Reclaimed && (entries[0].Kind != "interrupted" || entries[0].Attempt != int(claimed.Attempt)) {
		t.Fatalf("release did not preserve prior interruption: %+v", entries)
	}
	last := entries[len(entries)-1]
	if last.Kind != "released" || last.Message != "shutdown" || last.Attempt != int(claimed.Attempt)+1 {
		t.Fatalf("shutdown error entry: %+v", last)
	}
}

func TestLeaseLossAndCompletion(t *testing.T) {
	t.Parallel()
	e := &Engine{log: slog.New(slog.NewTextHandler(io.Discard, nil)), metrics: nopMetrics{}}
	e.inflight.m = map[int64]*inflight{}
	for round := range 10000 {
		_, cancel := context.WithCancelCause(t.Context())
		inf := &inflight{id: 1, cancel: cancel}
		e.inflight.add(t.Context(), inf)
		start := make(chan struct{})
		finished := make(chan bool, 1)
		dropped := make(chan struct{})
		go func() {
			<-start
			e.drop(inf)
			close(dropped)
		}()
		go func() {
			<-start
			e.inflight.removeIf(inf, false)
			finished <- !inf.dropped.Load()
		}()
		close(start)
		maySettle := <-finished
		<-dropped
		cancel(nil)
		if inf.dropped.Load() && maySettle {
			t.Fatalf("round %d: completion missed the winning lease-loss removal", round)
		}
	}
}

func TestBlockedNodeContext(t *testing.T) {
	for _, stop := range []bool{true, false} {
		name := "deadline"
		if stop {
			name = "shutdown"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			pool, schema := freshSchema(t)
			e, err := New(pool, fastConfig(schema))
			if err != nil {
				t.Fatal(err)
			}
			var calls atomic.Int32
			e.Register("x", func(context.Context, *Request) (RawJSON, error) {
				calls.Add(1)
				return nil, nil
			})
			declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
			declareWorkflow(t, e, WorkflowSpec{Name: "w", Nodes: []Node{{Job: "j"}}})
			triggerWorkflow(t, e, "w", "")
			c := claimAsDeadHolder(t, pool, schema, "x")
			e.st = store.Open(namedPool(t, schema), schema)
			tx, err := pool.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(context.Background())
			// A row lock does not block NodeContext's plain SELECT.
			execSql(t, tx, "LOCK TABLE "+qualified(schema, "workflow_run")+" IN ACCESS EXCLUSIVE MODE")
			e.dispatch(c)
			waitBlocked(t, pool, schema)
			if stop {
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				if err := e.Shutdown(ctx); !errors.Is(err, ErrNotDrained) {
					t.Fatalf("shutdown with blocked preflight: %v", err)
				}
				if err := tx.Commit(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			if !waitGroup(t.Context(), &e.execs, claimTimeout+5*time.Second) {
				t.Fatal("blocked NodeContext exceeded its deadline")
			}
			if err := tx.Rollback(t.Context()); err != nil && !stop {
				t.Fatal(err)
			}
			run, err := e.Runs().Get(t.Context(), c.Id)
			if err != nil {
				t.Fatal(err)
			}
			want := StateRunning // a failed preflight leaves the lease for reclaim
			if stop {
				want = StatePending
			}
			if calls.Load() != 0 || run.State != want || run.Attempt != 0 {
				t.Fatalf("calls %d, state %s, attempt %d", calls.Load(), run.State, run.Attempt)
			}
		})
	}
}

func TestObservedCancellationAfterContextEnded(t *testing.T) {
	for _, first := range []string{"timeout", "shutdown"} {
		for _, kind := range []string{"plain", "workflow"} {
			t.Run(first+"/"+kind, func(t *testing.T) {
				t.Parallel()
				pool, schema := freshSchema(t)
				cfg := fastConfig(schema)
				cfg.CancelTimeout = 5 * time.Second
				cfg.HeartbeatInterval = time.Second
				cfg.LeaseTTL = 10 * time.Second
				e, err := New(pool, cfg)
				if err != nil {
					t.Fatal(err)
				}
				entered := make(chan struct{})
				ended := make(chan error, 1)
				exit := make(chan struct{})
				release := sync.OnceFunc(func() { close(exit) })
				defer func() {
					release()
					if !waitGroup(t.Context(), &e.execs, 5*time.Second) {
						t.Error("executor did not finish")
					}
				}()
				e.Register("x", func(ctx context.Context, _ *Request) (RawJSON, error) {
					close(entered)
					<-ctx.Done()
					ended <- context.Cause(ctx)
					<-exit
					return RawJSON(`"finished anyway"`), nil
				})
				declare(t, e, JobSpec{Name: "j", ExecutorType: "x", Timeout: time.Second})
				var wfId int64
				if kind == "workflow" {
					declareWorkflow(t, e, WorkflowSpec{Name: "w", Nodes: []Node{{Job: "j"}}})
					wfId = triggerWorkflow(t, e, "w", "")
				} else {
					trigger(t, e, "j", "")
				}
				c := claimAsDeadHolder(t, pool, schema, "x")
				e.dispatch(c)
				select {
				case <-entered:
				case <-time.After(5 * time.Second):
					t.Fatal("executor did not enter")
				}
				want := errTimeout
				if first == "shutdown" {
					want = errShuttingDown
					e.inflight.cancelAll(want)
				}
				select {
				case got := <-ended:
					if !errors.Is(got, want) {
						t.Fatalf("first context cause: %v, want %v", got, want)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("executor context did not end")
				}
				if kind == "workflow" {
					err = e.Workflows().CancelRun(t.Context(), wfId)
				} else {
					err = e.Runs().Cancel(t.Context(), c.Id)
				}
				if err != nil {
					t.Fatal(err)
				}
				// Deliver the persisted request only after the first cause is fixed,
				// and do not let the executor return until delivery is complete.
				e.heartbeatOnce(t.Context())
				release()
				if !waitGroup(t.Context(), &e.execs, 5*time.Second) {
					t.Fatal("executor did not settle")
				}
				run, err := e.Runs().Get(t.Context(), c.Id)
				if err != nil {
					t.Fatal(err)
				}
				if run.State != StateCancelled || len(run.Output) != 0 || run.Attempt != 0 || string(run.Errors) != "[]" {
					t.Fatalf("observed cancellation lost: %+v", run)
				}
				if kind == "workflow" {
					waitWorkflow(t, e, wfId, WorkflowCancelled)
				}
			})
		}
	}
}

func TestCancelledLoopsDoNotStartTransactions(t *testing.T) {
	for _, name := range []string{"claim", "schedule"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			pool, schema := freshSchema(t)
			e, err := New(pool, fastConfig(schema))
			if err != nil {
				t.Fatal(err)
			}
			types := []string{"x"}
			e.types.Store(&types)
			e.stopLoops()
			e.wakeClaimer()
			e.wakeScheduler()
			before := pool.Stat().AcquireCount()
			if name == "claim" {
				e.claimLoop(e.loopCtx)
			} else {
				e.scheduleLoop(e.loopCtx)
			}
			if got := pool.Stat().AcquireCount() - before; got != 0 {
				t.Fatalf("cancelled %s loop acquired %d connections", name, got)
			}
		})
	}
}

func TestClaimDoesNotExtendHeartbeatDeadline(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	e, err := New(pool, fastConfig(schema))
	if err != nil {
		t.Fatal(err)
	}
	e.Register("x", func(context.Context, *Request) (RawJSON, error) { return nil, nil })
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
	trigger(t, e, "j", "")
	last := time.Now().Add(-time.Hour)
	e.lastOK = last
	e.claimOnce()
	if !waitGroup(t.Context(), &e.execs, 5*time.Second) {
		t.Fatal("claimed executor did not finish")
	}
	if e.lastOK != last {
		t.Fatal("a successful claim moved the heartbeat failure deadline")
	}
}
