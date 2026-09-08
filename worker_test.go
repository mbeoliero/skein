package skein

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mbeoliero/skein/internal/store"
)

// Pause after sampling the shutdown state, before admission can register its lease.
type admissionContext struct {
	context.Context
	entered chan struct{}
	release chan struct{}
	once    atomic.Bool
}

func (c *admissionContext) Err() error {
	err := c.Context.Err()
	if c.once.CompareAndSwap(false, true) {
		close(c.entered)
		<-c.release
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
	defer stopExecutor()
	cause := make(chan error, 1)
	e.Register("x", func(ctx context.Context, _ *Request) (RawJSON, error) {
		select {
		case <-ctx.Done():
		case <-exit:
		}
		cause <- context.Cause(ctx)
		return nil, ctx.Err()
	})
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
	trigger(t, e, "j", "")
	c := claimAsDeadHolder(t, pool, schema, "x")
	gate := &admissionContext{Context: e.loopCtx, entered: make(chan struct{}), release: make(chan struct{})}
	e.loopCtx = gate
	release := sync.OnceFunc(func() { close(gate.release) })
	defer release()
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
	if got := <-cause; !errors.Is(got, errShuttingDown) {
		t.Fatalf("executor missed shutdown cancellation: %v", got)
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

func TestClaimDoesNotExtendHeartbeatDeadline(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	e, err := New(pool, fastConfig(schema))
	if err != nil {
		t.Fatal(err)
	}
	e.Register("x", func(context.Context, *Request) (RawJSON, error) { return nil, nil })
	e.types = e.registeredTypes()
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
