package skein

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestHealthLifecycleAndRoles(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		worker    bool
		scheduler bool
	}{
		{name: "both", worker: true, scheduler: true},
		{name: "worker", worker: true},
		{name: "scheduler", scheduler: true},
		{name: "submit_only"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool, schema := freshSchema(t)
			cfg := fastConfig(schema)
			cfg.DisableWorker, cfg.DisableScheduler = !tc.worker, !tc.scheduler
			cfg.Concurrency = 3
			e, err := New(pool, cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = e.Shutdown(context.Background()) })
			initial := readHealth(t, e)
			if initial.Instance == "" {
				t.Fatal("new engine has no instance identity")
			}
			check := func(want EngineState) {
				t.Helper()
				got := readHealth(t, e)
				if got.State != want || got.Instance != initial.Instance {
					t.Errorf("lifecycle snapshot = %+v, want state %s and instance %q", got, want, initial.Instance)
				}
				if got.WorkerEnabled != tc.worker || got.SchedulerEnabled != tc.scheduler {
					t.Errorf("configured roles changed in state %s: %+v", want, got)
				}
				if got.Concurrency != 3 || got.SlotsUsed != 0 || got.Leases != 0 {
					t.Errorf("idle resources in state %s: %+v", want, got)
				}
			}
			check(EngineNew)
			if initial.Claim != (LoopHealth{}) || initial.Heartbeat != (LoopHealth{}) || initial.Scan != (LoopHealth{}) {
				t.Errorf("loops reported progress before Start: %+v", initial)
			}
			if err := e.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			check(EngineRunning)
			if err := e.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
			check(EngineStopped)
			if err := e.Start(t.Context()); err == nil {
				t.Fatal("Start succeeded after Shutdown")
			}
			check(EngineStopped)
		})
	}
}

func TestHealthStartingAndRetryAfterFailure(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	appName := schema + "_start"
	startPool := namedPool(t, appName)
	cfg := submitOnly(schema)
	cfg.DisableScheduler = true
	e, err := New(startPool, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Shutdown(context.Background()) })
	initial := readHealth(t, e)
	held, err := pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer rollbackTx(t, held)
	// The version check is a plain SELECT, so block it with a table lock.
	execSql(t, held, "LOCK TABLE "+qualified(schema, "schema_version")+" IN ACCESS EXCLUSIVE MODE")
	startCtx, cancelStart := context.WithCancel(t.Context())
	defer cancelStart()
	started := make(chan error, 1)
	go func() { started <- e.Start(startCtx) }()
	waitBlocked(t, pool, appName)
	if got := readHealth(t, e); got.State != EngineStarting || got.Instance != initial.Instance {
		t.Fatalf("blocked startup snapshot: %+v", got)
	}
	cancelStart()
	select {
	case err := <-started:
		if err == nil {
			t.Fatal("cancelled schema check succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled Start did not return")
	}
	got := readHealth(t, e)
	if got.State != EngineNew || got.Instance != initial.Instance {
		t.Errorf("failed Start did not return to new: %+v", got)
	}
	if got.Claim != (LoopHealth{}) || got.Heartbeat != (LoopHealth{}) || got.Scan != (LoopHealth{}) {
		t.Errorf("failed Start published loop progress: %+v", got)
	}
	if err := held.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := e.Start(t.Context()); err != nil {
		t.Fatalf("retry Start: %v", err)
	}
	if got := readHealth(t, e); got.State != EngineRunning || got.Instance != initial.Instance {
		t.Errorf("successful retry snapshot: %+v", got)
	}
}

func TestHealthSlotsOutliveLeaseAndShutdown(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	entered := make(chan struct{})
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	defer release()
	cfg := fastConfig(schema)
	cfg.DisableScheduler = true
	cfg.Concurrency = 1
	cfg.HeartbeatInterval = time.Second
	cfg.LeaseTTL = 5 * time.Second
	cfg.ShutdownGrace = time.Minute
	e := startEngineNotDrained(t, pool, cfg, func(e *Engine) {
		e.Register("stubborn", func(context.Context, *Request) (RawJSON, error) {
			close(entered)
			<-gate
			return nil, nil
		})
	})
	defer func() {
		release()
		if !waitGroup(context.Background(), &e.execs, 5*time.Second) {
			t.Error("executor did not return after gate opened")
		}
	}()
	declare(t, e, JobSpec{Name: "j", ExecutorType: "stubborn"})
	trigger(t, e, "j", "")
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("executor did not enter")
	}
	if got := readHealth(t, e); got.State != EngineRunning || got.SlotsUsed != 1 || got.Leases != 1 {
		t.Fatalf("executing snapshot: %+v", got)
	}
	_, _, infs := e.inflight.snapshot()
	if len(infs) != 1 {
		t.Fatalf("tracked leases = %d, want 1", len(infs))
	}
	e.drop(infs[0])
	if got := readHealth(t, e); got.State != EngineRunning || got.SlotsUsed != 1 || got.Leases != 0 {
		t.Fatalf("lost lease should retain execution slot: %+v", got)
	}
	stopCtx, cancelStop := context.WithCancel(t.Context())
	defer cancelStop()
	stopped := make(chan error, 1)
	go func() { stopped <- e.Shutdown(stopCtx) }()
	waitFor(t, "Shutdown to stop admission", func(ctx context.Context) bool { return e.loopCtx.Err() != nil })
	if got := readHealth(t, e); got.State != EngineStopping || got.SlotsUsed != 1 || got.Leases != 0 {
		t.Fatalf("draining snapshot: %+v", got)
	}
	cancelStop()
	select {
	case err := <-stopped:
		if !errors.Is(err, ErrNotDrained) {
			t.Fatalf("Shutdown = %v, want ErrNotDrained", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not honor its cancelled budget")
	}
	if got := readHealth(t, e); got.State != EngineStopped || got.SlotsUsed != 1 || got.Leases != 0 {
		t.Fatalf("stopped engine lost its remaining execution slot: %+v", got)
	}
	release()
	if !waitGroup(t.Context(), &e.execs, 5*time.Second) {
		t.Fatal("executor did not finish")
	}
	if got := readHealth(t, e); got.State != EngineStopped || got.SlotsUsed != 0 || got.Leases != 0 {
		t.Errorf("completed executor left resources behind: %+v", got)
	}
}

func TestHealthFromObserverDuringShutdown(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	entered := make(chan struct{})
	gate := make(chan struct{})
	release := sync.OnceFunc(func() { close(gate) })
	defer release()
	observed := make(chan Health, 1)
	var e *Engine
	cfg := submitOnly(schema)
	cfg.DisableScheduler = true
	cfg.Concurrency = 1
	cfg.ShutdownGrace = time.Minute
	cfg.Observer = ObserverFunc(func(context.Context, Event) {
		close(entered)
		<-gate
		observed <- e.Health()
	})
	e = startEngine(t, pool, cfg, func(e *Engine) {
		e.Register("quick", func(context.Context, *Request) (RawJSON, error) { return nil, nil })
	})
	declare(t, e, JobSpec{Name: "j", ExecutorType: "quick"})
	id := trigger(t, e, "j", "")
	e.dispatch(claimAsDeadHolder(t, pool, schema, "quick"))
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("observer did not enter")
	}
	waitRun(t, e, id, StateSucceeded)
	if got := readHealth(t, e); got.SlotsUsed != 1 || got.Leases != 0 {
		t.Fatalf("observer should retain a slot after lease settlement: %+v", got)
	}
	stopCtx, cancelStop := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelStop()
	stopped := make(chan error, 1)
	go func() { stopped <- e.Shutdown(stopCtx) }()
	// Shutdown has acquired lifecycle and waits for the callback's worker.
	waitFor(t, "Shutdown to stop admission", func(ctx context.Context) bool { return e.loopCtx.Err() != nil })
	release()
	select {
	case got := <-observed:
		if got.State != EngineStopping || got.SlotsUsed != 1 || got.Leases != 0 {
			t.Errorf("observer snapshot during shutdown: %+v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Health in Observer waited for Shutdown's lifecycle lock")
	}
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("Shutdown after callback completed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not finish after Observer returned")
	}
	if got := readHealth(t, e); got.State != EngineStopped || got.SlotsUsed != 0 || got.Leases != 0 {
		t.Errorf("final snapshot: %+v", got)
	}
}

func readHealth(t *testing.T, e *Engine) Health {
	t.Helper()
	done := make(chan Health, 1)
	go func() { done <- e.Health() }()
	select {
	case got := <-done:
		return got
	case <-time.After(5 * time.Second):
		t.Fatal("Health blocked on engine work")
		return Health{}
	}
}
