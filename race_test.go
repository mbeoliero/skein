package skein

import (
	"errors"
	"sync"
	"testing"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mbeoliero/skein/internal/store"
)

// §2.9 lock-order checks: each pair of transactions that can touch the same rows runs
// concurrently for many rounds; a deadlock surfaces as SQLSTATE 40P01. The pairs are
// driven through Store directly so both sides start at the same instant.

func raceRounds() int {
	if testing.Short() {
		return 100
	}
	return 1000
}

func isDeadlock(err error) bool {
	pgErr, ok := errors.AsType[*pgconn.PgError](err)
	return ok && pgErr.Code == "40P01"
}

// parallel runs fns at once, released together by one start signal so the first is
// not already inside its transaction while the last is still being spawned, and
// returns their errors in order.
func parallel(fns ...func() error) []error {
	errs := make([]error, len(fns))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, fn := range fns {
		wg.Go(func() {
			<-start
			errs[i] = fn()
		})
	}
	close(start)
	wg.Wait()
	return errs
}

func noDeadlock(t *testing.T, round int, errs []error) {
	t.Helper()
	for _, err := range errs {
		if isDeadlock(err) {
			t.Fatalf("round %d: %v", round, err)
		}
	}
}

// claimAsDeadHolder claims one pending row of the type with a lease that never renews.
func claimAsDeadHolder(t *testing.T, pool *pgxpool.Pool, schema, executorType string) store.Claimed {
	t.Helper()
	got, err := store.Open(pool, schema).ClaimPending(t.Context(), []string{executorType}, 1, "dead", 200*time.Millisecond)
	if err != nil || len(got) != 1 {
		t.Fatalf("claim: %v %v", got, err)
	}
	return got[0]
}

func TestRaceClaimVsCancel(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	e := startEngine(t, pool, submitOnly(schema), nil)
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
	st := store.Open(pool, schema)
	ctx := t.Context()
	for round := range raceRounds() {
		id := trigger(t, e, "j", "")
		errs := parallel(
			func() error { _, err := st.ClaimPending(ctx, []string{"x"}, 1, "A", time.Minute); return err },
			func() error { return e.Runs().Cancel(ctx, id) },
		)
		noDeadlock(t, round, errs)
		if errs[0] != nil || errs[1] != nil {
			t.Fatalf("round %d: claim %v cancel %v", round, errs[0], errs[1])
		}
		run, _ := e.Runs().Get(ctx, id)
		if !(run.State == StateCancelled || (run.State == StateRunning && run.CancelRequested)) {
			t.Fatalf("round %d: %s cancel_requested %v", round, run.State, run.CancelRequested)
		}
	}
}

// Two parallel predecessors settle at the same instant; the join is activated once
// and the parent lock serialises the two propagates.
func TestRaceTwoSettlesSameWorkflow(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	e := startEngine(t, pool, submitOnly(schema), nil)
	for _, j := range []string{"x", "y", "z"} {
		declare(t, e, JobSpec{Name: j, ExecutorType: "x"})
	}
	declareWorkflow(t, e, WorkflowSpec{Name: "w", Nodes: []Node{{Job: "x"}, {Job: "y"}, {Job: "z", Deps: []string{"x", "y"}}}})
	st := store.Open(pool, schema)
	ctx := t.Context()
	for round := range raceRounds() {
		id := triggerWorkflow(t, e, "w", "")
		got, err := st.ClaimPending(ctx, []string{"x"}, 2, "A", time.Minute)
		if err != nil || len(got) != 2 {
			t.Fatalf("round %d claim: %v %v", round, got, err)
		}
		settle := func(c store.Claimed) func() error {
			return func() error {
				s := settlementFor(c, store.Succeeded)
				s.Output = []byte(`1`)
				_, err := st.Settle(ctx, s)
				return err
			}
		}
		errs := parallel(settle(got[0]), settle(got[1]))
		noDeadlock(t, round, errs)
		if errs[0] != nil || errs[1] != nil {
			t.Fatalf("round %d: %v %v", round, errs[0], errs[1])
		}
		run, _ := e.Workflows().GetRun(ctx, id)
		if z := nodeOf(t, run, "z"); z.State != StatePending || run.State != WorkflowRunning {
			t.Fatalf("round %d: z %s workflow %s", round, z.State, run.State)
		}
		// settle z so the next round's claim picks the new x and y only
		zc, err := st.ClaimPending(ctx, []string{"x"}, 1, "A", time.Minute)
		if err != nil || len(zc) != 1 {
			t.Fatalf("round %d claim z: %v %v", round, zc, err)
		}
		if _, err := st.Settle(ctx, settlementFor(zc[0], store.Cancelled)); err != nil {
			t.Fatal(err)
		}
	}
}

// The last node's failing settle (which finalises the run) races a Resume call.
func TestRaceSettleVsResume(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	e := startEngine(t, pool, submitOnly(schema), nil)
	declare(t, e, JobSpec{Name: "x", ExecutorType: "x", Retry: RetryPolicy{MaxAttempts: 1}})
	declareWorkflow(t, e, WorkflowSpec{Name: "w", Nodes: []Node{{Job: "x"}}})
	st := store.Open(pool, schema)
	ctx := t.Context()
	entry := encodeErr(errEntry{Attempt: 1, Kind: "business", Message: "x"})
	for round := range raceRounds() {
		id := triggerWorkflow(t, e, "w", "")
		c := claimAsDeadHolder(t, pool, schema, "x")
		errs := parallel(
			func() error {
				s := settlementFor(c, store.Failed)
				s.Err = entry
				_, err := st.Settle(ctx, s)
				return err
			},
			func() error { return e.Workflows().Resume(ctx, id) },
		)
		noDeadlock(t, round, errs)
		if errs[0] != nil {
			t.Fatalf("round %d: settle %v", round, errs[0])
		}
		run, _ := e.Workflows().GetRun(ctx, id)
		x := nodeOf(t, run, "x")
		switch {
		case errs[1] == nil && run.State == WorkflowRunning && x.State == StatePending:
		case errors.Is(errs[1], ErrNotResumable) && run.State == WorkflowFailed && x.State == StateFailed:
		default:
			t.Fatalf("round %d: resume %v, workflow %s, x %s", round, errs[1], run.State, x.State)
		}
		if run.State == WorkflowRunning { // finish the resumed run so the next round's claim is clean
			if err := e.Workflows().CancelRun(ctx, id); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestRaceSnoozeVsWorkflowCancellation(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"cancel", "fail_fast"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			pool, schema := freshSchema(t)
			e := startEngine(t, pool, submitOnly(schema), nil)
			for _, job := range []string{"x", "y", "z"} {
				declare(t, e, JobSpec{Name: job, ExecutorType: "x"})
			}
			nodes := []Node{{Job: "y"}, {Job: "z", Deps: []string{"y"}}}
			roots, want := 1, WorkflowCancelled
			if name == "fail_fast" {
				nodes = append(nodes, Node{Job: "x"})
				roots, want = 2, WorkflowFailed
			}
			declareWorkflow(t, e, WorkflowSpec{Name: "w", Nodes: nodes})
			st, ctx := store.Open(pool, schema), t.Context()
			for round := range raceRounds() {
				id := triggerWorkflow(t, e, "w", "")
				got, err := st.ClaimPending(ctx, []string{"x"}, roots, "A", time.Minute)
				if err != nil || len(got) != roots {
					t.Fatalf("round %d claim: %v %v", round, got, err)
				}
				var snoozing, failing store.Claimed
				for _, c := range got {
					if c.JobName == "y" {
						snoozing = c
					} else {
						failing = c
					}
				}
				errs := parallel(
					func() error {
						s := settlementFor(snoozing, store.Snoozed)
						s.Delay = 24 * time.Hour
						_, err := st.Settle(ctx, s)
						return err
					},
					func() error {
						if name == "cancel" {
							_, err := st.CancelWorkflow(ctx, id)
							return err
						}
						s := settlementFor(failing, store.Failed)
						s.Err = encodeErr(errEntry{Attempt: 1, Kind: "business", Message: "failed"})
						_, err := st.Settle(ctx, s)
						return err
					},
				)
				noDeadlock(t, round, errs)
				if errs[0] != nil || errs[1] != nil {
					t.Fatalf("round %d: snooze %v cancel %v", round, errs[0], errs[1])
				}
				run, err := e.Workflows().GetRun(ctx, id)
				if err != nil || run.State != want {
					t.Fatalf("round %d: workflow %+v %v", round, run, err)
				}
				y, z := nodeOf(t, run, "y"), nodeOf(t, run, "z")
				if y.State != StateCancelled || y.Attempt != 0 || y.Output != nil || z.State != StateCancelled {
					t.Fatalf("round %d: y %+v z %+v", round, y, z)
				}
			}
		})
	}
}

// A heartbeat renewing two running nodes of one workflow races the fail-fast settle
// of one of them: fail-fast must not wait for additional running rows (§2.9).
func TestRaceHeartbeatVsFailFast(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	e := startEngine(t, pool, submitOnly(schema), nil)
	for _, j := range []string{"x", "y", "z"} {
		declare(t, e, JobSpec{Name: j, ExecutorType: "x", Retry: RetryPolicy{MaxAttempts: 1}})
	}
	declareWorkflow(t, e, WorkflowSpec{Name: "w", Nodes: []Node{{Job: "x"}, {Job: "y"}, {Job: "z", Deps: []string{"x"}}}})
	st := store.Open(pool, schema)
	ctx := t.Context()
	entry := encodeErr(errEntry{Attempt: 1, Kind: "business", Message: "x"})
	for round := range raceRounds() {
		id := triggerWorkflow(t, e, "w", "")
		got, err := st.ClaimPending(ctx, []string{"x"}, 2, "A", time.Minute)
		if err != nil || len(got) != 2 {
			t.Fatalf("round %d claim: %v %v", round, got, err)
		}
		ids, tokens := []int64{got[0].Id, got[1].Id}, []uuid.UUID{got[0].LeaseToken, got[1].LeaseToken}
		var failing store.Claimed
		for _, c := range got {
			if c.JobName == "x" {
				failing = c
			}
		}
		errs := parallel(
			func() error {
				for range 3 {
					if _, err := st.Heartbeat(ctx, ids, tokens, time.Minute, time.Second); err != nil {
						return err
					}
				}
				return nil
			},
			func() error {
				s := settlementFor(failing, store.Failed)
				s.Err = entry
				_, err := st.Settle(ctx, s)
				return err
			},
		)
		noDeadlock(t, round, errs)
		if errs[0] != nil || errs[1] != nil {
			t.Fatalf("round %d: %v %v", round, errs[0], errs[1])
		}
		run, _ := e.Workflows().GetRun(ctx, id)
		if run.State != WorkflowCancelling || nodeOf(t, run, "y").State != StateRunning || nodeOf(t, run, "z").State != StateCancelled {
			t.Fatalf("round %d: workflow %s y %s z %s", round, run.State, nodeOf(t, run, "y").State, nodeOf(t, run, "z").State)
		}
		if err := e.Workflows().CancelRun(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRaceScheduledResumeVsScan(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		workflow  bool
		recreated bool
	}{
		{name: "job"},
		{name: "workflow", workflow: true},
		{name: "recreated_job", recreated: true},
		{name: "recreated_workflow", workflow: true, recreated: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e, pool, schema := protocolEngine(t)
			for round := range raceRounds() {
				spec := protocolSpec(tc.workflow)
				if err := e.Schedules().Put(t.Context(), spec); err != nil {
					t.Fatal(err)
				}
				old := protocolBeat(t, e, pool, schema, "s", 1)
				cancelProtocolBeat(t, e, old)
				spec.Overlap = OverlapSkip
				if tc.recreated {
					if err := e.Schedules().Delete(t.Context(), "s"); err != nil {
						t.Fatal(err)
					}
				} else {
					if err := e.Schedules().Put(t.Context(), spec); err != nil {
						t.Fatal(err)
					}
					execSql(t, pool, "UPDATE "+qualified(schema, "schedule")+" SET next_run_at = now() - interval '1 hour'")
				}
				var scan store.Scan
				errs := parallel(
					func() error { return resumeProtocolBeat(t.Context(), e, old) },
					func() error {
						if tc.recreated {
							if err := e.Schedules().Put(t.Context(), spec); err != nil {
								return err
							}
							_, err := pool.Exec(t.Context(), "UPDATE "+qualified(schema, "schedule")+" SET next_run_at = now() - interval '1 hour'")
							if err != nil {
								return err
							}
						}
						var err error
						scan, err = e.st.ScanDue(t.Context(), nextRun)
						return err
					},
				)
				noDeadlock(t, round, errs)
				if errs[1] != nil {
					t.Fatalf("round %d scan: %v", round, errs[1])
				}
				switch {
				case errs[0] == nil:
					if len(scan.Fired) > 0 && !scan.Fired[0].Skipped {
						t.Fatalf("round %d resumed and fired: %+v", round, scan)
					}
					cancelProtocolBeat(t, e, old)
				case errors.Is(errs[0], ErrDuplicate):
					if len(scan.Fired) != 1 || scan.Fired[0].Skipped || scan.Fired[0].RunId == 0 {
						t.Fatalf("round %d duplicate without a winning scan: %+v", round, scan)
					}
					cancelProtocolBeat(t, e, scan.Fired[0])
				default:
					t.Fatalf("round %d resume: %v", round, errs[0])
				}
				// A skipped locked candidate remains due; clear it before making the next fixture.
				dueAt(t, pool, schema, "s", nextNewYear())
			}
		})
	}
}

func TestRaceHistoricalAllowResumes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		workflow bool
	}{
		{name: "job"},
		{name: "workflow", workflow: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e, pool, schema := protocolEngine(t)
			for round := range raceRounds() {
				spec := protocolSpec(tc.workflow)
				if err := e.Schedules().Put(t.Context(), spec); err != nil {
					t.Fatal(err)
				}
				first := protocolBeat(t, e, pool, schema, "s", 1)
				second := protocolBeat(t, e, pool, schema, "s", 2)
				cancelProtocolBeat(t, e, first)
				cancelProtocolBeat(t, e, second)
				spec.Overlap = OverlapSkip
				if err := e.Schedules().Put(t.Context(), spec); err != nil {
					t.Fatal(err)
				}
				errs := parallel(
					func() error { return resumeProtocolBeat(t.Context(), e, first) },
					func() error { return resumeProtocolBeat(t.Context(), e, second) },
				)
				noDeadlock(t, round, errs)
				switch {
				case errs[0] == nil && errors.Is(errs[1], ErrDuplicate):
					cancelProtocolBeat(t, e, first)
				case errs[1] == nil && errors.Is(errs[0], ErrDuplicate):
					cancelProtocolBeat(t, e, second)
				default:
					t.Fatalf("round %d needs exactly one admitted Resume: %v", round, errs)
				}
			}
		})
	}
}
