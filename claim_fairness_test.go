package skein

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func claimTestEngine(t *testing.T, pool *pgxpool.Pool, schema string, concurrency int) *Engine {
	t.Helper()
	cfg := fastConfig(schema)
	cfg.Concurrency, cfg.LeaseTTL = concurrency, time.Minute
	e, err := New(pool, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := e.Shutdown(ctx); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})
	return e
}

func finishClaimRound(t *testing.T, e *Engine) {
	t.Helper()
	e.claimOnce()
	if !waitGroup(t.Context(), &e.execs, 5*time.Second) {
		t.Fatal("claimed executions did not finish")
	}
}

func TestClaimFairnessWithOneSlot(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"both", "pending_only", "expired_only"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			pool, schema := freshSchema(t)
			e := claimTestEngine(t, pool, schema, 1)
			for range 3 {
				e.claimOnce() // An unregistered idle tick must not consume the cycle.
			}
			entered := make(chan string, 1)
			executor := func(_ context.Context, req *Request) (RawJSON, error) {
				entered <- req.JobName
				return nil, nil
			}
			e.Register("pending", executor)
			e.Register("expired", executor)
			declare(t, e, JobSpec{Name: "pending", ExecutorType: "pending"})
			declare(t, e, JobSpec{Name: "expired", ExecutorType: "expired"})
			const rounds = 16
			if mode != "pending_only" {
				for range rounds {
					trigger(t, e, "expired", "")
				}
				rows, err := e.st.ClaimPending(t.Context(), []string{"expired"}, rounds, "dead", time.Minute)
				if err != nil || len(rows) != rounds {
					t.Fatalf("prepare expired claims: %d rows, %v", len(rows), err)
				}
				execSql(t, pool, "UPDATE "+qualified(schema, "job_run")+
					" SET lease_expires_at = now() - interval '1 hour' WHERE executor_type = 'expired'")
			}
			for round := 1; round <= rounds; round++ {
				if mode != "expired_only" {
					trigger(t, e, "pending", "") // Keep pending work available across recovery rounds.
				}
				e.slots <- struct{}{}
				e.claimOnce() // A full-slot idle tick must not consume the cycle either.
				<-e.slots
				finishClaimRound(t, e)
				want := "pending"
				if mode == "expired_only" || mode == "both" && round%8 == 0 {
					want = "expired"
				}
				select {
				case got := <-entered:
					if got != want {
						t.Fatalf("round %d executed %s, want %s", round, got, want)
					}
				default:
					t.Fatalf("round %d left its slot unused", round)
				}
			}
		})
	}
}

func TestClaimFairnessFinishesCancelledWorkflow(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	e := claimTestEngine(t, pool, schema, 1)
	var cancelledExecutions atomic.Int32
	e.Register("expired", func(context.Context, *Request) (RawJSON, error) {
		cancelledExecutions.Add(1)
		return nil, nil
	})
	e.Register("pending", func(context.Context, *Request) (RawJSON, error) { return nil, nil })
	declare(t, e, JobSpec{Name: "node", ExecutorType: "expired"})
	declare(t, e, JobSpec{Name: "pending", ExecutorType: "pending"})
	declareWorkflow(t, e, WorkflowSpec{Name: "w", Nodes: []Node{{Job: "node"}}})
	id := triggerWorkflow(t, e, "w", "")
	claimAsDeadHolder(t, pool, schema, "expired")
	if err := e.Workflows().CancelRun(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	execSql(t, pool, "UPDATE "+qualified(schema, "job_run")+
		" SET lease_expires_at = now() - interval '1 hour' WHERE executor_type = 'expired'")
	for range 8 {
		trigger(t, e, "pending", "")
		finishClaimRound(t, e)
	}
	run, err := e.Workflows().GetRun(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if run.State != WorkflowCancelled || run.Nodes[0].State != StateCancelled || cancelledExecutions.Load() != 0 {
		t.Fatalf("cancelled workflow did not converge during pending backlog: %+v, calls %d", run, cancelledExecutions.Load())
	}
}

func TestClaimBranchFailurePreservesOtherBranch(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name         string
		expiredFirst bool
		failExpired  bool
	}{
		{name: "pending_first_failure"},
		{name: "pending_first_success", failExpired: true},
		{name: "expired_first_failure", expiredFirst: true, failExpired: true},
		{name: "expired_first_success", expiredFirst: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool, schema := freshSchema(t)
			e := claimTestEngine(t, pool, schema, 2)
			e.Register("x", func(context.Context, *Request) (RawJSON, error) { return nil, nil })
			rounds := 1
			if tc.expiredFirst {
				rounds = 7
			}
			for range rounds {
				e.claimOnce()
			}
			good := e.Health().Claim
			declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
			oldId := trigger(t, e, "j", "")
			old := claimAsDeadHolder(t, pool, schema, "x")
			jr, fn := qualified(schema, "job_run"), qualified(schema, "reject_branch")
			execSql(t, pool, "UPDATE "+jr+
				" SET lease_expires_at = now() - interval '1 hour' WHERE id = "+itoa(oldId))
			pendingId := trigger(t, e, "j", "")
			oldState, successfulId := "pending", oldId
			if tc.failExpired {
				oldState, successfulId = "running", pendingId
			}
			execSql(t, pool, "CREATE FUNCTION "+fn+`() RETURNS trigger LANGUAGE plpgsql AS $$
				BEGIN RAISE EXCEPTION 'forced branch failure'; END $$`)
			execSql(t, pool, "CREATE TRIGGER reject_branch BEFORE UPDATE ON "+jr+
				" FOR EACH ROW WHEN (OLD.state = '"+oldState+"' AND NEW.state = 'running') EXECUTE FUNCTION "+fn+"()")
			finishClaimRound(t, e)
			health := e.Health().Claim
			if health.ConsecutiveErrors != 1 || !health.LastSuccess.Equal(good.LastSuccess) ||
				!strings.Contains(health.LastError, "forced branch failure") {
				t.Fatalf("failed round lost its error: %+v", health)
			}
			waitRun(t, e, successfulId, StateSucceeded)
			if tc.failExpired {
				if token := leaseToken(t, pool, schema, oldId); token != old.LeaseToken {
					t.Fatal("failed expired branch replaced its lease")
				}
			} else {
				waitRun(t, e, pendingId, StatePending)
			}
			execSql(t, pool, "DROP TRIGGER reject_branch ON "+jr)
			finishClaimRound(t, e)
			waitRun(t, e, oldId, StateSucceeded)
			waitRun(t, e, pendingId, StateSucceeded)
			if health := e.Health().Claim; health.ConsecutiveErrors != 0 || health.LastError != "" {
				t.Fatalf("successful recovery retained error: %+v", health)
			}
		})
	}
}

func TestClaimFailuresAdvanceFairnessCycle(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	e := claimTestEngine(t, pool, schema, 1)
	entered := make(chan int64, 1)
	e.Register("x", func(_ context.Context, req *Request) (RawJSON, error) {
		entered <- req.RunId
		return nil, nil
	})
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
	oldId := trigger(t, e, "j", "")
	claimAsDeadHolder(t, pool, schema, "x")
	jr, fn := qualified(schema, "job_run"), qualified(schema, "reject_all_claims")
	execSql(t, pool, "UPDATE "+jr+" SET lease_expires_at = now() - interval '1 hour' WHERE id = "+itoa(oldId))
	trigger(t, e, "j", "")
	execSql(t, pool, "CREATE FUNCTION "+fn+`() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'forced claim failure'; END $$`)
	execSql(t, pool, "CREATE TRIGGER reject_all_claims BEFORE UPDATE ON "+jr+
		" FOR EACH ROW WHEN (NEW.state = 'running') EXECUTE FUNCTION "+fn+"()")
	for round := 1; round <= 7; round++ {
		finishClaimRound(t, e)
		if health := e.Health().Claim; health.ConsecutiveErrors != round {
			t.Fatalf("round %d counted multiple branch errors: %+v", round, health)
		}
	}
	execSql(t, pool, "DROP TRIGGER reject_all_claims ON "+jr)
	finishClaimRound(t, e)
	select {
	case id := <-entered:
		if id != oldId {
			t.Fatalf("eighth round executed pending %d instead of expired %d", id, oldId)
		}
	default:
		t.Fatal("eighth round did not execute either candidate")
	}
}

func TestClaimExpiredOrdersOldestFirst(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	e := claimTestEngine(t, pool, schema, 1)
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
	ids := make([]int64, 5)
	for i := range ids {
		ids[i] = trigger(t, e, "j", "")
	}
	rows, err := e.st.ClaimPending(t.Context(), []string{"x"}, len(ids), "dead", time.Minute)
	if err != nil || len(rows) != len(ids) {
		t.Fatalf("prepare expired claims: %d rows, %v", len(rows), err)
	}
	now, err := e.st.Now(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	// Creation order disagrees with expiry order; both oldest and newest pairs
	// share exact database timestamps, so their tie must be resolved by id.
	for i, hoursAgo := range []int{1, 3, 2, 3, 1} {
		_, err := pool.Exec(
			t.Context(),
			"UPDATE "+qualified(schema, "job_run")+" SET lease_expires_at = $2 WHERE id = $1",
			ids[i],
			now.Add(-time.Duration(hoursAgo)*time.Hour),
		)
		if err != nil {
			t.Fatal(err)
		}
	}
	for turn, index := range []int{1, 3, 2, 0, 4} {
		claimed, err := e.st.ClaimExpired(t.Context(), []string{"x"}, 1, "recovered", time.Hour)
		if err != nil || len(claimed) != 1 {
			t.Fatalf("reclaim %d: %d rows, %v", turn, len(claimed), err)
		}
		if claimed[0].Id != ids[index] {
			t.Fatalf("reclaim %d chose run %d, want run %d", turn, claimed[0].Id, ids[index])
		}
	}
}
