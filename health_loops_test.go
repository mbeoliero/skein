package skein

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mbeoliero/skein/internal/store"
)

func TestHealthClaimFailureIdleAndRecovery(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	cfg := fastConfig(schema)
	cfg.Concurrency = 1
	e, err := New(pool, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.stopLoops)
	t.Cleanup(e.stopHb)
	e.claimOnce()
	if h := e.Health().Claim; h.LastTick.IsZero() || !h.LastSuccess.IsZero() || h.ConsecutiveErrors != 0 {
		t.Fatalf("unregistered idle: %+v", h)
	}
	e.Register("x", func(context.Context, *Request) (RawJSON, error) { return nil, nil })
	e.claimOnce()
	good := e.Health().Claim
	if good.LastSuccess.IsZero() || good.ConsecutiveErrors != 0 {
		t.Fatalf("empty claim: %+v", good)
	}
	jr := qualified(schema, "job_run")
	fn := qualified(schema, "reject_claim")
	execSql(t, pool, "CREATE FUNCTION "+fn+`() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'forced pending claim failure'; END $$`)
	execSql(t, pool, "CREATE TRIGGER reject_claim BEFORE UPDATE ON "+jr+" FOR EACH STATEMENT EXECUTE FUNCTION "+fn+"()")
	for n := range 2 {
		e.claimOnce()
		if h := e.Health().Claim; h.ConsecutiveErrors != n+1 || !h.LastSuccess.Equal(good.LastSuccess) || !strings.Contains(h.LastError, "forced pending claim failure") {
			t.Fatalf("failed round %d: %+v", n+1, h)
		}
	}
	failed := e.Health().Claim
	e.slots <- struct{}{}
	e.claimOnce()
	<-e.slots
	if h := e.Health().Claim; !h.LastTick.After(failed.LastTick) || !h.LastSuccess.Equal(failed.LastSuccess) || h.ConsecutiveErrors != failed.ConsecutiveErrors || h.LastError != failed.LastError {
		t.Fatalf("full-slot idle changed result: before %+v, after %+v", failed, h)
	}
	execSql(t, pool, "DROP TRIGGER reject_claim ON "+jr)
	e.claimOnce()
	if h := e.Health().Claim; !h.LastSuccess.After(good.LastSuccess) || h.ConsecutiveErrors != 0 || h.LastError != "" {
		t.Fatalf("recovery: %+v", h)
	}
}

func TestHealthClaimDispatchesPendingWhenReclaimFails(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	cfg := fastConfig(schema)
	cfg.Concurrency = 2
	cfg.LeaseTTL = time.Minute
	e, err := New(pool, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.stopLoops)
	t.Cleanup(e.stopHb)
	t.Cleanup(e.execs.Wait)
	e.Register("x", func(context.Context, *Request) (RawJSON, error) { return nil, nil })
	e.claimOnce()
	good := e.Health().Claim
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
	oldId := trigger(t, e, "j", "")
	old := claimAsDeadHolder(t, pool, schema, "x")
	jr, fn := qualified(schema, "job_run"), qualified(schema, "reject_reclaim")
	execSql(t, pool, "UPDATE "+jr+" SET lease_expires_at = now() - interval '1 second' WHERE id = "+itoa(oldId))
	pendingId := trigger(t, e, "j", "")
	execSql(t, pool, "CREATE FUNCTION "+fn+`() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'forced expired claim failure'; END $$`)
	execSql(t, pool, "CREATE TRIGGER reject_reclaim BEFORE UPDATE ON "+jr+
		" FOR EACH ROW WHEN (OLD.state = 'running' AND NEW.state = 'running') EXECUTE FUNCTION "+fn+"()")
	e.claimOnce()
	if h := e.Health().Claim; h.ConsecutiveErrors != 1 || !h.LastSuccess.Equal(good.LastSuccess) || !strings.Contains(h.LastError, "forced expired claim failure") {
		t.Fatalf("mixed round was reported successful: %+v", h)
	}
	waitRun(t, e, pendingId, StateSucceeded)
	e.execs.Wait()
	if token := leaseToken(t, pool, schema, oldId); token != old.LeaseToken {
		t.Fatalf("failed reclaim changed token: %v != %v", token, old.LeaseToken)
	}
	execSql(t, pool, "DROP TRIGGER reject_reclaim ON "+jr)
	e.claimOnce()
	if h := e.Health().Claim; !h.LastSuccess.After(good.LastSuccess) || h.ConsecutiveErrors != 0 || h.LastError != "" {
		t.Fatalf("reclaim recovery: %+v", h)
	}
	waitRun(t, e, oldId, StateSucceeded)
}

type healthClaimBoundary struct {
	nopMetrics
	afterExpired func()
}

func (m healthClaimBoundary) Count(name string, _ int, _ ...string) {
	if name == "reclaim_total" {
		m.afterExpired()
	}
}

func TestHealthClaimIncludesNextPendingRead(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	e, err := New(pool, fastConfig(schema))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.stopLoops)
	t.Cleanup(e.stopHb)
	e.Register("x", func(context.Context, *Request) (RawJSON, error) { return nil, nil })
	e.claimOnce()
	good := e.Health().Claim
	// This existing callback runs between the committed expired branch and the
	// next-due read. Hide the empty fixture table at that exact boundary.
	e.metrics = healthClaimBoundary{afterExpired: func() {
		execSql(t, pool, "ALTER TABLE "+qualified(schema, "job_run")+" RENAME TO hidden_job_run")
	}}
	e.claimOnce()
	if h := e.Health().Claim; h.ConsecutiveErrors != 1 || !h.LastSuccess.Equal(good.LastSuccess) || !strings.Contains(h.LastError, "job_run") {
		t.Fatalf("next-due failure lost: %+v", h)
	}
	e.metrics = nopMetrics{}
	execSql(t, pool, "ALTER TABLE "+qualified(schema, "hidden_job_run")+" RENAME TO job_run")
	e.claimOnce()
	if h := e.Health().Claim; !h.LastSuccess.After(good.LastSuccess) || h.ConsecutiveErrors != 0 || h.LastError != "" {
		t.Fatalf("next-due recovery: %+v", h)
	}
}

func TestHealthHeartbeatFailureIdleRecoveryAndShutdown(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	cfg := fastConfig(schema)
	cfg.HeartbeatInterval = 5 * time.Second
	cfg.LeaseTTL = time.Minute
	e, err := New(pool, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.stopLoops)
	t.Cleanup(e.stopHb)
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
	id := trigger(t, e, "j", "")
	c := claimAsDeadHolder(t, pool, schema, "x")
	_, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	inf := &inflight{id: id, token: c.LeaseToken, cancel: cancel}
	if !e.inflight.add(e.loopCtx, inf) {
		t.Fatal("lease admission rejected")
	}
	e.heartbeatOnce(t.Context())
	good := e.Health().Heartbeat
	if good.LastSuccess.IsZero() || good.ConsecutiveErrors != 0 || inf.dropped.Load() {
		t.Fatalf("initial heartbeat: %+v, dropped %v", good, inf.dropped.Load())
	}
	jr, fn := qualified(schema, "job_run"), qualified(schema, "reject_heartbeat")
	execSql(t, pool, "CREATE FUNCTION "+fn+`() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'forced heartbeat failure'; END $$`)
	execSql(t, pool, "CREATE TRIGGER reject_heartbeat BEFORE UPDATE OF lease_expires_at ON "+jr+
		" FOR EACH ROW EXECUTE FUNCTION "+fn+"()")
	for n := range 2 {
		e.heartbeatOnce(t.Context())
		if h := e.Health().Heartbeat; h.ConsecutiveErrors != n+1 || !h.LastSuccess.Equal(good.LastSuccess) || !strings.Contains(h.LastError, "forced heartbeat failure") || inf.dropped.Load() {
			t.Fatalf("heartbeat failure %d: %+v, dropped %v", n+1, h, inf.dropped.Load())
		}
	}
	failed := e.Health().Heartbeat
	e.inflight.removeIf(inf, false)
	e.heartbeatOnce(t.Context())
	if h := e.Health().Heartbeat; !h.LastTick.After(failed.LastTick) || !h.LastSuccess.Equal(failed.LastSuccess) || h.ConsecutiveErrors != failed.ConsecutiveErrors || h.LastError != failed.LastError {
		t.Fatalf("idle heartbeat changed result: before %+v, after %+v", failed, h)
	}
	execSql(t, pool, "DROP TRIGGER reject_heartbeat ON "+jr)
	if !e.inflight.add(e.loopCtx, inf) {
		t.Fatal("lease readmission rejected")
	}
	e.heartbeatOnce(t.Context())
	recovered := e.Health().Heartbeat
	if !recovered.LastSuccess.After(good.LastSuccess) || recovered.ConsecutiveErrors != 0 || recovered.LastError != "" {
		t.Fatalf("heartbeat recovery: %+v", recovered)
	}
	lastOK := e.lastOK
	e.st = store.Open(namedPool(t, schema), schema)
	tx, err := pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	execSql(t, tx, "SELECT id FROM "+jr+" WHERE id = "+itoa(id)+" FOR UPDATE")
	hctx, stop := context.WithCancel(t.Context())
	defer stop()
	done := make(chan struct{})
	go func() {
		defer close(done)
		e.heartbeatOnce(hctx)
	}()
	waitBlocked(t, pool, schema)
	stop()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("heartbeat ignored shutdown cancellation")
	}
	if h := e.Health().Heartbeat; !h.LastTick.After(recovered.LastTick) || !h.LastSuccess.Equal(recovered.LastSuccess) || h.ConsecutiveErrors != 0 || h.LastError != "" || !e.lastOK.Equal(lastOK) || inf.dropped.Load() {
		t.Fatalf("shutdown heartbeat changed result or lease deadline: %+v", h)
	}
}

func TestHealthScanFailureRecoverySkipAndDisable(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	rec := &metricsRec{}
	cfg := fastConfig(schema)
	cfg.Metrics = rec
	e, err := New(pool, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.stopLoops)
	t.Cleanup(e.stopHb)
	e.scanOnce()
	good := e.Health().Scan
	if good.LastSuccess.IsZero() || good.ConsecutiveErrors != 0 {
		t.Fatalf("empty scan: %+v", good)
	}
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
	if err := e.Schedules().Put(t.Context(), ScheduleSpec{Name: "s", Job: "j", Cron: yearly}); err != nil {
		t.Fatal(err)
	}
	sc, fn := qualified(schema, "schedule"), qualified(schema, "reject_scan")
	execSql(t, pool, "UPDATE "+sc+" SET next_run_at = now() - interval '1 minute' WHERE name = 's'")
	execSql(t, pool, "CREATE FUNCTION "+fn+`() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'forced scan failure'; END $$`)
	execSql(t, pool, "CREATE TRIGGER reject_scan BEFORE UPDATE ON "+sc+" FOR EACH ROW EXECUTE FUNCTION "+fn+"()")
	for n := range 2 {
		e.scanOnce()
		if h := e.Health().Scan; h.ConsecutiveErrors != n+1 || !h.LastSuccess.Equal(good.LastSuccess) || !strings.Contains(h.LastError, "forced scan failure") {
			t.Fatalf("scan failure %d: %+v", n+1, h)
		}
	}
	if n := len(scheduledRuns(t, e, "s")); n != 0 {
		t.Fatalf("failed scan created %d runs", n)
	}
	execSql(t, pool, "DROP TRIGGER reject_scan ON "+sc)
	e.scanOnce()
	recovered := e.Health().Scan
	if !recovered.LastSuccess.After(good.LastSuccess) || recovered.ConsecutiveErrors != 0 || recovered.LastError != "" {
		t.Fatalf("scan recovery: %+v", recovered)
	}
	execSql(t, pool, "UPDATE "+sc+" SET next_run_at = now() - interval '1 minute' WHERE name = 's'")
	e.scanOnce()
	skipped := e.Health().Scan
	if !skipped.LastSuccess.After(recovered.LastSuccess) || skipped.ConsecutiveErrors != 0 || rec.get("schedule_skipped_total", "reason", "overlap") != 1 {
		t.Fatalf("overlap skip should be healthy: %+v", skipped)
	}
	execSql(t, pool, "UPDATE "+sc+" SET cron = '0 0 31 2 *', next_run_at = now() - interval '1 minute' WHERE name = 's'")
	e.scanOnce()
	if h := e.Health().Scan; !h.LastSuccess.After(skipped.LastSuccess) || h.ConsecutiveErrors != 0 || h.LastError != "" || rec.get("schedule_skipped_total", "reason", "no_next") != 1 {
		t.Fatalf("disabling exhausted schedule should be healthy: %+v", h)
	}
}
