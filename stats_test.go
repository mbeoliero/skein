package skein

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestStatsByExecutorCountsAndRegistry(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	e, err := New(pool, Config{Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	for _, typ := range []string{"alpha", "idle"} {
		e.Register(typ, func(context.Context, *Request) (RawJSON, error) { return nil, nil })
	}
	empty, err := e.Stats(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if empty.PendingDue != 0 || empty.Running != 0 || empty.UnregisteredDue != 0 || empty.OldestPendingAge != 0 ||
		empty.ByExecutor == nil || len(empty.ByExecutor) != 0 {
		t.Fatalf("empty database must return zero totals and an initialized empty map: %+v", empty)
	}
	base, err := e.st.Now(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var parent int64
	if err := pool.QueryRow(t.Context(), "INSERT INTO "+qualified(schema, "workflow_run")+
		" (workflow_name, state) VALUES ('fixture', 'running') RETURNING id").Scan(&parent); err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(t.Context(), "INSERT INTO "+qualified(schema, "job_run")+`
		(job_name, executor_type, params, timeout, retry_policy, state, workflow_run_id,
		 run_at, lease_token, lease_expires_at, extra, finished_at)
		SELECT typ, typ, '{}', 60, '{"max_attempts":3}', state,
		       CASE WHEN state='blocked' THEN $1::bigint END,
		       $2::timestamptz + delta::interval,
		       CASE WHEN state='running' THEN gen_random_uuid() END,
		       CASE WHEN state='running' THEN now()+interval '1 minute' END,
		       CASE WHEN state='running' THEN '{"lease_owner":"fixture"}'::jsonb ELSE '{}'::jsonb END,
		       CASE WHEN state IN ('succeeded','failed','cancelled') THEN now() END
		  FROM (VALUES ('alpha', 'pending', '-3 hours'), ('alpha', 'pending', '-1 hour'),
		               ('alpha', 'running', '-1 hour'), ('alpha', 'pending', '1 hour'),
		               ('alpha', 'blocked', '-8 hours'), ('alpha', 'succeeded', '-8 hours'),
		               ('beta', 'pending', '-2 hours'), ('beta', 'running', '-1 hour'),
		               ('beta', 'running', '-1 hour'), ('future', 'pending', '1 hour'),
		               ('running_only', 'running', '-1 hour'), ('blocked_only', 'blocked', '-8 hours'),
		               ('terminal_only', 'succeeded', '-8 hours'), ('terminal_only', 'failed', '-8 hours'),
		               ('terminal_only', 'cancelled', '-8 hours')) AS v(typ, state, delta)`, parent, base)
	if err != nil {
		t.Fatal(err)
	}
	before, err := e.st.Now(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	stats, err := e.Stats(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	after, err := e.st.Now(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]ExecutorStats{
		"alpha":        {PendingDue: 2, Running: 1, Registered: true},
		"beta":         {PendingDue: 1, Running: 2},
		"future":       {},
		"running_only": {Running: 1},
	}
	if len(stats.ByExecutor) != len(want) {
		t.Fatalf("types with only blocked/terminal rows or no rows appeared: %+v", stats.ByExecutor)
	}
	for typ, expected := range want {
		got, ok := stats.ByExecutor[typ]
		if !ok || got.PendingDue != expected.PendingDue || got.Running != expected.Running || got.Registered != expected.Registered {
			t.Errorf("%s = %+v, want counts/registration %+v", typ, got, expected)
		}
		if expected.PendingDue == 0 && got.OldestPendingAge != 0 {
			t.Errorf("%s has no due rows but age %s", typ, got.OldestPendingAge)
		}
	}
	for typ, age := range map[string]time.Duration{"alpha": 3 * time.Hour, "beta": 2 * time.Hour} {
		got := stats.ByExecutor[typ].OldestPendingAge
		due := base.Add(-age)
		if got < before.Sub(due) || got > after.Sub(due) {
			t.Errorf("%s oldest age %s outside database clock bounds [%s, %s]", typ, got, before.Sub(due), after.Sub(due))
		}
	}
	if stats.PendingDue != 3 || stats.Running != 4 || stats.UnregisteredDue != 1 ||
		stats.OldestPendingAge != stats.ByExecutor["alpha"].OldestPendingAge {
		t.Fatalf("totals disagree with the type snapshot: %+v", stats)
	}

	unregistered, err := New(pool, Config{Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	other, err := unregistered.Stats(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if other.PendingDue != stats.PendingDue || other.Running != stats.Running || other.UnregisteredDue != stats.PendingDue {
		t.Fatalf("empty registry changed queue counts: %+v", other)
	}
	for typ, counts := range other.ByExecutor {
		if counts.Registered {
			t.Errorf("empty registry reported %s as registered", typ)
		}
	}
}

func TestStatsByExecutorSnapshotsDoNotShareMaps(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	e, err := New(pool, Config{Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
	trigger(t, e, "j", "")
	first, err := e.Stats(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	second, err := e.Stats(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	delete(first.ByExecutor, "x")
	first.ByExecutor["invented"] = ExecutorStats{PendingDue: 99}
	if second.ByExecutor["x"].PendingDue != 1 || len(second.ByExecutor) != 1 {
		t.Fatalf("caller mutation changed another snapshot: %+v", second)
	}
	third, err := e.Stats(t.Context())
	if err != nil || third.ByExecutor["x"].PendingDue != 1 || len(third.ByExecutor) != 1 {
		t.Fatalf("caller mutation changed later observations: %+v, %v", third, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	failed, err := e.Stats(ctx)
	if !errors.Is(err, context.Canceled) || failed.ByExecutor != nil || failed.PendingDue != 0 || failed.Running != 0 {
		t.Fatalf("failed query returned a valid snapshot: %+v, %v", failed, err)
	}
}
