package skein

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"strings"
	"testing"
	"time"
	"uuid"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/mbeoliero/skein/internal/store"
	"github.com/mbeoliero/skein/migrations"
)

func TestMigrateRunExtraPreservesVersionOneData(t *testing.T) {
	t.Parallel()
	pool := testPool(t)
	schema := "skein_test_" + uuid.New().String()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); err != nil {
			t.Errorf("drop migration schema: %v", err)
		}
	})
	initial, err := fs.ReadFile(migrations.FS, "00001_init.sql")
	if err != nil {
		t.Fatal(err)
	}
	st := store.Open(pool, schema)
	if err := st.Migrate(t.Context(), []store.Migration{{Version: 1, Sql: string(initial)}}); err != nil {
		t.Fatal(err)
	}
	assertVersion := func(want int) {
		t.Helper()
		if got, err := st.SchemaVersion(t.Context()); err != nil || got != want {
			t.Fatalf("schema version = %d, %v; want %d", got, err, want)
		}
	}
	assertVersion(1)
	e, err := New(pool, submitOnly(schema))
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Start(t.Context()); err == nil || !strings.Contains(err.Error(), "schema version is 1") {
		t.Fatalf("Start on old schema = %v", err)
	}

	wf, runs := qualified(schema, "workflow_run"), qualified(schema, "job_run")
	execSql(t, pool, "INSERT INTO "+wf+` (workflow_name, input, dag, state, finished_at)
		VALUES ('active', '{"source":"old"}', '{"node":[],"blocked":["node"]}', 'running', NULL),
		       ('finished', '{"source":"old"}', '{}', 'failed', now())`)
	const oldOwner = "old \"holder\"\\node"
	_, err = pool.Exec(t.Context(), "INSERT INTO "+runs+`
		(job_name, executor_type, params, timeout, retry_policy, state, attempt, cancel_requested,
		 workflow_run_id, lease_token, lease_owner, lease_expires_at, started_at, finished_at, output, errors)
		SELECT name, 'x', '{"snapshot":1}', 60, '{"max_attempts":3}', state, attempt, cancelling, parent,
		       CASE WHEN state='running' THEN gen_random_uuid() END, owner,
		       CASE WHEN state='running' THEN now()+interval '1 minute' END,
		       CASE WHEN state NOT IN ('pending','blocked') THEN now()-interval '1 minute' END,
		       CASE WHEN state IN ('succeeded','failed','cancelled') THEN now() END,
		       CASE WHEN state='succeeded' THEN '{"result":1}'::jsonb END,
		       '[{"attempt":1,"kind":"test","message":"preserve"}]'::jsonb
		  FROM (VALUES ('plain', 'running', $1::text, NULL::bigint, 2, true),
		               ('node', 'running', 'node-owner', 1, 0, false),
		               ('blocked', 'blocked', NULL, 1, 0, false),
		               ('pending', 'pending', NULL, NULL, 0, false),
		               ('failed', 'failed', NULL, NULL, 2, false),
		               ('succeeded', 'succeeded', NULL, NULL, 1, false),
		               ('cancelled', 'cancelled', NULL, NULL, 1, true))
		       AS v(name, state, owner, parent, attempt, cancelling)`, oldOwner)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := func(table, omitted string) []byte {
		t.Helper()
		var data []byte
		if err := pool.QueryRow(t.Context(), "SELECT jsonb_agg(to_jsonb(r)-$1::text ORDER BY id) FROM "+table+" r", omitted).Scan(&data); err != nil {
			t.Fatal(err)
		}
		return data
	}
	beforeRuns, beforeWf := snapshot(runs, "lease_owner"), snapshot(wf, "extra")
	var id int64
	var token uuid.UUID
	if err := pool.QueryRow(t.Context(), "SELECT id, lease_token FROM "+runs+" WHERE job_name='plain'").Scan(&id, &token); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := Migrate(t.Context(), pool, schema); err != nil {
			t.Fatal(err)
		}
		assertVersion(schemaVersion)
		if got := snapshot(runs, "extra"); !bytes.Equal(got, beforeRuns) {
			t.Fatalf("migration changed job data:\nbefore %s\nafter  %s", beforeRuns, got)
		}
		if got := snapshot(wf, "extra"); !bytes.Equal(got, beforeWf) {
			t.Fatalf("migration changed workflow data:\nbefore %s\nafter  %s", beforeWf, got)
		}
		var correct bool
		if err := pool.QueryRow(t.Context(), "SELECT bool_and(extra = CASE job_name WHEN 'plain' THEN jsonb_build_object('lease_owner', $1::text) WHEN 'node' THEN '{\"lease_owner\":\"node-owner\"}'::jsonb ELSE '{}'::jsonb END) FROM "+runs, oldOwner).Scan(&correct); err != nil || !correct {
			t.Fatalf("owner migration = %v, %v", correct, err)
		}
		if err := pool.QueryRow(t.Context(), "SELECT bool_and(extra='{}'::jsonb) FROM "+wf).Scan(&correct); err != nil || !correct {
			t.Fatalf("workflow extra default = %v, %v", correct, err)
		}
	}
	started := startEngine(t, pool, submitOnly(schema), nil)
	if run, err := started.Runs().Get(t.Context(), id); err != nil || run.LeaseOwner != oldOwner || !run.CancelRequested {
		t.Fatalf("migrated public run = %+v, %v", run, err)
	}
	beats, err := st.Heartbeat(t.Context(), []int64{id}, []uuid.UUID{token}, time.Minute, time.Second)
	if err != nil || len(beats) != 1 || beats[0].Id != id || !beats[0].CancelRequested {
		t.Fatalf("heartbeat with pre-migration token = %+v, %v", beats, err)
	}
	settled, err := st.Settle(t.Context(), store.Settlement{Id: id, Token: token, Outcome: store.Cancelled})
	if err != nil || settled.State != string(StateCancelled) {
		t.Fatalf("settle with pre-migration token = %+v, %v", settled, err)
	}
	if run, err := started.Runs().Get(t.Context(), id); err != nil || run.LeaseOwner != "" || run.LeaseExpiresAt != nil || run.FinishedAt == nil {
		t.Fatalf("settled migrated run = %+v, %v", run, err)
	}
}

func TestRunExtraConstraints(t *testing.T) {
	t.Parallel()
	pool, schema := freshSchema(t)
	e := startEngine(t, pool, submitOnly(schema), nil)
	declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
	id := trigger(t, e, "j", "")
	type constraintCase struct {
		name           string
		state          string
		token, expires bool
		extra          *string
		code           string
	}
	tests := []constraintCase{
		{"running_owner", "running", true, true, new(`{"lease_owner":"worker"}`), ""},
		{"running_empty_owner", "running", true, true, new(`{"lease_owner":""}`), ""},
		{"missing_owner", "running", true, true, new(`{}`), "23514"},
		{"null_owner", "running", true, true, new(`{"lease_owner":null}`), "23514"},
		{"numeric_owner", "running", true, true, new(`{"lease_owner":1}`), "23514"},
		{"boolean_owner", "running", true, true, new(`{"lease_owner":true}`), "23514"},
		{"object_owner", "running", true, true, new(`{"lease_owner":{}}`), "23514"},
		{"array_owner", "running", true, true, new(`{"lease_owner":[]}`), "23514"},
		{"missing_expiry", "running", true, false, new(`{"lease_owner":"worker"}`), "23514"},
		{"missing_token", "running", false, true, new(`{"lease_owner":"worker"}`), "23514"},
		{"running_without_lease", "running", false, false, new(`{}`), "23514"},
		{"pending_with_owner", "pending", false, false, new(`{"lease_owner":"worker"}`), "23514"},
		{"pending_with_null_owner", "pending", false, false, new(`{"lease_owner":null}`), "23514"},
		{"pending_with_expiry", "pending", false, true, new(`{}`), "23514"},
		{"pending_with_token", "pending", true, false, new(`{}`), "23514"},
	}
	extraTests := []constraintCase{
		{"extra_array", "pending", false, false, new(`[]`), "23514"},
		{"extra_scalar", "pending", false, false, new(`"metadata"`), "23514"},
		{"extra_json_null", "pending", false, false, new(`null`), "23514"},
		{"extra_sql_null", "pending", false, false, nil, "23502"},
		{"pending_metadata", "pending", false, false, new(`{"future":{"env":"dev"}}`), ""},
	}
	for _, tc := range append(tests, extraTests...) {
		t.Run(tc.name, func(t *testing.T) {
			_, err := pool.Exec(t.Context(), "UPDATE "+qualified(schema, "job_run")+`
				SET state=$2, lease_token=CASE WHEN $3 THEN gen_random_uuid() END,
				    lease_expires_at=CASE WHEN $4 THEN now()+interval '1 minute' END, extra=$5::jsonb
				WHERE id=$1`, id, tc.state, tc.token, tc.expires, tc.extra)
			if tc.code == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if pgErr, ok := errors.AsType[*pgconn.PgError](err); !ok || pgErr.Code != tc.code {
				t.Fatalf("constraint error = %v; want SQLSTATE %s", err, tc.code)
			}
		})
	}
	for _, tc := range extraTests {
		t.Run("workflow_"+tc.name, func(t *testing.T) {
			_, err := pool.Exec(t.Context(), "INSERT INTO "+qualified(schema, "workflow_run")+
				" (workflow_name, state, extra) VALUES ('w', 'running', $1::jsonb)", tc.extra)
			if tc.code == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if pgErr, ok := errors.AsType[*pgconn.PgError](err); !ok || pgErr.Code != tc.code {
				t.Fatalf("workflow extra error = %v; want SQLSTATE %s", err, tc.code)
			}
		})
	}
}

func TestLeaseLifecyclePreservesExtra(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		outcome store.Outcome
		state   RunState
	}{
		{"succeeded", store.Succeeded, StateSucceeded},
		{"cancelled", store.Cancelled, StateCancelled},
		{"released", store.Released, StatePending},
		{"retry", store.Retry, StatePending},
		{"failed", store.Failed, StateFailed},
		{"interrupted", store.Interrupted, StateFailed},
		{"snoozed", store.Snoozed, StatePending},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool, schema := freshSchema(t)
			e := startEngine(t, pool, submitOnly(schema), nil)
			declare(t, e, JobSpec{Name: "j", ExecutorType: "x"})
			const extra = `{"traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01","tracestate":"vendor=value","future":{"env":"dev"}}`
			id, err := e.st.TriggerJob(t.Context(), nil, store.TriggerJobParams{JobName: "j", Params: []byte(`{}`), Extra: []byte(extra)})
			if err != nil {
				t.Fatal(err)
			}
			assertExtra := func(owner string, state RunState) {
				t.Helper()
				var preserved bool
				if err := pool.QueryRow(t.Context(), "SELECT extra-'lease_owner'=$2::jsonb FROM "+qualified(schema, "job_run")+" WHERE id=$1", id, extra).Scan(&preserved); err != nil || !preserved {
					t.Fatalf("metadata preserved = %v, %v", preserved, err)
				}
				r, err := e.Runs().Get(t.Context(), id)
				if err != nil || r.State != state || r.LeaseOwner != owner {
					t.Fatalf("run = %+v, %v; want %s owner %q", r, err, state, owner)
				}
			}
			first, err := e.st.ClaimPending(t.Context(), []string{"x"}, 1, "first", time.Minute)
			if err != nil || len(first) != 1 {
				t.Fatalf("claim = %+v, %v", first, err)
			}
			assertExtra("first", StateRunning)
			execSql(t, pool, "UPDATE "+qualified(schema, "job_run")+" SET lease_expires_at=now()-interval '1 second' WHERE id="+itoa(id))
			second, err := e.st.ClaimExpired(t.Context(), []string{"x"}, 1, "second", time.Minute)
			if err != nil || len(second) != 1 || second[0].LeaseToken == first[0].LeaseToken {
				t.Fatalf("reclaim = %+v, %v", second, err)
			}
			assertExtra("second", StateRunning)
			r, err := e.Runs().Get(t.Context(), id)
			if err != nil {
				t.Fatal(err)
			}
			if history := errorsOf(t, r); len(history) != 1 || !strings.Contains(history[0].Message, "first") {
				t.Fatalf("reclaim did not retain old owner: %+v", history)
			}
			_, err = e.st.Settle(t.Context(), store.Settlement{
				Id: id, Token: second[0].LeaseToken, Outcome: tc.outcome,
				Output: []byte(`{"done":true}`), Err: []byte(`[{"kind":"test"}]`),
				Backoff: time.Hour, Delay: time.Hour,
			})
			if err != nil {
				t.Fatal(err)
			}
			assertExtra("", tc.state)
			if tc.state == StateFailed || tc.state == StateCancelled {
				if err := e.Runs().Resume(t.Context(), id); err != nil {
					t.Fatal(err)
				}
				assertExtra("", StatePending)
			}
		})
	}
}
