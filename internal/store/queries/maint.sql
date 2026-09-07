-- §6.10 retention: one holder at a time (session advisory lock), each step its own transaction

-- name: TryMaintenanceLock :one
-- §6.10: session-level advisory lock, one holder per database; the loser reports skipped
SELECT pg_try_advisory_lock(hashtext('skein:maint'))::boolean AS locked;

-- name: MaintenanceUnlock :exec
-- §6.10: released on the same connection; a failed unlock closes that connection instead of pooling it
SELECT pg_advisory_unlock(hashtext('skein:maint'));

-- The outer WHERE repeats the subquery's conditions on purpose: the subquery picks ids from
-- the statement snapshot, and a row locked by a concurrent transaction is re-checked on its
-- new version only against the outer WHERE (EvalPlanQual). With id IN (...) alone, a
-- workflow_run that Resume just moved back to running would be deleted with all its nodes.

-- name: DeleteOldJobRuns :execrows
-- plain runs only: nodes go with their workflow_run (FK cascade), never on their own
DELETE FROM job_run r WHERE r.id IN (
    SELECT id FROM job_run
     WHERE state = ANY(@states::text[]) AND finished_at < now() - @age::interval AND workflow_run_id IS NULL
     LIMIT @lim::int)
   AND r.state = ANY(@states::text[]) AND r.finished_at < now() - @age::interval;

-- name: DeleteOldWorkflowRuns :execrows
-- §6.10 retention for workflow runs; nodes go with the parent (ON DELETE CASCADE); outer WHERE re-checked as above
DELETE FROM workflow_run w WHERE w.id IN (
    SELECT id FROM workflow_run
     WHERE state = ANY(@states::text[]) AND finished_at < now() - @age::interval
     LIMIT @lim::int)
   AND w.state = ANY(@states::text[]) AND w.finished_at < now() - @age::interval;

-- name: StaleWorkflowRuns :one
-- §6.10 stale-active check (stale_active_total): in flight longer than the failed-retention window
SELECT count(*)::int AS n FROM workflow_run WHERE state IN ('running', 'cancelling') AND created_at < now() - @age::interval;

-- name: StaleJobRuns :one
-- §6.10 stale-active check for plain runs; nodes are counted through their parent
SELECT count(*)::int AS n FROM job_run
 WHERE state IN ('pending', 'running') AND workflow_run_id IS NULL AND created_at < now() - @age::interval;

-- name: Stats :one
-- §8 Engine.Stats: one statement over the in-flight rows (both partial indexes)
SELECT count(*) FILTER (WHERE state = 'pending' AND run_at <= now())::int AS pending_due,
       count(*) FILTER (WHERE state = 'running')::int AS running,
       coalesce(extract(epoch FROM now() - min(run_at) FILTER (WHERE state = 'pending' AND run_at <= now())), 0)::float8 AS oldest_pending_sec,
       count(*) FILTER (WHERE state = 'pending' AND run_at <= now() AND NOT (executor_type = ANY(@registered::text[])))::int AS unregistered_due
  FROM job_run WHERE state IN ('pending', 'running');
