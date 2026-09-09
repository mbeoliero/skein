-- name: TriggerJob :one
-- §2.1 Jobs.Trigger: snapshot the job into a pending run; ErrNoRows = job missing or dedup conflict
INSERT INTO job_run (job_name, dedup_key, schedule_name, scheduled_at, executor_type, params, timeout, retry_policy, state, run_at)
SELECT j.name, sqlc.narg(dedup_key)::text, sqlc.narg(schedule_name)::text, sqlc.narg(scheduled_at)::timestamptz,
       j.executor_type, j.params || @params::jsonb, j.timeout, j.retry_policy,
       'pending', coalesce(sqlc.narg(run_at)::timestamptz, now())
  FROM job j WHERE j.name = @job_name
ON CONFLICT DO NOTHING
RETURNING id;

-- name: FindDedupRun :one
-- §2.1: the run holding a user dedup key, terminal or not (idx_job_run_dedup)
SELECT id FROM job_run
 WHERE job_name = @job_name AND dedup_key = @dedup_key::text AND schedule_name IS NULL;

-- name: FindInflightBeat :one
-- §2.1: the in-flight overlap=skip beat holding 'sched:<name>' (idx_job_run_overlap)
SELECT id FROM job_run
 WHERE job_name = @job_name AND dedup_key = @dedup_key::text AND schedule_name IS NOT NULL AND state IN ('pending', 'running');

-- name: GetJobRun :one
-- §3.3 Runs.Get and the tests' state reads
SELECT * FROM job_run WHERE id = @id;

-- name: ClaimPending :many
-- §2.2 branch one: due pending rows via idx_job_run_claim
WITH picked AS (
    SELECT id FROM job_run
     WHERE state = 'pending' AND executor_type = ANY(@executor_types::text[]) AND run_at <= now()
     ORDER BY run_at LIMIT @lim::int
       FOR UPDATE SKIP LOCKED)
UPDATE job_run r
   SET state = 'running', lease_token = gen_random_uuid(), lease_owner = @owner::text,
       lease_expires_at = now() + @lease_ttl::interval, started_at = now()
  FROM picked WHERE r.id = picked.id
RETURNING r.id, r.job_name, r.workflow_run_id, r.executor_type, r.params, r.timeout,
          r.retry_policy, r.attempt, r.cancel_requested, r.lease_token::uuid AS lease_token,
          extract(epoch FROM now() - r.run_at)::float8 AS waited_sec;

-- name: NextPendingAt :many
-- §2.2 / §2.7: the earliest pending run_at per registered type, one idx_job_run_claim probe each; the caller
-- takes the minimum. Due rows are included on purpose: one that came due after the claim statement, or one
-- another instance's claim skipped and then rolled back, is found here and reclaimed after wakeFloor instead
-- of at the next poll. db_now is clock_timestamp(): now() would be the transaction start
SELECT x.run_at::timestamptz AS next_due, clock_timestamp()::timestamptz AS db_now
  FROM unnest(@executor_types::text[]) AS t(executor_type)
  CROSS JOIN LATERAL (
    SELECT run_at FROM job_run
     WHERE state = 'pending' AND executor_type = t.executor_type
     ORDER BY run_at LIMIT 1) x;

-- name: ClaimExpired :many
-- §2.2 branch two: running rows whose lease expired via idx_job_run_running; the previous holder counts as one interrupted attempt.
-- attempt is a smallint and max_attempts may be 32767: a holder that crashes at the cap would otherwise overflow the
-- next reclaim and fail the whole batch; capped, the post-claim check settles it failed (§2.2).
WITH picked AS (
    SELECT id FROM job_run
     WHERE state = 'running' AND executor_type = ANY(@executor_types::text[]) AND lease_expires_at <= now()
     LIMIT @lim::int
       FOR UPDATE SKIP LOCKED)
UPDATE job_run r
   SET attempt = LEAST(r.attempt + 1, 32767),
       errors = r.errors || jsonb_build_array(jsonb_build_object(
                  'attempt', LEAST(r.attempt + 1, 32767), 'at', now(), 'kind', 'interrupted',
                  'message', 'lease expired; last owner ' || coalesce(r.lease_owner, '?'))),
       lease_token = gen_random_uuid(), lease_owner = @owner::text,
       lease_expires_at = now() + @lease_ttl::interval, started_at = now()
  FROM picked WHERE r.id = picked.id
RETURNING r.id, r.job_name, r.workflow_run_id, r.executor_type, r.params, r.timeout,
          r.retry_policy, r.attempt, r.cancel_requested, r.lease_token::uuid AS lease_token,
          extract(epoch FROM now() - r.run_at)::float8 AS waited_sec;

-- name: Heartbeat :many
-- §2.3: one statement renews every lease this process holds; only lease_expires_at changes (HOT);
-- renewal is capped at started_at + timeout + CancelTimeout; ids missing from the result lost their lease
UPDATE job_run r
   SET lease_expires_at = LEAST(now() + @lease_ttl::interval,
                                r.started_at + r.timeout * interval '1 second' + @cancel_timeout::interval)
  FROM (SELECT unnest(@ids::bigint[]) AS id, unnest(@tokens::uuid[]) AS token) v
 WHERE r.id = v.id AND r.lease_token = v.token AND r.state = 'running'
RETURNING r.id, r.cancel_requested,
          coalesce((SELECT w.state = 'cancelling' FROM workflow_run w WHERE w.id = r.workflow_run_id), false)::boolean AS wf_cancelling;

-- §2.4 settle: one fenced UPDATE per outcome; WHERE lease_token = @token::uuid AND state = 'running' is the fence,
-- ErrNoRows = ErrLeaseLost. Non-terminal outcomes apply the cancel-hit CASE:
-- cancel_requested OR @wf_cancelling → cancelled instead of pending / failed.

-- name: SettleSucceeded :one
-- §2.4 succeeded exit; the fence is lease_token AND state = 'running', zero rows = ErrLeaseLost
UPDATE job_run
   SET state = 'succeeded', output = @output, finished_at = now(),
       lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL
 WHERE id = @id AND lease_token = @token::uuid AND state = 'running'
RETURNING state;

-- name: SettleCancelled :one
-- §2.4 cancelled exit, same fence
UPDATE job_run
   SET state = 'cancelled', finished_at = now(),
       errors = errors || COALESCE(sqlc.narg(err)::jsonb, '[]'::jsonb),
       lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL
 WHERE id = @id AND lease_token = @token::uuid AND state = 'running'
RETURNING state;

-- name: SettleReleased :one
-- §2.4 / §2.6 graceful shutdown: back to pending now, attempt unchanged, a released entry for the alert
UPDATE job_run
   SET state       = CASE WHEN cancel_requested OR @wf_cancelling::boolean THEN 'cancelled' ELSE 'pending' END,
       finished_at = CASE WHEN cancel_requested OR @wf_cancelling::boolean THEN now() END,
       run_at = now(), errors = errors || @err::jsonb,
       lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL
 WHERE id = @id AND lease_token = @token::uuid AND state = 'running'
RETURNING state,
          (SELECT count(*) FROM jsonb_array_elements(errors) x WHERE x->>'kind' = 'released')::int AS released_count;

-- name: SettleRetry :one
-- §2.4 retryable failure with attempts left: pending again after backoff
UPDATE job_run
   SET state       = CASE WHEN cancel_requested OR @wf_cancelling::boolean THEN 'cancelled' ELSE 'pending' END,
       finished_at = CASE WHEN cancel_requested OR @wf_cancelling::boolean THEN now() END,
       attempt = attempt + 1, run_at = now() + @backoff::interval, errors = errors || @err::jsonb,
       lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL
 WHERE id = @id AND lease_token = @token::uuid AND state = 'running'
RETURNING state;

-- name: SettleSnoozed :one
-- §2.4 / §3.2: normal waiting, no attempt/error/output change; cancellation still wins.
-- Native interval input also handles PG 13; add the rounding remainder without Duration overflow.
-- sqlc.arg avoids sqlc 1.31's @ rewrite bug around this interval CASE.
UPDATE job_run
   SET state       = CASE WHEN cancel_requested OR @wf_cancelling::boolean THEN 'cancelled' ELSE 'pending' END,
       finished_at = CASE WHEN cancel_requested OR @wf_cancelling::boolean THEN now() END,
       run_at = now() + sqlc.arg(delay)::interval
              + CASE WHEN sqlc.arg(round_up)::boolean THEN interval '1 microsecond' ELSE interval '0' END,
       lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL
 WHERE id = @id AND lease_token = @token::uuid AND state = 'running'
RETURNING state;

-- name: SettleFailed :one
-- §2.4 permanent failure or no attempts left
UPDATE job_run
   SET state = CASE WHEN cancel_requested OR @wf_cancelling::boolean THEN 'cancelled' ELSE 'failed' END,
       attempt = attempt + 1, errors = errors || @err::jsonb, finished_at = now(),
       lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL
 WHERE id = @id AND lease_token = @token::uuid AND state = 'running'
RETURNING state;

-- name: SettleInterrupted :one
-- §2.2: reclaimed with attempt >= max_attempts; the interrupted entry was appended by ClaimExpired
UPDATE job_run
   SET state = CASE WHEN cancel_requested OR @wf_cancelling::boolean THEN 'cancelled' ELSE 'failed' END,
       finished_at = now(),
       lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL
 WHERE id = @id AND lease_token = @token::uuid AND state = 'running'
RETURNING state;

-- name: CancelRun :one
-- §2.5 Runs.Cancel: plain runs only; a pending row ends now, a running row is flagged and its
-- holder settles it as cancelled after the next heartbeat. One statement for both states: a row
-- that a concurrent claim or settle moves between them is re-checked on its new version, so
-- zero rows keeps its §2.9 meaning (terminal, a node, or missing) and never means "moved".
UPDATE job_run
   SET state            = CASE WHEN state = 'pending' THEN 'cancelled' ELSE state END,
       finished_at      = CASE WHEN state = 'pending' THEN now() ELSE finished_at END,
       cancel_requested = CASE WHEN state = 'running' THEN true ELSE cancel_requested END
 WHERE id = @id AND state IN ('pending', 'running') AND workflow_run_id IS NULL
RETURNING state;

-- name: ResumeRun :execrows
-- §2.5: ordinary terminal runs only; recheck state after a concurrent resume.
UPDATE job_run
   SET state = 'pending', attempt = 0, run_at = now(),
       lease_token = NULL, lease_owner = NULL, lease_expires_at = NULL,
       started_at = NULL, finished_at = NULL, output = NULL, cancel_requested = false
 WHERE id = @id AND workflow_run_id IS NULL AND state IN ('failed', 'cancelled');

-- name: RunIsNode :one
-- §2.5 Runs.Cancel: a node is cancelled through its parent (Workflows.CancelRun), never directly
SELECT (workflow_run_id IS NOT NULL)::boolean AS is_node FROM job_run WHERE id = @id;

-- name: ListJobRuns :many
-- §3.3 Runs.List: newest first, cursor = last id of the previous page, via idx_job_run_job
SELECT * FROM job_run
 WHERE (sqlc.narg(job_name)::text IS NULL OR job_name = sqlc.narg(job_name)::text)
   AND (sqlc.narg(state)::text IS NULL OR state = sqlc.narg(state)::text)
   AND (@cursor::bigint = 0 OR id < @cursor::bigint)
 ORDER BY id DESC LIMIT @lim::int;
