-- name: TriggerWorkflow :one
-- §2.1: the parent row with the DAG snapshot; ErrNoRows = dedup conflict
INSERT INTO workflow_run (workflow_name, dedup_key, schedule_name, scheduled_at, input, dag, state, extra)
VALUES (@workflow_name, sqlc.narg(dedup_key)::text, sqlc.narg(schedule_name)::text, sqlc.narg(scheduled_at)::timestamptz,
        @input::jsonb, @dag::jsonb, 'running', coalesce(sqlc.narg(extra)::jsonb, '{}'::jsonb))
ON CONFLICT DO NOTHING
RETURNING id;

-- name: FindDedupWorkflowRun :one
-- §2.1: the workflow run holding a user dedup key, terminal or not (idx_workflow_run_dedup)
SELECT id FROM workflow_run
 WHERE workflow_name = @workflow_name AND dedup_key = @dedup_key::text AND schedule_name IS NULL;

-- name: FindInflightWorkflowBeat :one
-- §2.1: the in-flight overlap=skip beat holding 'sched:<name>' (idx_workflow_run_overlap)
SELECT id FROM workflow_run
 WHERE workflow_name = @workflow_name AND dedup_key = @dedup_key::text AND schedule_name IS NOT NULL AND state IN ('running', 'cancelling');

-- name: InsertNodeRuns :exec
-- §2.1: bulk-create the already-read node snapshots in the parent's transaction.
INSERT INTO job_run (job_name, workflow_run_id, executor_type, params, timeout, retry_policy, state)
SELECT v.job_name, @workflow_run_id::bigint, v.executor_type, v.params, v.timeout, v.retry_policy, v.state
  FROM (SELECT unnest(@job_names::text[]) AS job_name,
               unnest(@executor_types::text[]) AS executor_type,
               unnest(@params::jsonb[]) AS params,
               unnest(@timeouts::int[]) AS timeout,
               unnest(@retry_policies::jsonb[]) AS retry_policy,
               unnest(@states::text[]) AS state) v;

-- name: GetWorkflowRun :one
-- §2.5 Resume and §3.3: the parent row alone
SELECT * FROM workflow_run WHERE id = @id;

-- name: WorkflowResumeIdentity :one
-- §2.5 / §2.9: schedule_name is immutable; discover its name lock before locking the parent.
SELECT schedule_name FROM workflow_run WHERE id = @id;

-- name: WorkflowRunHeader :one
-- §2.2: the worker reads the parent without a lock
SELECT state, input, dag, extra, workflow_name FROM workflow_run WHERE id = @id;

-- name: LockWorkflowRun :one
-- §2.4 / §2.5: first link of the lock order workflow_run → job_run
SELECT state, dag, workflow_name, extra FROM workflow_run WHERE id = @id FOR UPDATE;

-- name: WorkflowRunWithNodes :many
-- §3.3 Workflows.GetRun: parent and nodes from one statement snapshot, so a concurrent Resume
-- cannot show a failed parent next to pending nodes; a run always has at least one node
SELECT sqlc.embed(w), sqlc.embed(r)
  FROM workflow_run w JOIN job_run r ON r.workflow_run_id = w.id
 WHERE w.id = @id ORDER BY r.job_name;

-- name: WorkflowRunWithNodesByDedup :many
-- §3.3 Workflows.FindRun: WorkflowRunWithNodes keyed by the user dedup key (idx_workflow_run_dedup)
SELECT sqlc.embed(w), sqlc.embed(r)
  FROM workflow_run w JOIN job_run r ON r.workflow_run_id = w.id
 WHERE w.workflow_name = @workflow_name AND w.dedup_key = @dedup_key::text AND w.schedule_name IS NULL
 ORDER BY r.job_name;

-- name: NodeStates :many
-- §2.4 / §2.5: propagate / finalize / resume view of the workflow, via idx_job_run_node
SELECT job_name, state FROM job_run WHERE workflow_run_id = @workflow_run_id::bigint;

-- name: NodeOutputs :many
-- §2.2 post-claim: the direct predecessors' outputs for Request.Deps
SELECT job_name, output FROM job_run
 WHERE workflow_run_id = @workflow_run_id::bigint AND job_name = ANY(@job_names::text[]);

-- name: MarkWorkflowCancelling :one
-- §2.4 / §2.5: fail-fast and Workflows.Cancel: running → cancelling; 0 rows = already cancelling or terminal
UPDATE workflow_run SET state = 'cancelling' WHERE id = @id AND state = 'running'
RETURNING id, workflow_name, state, extra;

-- name: CancelUnstartedNodes :many
-- §2.4 / §2.5 / §2.9: never wait for a claim or heartbeat on a newly running version;
-- skipped pending nodes observe the cancelling parent after this or a later claim
WITH picked AS (
    SELECT id FROM job_run
     WHERE workflow_run_id = @workflow_run_id::bigint AND state IN ('blocked', 'pending')
       FOR UPDATE SKIP LOCKED)
UPDATE job_run r SET state = 'cancelled', finished_at = now(), errors = r.errors || @err::jsonb
  FROM picked WHERE r.id = picked.id AND r.state IN ('blocked', 'pending')
RETURNING r.id, r.job_name, r.workflow_run_id, r.executor_type, r.state, r.lease_token, r.extra;

-- name: ActivateNodes :execrows
-- §2.4 propagate: blocked nodes whose predecessors all succeeded
UPDATE job_run SET state = 'pending', run_at = now()
 WHERE workflow_run_id = @workflow_run_id::bigint AND job_name = ANY(@job_names::text[]) AND state = 'blocked';

-- name: FinalizeWorkflowRun :one
-- §2.4 finalize: every node terminal, the run takes succeeded / failed / cancelled
UPDATE workflow_run SET state = @state, finished_at = now() WHERE id = @id
RETURNING id, workflow_name, state, extra;

-- name: ResumeNodes :many
-- §2.5: the reset set goes back to pending / blocked with attempt 0; errors are kept
UPDATE job_run r
   SET state = v.state, attempt = 0, run_at = now(),
       started_at = NULL, finished_at = NULL, output = NULL
  FROM (SELECT unnest(@job_names::text[]) AS job_name, unnest(@states::text[]) AS state) v
 WHERE r.workflow_run_id = @workflow_run_id::bigint AND r.job_name = v.job_name
RETURNING r.id, r.job_name, r.workflow_run_id, r.executor_type, r.state, r.lease_token, r.extra;

-- name: ReopenWorkflowRun :one
-- §2.5: back to running; the dedup index may reject it with 23505 → ErrDuplicate
UPDATE workflow_run SET state = 'running', finished_at = NULL WHERE id = @id
RETURNING id, workflow_name, state, extra;

-- name: ListWorkflowRuns :many
-- §3.3 Workflows.ListRuns: newest first, cursor = last id of the previous page, via idx_workflow_run_wf
SELECT * FROM workflow_run
 WHERE (sqlc.narg(workflow_name)::text IS NULL OR workflow_name = sqlc.narg(workflow_name)::text)
   AND (sqlc.narg(state)::text IS NULL OR state = sqlc.narg(state)::text)
   AND (@cursor::bigint = 0 OR id < @cursor::bigint)
 ORDER BY id DESC LIMIT @lim::int;
