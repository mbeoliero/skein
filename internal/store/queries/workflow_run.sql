-- name: TriggerWorkflow :one
-- §6.1 step 2: the parent row with the DAG snapshot; ErrNoRows = dedup conflict
INSERT INTO workflow_run (workflow_name, dedup_key, schedule_name, scheduled_at, input, dag, state)
VALUES (@workflow_name, sqlc.narg(dedup_key)::text, sqlc.narg(schedule_name)::text, sqlc.narg(scheduled_at)::timestamptz,
        @input::jsonb, @dag::jsonb, 'running')
ON CONFLICT DO NOTHING
RETURNING id;

-- name: FindInflightWorkflowRun :one
-- §6.1: the workflow run holding the dedup key right now (returned with ErrDuplicate)
SELECT id FROM workflow_run
 WHERE workflow_name = @workflow_name AND dedup_key = @dedup_key::text AND state IN ('running', 'cancelling');

-- name: InsertNodeRun :exec
-- §6.1 step 3: one job_run per node, blocked while it has predecessors
INSERT INTO job_run (job_name, workflow_run_id, executor_type, params, timeout, retry_policy, state)
VALUES (@job_name, @workflow_run_id::bigint, @executor_type, @params, @timeout, @retry_policy, @state);

-- name: GetWorkflowRun :one
-- §6.7 Resume and §8: the parent row alone
SELECT * FROM workflow_run WHERE id = @id;

-- name: WorkflowRunHeader :one
-- §6.3 step 2 and 4: the worker reads the parent without a lock
SELECT state, input, dag FROM workflow_run WHERE id = @id;

-- name: LockWorkflowRun :one
-- §6.5 / §6.7: first link of the lock order workflow_run → job_run
SELECT state, dag FROM workflow_run WHERE id = @id FOR UPDATE;

-- name: WorkflowRunWithNodes :many
-- §8 Workflows.GetRun: parent and nodes from one statement snapshot, so a concurrent Resume
-- cannot show a failed parent next to pending nodes; a run always has at least one node
SELECT sqlc.embed(w), sqlc.embed(r)
  FROM workflow_run w JOIN job_run r ON r.workflow_run_id = w.id
 WHERE w.id = @id ORDER BY r.job_name;

-- name: NodeStates :many
-- §6.5 / §6.7: propagate / finalize / resume view of the workflow, via idx_job_run_node
SELECT job_name, state FROM job_run WHERE workflow_run_id = @workflow_run_id::bigint;

-- name: NodeOutputs :many
-- §6.3 post-claim step 4: the direct predecessors' outputs for Request.Deps
SELECT job_name, output FROM job_run
 WHERE workflow_run_id = @workflow_run_id::bigint AND job_name = ANY(@job_names::text[]);

-- name: MarkWorkflowCancelling :execrows
-- §6.5 / §6.6: fail-fast and Workflows.Cancel: running → cancelling; 0 rows = already cancelling or terminal
UPDATE workflow_run SET state = 'cancelling' WHERE id = @id AND state = 'running';

-- name: CancelUnstartedNodes :execrows
-- §6.5 / §6.6 / §7.3: never wait for a claim or heartbeat on a newly running version;
-- skipped pending nodes observe the cancelling parent after this or a later claim
WITH picked AS (
    SELECT id FROM job_run
     WHERE workflow_run_id = @workflow_run_id::bigint AND state IN ('blocked', 'pending')
       FOR UPDATE SKIP LOCKED)
UPDATE job_run r SET state = 'cancelled', finished_at = now(), errors = r.errors || @err::jsonb
  FROM picked WHERE r.id = picked.id AND r.state IN ('blocked', 'pending');

-- name: ActivateNodes :execrows
-- §6.5 propagate: blocked nodes whose predecessors all succeeded
UPDATE job_run SET state = 'pending', run_at = now()
 WHERE workflow_run_id = @workflow_run_id::bigint AND job_name = ANY(@job_names::text[]) AND state = 'blocked';

-- name: FinalizeWorkflowRun :exec
-- §6.5 finalize: every node terminal, the run takes succeeded / failed / cancelled
UPDATE workflow_run SET state = @state, finished_at = now() WHERE id = @id;

-- name: ResumeNodes :exec
-- §6.7: the reset set goes back to pending / blocked with attempt 0; errors are kept
UPDATE job_run r
   SET state = v.state, attempt = 0, run_at = now(),
       started_at = NULL, finished_at = NULL, output = NULL
  FROM (SELECT unnest(@job_names::text[]) AS job_name, unnest(@states::text[]) AS state) v
 WHERE r.workflow_run_id = @workflow_run_id::bigint AND r.job_name = v.job_name;

-- name: ReopenWorkflowRun :exec
-- §6.7: back to running; the dedup index may reject it with 23505 → ErrDuplicate
UPDATE workflow_run SET state = 'running', finished_at = NULL WHERE id = @id;

-- name: ListWorkflowRuns :many
-- §8 Workflows.ListRuns: newest first, cursor = last id of the previous page, via idx_workflow_run_wf
SELECT * FROM workflow_run
 WHERE (sqlc.narg(workflow_name)::text IS NULL OR workflow_name = sqlc.narg(workflow_name)::text)
   AND (sqlc.narg(state)::text IS NULL OR state = sqlc.narg(state)::text)
   AND (@cursor::bigint = 0 OR id < @cursor::bigint)
 ORDER BY id DESC LIMIT @lim::int;
