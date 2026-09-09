-- name: DeclareWorkflow :exec
-- §2.1 Workflows.Declare: upsert the workflow row; nodes are replaced by the caller in the same transaction
INSERT INTO workflow (name) VALUES (@name)
ON CONFLICT (name) DO UPDATE SET updated_at = now();

-- name: DeleteWorkflowNodes :exec
-- §2.1 Workflows.Declare: the node set is replaced, old rows first
DELETE FROM workflow_node WHERE workflow_name = @workflow_name;

-- name: InsertWorkflowNode :exec
-- §2.1 Workflows.Declare: one row per node after the in-memory validation
INSERT INTO workflow_node (workflow_name, job_name, deps) VALUES (@workflow_name, @job_name, @deps::text[]);

-- name: DeleteWorkflow :execrows
-- §3.3 Workflows.Delete: FK RESTRICT from schedule surfaces as 23503 → ErrReferenced; nodes cascade
DELETE FROM workflow WHERE name = @name;

-- name: ExistingJobs :many
-- §2.1 Workflows.Declare validation: every node's job must be declared
SELECT name FROM job WHERE name = ANY(@names::text[]);

-- name: WorkflowNodes :many
-- §2.1: the only read of definition tables in a workflow's life
SELECT n.job_name, n.deps, j.executor_type, j.params, j.timeout, j.retry_policy
  FROM workflow_node n JOIN job j ON j.name = n.job_name
 WHERE n.workflow_name = @workflow_name
 ORDER BY n.job_name;
