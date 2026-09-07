-- name: DeclareJob :exec
-- §6.1 Jobs.Declare: upsert by name
INSERT INTO job (name, executor_type, params, timeout, retry_policy)
VALUES (@name, @executor_type, @params, @timeout, @retry_policy)
ON CONFLICT (name) DO UPDATE SET
    executor_type = EXCLUDED.executor_type,
    params        = EXCLUDED.params,
    timeout       = EXCLUDED.timeout,
    retry_policy  = EXCLUDED.retry_policy,
    updated_at    = now();

-- name: DeleteJob :execrows
-- §8 Jobs.Delete: FK RESTRICT from workflow_node / schedule surfaces as 23503 → ErrReferenced
DELETE FROM job WHERE name = @name;

-- name: JobExists :one
-- §6.1 Jobs.Trigger: zero rows from the INSERT is ErrNotFound when the job is missing, else a dedup conflict
SELECT EXISTS (SELECT 1 FROM job WHERE name = @name) AS found;
