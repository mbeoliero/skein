-- name: PutSchedule :exec
-- §2.1 Schedules.Put: upsert; next_run_at is recomputed only when the rule changed or the
-- schedule went from disabled to enabled, so a rolling deploy does not keep postponing it
INSERT INTO schedule (name, job_name, workflow_name, cron, timezone, overlap, enabled, next_run_at)
VALUES (@name, sqlc.narg(job_name)::text, sqlc.narg(workflow_name)::text, @cron, @timezone, @overlap, @enabled, @next_run_at)
ON CONFLICT (name) DO UPDATE SET
    job_name = EXCLUDED.job_name, workflow_name = EXCLUDED.workflow_name,
    cron = EXCLUDED.cron, timezone = EXCLUDED.timezone, overlap = EXCLUDED.overlap,
    enabled = EXCLUDED.enabled, updated_at = now(),
    next_run_at = CASE WHEN schedule.cron IS DISTINCT FROM EXCLUDED.cron
                         OR schedule.timezone IS DISTINCT FROM EXCLUDED.timezone
                         OR (NOT schedule.enabled AND EXCLUDED.enabled)
                       THEN EXCLUDED.next_run_at ELSE schedule.next_run_at END;

-- name: DeleteSchedule :execrows
-- §3.3 Schedules.Delete; zero rows = ErrNotFound; the schedule trigger notifies the scanners (§2.7)
DELETE FROM schedule WHERE name = @name;

-- name: GetSchedule :one
-- §3.3: read back one rule (tests and diagnostics)
SELECT * FROM schedule WHERE name = @name;

-- name: LockScheduleName :exec
-- §1.3 / §2.1 / §2.5 / §2.9: coordinate a schema/name even while its schedule row is absent.
-- The two-integer lock space is separate from the one-bigint maintenance and migration locks.
SELECT pg_advisory_xact_lock(hashtext('skein:schedule'), hashtext(jsonb_build_array(current_schema(), @name::text)::text));

-- name: TryLockScheduleName :one
-- §2.1 / §2.9: a scanner never waits while acquiring another name in its batch.
SELECT pg_try_advisory_xact_lock(hashtext('skein:schedule'), hashtext(jsonb_build_array(current_schema(), @name::text)::text))::boolean AS locked;

-- name: LockScheduleRule :one
-- §2.5 / §2.9: scheduled Resume holds the name lock before reading the current rule; no row means deleted.
SELECT overlap FROM schedule WHERE name = @name FOR UPDATE;

-- name: InflightScheduleRunExists :one
-- §2.1 / §2.5: current skip rules cover historical allow beats and target changes across both run tables.
-- The name lock serializes all openings, so these candidates must remain unlocked; settlement may only remove them.
SELECT (EXISTS (
    SELECT 1 FROM job_run
     WHERE schedule_name = @name::text AND state IN ('pending', 'running') AND id <> @exclude_run_id::bigint
) OR EXISTS (
    SELECT 1 FROM workflow_run
     WHERE schedule_name = @name::text AND state IN ('running', 'cancelling') AND id <> @exclude_workflow_run_id::bigint
))::boolean AS found;

-- name: DueScheduleCandidates :many
-- §2.1: enumerate without locks, continuing past names held by other transactions until a real batch is filled.
SELECT name, next_run_at FROM schedule
 WHERE enabled AND next_run_at <= now()
   AND (sqlc.narg(after_due)::timestamptz IS NULL
        OR (next_run_at, name) > (sqlc.narg(after_due)::timestamptz, @after_name::text))
 ORDER BY next_run_at, name LIMIT @lim::int;

-- name: LockDueSchedule :one
-- §2.1 / §2.9: only after the name lock; recheck a candidate's current rule and due time without waiting on a row.
SELECT name, job_name, workflow_name, cron, timezone, overlap, next_run_at, now()::timestamptz AS db_now
  FROM schedule
 WHERE name = @name AND enabled AND next_run_at <= now()
   FOR UPDATE SKIP LOCKED;

-- name: NextScheduleAt :one
-- §2.1 / §2.7: the nearest fire time after this tick's advances, read in the same transaction; idx_schedule_due first entry.
-- ErrNoRows = no enabled schedule. db_now is clock_timestamp(): now() is the transaction start, and the tick's own
-- work (up to 50 fires) would otherwise be waited for a second time
SELECT next_run_at AS next_due, clock_timestamp()::timestamptz AS db_now FROM schedule WHERE enabled ORDER BY next_run_at LIMIT 1;

-- name: AdvanceSchedule :exec
-- §2.1: move the rule to its next fire time; does not touch updated_at and does not notify (§2.7)
UPDATE schedule SET next_run_at = @next_run_at WHERE name = @name;

-- name: DisableSchedule :exec
-- §2.1: the rule has no fire time within robfig's five-year horizon; a zero next_run_at would be due every tick
UPDATE schedule SET enabled = false, updated_at = now() WHERE name = @name;

-- name: DbNow :one
-- §2.1 Schedules.Put computes next_run_at from the database clock
SELECT now()::timestamptz AS now;
