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

-- name: DueSchedules :many
-- §2.1: due schedules, locked so two instances never fire the same one; db_now drives the next computation
SELECT name, job_name, workflow_name, cron, timezone, overlap, next_run_at, now()::timestamptz AS db_now
  FROM schedule
 WHERE enabled AND next_run_at <= now()
 ORDER BY next_run_at LIMIT @lim::int
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
