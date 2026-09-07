-- name: SetSearchPath :exec
-- Store.tx: transaction-local search_path (set_config(..., true) == SET LOCAL), nothing leaks to the host's pool;
-- the value is <schema>, pg_temp so a session's temp table cannot shadow a library table
SELECT set_config('search_path', @path::text, true);

-- name: CurrentSearchPath :one
-- TriggerTx restores the caller's search_path with this
SELECT current_setting('search_path')::text AS path;

-- name: SchemaVersionTableExists :one
-- Migrate bootstrap: before the first migration there is no schema_version table
SELECT (to_regclass(@qualified_name::text) IS NOT NULL)::boolean AS found;

-- name: MigrateLock :exec
-- §8: migrations run serially
SELECT pg_advisory_xact_lock(hashtext('skein:migrate'));
