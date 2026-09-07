-- name: CurrentSchemaVersion :one
-- §6.9: Start refuses to run unless this equals skein.schemaVersion
SELECT coalesce(max(version), 0)::int AS version FROM schema_version;

-- name: RecordSchemaVersion :exec
-- §8 Migrate: one row per applied migration, under pg_advisory_xact_lock
INSERT INTO schema_version (version) VALUES (@version);
