-- §1.2–§1.3: run metadata is separate from executor payloads and lease fencing.
ALTER TABLE job_run
    ADD COLUMN extra jsonb NOT NULL DEFAULT '{}',
    ADD CONSTRAINT job_run_extra_object CHECK (jsonb_typeof(extra) = 'object');

ALTER TABLE workflow_run
    ADD COLUMN extra jsonb NOT NULL DEFAULT '{}',
    ADD CONSTRAINT workflow_run_extra_object CHECK (jsonb_typeof(extra) = 'object');

UPDATE job_run
   SET extra = jsonb_set(extra, '{lease_owner}', to_jsonb(lease_owner))
 WHERE lease_owner IS NOT NULL;

-- Dropping owner also drops the old combined owner/expiry CHECK.
ALTER TABLE job_run DROP COLUMN lease_owner;

ALTER TABLE job_run
    ADD CONSTRAINT job_run_lease_expires_check
        CHECK ((lease_token IS NULL) = (lease_expires_at IS NULL)),
    ADD CONSTRAINT job_run_lease_owner_check
        CHECK (CASE WHEN lease_token IS NULL THEN NOT (extra ? 'lease_owner')
                    ELSE coalesce(jsonb_typeof(extra -> 'lease_owner') = 'string', false) END);
