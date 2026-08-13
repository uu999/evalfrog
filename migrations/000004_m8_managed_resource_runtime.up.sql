ALTER TABLE connections
    ADD COLUMN resource_revision BIGINT NOT NULL DEFAULT 1 CHECK (resource_revision > 0);

ALTER TABLE rpc_services
    ADD COLUMN resource_revision BIGINT NOT NULL DEFAULT 1 CHECK (resource_revision > 0);

CREATE TABLE attempt_resource_revisions (
    project_id UUID NOT NULL,
    run_id UUID NOT NULL,
    attempt_id UUID NOT NULL,
    resource_kind TEXT NOT NULL CHECK (resource_kind IN ('connection', 'service_operation')),
    resource_id UUID NOT NULL,
    resource_revision BIGINT NOT NULL CHECK (resource_revision > 0),
    contract_revision TEXT,
    resolved_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (project_id, attempt_id, resource_kind, resource_id),
    FOREIGN KEY (project_id, run_id, attempt_id)
        REFERENCES node_attempts(project_id, run_id, attempt_id)
);

CREATE INDEX attempt_resource_revisions_run_idx
    ON attempt_resource_revisions(project_id, run_id, attempt_id);
