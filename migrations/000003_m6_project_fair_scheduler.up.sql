ALTER TABLE node_runs
    ADD COLUMN operation_type TEXT,
    ADD COLUMN operation_version INTEGER,
    ADD COLUMN resource_class TEXT;

UPDATE node_runs n
SET operation_type = definition.operation_type,
    operation_version = definition.operation_version,
    resource_class = CASE
        WHEN n.kind = 'control' THEN NULL
        WHEN definition.operation_type = 'task.python' THEN 'sandbox'
        WHEN definition.operation_type IN ('task.http', 'task.rpc') THEN 'builtin'
    END
FROM (
  SELECT nr.node_run_id,
         node->'operation'->>'type' AS operation_type,
         (node->'operation'->>'version')::integer AS operation_version
  FROM node_runs nr
  JOIN workflow_runs r ON r.project_id=nr.project_id AND r.run_id=nr.run_id
  JOIN workflow_execution_snapshots s
    ON s.project_id=r.project_id AND s.workflow_id=r.workflow_id AND s.snapshot_id=r.snapshot_id
  CROSS JOIN LATERAL jsonb_array_elements(s.dsl_json->'nodes') node
  WHERE node->>'id'=nr.execution_node_id
) definition
WHERE definition.node_run_id=n.node_run_id;

ALTER TABLE node_runs
    ALTER COLUMN operation_type SET NOT NULL,
    ALTER COLUMN operation_version SET NOT NULL,
    ADD CONSTRAINT node_runs_operation_identity_check
        CHECK (char_length(operation_type) BETWEEN 1 AND 200 AND operation_version > 0),
    ADD CONSTRAINT node_runs_resource_class_check
        CHECK (
            (kind = 'control' AND resource_class IS NULL)
            OR
            (kind = 'task' AND resource_class IN ('builtin', 'sandbox'))
        );

CREATE TABLE node_task_outbox (
    task_id UUID PRIMARY KEY,
    project_id UUID NOT NULL,
    run_id UUID NOT NULL,
    node_run_id UUID NOT NULL,
    execution_node_id TEXT NOT NULL CHECK (char_length(execution_node_id) BETWEEN 1 AND 200),
    attempt_id UUID NOT NULL,
    attempt_seq INTEGER NOT NULL CHECK (attempt_seq > 0),
    resource_class TEXT NOT NULL CHECK (resource_class IN ('builtin', 'sandbox')),
    message_version INTEGER NOT NULL CHECK (message_version = 1),
    occurred_at TIMESTAMPTZ NOT NULL,
    trace_id TEXT NOT NULL CHECK (char_length(trace_id) BETWEEN 1 AND 200),
    available_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    claim_owner TEXT,
    claim_token UUID,
    claim_expires_at TIMESTAMPTZ,
    publish_attempts INTEGER NOT NULL DEFAULT 0 CHECK (publish_attempts >= 0),
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (project_id, run_id, task_id),
    UNIQUE (project_id, run_id, attempt_id),
    FOREIGN KEY (project_id, run_id, node_run_id)
        REFERENCES node_runs(project_id, run_id, node_run_id),
    FOREIGN KEY (project_id, run_id, attempt_id)
        REFERENCES node_attempts(project_id, run_id, attempt_id),
    CHECK (
        (claim_token IS NULL AND claim_owner IS NULL AND claim_expires_at IS NULL)
        OR
        (claim_token IS NOT NULL AND claim_owner IS NOT NULL AND claim_expires_at IS NOT NULL)
    )
);

CREATE INDEX node_task_outbox_relay_idx
    ON node_task_outbox(resource_class, available_at, task_id)
    WHERE published_at IS NULL;

CREATE INDEX node_task_outbox_claim_idx
    ON node_task_outbox(claim_expires_at, task_id)
    WHERE published_at IS NULL AND claim_token IS NOT NULL;

CREATE INDEX node_attempts_project_inflight_idx
    ON node_attempts(project_id, state, attempt_id)
    WHERE state IN ('queued', 'running');
