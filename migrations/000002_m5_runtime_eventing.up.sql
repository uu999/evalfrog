ALTER TABLE workflow_versions
    ADD CONSTRAINT workflow_versions_snapshot_identity_unique
    UNIQUE (project_id, workflow_id, version_id, execution_snapshot_id);

CREATE TABLE workflow_draft_test_snapshots (
    project_id UUID NOT NULL,
    workflow_id UUID NOT NULL,
    draft_revision_id UUID NOT NULL,
    snapshot_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (project_id, workflow_id, draft_revision_id, snapshot_id),
    FOREIGN KEY (project_id, workflow_id, draft_revision_id)
        REFERENCES workflow_draft_revisions(project_id, workflow_id, draft_revision_id),
    FOREIGN KEY (project_id, workflow_id, snapshot_id)
        REFERENCES workflow_execution_snapshots(project_id, workflow_id, snapshot_id)
);

CREATE TABLE workflow_runs (
    run_id UUID PRIMARY KEY,
    project_id UUID NOT NULL REFERENCES projects(project_id),
    workflow_id UUID NOT NULL,
    snapshot_id UUID NOT NULL,
    published_version_id UUID,
    execution_identity_id UUID NOT NULL,
    purpose TEXT NOT NULL CHECK (purpose IN ('test', 'production')),
    definition_source TEXT NOT NULL CHECK (definition_source IN ('draft_snapshot', 'published_version')),
    definition_hash TEXT NOT NULL CHECK (definition_hash ~ '^[0-9a-f]{64}$'),
    state TEXT NOT NULL CHECK (state IN ('pending', 'running', 'succeeded', 'failed', 'canceled', 'timed_out')),
    state_version BIGINT NOT NULL CHECK (state_version > 0),
    input_json JSONB NOT NULL CHECK (jsonb_typeof(input_json) = 'object'),
    workflow_output_json JSONB CHECK (workflow_output_json IS NULL OR jsonb_typeof(workflow_output_json) = 'object'),
    execution_node_ids JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(execution_node_ids) = 'array'),
    termination_intent_json JSONB CHECK (termination_intent_json IS NULL OR jsonb_typeof(termination_intent_json) = 'object'),
    deadline_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (project_id, run_id),
    FOREIGN KEY (project_id, workflow_id)
        REFERENCES workflows(project_id, workflow_id),
    FOREIGN KEY (project_id, workflow_id, snapshot_id)
        REFERENCES workflow_execution_snapshots(project_id, workflow_id, snapshot_id),
    FOREIGN KEY (project_id, execution_identity_id)
        REFERENCES project_execution_identities(project_id, execution_identity_id),
    FOREIGN KEY (project_id, workflow_id, published_version_id, snapshot_id)
        REFERENCES workflow_versions(project_id, workflow_id, version_id, execution_snapshot_id),
    CHECK (deadline_at > created_at),
    CHECK (
        (purpose = 'test' AND definition_source = 'draft_snapshot' AND published_version_id IS NULL)
        OR
        (purpose = 'production' AND definition_source = 'published_version' AND published_version_id IS NOT NULL)
    ),
    CHECK ((state = 'pending' AND execution_node_ids = '[]'::jsonb) OR state <> 'pending')
);

CREATE TABLE node_runs (
    node_run_id UUID PRIMARY KEY,
    project_id UUID NOT NULL,
    run_id UUID NOT NULL,
    execution_node_id TEXT NOT NULL CHECK (char_length(execution_node_id) BETWEEN 1 AND 200),
    kind TEXT NOT NULL CHECK (kind IN ('control', 'task')),
    state TEXT NOT NULL CHECK (state IN (
        'pending', 'ready', 'queued', 'running', 'retry_wait',
        'succeeded', 'failed', 'timed_out', 'skipped', 'canceled'
    )),
    state_version BIGINT NOT NULL CHECK (state_version > 0),
    activated BOOLEAN NOT NULL DEFAULT FALSE,
    selected_route TEXT NOT NULL DEFAULT '',
    resolved_inputs_json JSONB CHECK (resolved_inputs_json IS NULL OR jsonb_typeof(resolved_inputs_json) = 'object'),
    current_attempt_id UUID,
    effective_attempt_id UUID,
    next_attempt_seq INTEGER NOT NULL DEFAULT 0 CHECK (next_attempt_seq >= 0),
    business_attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (business_attempt_count >= 0),
    recovery_count INTEGER NOT NULL DEFAULT 0 CHECK (recovery_count >= 0),
    next_attempt_kind TEXT NOT NULL DEFAULT 'initial' CHECK (next_attempt_kind IN ('initial', 'business_retry', 'recovery')),
    priority INTEGER NOT NULL DEFAULT 0,
    ready_at TIMESTAMPTZ,
    next_retry_at TIMESTAMPTZ,
    failure_json JSONB CHECK (failure_json IS NULL OR jsonb_typeof(failure_json) = 'object'),
    cancel_reason TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (project_id, run_id, execution_node_id),
    UNIQUE (project_id, run_id, node_run_id),
    FOREIGN KEY (project_id, run_id)
        REFERENCES workflow_runs(project_id, run_id),
    CHECK ((state = 'skipped' AND activated = FALSE) OR state <> 'skipped'),
    CHECK ((effective_attempt_id IS NULL) OR state = 'succeeded'),
    CHECK ((state = 'ready' AND ready_at IS NOT NULL) OR state <> 'ready'),
    CHECK ((state = 'retry_wait' AND next_retry_at IS NOT NULL) OR state <> 'retry_wait'),
    CHECK ((kind = 'control' AND current_attempt_id IS NULL AND effective_attempt_id IS NULL AND next_attempt_seq = 0) OR kind = 'task')
);

CREATE TABLE node_attempts (
    attempt_id UUID PRIMARY KEY,
    project_id UUID NOT NULL,
    run_id UUID NOT NULL,
    node_run_id UUID NOT NULL,
    attempt_seq INTEGER NOT NULL CHECK (attempt_seq > 0),
    attempt_kind TEXT NOT NULL CHECK (attempt_kind IN ('initial', 'business_retry', 'recovery')),
    state TEXT NOT NULL CHECK (state IN ('queued', 'running', 'succeeded', 'failed', 'timed_out', 'canceled', 'lost')),
    state_version BIGINT NOT NULL CHECK (state_version > 0),
    retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    recovery_count INTEGER NOT NULL DEFAULT 0 CHECK (recovery_count >= 0),
    lease_token UUID,
    lease_owner TEXT,
    lease_expires_at TIMESTAMPTZ,
    fencing_token BIGINT NOT NULL DEFAULT 0 CHECK (fencing_token >= 0),
    executor_build TEXT,
    error_json JSONB CHECK (error_json IS NULL OR jsonb_typeof(error_json) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (project_id, run_id, node_run_id, attempt_seq),
    UNIQUE (project_id, run_id, attempt_id),
    UNIQUE (attempt_id),
    FOREIGN KEY (project_id, run_id, node_run_id)
        REFERENCES node_runs(project_id, run_id, node_run_id),
    CHECK (
        (state = 'running' AND lease_token IS NOT NULL AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL AND fencing_token > 0)
        OR state <> 'running'
    )
);

ALTER TABLE node_runs
    ADD CONSTRAINT node_runs_current_attempt_fk
        FOREIGN KEY (project_id, run_id, current_attempt_id)
        REFERENCES node_attempts(project_id, run_id, attempt_id)
        DEFERRABLE INITIALLY DEFERRED,
    ADD CONSTRAINT node_runs_effective_attempt_fk
        FOREIGN KEY (project_id, run_id, effective_attempt_id)
        REFERENCES node_attempts(project_id, run_id, attempt_id)
        DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE node_output_values (
    project_id UUID NOT NULL,
    attempt_id UUID NOT NULL,
    run_id UUID NOT NULL,
    node_run_id UUID NOT NULL,
    value_json JSONB NOT NULL CHECK (jsonb_typeof(value_json) = 'object'),
    size_bytes INTEGER NOT NULL CHECK (size_bytes >= 2 AND size_bytes <= 1048576),
    content_hash TEXT NOT NULL CHECK (content_hash ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (project_id, attempt_id),
    FOREIGN KEY (project_id, run_id, attempt_id)
        REFERENCES node_attempts(project_id, run_id, attempt_id),
    FOREIGN KEY (project_id, run_id, node_run_id)
        REFERENCES node_runs(project_id, run_id, node_run_id)
);

CREATE TABLE outbox_events (
    event_id UUID PRIMARY KEY,
    project_id UUID NOT NULL,
    run_id UUID NOT NULL,
    aggregate_type TEXT NOT NULL CHECK (aggregate_type IN ('workflow_run', 'node_attempt')),
    aggregate_id UUID NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN (
        'run.created', 'run.cancel_requested', 'attempt.completed',
        'attempt.lost', 'retry.due', 'run.deadline_reached'
    )),
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
    UNIQUE (project_id, run_id, event_id),
    FOREIGN KEY (project_id, run_id)
        REFERENCES workflow_runs(project_id, run_id),
    CHECK (
        (claim_token IS NULL AND claim_owner IS NULL AND claim_expires_at IS NULL)
        OR
        (claim_token IS NOT NULL AND claim_owner IS NOT NULL AND claim_expires_at IS NOT NULL)
    )
);

CREATE TABLE inbox_events (
    project_id UUID NOT NULL,
    run_id UUID NOT NULL,
    consumer_name TEXT NOT NULL CHECK (char_length(consumer_name) BETWEEN 1 AND 200),
    event_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    processed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (consumer_name, event_id),
    FOREIGN KEY (project_id, run_id)
        REFERENCES workflow_runs(project_id, run_id)
);

CREATE TABLE runtime_idempotency (
    project_id UUID NOT NULL REFERENCES projects(project_id),
    command_name TEXT NOT NULL CHECK (command_name IN ('test_draft', 'create_run', 'create_historical_version_run')),
    target_scope TEXT NOT NULL CHECK (char_length(target_scope) BETWEEN 1 AND 200),
    idempotency_key TEXT NOT NULL CHECK (char_length(idempotency_key) BETWEEN 8 AND 200),
    request_hash TEXT NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    run_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (project_id, command_name, target_scope, idempotency_key),
    FOREIGN KEY (project_id, run_id)
        REFERENCES workflow_runs(project_id, run_id)
);

CREATE INDEX node_runs_ready_idx
    ON node_runs(project_id, priority DESC, ready_at, node_run_id)
    WHERE state = 'ready';
CREATE INDEX node_runs_retry_idx
    ON node_runs(next_retry_at, node_run_id)
    WHERE state = 'retry_wait';
CREATE INDEX node_attempts_lease_idx
    ON node_attempts(lease_expires_at, attempt_id)
    WHERE state = 'running';
CREATE INDEX workflow_runs_query_idx
    ON workflow_runs(project_id, state, updated_at DESC, run_id);
CREATE INDEX outbox_events_relay_idx
    ON outbox_events(available_at, event_id)
    WHERE published_at IS NULL;
CREATE INDEX outbox_events_claim_idx
    ON outbox_events(claim_expires_at, event_id)
    WHERE published_at IS NULL AND claim_token IS NOT NULL;
CREATE INDEX inbox_events_retention_idx
    ON inbox_events(processed_at, event_id);
