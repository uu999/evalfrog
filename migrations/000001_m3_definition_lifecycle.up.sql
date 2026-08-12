CREATE TABLE principals (
    principal_id UUID PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('user', 'service_account')),
    display_name TEXT NOT NULL CHECK (char_length(display_name) BETWEEN 1 AND 200),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE principal_credentials (
    credential_id UUID PRIMARY KEY,
    principal_id UUID NOT NULL REFERENCES principals(principal_id),
    credential_hash BYTEA NOT NULL UNIQUE CHECK (octet_length(credential_hash) = 32),
    expires_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE projects (
    project_id UUID PRIMARY KEY,
    name TEXT NOT NULL CHECK (char_length(name) BETWEEN 1 AND 200),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE project_memberships (
    project_id UUID NOT NULL REFERENCES projects(project_id),
    principal_id UUID NOT NULL REFERENCES principals(principal_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (project_id, principal_id)
);

CREATE TABLE project_membership_permissions (
    project_id UUID NOT NULL,
    principal_id UUID NOT NULL,
    permission TEXT NOT NULL CHECK (permission IN (
        'workflow.read', 'workflow.write', 'workflow.test', 'workflow.publish',
        'run.create', 'run.read', 'run.cancel', 'connection.use',
        'service.use', 'project.admin'
    )),
    PRIMARY KEY (project_id, principal_id, permission),
    FOREIGN KEY (project_id, principal_id)
        REFERENCES project_memberships(project_id, principal_id)
);

CREATE TABLE project_execution_identities (
    execution_identity_id UUID PRIMARY KEY,
    project_id UUID NOT NULL UNIQUE REFERENCES projects(project_id),
    display_name TEXT NOT NULL CHECK (char_length(display_name) BETWEEN 1 AND 200),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (project_id, execution_identity_id)
);

CREATE TABLE secret_references (
    secret_reference_id UUID PRIMARY KEY,
    project_id UUID NOT NULL REFERENCES projects(project_id),
    provider TEXT NOT NULL CHECK (char_length(provider) BETWEEN 1 AND 100),
    external_reference TEXT NOT NULL CHECK (char_length(external_reference) BETWEEN 1 AND 1000),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (project_id, secret_reference_id),
    UNIQUE (project_id, provider, external_reference)
);

CREATE TABLE connections (
    connection_id UUID PRIMARY KEY,
    project_id UUID NOT NULL REFERENCES projects(project_id),
    reference TEXT NOT NULL CHECK (reference ~ '^[a-z][a-z0-9_]{0,127}$'),
    base_url TEXT NOT NULL CHECK (char_length(base_url) BETWEEN 1 AND 4096),
    secret_reference_id UUID,
    policy_json JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(policy_json) = 'object'),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (project_id, connection_id),
    UNIQUE (project_id, reference),
    FOREIGN KEY (project_id, secret_reference_id)
        REFERENCES secret_references(project_id, secret_reference_id)
);

CREATE TABLE connection_execution_grants (
    project_id UUID NOT NULL,
    connection_id UUID NOT NULL,
    execution_identity_id UUID NOT NULL,
    purpose TEXT NOT NULL CHECK (purpose IN ('test', 'production')),
    PRIMARY KEY (project_id, connection_id, execution_identity_id, purpose),
    FOREIGN KEY (project_id, connection_id)
        REFERENCES connections(project_id, connection_id),
    FOREIGN KEY (project_id, execution_identity_id)
        REFERENCES project_execution_identities(project_id, execution_identity_id)
);

CREATE TABLE rpc_services (
    service_id UUID PRIMARY KEY,
    project_id UUID NOT NULL REFERENCES projects(project_id),
    reference TEXT NOT NULL CHECK (reference ~ '^[a-z][a-z0-9_]{0,127}$'),
    protocol TEXT NOT NULL CHECK (char_length(protocol) BETWEEN 1 AND 50),
    discovery_reference TEXT NOT NULL CHECK (char_length(discovery_reference) BETWEEN 1 AND 1000),
    secret_reference_id UUID,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (project_id, service_id),
    UNIQUE (project_id, reference),
    FOREIGN KEY (project_id, secret_reference_id)
        REFERENCES secret_references(project_id, secret_reference_id)
);

CREATE TABLE rpc_service_operations (
    project_id UUID NOT NULL,
    service_id UUID NOT NULL,
    operation TEXT NOT NULL CHECK (char_length(operation) BETWEEN 1 AND 200),
    contract_revision TEXT NOT NULL CHECK (char_length(contract_revision) BETWEEN 1 AND 200),
    request_schema JSONB NOT NULL CHECK (jsonb_typeof(request_schema) = 'object'),
    response_schema JSONB NOT NULL CHECK (jsonb_typeof(response_schema) = 'object'),
    idempotent BOOLEAN NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (project_id, service_id, operation),
    FOREIGN KEY (project_id, service_id)
        REFERENCES rpc_services(project_id, service_id)
);

CREATE TABLE rpc_service_execution_grants (
    project_id UUID NOT NULL,
    service_id UUID NOT NULL,
    execution_identity_id UUID NOT NULL,
    purpose TEXT NOT NULL CHECK (purpose IN ('test', 'production')),
    PRIMARY KEY (project_id, service_id, execution_identity_id, purpose),
    FOREIGN KEY (project_id, service_id)
        REFERENCES rpc_services(project_id, service_id),
    FOREIGN KEY (project_id, execution_identity_id)
        REFERENCES project_execution_identities(project_id, execution_identity_id)
);

CREATE TABLE workflows (
    workflow_id UUID PRIMARY KEY,
    project_id UUID NOT NULL REFERENCES projects(project_id),
    name TEXT NOT NULL CHECK (char_length(name) BETWEEN 1 AND 200),
    active_version_id UUID,
    cloned_from_version_id UUID,
    created_by UUID NOT NULL REFERENCES principals(principal_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (project_id, workflow_id)
);

CREATE TABLE workflow_draft_revisions (
    draft_revision_id UUID PRIMARY KEY,
    project_id UUID NOT NULL,
    workflow_id UUID NOT NULL,
    revision_number BIGINT NOT NULL CHECK (revision_number > 0),
    ir_json JSONB NOT NULL CHECK (jsonb_typeof(ir_json) = 'object'),
    catalog_revision TEXT NOT NULL CHECK (char_length(catalog_revision) BETWEEN 1 AND 200),
    cloned_from_version_id UUID,
    created_by UUID NOT NULL REFERENCES principals(principal_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (project_id, workflow_id, revision_number),
    UNIQUE (project_id, workflow_id, draft_revision_id),
    UNIQUE (project_id, workflow_id, revision_number, draft_revision_id),
    FOREIGN KEY (project_id, workflow_id)
        REFERENCES workflows(project_id, workflow_id)
);

CREATE TABLE workflow_drafts (
    project_id UUID NOT NULL,
    workflow_id UUID NOT NULL,
    current_revision BIGINT NOT NULL CHECK (current_revision > 0),
    current_revision_id UUID NOT NULL,
    state_version BIGINT NOT NULL CHECK (state_version > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (project_id, workflow_id),
    FOREIGN KEY (project_id, workflow_id)
        REFERENCES workflows(project_id, workflow_id),
    FOREIGN KEY (project_id, workflow_id, current_revision, current_revision_id)
        REFERENCES workflow_draft_revisions(project_id, workflow_id, revision_number, draft_revision_id)
);

CREATE TABLE workflow_execution_snapshots (
    snapshot_id UUID PRIMARY KEY,
    project_id UUID NOT NULL,
    workflow_id UUID NOT NULL,
    origin_kind TEXT NOT NULL CHECK (origin_kind IN ('draft_test', 'published')),
    origin_id UUID NOT NULL,
    ir_json JSONB NOT NULL CHECK (jsonb_typeof(ir_json) = 'object'),
    dsl_json JSONB NOT NULL CHECK (jsonb_typeof(dsl_json) = 'object'),
    source_map_json JSONB NOT NULL CHECK (jsonb_typeof(source_map_json) = 'object'),
    compiler_manifest_json JSONB NOT NULL CHECK (jsonb_typeof(compiler_manifest_json) = 'object'),
    ir_hash TEXT NOT NULL CHECK (ir_hash ~ '^[0-9a-f]{64}$'),
    dsl_hash TEXT NOT NULL CHECK (dsl_hash ~ '^[0-9a-f]{64}$'),
    source_map_hash TEXT NOT NULL CHECK (source_map_hash ~ '^[0-9a-f]{64}$'),
    definition_hash TEXT NOT NULL CHECK (definition_hash ~ '^[0-9a-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (project_id, workflow_id, snapshot_id),
    UNIQUE (project_id, workflow_id, definition_hash),
    FOREIGN KEY (project_id, workflow_id)
        REFERENCES workflows(project_id, workflow_id)
);

CREATE TABLE workflow_versions (
    version_id UUID PRIMARY KEY,
    project_id UUID NOT NULL,
    workflow_id UUID NOT NULL,
    version_number BIGINT NOT NULL CHECK (version_number > 0),
    source_draft_revision_id UUID NOT NULL,
    execution_snapshot_id UUID NOT NULL,
    change_log TEXT NOT NULL CHECK (char_length(change_log) <= 4000),
    created_by UUID NOT NULL REFERENCES principals(principal_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (project_id, workflow_id, version_number),
    UNIQUE (project_id, workflow_id, version_id),
    UNIQUE (project_id, version_id),
    FOREIGN KEY (project_id, workflow_id)
        REFERENCES workflows(project_id, workflow_id),
    FOREIGN KEY (project_id, workflow_id, source_draft_revision_id)
        REFERENCES workflow_draft_revisions(project_id, workflow_id, draft_revision_id),
    FOREIGN KEY (project_id, workflow_id, execution_snapshot_id)
        REFERENCES workflow_execution_snapshots(project_id, workflow_id, snapshot_id)
);

ALTER TABLE workflows
    ADD CONSTRAINT workflows_active_version_fk
    FOREIGN KEY (project_id, workflow_id, active_version_id)
    REFERENCES workflow_versions(project_id, workflow_id, version_id)
    DEFERRABLE INITIALLY DEFERRED,
    ADD CONSTRAINT workflows_cloned_from_version_fk
    FOREIGN KEY (project_id, cloned_from_version_id)
    REFERENCES workflow_versions(project_id, version_id);

ALTER TABLE workflow_draft_revisions
    ADD CONSTRAINT draft_revisions_cloned_from_version_fk
    FOREIGN KEY (project_id, cloned_from_version_id)
    REFERENCES workflow_versions(project_id, version_id);

CREATE TABLE workflow_definition_audits (
    audit_id UUID PRIMARY KEY,
    project_id UUID NOT NULL,
    workflow_id UUID NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN ('publish', 'rollback')),
    version_id UUID NOT NULL,
    principal_id UUID NOT NULL REFERENCES principals(principal_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (project_id, workflow_id, version_id)
        REFERENCES workflow_versions(project_id, workflow_id, version_id)
);

CREATE TABLE definition_idempotency (
    project_id UUID NOT NULL REFERENCES projects(project_id),
    command_name TEXT NOT NULL CHECK (char_length(command_name) BETWEEN 1 AND 100),
    target_scope TEXT NOT NULL CHECK (char_length(target_scope) BETWEEN 1 AND 200),
    idempotency_key TEXT NOT NULL CHECK (char_length(idempotency_key) BETWEEN 8 AND 200),
    request_hash TEXT NOT NULL CHECK (request_hash ~ '^[0-9a-f]{64}$'),
    response_kind TEXT NOT NULL CHECK (char_length(response_kind) BETWEEN 1 AND 100),
    response_id UUID NOT NULL,
    response_aux_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (project_id, command_name, target_scope, idempotency_key)
);

CREATE INDEX workflows_project_updated_idx
    ON workflows(project_id, updated_at DESC, workflow_id);
CREATE INDEX workflow_versions_latest_idx
    ON workflow_versions(project_id, workflow_id, version_number DESC);
CREATE INDEX draft_revisions_history_idx
    ON workflow_draft_revisions(project_id, workflow_id, revision_number DESC);

CREATE FUNCTION reject_immutable_definition_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is immutable', TG_TABLE_NAME USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER workflow_draft_revisions_immutable
    BEFORE UPDATE OR DELETE ON workflow_draft_revisions
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_definition_mutation();
CREATE TRIGGER workflow_execution_snapshots_immutable
    BEFORE UPDATE ON workflow_execution_snapshots
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_definition_mutation();
CREATE TRIGGER workflow_versions_immutable
    BEFORE UPDATE OR DELETE ON workflow_versions
    FOR EACH ROW EXECUTE FUNCTION reject_immutable_definition_mutation();
