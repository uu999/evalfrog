-- M11 keeps recovery as a durable wake-up concern. PostgreSQL remains the
-- authority for Runtime state; this migration adds only recovery de-duplication,
-- trace correlation and audit facts.

ALTER TABLE workflow_runs
    ADD COLUMN trace_id TEXT;

UPDATE workflow_runs r
SET trace_id = COALESCE((
    SELECT e.trace_id
    FROM outbox_events e
    WHERE e.project_id = r.project_id
      AND e.run_id = r.run_id
      AND e.event_type = 'run.created'
    ORDER BY e.occurred_at, e.event_id
    LIMIT 1
), 'migration:' || r.run_id::text);

ALTER TABLE workflow_runs
    ALTER COLUMN trace_id SET NOT NULL,
    ADD CONSTRAINT workflow_runs_trace_id_check
        CHECK (char_length(trace_id) BETWEEN 1 AND 200);

-- One row per still-actionable wake-up identity. Repeated scans may emit a
-- new event only after the configured cooldown; the Engine Inbox and CAS make
-- every emitted event safe to deliver at least once.
CREATE TABLE runtime_recovery_wakeups (
    project_id UUID NOT NULL,
    run_id UUID NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN (
        'run.created', 'run.cancel_requested', 'attempt.completed',
        'attempt.lost', 'retry.due', 'run.deadline_reached'
    )),
    aggregate_id UUID NOT NULL,
    last_event_id UUID NOT NULL,
    last_emitted_at TIMESTAMPTZ NOT NULL,
    emit_count INTEGER NOT NULL DEFAULT 1 CHECK (emit_count > 0),
    PRIMARY KEY (project_id, run_id, event_type, aggregate_id),
    FOREIGN KEY (project_id, run_id)
        REFERENCES workflow_runs(project_id, run_id)
);

-- Audit records intentionally contain identifiers, event type and safe
-- operational metadata only: never Workflow input/output, Secret references,
-- lease tokens or executor stderr.
CREATE TABLE runtime_audit_events (
    audit_id UUID PRIMARY KEY,
    project_id UUID NOT NULL,
    run_id UUID NOT NULL,
    action TEXT NOT NULL CHECK (action IN (
        'run.cancel_requested', 'recovery.wakeup_emitted', 'recovery.manual_replay'
    )),
    actor_type TEXT NOT NULL CHECK (actor_type IN ('principal', 'system')),
    actor_id TEXT NOT NULL CHECK (char_length(actor_id) BETWEEN 1 AND 200),
    trace_id TEXT NOT NULL CHECK (char_length(trace_id) BETWEEN 1 AND 200),
    details_json JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(details_json) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (project_id, run_id)
        REFERENCES workflow_runs(project_id, run_id)
);

CREATE INDEX workflow_runs_deadline_idx
    ON workflow_runs(deadline_at, run_id)
    WHERE state IN ('pending', 'running');
CREATE INDEX runtime_audit_events_run_idx
    ON runtime_audit_events(project_id, run_id, created_at DESC, audit_id);
