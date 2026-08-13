package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/eventing"
	"github.com/uu999/evalfrog/internal/runtime"
	"github.com/uu999/evalfrog/internal/runtime/engine"
	"github.com/uu999/evalfrog/internal/scheduling"
)

func (store *Store) CreatePendingRun(ctx context.Context, record runtime.CreatePendingRunRecord) (runtime.WorkflowRunRecord, error) {
	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return runtime.WorkflowRunRecord{}, err
	}
	defer tx.Rollback(ctx)
	scope := record.WorkflowID
	lockKey := record.ProjectID + ":" + commandName(record.Purpose) + ":" + scope + ":" + record.IdempotencyKey
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, lockKey); err != nil {
		return runtime.WorkflowRunRecord{}, err
	}
	var existingRun, existingHash string
	err = tx.QueryRow(ctx, `
		SELECT run_id::text, request_hash FROM runtime_idempotency
		WHERE project_id=$1 AND command_name=$2 AND target_scope=$3 AND idempotency_key=$4`,
		record.ProjectID, commandName(record.Purpose), scope, record.IdempotencyKey).Scan(&existingRun, &existingHash)
	if err == nil {
		if existingHash != record.RequestHash {
			return runtime.WorkflowRunRecord{}, runtime.ErrRunIdempotencyReuse
		}
		result, loadErr := loadRun(ctx, tx, record.ProjectID, existingRun, false)
		if loadErr != nil {
			return runtime.WorkflowRunRecord{}, loadErr
		}
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return runtime.WorkflowRunRecord{}, commitErr
		}
		return result, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return runtime.WorkflowRunRecord{}, err
	}

	var snapshotID, definitionHash, definitionSource, versionID string
	if record.Purpose == runtime.RunPurposeTest {
		err = tx.QueryRow(ctx, `
			SELECT s.snapshot_id::text, s.definition_hash
			FROM workflow_draft_revisions r
			JOIN workflow_draft_test_snapshots b
			  ON b.project_id=r.project_id AND b.workflow_id=r.workflow_id
			 AND b.draft_revision_id=r.draft_revision_id
			JOIN workflow_execution_snapshots s
			  ON s.project_id=b.project_id AND s.workflow_id=b.workflow_id AND s.snapshot_id=b.snapshot_id
			WHERE s.project_id=$1 AND s.workflow_id=$2 AND s.snapshot_id=$3
			  AND r.revision_number=$4`, record.ProjectID, record.WorkflowID,
			record.SnapshotID, record.DraftRevisionNumber).Scan(&snapshotID, &definitionHash)
		definitionSource = string(runtime.DefinitionDraftSnapshot)
	} else {
		err = tx.QueryRow(ctx, `
			SELECT s.snapshot_id::text, s.definition_hash, v.version_id::text
			FROM workflows w
			JOIN workflow_versions v
			  ON v.project_id=w.project_id AND v.workflow_id=w.workflow_id AND v.version_id=w.active_version_id
			JOIN workflow_execution_snapshots s
			  ON s.project_id=v.project_id AND s.workflow_id=v.workflow_id AND s.snapshot_id=v.execution_snapshot_id
			WHERE w.project_id=$1 AND w.workflow_id=$2
			FOR SHARE OF w`, record.ProjectID, record.WorkflowID).Scan(&snapshotID, &definitionHash, &versionID)
		definitionSource = string(runtime.DefinitionPublishedVersion)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return runtime.WorkflowRunRecord{}, runtime.ErrRunSourceInvalid
	}
	if err != nil {
		return runtime.WorkflowRunRecord{}, err
	}
	var executionIdentityID string
	if err = tx.QueryRow(ctx, `
		SELECT execution_identity_id::text FROM project_execution_identities
		WHERE project_id=$1 AND enabled=true`, record.ProjectID).Scan(&executionIdentityID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return runtime.WorkflowRunRecord{}, runtime.ErrRunSourceInvalid
		}
		return runtime.WorkflowRunRecord{}, err
	}
	var nullableVersion any
	if versionID != "" {
		nullableVersion = versionID
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO workflow_runs (
			run_id, project_id, workflow_id, snapshot_id, published_version_id,
			execution_identity_id, purpose, definition_source, definition_hash,
			state, state_version, input_json, deadline_at, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'pending',1,$10,$11,$12,$12)`,
		record.RunID, record.ProjectID, record.WorkflowID, snapshotID, nullableVersion,
		executionIdentityID, record.Purpose, definitionSource, definitionHash,
		record.WorkflowInput, record.DeadlineAt, record.CreatedAt)
	if err != nil {
		return runtime.WorkflowRunRecord{}, err
	}
	event := eventing.RuntimeEvent{
		MessageVersion: eventing.RuntimeMessageVersion, EventID: record.EventID,
		ProjectID: record.ProjectID, RunID: record.RunID, AggregateType: eventing.WorkflowRunAggregate,
		AggregateID: record.RunID, EventType: eventing.RunCreated, OccurredAt: record.CreatedAt, TraceID: record.TraceID,
	}
	if err = insertOutbox(ctx, tx, event); err != nil {
		return runtime.WorkflowRunRecord{}, err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO runtime_idempotency (
			project_id, command_name, target_scope, idempotency_key, request_hash, run_id, created_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7)`, record.ProjectID, commandName(record.Purpose), scope,
		record.IdempotencyKey, record.RequestHash, record.RunID, record.CreatedAt)
	if err != nil {
		return runtime.WorkflowRunRecord{}, err
	}
	result, err := loadRun(ctx, tx, record.ProjectID, record.RunID, false)
	if err != nil {
		return runtime.WorkflowRunRecord{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return runtime.WorkflowRunRecord{}, err
	}
	return result, nil
}

func commandName(purpose runtime.RunPurpose) string {
	if purpose == runtime.RunPurposeTest {
		return "test_draft"
	}
	return "create_run"
}

type runtimeTransaction struct {
	tx       pgx.Tx
	router   scheduling.Router
	snapshot *engine.Snapshot
}

func (store *Store) WithRunTransaction(ctx context.Context, event eventing.RuntimeEvent, operation func(engine.RunTransaction) error) error {
	tx, err := store.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	adapter := &runtimeTransaction{tx: tx, router: store.router}
	if err = operation(adapter); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (transaction *runtimeTransaction) AcceptInbox(ctx context.Context, consumer string, event eventing.RuntimeEvent) (bool, error) {
	tag, err := transaction.tx.Exec(ctx, `
		INSERT INTO inbox_events (project_id, run_id, consumer_name, event_id, event_type, received_at, processed_at)
		VALUES ($1,$2,$3,$4,$5,clock_timestamp(),clock_timestamp())
		ON CONFLICT (consumer_name, event_id) DO NOTHING`,
		event.ProjectID, event.RunID, consumer, event.EventID, event.EventType)
	return tag.RowsAffected() == 1, err
}

func (transaction *runtimeTransaction) LoadRun(ctx context.Context, projectID, runID string) (runtime.WorkflowRunRecord, error) {
	return loadRun(ctx, transaction.tx, projectID, runID, true)
}

func (transaction *runtimeTransaction) LoadSnapshot(ctx context.Context, projectID, snapshotID string) (engine.Snapshot, error) {
	var result engine.Snapshot
	var raw []byte
	err := transaction.tx.QueryRow(ctx, `
		SELECT snapshot_id::text, definition_hash, dsl_json
		FROM workflow_execution_snapshots WHERE project_id=$1 AND snapshot_id=$2`, projectID, snapshotID).
		Scan(&result.ID, &result.DefinitionHash, &raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return engine.Snapshot{}, runtime.ErrRunSourceInvalid
	}
	if err != nil {
		return engine.Snapshot{}, err
	}
	if err = json.Unmarshal(raw, &result.DSL); err != nil {
		return engine.Snapshot{}, fmt.Errorf("decode immutable runtime DSL: %w", err)
	}
	transaction.snapshot = &result
	return result, nil
}

func (transaction *runtimeTransaction) LoadEngineState(ctx context.Context, projectID, runID string) (engine.State, error) {
	run, err := loadRun(ctx, transaction.tx, projectID, runID, true)
	if err != nil {
		return engine.State{}, err
	}
	rows, err := transaction.tx.Query(ctx, `
		SELECT n.execution_node_id, n.kind, n.state, n.state_version, n.activated,
		       n.selected_route, n.resolved_inputs_json, n.current_attempt_id::text,
		       n.effective_attempt_id::text, n.next_attempt_seq, n.business_attempt_count,
		       n.recovery_count, n.next_attempt_kind, n.next_retry_at, n.failure_json,
		       n.cancel_reason, o.value_json
		FROM node_runs n
		LEFT JOIN node_output_values o
		  ON o.project_id=n.project_id AND o.attempt_id=n.effective_attempt_id
		WHERE n.project_id=$1 AND n.run_id=$2
		ORDER BY n.execution_node_id
		FOR UPDATE OF n`, projectID, runID)
	if err != nil {
		return engine.State{}, err
	}
	defer rows.Close()
	state := engine.State{Run: run}
	for rows.Next() {
		var node runtime.NodeRunRecord
		var inputs, failure, outputs []byte
		var current, effective *string
		var nextRetry *time.Time
		err = rows.Scan(&node.ExecutionNodeID, &node.Kind, &node.State, &node.StateVersion, &node.Activated,
			&node.SelectedRoute, &inputs, &current, &effective, &node.NextAttemptSeq,
			&node.BusinessAttemptCount, &node.RecoveryCount, &node.NextAttemptKind, &nextRetry,
			&failure, &node.CancelReason, &outputs)
		if err != nil {
			return engine.State{}, err
		}
		node.RunID = runID
		if current != nil {
			node.CurrentAttemptID = *current
		}
		if effective != nil {
			node.EffectiveAttemptID = *effective
		}
		if nextRetry != nil {
			node.NextRetryAt = *nextRetry
		}
		if err = decodeJSONMap(inputs, &node.ResolvedInputs); err != nil {
			return engine.State{}, err
		}
		if err = decodeOptionalJSON(failure, &node.Failure); err != nil {
			return engine.State{}, err
		}
		if err = decodeJSONMap(outputs, &node.EffectiveOutputs); err != nil {
			return engine.State{}, err
		}
		state.Nodes = append(state.Nodes, node)
	}
	if err = rows.Err(); err != nil {
		return engine.State{}, err
	}
	attemptRows, err := transaction.tx.Query(ctx, `
		SELECT a.attempt_id::text, n.execution_node_id, a.attempt_seq, a.attempt_kind,
		       a.state, a.state_version, a.error_json, o.value_json
		FROM node_attempts a
		JOIN node_runs n ON n.project_id=a.project_id AND n.run_id=a.run_id AND n.node_run_id=a.node_run_id
		LEFT JOIN node_output_values o ON o.project_id=a.project_id AND o.attempt_id=a.attempt_id
		WHERE a.project_id=$1 AND a.run_id=$2
		ORDER BY a.attempt_seq, a.attempt_id
		FOR UPDATE OF a`, projectID, runID)
	if err != nil {
		return engine.State{}, err
	}
	defer attemptRows.Close()
	for attemptRows.Next() {
		var attempt runtime.NodeAttemptRecord
		var executionNodeID string
		var errorJSON, outputJSON []byte
		if err = attemptRows.Scan(&attempt.ID, &executionNodeID, &attempt.Sequence, &attempt.Kind,
			&attempt.State, &attempt.StateVersion, &errorJSON, &outputJSON); err != nil {
			return engine.State{}, err
		}
		attempt.NodeRunID = runID + ":" + executionNodeID
		if attempt.State.Terminal() {
			result := runtime.AttemptResult{State: attempt.State}
			if len(errorJSON) != 0 {
				var failure struct {
					ErrorCode    string         `json:"error_code"`
					Message      string         `json:"message"`
					DSLField     string         `json:"dsl_field"`
					ErrorDetails map[string]any `json:"error_details"`
				}
				if err = json.Unmarshal(errorJSON, &failure); err != nil {
					return engine.State{}, err
				}
				result.ErrorCode, result.Message, result.DSLField, result.ErrorDetails = failure.ErrorCode, failure.Message, failure.DSLField, failure.ErrorDetails
			}
			if err = decodeJSONMap(outputJSON, &result.Outputs); err != nil {
				return engine.State{}, err
			}
			attempt.Result = &result
		}
		state.Attempts = append(state.Attempts, attempt)
	}
	return state, attemptRows.Err()
}

func (transaction *runtimeTransaction) InitializeRun(ctx context.Context, before runtime.WorkflowRunRecord, after engine.State, at time.Time) error {
	if transaction.snapshot == nil || transaction.router == nil {
		return fmt.Errorf("runtime snapshot and routing policy are required for initialization")
	}
	definitions := make(map[string]dsl.Node, len(transaction.snapshot.DSL.Nodes))
	for _, definition := range transaction.snapshot.DSL.Nodes {
		definitions[string(definition.ID)] = definition
	}
	for _, node := range after.Nodes {
		definition, exists := definitions[node.ExecutionNodeID]
		if !exists {
			return fmt.Errorf("runtime node %q is absent from immutable snapshot", node.ExecutionNodeID)
		}
		var resourceClass any
		if node.Kind == runtime.NodeTask {
			resolved, routable := transaction.router.Resolve(definition.Operation.Coordinate())
			if !routable {
				return fmt.Errorf("runtime operation %s@%d has no routing policy", definition.Operation.Type, definition.Operation.Version)
			}
			resourceClass = resolved
		}
		nodeRunID := deterministicNodeRunID(after.Run.ID, node.ExecutionNodeID)
		var readyAt any
		if node.State == runtime.NodeReady {
			readyAt = at
		}
		_, err := transaction.tx.Exec(ctx, `
			INSERT INTO node_runs (
				node_run_id, project_id, run_id, execution_node_id, kind, state, state_version,
				operation_type, operation_version, resource_class,
				activated, selected_route, resolved_inputs_json, next_attempt_seq,
				business_attempt_count, recovery_count, next_attempt_kind, priority,
				ready_at, next_retry_at, failure_json, cancel_reason, created_at, updated_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,0,$18,$19,$20,$21,$22,$22)`,
			nodeRunID, after.Run.ProjectID, after.Run.ID, node.ExecutionNodeID, node.Kind,
			node.State, node.StateVersion, definition.Operation.Type, definition.Operation.Version, resourceClass,
			node.Activated, node.SelectedRoute,
			nullableJSON(node.ResolvedInputs), node.NextAttemptSeq, node.BusinessAttemptCount,
			node.RecoveryCount, node.NextAttemptKind, readyAt, nullableTime(node.NextRetryAt),
			nullableJSON(node.Failure), node.CancelReason, at)
		if err != nil {
			return err
		}
	}
	return transaction.updateRunCAS(ctx, before, after.Run, at)
}

func (transaction *runtimeTransaction) FailRunInitialization(ctx context.Context, before, after runtime.WorkflowRunRecord, at time.Time) error {
	return transaction.updateRunCAS(ctx, before, after, at)
}

func (transaction *runtimeTransaction) AdvanceRun(ctx context.Context, before, after engine.State, at time.Time) error {
	beforeNodes := make(map[string]runtime.NodeRunRecord, len(before.Nodes))
	for _, node := range before.Nodes {
		beforeNodes[node.ExecutionNodeID] = node
	}
	for _, node := range after.Nodes {
		previous, exists := beforeNodes[node.ExecutionNodeID]
		if !exists {
			return fmt.Errorf("%w: node set changed during advancement", runtime.ErrRunConflict)
		}
		if node.StateVersion == previous.StateVersion {
			continue
		}
		var readyAt any
		if node.State == runtime.NodeReady {
			readyAt = at
		}
		tag, err := transaction.tx.Exec(ctx, `
			UPDATE node_runs SET
				state=$1, state_version=$2, activated=$3, selected_route=$4,
				resolved_inputs_json=$5, current_attempt_id=$6, effective_attempt_id=$7,
				next_attempt_seq=$8, business_attempt_count=$9, recovery_count=$10,
				next_attempt_kind=$11, ready_at=$12, next_retry_at=$13,
				failure_json=$14, cancel_reason=$15, updated_at=$16
			WHERE project_id=$17 AND run_id=$18 AND execution_node_id=$19 AND state_version=$20`,
			node.State, node.StateVersion, node.Activated, node.SelectedRoute,
			nullableJSON(node.ResolvedInputs), nullableString(node.CurrentAttemptID), nullableString(node.EffectiveAttemptID),
			node.NextAttemptSeq, node.BusinessAttemptCount, node.RecoveryCount, node.NextAttemptKind,
			readyAt, nullableTime(node.NextRetryAt), nullableJSON(node.Failure), node.CancelReason, at,
			after.Run.ProjectID, after.Run.ID, node.ExecutionNodeID, previous.StateVersion)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return runtime.ErrRunConflict
		}
	}
	if after.Run.StateVersion != before.Run.StateVersion {
		return transaction.updateRunCAS(ctx, before.Run, after.Run, at)
	}
	return nil
}

func (transaction *runtimeTransaction) updateRunCAS(ctx context.Context, before, after runtime.WorkflowRunRecord, at time.Time) error {
	ids, err := json.Marshal(after.ExecutionNodeIDs)
	if err != nil {
		return err
	}
	tag, err := transaction.tx.Exec(ctx, `
		UPDATE workflow_runs SET
			state=$1, state_version=$2, workflow_output_json=$3,
			execution_node_ids=$4, termination_intent_json=$5, updated_at=$6
		WHERE project_id=$7 AND run_id=$8 AND state=$9 AND state_version=$10`,
		after.State, after.StateVersion, nullableRaw(after.WorkflowOutput), ids,
		nullableJSON(after.Termination), at, after.ProjectID, after.ID, before.State, before.StateVersion)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return runtime.ErrRunConflict
	}
	return nil
}

func loadRun(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, projectID, runID string, forUpdate bool) (runtime.WorkflowRunRecord, error) {
	query := `
		SELECT run_id::text, project_id::text, workflow_id::text, purpose,
		       snapshot_id::text, definition_hash, definition_source,
		       published_version_id::text, input_json, workflow_output_json,
		       deadline_at, created_at, state, state_version,
		       execution_node_ids, termination_intent_json
		FROM workflow_runs WHERE project_id=$1 AND run_id=$2`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	var result runtime.WorkflowRunRecord
	var versionID *string
	var source runtime.DefinitionSource
	var output, nodeIDs, termination []byte
	err := queryer.QueryRow(ctx, query, projectID, runID).Scan(
		&result.ID, &result.ProjectID, &result.WorkflowID, &result.Purpose,
		&result.Definition.SnapshotID, &result.Definition.DefinitionHash, &source,
		&versionID, &result.WorkflowInput, &output, &result.DeadlineAt, &result.CreatedAt,
		&result.State, &result.StateVersion, &nodeIDs, &termination)
	if errors.Is(err, pgx.ErrNoRows) {
		return runtime.WorkflowRunRecord{}, runtime.ErrRunNotFound
	}
	if err != nil {
		return runtime.WorkflowRunRecord{}, err
	}
	result.Definition.Source = source
	if versionID != nil {
		result.Definition.PublishedVersionID = *versionID
	}
	result.WorkflowOutput = output
	if err = json.Unmarshal(nodeIDs, &result.ExecutionNodeIDs); err != nil {
		return runtime.WorkflowRunRecord{}, err
	}
	if err = decodeOptionalJSON(termination, &result.Termination); err != nil {
		return runtime.WorkflowRunRecord{}, err
	}
	return result, nil
}

func insertOutbox(ctx context.Context, tx pgx.Tx, event eventing.RuntimeEvent) error {
	if err := event.Validate(); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO outbox_events (
			event_id, project_id, run_id, aggregate_type, aggregate_id,
			event_type, message_version, occurred_at, trace_id, available_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$8)`, event.EventID, event.ProjectID,
		event.RunID, event.AggregateType, event.AggregateID, event.EventType,
		event.MessageVersion, event.OccurredAt, event.TraceID)
	return err
}

func deterministicNodeRunID(runID, executionNodeID string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(runID+":"+executionNodeID)).String()
}

func nullableJSON(value any) any {
	if value == nil {
		return nil
	}
	bytes, err := json.Marshal(value)
	if err != nil || string(bytes) == "null" {
		return nil
	}
	return bytes
}

func nullableRaw(value json.RawMessage) any {
	if len(value) == 0 {
		return nil
	}
	return value
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func nullableTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value
}

func decodeJSONMap(raw []byte, target *map[string]json.RawMessage) error {
	if len(raw) == 0 {
		*target = nil
		return nil
	}
	return json.Unmarshal(raw, target)
}

func decodeOptionalJSON[T any](raw []byte, target **T) error {
	if len(raw) == 0 {
		*target = nil
		return nil
	}
	var value T
	if err := json.Unmarshal(raw, &value); err != nil {
		return err
	}
	*target = &value
	return nil
}

var _ engine.TransactionManager = (*Store)(nil)
var _ runtime.RunRepository = (*Store)(nil)
var _ = dsl.Document{}
