package postgres

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/uu999/evalfrog/internal/runtime/attempt"
	runtimecontext "github.com/uu999/evalfrog/internal/runtime/context"
)

func (store *Store) LoadAttemptMetadata(ctx context.Context, command runtimecontext.LoadCommand) (runtimecontext.Metadata, error) {
	var value runtimecontext.Metadata
	var inputs []byte
	err := store.pool.QueryRow(ctx, `
		SELECT a.project_id::text, a.run_id::text, a.node_run_id::text,
		       n.execution_node_id, a.attempt_id::text, a.attempt_seq,
		       r.snapshot_id::text, n.operation_type, n.operation_version,
		       n.resource_class, n.resolved_inputs_json
		FROM node_attempts a
		JOIN node_runs n ON n.project_id=a.project_id AND n.run_id=a.run_id AND n.node_run_id=a.node_run_id
		JOIN workflow_runs r ON r.project_id=a.project_id AND r.run_id=a.run_id
		WHERE a.project_id=$1 AND a.run_id=$2 AND a.attempt_id=$3 AND a.attempt_seq=$4
		  AND a.state='running' AND a.lease_token=$5 AND a.fencing_token=$6
		  AND a.lease_expires_at >= clock_timestamp()`,
		command.ProjectID, command.RunID, command.AttemptID, command.AttemptSequence,
		command.LeaseToken, command.FencingToken).
		Scan(&value.ProjectID, &value.RunID, &value.NodeRunID, &value.ExecutionNodeID,
			&value.AttemptID, &value.AttemptSequence, &value.SnapshotID,
			&value.Operation.Type, &value.Operation.Version, &value.ResourceClass, &inputs)
	if errors.Is(err, pgx.ErrNoRows) {
		return runtimecontext.Metadata{}, attempt.ErrNotFound
	}
	if err != nil {
		return runtimecontext.Metadata{}, err
	}
	if err = json.Unmarshal(inputs, &value.ResolvedInputs); err != nil {
		return runtimecontext.Metadata{}, err
	}
	return value, nil
}

func (store *Store) LoadSnapshotDSL(ctx context.Context, projectID, snapshotID string) (json.RawMessage, error) {
	var value []byte
	err := store.pool.QueryRow(ctx, `SELECT dsl_json FROM workflow_execution_snapshots WHERE project_id=$1 AND snapshot_id=$2`, projectID, snapshotID).Scan(&value)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, attempt.ErrNotFound
	}
	return json.RawMessage(value), err
}

func (store *Store) LoadRunInput(ctx context.Context, projectID, runID string) (json.RawMessage, error) {
	var value []byte
	err := store.pool.QueryRow(ctx, `SELECT input_json FROM workflow_runs WHERE project_id=$1 AND run_id=$2`, projectID, runID).Scan(&value)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, attempt.ErrNotFound
	}
	return json.RawMessage(value), err
}

func (store *Store) LoadEffectiveOutput(ctx context.Context, projectID, runID, executionNodeID string) (json.RawMessage, error) {
	var value []byte
	err := store.pool.QueryRow(ctx, `
		SELECT o.value_json
		FROM node_runs n JOIN node_output_values o
		  ON o.project_id=n.project_id AND o.run_id=n.run_id AND o.attempt_id=n.effective_attempt_id
		WHERE n.project_id=$1 AND n.run_id=$2 AND n.execution_node_id=$3 AND n.state='succeeded'`,
		projectID, runID, executionNodeID).Scan(&value)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, attempt.ErrNotFound
	}
	return json.RawMessage(value), err
}

var _ runtimecontext.Repository = (*Store)(nil)
