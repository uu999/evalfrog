package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/eventing"
	"github.com/uu999/evalfrog/internal/runtime"
	"github.com/uu999/evalfrog/internal/runtime/attempt"
	"github.com/uu999/evalfrog/internal/scheduling"
)

func (store *Store) Claim(ctx context.Context, record attempt.ClaimRecord) (attempt.Lease, error) {
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return attempt.Lease{}, err
	}
	defer tx.Rollback(ctx)
	var state runtime.AttemptState
	var stateVersion uint64
	var sequence uint32
	var nodeState runtime.NodeState
	var currentAttemptID *string
	var nodeRunID string
	var existingToken, existingOwner *string
	var existingFencing uint64
	var existingExpiry *time.Time
	var existingLeaseValid bool
	var operation dsl.Coordinate
	var resourceClass scheduling.ResourceClass
	err = tx.QueryRow(ctx, `
		SELECT node_run_id::text FROM node_attempts
		WHERE project_id=$1 AND run_id=$2 AND attempt_id=$3`,
		record.ProjectID, record.RunID, record.AttemptID).Scan(&nodeRunID)
	if errors.Is(err, pgx.ErrNoRows) {
		return attempt.Lease{}, attempt.ErrNotFound
	}
	if err != nil {
		return attempt.Lease{}, err
	}
	// Keep the same Node -> Attempt row-lock order used by Engine state restore.
	err = tx.QueryRow(ctx, `
		SELECT state, current_attempt_id::text, operation_type, operation_version, resource_class FROM node_runs
		WHERE project_id=$1 AND run_id=$2 AND node_run_id=$3
		FOR UPDATE`, record.ProjectID, record.RunID, nodeRunID).
		Scan(&nodeState, &currentAttemptID, &operation.Type, &operation.Version, &resourceClass)
	if err != nil {
		return attempt.Lease{}, err
	}
	err = tx.QueryRow(ctx, `
		SELECT state, state_version, attempt_seq, lease_token::text, lease_owner,
		       fencing_token, lease_expires_at,
		       COALESCE(lease_expires_at >= clock_timestamp(), false)
		FROM node_attempts
		WHERE project_id=$1 AND run_id=$2 AND attempt_id=$3
		FOR UPDATE`, record.ProjectID, record.RunID, record.AttemptID).
		Scan(&state, &stateVersion, &sequence,
			&existingToken, &existingOwner, &existingFencing, &existingExpiry, &existingLeaseValid)
	if err != nil {
		return attempt.Lease{}, err
	}
	if sequence != record.AttemptSequence || currentAttemptID == nil || *currentAttemptID != record.AttemptID {
		return attempt.Lease{}, attempt.ErrNotCurrent
	}
	if resourceClass != record.ResourceClass || !containsCapability(record.Capabilities, operation) {
		return attempt.Lease{}, attempt.ErrCapabilityMismatch
	}
	if state == runtime.AttemptRunning {
		if existingOwner != nil && *existingOwner == record.WorkerID && existingToken != nil && existingExpiry != nil && existingLeaseValid {
			if err = tx.Commit(ctx); err != nil {
				return attempt.Lease{}, err
			}
			return attempt.Lease{Token: *existingToken, Owner: *existingOwner, FencingToken: existingFencing, ExpiresAt: *existingExpiry}, nil
		}
		return attempt.Lease{}, attempt.ErrStateConflict
	}
	if state != runtime.AttemptQueued || nodeState != runtime.NodeQueued {
		return attempt.Lease{}, attempt.ErrStateConflict
	}
	fencing := uint64(record.AttemptSequence)
	var expiresAt time.Time
	err = tx.QueryRow(ctx, `
		UPDATE node_attempts SET state='running', state_version=state_version+1,
		       lease_token=$1, lease_owner=$2,
		       lease_expires_at=clock_timestamp()+($3 * interval '1 millisecond'),
		       fencing_token=$4, executor_build=$5, updated_at=clock_timestamp()
		WHERE project_id=$6 AND run_id=$7 AND attempt_id=$8
		  AND state='queued' AND state_version=$9 AND attempt_seq=$10
		RETURNING lease_expires_at`,
		record.LeaseToken, record.WorkerID, record.LeaseDuration.Milliseconds(), fencing,
		record.ExecutorBuild, record.ProjectID, record.RunID, record.AttemptID,
		stateVersion, record.AttemptSequence).Scan(&expiresAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return attempt.Lease{}, attempt.ErrStateConflict
		}
		return attempt.Lease{}, err
	}
	nodeTag, err := tx.Exec(ctx, `
		UPDATE node_runs SET state='running', state_version=state_version+1, updated_at=$1
		WHERE project_id=$2 AND run_id=$3 AND current_attempt_id=$4 AND state='queued'`,
		record.Now, record.ProjectID, record.RunID, record.AttemptID)
	if err != nil || nodeTag.RowsAffected() != 1 {
		if err == nil {
			err = attempt.ErrStateConflict
		}
		return attempt.Lease{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return attempt.Lease{}, err
	}
	return attempt.Lease{Token: record.LeaseToken, Owner: record.WorkerID, FencingToken: fencing, ExpiresAt: expiresAt}, nil
}

func containsCapability(capabilities []dsl.Coordinate, required dsl.Coordinate) bool {
	for _, capability := range capabilities {
		if capability == required {
			return true
		}
	}
	return false
}

func (store *Store) Heartbeat(ctx context.Context, record attempt.HeartbeatRecord) (attempt.Lease, error) {
	var owner string
	var expiresAt time.Time
	err := store.pool.QueryRow(ctx, `
		UPDATE node_attempts SET
		       lease_expires_at=clock_timestamp()+($1 * interval '1 millisecond'),
		       updated_at=clock_timestamp()
		WHERE project_id=$2 AND run_id=$3 AND attempt_id=$4 AND attempt_seq=$5
		  AND state='running' AND lease_token=$6 AND fencing_token=$7
		  AND lease_expires_at >= clock_timestamp()
		RETURNING lease_owner, lease_expires_at`, record.ExtendBy.Milliseconds(),
		record.ProjectID, record.RunID, record.AttemptID, record.AttemptSequence,
		record.LeaseToken, record.FencingToken).Scan(&owner, &expiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return attempt.Lease{}, attempt.ErrLeaseMismatch
	}
	return attempt.Lease{Token: record.LeaseToken, Owner: owner, FencingToken: record.FencingToken, ExpiresAt: expiresAt}, err
}

func (store *Store) Complete(ctx context.Context, record attempt.CompleteRecord) (bool, error) {
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	var state runtime.AttemptState
	var stateVersion uint64
	var sequence uint32
	var nodeRunID, currentAttemptID string
	var leaseToken *string
	var fencing uint64
	var leaseExpiresAt *time.Time
	err = tx.QueryRow(ctx, `
		SELECT node_run_id::text FROM node_attempts
		WHERE project_id=$1 AND run_id=$2 AND attempt_id=$3`,
		record.ProjectID, record.RunID, record.AttemptID).Scan(&nodeRunID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, attempt.ErrNotFound
	}
	if err != nil {
		return false, err
	}
	// Lock the owning Node before its Attempt so all runtime transactions agree
	// on one row-lock order and PostgreSQL only needs deadlock retries for true
	// cross-aggregate conflicts.
	err = tx.QueryRow(ctx, `
		SELECT current_attempt_id::text FROM node_runs
		WHERE project_id=$1 AND run_id=$2 AND node_run_id=$3
		FOR UPDATE`, record.ProjectID, record.RunID, nodeRunID).Scan(&currentAttemptID)
	if err != nil {
		return false, err
	}
	err = tx.QueryRow(ctx, `
		SELECT state, state_version, attempt_seq, lease_token::text, fencing_token, lease_expires_at
		FROM node_attempts
		WHERE project_id=$1 AND run_id=$2 AND attempt_id=$3
		FOR UPDATE`, record.ProjectID, record.RunID, record.AttemptID).
		Scan(&state, &stateVersion, &sequence, &leaseToken, &fencing, &leaseExpiresAt)
	if err != nil {
		return false, err
	}
	if sequence != record.AttemptSequence {
		return false, attempt.ErrNotCurrent
	}
	if leaseToken == nil || *leaseToken != record.LeaseToken || fencing != record.FencingToken {
		return false, attempt.ErrLeaseMismatch
	}
	if state.Terminal() {
		var existingError []byte
		var existingOutput []byte
		if err = tx.QueryRow(ctx, `
			SELECT a.error_json, o.value_json
			FROM node_attempts a LEFT JOIN node_output_values o
			  ON o.project_id=a.project_id AND o.attempt_id=a.attempt_id
			WHERE a.project_id=$1 AND a.attempt_id=$2`, record.ProjectID, record.AttemptID).
			Scan(&existingError, &existingOutput); err != nil {
			return false, err
		}
		if !sameCompletion(state, existingError, existingOutput, record.Result) {
			return false, attempt.ErrStateConflict
		}
		if err = tx.Commit(ctx); err != nil {
			return false, err
		}
		return false, nil
	}
	if currentAttemptID != record.AttemptID {
		return false, attempt.ErrNotCurrent
	}
	if state != runtime.AttemptRunning || leaseExpiresAt == nil {
		return false, attempt.ErrLeaseMismatch
	}
	errorJSON := completionError(record.Result)
	tag, err := tx.Exec(ctx, `
		UPDATE node_attempts SET state=$1, state_version=state_version+1,
		       error_json=$2, updated_at=$3
		WHERE project_id=$4 AND run_id=$5 AND attempt_id=$6 AND state='running'
		  AND state_version=$7 AND attempt_seq=$8 AND lease_token=$9 AND fencing_token=$10
		  AND lease_expires_at >= clock_timestamp()`,
		record.Result.State, nullableRaw(errorJSON), record.Now, record.ProjectID, record.RunID,
		record.AttemptID, stateVersion, record.AttemptSequence, record.LeaseToken, record.FencingToken)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() != 1 {
		return false, attempt.ErrStateConflict
	}
	if record.Result.State == runtime.AttemptSucceeded {
		valueJSON, marshalErr := json.Marshal(record.Result.Outputs)
		if marshalErr != nil {
			return false, marshalErr
		}
		digest := sha256.Sum256(valueJSON)
		_, err = tx.Exec(ctx, `
			INSERT INTO node_output_values (
				project_id, attempt_id, run_id, node_run_id, value_json, size_bytes, content_hash, created_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`, record.ProjectID, record.AttemptID,
			record.RunID, nodeRunID, valueJSON, len(valueJSON), hex.EncodeToString(digest[:]), record.Now)
		if err != nil {
			return false, err
		}
	}
	event := eventing.RuntimeEvent{
		MessageVersion: eventing.RuntimeMessageVersion, EventID: record.EventID,
		ProjectID: record.ProjectID, RunID: record.RunID, AggregateType: eventing.NodeAttemptAggregate,
		AggregateID: record.AttemptID, EventType: eventing.AttemptCompleted,
		OccurredAt: record.Now, TraceID: record.TraceID,
	}
	if err = insertOutbox(ctx, tx, event); err != nil {
		return false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func (store *Store) MarkExpiredLost(ctx context.Context, record attempt.MarkLostRecord) (bool, error) {
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	var state runtime.AttemptState
	var stateVersion uint64
	var sequence uint32
	var currentAttemptID string
	var nodeRunID string
	err = tx.QueryRow(ctx, `
		SELECT node_run_id::text FROM node_attempts
		WHERE project_id=$1 AND run_id=$2 AND attempt_id=$3`,
		record.ProjectID, record.RunID, record.AttemptID).Scan(&nodeRunID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, attempt.ErrNotFound
	}
	if err != nil {
		return false, err
	}
	err = tx.QueryRow(ctx, `
		SELECT current_attempt_id::text FROM node_runs
		WHERE project_id=$1 AND run_id=$2 AND node_run_id=$3
		FOR UPDATE`, record.ProjectID, record.RunID, nodeRunID).Scan(&currentAttemptID)
	if err != nil {
		return false, err
	}
	err = tx.QueryRow(ctx, `
		SELECT state, state_version, attempt_seq FROM node_attempts
		WHERE project_id=$1 AND run_id=$2 AND attempt_id=$3
		FOR UPDATE`, record.ProjectID, record.RunID, record.AttemptID).
		Scan(&state, &stateVersion, &sequence)
	if err != nil {
		return false, err
	}
	if sequence != record.AttemptSequence || currentAttemptID != record.AttemptID {
		return false, attempt.ErrNotCurrent
	}
	if state == runtime.AttemptLost {
		if err = tx.Commit(ctx); err != nil {
			return false, err
		}
		return false, nil
	}
	if state != runtime.AttemptRunning {
		return false, attempt.ErrStateConflict
	}
	errorJSON := completionError(runtime.AttemptResult{State: runtime.AttemptLost, ErrorCode: "LEASE_LOST", Message: "attempt lease expired"})
	tag, err := tx.Exec(ctx, `
		UPDATE node_attempts SET state='lost', state_version=state_version+1,
		       error_json=$1, updated_at=clock_timestamp()
		WHERE project_id=$2 AND run_id=$3 AND attempt_id=$4
		  AND state='running' AND state_version=$5 AND attempt_seq=$6
		  AND lease_expires_at <= clock_timestamp()`, errorJSON, record.ProjectID,
		record.RunID, record.AttemptID, stateVersion, record.AttemptSequence)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		return false, attempt.ErrLeaseMismatch
	}
	event := eventing.RuntimeEvent{
		MessageVersion: eventing.RuntimeMessageVersion, EventID: record.EventID,
		ProjectID: record.ProjectID, RunID: record.RunID, AggregateType: eventing.NodeAttemptAggregate,
		AggregateID: record.AttemptID, EventType: eventing.AttemptLost,
		OccurredAt: record.Now, TraceID: record.TraceID,
	}
	if err = insertOutbox(ctx, tx, event); err != nil {
		return false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func completionError(result runtime.AttemptResult) json.RawMessage {
	if result.State == runtime.AttemptSucceeded {
		return nil
	}
	value, _ := json.Marshal(struct {
		ErrorCode string `json:"error_code"`
		Message   string `json:"message"`
	}{result.ErrorCode, result.Message})
	return value
}

func sameCompletion(state runtime.AttemptState, errorJSON, outputJSON []byte, result runtime.AttemptResult) bool {
	if state != result.State {
		return false
	}
	if state == runtime.AttemptSucceeded {
		candidate, err := json.Marshal(result.Outputs)
		return err == nil && jsonEqual(candidate, outputJSON)
	}
	return jsonEqual(completionError(result), errorJSON)
}

func jsonEqual(left, right []byte) bool {
	var a, b any
	return json.Unmarshal(left, &a) == nil && json.Unmarshal(right, &b) == nil && reflect.DeepEqual(a, b)
}

var _ attempt.Repository = (*Store)(nil)
