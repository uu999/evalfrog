package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/uu999/evalfrog/internal/resources"
)

func (store *Store) ResolveConnection(ctx context.Context, projectID, executionIdentityID, reference string, purpose resources.Purpose) (resources.Connection, error) {
	var result resources.Connection
	err := store.pool.QueryRow(ctx, `
		SELECT c.connection_id::text, c.project_id::text, c.reference
		FROM connections c
		JOIN connection_execution_grants g
		  ON g.project_id = c.project_id AND g.connection_id = c.connection_id
		WHERE c.project_id = $1 AND c.reference = $2 AND c.enabled
		  AND g.execution_identity_id = $3 AND g.purpose = $4`,
		projectID, reference, executionIdentityID, purpose,
	).Scan(&result.ID, &result.ProjectID, &result.Reference)
	if errors.Is(err, pgx.ErrNoRows) {
		return resources.Connection{}, resources.ErrResourceNotFound
	}
	return result, err
}

func (store *Store) ResolveServiceOperation(ctx context.Context, projectID, executionIdentityID, reference, operation string, purpose resources.Purpose) (resources.ServiceOperation, error) {
	var result resources.ServiceOperation
	err := store.pool.QueryRow(ctx, `
		SELECT s.service_id::text, s.project_id::text, s.reference,
		       o.operation, o.contract_revision, o.idempotent
		FROM rpc_services s
		JOIN rpc_service_operations o
		  ON o.project_id = s.project_id AND o.service_id = s.service_id
		JOIN rpc_service_execution_grants g
		  ON g.project_id = s.project_id AND g.service_id = s.service_id
		WHERE s.project_id = $1 AND s.reference = $2 AND s.enabled
		  AND o.operation = $3 AND o.enabled
		  AND g.execution_identity_id = $4 AND g.purpose = $5`,
		projectID, reference, operation, executionIdentityID, purpose,
	).Scan(&result.ServiceID, &result.ProjectID, &result.ServiceReference, &result.Operation, &result.ContractRevision, &result.Idempotent)
	if errors.Is(err, pgx.ErrNoRows) {
		return resources.ServiceOperation{}, resources.ErrResourceNotFound
	}
	return result, err
}
