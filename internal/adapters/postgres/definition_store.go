package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/uu999/evalfrog/internal/definition"
)

type scanner interface {
	Scan(...any) error
}

type idempotencyResult struct {
	requestHash string
	responseID  string
	auxiliaryID *string
}

func (store *Store) CreateWorkflow(ctx context.Context, record definition.CreateWorkflowRecord) (definition.Workflow, definition.DraftRevision, error) {
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return definition.Workflow{}, definition.DraftRevision{}, err
	}
	defer tx.Rollback(ctx)
	result, exists, err := lockIdempotency(ctx, tx, record.ProjectID, "create_workflow", "project", record.IdempotencyKey)
	if err != nil {
		return definition.Workflow{}, definition.DraftRevision{}, err
	}
	if exists {
		if result.requestHash != record.RequestHash {
			return definition.Workflow{}, definition.DraftRevision{}, definition.ErrIdempotencyKeyReused
		}
		workflow, err := getWorkflow(ctx, tx, record.ProjectID, result.responseID, false)
		if err != nil {
			return definition.Workflow{}, definition.DraftRevision{}, err
		}
		if result.auxiliaryID == nil {
			return definition.Workflow{}, definition.DraftRevision{}, fmt.Errorf("create workflow idempotency record has no draft revision")
		}
		revision, err := getDraftRevisionByID(ctx, tx, record.ProjectID, result.responseID, *result.auxiliaryID)
		return workflow, revision, err
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO workflows (workflow_id, project_id, name, created_by)
		VALUES ($1, $2, $3, $4)`, record.WorkflowID, record.ProjectID, record.Name, record.PrincipalID); err != nil {
		return definition.Workflow{}, definition.DraftRevision{}, err
	}
	if err = insertDraftRevision(ctx, tx, definition.SaveDraftRecord{
		DraftRevisionID: record.DraftRevisionID, ProjectID: record.ProjectID, WorkflowID: record.WorkflowID,
		IRJSON: record.IRJSON, CatalogRevision: record.CatalogRevision, PrincipalID: record.PrincipalID,
	}, 1, nil); err != nil {
		return definition.Workflow{}, definition.DraftRevision{}, err
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO workflow_drafts (project_id, workflow_id, current_revision, current_revision_id, state_version)
		VALUES ($1, $2, 1, $3, 1)`, record.ProjectID, record.WorkflowID, record.DraftRevisionID); err != nil {
		return definition.Workflow{}, definition.DraftRevision{}, err
	}
	if err = insertIdempotency(ctx, tx, record.ProjectID, "create_workflow", "project", record.IdempotencyKey, record.RequestHash, "workflow", record.WorkflowID, &record.DraftRevisionID); err != nil {
		return definition.Workflow{}, definition.DraftRevision{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return definition.Workflow{}, definition.DraftRevision{}, err
	}
	return store.GetWorkflowAndRevision(ctx, record.ProjectID, record.WorkflowID, record.DraftRevisionID)
}

func (store *Store) GetWorkflowAndRevision(ctx context.Context, projectID, workflowID, revisionID string) (definition.Workflow, definition.DraftRevision, error) {
	workflow, err := store.GetWorkflow(ctx, projectID, workflowID)
	if err != nil {
		return definition.Workflow{}, definition.DraftRevision{}, err
	}
	revision, err := getDraftRevisionByID(ctx, store.pool, projectID, workflowID, revisionID)
	return workflow, revision, err
}

func (store *Store) GetWorkflow(ctx context.Context, projectID, workflowID string) (definition.Workflow, error) {
	return getWorkflow(ctx, store.pool, projectID, workflowID, false)
}

func (store *Store) GetCurrentDraft(ctx context.Context, projectID, workflowID string) (definition.Draft, error) {
	row := store.pool.QueryRow(ctx, `
		SELECT d.project_id::text, d.workflow_id::text, d.current_revision, d.state_version,
		       r.draft_revision_id::text, r.project_id::text, r.workflow_id::text,
		       r.revision_number, r.ir_json, r.catalog_revision,
		       r.created_by::text, r.created_at, r.cloned_from_version_id::text
		FROM workflow_drafts d
		JOIN workflow_draft_revisions r
		  ON r.project_id = d.project_id AND r.workflow_id = d.workflow_id
		 AND r.draft_revision_id = d.current_revision_id
		WHERE d.project_id = $1 AND d.workflow_id = $2`, projectID, workflowID)
	var draft definition.Draft
	err := row.Scan(&draft.ProjectID, &draft.WorkflowID, &draft.CurrentRevision, &draft.StateVersion,
		&draft.Current.ID, &draft.Current.ProjectID, &draft.Current.WorkflowID, &draft.Current.RevisionNumber,
		&draft.Current.IRJSON, &draft.Current.CatalogRevision, &draft.Current.CreatedBy,
		&draft.Current.CreatedAt, &draft.Current.ClonedFromVersionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return definition.Draft{}, definition.ErrResourceNotFound
	}
	return draft, err
}

func (store *Store) GetDraftRevision(ctx context.Context, projectID, workflowID string, revisionNumber int64) (definition.DraftRevision, error) {
	return scanDraftRevision(store.pool.QueryRow(ctx, draftRevisionSelect+`
		WHERE project_id = $1 AND workflow_id = $2 AND revision_number = $3`, projectID, workflowID, revisionNumber))
}

func (store *Store) SaveDraft(ctx context.Context, record definition.SaveDraftRecord) (definition.DraftRevision, error) {
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return definition.DraftRevision{}, err
	}
	defer tx.Rollback(ctx)
	scope := record.WorkflowID
	result, exists, err := lockIdempotency(ctx, tx, record.ProjectID, "save_draft", scope, record.IdempotencyKey)
	if err != nil {
		return definition.DraftRevision{}, err
	}
	if exists {
		if result.requestHash != record.RequestHash {
			return definition.DraftRevision{}, definition.ErrIdempotencyKeyReused
		}
		return getDraftRevisionByID(ctx, tx, record.ProjectID, record.WorkflowID, result.responseID)
	}
	var current int64
	err = tx.QueryRow(ctx, `
		SELECT current_revision FROM workflow_drafts
		WHERE project_id = $1 AND workflow_id = $2
		FOR UPDATE`, record.ProjectID, record.WorkflowID).Scan(&current)
	if errors.Is(err, pgx.ErrNoRows) {
		return definition.DraftRevision{}, definition.ErrResourceNotFound
	}
	if err != nil {
		return definition.DraftRevision{}, err
	}
	if current != record.ExpectedRevision {
		return definition.DraftRevision{}, definition.ErrDraftRevisionConflict
	}
	next := current + 1
	if err = insertDraftRevision(ctx, tx, record, next, nil); err != nil {
		return definition.DraftRevision{}, err
	}
	command, err := tx.Exec(ctx, `
		UPDATE workflow_drafts
		SET current_revision = $3, current_revision_id = $4,
		    state_version = state_version + 1, updated_at = clock_timestamp()
		WHERE project_id = $1 AND workflow_id = $2 AND current_revision = $5`,
		record.ProjectID, record.WorkflowID, next, record.DraftRevisionID, record.ExpectedRevision)
	if err != nil {
		return definition.DraftRevision{}, err
	}
	if command.RowsAffected() != 1 {
		return definition.DraftRevision{}, definition.ErrDraftRevisionConflict
	}
	if _, err = tx.Exec(ctx, `UPDATE workflows SET updated_at = clock_timestamp() WHERE project_id = $1 AND workflow_id = $2`, record.ProjectID, record.WorkflowID); err != nil {
		return definition.DraftRevision{}, err
	}
	if err = insertIdempotency(ctx, tx, record.ProjectID, "save_draft", scope, record.IdempotencyKey, record.RequestHash, "draft_revision", record.DraftRevisionID, nil); err != nil {
		return definition.DraftRevision{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return definition.DraftRevision{}, err
	}
	return getDraftRevisionByID(ctx, store.pool, record.ProjectID, record.WorkflowID, record.DraftRevisionID)
}

func (store *Store) StoreTestSnapshot(ctx context.Context, snapshot definition.ExecutionSnapshot) (definition.ExecutionSnapshot, error) {
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return definition.ExecutionSnapshot{}, err
	}
	defer tx.Rollback(ctx)
	stored, err := upsertSnapshot(ctx, tx, snapshot)
	if err != nil {
		return definition.ExecutionSnapshot{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return definition.ExecutionSnapshot{}, err
	}
	return stored, nil
}

func (store *Store) FindPublishedByIdempotency(ctx context.Context, projectID, workflowID, key, requestHash string) (definition.PublishedVersion, definition.ExecutionSnapshot, bool, error) {
	var storedHash, versionID string
	err := store.pool.QueryRow(ctx, `
		SELECT request_hash, response_id::text
		FROM definition_idempotency
		WHERE project_id = $1 AND command_name = 'publish_workflow'
		  AND target_scope = $2 AND idempotency_key = $3`,
		projectID, workflowID, key).Scan(&storedHash, &versionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, false, nil
	}
	if err != nil {
		return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, false, err
	}
	if storedHash != requestHash {
		return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, false, definition.ErrIdempotencyKeyReused
	}
	version, snapshot, err := getVersionAndSnapshotByID(ctx, store.pool, projectID, workflowID, versionID)
	return version, snapshot, err == nil, err
}

func (store *Store) Publish(ctx context.Context, record definition.PublishRecord) (definition.PublishedVersion, definition.ExecutionSnapshot, error) {
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, err
	}
	defer tx.Rollback(ctx)
	result, exists, err := lockIdempotency(ctx, tx, record.ProjectID, "publish_workflow", record.WorkflowID, record.IdempotencyKey)
	if err != nil {
		return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, err
	}
	if exists {
		if result.requestHash != record.RequestHash {
			return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, definition.ErrIdempotencyKeyReused
		}
		version, snapshot, err := getVersionAndSnapshotByID(ctx, tx, record.ProjectID, record.WorkflowID, result.responseID)
		return version, snapshot, err
	}
	var currentRevision int64
	err = tx.QueryRow(ctx, `
		SELECT d.current_revision
		FROM workflow_drafts d
		JOIN workflows w ON w.project_id = d.project_id AND w.workflow_id = d.workflow_id
		WHERE d.project_id = $1 AND d.workflow_id = $2
		FOR UPDATE OF d, w`, record.ProjectID, record.WorkflowID).Scan(&currentRevision)
	if errors.Is(err, pgx.ErrNoRows) {
		return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, definition.ErrResourceNotFound
	}
	if err != nil {
		return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, err
	}
	if currentRevision != record.ExpectedRevision {
		return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, definition.ErrDraftRevisionConflict
	}
	var actualDraftID string
	if err = tx.QueryRow(ctx, `
		SELECT draft_revision_id::text FROM workflow_draft_revisions
		WHERE project_id = $1 AND workflow_id = $2 AND revision_number = $3`,
		record.ProjectID, record.WorkflowID, record.ExpectedRevision).Scan(&actualDraftID); err != nil {
		return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, err
	}
	if actualDraftID != record.DraftRevisionID {
		return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, definition.ErrDraftRevisionConflict
	}
	stored, err := upsertSnapshot(ctx, tx, record.Snapshot)
	if err != nil {
		return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, err
	}
	var versionNumber int64
	if err = tx.QueryRow(ctx, `
		SELECT COALESCE(MAX(version_number), 0) + 1
		FROM workflow_versions WHERE project_id = $1 AND workflow_id = $2`, record.ProjectID, record.WorkflowID).Scan(&versionNumber); err != nil {
		return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, err
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO workflow_versions (
			version_id, project_id, workflow_id, version_number,
			source_draft_revision_id, execution_snapshot_id, change_log, created_by
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		record.VersionID, record.ProjectID, record.WorkflowID, versionNumber,
		record.DraftRevisionID, stored.ID, record.ChangeLog, record.PrincipalID); err != nil {
		return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, err
	}
	if _, err = tx.Exec(ctx, `
		UPDATE workflows SET active_version_id = $3, updated_at = clock_timestamp()
		WHERE project_id = $1 AND workflow_id = $2`, record.ProjectID, record.WorkflowID, record.VersionID); err != nil {
		return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, err
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO workflow_definition_audits (audit_id, project_id, workflow_id, event_type, version_id, principal_id)
		VALUES ($1,$2,$3,'publish',$4,$5)`, record.AuditID, record.ProjectID, record.WorkflowID, record.VersionID, record.PrincipalID); err != nil {
		return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, err
	}
	if err = insertIdempotency(ctx, tx, record.ProjectID, "publish_workflow", record.WorkflowID, record.IdempotencyKey, record.RequestHash, "published_version", record.VersionID, &stored.ID); err != nil {
		return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, err
	}
	version, _, err := getVersionAndSnapshotByID(ctx, store.pool, record.ProjectID, record.WorkflowID, record.VersionID)
	return version, stored, err
}

func (store *Store) Rollback(ctx context.Context, record definition.RollbackRecord) (definition.PublishedVersion, error) {
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return definition.PublishedVersion{}, err
	}
	defer tx.Rollback(ctx)
	var currentVersionID *string
	err = tx.QueryRow(ctx, `
		SELECT active_version_id::text FROM workflows
		WHERE project_id = $1 AND workflow_id = $2 FOR UPDATE`, record.ProjectID, record.WorkflowID).Scan(&currentVersionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return definition.PublishedVersion{}, definition.ErrResourceNotFound
	}
	if err != nil {
		return definition.PublishedVersion{}, err
	}
	version, err := scanVersion(tx.QueryRow(ctx, versionSelect+`
		WHERE project_id = $1 AND workflow_id = $2 AND version_number = $3`, record.ProjectID, record.WorkflowID, record.VersionNumber))
	if err != nil {
		return definition.PublishedVersion{}, err
	}
	if currentVersionID != nil && *currentVersionID == version.ID {
		return version, tx.Commit(ctx)
	}
	if _, err = tx.Exec(ctx, `UPDATE workflows SET active_version_id = $3, updated_at = clock_timestamp() WHERE project_id = $1 AND workflow_id = $2`, record.ProjectID, record.WorkflowID, version.ID); err != nil {
		return definition.PublishedVersion{}, err
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO workflow_definition_audits (audit_id, project_id, workflow_id, event_type, version_id, principal_id)
		VALUES ($1,$2,$3,'rollback',$4,$5)`, record.AuditID, record.ProjectID, record.WorkflowID, version.ID, record.PrincipalID); err != nil {
		return definition.PublishedVersion{}, err
	}
	return version, tx.Commit(ctx)
}

func (store *Store) CopyPublishedVersion(ctx context.Context, record definition.CopyRecord) (definition.Workflow, definition.DraftRevision, error) {
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		return definition.Workflow{}, definition.DraftRevision{}, err
	}
	defer tx.Rollback(ctx)
	result, exists, err := lockIdempotency(ctx, tx, record.ProjectID, "copy_published_version", "project", record.IdempotencyKey)
	if err != nil {
		return definition.Workflow{}, definition.DraftRevision{}, err
	}
	if exists {
		if result.requestHash != record.RequestHash {
			return definition.Workflow{}, definition.DraftRevision{}, definition.ErrIdempotencyKeyReused
		}
		if result.auxiliaryID == nil {
			return definition.Workflow{}, definition.DraftRevision{}, fmt.Errorf("copy idempotency record has no draft revision")
		}
		workflow, err := getWorkflow(ctx, tx, record.ProjectID, result.responseID, false)
		if err != nil {
			return definition.Workflow{}, definition.DraftRevision{}, err
		}
		revision, err := getDraftRevisionByID(ctx, tx, record.ProjectID, result.responseID, *result.auxiliaryID)
		return workflow, revision, err
	}
	var sourceVersionID, catalogRevision string
	var irJSON json.RawMessage
	err = tx.QueryRow(ctx, `
		SELECT v.version_id::text, s.ir_json, s.compiler_manifest_json->>'catalog_revision'
		FROM workflow_versions v
		JOIN workflow_execution_snapshots s
		  ON s.project_id = v.project_id AND s.workflow_id = v.workflow_id
		 AND s.snapshot_id = v.execution_snapshot_id
		WHERE v.project_id = $1 AND v.workflow_id = $2 AND v.version_number = $3`,
		record.ProjectID, record.SourceWorkflowID, record.SourceVersionNumber).Scan(&sourceVersionID, &irJSON, &catalogRevision)
	if errors.Is(err, pgx.ErrNoRows) {
		return definition.Workflow{}, definition.DraftRevision{}, definition.ErrResourceNotFound
	}
	if err != nil {
		return definition.Workflow{}, definition.DraftRevision{}, err
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO workflows (workflow_id, project_id, name, cloned_from_version_id, created_by)
		VALUES ($1,$2,$3,$4,$5)`, record.WorkflowID, record.ProjectID, record.Name, sourceVersionID, record.PrincipalID); err != nil {
		return definition.Workflow{}, definition.DraftRevision{}, err
	}
	if err = insertDraftRevision(ctx, tx, definition.SaveDraftRecord{
		DraftRevisionID: record.DraftRevisionID, ProjectID: record.ProjectID, WorkflowID: record.WorkflowID,
		IRJSON: irJSON, CatalogRevision: catalogRevision, PrincipalID: record.PrincipalID,
	}, 1, &sourceVersionID); err != nil {
		return definition.Workflow{}, definition.DraftRevision{}, err
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO workflow_drafts (project_id, workflow_id, current_revision, current_revision_id, state_version)
		VALUES ($1,$2,1,$3,1)`, record.ProjectID, record.WorkflowID, record.DraftRevisionID); err != nil {
		return definition.Workflow{}, definition.DraftRevision{}, err
	}
	if err = insertIdempotency(ctx, tx, record.ProjectID, "copy_published_version", "project", record.IdempotencyKey, record.RequestHash, "workflow", record.WorkflowID, &record.DraftRevisionID); err != nil {
		return definition.Workflow{}, definition.DraftRevision{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return definition.Workflow{}, definition.DraftRevision{}, err
	}
	return store.GetWorkflowAndRevision(ctx, record.ProjectID, record.WorkflowID, record.DraftRevisionID)
}

func (store *Store) ResolveProductionDefinition(ctx context.Context, projectID, workflowID string) (definition.ProductionDefinition, error) {
	workflow, err := store.GetWorkflow(ctx, projectID, workflowID)
	if err != nil {
		return definition.ProductionDefinition{}, err
	}
	if workflow.ActiveVersionID == nil {
		return definition.ProductionDefinition{}, definition.ErrWorkflowNotPublished
	}
	version, snapshot, err := getVersionAndSnapshotByID(ctx, store.pool, projectID, workflowID, *workflow.ActiveVersionID)
	if err != nil {
		return definition.ProductionDefinition{}, err
	}
	return definition.ProductionDefinition{Workflow: workflow, Version: version, Snapshot: snapshot}, nil
}

func getWorkflow(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, projectID, workflowID string, forUpdate bool) (definition.Workflow, error) {
	query := `
		SELECT workflow_id::text, project_id::text, name, active_version_id::text,
		       created_by::text, created_at, updated_at
		FROM workflows WHERE project_id = $1 AND workflow_id = $2`
	if forUpdate {
		query += " FOR UPDATE"
	}
	var result definition.Workflow
	err := queryer.QueryRow(ctx, query, projectID, workflowID).Scan(
		&result.ID, &result.ProjectID, &result.Name, &result.ActiveVersionID,
		&result.CreatedBy, &result.CreatedAt, &result.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return definition.Workflow{}, definition.ErrResourceNotFound
	}
	return result, err
}

const draftRevisionSelect = `
	SELECT draft_revision_id::text, project_id::text, workflow_id::text,
	       revision_number, ir_json, catalog_revision, created_by::text,
	       created_at, cloned_from_version_id::text
	FROM workflow_draft_revisions `

func scanDraftRevision(row scanner) (definition.DraftRevision, error) {
	var result definition.DraftRevision
	err := row.Scan(&result.ID, &result.ProjectID, &result.WorkflowID, &result.RevisionNumber,
		&result.IRJSON, &result.CatalogRevision, &result.CreatedBy, &result.CreatedAt, &result.ClonedFromVersionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return definition.DraftRevision{}, definition.ErrResourceNotFound
	}
	return result, err
}

func getDraftRevisionByID(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, projectID, workflowID, revisionID string) (definition.DraftRevision, error) {
	return scanDraftRevision(queryer.QueryRow(ctx, draftRevisionSelect+`
		WHERE project_id = $1 AND workflow_id = $2 AND draft_revision_id = $3`, projectID, workflowID, revisionID))
}

func insertDraftRevision(ctx context.Context, tx pgx.Tx, record definition.SaveDraftRecord, revision int64, clonedFrom *string) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO workflow_draft_revisions (
			draft_revision_id, project_id, workflow_id, revision_number,
			ir_json, catalog_revision, cloned_from_version_id, created_by
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		record.DraftRevisionID, record.ProjectID, record.WorkflowID, revision,
		record.IRJSON, record.CatalogRevision, clonedFrom, record.PrincipalID)
	return err
}

func upsertSnapshot(ctx context.Context, tx pgx.Tx, snapshot definition.ExecutionSnapshot) (definition.ExecutionSnapshot, error) {
	_, err := tx.Exec(ctx, `
		INSERT INTO workflow_execution_snapshots (
			snapshot_id, project_id, workflow_id, origin_kind, origin_id,
			ir_json, dsl_json, source_map_json, compiler_manifest_json,
			ir_hash, dsl_hash, source_map_hash, definition_hash
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
		ON CONFLICT (project_id, workflow_id, definition_hash) DO NOTHING`,
		snapshot.ID, snapshot.ProjectID, snapshot.WorkflowID, snapshot.OriginKind, snapshot.OriginID,
		snapshot.IRJSON, snapshot.DSLJSON, snapshot.SourceMapJSON, snapshot.ManifestJSON,
		snapshot.IRHash, snapshot.DSLHash, snapshot.SourceMapHash, snapshot.DefinitionHash)
	if err != nil {
		return definition.ExecutionSnapshot{}, err
	}
	stored, err := scanSnapshot(tx.QueryRow(ctx, snapshotSelect+`
		WHERE project_id = $1 AND workflow_id = $2 AND definition_hash = $3
		  AND ir_hash = $4 AND dsl_hash = $5 AND source_map_hash = $6
		  AND compiler_manifest_json = $7::jsonb`, snapshot.ProjectID, snapshot.WorkflowID,
		snapshot.DefinitionHash, snapshot.IRHash, snapshot.DSLHash, snapshot.SourceMapHash, snapshot.ManifestJSON))
	if errors.Is(err, definition.ErrResourceNotFound) {
		return definition.ExecutionSnapshot{}, fmt.Errorf("definition hash collision or compiler manifest mismatch")
	}
	return stored, err
}

const snapshotSelect = `
	SELECT snapshot_id::text, project_id::text, workflow_id::text, origin_kind,
	       origin_id::text, ir_json, dsl_json, source_map_json, compiler_manifest_json,
	       ir_hash, dsl_hash, source_map_hash, definition_hash, created_at
	FROM workflow_execution_snapshots `

func scanSnapshot(row scanner) (definition.ExecutionSnapshot, error) {
	var result definition.ExecutionSnapshot
	err := row.Scan(&result.ID, &result.ProjectID, &result.WorkflowID, &result.OriginKind,
		&result.OriginID, &result.IRJSON, &result.DSLJSON, &result.SourceMapJSON, &result.ManifestJSON,
		&result.IRHash, &result.DSLHash, &result.SourceMapHash, &result.DefinitionHash, &result.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return definition.ExecutionSnapshot{}, definition.ErrResourceNotFound
	}
	return result, err
}

const versionSelect = `
	SELECT version_id::text, project_id::text, workflow_id::text, version_number,
	       source_draft_revision_id::text, execution_snapshot_id::text,
	       change_log, created_by::text, created_at
	FROM workflow_versions `

func scanVersion(row scanner) (definition.PublishedVersion, error) {
	var result definition.PublishedVersion
	err := row.Scan(&result.ID, &result.ProjectID, &result.WorkflowID, &result.VersionNumber,
		&result.SourceDraftRevisionID, &result.ExecutionSnapshotID, &result.ChangeLog,
		&result.CreatedBy, &result.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return definition.PublishedVersion{}, definition.ErrResourceNotFound
	}
	return result, err
}

func getVersionAndSnapshotByID(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, projectID, workflowID, versionID string) (definition.PublishedVersion, definition.ExecutionSnapshot, error) {
	version, err := scanVersion(queryer.QueryRow(ctx, versionSelect+`
		WHERE project_id = $1 AND workflow_id = $2 AND version_id = $3`, projectID, workflowID, versionID))
	if err != nil {
		return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, err
	}
	snapshot, err := scanSnapshot(queryer.QueryRow(ctx, snapshotSelect+`
		WHERE project_id = $1 AND workflow_id = $2 AND snapshot_id = $3`, projectID, workflowID, version.ExecutionSnapshotID))
	return version, snapshot, err
}

func lockIdempotency(ctx context.Context, tx pgx.Tx, projectID, commandName, targetScope, key string) (idempotencyResult, bool, error) {
	lockDigest := sha256.Sum256([]byte(projectID + "\x00" + commandName + "\x00" + targetScope + "\x00" + key))
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, fmt.Sprintf("%x", lockDigest)); err != nil {
		return idempotencyResult{}, false, err
	}
	var result idempotencyResult
	err := tx.QueryRow(ctx, `
		SELECT request_hash, response_id::text, response_aux_id::text
		FROM definition_idempotency
		WHERE project_id = $1 AND command_name = $2 AND target_scope = $3 AND idempotency_key = $4`,
		projectID, commandName, targetScope, key).Scan(&result.requestHash, &result.responseID, &result.auxiliaryID)
	if errors.Is(err, pgx.ErrNoRows) {
		return idempotencyResult{}, false, nil
	}
	return result, err == nil, err
}

func insertIdempotency(ctx context.Context, tx pgx.Tx, projectID, commandName, targetScope, key, requestHash, responseKind, responseID string, auxiliaryID *string) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO definition_idempotency (
			project_id, command_name, target_scope, idempotency_key,
			request_hash, response_kind, response_id, response_aux_id
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		projectID, commandName, targetScope, key, requestHash, responseKind, responseID, auxiliaryID)
	return err
}
