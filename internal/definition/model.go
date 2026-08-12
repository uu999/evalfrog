// Package definition owns Workflow, immutable Draft Revision, Published
// Version, and Execution Snapshot lifecycles. Runtime consumes only Snapshot.
package definition

import (
	"encoding/json"
	"time"
)

type Workflow struct {
	ID              string    `json:"workflow_id"`
	ProjectID       string    `json:"project_id"`
	Name            string    `json:"name"`
	ActiveVersionID *string   `json:"active_version_id,omitempty"`
	CreatedBy       string    `json:"created_by"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

type Draft struct {
	ProjectID       string        `json:"project_id"`
	WorkflowID      string        `json:"workflow_id"`
	CurrentRevision int64         `json:"current_revision"`
	StateVersion    int64         `json:"state_version"`
	Current         DraftRevision `json:"current"`
}

type DraftRevision struct {
	ID                  string          `json:"draft_revision_id"`
	ProjectID           string          `json:"project_id"`
	WorkflowID          string          `json:"workflow_id"`
	RevisionNumber      int64           `json:"revision_number"`
	IRJSON              json.RawMessage `json:"ir"`
	CatalogRevision     string          `json:"catalog_revision"`
	CreatedBy           string          `json:"created_by"`
	CreatedAt           time.Time       `json:"created_at"`
	ClonedFromVersionID *string         `json:"cloned_from_version_id,omitempty"`
}

type SnapshotOrigin string

const (
	SnapshotOriginDraftTest SnapshotOrigin = "draft_test"
	SnapshotOriginPublished SnapshotOrigin = "published"
)

type ExecutionSnapshot struct {
	ID             string          `json:"snapshot_id"`
	ProjectID      string          `json:"project_id"`
	WorkflowID     string          `json:"workflow_id"`
	OriginKind     SnapshotOrigin  `json:"origin_kind"`
	OriginID       string          `json:"origin_id"`
	IRJSON         json.RawMessage `json:"ir"`
	DSLJSON        json.RawMessage `json:"dsl"`
	SourceMapJSON  json.RawMessage `json:"source_map"`
	ManifestJSON   json.RawMessage `json:"compiler_manifest"`
	IRHash         string          `json:"ir_hash"`
	DSLHash        string          `json:"dsl_hash"`
	SourceMapHash  string          `json:"source_map_hash"`
	DefinitionHash string          `json:"definition_hash"`
	CreatedAt      time.Time       `json:"created_at"`
}

type PublishedVersion struct {
	ID                    string    `json:"version_id"`
	ProjectID             string    `json:"project_id"`
	WorkflowID            string    `json:"workflow_id"`
	VersionNumber         int64     `json:"version_number"`
	SourceDraftRevisionID string    `json:"source_draft_revision_id"`
	ExecutionSnapshotID   string    `json:"execution_snapshot_id"`
	ChangeLog             string    `json:"change_log"`
	CreatedBy             string    `json:"created_by"`
	CreatedAt             time.Time `json:"created_at"`
}

type ProductionDefinition struct {
	Workflow Workflow          `json:"workflow"`
	Version  PublishedVersion  `json:"version"`
	Snapshot ExecutionSnapshot `json:"snapshot"`
}
