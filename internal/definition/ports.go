package definition

import (
	"context"

	"github.com/uu999/evalfrog/internal/access"
	"github.com/uu999/evalfrog/internal/compiler"
	"github.com/uu999/evalfrog/internal/ir"
	"github.com/uu999/evalfrog/internal/resources"
)

type AccessControl interface {
	Require(context.Context, string, string, access.Permission) error
}

type ResourceResolver interface {
	Resolve(context.Context, string, string, resources.Purpose, ir.Document) (compiler.ResourceBindings, []ir.Diagnostic, error)
}

type Compiler interface {
	Compile(compiler.Request) (compiler.Result, []ir.Diagnostic)
}

type Repository interface {
	CreateWorkflow(context.Context, CreateWorkflowRecord) (Workflow, DraftRevision, error)
	GetWorkflow(context.Context, string, string) (Workflow, error)
	GetCurrentDraft(context.Context, string, string) (Draft, error)
	GetDraftRevision(context.Context, string, string, int64) (DraftRevision, error)
	SaveDraft(context.Context, SaveDraftRecord) (DraftRevision, error)
	StoreTestSnapshot(context.Context, ExecutionSnapshot) (ExecutionSnapshot, error)
	FindPublishedByIdempotency(context.Context, string, string, string, string) (PublishedVersion, ExecutionSnapshot, bool, error)
	Publish(context.Context, PublishRecord) (PublishedVersion, ExecutionSnapshot, error)
	Rollback(context.Context, RollbackRecord) (PublishedVersion, error)
	CopyPublishedVersion(context.Context, CopyRecord) (Workflow, DraftRevision, error)
	ResolveProductionDefinition(context.Context, string, string) (ProductionDefinition, error)
}

type CreateWorkflowRecord struct {
	WorkflowID      string
	DraftRevisionID string
	ProjectID       string
	Name            string
	IRJSON          []byte
	CatalogRevision string
	PrincipalID     string
	IdempotencyKey  string
	RequestHash     string
}

type SaveDraftRecord struct {
	DraftRevisionID  string
	ProjectID        string
	WorkflowID       string
	ExpectedRevision int64
	IRJSON           []byte
	CatalogRevision  string
	PrincipalID      string
	IdempotencyKey   string
	RequestHash      string
}

type PublishRecord struct {
	VersionID        string
	AuditID          string
	ProjectID        string
	WorkflowID       string
	ExpectedRevision int64
	DraftRevisionID  string
	ChangeLog        string
	PrincipalID      string
	IdempotencyKey   string
	RequestHash      string
	Snapshot         ExecutionSnapshot
}

type RollbackRecord struct {
	AuditID       string
	ProjectID     string
	WorkflowID    string
	VersionNumber int64
	PrincipalID   string
}

type CopyRecord struct {
	WorkflowID          string
	DraftRevisionID     string
	ProjectID           string
	SourceWorkflowID    string
	SourceVersionNumber int64
	Name                string
	PrincipalID         string
	IdempotencyKey      string
	RequestHash         string
}
