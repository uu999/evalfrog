// Package httpapi adapts the versioned HTTP/JSON command surface to Definition
// application commands. It never accesses PostgreSQL directly.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/uu999/evalfrog/internal/access"
	"github.com/uu999/evalfrog/internal/definition"
	"github.com/uu999/evalfrog/internal/ir"
	"github.com/uu999/evalfrog/internal/platform/identity"
	"github.com/uu999/evalfrog/internal/platform/traceid"
)

const maxRequestBytes = 2 << 20

type Authenticator interface {
	Authenticate(context.Context, string) (access.Principal, error)
}

type DefinitionApplication interface {
	CreateWorkflow(context.Context, definition.CreateWorkflowCommand) (definition.Workflow, definition.DraftRevision, []ir.Diagnostic, error)
	GetWorkflow(context.Context, string, string, string) (definition.Workflow, error)
	GetDraft(context.Context, string, string, string) (definition.Draft, error)
	SaveDraft(context.Context, definition.SaveDraftCommand) (definition.DraftRevision, []ir.Diagnostic, error)
	ValidateDraft(context.Context, string, string, string, int64) ([]ir.Diagnostic, error)
	CompileDraftTestSnapshot(context.Context, string, string, string, int64) (definition.ExecutionSnapshot, []ir.Diagnostic, error)
	Publish(context.Context, definition.PublishCommand) (definition.PublishedVersion, definition.ExecutionSnapshot, []ir.Diagnostic, error)
	Rollback(context.Context, string, string, string, int64) (definition.PublishedVersion, error)
	CopyPublishedVersion(context.Context, definition.CopyCommand) (definition.Workflow, definition.DraftRevision, error)
}

type Handler struct {
	authenticator Authenticator
	definition    DefinitionApplication
	ids           identity.Generator
	router        *http.ServeMux
}

func New(authenticator Authenticator, application DefinitionApplication) *Handler {
	handler := &Handler{authenticator: authenticator, definition: application, ids: identity.UUIDv7Generator{}, router: http.NewServeMux()}
	handler.router.HandleFunc("POST /v1/projects/{project_id}/workflows", handler.createWorkflow)
	handler.router.HandleFunc("POST /v1/projects/{project_id}/workflows:copy", handler.copyWorkflow)
	handler.router.HandleFunc("GET /v1/projects/{project_id}/workflows/{workflow_id}", handler.getWorkflow)
	handler.router.HandleFunc("GET /v1/projects/{project_id}/workflows/{workflow_id}/draft", handler.getDraft)
	handler.router.HandleFunc("PUT /v1/projects/{project_id}/workflows/{workflow_id}/draft", handler.saveDraft)
	handler.router.HandleFunc("POST /v1/projects/{project_id}/workflows/{workflow_id}/draft/validate", handler.validateDraft)
	handler.router.HandleFunc("POST /v1/projects/{project_id}/workflows/{workflow_id}/draft/test-snapshots", handler.compileTestSnapshot)
	handler.router.HandleFunc("POST /v1/projects/{project_id}/workflows/{workflow_id}/publish", handler.publish)
	handler.router.HandleFunc("POST /v1/projects/{project_id}/workflows/{workflow_id}/versions/{version_number}/activate", handler.rollback)
	return handler
}

func (handler *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	handler.router.ServeHTTP(writer, request)
}

type createWorkflowRequest struct {
	Name string          `json:"name"`
	IR   json.RawMessage `json:"ir"`
}

func (handler *Handler) createWorkflow(writer http.ResponseWriter, request *http.Request) {
	principal, ok := handler.authenticate(writer, request)
	if !ok {
		return
	}
	var body createWorkflowRequest
	if !handler.decode(writer, request, &body) {
		return
	}
	workflow, revision, diagnostics, err := handler.definition.CreateWorkflow(request.Context(), definition.CreateWorkflowCommand{
		ProjectID: request.PathValue("project_id"), PrincipalID: principal.ID, Name: body.Name,
		IRJSON: body.IR, IdempotencyKey: request.Header.Get("Idempotency-Key"),
	})
	if handler.writeResultError(writer, request, diagnostics, err) {
		return
	}
	writeJSON(writer, http.StatusCreated, map[string]any{"workflow": workflow, "draft_revision": revision})
}

func (handler *Handler) copyWorkflow(writer http.ResponseWriter, request *http.Request) {
	principal, ok := handler.authenticate(writer, request)
	if !ok {
		return
	}
	var body struct {
		SourceWorkflowID    string `json:"source_workflow_id"`
		SourceVersionNumber int64  `json:"source_version_number"`
		Name                string `json:"name"`
	}
	if !handler.decode(writer, request, &body) {
		return
	}
	workflow, revision, err := handler.definition.CopyPublishedVersion(request.Context(), definition.CopyCommand{
		ProjectID: request.PathValue("project_id"), PrincipalID: principal.ID,
		SourceWorkflowID: body.SourceWorkflowID, SourceVersionNumber: body.SourceVersionNumber,
		Name: body.Name, IdempotencyKey: request.Header.Get("Idempotency-Key"),
	})
	if handler.writeResultError(writer, request, nil, err) {
		return
	}
	writeJSON(writer, http.StatusCreated, map[string]any{"workflow": workflow, "draft_revision": revision})
}

func (handler *Handler) getWorkflow(writer http.ResponseWriter, request *http.Request) {
	principal, ok := handler.authenticate(writer, request)
	if !ok {
		return
	}
	workflow, err := handler.definition.GetWorkflow(request.Context(), request.PathValue("project_id"), principal.ID, request.PathValue("workflow_id"))
	if handler.writeResultError(writer, request, nil, err) {
		return
	}
	writeJSON(writer, http.StatusOK, workflow)
}

func (handler *Handler) getDraft(writer http.ResponseWriter, request *http.Request) {
	principal, ok := handler.authenticate(writer, request)
	if !ok {
		return
	}
	draft, err := handler.definition.GetDraft(request.Context(), request.PathValue("project_id"), principal.ID, request.PathValue("workflow_id"))
	if handler.writeResultError(writer, request, nil, err) {
		return
	}
	writeJSON(writer, http.StatusOK, draft)
}

func (handler *Handler) saveDraft(writer http.ResponseWriter, request *http.Request) {
	principal, ok := handler.authenticate(writer, request)
	if !ok {
		return
	}
	var body struct {
		ExpectedRevision int64           `json:"expected_revision"`
		IR               json.RawMessage `json:"ir"`
	}
	if !handler.decode(writer, request, &body) {
		return
	}
	revision, diagnostics, err := handler.definition.SaveDraft(request.Context(), definition.SaveDraftCommand{
		ProjectID: request.PathValue("project_id"), PrincipalID: principal.ID, WorkflowID: request.PathValue("workflow_id"),
		ExpectedRevision: body.ExpectedRevision, IRJSON: body.IR, IdempotencyKey: request.Header.Get("Idempotency-Key"),
	})
	if handler.writeResultError(writer, request, diagnostics, err) {
		return
	}
	writeJSON(writer, http.StatusCreated, revision)
}

func (handler *Handler) validateDraft(writer http.ResponseWriter, request *http.Request) {
	principal, revision, ok := handler.principalAndRevision(writer, request)
	if !ok {
		return
	}
	diagnostics, err := handler.definition.ValidateDraft(request.Context(), request.PathValue("project_id"), principal.ID, request.PathValue("workflow_id"), revision)
	if handler.writeResultError(writer, request, nil, err) {
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"valid": !ir.HasErrors(diagnostics), "diagnostics": diagnostics})
}

func (handler *Handler) compileTestSnapshot(writer http.ResponseWriter, request *http.Request) {
	principal, revision, ok := handler.principalAndRevision(writer, request)
	if !ok {
		return
	}
	snapshot, diagnostics, err := handler.definition.CompileDraftTestSnapshot(request.Context(), request.PathValue("project_id"), principal.ID, request.PathValue("workflow_id"), revision)
	if handler.writeResultError(writer, request, diagnostics, err) {
		return
	}
	writeJSON(writer, http.StatusCreated, snapshot)
}

func (handler *Handler) publish(writer http.ResponseWriter, request *http.Request) {
	principal, ok := handler.authenticate(writer, request)
	if !ok {
		return
	}
	var body struct {
		ExpectedRevision int64  `json:"expected_revision"`
		ChangeLog        string `json:"change_log"`
	}
	if !handler.decode(writer, request, &body) {
		return
	}
	version, snapshot, diagnostics, err := handler.definition.Publish(request.Context(), definition.PublishCommand{
		ProjectID: request.PathValue("project_id"), PrincipalID: principal.ID, WorkflowID: request.PathValue("workflow_id"),
		ExpectedRevision: body.ExpectedRevision, ChangeLog: body.ChangeLog, IdempotencyKey: request.Header.Get("Idempotency-Key"),
	})
	if handler.writeResultError(writer, request, diagnostics, err) {
		return
	}
	writeJSON(writer, http.StatusCreated, map[string]any{"version": version, "execution_snapshot": snapshot})
}

func (handler *Handler) rollback(writer http.ResponseWriter, request *http.Request) {
	principal, ok := handler.authenticate(writer, request)
	if !ok {
		return
	}
	versionNumber, err := strconv.ParseInt(request.PathValue("version_number"), 10, 64)
	if err != nil || versionNumber < 1 {
		handler.writeError(writer, request, http.StatusBadRequest, definition.CodeInvalidArgument, "version_number must be positive", nil)
		return
	}
	version, err := handler.definition.Rollback(request.Context(), request.PathValue("project_id"), principal.ID, request.PathValue("workflow_id"), versionNumber)
	if handler.writeResultError(writer, request, nil, err) {
		return
	}
	writeJSON(writer, http.StatusOK, version)
}

func (handler *Handler) principalAndRevision(writer http.ResponseWriter, request *http.Request) (access.Principal, int64, bool) {
	principal, ok := handler.authenticate(writer, request)
	if !ok {
		return access.Principal{}, 0, false
	}
	var body struct {
		Revision int64 `json:"revision"`
	}
	if !handler.decode(writer, request, &body) {
		return access.Principal{}, 0, false
	}
	if body.Revision < 1 {
		handler.writeError(writer, request, http.StatusBadRequest, definition.CodeInvalidArgument, "revision must be positive", nil)
		return access.Principal{}, 0, false
	}
	return principal, body.Revision, true
}

func (handler *Handler) authenticate(writer http.ResponseWriter, request *http.Request) (access.Principal, bool) {
	header := request.Header.Get("Authorization")
	if !strings.HasPrefix(header, "Bearer ") {
		handler.writeError(writer, request, http.StatusUnauthorized, "UNAUTHENTICATED", "bearer credential is required", nil)
		return access.Principal{}, false
	}
	principal, err := handler.authenticator.Authenticate(request.Context(), strings.TrimPrefix(header, "Bearer "))
	if err != nil {
		status, code := http.StatusInternalServerError, "INTERNAL_ERROR"
		if errors.Is(err, access.ErrUnauthenticated) {
			status, code = http.StatusUnauthorized, "UNAUTHENTICATED"
		}
		handler.writeError(writer, request, status, code, "authentication failed", nil)
		return access.Principal{}, false
	}
	return principal, true
}

func (handler *Handler) decode(writer http.ResponseWriter, request *http.Request, target any) bool {
	reader := http.MaxBytesReader(writer, request.Body, maxRequestBytes)
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		handler.writeError(writer, request, http.StatusBadRequest, definition.CodeInvalidArgument, "request JSON is invalid", nil)
		return false
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		handler.writeError(writer, request, http.StatusBadRequest, definition.CodeInvalidArgument, "request must contain one JSON object", nil)
		return false
	}
	return true
}

func (handler *Handler) writeResultError(writer http.ResponseWriter, request *http.Request, diagnostics []ir.Diagnostic, err error) bool {
	if err == nil && !ir.HasErrors(diagnostics) {
		return false
	}
	if err == nil {
		handler.writeError(writer, request, http.StatusUnprocessableEntity, "VALIDATION_FAILED", "workflow definition is invalid", map[string]any{"diagnostics": diagnostics})
		return true
	}
	status, code, message, details := http.StatusInternalServerError, "INTERNAL_ERROR", "internal error", map[string]any(nil)
	var domainError *definition.Error
	if errors.As(err, &domainError) {
		code, message, details = domainError.Code, domainError.Message, domainError.Details
		switch code {
		case definition.CodeInvalidArgument:
			status = http.StatusBadRequest
		case definition.CodePermissionDenied:
			status = http.StatusForbidden
		case definition.CodeResourceNotFound:
			status = http.StatusNotFound
		case definition.CodeDraftRevisionConflict, definition.CodeIdempotencyKeyReused, definition.CodeWorkflowNotPublished:
			status = http.StatusConflict
		case definition.CodeCatalogUnavailable:
			status = http.StatusUnprocessableEntity
		}
	}
	handler.writeError(writer, request, status, code, message, details)
	return true
}

func (handler *Handler) writeError(writer http.ResponseWriter, request *http.Request, status int, code, message string, details map[string]any) {
	requestID, err := handler.ids.New()
	if err != nil {
		requestID = traceid.From(request.Context())
	}
	writeJSON(writer, status, map[string]any{"error": map[string]any{
		"code": code, "message": message, "retryable": status >= 500,
		"request_id": requestID, "trace_id": traceid.From(request.Context()), "details": details,
	}})
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
