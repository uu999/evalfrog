package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/uu999/evalfrog/internal/access"
	"github.com/uu999/evalfrog/internal/definition"
	"github.com/uu999/evalfrog/internal/ir"
)

type authenticatorStub struct{}

func (authenticatorStub) Authenticate(_ context.Context, token string) (access.Principal, error) {
	if token != "0123456789abcdef" {
		return access.Principal{}, access.ErrUnauthenticated
	}
	return access.Principal{ID: "principal-1", Kind: access.PrincipalServiceAccount}, nil
}

type applicationStub struct {
	createCalled bool
	getError     error
}

func (stub *applicationStub) CreateWorkflow(context.Context, definition.CreateWorkflowCommand) (definition.Workflow, definition.DraftRevision, []ir.Diagnostic, error) {
	stub.createCalled = true
	return definition.Workflow{ID: "workflow-1"}, definition.DraftRevision{RevisionNumber: 1}, nil, nil
}
func (stub *applicationStub) GetWorkflow(context.Context, string, string, string) (definition.Workflow, error) {
	return definition.Workflow{}, stub.getError
}
func (*applicationStub) GetDraft(context.Context, string, string, string) (definition.Draft, error) {
	return definition.Draft{}, nil
}
func (*applicationStub) SaveDraft(context.Context, definition.SaveDraftCommand) (definition.DraftRevision, []ir.Diagnostic, error) {
	return definition.DraftRevision{}, nil, nil
}
func (*applicationStub) ValidateDraft(context.Context, string, string, string, int64) ([]ir.Diagnostic, error) {
	return nil, nil
}
func (*applicationStub) CompileDraftTestSnapshot(context.Context, string, string, string, int64) (definition.ExecutionSnapshot, []ir.Diagnostic, error) {
	return definition.ExecutionSnapshot{}, nil, nil
}
func (*applicationStub) Publish(context.Context, definition.PublishCommand) (definition.PublishedVersion, definition.ExecutionSnapshot, []ir.Diagnostic, error) {
	return definition.PublishedVersion{}, definition.ExecutionSnapshot{}, nil, nil
}
func (*applicationStub) Rollback(context.Context, string, string, string, int64) (definition.PublishedVersion, error) {
	return definition.PublishedVersion{}, nil
}
func (*applicationStub) CopyPublishedVersion(context.Context, definition.CopyCommand) (definition.Workflow, definition.DraftRevision, error) {
	return definition.Workflow{}, definition.DraftRevision{}, nil
}

func TestAuthoringAPIRejectsClientDSLAndSourceMap(t *testing.T) {
	for _, forbidden := range []string{"dsl", "source_map"} {
		application := &applicationStub{}
		handler := New(authenticatorStub{}, application)
		body := `{"name":"frog","ir":{},"` + forbidden + `":{}}`
		request := httptest.NewRequest(http.MethodPost, "/v1/projects/project-1/workflows", strings.NewReader(body))
		request.Header.Set("Authorization", "Bearer 0123456789abcdef")
		request.Header.Set("Idempotency-Key", "create-frog-0001")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest || application.createCalled {
			t.Fatalf("field %s status=%d create_called=%v body=%s", forbidden, response.Code, application.createCalled, response.Body.String())
		}
	}
}

func TestThereIsNoDSLUploadRoute(t *testing.T) {
	handler := New(authenticatorStub{}, &applicationStub{})
	request := httptest.NewRequest(http.MethodPost, "/v1/projects/project-1/workflows/workflow-1/dsl", strings.NewReader(`{}`))
	request.Header.Set("Authorization", "Bearer 0123456789abcdef")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("DSL upload route status=%d", response.Code)
	}
}

func TestDomainErrorsUseStableHTTPContract(t *testing.T) {
	application := &applicationStub{getError: &definition.Error{Code: definition.CodeDraftRevisionConflict, Message: "draft revision has changed", Cause: definition.ErrDraftRevisionConflict}}
	handler := New(authenticatorStub{}, application)
	request := httptest.NewRequest(http.MethodGet, "/v1/projects/project-1/workflows/workflow-1", nil)
	request.Header.Set("Authorization", "Bearer 0123456789abcdef")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusConflict {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var envelope struct {
		Error struct {
			Code      string `json:"code"`
			RequestID string `json:"request_id"`
		} `json:"error"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Error.Code != definition.CodeDraftRevisionConflict || envelope.Error.RequestID == "" {
		t.Fatalf("unexpected error envelope: %+v", envelope)
	}
}

func TestAuthenticationFailsClosed(t *testing.T) {
	handler := New(authenticatorStub{}, &applicationStub{})
	request := httptest.NewRequest(http.MethodGet, "/v1/projects/project-1/workflows/workflow-1", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}
