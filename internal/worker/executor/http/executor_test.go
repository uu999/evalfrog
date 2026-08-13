package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/resources"
	platformruntime "github.com/uu999/evalfrog/internal/runtime"
	runtimecontext "github.com/uu999/evalfrog/internal/runtime/context"
)

type resolverStub struct{ connection resources.ConnectionRuntime }

func (stub resolverStub) ResolveConnection(context.Context, resources.RuntimeResolveCommand) (resources.ConnectionRuntime, error) {
	return stub.connection, nil
}
func (resolverStub) ResolveServiceOperation(context.Context, resources.RuntimeResolveCommand) (resources.ServiceOperationRuntime, error) {
	panic("unused")
}

func httpContext(inputs map[string]json.RawMessage) runtimecontext.ExecutionContext {
	return runtimecontext.ExecutionContext{ProjectID: "project", RunID: "run", AttemptID: "attempt", AttemptSequence: 1, ExecutionNodeID: "node", LeaseToken: "lease", FencingToken: 1, Operation: dsl.Operation{Type: "task.http", Version: 1, Config: map[string]json.RawMessage{"connection_id": json.RawMessage(`"conn"`), "method": json.RawMessage(`"GET"`), "accepted_statuses": json.RawMessage(`[200]`)}}, Inputs: inputs}
}

func TestExecutorUsesManagedOriginAndInjectsTransientCredential(t *testing.T) {
	var received *http.Request
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		received = request
		if request.URL.Path != "/api/v1/orders" || request.URL.Query().Get("page") != "2" {
			t.Errorf("request target=%s", request.URL.String())
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()
	executor := NewExecutor(resolverStub{connection: resources.ConnectionRuntime{ID: "conn", BaseURL: server.URL + "/api", SecretHeaders: map[string]string{"Authorization": "Bearer secret"}}}, nil)
	result := executor.Execute(context.Background(), httpContext(map[string]json.RawMessage{"relative_path": json.RawMessage(`"/v1/orders"`), "query": json.RawMessage(`{"page":2}`)}))
	if result.State != platformruntime.AttemptSucceeded || received == nil {
		t.Fatalf("result=%+v", result)
	}
	if received.Header.Get("Authorization") != "Bearer secret" || received.Header.Get("Idempotency-Key") != "run:node" {
		t.Fatalf("headers=%v", received.Header)
	}
	if strings.Contains(result.Message, "secret") {
		t.Fatal("secret leaked in result")
	}
}

func TestExecutorRejectsAbsolutePathAndProtectedHeader(t *testing.T) {
	executor := NewExecutor(resolverStub{connection: resources.ConnectionRuntime{ID: "conn", BaseURL: "https://api.example"}}, nil)
	for name, inputs := range map[string]map[string]json.RawMessage{
		"absolute": {"relative_path": json.RawMessage(`"https://evil.example"`)},
		"host":     {"relative_path": json.RawMessage(`"/ok"`), "headers": json.RawMessage(`{"Host":"evil.example"}`)},
	} {
		t.Run(name, func(t *testing.T) {
			result := executor.Execute(context.Background(), httpContext(inputs))
			if result.State != platformruntime.AttemptFailed {
				t.Fatalf("result=%+v", result)
			}
		})
	}
}
