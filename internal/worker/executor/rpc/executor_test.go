package rpc

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/resources"
	platformruntime "github.com/uu999/evalfrog/internal/runtime"
	runtimecontext "github.com/uu999/evalfrog/internal/runtime/context"
)

type resolver struct{}

func (resolver) ResolveConnection(context.Context, resources.RuntimeResolveCommand) (resources.ConnectionRuntime, error) {
	panic("unused")
}
func (resolver) ResolveServiceOperation(_ context.Context, command resources.RuntimeResolveCommand) (resources.ServiceOperationRuntime, error) {
	if command.ContractRevision != "contract-1" {
		return resources.ServiceOperationRuntime{}, resources.ErrResourceNotFound
	}
	return resources.ServiceOperationRuntime{ServiceOperation: resources.ServiceOperation{ServiceID: "svc", Operation: "Create", ContractRevision: "contract-1", Idempotent: true}}, nil
}

type invoker struct{ key string }

func (value *invoker) Invoke(_ context.Context, _ resources.ServiceOperationRuntime, request json.RawMessage, key string) (json.RawMessage, error) {
	value.key = key
	return request, nil
}

func TestExecutorResolvesRegisteredOperationAndStableIdempotencyKey(t *testing.T) {
	call := &invoker{}
	executor := NewExecutor(resolver{}, call)
	value := runtimecontext.ExecutionContext{ProjectID: "p", RunID: "run", ExecutionNodeID: "node", AttemptID: "a", AttemptSequence: 1, Operation: dsl.Operation{Type: "task.rpc", Version: 1, Config: map[string]json.RawMessage{"service_id": json.RawMessage(`"svc"`), "operation": json.RawMessage(`"Create"`), "contract_revision": json.RawMessage(`"contract-1"`)}}, Inputs: map[string]json.RawMessage{"request": json.RawMessage(`{"id":1}`)}}
	result := executor.Execute(context.Background(), value)
	if result.State != platformruntime.AttemptSucceeded || call.key != "run:node" {
		t.Fatalf("result=%+v key=%q", result, call.key)
	}
}

func TestExecutorRejectsContractMismatch(t *testing.T) {
	executor := NewExecutor(resolver{}, &invoker{})
	value := runtimecontext.ExecutionContext{Operation: dsl.Operation{Type: "task.rpc", Version: 1, Config: map[string]json.RawMessage{"service_id": json.RawMessage(`"svc"`), "operation": json.RawMessage(`"Create"`), "contract_revision": json.RawMessage(`"old"`)}}}
	if result := executor.Execute(context.Background(), value); result.ErrorCode != "RPC_FORBIDDEN" {
		t.Fatalf("result=%+v", result)
	}
}
