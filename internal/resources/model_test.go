package resources

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/uu999/evalfrog/internal/access"
	"github.com/uu999/evalfrog/internal/ir"
)

type authorizerStub struct {
	permissions map[access.Permission]bool
}

func (stub authorizerStub) Require(_ context.Context, _, _ string, permission access.Permission) error {
	if !stub.permissions[permission] {
		return access.ErrPermissionDenied
	}
	return nil
}

func (authorizerStub) ExecutionIdentity(_ context.Context, projectID string) (access.ExecutionIdentity, error) {
	return access.ExecutionIdentity{ID: "execution-1", ProjectID: projectID}, nil
}

type repositoryStub struct{}

func (repositoryStub) ResolveConnection(_ context.Context, projectID, _, reference string, _ Purpose) (Connection, error) {
	if projectID != "project-a" || reference != "customer_api" {
		return Connection{}, ErrResourceNotFound
	}
	return Connection{ID: "connection-1", ProjectID: projectID, Reference: reference}, nil
}

func (repositoryStub) ResolveServiceOperation(_ context.Context, projectID, _, reference, operation string, _ Purpose) (ServiceOperation, error) {
	if projectID != "project-a" || reference != "orders" || operation != "Create" {
		return ServiceOperation{}, ErrResourceNotFound
	}
	return ServiceOperation{ServiceID: "service-1", ProjectID: projectID, ServiceReference: reference, Operation: operation, ContractRevision: "contract-3", Idempotent: true}, nil
}

func TestResolverPerformsAuthorAndExecutionIdentityAuthorization(t *testing.T) {
	document := ir.Document{Nodes: []ir.Node{
		{ID: "http", Type: "http", Inputs: []ir.Input{{Name: "connection_ref", DataType: ir.TypeString, Source: ir.SourceLiteral, Value: json.RawMessage(`"customer_api"`)}}},
		{ID: "rpc", Type: "rpc", Inputs: []ir.Input{{Name: "service_ref", DataType: ir.TypeString, Source: ir.SourceLiteral, Value: json.RawMessage(`"orders"`)}, {Name: "operation", DataType: ir.TypeString, Source: ir.SourceLiteral, Value: json.RawMessage(`"Create"`)}}},
	}}
	resolver := NewResolver(repositoryStub{}, authorizerStub{permissions: map[access.Permission]bool{access.PermissionConnectionUse: true, access.PermissionServiceUse: true}})
	bindings, diagnostics, err := resolver.Resolve(context.Background(), "project-a", "principal-a", PurposeProduction, document)
	if err != nil || len(diagnostics) != 0 {
		t.Fatalf("resolve failed: diagnostics=%+v err=%v", diagnostics, err)
	}
	if connection, ok := bindings.Connection("customer_api"); !ok || connection.ConnectionID != "connection-1" {
		t.Fatalf("unexpected connection binding: %+v %v", connection, ok)
	}
	if service, ok := bindings.ServiceOperation("orders", "Create"); !ok || service.ContractRevision != "contract-3" || !service.Idempotent {
		t.Fatalf("unexpected service binding: %+v %v", service, ok)
	}
}

func TestResolverDoesNotLeakInvisibleResources(t *testing.T) {
	document := ir.Document{Nodes: []ir.Node{{ID: "http", Type: "http", Inputs: []ir.Input{{Name: "connection_ref", DataType: ir.TypeString, Source: ir.SourceLiteral, Value: json.RawMessage(`"other_project"`)}}}}}
	resolver := NewResolver(repositoryStub{}, authorizerStub{permissions: map[access.Permission]bool{access.PermissionConnectionUse: true}})
	_, diagnostics, err := resolver.Resolve(context.Background(), "project-a", "principal-a", PurposeTest, document)
	if err != nil || len(diagnostics) != 1 || diagnostics[0].Code != "RESOURCE_NOT_FOUND" {
		t.Fatalf("unexpected result: diagnostics=%+v err=%v", diagnostics, err)
	}
	if diagnostics[0].Message == "other_project" {
		t.Fatal("diagnostic leaked the invisible reference")
	}

	denied := NewResolver(repositoryStub{}, authorizerStub{permissions: map[access.Permission]bool{}})
	if _, _, err := denied.Resolve(context.Background(), "project-a", "principal-a", PurposeTest, document); !errors.Is(err, access.ErrPermissionDenied) {
		t.Fatalf("missing author permission error = %v", err)
	}
}
