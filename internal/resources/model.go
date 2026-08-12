// Package resources owns project-scoped managed resource metadata and turns
// author references into stable compiler bindings without exposing endpoints
// or credentials to immutable DSL.
package resources

import (
	"context"
	"errors"

	"github.com/uu999/evalfrog/internal/access"
	"github.com/uu999/evalfrog/internal/compiler"
	"github.com/uu999/evalfrog/internal/ir"
)

type Purpose string

const (
	PurposeTest       Purpose = "test"
	PurposeProduction Purpose = "production"
)

var ErrResourceNotFound = errors.New("resource not found")

type Connection struct {
	ID        string
	ProjectID string
	Reference string
}

type ServiceOperation struct {
	ServiceID        string
	ProjectID        string
	ServiceReference string
	Operation        string
	ContractRevision string
	Idempotent       bool
}

type Repository interface {
	ResolveConnection(context.Context, string, string, string, Purpose) (Connection, error)
	ResolveServiceOperation(context.Context, string, string, string, string, Purpose) (ServiceOperation, error)
}

type Authorizer interface {
	Require(context.Context, string, string, access.Permission) error
	ExecutionIdentity(context.Context, string) (access.ExecutionIdentity, error)
}

type Resolver struct {
	repository Repository
	authorizer Authorizer
}

func NewResolver(repository Repository, authorizer Authorizer) Resolver {
	return Resolver{repository: repository, authorizer: authorizer}
}

func (resolver Resolver) Resolve(ctx context.Context, projectID, principalID string, purpose Purpose, document ir.Document) (compiler.ResourceBindings, []ir.Diagnostic, error) {
	connections := make(map[string]compiler.ConnectionBinding)
	services := make(map[compiler.ServiceOperationKey]compiler.ServiceOperationBinding)
	identity, err := resolver.authorizer.ExecutionIdentity(ctx, projectID)
	if err != nil {
		return compiler.ResourceBindings{}, nil, err
	}
	for nodeIndex, node := range document.Nodes {
		switch node.Type {
		case "http":
			if err := resolver.authorizer.Require(ctx, projectID, principalID, access.PermissionConnectionUse); err != nil {
				return compiler.ResourceBindings{}, nil, err
			}
			reference, inputIndex, ok := literalStringInput(node, "connection_ref")
			if !ok {
				continue
			}
			if _, exists := connections[reference]; exists {
				continue
			}
			connection, resolveErr := resolver.repository.ResolveConnection(ctx, projectID, identity.ID, reference, purpose)
			if resolveErr != nil {
				if errors.Is(resolveErr, ErrResourceNotFound) {
					return compiler.ResourceBindings{}, []ir.Diagnostic{resourceDiagnostic(node, nodeIndex, "connection_ref", inputIndex, "RESOURCE_NOT_FOUND", "managed resource is unavailable")}, nil
				}
				return compiler.ResourceBindings{}, nil, resolveErr
			}
			connections[reference] = compiler.ConnectionBinding{ConnectionID: connection.ID}
		case "rpc":
			if err := resolver.authorizer.Require(ctx, projectID, principalID, access.PermissionServiceUse); err != nil {
				return compiler.ResourceBindings{}, nil, err
			}
			serviceReference, serviceInputIndex, serviceOK := literalStringInput(node, "service_ref")
			operation, operationInputIndex, operationOK := literalStringInput(node, "operation")
			if !serviceOK || !operationOK {
				continue
			}
			key := compiler.ServiceOperationKey{ServiceRef: serviceReference, Operation: operation}
			if _, exists := services[key]; exists {
				continue
			}
			service, resolveErr := resolver.repository.ResolveServiceOperation(ctx, projectID, identity.ID, serviceReference, operation, purpose)
			if resolveErr != nil {
				if errors.Is(resolveErr, ErrResourceNotFound) {
					inputName, inputIndex := "service_ref", serviceInputIndex
					if serviceReference != "" {
						inputName, inputIndex = "operation", operationInputIndex
					}
					return compiler.ResourceBindings{}, []ir.Diagnostic{resourceDiagnostic(node, nodeIndex, inputName, inputIndex, "RESOURCE_NOT_FOUND", "managed resource is unavailable")}, nil
				}
				return compiler.ResourceBindings{}, nil, resolveErr
			}
			services[key] = compiler.ServiceOperationBinding{ServiceID: service.ServiceID, ContractRevision: service.ContractRevision, Idempotent: service.Idempotent}
		}
	}
	bindings, err := compiler.NewResourceBindings(connections, services, nil)
	if err != nil {
		return compiler.ResourceBindings{}, nil, err
	}
	return bindings, nil, nil
}

func literalStringInput(node ir.Node, name ir.PortName) (string, int, bool) {
	for index, input := range node.Inputs {
		if input.Name != name || input.Source != ir.SourceLiteral || !input.HasValue() {
			continue
		}
		value, _, err := ir.DecodeLiteral(input.Value)
		text, ok := value.(string)
		return text, index, err == nil && ok && text != ""
	}
	return "", -1, false
}

func resourceDiagnostic(node ir.Node, nodeIndex int, inputName string, inputIndex int, code, message string) ir.Diagnostic {
	path := ir.NodePath(node.ID, nodeIndex) + "/inputs/" + inputName
	if inputIndex >= 0 {
		path = ir.InputPath(node.ID, nodeIndex, ir.PortName(inputName), inputIndex)
	}
	return ir.ErrorDiagnostic(ir.PhaseResource, code, message, ir.Location{LogicalNodeID: node.ID, IRPath: path})
}
