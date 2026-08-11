package compiler

import "fmt"

type ConnectionBinding struct {
	ConnectionID string
}

type ServiceOperationBinding struct {
	ServiceID        string
	ContractRevision string
	Idempotent       bool
}

type ModelBinding struct {
	ArtifactID string
	Digest     string
}

type ServiceOperationKey struct {
	ServiceRef string
	Operation  string
}

// ResourceBindings is the deterministic output of M3 resource resolution and
// authorization. It contains stable IDs and public capability metadata only;
// credentials, endpoints, and secrets have no representation here.
type ResourceBindings struct {
	connections map[string]ConnectionBinding
	services    map[ServiceOperationKey]ServiceOperationBinding
	models      map[string]ModelBinding
}

func NewResourceBindings(connections map[string]ConnectionBinding, services map[ServiceOperationKey]ServiceOperationBinding, models map[string]ModelBinding) (ResourceBindings, error) {
	result := ResourceBindings{
		connections: make(map[string]ConnectionBinding, len(connections)),
		services:    make(map[ServiceOperationKey]ServiceOperationBinding, len(services)),
		models:      make(map[string]ModelBinding, len(models)),
	}
	for reference, binding := range connections {
		if reference == "" || binding.ConnectionID == "" {
			return ResourceBindings{}, fmt.Errorf("connection binding requires reference and stable id")
		}
		result.connections[reference] = binding
	}
	for key, binding := range services {
		if key.ServiceRef == "" || key.Operation == "" || binding.ServiceID == "" || binding.ContractRevision == "" {
			return ResourceBindings{}, fmt.Errorf("service binding requires reference, operation, stable id, and contract revision")
		}
		result.services[key] = binding
	}
	for reference, binding := range models {
		if reference == "" || binding.ArtifactID == "" || binding.Digest == "" {
			return ResourceBindings{}, fmt.Errorf("model binding requires reference, artifact id, and digest")
		}
		result.models[reference] = binding
	}
	return result, nil
}

func EmptyResourceBindings() ResourceBindings {
	result, err := NewResourceBindings(nil, nil, nil)
	if err != nil {
		panic(err)
	}
	return result
}

func (bindings ResourceBindings) Connection(reference string) (ConnectionBinding, bool) {
	value, exists := bindings.connections[reference]
	return value, exists
}

func (bindings ResourceBindings) ServiceOperation(serviceRef, operation string) (ServiceOperationBinding, bool) {
	value, exists := bindings.services[ServiceOperationKey{ServiceRef: serviceRef, Operation: operation}]
	return value, exists
}

func (bindings ResourceBindings) Model(reference string) (ModelBinding, bool) {
	value, exists := bindings.models[reference]
	return value, exists
}
