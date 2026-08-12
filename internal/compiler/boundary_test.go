package compiler

import (
	"encoding/json"
	"testing"

	"github.com/uu999/evalfrog/internal/catalog"
	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/ir"
	"github.com/uu999/evalfrog/internal/sourcemap"
)

type boundaryHandler struct {
	nodeType   ir.NodeType
	coordinate dsl.Coordinate
}

func (handler boundaryHandler) NodeType() ir.NodeType      { return handler.nodeType }
func (handler boundaryHandler) Kind() dsl.NodeKind         { return dsl.KindTask }
func (handler boundaryHandler) Coordinate() dsl.Coordinate { return handler.coordinate }
func (boundaryHandler) Compile(Context, ir.Node) (NodeProduct, []ir.Diagnostic) {
	return NodeProduct{Config: map[string]json.RawMessage{}, Inputs: map[dsl.PortName]dsl.InputBinding{}, Outputs: map[dsl.PortName]dsl.DataType{}, FieldMappings: map[string]string{}}, nil
}

func TestConstructorsAndValueAccessorsRejectInvalidBoundaries(t *testing.T) {
	if _, err := New("", Registry{}, dsl.BuiltinV1Contract(), dsl.BuiltinV1Compatibility()); err == nil {
		t.Fatal("empty compiler version accepted")
	}
	if _, err := New(VersionV1, Registry{}, dsl.Contract{}, dsl.BuiltinV1Compatibility()); err == nil {
		t.Fatal("empty DSL contract accepted")
	}
	if _, err := NewPolicy("", nil); err == nil {
		t.Fatal("empty policy revision accepted")
	}
	valid := dsl.ExecutionPolicy{MaxAttempts: 1, AttemptTimeoutMS: 1, RetryBackoff: &dsl.RetryBackoff{Kind: "fixed", DelayMS: 1}}
	for name, policy := range map[string]dsl.ExecutionPolicy{
		"missing required": {},
		"invalid backoff":  {MaxAttempts: 1, AttemptTimeoutMS: 1, RetryBackoff: &dsl.RetryBackoff{Kind: "linear", DelayMS: 1}},
		"empty retry code": {MaxAttempts: 1, AttemptTimeoutMS: 1, RetryBackoff: &dsl.RetryBackoff{Kind: "fixed", DelayMS: 1}, RetryableErrorCodes: []string{""}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewPolicy("revision", map[ir.NodeType]dsl.ExecutionPolicy{"code": policy}); err == nil {
				t.Fatal("invalid policy accepted")
			}
		})
	}
	if _, err := NewPolicy("revision", map[ir.NodeType]dsl.ExecutionPolicy{"invalid type": valid}); err == nil {
		t.Fatal("invalid node type accepted")
	}
	policy, err := NewPolicy("revision", map[ir.NodeType]dsl.ExecutionPolicy{"code": valid})
	if err != nil || policy.Revision() != "revision" {
		t.Fatalf("policy=%+v err=%v", policy, err)
	}

	for _, test := range []struct {
		name        string
		connections map[string]ConnectionBinding
		services    map[ServiceOperationKey]ServiceOperationBinding
		models      map[string]ModelBinding
	}{
		{"connection", map[string]ConnectionBinding{"": {}}, nil, nil},
		{"service", nil, map[ServiceOperationKey]ServiceOperationBinding{{}: {}}, nil},
		{"model", nil, nil, map[string]ModelBinding{"": {}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewResourceBindings(test.connections, test.services, test.models); err == nil {
				t.Fatal("invalid resource binding accepted")
			}
		})
	}
	resources, err := NewResourceBindings(nil, nil, map[string]ModelBinding{"small": {ArtifactID: "artifact", Digest: "sha256"}})
	if err != nil {
		t.Fatal(err)
	}
	if model, exists := resources.Model("small"); !exists || model.ArtifactID != "artifact" {
		t.Fatalf("model=%+v exists=%v", model, exists)
	}

	if _, err := NewRegistry(nil); err == nil {
		t.Fatal("nil handler accepted")
	}
	if _, err := NewRegistry(boundaryHandler{nodeType: "custom", coordinate: dsl.Coordinate{}}); err == nil {
		t.Fatal("invalid coordinate accepted")
	}
	registry, err := NewRegistry(
		boundaryHandler{nodeType: "zeta", coordinate: dsl.Coordinate{Type: "task.zeta", Version: 1}},
		boundaryHandler{nodeType: "alpha", coordinate: dsl.Coordinate{Type: "task.alpha", Version: 1}},
	)
	if err != nil {
		t.Fatal(err)
	}
	types := registry.NodeTypes()
	if len(types) != 2 || types[0] != "alpha" || types[1] != "zeta" {
		t.Fatalf("node types=%v", types)
	}
}

func TestCompilerAndContextDefensivePaths(t *testing.T) {
	compiler := BuiltinV1()
	if _, diagnostics := compiler.Compile(Request{}); len(diagnostics) != 1 || diagnostics[0].Code != "CATALOG_REQUIRED" {
		t.Fatalf("missing catalog diagnostics=%+v", diagnostics)
	}
	if _, diagnostics := compiler.Compile(Request{Catalog: catalog.BuiltinV1()}); len(diagnostics) != 1 || diagnostics[0].Code != "POLICY_REQUIRED" {
		t.Fatalf("missing policy diagnostics=%+v", diagnostics)
	}
	resources := EmptyResourceBindings()
	context := Context{
		executionNodeID: "xn_000000000000000000000001",
		executionIDs:    map[ir.LogicalID]dsl.NodeID{"logical": "xn_000000000000000000000002"},
		policy:          dsl.ExecutionPolicy{RetryBackoff: &dsl.RetryBackoff{Kind: "fixed", DelayMS: 1}, RetryableErrorCodes: []string{"A"}},
		resources:       resources,
		nodeIndex:       4,
	}
	if context.ExecutionNodeID() == "" || context.NodeIndex() != 4 || context.Resources().models == nil {
		t.Fatal("context accessors lost values")
	}
	if id, exists := context.ExecutionNodeIDFor("logical"); !exists || id == "" {
		t.Fatalf("id=%s exists=%v", id, exists)
	}
	if _, exists := context.ExecutionNodeIDFor("missing"); exists {
		t.Fatal("missing execution id exists")
	}
	copy := context.ExecutionPolicy()
	copy.RetryBackoff.DelayMS = 99
	copy.RetryableErrorCodes[0] = "changed"
	if context.policy.RetryBackoff.DelayMS == 99 || context.policy.RetryableErrorCodes[0] == "changed" {
		t.Fatal("execution policy accessor leaked mutable storage")
	}
}

func TestDSLDiagnosticMappingPreservesNodeEdgeAndFieldCoordinates(t *testing.T) {
	nodeID := dsl.NodeID("xn_000000000000000000000001")
	edgeID := dsl.EdgeID("xe_000000000000000000000001")
	mapping := sourcemap.Document{
		Nodes:  map[dsl.NodeID]string{nodeID: "logical_node"},
		Edges:  map[dsl.EdgeID]string{edgeID: "logical_edge"},
		Fields: map[dsl.NodeID]map[string]string{nodeID: {"inputs.value": "nodes.logical_node.inputs.value"}},
	}
	diagnostics := dslIssuesToDiagnostics([]dsl.Issue{{Code: "BROKEN", Message: "broken", NodeID: nodeID, EdgeID: edgeID, Field: "inputs.value"}}, mapping)
	if len(diagnostics) != 1 || len(diagnostics[0].Locations) != 1 || diagnostics[0].Locations[0].LogicalNodeID != "logical_node" || diagnostics[0].Locations[0].LogicalEdgeID != "logical_edge" || diagnostics[0].Locations[0].IRPath == "" {
		t.Fatalf("diagnostics=%+v", diagnostics)
	}
}
