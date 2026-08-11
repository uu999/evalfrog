package compiler_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/uu999/evalfrog/internal/catalog"
	"github.com/uu999/evalfrog/internal/compiler"
	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/ir"
)

func TestIRToDSLAndSourceMapGolden(t *testing.T) {
	result := compileFixture(t)
	assertGolden(t, "all_operations.dsl.golden.json", result.CanonicalDSL)
	assertGolden(t, "all_operations.source-map.golden.json", result.CanonicalSourceMap)
	manifest, err := json.Marshal(struct {
		Manifest compiler.Manifest `json:"manifest"`
		Hashes   compiler.Hashes   `json:"hashes"`
	}{Manifest: result.Manifest, Hashes: result.Hashes})
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := ir.CanonicalizeJSON(manifest, ir.DefaultParseLimits)
	if err != nil {
		t.Fatal(err)
	}
	assertGolden(t, "all_operations.manifest.golden.json", canonical)
}

func TestCompilerIsByteDeterministicOneHundredTimes(t *testing.T) {
	first := compileFixture(t)
	for iteration := 0; iteration < 100; iteration++ {
		actual := compileFixture(t)
		if !bytes.Equal(first.CanonicalIR, actual.CanonicalIR) || !bytes.Equal(first.CanonicalDSL, actual.CanonicalDSL) || !bytes.Equal(first.CanonicalSourceMap, actual.CanonicalSourceMap) || first.Hashes != actual.Hashes || first.Manifest != actual.Manifest {
			t.Fatalf("compiler output changed at iteration %d", iteration)
		}
	}
}

func TestLayoutChangesOnlyIRAndDefinitionHashes(t *testing.T) {
	document := loadFixture(t)
	first := compileDocument(t, document)
	value := json.Number("1234.5")
	position := document.Layout["prepare_order"]
	position.X = &value
	document.Layout["prepare_order"] = position
	second := compileDocument(t, document)
	if first.Hashes.IRHash == second.Hashes.IRHash || first.Hashes.DefinitionHash == second.Hashes.DefinitionHash {
		t.Fatal("layout change did not alter IR and definition hashes")
	}
	if first.Hashes.DSLHash != second.Hashes.DSLHash || first.Hashes.SourceMapHash != second.Hashes.SourceMapHash {
		t.Fatal("layout leaked into runtime artifacts")
	}
}

func TestPolicyRevisionParticipatesOnlyThroughManifestAndDefinitionHash(t *testing.T) {
	document := loadFixture(t)
	first := compileDocument(t, document)
	policy, err := compiler.NewPolicy("policy-v2", nil)
	if err != nil {
		t.Fatal(err)
	}
	request := compileRequest(t, document)
	request.Policy = policy
	second, diagnostics := compiler.BuiltinV1().Compile(request)
	if ir.HasErrors(diagnostics) {
		t.Fatalf("compile failed: %+v", diagnostics)
	}
	if first.Hashes.DSLHash != second.Hashes.DSLHash || first.Hashes.SourceMapHash != second.Hashes.SourceMapHash || first.Hashes.IRHash != second.Hashes.IRHash {
		t.Fatal("policy revision string leaked into IR, DSL, or Source Map content")
	}
	if first.Hashes.DefinitionHash == second.Hashes.DefinitionHash {
		t.Fatal("definition hash ignored compiler manifest policy revision")
	}
}

func TestManagedResourceBindingsAreRequiredAndAuthorRefsDoNotLeak(t *testing.T) {
	document := loadFixture(t)
	_, diagnostics := compiler.BuiltinV1().Compile(compiler.Request{IR: document, Catalog: catalog.BuiltinV1(), Policy: compiler.DefaultPolicyV1(), Resources: compiler.EmptyResourceBindings()})
	assertDiagnostic(t, diagnostics, "CONNECTION_BINDING_REQUIRED")
	assertDiagnostic(t, diagnostics, "SERVICE_BINDING_REQUIRED")

	result := compileDocument(t, document)
	for _, authorRef := range []string{"customer_api", "order_service"} {
		if bytes.Contains(result.CanonicalDSL, []byte(authorRef)) {
			t.Fatalf("author resource reference %q leaked into immutable DSL", authorRef)
		}
	}
}

func TestCompilerRejectsUnsafeAutomaticRetryPolicies(t *testing.T) {
	policy, err := compiler.NewPolicy("unsafe-retry-policy", map[ir.NodeType]dsl.ExecutionPolicy{
		"http": {
			MaxAttempts: 2, MaxRecoveries: 3, AttemptTimeoutMS: 30_000,
			RetryBackoff: &dsl.RetryBackoff{Kind: "fixed", DelayMS: 1_000},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := compileRequest(t, loadFixture(t))
	request.Policy = policy
	_, diagnostics := compiler.BuiltinV1().Compile(request)
	assertDiagnostic(t, diagnostics, "HTTP_RETRY_POLICY_UNSAFE")
}

func TestCompilerRejectsUnsafeAndNonUpstreamReferences(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ir.Document)
		code   string
	}{
		{
			name: "branch specific output at join",
			mutate: func(document *ir.Document) {
				end := nodeByID(document, "end")
				end.Inputs[0] = ir.Input{Name: "workflow_output", DataType: ir.TypeObject, Source: ir.SourceRef, RefNode: "create_via_http", RefOutput: "response"}
			},
			code: "UNSAFE_DATA_BINDING",
		},
		{
			name: "sibling branch source",
			mutate: func(document *ir.Document) {
				http := nodeByID(document, "create_via_http")
				for index := range http.Inputs {
					if http.Inputs[index].Name == "body" {
						http.Inputs[index].RefNode = "create_via_rpc"
						http.Inputs[index].RefOutput = "response"
					}
				}
			},
			code: "REF_SOURCE_NOT_CONTROL_UPSTREAM",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			document := loadFixture(t)
			test.mutate(&document)
			_, diagnostics := compiler.BuiltinV1().Compile(compileRequest(t, document))
			assertDiagnostic(t, diagnostics, test.code)
		})
	}
}

func TestControlGraphStructuralDiagnostics(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ir.Document)
		code   string
	}{
		{
			name: "cycle",
			mutate: func(document *ir.Document) {
				document.Edges = append(document.Edges, ir.Edge{ID: "end_to_start", Source: "end", Target: "start"})
			},
			code: "CONTROL_GRAPH_CYCLE",
		},
		{
			name: "undeclared branch route",
			mutate: func(document *ir.Document) {
				for index := range document.Edges {
					if document.Edges[index].ID == "branch_to_http" {
						document.Edges[index].Route = "unknown_route"
					}
				}
			},
			code: "BRANCH_EDGE_ROUTE_UNDECLARED",
		},
		{
			name: "isolated node",
			mutate: func(document *ir.Document) {
				x, y := json.Number("10"), json.Number("10")
				document.Nodes = append(document.Nodes, ir.Node{ID: "orphan", Type: "code", Title: "孤立", Inputs: []ir.Input{{Name: "source_code", DataType: ir.TypeString, Source: ir.SourceLiteral, Value: json.RawMessage(`"def main(inputs):\n    return {}"`)}}, Outputs: []ir.Output{}})
				document.Layout["orphan"] = ir.Position{X: &x, Y: &y}
			},
			code: "NODE_NOT_REACHABLE_FROM_START",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			document := loadFixture(t)
			test.mutate(&document)
			_, diagnostics := compiler.BuiltinV1().Compile(compileRequest(t, document))
			assertDiagnostic(t, diagnostics, test.code)
		})
	}
}

func TestORJoinActivationAllowsOnlyGuaranteedData(t *testing.T) {
	result := compileFixture(t)
	if len(result.DSL.Nodes) != 7 {
		t.Fatalf("expected seven execution nodes, got %d", len(result.DSL.Nodes))
	}
	var finalize dsl.Node
	for _, node := range result.DSL.Nodes {
		if result.SourceMap.Nodes[node.ID] == "finalize" {
			finalize = node
		}
	}
	if finalize.ID == "" {
		t.Fatal("OR-Join node was not compiled")
	}
}

func TestBranchSixTypesAndOperatorMatrix(t *testing.T) {
	matrix := map[ir.DataType][]string{
		ir.TypeString:  {"eq", "neq", "contains", "not_contains", "starts_with", "ends_with"},
		ir.TypeInteger: {"eq", "neq", "gt", "gte", "lt", "lte"},
		ir.TypeNumber:  {"eq", "neq", "gt", "gte", "lt", "lte"},
		ir.TypeBoolean: {"eq", "neq"},
		ir.TypeArray:   {"eq", "neq", "contains", "not_contains"},
		ir.TypeObject:  {"eq", "neq", "has_key", "not_has_key"},
	}
	types := make([]ir.DataType, 0, len(matrix))
	for dataType := range matrix {
		types = append(types, dataType)
	}
	sort.Slice(types, func(left, right int) bool { return types[left] < types[right] })
	for _, dataType := range types {
		for _, operator := range matrix[dataType] {
			t.Run(string(dataType)+"/"+operator, func(t *testing.T) {
				document := branchDocument(dataType, operator)
				_, diagnostics := compiler.BuiltinV1().Compile(compiler.Request{IR: document, Catalog: catalog.BuiltinV1(), Policy: compiler.DefaultPolicyV1(), Resources: compiler.EmptyResourceBindings()})
				if ir.HasErrors(diagnostics) {
					t.Fatalf("valid branch contract rejected: %+v", diagnostics)
				}
			})
		}
	}
	document := branchDocument(ir.TypeString, "gt")
	_, diagnostics := compiler.BuiltinV1().Compile(compiler.Request{IR: document, Catalog: catalog.BuiltinV1(), Policy: compiler.DefaultPolicyV1(), Resources: compiler.EmptyResourceBindings()})
	assertDiagnostic(t, diagnostics, "BRANCH_OPERATOR_TYPE_MISMATCH")
}

func TestSourceMapCoverageFailureIsRejected(t *testing.T) {
	result := compileFixture(t)
	for nodeID, logicalID := range result.SourceMap.Nodes {
		if logicalID == "prepare_order" {
			delete(result.SourceMap.Fields[nodeID], "operation.config.source_code")
			break
		}
	}
	diagnostics := compiler.ValidateSourceMap(result.DSL, result.SourceMap)
	assertDiagnostic(t, diagnostics, "SOURCE_MAP_FIELD_MISSING")
}

func TestUnsupportedOperationVersionRejectedBeforeExecution(t *testing.T) {
	result := compileFixture(t)
	result.DSL.Nodes[0].Operation.Version = 99
	issues := dsl.BuiltinV1Compatibility().CheckAll(result.DSL)
	if len(issues) != 1 || issues[0].Code != "RUNTIME_OPERATION_UNSUPPORTED" {
		t.Fatalf("unexpected compatibility result: %+v", issues)
	}
}

type customHandler struct{}

func (customHandler) NodeType() ir.NodeType { return "custom_task" }
func (customHandler) Kind() dsl.NodeKind    { return dsl.KindTask }
func (customHandler) Coordinate() dsl.Coordinate {
	return dsl.Coordinate{Type: "task.custom", Version: 1}
}
func (customHandler) Compile(compiler.Context, ir.Node) (compiler.NodeProduct, []ir.Diagnostic) {
	return compiler.NodeProduct{}, nil
}

func TestHandlerRegistryExtendsWithoutCentralSwitch(t *testing.T) {
	registry, err := compiler.NewRegistry(customHandler{})
	if err != nil {
		t.Fatal(err)
	}
	if handler, exists := registry.Handler("custom_task"); !exists || handler.Coordinate().Type != "task.custom" {
		t.Fatal("custom compiler handler was not registered")
	}
	if _, err = compiler.NewRegistry(customHandler{}, customHandler{}); err == nil {
		t.Fatal("duplicate compiler handler was accepted")
	}
}

func compileFixture(t *testing.T) compiler.Result {
	t.Helper()
	return compileDocument(t, loadFixture(t))
}

func compileDocument(t *testing.T, document ir.Document) compiler.Result {
	t.Helper()
	result, diagnostics := compiler.BuiltinV1().Compile(compileRequest(t, document))
	if ir.HasErrors(diagnostics) {
		t.Fatalf("compile failed: %+v", diagnostics)
	}
	return result
}

func compileRequest(t *testing.T, document ir.Document) compiler.Request {
	t.Helper()
	resources, err := compiler.NewResourceBindings(
		map[string]compiler.ConnectionBinding{"customer_api": {ConnectionID: "conn_018f_customer"}},
		map[compiler.ServiceOperationKey]compiler.ServiceOperationBinding{{ServiceRef: "order_service", Operation: "CreateOrder"}: {ServiceID: "svc_018f_order", ContractRevision: "contract-7", Idempotent: true}},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	return compiler.Request{IR: document, Catalog: catalog.BuiltinV1(), Policy: compiler.DefaultPolicyV1(), Resources: resources}
}

func loadFixture(t *testing.T) ir.Document {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "all_operations.ir.json"))
	if err != nil {
		t.Fatal(err)
	}
	document, diagnostics := ir.DefaultParser().ParseDraft(raw)
	if ir.HasErrors(diagnostics) {
		t.Fatalf("fixture parse failed: %+v", diagnostics)
	}
	return document
}

func assertGolden(t *testing.T, name string, actual []byte) {
	t.Helper()
	expected, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	expected = bytes.TrimSpace(expected)
	if !bytes.Equal(expected, actual) {
		t.Fatalf("golden %s changed:\n%s", name, actual)
	}
}

func assertDiagnostic(t *testing.T, diagnostics []ir.Diagnostic, code string) {
	t.Helper()
	for _, diagnostic := range diagnostics {
		if diagnostic.Code == code {
			return
		}
	}
	t.Fatalf("diagnostic %s not found in %+v", code, diagnostics)
}

func nodeByID(document *ir.Document, id ir.LogicalID) *ir.Node {
	for index := range document.Nodes {
		if document.Nodes[index].ID == id {
			return &document.Nodes[index]
		}
	}
	panic("node not found")
}

func branchDocument(dataType ir.DataType, operator string) ir.Document {
	value, comparison := branchValues(dataType, operator)
	cases := mustJSON([]any{map[string]any{"route": "matched", "operator": operator, "value": comparison}})
	x0, x1, x2, y := json.Number("0"), json.Number("1"), json.Number("2"), json.Number("0")
	return ir.Document{
		IRVersion: ir.VersionV1,
		Nodes: []ir.Node{
			{ID: "start", Type: "start", Title: "Start", Inputs: []ir.Input{}, Outputs: []ir.Output{{Name: "workflow_input", DataType: ir.TypeObject}}},
			{ID: "branch", Type: "branch", Title: "Branch", Inputs: []ir.Input{
				{Name: "value", DataType: dataType, Source: ir.SourceLiteral, Value: mustJSON(value)},
				{Name: "cases", DataType: ir.TypeArray, Source: ir.SourceLiteral, Value: cases},
				{Name: "default_route", DataType: ir.TypeString, Source: ir.SourceLiteral, Value: mustJSON("default")},
			}, Outputs: []ir.Output{}},
			{ID: "end", Type: "end", Title: "End", Inputs: []ir.Input{{Name: "workflow_output", DataType: ir.TypeObject, Source: ir.SourceLiteral, Value: mustJSON(map[string]any{})}}, Outputs: []ir.Output{}},
		},
		Edges: []ir.Edge{
			{ID: "start_to_branch", Source: "start", Target: "branch"},
			{ID: "matched_to_end", Source: "branch", Target: "end", Route: "matched"},
			{ID: "default_to_end", Source: "branch", Target: "end", Route: "default"},
		},
		Layout: map[ir.LogicalID]ir.Position{
			"start": {X: &x0, Y: &y}, "branch": {X: &x1, Y: &y}, "end": {X: &x2, Y: &y},
		},
	}
}

func branchValues(dataType ir.DataType, operator string) (any, any) {
	switch dataType {
	case ir.TypeString:
		return "frog", "frog"
	case ir.TypeInteger:
		return 2, 1
	case ir.TypeNumber:
		return 2.5, 1.5
	case ir.TypeBoolean:
		return true, true
	case ir.TypeArray:
		if operator == "contains" || operator == "not_contains" {
			return []any{1, "frog"}, "frog"
		}
		return []any{1, "frog"}, []any{1, "frog"}
	case ir.TypeObject:
		if operator == "has_key" || operator == "not_has_key" {
			return map[string]any{"frog": true}, "frog"
		}
		return map[string]any{"frog": true}, map[string]any{"frog": true}
	default:
		panic("unsupported type")
	}
}

func mustJSON(value any) json.RawMessage {
	raw, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return raw
}

func TestCompiledResultHasNoIRLogicalIDsInDSL(t *testing.T) {
	result := compileFixture(t)
	text := string(result.CanonicalDSL)
	for _, logicalID := range []string{"prepare_order", "choose_transport", "branch_to_http", "create_via_rpc"} {
		if bytes.Contains([]byte(text), []byte(logicalID)) {
			t.Fatalf("author logical id %q leaked into DSL", logicalID)
		}
	}
	if reflect.DeepEqual(result.CanonicalIR, result.CanonicalDSL) {
		t.Fatal("IR and DSL unexpectedly became copies")
	}
}
