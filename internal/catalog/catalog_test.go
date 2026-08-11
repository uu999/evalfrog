package catalog

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/uu999/evalfrog/internal/ir"
)

func TestBuiltinCatalogPublishesSixVersionlessDescriptions(t *testing.T) {
	t.Parallel()
	catalog := BuiltinV1()
	if catalog.Revision() != BuiltinRevisionV1 {
		t.Fatalf("unexpected catalog revision %q", catalog.Revision())
	}
	descriptions := catalog.Descriptions()
	actualTypes := make([]ir.NodeType, 0, len(descriptions))
	for _, description := range descriptions {
		actualTypes = append(actualTypes, description.Type)
		if description.Description == "" || len(description.Examples) == 0 {
			t.Fatalf("description %s is incomplete", description.Type)
		}
		revision, exists := catalog.ContractRevision(description.Type)
		if !exists || revision != 1 {
			t.Fatalf("description %s has no internal immutable revision", description.Type)
		}
		encoded, err := json.Marshal(description)
		if err != nil {
			t.Fatal(err)
		}
		for _, forbidden := range [][]byte{
			[]byte("version"), []byte("catalog_revision"), []byte("operation_version"),
			[]byte("kafka"), []byte("worker"), []byte("lease"), []byte("retry"), []byte("timeout"),
		} {
			if bytes.Contains(bytes.ToLower(encoded), forbidden) {
				t.Fatalf("public %s description exposes forbidden runtime/version field %q: %s", description.Type, forbidden, encoded)
			}
		}
		for _, example := range description.Examples {
			if diagnostics := catalog.ValidateNode(example); ir.HasErrors(diagnostics) {
				t.Fatalf("catalog example %s is invalid: %+v", description.Type, diagnostics)
			}
		}
	}
	expected := []ir.NodeType{"branch", "code", "end", "http", "rpc", "start"}
	if !reflect.DeepEqual(actualTypes, expected) {
		t.Fatalf("node types changed: got %v want %v", actualTypes, expected)
	}
}

func TestRuntimeCoordinatesAndPoliciesStayInternal(t *testing.T) {
	t.Parallel()
	builtin := BuiltinV1()
	encoded, err := json.Marshal(builtin.Descriptions())
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range [][]byte{[]byte("operation_version"), []byte("operation_type"), []byte("retry_backoff"), []byte("attempt_timeout")} {
		if bytes.Contains(encoded, forbidden) {
			t.Fatalf("public Node Description leaked internal runtime field %q", forbidden)
		}
	}
	for _, description := range builtin.Descriptions() {
		runtimeContract, exists := builtin.RuntimeContract(description.Type)
		if !exists || runtimeContract.Kind != description.Kind || runtimeContract.OperationVersion != 1 {
			t.Fatalf("invalid runtime contract for %s: %+v", description.Type, runtimeContract)
		}
		if description.Kind == KindTask && runtimeContract.DefaultExecutionPolicy.MaxAttempts == 0 {
			t.Fatalf("task %s has no internal execution policy", description.Type)
		}
	}
}

func TestDescriptionsAreDefensiveCopies(t *testing.T) {
	t.Parallel()
	catalog := BuiltinV1()
	description, ok := catalog.Describe("http")
	if !ok {
		t.Fatal("http description missing")
	}
	description.Inputs[0].Name = "mutated"
	again, _ := catalog.Describe("http")
	if again.Inputs[0].Name == "mutated" {
		t.Fatal("public caller mutated catalog contract")
	}
}

func TestCatalogRejectsInvalidContractAtRegistration(t *testing.T) {
	t.Parallel()
	_, err := newCatalog("bad", []contract{{revision: 1, description: NodeDescription{
		Type: "custom", Kind: KindTask, Description: "broken", Examples: []ir.Node{{Type: "custom"}},
		Inputs:  []InputDescription{{Name: "bad-name", DataTypes: []ir.DataType{ir.TypeString}, Sources: []ir.InputSource{ir.SourceLiteral}}},
		Outputs: OutputDescription{Mode: OutputFixed, Fields: []PortDescription{}},
	}}})
	if err == nil {
		t.Fatal("invalid catalog contract was registered")
	}
}

func TestNodeSpecificConstraints(t *testing.T) {
	t.Parallel()
	catalog := BuiltinV1()
	tests := []struct {
		name string
		node ir.Node
		code string
	}{
		{
			name: "http full url",
			node: ir.Node{ID: "http", Type: "http", Title: "HTTP", Inputs: []ir.Input{
				{Name: "connection_ref", DataType: ir.TypeString, Source: ir.SourceLiteral, Value: json.RawMessage(`"api"`)},
				{Name: "method", DataType: ir.TypeString, Source: ir.SourceLiteral, Value: json.RawMessage(`"GET"`)},
				{Name: "relative_path", DataType: ir.TypeString, Source: ir.SourceLiteral, Value: json.RawMessage(`"https://evil.example"`)},
			}, Outputs: []ir.Output{{Name: "response", DataType: ir.TypeObject}}},
			code: "HTTP_INVALID_RELATIVE_PATH",
		},
		{
			name: "branch malformed case",
			node: ir.Node{ID: "branch", Type: "branch", Title: "Branch", Inputs: []ir.Input{
				{Name: "value", DataType: ir.TypeInteger, Source: ir.SourceLiteral, Value: json.RawMessage(`1`)},
				{Name: "cases", DataType: ir.TypeArray, Source: ir.SourceLiteral, Value: json.RawMessage(`[{"route":"yes","operator":"eval","value":1}]`)},
				{Name: "default_route", DataType: ir.TypeString, Source: ir.SourceLiteral, Value: json.RawMessage(`"no"`)},
			}, Outputs: []ir.Output{}},
			code: "BRANCH_CASES_INVALID",
		},
		{
			name: "branch operator type mismatch",
			node: ir.Node{ID: "branch", Type: "branch", Title: "Branch", Inputs: []ir.Input{
				{Name: "value", DataType: ir.TypeBoolean, Source: ir.SourceLiteral, Value: json.RawMessage(`true`)},
				{Name: "cases", DataType: ir.TypeArray, Source: ir.SourceLiteral, Value: json.RawMessage(`[{"route":"yes","operator":"gt","value":true}]`)},
				{Name: "default_route", DataType: ir.TypeString, Source: ir.SourceLiteral, Value: json.RawMessage(`"no"`)},
			}, Outputs: []ir.Output{}},
			code: "BRANCH_OPERATOR_TYPE_MISMATCH",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			diagnostics := catalog.ValidateNode(test.node)
			assertCatalogDiagnostic(t, diagnostics, test.code)
		})
	}
}

func assertCatalogDiagnostic(t *testing.T, diagnostics []ir.Diagnostic, code string) {
	t.Helper()
	for _, diagnostic := range diagnostics {
		if diagnostic.Code == code {
			return
		}
	}
	t.Fatalf("diagnostic %s not found in %+v", code, diagnostics)
}
