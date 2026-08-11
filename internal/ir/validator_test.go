package ir

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

func TestValidatorAcceptsOnlyIntegerToNumberReferencePromotion(t *testing.T) {
	t.Parallel()
	document := Document{
		IRVersion: VersionV1,
		Nodes: []Node{
			{ID: "source", Type: "code", Title: "Source", Inputs: []Input{}, Outputs: []Output{{Name: "value", DataType: TypeInteger}}},
			{ID: "target", Type: "code", Title: "Target", Inputs: []Input{{Name: "value", DataType: TypeNumber, Source: SourceRef, RefNode: "source", RefOutput: "value"}}, Outputs: []Output{}},
		},
		Edges: []Edge{{ID: "source_to_target", Source: "source", Target: "target"}},
		Layout: map[LogicalID]Position{
			"source": {X: jsonNumber("0"), Y: jsonNumber("0")},
			"target": {X: jsonNumber("1"), Y: jsonNumber("0")},
		},
	}
	diagnostics := NewStructuralValidator().Validate(document)
	for _, diagnostic := range diagnostics {
		if diagnostic.Code == "REF_TYPE_MISMATCH" {
			t.Fatalf("integer to number promotion was rejected: %+v", diagnostics)
		}
	}

	document.Nodes[1].Inputs[0].DataType = TypeString
	diagnostics = NewStructuralValidator().Validate(document)
	assertDiagnosticCode(t, diagnostics, "REF_TYPE_MISMATCH")
}

func TestDiagnosticsAreDeterministicAndBounded(t *testing.T) {
	t.Parallel()
	document := Document{IRVersion: VersionV1, Nodes: make([]Node, 150), Edges: []Edge{}, Layout: map[LogicalID]Position{}}
	for index := range document.Nodes {
		document.Nodes[index] = Node{ID: LogicalID(fmt.Sprintf("bad.%03d", index)), Inputs: []Input{}, Outputs: []Output{}}
	}
	validator := NewStructuralValidator()
	first := validator.Validate(document)
	second := validator.Validate(document)
	if len(first) != MaxDiagnostics {
		t.Fatalf("got %d diagnostics, want %d", len(first), MaxDiagnostics)
	}
	if first[len(first)-1].Code != "DIAGNOSTIC_LIMIT_REACHED" {
		t.Fatalf("missing truncation marker: %+v", first[len(first)-1])
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatal("diagnostic order is not deterministic")
	}
}

func TestStrictValidatorRequiresCatalog(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Fatal("strict validator accepted a nil catalog")
		}
	}()
	_ = NewStrictValidator(nil)
}

func jsonNumber(value string) *json.Number {
	number := json.Number(value)
	return &number
}
