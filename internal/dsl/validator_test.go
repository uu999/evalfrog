package dsl_test

import (
	"encoding/json"
	"testing"

	"github.com/uu999/evalfrog/internal/dsl"
)

func TestBuiltinDSLContractValidatesSelfContainedDocument(t *testing.T) {
	document := minimalDocument()
	if issues := dsl.BuiltinV1Contract().Validate(document); len(issues) != 0 {
		t.Fatalf("valid DSL rejected: %+v", issues)
	}
	document.Nodes[1].Inputs["workflow_output"] = dsl.InputBinding{
		Kind: dsl.BindingNodeOutput, DataType: dsl.TypeObject,
		Output: &dsl.OutputReference{NodeID: document.Nodes[0].ID, Name: "missing"},
	}
	issues := dsl.BuiltinV1Contract().Validate(document)
	assertIssue(t, issues, "DSL_INPUT_OUTPUT_NOT_FOUND")
}

func TestDSLContractRejectsCorruptedBranchOperator(t *testing.T) {
	document := minimalDocument()
	branch := dsl.Node{
		ID: "xn_222222222222222222222222", Kind: dsl.KindControl,
		Operation: dsl.Operation{Type: "control.branch", Version: 1, Config: map[string]json.RawMessage{
			"cases":         json.RawMessage(`[{"route":"matched","operator":"gt","value":1}]`),
			"default_route": json.RawMessage(`"default"`),
		}},
		Inputs: map[dsl.PortName]dsl.InputBinding{
			"value": {Kind: dsl.BindingLiteral, DataType: dsl.TypeString, Value: json.RawMessage(`"frog"`)},
		},
		Outputs: map[dsl.PortName]dsl.DataType{}, ExecutionPolicy: dsl.ExecutionPolicy{},
	}
	document.Nodes = append(document.Nodes, branch)
	document.Edges = []dsl.Edge{
		{ID: "xe_111111111111111111111111", SourceNodeID: document.EntryNodeID, TargetNodeID: branch.ID, Activation: dsl.Activation{Kind: dsl.ActivationAlways}},
		{ID: "xe_222222222222222222222222", SourceNodeID: branch.ID, TargetNodeID: document.ExitNodeID, Activation: dsl.Activation{Kind: dsl.ActivationRoute, Route: "matched"}},
		{ID: "xe_333333333333333333333333", SourceNodeID: branch.ID, TargetNodeID: document.ExitNodeID, Activation: dsl.Activation{Kind: dsl.ActivationRoute, Route: "default"}},
	}
	issues := dsl.BuiltinV1Contract().Validate(document)
	assertIssue(t, issues, "DSL_BRANCH_OPERATOR_TYPE_MISMATCH")
}

func TestCompatibilityCheckerScansWholeSnapshot(t *testing.T) {
	document := minimalDocument()
	document.Nodes[0].Operation.Version = 7
	document.Nodes[1].Operation.Type = "control.unknown"
	issues := dsl.BuiltinV1Compatibility().CheckAll(document)
	if len(issues) != 2 {
		t.Fatalf("expected both unsupported operations before execution, got %+v", issues)
	}
	for _, issue := range issues {
		if issue.Code != "RUNTIME_OPERATION_UNSUPPORTED" {
			t.Fatalf("unexpected issue: %+v", issue)
		}
	}
}

func minimalDocument() dsl.Document {
	startID := dsl.NodeID("xn_000000000000000000000000")
	endID := dsl.NodeID("xn_111111111111111111111111")
	return dsl.Document{
		DSLVersion: dsl.VersionV1, EntryNodeID: startID, ExitNodeID: endID,
		Nodes: []dsl.Node{
			{ID: startID, Kind: dsl.KindControl, Operation: dsl.Operation{Type: "control.start", Version: 1, Config: map[string]json.RawMessage{}}, Inputs: map[dsl.PortName]dsl.InputBinding{}, Outputs: map[dsl.PortName]dsl.DataType{"workflow_input": dsl.TypeObject}, ExecutionPolicy: dsl.ExecutionPolicy{}},
			{ID: endID, Kind: dsl.KindControl, Operation: dsl.Operation{Type: "control.end", Version: 1, Config: map[string]json.RawMessage{}}, Inputs: map[dsl.PortName]dsl.InputBinding{"workflow_output": {Kind: dsl.BindingLiteral, DataType: dsl.TypeObject, Value: json.RawMessage(`{}`)}}, Outputs: map[dsl.PortName]dsl.DataType{}, ExecutionPolicy: dsl.ExecutionPolicy{}},
		},
		Edges: []dsl.Edge{{ID: "xe_000000000000000000000000", SourceNodeID: startID, TargetNodeID: endID, Activation: dsl.Activation{Kind: dsl.ActivationAlways}}},
	}
}

func assertIssue(t *testing.T, issues []dsl.Issue, code string) {
	t.Helper()
	for _, issue := range issues {
		if issue.Code == code {
			return
		}
	}
	t.Fatalf("issue %s not found in %+v", code, issues)
}
