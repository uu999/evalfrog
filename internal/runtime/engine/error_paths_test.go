package engine

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/runtime"
)

func TestEngineCommandGuardsAndReadQueries(t *testing.T) {
	harness := newTestHarness(t, linearDocument(1))
	ready := harness.ReadyIDs()[0]
	if applied, err := harness.Engine.RequestCancel(time.Time{}, "invalid"); err == nil || applied {
		t.Fatalf("zero-time cancel applied=%v err=%v", applied, err)
	}
	if err := harness.Engine.CancelAttempt("missing", harness.Now()); err == nil {
		t.Fatal("missing attempt cancellation accepted")
	}
	if state, exists := harness.Engine.EdgeState(eid(1)); !exists || state != EdgeActive {
		t.Fatalf("entry edge state=%s exists=%v", state, exists)
	}
	if _, exists := harness.Engine.EdgeState("missing"); exists {
		t.Fatal("missing edge exists")
	}
	if _, exists := harness.Engine.Node("missing"); exists {
		t.Fatal("missing node exists")
	}
	if _, exists := harness.Engine.Attempt("missing"); exists {
		t.Fatal("missing attempt exists")
	}
	if _, err := harness.Engine.QueueNode("missing", "attempt-missing-node"); err == nil {
		t.Fatal("missing node was queued")
	}
	if _, err := harness.Engine.QueueNode(ready, "attempt-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := harness.Engine.QueueNode(ready, "attempt-1"); err == nil {
		t.Fatal("duplicate attempt id was accepted")
	}
	if err := harness.Engine.StartAttempt("missing"); err == nil {
		t.Fatal("missing attempt was started")
	}
	if err := harness.Engine.StartAttempt("attempt-1"); err != nil {
		t.Fatal(err)
	}
	if err := harness.Engine.StartAttempt("attempt-1"); err != nil {
		t.Fatalf("start replay: %v", err)
	}
	if _, err := harness.Engine.RecordAttemptResult("missing", runtime.AttemptResult{State: runtime.AttemptFailed}); err == nil {
		t.Fatal("missing attempt result was recorded")
	}
	if err := harness.Engine.HandleAttemptCompleted("missing", harness.Now()); err == nil {
		t.Fatal("missing completion was handled")
	}
	if err := harness.Engine.HandleAttemptCompleted("attempt-1", time.Time{}); err == nil {
		t.Fatal("zero completion time was accepted")
	}
	if err := harness.Engine.HandleAttemptCompleted("attempt-1", harness.Engine.Run().CreatedAt().Add(-time.Nanosecond)); err == nil {
		t.Fatal("completion before run creation was accepted")
	}
	if err := harness.Engine.HandleAttemptCompleted("attempt-1", harness.Now()); err == nil {
		t.Fatal("completion without an authoritative attempt result was accepted")
	}
	result := runtime.AttemptResult{State: runtime.AttemptSucceeded, Outputs: outputsFor(harness.Engine.nodeDefs[ready], 1)}
	if accepted, err := harness.Engine.RecordAttemptResult("attempt-1", result); err != nil || !accepted {
		t.Fatalf("accepted=%v err=%v", accepted, err)
	}
	conflict := runtime.AttemptResult{State: runtime.AttemptFailed, ErrorCode: "DIFFERENT"}
	if _, err := harness.Engine.RecordAttemptResult("attempt-1", conflict); err == nil {
		t.Fatal("conflicting terminal result was accepted")
	}
	if err := harness.Engine.HandleAttemptCompleted("attempt-1", harness.Now()); err != nil {
		t.Fatal(err)
	}
	if err := harness.Engine.StartAttempt("attempt-1"); err == nil {
		t.Fatal("terminal attempt was restarted")
	}
	if err := harness.Engine.RetryDue("missing", harness.Now()); err == nil {
		t.Fatal("missing retry node was accepted")
	}
	if err := harness.Engine.RetryDue(ready, harness.Now()); err != nil {
		t.Fatalf("late retry signal: %v", err)
	}
}

func TestTerminatingRunRejectsDispatchAndSettlesAttemptResults(t *testing.T) {
	for _, result := range []runtime.AttemptResult{
		{State: runtime.AttemptFailed, ErrorCode: "FAILED", Message: "failed while canceling"},
		{State: runtime.AttemptSucceeded, Outputs: map[string]json.RawMessage{"result": json.RawMessage(`"wrong"`)}},
	} {
		harness := newTestHarness(t, linearDocument(1))
		ready := harness.ReadyIDs()[0]
		attempts, _ := harness.StartReady()
		if applied, err := harness.Engine.RequestCancel(harness.Now(), "stop"); err != nil || !applied {
			t.Fatalf("cancel applied=%v err=%v", applied, err)
		}
		if len(harness.Engine.ReadyNodeIDs()) != 0 {
			t.Fatal("terminating run exposed ready work")
		}
		if _, err := harness.Engine.QueueNode(ready, "late"); err == nil {
			t.Fatal("terminating run accepted dispatch")
		}
		if accepted, err := harness.Engine.RecordAttemptResult(attempts[0], result); err != nil || !accepted {
			t.Fatalf("record accepted=%v err=%v", accepted, err)
		}
		if err := harness.Engine.HandleAttemptCompleted(attempts[0], harness.Now()); err != nil {
			t.Fatal(err)
		}
		if harness.Engine.Run().State() != runtime.RunCanceled {
			t.Fatalf("run=%s", harness.Engine.Run().State())
		}
	}
}

func TestAttemptCanceledAndLostBudgetExhaustionFailRun(t *testing.T) {
	tests := []runtime.AttemptResult{
		{State: runtime.AttemptCanceled, ErrorCode: "WORKER_CANCELED"},
		{State: runtime.AttemptLost, ErrorCode: "LEASE_LOST"},
	}
	for _, result := range tests {
		policy := dsl.ExecutionPolicy{MaxAttempts: 1, MaxRecoveries: 0, AttemptTimeoutMS: 1000, RetryBackoff: &dsl.RetryBackoff{Kind: "fixed", DelayMS: 10}}
		harness := newTestHarness(t, linearDocumentWithPolicy(1, policy))
		attempts, _ := harness.StartReady()
		if err := harness.Complete(attempts[0], result); err != nil {
			t.Fatal(err)
		}
		if harness.Engine.Run().State() != runtime.RunFailed {
			t.Fatalf("result=%s run=%s", result.State, harness.Engine.Run().State())
		}
	}
}

func TestBranchAndJSONErrorBoundaries(t *testing.T) {
	node := dsl.Node{ID: nid(2), Operation: dsl.Operation{Config: map[string]json.RawMessage{}}}
	if _, err := evaluateBranch(node, json.RawMessage(`true`)); err == nil {
		t.Fatal("missing branch config accepted")
	}
	node.Operation.Config = map[string]json.RawMessage{"cases": json.RawMessage(`[{"route":"x","operator":"eq","value":true}]`), "default_route": json.RawMessage(`"fallback"`)}
	if _, err := evaluateBranch(node, json.RawMessage(`{`)); err == nil {
		t.Fatal("invalid branch input accepted")
	}
	node.Operation.Config["cases"] = json.RawMessage(`[{"route":"x","operator":"eq","value":{]`)
	if _, err := evaluateBranch(node, json.RawMessage(`true`)); err == nil {
		t.Fatal("invalid comparison value accepted")
	}
	comparisons := []struct {
		actual   any
		operator string
		expected any
	}{
		{json.Number("x"), "gt", json.Number("1")},
		{"frog", "contains", true},
		{true, "contains", true},
		{true, "starts_with", "x"},
		{true, "has_key", "x"},
		{true, "unknown", true},
	}
	for _, test := range comparisons {
		if _, err := compareBranch(test.actual, test.operator, test.expected); err == nil {
			t.Fatalf("comparison accepted: %#v", test)
		}
	}
	if _, err := equalJSON(make(chan int), make(chan int)); err == nil {
		t.Fatal("unsupported JSON value was compared")
	}
}

func TestRawDataTypeMatrixAndEngineErrorUnwrap(t *testing.T) {
	tests := []struct {
		raw  string
		want dsl.DataType
		ok   bool
	}{
		{`"frog"`, dsl.TypeString, true}, {`true`, dsl.TypeBoolean, true},
		{`1`, dsl.TypeInteger, true}, {`1.5`, dsl.TypeNumber, true},
		{`[]`, dsl.TypeArray, true}, {`{}`, dsl.TypeObject, true},
		{`null`, "", false}, {`{`, "", false},
	}
	for _, test := range tests {
		got, ok := rawDataType(json.RawMessage(test.raw))
		if got != test.want || ok != test.ok {
			t.Fatalf("raw=%s got=%s,%v want=%s,%v", test.raw, got, ok, test.want, test.ok)
		}
	}
	cause := errors.New("cause")
	err := &Error{Code: "CODE", NodeID: "node", Cause: cause}
	if err.Error() != "CODE at node node" || !errors.Is(err, cause) {
		t.Fatalf("error=%s unwrap=%v", err, errors.Unwrap(err))
	}
	if (&Error{Code: "CODE"}).Error() != "CODE" {
		t.Fatal("coordinate-free error changed")
	}
}

func TestOutputContractsCoverAllTopLevelTypes(t *testing.T) {
	node := taskNode(nid(2), dsl.ExecutionPolicy{})
	tests := []struct {
		dataType dsl.DataType
		value    string
		ok       bool
	}{
		{dsl.TypeString, `"frog"`, true}, {dsl.TypeBoolean, `true`, true},
		{dsl.TypeInteger, `1`, true}, {dsl.TypeNumber, `1`, true},
		{dsl.TypeArray, `[]`, true}, {dsl.TypeObject, `{}`, true},
		{dsl.TypeObject, `null`, false},
	}
	for _, test := range tests {
		node.Outputs = map[dsl.PortName]dsl.DataType{"value": test.dataType}
		err := validateOutputs(node, map[string]json.RawMessage{"value": json.RawMessage(test.value)})
		if (err == nil) != test.ok {
			t.Fatalf("type=%s value=%s err=%v", test.dataType, test.value, err)
		}
	}
	node.Outputs = map[dsl.PortName]dsl.DataType{"value": dsl.TypeObject}
	if err := validateOutputs(node, nil); err == nil {
		t.Fatal("missing output field accepted")
	}
	if err := validateOutputs(node, map[string]json.RawMessage{"other": json.RawMessage(`{}`)}); err == nil {
		t.Fatal("wrong output field accepted")
	}
}

func TestResolveInputDefensiveBoundaries(t *testing.T) {
	harness := newTestHarness(t, linearDocument(1))
	taskID := harness.ReadyIDs()[0]
	definition := harness.Engine.nodeDefs[taskID]
	definitions := []dsl.InputBinding{
		{Kind: dsl.BindingNodeOutput, DataType: dsl.TypeObject},
		{Kind: dsl.BindingNodeOutput, DataType: dsl.TypeObject, Output: &dsl.OutputReference{NodeID: "xn_ffffffffffffffffffffffff", Name: "value"}},
		{Kind: dsl.BindingNodeOutput, DataType: dsl.TypeObject, Output: &dsl.OutputReference{NodeID: taskID, Name: "result"}},
		{Kind: dsl.BindingNodeOutput, DataType: dsl.TypeObject, Output: &dsl.OutputReference{NodeID: harness.Engine.doc.EntryNodeID, Name: "missing"}},
		{Kind: dsl.InputBindingKind("unsupported"), DataType: dsl.TypeObject},
	}
	for _, binding := range definitions {
		definition.Inputs = map[dsl.PortName]dsl.InputBinding{"value": binding}
		_, ready, err := harness.Engine.resolveInputs(definition)
		if binding.Output != nil && binding.Output.NodeID == taskID {
			if err != nil || ready {
				t.Fatalf("unsettled source ready=%v err=%v", ready, err)
			}
			continue
		}
		if err == nil {
			t.Fatalf("invalid binding accepted: %+v", binding)
		}
	}
	definition.Inputs = map[dsl.PortName]dsl.InputBinding{"value": {Kind: dsl.BindingLiteral, DataType: dsl.TypeObject, Value: json.RawMessage(`{"stable":true}`)}}
	values, ready, err := harness.Engine.resolveInputs(definition)
	if err != nil || !ready || string(values["value"]) != `{"stable":true}` {
		t.Fatalf("values=%v ready=%v err=%v", values, ready, err)
	}
	values["value"][2] = 'X'
	if string(definition.Inputs["value"].Value) != `{"stable":true}` {
		t.Fatal("resolved literal aliases immutable DSL")
	}
}

func TestHarnessReportsConstructionAndCompletionErrors(t *testing.T) {
	if _, err := NewHarness(Snapshot{}, runtime.CreateRunCommand{}); err == nil {
		t.Fatal("invalid harness construction succeeded")
	}
	harness := newTestHarness(t, linearDocument(1))
	if err := harness.Complete("missing", runtime.AttemptResult{State: runtime.AttemptFailed}); err == nil {
		t.Fatal("missing harness attempt completed")
	}
}

func TestEngineConstructionAndTerminalControlReplays(t *testing.T) {
	harness := newTestHarness(t, linearDocument(1))
	command := runtime.CreateRunCommand{
		RunID: "run", ProjectID: "project", WorkflowID: "workflow", Purpose: runtime.RunPurposeTest,
		Definition: harness.Engine.Run().Definition(), WorkflowInput: json.RawMessage(`{}`),
		CreatedAt: harness.Now(), DeadlineAt: harness.Now().Add(time.Hour),
	}
	if _, err := New(Snapshot{ID: "snapshot", DefinitionHash: "hash", DSL: harness.Engine.snapshot.DSL}, command, dsl.BuiltinV1Contract(), dsl.NewCompatibilityChecker()); err == nil {
		t.Fatal("unsupported runtime compatibility accepted")
	}
	invalidDSL := harness.Engine.snapshot
	invalidDSL.DSL.Nodes = append([]dsl.Node(nil), harness.Engine.snapshot.DSL.Nodes...)
	invalidDSL.DSL.Nodes[0].Inputs = nil
	if _, err := New(invalidDSL, command, dsl.BuiltinV1Contract(), dsl.BuiltinV1Compatibility()); err == nil {
		t.Fatal("invalid DSL accepted")
	}
	invalidCommand := command
	invalidCommand.RunID = ""
	if _, err := New(harness.Engine.snapshot, invalidCommand, dsl.BuiltinV1Contract(), dsl.BuiltinV1Compatibility()); err == nil {
		t.Fatal("invalid run command accepted")
	}

	completeAllReady(t, harness)
	if applied, err := harness.Engine.RequestCancel(harness.Now(), "late"); err != nil || applied {
		t.Fatalf("terminal cancel applied=%v err=%v", applied, err)
	}
	if err := harness.Engine.RetryDue(nid(2), harness.Now()); err != nil {
		t.Fatalf("terminal retry signal: %v", err)
	}
}

func TestFailureDefaultsAndStructuralJSONMismatches(t *testing.T) {
	for _, pair := range []struct{ left, right any }{
		{[]any{true}, map[string]any{}},
		{[]any{true}, []any{}},
		{map[string]any{"a": true}, []any{}},
		{map[string]any{"a": true}, map[string]any{}},
		{map[string]any{"a": true}, map[string]any{"b": true}},
	} {
		equal, err := equalJSON(pair.left, pair.right)
		if err != nil || equal {
			t.Fatalf("left=%v right=%v equal=%v err=%v", pair.left, pair.right, equal, err)
		}
	}
	if comparableKinds(make(chan int), make(chan int)) {
		t.Fatal("unsupported comparable kind accepted")
	}
	if cancelReason(runtime.TerminationFailed) != FailureNodeFailed {
		t.Fatal("failure termination default reason changed")
	}
	if _, exists := lookupPath(map[string]any{"nested": true}, "nested.value"); exists {
		t.Fatal("path traversal through a scalar succeeded")
	}
	if _, err := compareBranch(json.Number("1"), "gt", "1"); err == nil {
		t.Fatal("mixed numeric comparison accepted")
	}
	if comparableKinds(json.Number("1"), "1") || !comparableKinds(nil, nil) || comparableKinds(nil, true) {
		t.Fatal("comparable kind boundary changed")
	}
	if equal, err := equalJSON(json.Number("1"), "1"); err != nil || equal {
		t.Fatalf("mixed number equality=%v err=%v", equal, err)
	}
	if equal, err := equalJSON(true, false); err != nil || equal {
		t.Fatalf("boolean equality=%v err=%v", equal, err)
	}
	if _, err := compareBranch("frog", "starts_with", true); err == nil {
		t.Fatal("mixed string comparison accepted")
	}
	if _, err := compareBranch(map[string]any{"frog": true}, "has_key", true); err == nil {
		t.Fatal("non-string object key accepted")
	}
	if comparableKinds("frog", true) || comparableKinds(true, "true") || comparableKinds([]any{}, map[string]any{}) || comparableKinds(map[string]any{}, []any{}) {
		t.Fatal("incompatible equality operands became comparable")
	}
	if _, err := compareBranch("frog", "eq", true); err == nil {
		t.Fatal("incompatible equality comparison accepted")
	}
	if _, err := compareBranch([]any{make(chan int)}, "contains", make(chan int)); err == nil {
		t.Fatal("unsupported array value comparison accepted")
	}
	if _, err := compareBranch("frog", "eq", "frog"); err != nil {
		t.Fatalf("valid equality comparison failed: %v", err)
	}
	if equal, err := equalJSON("frog", true); err != nil || equal {
		t.Fatalf("mixed string equality=%v err=%v", equal, err)
	}
	if equal, err := equalJSON(nil, nil); err != nil || !equal {
		t.Fatalf("null equality=%v err=%v", equal, err)
	}
}
