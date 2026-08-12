package engine

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/runtime"
)

func TestLinearWorkflowCompletesAndControlNodesHaveNoAttempts(t *testing.T) {
	document := linearDocument(2)
	harness := newTestHarness(t, document)
	completeAllReady(t, harness)
	if harness.Engine.Run().State() != runtime.RunSucceeded {
		t.Fatalf("run state=%s", harness.Engine.Run().State())
	}
	if string(harness.Engine.Run().WorkflowOutput()) != `{"step":3}` {
		t.Fatalf("output=%s", harness.Engine.Run().WorkflowOutput())
	}
	if len(harness.Engine.attempts) != 2 {
		t.Fatalf("control nodes created attempts: %d", len(harness.Engine.attempts))
	}
}

func TestParallelORJoinWaitsForAllInputsAndRunsOnce(t *testing.T) {
	document, left, right, end := parallelDocument(2)
	harness := newTestHarness(t, document)
	attempts, err := harness.StartReady()
	if err != nil || len(attempts) != 2 {
		t.Fatalf("attempts=%v err=%v", attempts, err)
	}
	byNode := attemptByNode(harness.Engine)
	if err := harness.Succeed(byNode[right], outputsFor(harness.Engine.nodeDefs[right], 1)); err != nil {
		t.Fatal(err)
	}
	endRun, _ := harness.Engine.Node(end)
	if endRun.State() != runtime.NodePending {
		t.Fatalf("OR join completed early: %s", endRun.State())
	}
	if err := harness.Succeed(byNode[left], outputsFor(harness.Engine.nodeDefs[left], 2)); err != nil {
		t.Fatal(err)
	}
	endRun, _ = harness.Engine.Node(end)
	if endRun.State() != runtime.NodeSucceeded || harness.Engine.Run().State() != runtime.RunSucceeded {
		t.Fatalf("end=%s run=%s", endRun.State(), harness.Engine.Run().State())
	}
}

func TestBranchExclusiveRouteSkippedPropagationAndORJoin(t *testing.T) {
	document, branch, selected, skipped, skippedTail, end := branchDocument(json.RawMessage(`85`), "gte", json.RawMessage(`80`))
	harness := newTestHarness(t, document)
	branchRun, _ := harness.Engine.Node(branch)
	if branchRun.State() != runtime.NodeSucceeded || branchRun.SelectedRoute() != "selected" {
		t.Fatalf("branch=%s route=%s", branchRun.State(), branchRun.SelectedRoute())
	}
	for _, id := range []dsl.NodeID{skipped, skippedTail} {
		node, _ := harness.Engine.Node(id)
		if node.State() != runtime.NodeSkipped || node.Activated() {
			t.Fatalf("node %s state=%s activated=%v", id, node.State(), node.Activated())
		}
	}
	attempts, err := harness.StartReady()
	if err != nil || len(attempts) != 1 {
		t.Fatalf("attempts=%v err=%v", attempts, err)
	}
	if harness.Engine.attemptNodes[attempts[0]] != selected {
		t.Fatalf("wrong branch task queued")
	}
	if err := harness.Succeed(attempts[0], outputsFor(harness.Engine.nodeDefs[selected], 1)); err != nil {
		t.Fatal(err)
	}
	endRun, _ := harness.Engine.Node(end)
	if endRun.State() != runtime.NodeSucceeded || harness.Engine.Run().State() != runtime.RunSucceeded {
		t.Fatalf("end=%s run=%s", endRun.State(), harness.Engine.Run().State())
	}
}

func TestRetryLateAndDuplicateResultsConverge(t *testing.T) {
	document := linearDocumentWithPolicy(1, dsl.ExecutionPolicy{MaxAttempts: 2, MaxRecoveries: 2, AttemptTimeoutMS: 1000, RetryBackoff: &dsl.RetryBackoff{Kind: "fixed", DelayMS: 10}, RetryableErrorCodes: []string{"TEMP"}})
	harness := newTestHarness(t, document)
	first, _ := harness.StartReady()
	if err := harness.Fail(first[0], "TEMP"); err != nil {
		t.Fatal(err)
	}
	nodeID := harness.Engine.attemptNodes[first[0]]
	node, _ := harness.Engine.Node(nodeID)
	if node.State() != runtime.NodeRetryWait || node.BusinessAttemptCount() != 1 {
		t.Fatalf("state=%s attempts=%d", node.State(), node.BusinessAttemptCount())
	}
	if node.NextRetryAt() != harness.Now().Add(10*time.Millisecond) {
		t.Fatalf("next retry at=%s", node.NextRetryAt())
	}
	if err := harness.Engine.RetryDue(nodeID, harness.Now()); err != nil {
		t.Fatal(err)
	}
	node, _ = harness.Engine.Node(nodeID)
	if node.State() != runtime.NodeRetryWait {
		t.Fatalf("early retry due changed state=%s", node.State())
	}
	harness.Advance(10 * time.Millisecond)
	if err := harness.Engine.RetryDue(nodeID, harness.Now()); err != nil {
		t.Fatal(err)
	}
	second, _ := harness.StartReady()
	if err := harness.Fail(first[0], "TEMP"); err != nil {
		t.Fatalf("late old completion must be ignored: %v", err)
	}
	node, _ = harness.Engine.Node(nodeID)
	if node.CurrentAttemptID() != second[0] || node.State() != runtime.NodeRunning {
		t.Fatalf("late result changed current attempt")
	}
	result := outputsFor(harness.Engine.nodeDefs[nodeID], 9)
	if err := harness.Succeed(second[0], result); err != nil {
		t.Fatal(err)
	}
	node, _ = harness.Engine.Node(nodeID)
	version := node.StateVersion()
	if err := harness.Succeed(second[0], result); err != nil {
		t.Fatalf("duplicate completion: %v", err)
	}
	node, _ = harness.Engine.Node(nodeID)
	if node.StateVersion() != version || harness.Engine.Run().State() != runtime.RunSucceeded {
		t.Fatalf("duplicate changed state")
	}
}

func TestLostRecoveryDoesNotConsumeBusinessRetryBudget(t *testing.T) {
	policy := dsl.ExecutionPolicy{MaxAttempts: 1, MaxRecoveries: 1, AttemptTimeoutMS: 1000, RetryBackoff: &dsl.RetryBackoff{Kind: "fixed", DelayMS: 10}, RetryableErrorCodes: []string{}}
	harness := newTestHarness(t, linearDocumentWithPolicy(1, policy))
	first, _ := harness.StartReady()
	if err := harness.Complete(first[0], runtime.AttemptResult{State: runtime.AttemptLost, ErrorCode: "LEASE_LOST"}); err != nil {
		t.Fatal(err)
	}
	nodeID := harness.Engine.attemptNodes[first[0]]
	node, _ := harness.Engine.Node(nodeID)
	harness.Advance(10 * time.Millisecond)
	if err := harness.Engine.RetryDue(nodeID, harness.Now()); err != nil {
		t.Fatal(err)
	}
	second, _ := harness.StartReady()
	node, _ = harness.Engine.Node(nodeID)
	if node.BusinessAttemptCount() != 1 || node.RecoveryCount() != 1 {
		t.Fatalf("business=%d recovery=%d", node.BusinessAttemptCount(), node.RecoveryCount())
	}
	if err := harness.Succeed(second[0], outputsFor(harness.Engine.nodeDefs[nodeID], 1)); err != nil {
		t.Fatal(err)
	}
}

func TestLateRetryDueAfterNewAttemptIsNoOp(t *testing.T) {
	document := linearDocumentWithPolicy(1, dsl.ExecutionPolicy{MaxAttempts: 2, MaxRecoveries: 1, AttemptTimeoutMS: 1000, RetryBackoff: &dsl.RetryBackoff{Kind: "fixed", DelayMS: 10}, RetryableErrorCodes: []string{"TEMP"}})
	harness := newTestHarness(t, document)
	first, _ := harness.StartReady()
	if err := harness.Fail(first[0], "TEMP"); err != nil {
		t.Fatal(err)
	}
	nodeID := harness.Engine.attemptNodes[first[0]]
	harness.Advance(10 * time.Millisecond)
	if err := harness.Engine.RetryDue(nodeID, harness.Now()); err != nil {
		t.Fatal(err)
	}
	second, _ := harness.StartReady()
	if err := harness.Engine.RetryDue(nodeID, harness.Now()); err != nil {
		t.Fatalf("late retry signal should be ignored: %v", err)
	}
	node, _ := harness.Engine.Node(nodeID)
	if node.State() != runtime.NodeRunning || node.CurrentAttemptID() != second[0] {
		t.Fatalf("late retry changed node: state=%s attempt=%s", node.State(), node.CurrentAttemptID())
	}
}

func TestAttemptTimeoutExhaustionFailsNodeAndRun(t *testing.T) {
	harness := newTestHarness(t, linearDocument(1))
	attempts, _ := harness.StartReady()
	nodeID := harness.Engine.attemptNodes[attempts[0]]
	result := runtime.AttemptResult{State: runtime.AttemptTimedOut, ErrorCode: FailureNodeTimeout, Message: "attempt deadline reached"}
	if err := harness.Complete(attempts[0], result); err != nil {
		t.Fatal(err)
	}
	node, _ := harness.Engine.Node(nodeID)
	if node.State() != runtime.NodeTimedOut || harness.Engine.Run().State() != runtime.RunFailed {
		t.Fatalf("node=%s run=%s", node.State(), harness.Engine.Run().State())
	}
}

func TestDeadlineBeforeDeadlineIsIgnoredAndRepeatedTerminalSignalIsNoOp(t *testing.T) {
	harness := newTestHarness(t, linearDocument(1))
	if applied, err := harness.Engine.DeadlineReached(harness.Engine.Run().DeadlineAt().Add(-time.Second)); err != nil || applied {
		t.Fatalf("early deadline applied=%v err=%v", applied, err)
	}
	attempts, _ := harness.StartReady()
	if applied, err := harness.Engine.DeadlineReached(harness.Engine.Run().DeadlineAt()); err != nil || !applied {
		t.Fatalf("deadline applied=%v err=%v", applied, err)
	}
	if err := harness.Engine.CancelAttempt(attempts[0], harness.Engine.Run().DeadlineAt()); err != nil {
		t.Fatal(err)
	}
	if applied, err := harness.Engine.DeadlineReached(harness.Engine.Run().DeadlineAt().Add(time.Second)); err != nil || applied {
		t.Fatalf("terminal replay applied=%v err=%v", applied, err)
	}
	if harness.Engine.Run().State() != runtime.RunTimedOut {
		t.Fatalf("run=%s", harness.Engine.Run().State())
	}
}

func TestFailFastCannotBeMaskedByParallelSuccess(t *testing.T) {
	document, left, right, _ := parallelDocument(2)
	harness := newTestHarness(t, document)
	_, _ = harness.StartReady()
	byNode := attemptByNode(harness.Engine)
	if err := harness.Fail(byNode[left], "PERMANENT"); err != nil {
		t.Fatal(err)
	}
	if harness.Engine.Run().State() != runtime.RunRunning {
		t.Fatalf("in-flight attempt should settle before final failure")
	}
	if err := harness.Succeed(byNode[right], outputsFor(harness.Engine.nodeDefs[right], 1)); err != nil {
		t.Fatal(err)
	}
	if harness.Engine.Run().State() != runtime.RunFailed {
		t.Fatalf("parallel success masked failure: %s", harness.Engine.Run().State())
	}
}

func TestFirstTerminationIntentWinsAcrossCancelAndDeadline(t *testing.T) {
	harness := newTestHarness(t, linearDocument(1))
	attempts, _ := harness.StartReady()
	if applied, err := harness.Engine.RequestCancel(harness.Now(), "user request"); err != nil || !applied {
		t.Fatalf("cancel applied=%v err=%v", applied, err)
	}
	if applied, err := harness.Engine.DeadlineReached(harness.Engine.Run().DeadlineAt()); err != nil || applied {
		t.Fatalf("deadline applied=%v err=%v", applied, err)
	}
	if err := harness.Engine.CancelAttempt(attempts[0], harness.Now()); err != nil {
		t.Fatal(err)
	}
	intent, _ := harness.Engine.Run().Termination()
	if intent.Kind != runtime.TerminationCanceled || harness.Engine.Run().State() != runtime.RunCanceled {
		t.Fatalf("intent=%s run=%s", intent.Kind, harness.Engine.Run().State())
	}
}

func TestOutputContractViolationFailsRunWithoutEffectiveOutput(t *testing.T) {
	harness := newTestHarness(t, linearDocument(1))
	attempts, _ := harness.StartReady()
	nodeID := harness.Engine.attemptNodes[attempts[0]]
	if err := harness.Succeed(attempts[0], map[string]json.RawMessage{"result": json.RawMessage(`"wrong"`)}); err != nil {
		t.Fatal(err)
	}
	node, _ := harness.Engine.Node(nodeID)
	if node.State() != runtime.NodeFailed || node.EffectiveAttemptID() != "" || len(node.EffectiveOutputs()) != 0 || harness.Engine.Run().State() != runtime.RunFailed {
		t.Fatalf("node=%s effective=%s run=%s", node.State(), node.EffectiveAttemptID(), harness.Engine.Run().State())
	}
}

func TestAttemptCandidateIsNotEffectiveUntilEngineHandlesCompletion(t *testing.T) {
	harness := newTestHarness(t, linearDocument(1))
	attempts, _ := harness.StartReady()
	attemptID := attempts[0]
	nodeID := harness.Engine.attemptNodes[attemptID]
	outputs := outputsFor(harness.Engine.nodeDefs[nodeID], 7)
	accepted, err := harness.Engine.RecordAttemptResult(attemptID, runtime.AttemptResult{State: runtime.AttemptSucceeded, Outputs: outputs})
	if err != nil || !accepted {
		t.Fatalf("record candidate: accepted=%v err=%v", accepted, err)
	}
	node, _ := harness.Engine.Node(nodeID)
	if node.State() != runtime.NodeRunning || node.EffectiveAttemptID() != "" || len(node.EffectiveOutputs()) != 0 {
		t.Fatalf("candidate leaked into semantic state: node=%s effective=%s", node.State(), node.EffectiveAttemptID())
	}
	if accepted, err := harness.Engine.RecordAttemptResult(attemptID, runtime.AttemptResult{State: runtime.AttemptSucceeded, Outputs: outputs}); err != nil || accepted {
		t.Fatalf("record replay: accepted=%v err=%v", accepted, err)
	}
	if err := harness.Engine.HandleAttemptCompleted(attemptID, harness.Now()); err != nil {
		t.Fatal(err)
	}
	node, _ = harness.Engine.Node(nodeID)
	if node.State() != runtime.NodeSucceeded || node.EffectiveAttemptID() != attemptID {
		t.Fatalf("completion was not accepted: node=%s effective=%s", node.State(), node.EffectiveAttemptID())
	}
}

func TestEngineQueriesAndQueueReturnReadOnlyCopies(t *testing.T) {
	harness := newTestHarness(t, linearDocument(1))
	nodeID := harness.ReadyIDs()[0]
	attempt, err := harness.Engine.QueueNode(nodeID, "copy-attempt")
	if err != nil {
		t.Fatal(err)
	}
	if err := attempt.Start(); err != nil {
		t.Fatal(err)
	}
	stored, _ := harness.Engine.Attempt("copy-attempt")
	if stored.State() != runtime.AttemptQueued {
		t.Fatalf("returned attempt mutated engine state: %s", stored.State())
	}
	node, _ := harness.Engine.Node(nodeID)
	if err := node.Cancel("external-mutation"); err != nil {
		t.Fatal(err)
	}
	storedNode, _ := harness.Engine.Node(nodeID)
	if storedNode.State() != runtime.NodeQueued {
		t.Fatalf("returned node mutated engine state: %s", storedNode.State())
	}
}

func TestBranchPathFailureIsStructuredRuntimeFailure(t *testing.T) {
	document, branch, _, _, _, _ := branchDocument(json.RawMessage(`{"score":85}`), "gte", json.RawMessage(`80`))
	for index := range document.Nodes {
		if document.Nodes[index].ID == branch {
			document.Nodes[index].Operation.Config["cases"] = json.RawMessage(`[{"route":"selected","path":"missing","operator":"gte","value":80}]`)
		}
	}
	harness := newTestHarness(t, document)
	node, _ := harness.Engine.Node(branch)
	failure, exists := node.Failure()
	if !exists || failure.Code != FailureBranchPathNotFound || harness.Engine.Run().State() != runtime.RunFailed {
		t.Fatalf("failure=%+v exists=%v run=%s", failure, exists, harness.Engine.Run().State())
	}
	if failure.RunID != "run" || failure.SnapshotID != "snapshot" || failure.DefinitionHash != "hash" || failure.ExecutionNodeID != string(branch) || failure.DSLField == "" {
		t.Fatalf("failure coordinates are incomplete: %+v", failure)
	}
}

func TestEngineOwnsImmutableSnapshotCopy(t *testing.T) {
	document := linearDocument(1)
	harness := newTestHarness(t, document)
	document.Nodes[1].Outputs["result"] = dsl.TypeString
	attempts, err := harness.StartReady()
	if err != nil {
		t.Fatal(err)
	}
	if err := harness.Succeed(attempts[0], map[string]json.RawMessage{"result": json.RawMessage(`{"stable":true}`)}); err != nil {
		t.Fatal(err)
	}
	if harness.Engine.Run().State() != runtime.RunSucceeded {
		t.Fatalf("external DSL mutation changed engine snapshot: %s", harness.Engine.Run().State())
	}
}

func TestBranchStructuralEqualityUsesDecimalSemantics(t *testing.T) {
	actual, _ := decodeJSON(json.RawMessage(`{"values":[1,{"score":1.0}]}`))
	expected, _ := decodeJSON(json.RawMessage(`{"values":[1.0,{"score":1}]}`))
	equal, err := equalJSON(actual, expected)
	if err != nil || !equal {
		t.Fatalf("decimal structural equality: equal=%v err=%v", equal, err)
	}
}

func TestBranchPathEqualityTypeMismatchFailsInsteadOfSelectingDefault(t *testing.T) {
	document, branch, _, _, _, _ := branchDocument(json.RawMessage(`{"status":"ready"}`), "eq", json.RawMessage(`true`))
	for index := range document.Nodes {
		if document.Nodes[index].ID == branch {
			document.Nodes[index].Operation.Config["cases"] = json.RawMessage(`[{"route":"selected","path":"status","operator":"eq","value":true}]`)
		}
	}
	harness := newTestHarness(t, document)
	node, _ := harness.Engine.Node(branch)
	failure, exists := node.Failure()
	if !exists || failure.Code != FailureBranchOperatorMismatch || harness.Engine.Run().State() != runtime.RunFailed {
		t.Fatalf("failure=%+v exists=%v run=%s", failure, exists, harness.Engine.Run().State())
	}
}

func TestBranchOperatorMatrix(t *testing.T) {
	tests := []struct {
		name     string
		actual   string
		operator string
		expected string
		matched  bool
	}{
		{"string eq", `"frog"`, "eq", `"frog"`, true}, {"string neq", `"frog"`, "neq", `"cat"`, true},
		{"string contains", `"evalfrog"`, "contains", `"frog"`, true}, {"string not contains", `"frog"`, "not_contains", `"cat"`, true},
		{"string starts", `"evalfrog"`, "starts_with", `"eval"`, true}, {"string ends", `"evalfrog"`, "ends_with", `"frog"`, true},
		{"integer eq", `2`, "eq", `2.0`, true}, {"integer neq", `2`, "neq", `3`, true},
		{"integer gt", `3`, "gt", `2`, true}, {"integer gte", `2`, "gte", `2`, true},
		{"integer lt", `1`, "lt", `2`, true}, {"integer lte", `2`, "lte", `2`, true},
		{"number eq", `1.5`, "eq", `1.50`, true}, {"number neq", `1.5`, "neq", `1.6`, true},
		{"number gt", `1.6`, "gt", `1.5`, true}, {"number gte", `1.5`, "gte", `1.5`, true},
		{"number lt", `1.4`, "lt", `1.5`, true}, {"number lte", `1.5`, "lte", `1.5`, true},
		{"boolean eq", `true`, "eq", `true`, true}, {"boolean neq", `true`, "neq", `false`, true},
		{"array eq", `[1,{"v":1.0}]`, "eq", `[1.0,{"v":1}]`, true}, {"array neq", `[1]`, "neq", `[2]`, true},
		{"array contains", `[1,{"v":2}]`, "contains", `{"v":2.0}`, true}, {"array not contains", `[1,2]`, "not_contains", `3`, true},
		{"object eq", `{"a":1,"b":2}`, "eq", `{"b":2.0,"a":1.0}`, true}, {"object neq", `{"a":1}`, "neq", `{"a":2}`, true},
		{"object has key", `{"a":null}`, "has_key", `"a"`, true}, {"object not has key", `{"a":1}`, "not_has_key", `"b"`, true},
		{"false comparison", `1`, "gt", `2`, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, err := decodeJSON(json.RawMessage(test.actual))
			if err != nil {
				t.Fatal(err)
			}
			expected, err := decodeJSON(json.RawMessage(test.expected))
			if err != nil {
				t.Fatal(err)
			}
			matched, err := compareBranch(actual, test.operator, expected)
			if err != nil || matched != test.matched {
				t.Fatalf("matched=%v want=%v err=%v", matched, test.matched, err)
			}
		})
	}
}

func TestSnapshotBindingMismatchFailsBeforeInitialization(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	snapshot := Snapshot{ID: "snapshot-a", DefinitionHash: "hash-a", DSL: linearDocument(1)}
	command := runtime.CreateRunCommand{
		RunID: "run", ProjectID: "project", WorkflowID: "workflow", Purpose: runtime.RunPurposeTest,
		Definition:    runtime.DefinitionReference{SnapshotID: "snapshot-b", DefinitionHash: "hash-a", Source: runtime.DefinitionDraftSnapshot},
		WorkflowInput: json.RawMessage(`{}`), CreatedAt: now, DeadlineAt: now.Add(time.Hour),
	}
	if _, err := NewBuiltinV1(snapshot, command); err == nil {
		t.Fatal("mismatched immutable snapshot binding was accepted")
	}
}

func TestAllInactiveORJoinIsSkipped(t *testing.T) {
	document, join := allInactiveJoinDocument()
	harness := newTestHarness(t, document)
	joinRun, _ := harness.Engine.Node(join)
	if joinRun.State() != runtime.NodeSkipped || joinRun.Activated() {
		t.Fatalf("join=%s activated=%v", joinRun.State(), joinRun.Activated())
	}
	completeAllReady(t, harness)
	if harness.Engine.Run().State() != runtime.RunSucceeded {
		t.Fatalf("run=%s", harness.Engine.Run().State())
	}
}

func TestTwentyRepresentativeWorkflowFixtures(t *testing.T) {
	fixtures := make([]struct {
		name     string
		document dsl.Document
	}, 0, 20)
	for count := 1; count <= 8; count++ {
		fixtures = append(fixtures, struct {
			name     string
			document dsl.Document
		}{fmt.Sprintf("linear_%d", count), linearDocument(count)})
	}
	for width := 2; width <= 5; width++ {
		document, _, _, _ := parallelDocument(width)
		fixtures = append(fixtures, struct {
			name     string
			document dsl.Document
		}{fmt.Sprintf("parallel_%d", width), document})
	}
	branchValues := []struct {
		name     string
		actual   json.RawMessage
		operator string
		expected json.RawMessage
	}{
		{"integer_case", json.RawMessage(`85`), "gte", json.RawMessage(`80`)},
		{"integer_default", json.RawMessage(`5`), "gte", json.RawMessage(`80`)},
		{"number", json.RawMessage(`1.25`), "gt", json.RawMessage(`1.2`)},
		{"string", json.RawMessage(`"evalfrog"`), "starts_with", json.RawMessage(`"eval"`)},
		{"boolean", json.RawMessage(`true`), "eq", json.RawMessage(`true`)},
		{"array", json.RawMessage(`[1,2]`), "contains", json.RawMessage(`2`)},
		{"object", json.RawMessage(`{"ready":true}`), "has_key", json.RawMessage(`"ready"`)},
		{"not_contains", json.RawMessage(`"frog"`), "not_contains", json.RawMessage(`"cat"`)},
	}
	for _, branch := range branchValues {
		document, _, _, _, _, _ := branchDocument(branch.actual, branch.operator, branch.expected)
		fixtures = append(fixtures, struct {
			name     string
			document dsl.Document
		}{"branch_" + branch.name, document})
	}
	if len(fixtures) != 20 {
		t.Fatalf("fixture count=%d", len(fixtures))
	}
	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			harness := newTestHarness(t, fixture.document)
			completeAllReady(t, harness)
			if harness.Engine.Run().State() != runtime.RunSucceeded {
				t.Fatalf("run=%s", harness.Engine.Run().State())
			}
		})
	}
}

func TestPropertyRandomDAGDuplicateCompletionsConverge(t *testing.T) {
	for seed := int64(1); seed <= 100; seed++ {
		random := rand.New(rand.NewSource(seed))
		document := layeredDAG(random, 2+random.Intn(4), 2+random.Intn(4))
		harness := newTestHarness(t, document)
		for harness.Engine.Run().State() == runtime.RunRunning {
			attempts, err := harness.StartReady()
			if err != nil {
				t.Fatalf("seed=%d: %v", seed, err)
			}
			if len(attempts) == 0 {
				t.Fatalf("seed=%d stalled", seed)
			}
			random.Shuffle(len(attempts), func(i, j int) { attempts[i], attempts[j] = attempts[j], attempts[i] })
			for _, attemptID := range attempts {
				nodeID := harness.Engine.attemptNodes[attemptID]
				outputs := outputsFor(harness.Engine.nodeDefs[nodeID], int(seed))
				if err := harness.Succeed(attemptID, outputs); err != nil {
					t.Fatalf("seed=%d: %v", seed, err)
				}
				if err := harness.Succeed(attemptID, outputs); err != nil {
					t.Fatalf("seed=%d replay: %v", seed, err)
				}
			}
		}
		if harness.Engine.Run().State() != runtime.RunSucceeded {
			t.Fatalf("seed=%d run=%s", seed, harness.Engine.Run().State())
		}
	}
}

func newTestHarness(t *testing.T, document dsl.Document) *Harness {
	t.Helper()
	now := time.Unix(1_700_000_000, 0).UTC()
	snapshot := Snapshot{ID: "snapshot", DefinitionHash: "hash", DSL: document}
	command := runtime.CreateRunCommand{RunID: "run", ProjectID: "project", WorkflowID: "workflow", Purpose: runtime.RunPurposeTest, Definition: runtime.DefinitionReference{SnapshotID: snapshot.ID, DefinitionHash: snapshot.DefinitionHash, Source: runtime.DefinitionDraftSnapshot}, WorkflowInput: json.RawMessage(`{"input":true}`), CreatedAt: now, DeadlineAt: now.Add(time.Hour)}
	harness, err := NewHarness(snapshot, command)
	if err != nil {
		t.Fatalf("new harness: %v", err)
	}
	return harness
}

func completeAllReady(t *testing.T, harness *Harness) {
	t.Helper()
	for harness.Engine.Run().State() == runtime.RunRunning {
		attempts, err := harness.StartReady()
		if err != nil {
			t.Fatal(err)
		}
		if len(attempts) == 0 {
			t.Fatalf("engine stalled")
		}
		for _, attemptID := range attempts {
			nodeID := harness.Engine.attemptNodes[attemptID]
			if err := harness.Succeed(attemptID, outputsFor(harness.Engine.nodeDefs[nodeID], indexFromNode(nodeID))); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func outputsFor(node dsl.Node, value int) map[string]json.RawMessage {
	outputs := make(map[string]json.RawMessage, len(node.Outputs))
	for name, dataType := range node.Outputs {
		switch dataType {
		case dsl.TypeObject:
			outputs[string(name)] = json.RawMessage(fmt.Sprintf(`{"step":%d}`, value))
		case dsl.TypeString:
			outputs[string(name)] = json.RawMessage(`"value"`)
		case dsl.TypeInteger:
			outputs[string(name)] = json.RawMessage(fmt.Sprintf(`%d`, value))
		case dsl.TypeNumber:
			outputs[string(name)] = json.RawMessage(`1.5`)
		case dsl.TypeBoolean:
			outputs[string(name)] = json.RawMessage(`true`)
		case dsl.TypeArray:
			outputs[string(name)] = json.RawMessage(`[]`)
		}
	}
	return outputs
}

func attemptByNode(engine *Engine) map[dsl.NodeID]string {
	result := map[dsl.NodeID]string{}
	for attempt, node := range engine.attemptNodes {
		result[node] = attempt
	}
	return result
}

func nid(index int) dsl.NodeID { return dsl.NodeID(fmt.Sprintf("xn_%024x", index)) }
func eid(index int) dsl.EdgeID { return dsl.EdgeID(fmt.Sprintf("xe_%024x", index)) }
func indexFromNode(id dsl.NodeID) int {
	var value int
	_, _ = fmt.Sscanf(string(id), "xn_%x", &value)
	return value
}

func startNode(id dsl.NodeID) dsl.Node {
	return dsl.Node{ID: id, Kind: dsl.KindControl, Operation: dsl.Operation{Type: "control.start", Version: 1, Config: map[string]json.RawMessage{}}, Inputs: map[dsl.PortName]dsl.InputBinding{}, Outputs: map[dsl.PortName]dsl.DataType{"workflow_input": dsl.TypeObject}}
}
func endNode(id dsl.NodeID, input dsl.InputBinding) dsl.Node {
	return dsl.Node{ID: id, Kind: dsl.KindControl, Operation: dsl.Operation{Type: "control.end", Version: 1, Config: map[string]json.RawMessage{}}, Inputs: map[dsl.PortName]dsl.InputBinding{"workflow_output": input}, Outputs: map[dsl.PortName]dsl.DataType{}}
}
func taskNode(id dsl.NodeID, policy dsl.ExecutionPolicy) dsl.Node {
	if policy.MaxAttempts == 0 {
		policy = dsl.ExecutionPolicy{MaxAttempts: 1, MaxRecoveries: 1, AttemptTimeoutMS: 1000, RetryBackoff: &dsl.RetryBackoff{Kind: "fixed", DelayMS: 10}, RetryableErrorCodes: []string{}}
	}
	return dsl.Node{ID: id, Kind: dsl.KindTask, Operation: dsl.Operation{Type: "task.python", Version: 1, Config: map[string]json.RawMessage{"source_code": json.RawMessage(`"def main(inputs): return {}"`), "sandbox_profile": json.RawMessage(`"test"`)}}, Inputs: map[dsl.PortName]dsl.InputBinding{}, Outputs: map[dsl.PortName]dsl.DataType{"result": dsl.TypeObject}, ExecutionPolicy: policy}
}
func edge(id dsl.EdgeID, source, target dsl.NodeID) dsl.Edge {
	return dsl.Edge{ID: id, SourceNodeID: source, TargetNodeID: target, Activation: dsl.Activation{Kind: dsl.ActivationAlways}}
}

func linearDocument(count int) dsl.Document {
	return linearDocumentWithPolicy(count, dsl.ExecutionPolicy{})
}
func linearDocumentWithPolicy(count int, policy dsl.ExecutionPolicy) dsl.Document {
	start, end := nid(1), nid(count+2)
	nodes := []dsl.Node{startNode(start)}
	edges := []dsl.Edge{}
	previous := start
	for i := 0; i < count; i++ {
		id := nid(i + 2)
		nodes = append(nodes, taskNode(id, policy))
		edges = append(edges, edge(eid(i+1), previous, id))
		previous = id
	}
	endInput := dsl.InputBinding{Kind: dsl.BindingNodeOutput, DataType: dsl.TypeObject, Output: &dsl.OutputReference{NodeID: previous, Name: "result"}}
	nodes = append(nodes, endNode(end, endInput))
	edges = append(edges, edge(eid(count+1), previous, end))
	return dsl.Document{DSLVersion: dsl.VersionV1, EntryNodeID: start, ExitNodeID: end, Nodes: nodes, Edges: edges}
}

func parallelDocument(width int) (dsl.Document, dsl.NodeID, dsl.NodeID, dsl.NodeID) {
	start, end := nid(1), nid(width+2)
	nodes := []dsl.Node{startNode(start)}
	edges := []dsl.Edge{}
	ids := []dsl.NodeID{}
	for i := 0; i < width; i++ {
		id := nid(i + 2)
		ids = append(ids, id)
		nodes = append(nodes, taskNode(id, dsl.ExecutionPolicy{}))
		edges = append(edges, edge(eid(i+1), start, id), edge(eid(width+i+1), id, end))
	}
	nodes = append(nodes, endNode(end, dsl.InputBinding{Kind: dsl.BindingLiteral, DataType: dsl.TypeObject, Value: json.RawMessage(`{}`)}))
	return dsl.Document{DSLVersion: dsl.VersionV1, EntryNodeID: start, ExitNodeID: end, Nodes: nodes, Edges: edges}, ids[0], ids[1], end
}

func branchDocument(actual json.RawMessage, operator string, expected json.RawMessage) (dsl.Document, dsl.NodeID, dsl.NodeID, dsl.NodeID, dsl.NodeID, dsl.NodeID) {
	start, branch, selected, skipped, tail, end := nid(1), nid(2), nid(3), nid(4), nid(5), nid(6)
	cases, _ := json.Marshal([]branchCase{{Route: "selected", Operator: operator, Value: expected}})
	branchNode := dsl.Node{ID: branch, Kind: dsl.KindControl, Operation: dsl.Operation{Type: "control.branch", Version: 1, Config: map[string]json.RawMessage{"cases": cases, "default_route": json.RawMessage(`"default"`)}}, Inputs: map[dsl.PortName]dsl.InputBinding{"value": {Kind: dsl.BindingLiteral, DataType: typeOfRaw(actual), Value: actual}}, Outputs: map[dsl.PortName]dsl.DataType{}}
	nodes := []dsl.Node{startNode(start), branchNode, taskNode(selected, dsl.ExecutionPolicy{}), taskNode(skipped, dsl.ExecutionPolicy{}), taskNode(tail, dsl.ExecutionPolicy{}), endNode(end, dsl.InputBinding{Kind: dsl.BindingLiteral, DataType: dsl.TypeObject, Value: json.RawMessage(`{}`)})}
	edges := []dsl.Edge{edge(eid(1), start, branch), {ID: eid(2), SourceNodeID: branch, TargetNodeID: selected, Activation: dsl.Activation{Kind: dsl.ActivationRoute, Route: "selected"}}, {ID: eid(3), SourceNodeID: branch, TargetNodeID: skipped, Activation: dsl.Activation{Kind: dsl.ActivationRoute, Route: "default"}}, edge(eid(4), selected, end), edge(eid(5), skipped, tail), edge(eid(6), tail, end)}
	return dsl.Document{DSLVersion: dsl.VersionV1, EntryNodeID: start, ExitNodeID: end, Nodes: nodes, Edges: edges}, branch, selected, skipped, tail, end
}

func allInactiveJoinDocument() (dsl.Document, dsl.NodeID) {
	start, branch, selected, inactiveA, inactiveB, join, end := nid(1), nid(2), nid(3), nid(4), nid(5), nid(6), nid(7)
	branchNode := dsl.Node{ID: branch, Kind: dsl.KindControl, Operation: dsl.Operation{Type: "control.branch", Version: 1, Config: map[string]json.RawMessage{"cases": json.RawMessage(`[{"route":"selected","operator":"eq","value":true}]`), "default_route": json.RawMessage(`"inactive"`)}}, Inputs: map[dsl.PortName]dsl.InputBinding{"value": {Kind: dsl.BindingLiteral, DataType: dsl.TypeBoolean, Value: json.RawMessage(`true`)}}, Outputs: map[dsl.PortName]dsl.DataType{}}
	nodes := []dsl.Node{startNode(start), branchNode, taskNode(selected, dsl.ExecutionPolicy{}), taskNode(inactiveA, dsl.ExecutionPolicy{}), taskNode(inactiveB, dsl.ExecutionPolicy{}), taskNode(join, dsl.ExecutionPolicy{}), endNode(end, dsl.InputBinding{Kind: dsl.BindingLiteral, DataType: dsl.TypeObject, Value: json.RawMessage(`{}`)})}
	edges := []dsl.Edge{
		edge(eid(1), start, branch),
		{ID: eid(2), SourceNodeID: branch, TargetNodeID: selected, Activation: dsl.Activation{Kind: dsl.ActivationRoute, Route: "selected"}},
		{ID: eid(3), SourceNodeID: branch, TargetNodeID: inactiveA, Activation: dsl.Activation{Kind: dsl.ActivationRoute, Route: "inactive"}},
		{ID: eid(4), SourceNodeID: branch, TargetNodeID: inactiveB, Activation: dsl.Activation{Kind: dsl.ActivationRoute, Route: "inactive"}},
		edge(eid(5), inactiveA, join), edge(eid(6), inactiveB, join), edge(eid(7), selected, end), edge(eid(8), join, end),
	}
	return dsl.Document{DSLVersion: dsl.VersionV1, EntryNodeID: start, ExitNodeID: end, Nodes: nodes, Edges: edges}, join
}

func typeOfRaw(raw json.RawMessage) dsl.DataType {
	value, _ := decodeJSON(raw)
	switch value.(type) {
	case string:
		return dsl.TypeString
	case bool:
		return dsl.TypeBoolean
	case json.Number:
		if bytesContainsDecimal(raw) {
			return dsl.TypeNumber
		}
		return dsl.TypeInteger
	case []any:
		return dsl.TypeArray
	default:
		return dsl.TypeObject
	}
}
func bytesContainsDecimal(raw []byte) bool {
	for _, b := range raw {
		if b == '.' || b == 'e' || b == 'E' {
			return true
		}
	}
	return false
}

func layeredDAG(random *rand.Rand, width, layers int) dsl.Document {
	start := nid(1)
	nodes := []dsl.Node{startNode(start)}
	edges := []dsl.Edge{}
	previous := []dsl.NodeID{start}
	nextIndex, edgeIndex := 2, 1
	for layer := 0; layer < layers; layer++ {
		current := make([]dsl.NodeID, width)
		for i := range current {
			current[i] = nid(nextIndex)
			nextIndex++
			nodes = append(nodes, taskNode(current[i], dsl.ExecutionPolicy{}))
			for _, source := range previous {
				if random.Intn(2) == 0 {
					edges = append(edges, edge(eid(edgeIndex), source, current[i]))
					edgeIndex++
				}
			}
			if noIncoming(current[i], edges) {
				source := previous[random.Intn(len(previous))]
				edges = append(edges, edge(eid(edgeIndex), source, current[i]))
				edgeIndex++
			}
		}
		for _, source := range previous {
			if noOutgoing(source, edges) {
				target := current[random.Intn(len(current))]
				edges = append(edges, edge(eid(edgeIndex), source, target))
				edgeIndex++
			}
		}
		previous = current
	}
	end := nid(nextIndex)
	nodes = append(nodes, endNode(end, dsl.InputBinding{Kind: dsl.BindingLiteral, DataType: dsl.TypeObject, Value: json.RawMessage(`{}`)}))
	for _, source := range previous {
		edges = append(edges, edge(eid(edgeIndex), source, end))
		edgeIndex++
	}
	return dsl.Document{DSLVersion: dsl.VersionV1, EntryNodeID: start, ExitNodeID: end, Nodes: nodes, Edges: edges}
}
func noIncoming(target dsl.NodeID, edges []dsl.Edge) bool {
	for _, edge := range edges {
		if edge.TargetNodeID == target {
			return false
		}
	}
	return true
}

func noOutgoing(source dsl.NodeID, edges []dsl.Edge) bool {
	for _, edge := range edges {
		if edge.SourceNodeID == source {
			return false
		}
	}
	return true
}
