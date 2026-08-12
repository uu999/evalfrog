package runtime

import (
	"encoding/json"
	"testing"
	"time"
)

func TestPersistenceRecordsRoundTripAndRejectCorruption(t *testing.T) {
	now := time.Now().UTC()
	run, err := NewWorkflowRun(CreateRunCommand{RunID: "run", ProjectID: "project", WorkflowID: "workflow", Purpose: RunPurposeTest, Definition: DefinitionReference{SnapshotID: "snapshot", DefinitionHash: "hash", Source: DefinitionDraftSnapshot}, WorkflowInput: json.RawMessage(`{}`), CreatedAt: now, DeadlineAt: now.Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if err = run.Start([]string{"start", "end"}); err != nil {
		t.Fatal(err)
	}
	restoredRun, err := RestoreWorkflowRun(run.Snapshot())
	if err != nil || restoredRun.State() != RunRunning || restoredRun.NodeRunCount() != 2 {
		t.Fatalf("restored run=%+v err=%v", restoredRun, err)
	}
	node, _ := NewNodeRun("run", "task", NodeTask)
	_ = node.Activate()
	_ = node.Ready(map[string]json.RawMessage{"input": json.RawMessage(`1`)})
	sequence, kind, _ := node.QueueAttempt("attempt")
	_ = node.AttemptStarted("attempt")
	attemptValue, _ := NewNodeAttempt("attempt", "run:task", sequence, kind)
	_ = attemptValue.Start()
	result := AttemptResult{State: AttemptSucceeded, Outputs: map[string]json.RawMessage{"result": json.RawMessage(`{}`)}}
	_ = attemptValue.Complete(result)
	if _, err = RestoreNodeRun(node.Snapshot()); err != nil {
		t.Fatal(err)
	}
	if restoredAttempt, restoreErr := RestoreNodeAttempt(attemptValue.Snapshot()); restoreErr != nil || restoredAttempt.State() != AttemptSucceeded {
		t.Fatalf("attempt=%+v err=%v", restoredAttempt, restoreErr)
	}

	badRun := run.Snapshot()
	badRun.StateVersion = 0
	if _, err = RestoreWorkflowRun(badRun); err == nil {
		t.Fatal("zero run version accepted")
	}
	badNode := node.Snapshot()
	badNode.State = "invalid"
	if _, err = RestoreNodeRun(badNode); err == nil {
		t.Fatal("invalid node state accepted")
	}
	badAttempt := attemptValue.Snapshot()
	badAttempt.Result = nil
	if _, err = RestoreNodeAttempt(badAttempt); err == nil {
		t.Fatal("terminal attempt without result accepted")
	}
}

func TestRestoreAllowsPendingAndInitializationFailureOnlyWithValidFacts(t *testing.T) {
	now := time.Now().UTC()
	command := CreateRunCommand{RunID: "run", ProjectID: "project", WorkflowID: "workflow", Purpose: RunPurposeTest, Definition: DefinitionReference{SnapshotID: "snapshot", DefinitionHash: "hash", Source: DefinitionDraftSnapshot}, WorkflowInput: json.RawMessage(`{}`), CreatedAt: now, DeadlineAt: now.Add(time.Hour)}
	run, _ := NewWorkflowRun(command)
	if _, err := RestoreWorkflowRun(run.Snapshot()); err != nil {
		t.Fatal(err)
	}
	_ = run.FailInitialization(Failure{Code: "RUNTIME_DSL_INVALID"}, now)
	if restored, err := RestoreWorkflowRun(run.Snapshot()); err != nil || restored.State() != RunFailed {
		t.Fatalf("restored=%+v err=%v", restored, err)
	}
}
