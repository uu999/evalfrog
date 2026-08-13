package runtime

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestRunTransitionMatrix(t *testing.T) {
	states := []RunState{RunPending, RunRunning, RunSucceeded, RunFailed, RunCanceled, RunTimedOut}
	allowed := map[[2]RunState]bool{
		{RunPending, RunRunning}: true, {RunPending, RunCanceled}: true, {RunPending, RunTimedOut}: true,
		{RunRunning, RunSucceeded}: true, {RunRunning, RunFailed}: true,
		{RunRunning, RunCanceled}: true, {RunRunning, RunTimedOut}: true,
	}
	for _, from := range states {
		for _, to := range states {
			if validRunTransition(from, to) != allowed[[2]RunState{from, to}] {
				t.Fatalf("run transition %s -> %s mismatch", from, to)
			}
		}
	}
}

func TestNodeTransitionMatrix(t *testing.T) {
	states := []NodeState{NodePending, NodeReady, NodeQueued, NodeRunning, NodeRetryWait, NodeSucceeded, NodeFailed, NodeTimedOut, NodeSkipped, NodeCanceled}
	taskAllowed := map[[2]NodeState]bool{
		{NodePending, NodeReady}: true, {NodePending, NodeFailed}: true, {NodePending, NodeSkipped}: true, {NodePending, NodeCanceled}: true,
		{NodeReady, NodeQueued}: true, {NodeReady, NodeCanceled}: true,
		{NodeQueued, NodeRunning}: true, {NodeQueued, NodeCanceled}: true,
		{NodeRunning, NodeRetryWait}: true, {NodeRunning, NodeSucceeded}: true, {NodeRunning, NodeFailed}: true,
		{NodeRunning, NodeTimedOut}: true, {NodeRunning, NodeCanceled}: true,
		{NodeRetryWait, NodeReady}: true, {NodeRetryWait, NodeCanceled}: true,
	}
	controlAllowed := map[[2]NodeState]bool{
		{NodePending, NodeSucceeded}: true, {NodePending, NodeFailed}: true,
		{NodePending, NodeSkipped}: true, {NodePending, NodeCanceled}: true,
	}
	for _, from := range states {
		for _, to := range states {
			pair := [2]NodeState{from, to}
			if validNodeTransition(NodeTask, from, to) != taskAllowed[pair] {
				t.Fatalf("task transition %s -> %s mismatch", from, to)
			}
			if validNodeTransition(NodeControl, from, to) != controlAllowed[pair] {
				t.Fatalf("control transition %s -> %s mismatch", from, to)
			}
		}
	}
}

func TestAttemptTransitionMatrix(t *testing.T) {
	states := []AttemptState{AttemptQueued, AttemptRunning, AttemptSucceeded, AttemptFailed, AttemptTimedOut, AttemptCanceled, AttemptLost}
	allowed := map[[2]AttemptState]bool{
		{AttemptQueued, AttemptRunning}: true, {AttemptQueued, AttemptCanceled}: true,
		{AttemptRunning, AttemptSucceeded}: true, {AttemptRunning, AttemptFailed}: true,
		{AttemptRunning, AttemptTimedOut}: true, {AttemptRunning, AttemptCanceled}: true,
		{AttemptRunning, AttemptLost}: true,
	}
	for _, from := range states {
		for _, to := range states {
			if validAttemptTransition(from, to) != allowed[[2]AttemptState{from, to}] {
				t.Fatalf("attempt transition %s -> %s mismatch", from, to)
			}
		}
	}
}

func TestCreateRunDefinitionSourceInvariant(t *testing.T) {
	command := validRunCommand()
	command.Purpose = RunPurposeProduction
	command.Definition.Source = DefinitionDraftSnapshot
	command.Definition.PublishedVersionID = ""
	if _, err := NewWorkflowRun(command); !errors.Is(err, ErrInvalidRun) {
		t.Fatalf("expected production/draft invariant error, got %v", err)
	}
	command.Purpose = RunPurposeTest
	if _, err := NewWorkflowRun(command); err != nil {
		t.Fatalf("test run should accept draft snapshot: %v", err)
	}
	command.Definition.PublishedVersionID = "version-invalid"
	if _, err := NewWorkflowRun(command); !errors.Is(err, ErrInvalidRun) {
		t.Fatalf("draft source accepted version identity: %v", err)
	}
}

func TestControlNodeNeverCreatesAttemptAndActivatedNeverSkipped(t *testing.T) {
	control, _ := NewNodeRun("run", "start", NodeControl)
	if _, _, err := control.QueueAttempt("attempt"); !errors.Is(err, ErrControlAttempt) {
		t.Fatalf("expected control attempt rejection, got %v", err)
	}
	task, _ := NewNodeRun("run", "task", NodeTask)
	if task.ExecutionIdempotencyKey() != "run:task" {
		t.Fatalf("execution idempotency key=%s", task.ExecutionIdempotencyKey())
	}
	if err := task.Activate(); err != nil {
		t.Fatal(err)
	}
	if err := task.Skip(); !errors.Is(err, ErrActivatedNodeSkipped) {
		t.Fatalf("expected activated skip rejection, got %v", err)
	}
}

func TestFirstTerminationIntentWins(t *testing.T) {
	run, err := NewWorkflowRun(validRunCommand())
	if err != nil {
		t.Fatal(err)
	}
	if err := run.Start([]string{"task", "other"}); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_700_000_001, 0).UTC()
	first := TerminationIntent{Kind: TerminationCanceled, RequestedAt: now, Cause: Failure{Code: "RUN_CANCELED"}}
	if applied, err := run.RequestTermination(first); err != nil || !applied {
		t.Fatalf("first intent: applied=%v err=%v", applied, err)
	}
	second := TerminationIntent{Kind: TerminationTimedOut, RequestedAt: now.Add(time.Second), Cause: Failure{Code: "RUN_TIMED_OUT"}}
	if applied, err := run.RequestTermination(second); err != nil || applied {
		t.Fatalf("second intent: applied=%v err=%v", applied, err)
	}
	actual, _ := run.Termination()
	if actual.Kind != TerminationCanceled {
		t.Fatalf("first intent was overwritten: %+v", actual)
	}
}

func TestPendingRunOnlyAcceptsCancellationOrDeadlineTimeout(t *testing.T) {
	run, _ := NewWorkflowRun(validRunCommand())
	if applied, err := run.RequestTermination(TerminationIntent{Kind: TerminationFailed, RequestedAt: run.CreatedAt().Add(time.Second), Cause: Failure{Code: "INVALID"}}); err == nil || applied {
		t.Fatalf("pending run accepted failure: applied=%v err=%v", applied, err)
	}
	if applied, err := run.RequestTermination(TerminationIntent{Kind: TerminationTimedOut, RequestedAt: run.DeadlineAt().Add(-time.Nanosecond), Cause: Failure{Code: "INVALID"}}); err == nil || applied {
		t.Fatalf("pending run accepted early timeout: applied=%v err=%v", applied, err)
	}
	if applied, err := run.RequestTermination(TerminationIntent{Kind: TerminationTimedOut, RequestedAt: run.DeadlineAt(), Cause: Failure{Code: "RUN_TIMED_OUT"}}); err != nil || !applied {
		t.Fatalf("pending run rejected deadline timeout: applied=%v err=%v", applied, err)
	}
	if err := run.CompleteTermination(nil); err != nil {
		t.Fatal(err)
	}
	if restored, err := RestoreWorkflowRun(run.Snapshot()); err != nil || restored.State() != RunTimedOut {
		t.Fatalf("uninitialized timed out run was not restorable: run=%+v err=%v", restored, err)
	}
}

func TestCompleteTerminationRequiresAllNodesTerminal(t *testing.T) {
	run, _ := NewWorkflowRun(validRunCommand())
	if err := run.Start([]string{"task", "other"}); err != nil {
		t.Fatal(err)
	}
	_, _ = run.RequestTermination(TerminationIntent{Kind: TerminationCanceled, RequestedAt: time.Now(), Cause: Failure{Code: "RUN_CANCELED"}})
	node, _ := NewNodeRun(run.ID(), "task", NodeTask)
	other, _ := NewNodeRun(run.ID(), "other", NodeControl)
	if err := other.Cancel("RUN_CANCELED"); err != nil {
		t.Fatal(err)
	}
	if err := run.CompleteTermination([]*NodeRun{node, other}); err == nil {
		t.Fatal("termination completed with pending node")
	}
	if err := node.Cancel("RUN_CANCELED"); err != nil {
		t.Fatal(err)
	}
	if err := run.CompleteTermination([]*NodeRun{node, other}); err != nil {
		t.Fatal(err)
	}
}

func TestAttemptDuplicateCompletionIsIdempotentButConflictingReplayFails(t *testing.T) {
	attempt, _ := NewNodeAttempt("attempt", "node", 1, AttemptInitial)
	if err := attempt.Start(); err != nil {
		t.Fatal(err)
	}
	result := AttemptResult{State: AttemptSucceeded, Outputs: map[string]json.RawMessage{"result": json.RawMessage(`{"ok":true}`)}}
	if err := attempt.Complete(result); err != nil {
		t.Fatal(err)
	}
	version := attempt.StateVersion()
	if err := attempt.Complete(result); err != nil {
		t.Fatalf("identical replay failed: %v", err)
	}
	if attempt.StateVersion() != version {
		t.Fatal("identical replay changed state version")
	}
	if err := attempt.Complete(AttemptResult{State: AttemptFailed, ErrorCode: "LATE"}); !IsInvalidTransition(err) {
		t.Fatalf("conflicting replay should fail, got %v", err)
	}
}

func TestWorkflowSuccessPreconditionDoesNotPartiallyWrite(t *testing.T) {
	run, _ := NewWorkflowRun(validRunCommand())
	end, _ := NewNodeRun(run.ID(), "end", NodeControl)
	if err := end.Activate(); err != nil {
		t.Fatal(err)
	}
	if err := CompleteWorkflowSuccess(run, end, []*NodeRun{end}, json.RawMessage(`{"ok":true}`)); err == nil {
		t.Fatal("pending run must not succeed")
	}
	if run.State() != RunPending || end.State() != NodePending || len(run.WorkflowOutput()) != 0 {
		t.Fatal("failed success operation partially mutated state")
	}
}

func TestWorkflowSuccessRejectsFailedOrUnsettledNode(t *testing.T) {
	for _, state := range []NodeState{NodePending, NodeFailed, NodeCanceled, NodeTimedOut} {
		run, _ := NewWorkflowRun(validRunCommand())
		_ = run.Start([]string{"task", "end"})
		end, _ := NewNodeRun(run.ID(), "end", NodeControl)
		_ = end.Activate()
		node, _ := NewNodeRun(run.ID(), "task", NodeTask)
		node.state = state
		if err := CompleteWorkflowSuccess(run, end, []*NodeRun{node, end}, json.RawMessage(`{}`)); err == nil {
			t.Fatalf("success accepted node state %s", state)
		}
		if run.State() != RunRunning || end.State() != NodePending {
			t.Fatalf("state %s partially mutated success", state)
		}
	}
}

func TestWorkflowSuccessRejectsPartialOrDuplicateNodeSet(t *testing.T) {
	run, _ := NewWorkflowRun(validRunCommand())
	if err := run.Start([]string{"start", "task", "end"}); err != nil {
		t.Fatal(err)
	}
	start, _ := NewNodeRun(run.ID(), "start", NodeControl)
	_ = start.Activate()
	_ = start.SucceedControl("", nil)
	end, _ := NewNodeRun(run.ID(), "end", NodeControl)
	_ = end.Activate()
	if err := CompleteWorkflowSuccess(run, end, []*NodeRun{start, end}, json.RawMessage(`{}`)); err == nil {
		t.Fatal("partial node set was accepted")
	}
	if err := CompleteWorkflowSuccess(run, end, []*NodeRun{start, start, end}, json.RawMessage(`{}`)); err == nil {
		t.Fatal("duplicate node set was accepted")
	}
}

func validRunCommand() CreateRunCommand {
	created := time.Unix(1_700_000_000, 0).UTC()
	return CreateRunCommand{
		RunID: "run-1", ProjectID: "project-1", WorkflowID: "workflow-1", Purpose: RunPurposeTest,
		Definition:    DefinitionReference{SnapshotID: "snapshot-1", DefinitionHash: "hash-1", Source: DefinitionDraftSnapshot},
		WorkflowInput: json.RawMessage(`{"request":"ok"}`), CreatedAt: created, DeadlineAt: created.Add(time.Hour),
	}
}
