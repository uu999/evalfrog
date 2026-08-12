package engine

import (
	"encoding/json"
	"testing"

	"github.com/uu999/evalfrog/internal/runtime"
)

func TestSnapshotRestorePreservesAggregateAndCanContinue(t *testing.T) {
	harness := newTestHarness(t, linearDocument(2))
	attempts, err := harness.StartReady()
	if err != nil || len(attempts) != 1 {
		t.Fatalf("attempts=%v err=%v", attempts, err)
	}
	nodeID := harness.Engine.attemptNodes[attempts[0]]
	if _, err = harness.Engine.RecordAttemptResult(attempts[0], runtime.AttemptResult{State: runtime.AttemptSucceeded, Outputs: outputsFor(harness.Engine.nodeDefs[nodeID], 1)}); err != nil {
		t.Fatal(err)
	}
	state := harness.Engine.SnapshotState()
	restored, err := RestoreBuiltinV1(harness.Engine.snapshot, state)
	if err != nil {
		t.Fatal(err)
	}
	if err = restored.HandleAttemptCompleted(attempts[0], harness.Now()); err != nil {
		t.Fatal(err)
	}
	next := restored.ReadyNodeIDs()
	if len(next) != 1 {
		t.Fatalf("ready after restore=%v", next)
	}
	if _, err = restored.QueueNode(next[0], "attempt-next"); err != nil {
		t.Fatal(err)
	}
	if err = restored.StartAttempt("attempt-next"); err != nil {
		t.Fatal(err)
	}
	if accepted, err := restored.RecordAttemptResult("attempt-next", runtime.AttemptResult{State: runtime.AttemptSucceeded, Outputs: map[string]json.RawMessage{"result": json.RawMessage(`{"done":true}`)}}); err != nil || !accepted {
		t.Fatalf("accepted=%v err=%v", accepted, err)
	}
	if err = restored.HandleAttemptCompleted("attempt-next", harness.Now()); err != nil || restored.Run().State() != runtime.RunSucceeded {
		t.Fatalf("run=%s err=%v", restored.Run().State(), err)
	}
}

func TestRestoreRejectsIncompleteOrCrossBoundState(t *testing.T) {
	harness := newTestHarness(t, linearDocument(1))
	state := harness.Engine.SnapshotState()
	state.Nodes = state.Nodes[:1]
	if _, err := RestoreBuiltinV1(harness.Engine.snapshot, state); err == nil {
		t.Fatal("incomplete node set accepted")
	}
	state = harness.Engine.SnapshotState()
	state.Run.Definition.SnapshotID = "other"
	if _, err := RestoreBuiltinV1(harness.Engine.snapshot, state); err == nil {
		t.Fatal("cross-bound snapshot accepted")
	}
}

func TestRestoreRejectsCorruptPersistedIdentitySets(t *testing.T) {
	harness := newTestHarness(t, linearDocument(1))
	attempts, err := harness.StartReady()
	if err != nil {
		t.Fatal(err)
	}
	base := harness.Engine.SnapshotState()
	tests := []struct {
		name   string
		mutate func(*State)
	}{
		{"invalid run", func(state *State) { state.Run.StateVersion = 0 }},
		{"unknown node", func(state *State) { state.Nodes[0].ExecutionNodeID = "xn_ffffffffffffffffffffffff" }},
		{"wrong node run", func(state *State) { state.Nodes[0].RunID = "other" }},
		{"invalid node", func(state *State) { state.Nodes[0].StateVersion = 0 }},
		{"duplicate node", func(state *State) { state.Nodes[1].ExecutionNodeID = state.Nodes[0].ExecutionNodeID }},
		{"invalid attempt", func(state *State) { state.Attempts[0].StateVersion = 0 }},
		{"orphan attempt", func(state *State) { state.Attempts[0].NodeRunID = "run:unknown" }},
		{"duplicate attempt", func(state *State) { state.Attempts = append(state.Attempts, state.Attempts[0]) }},
		{"missing current attempt", func(state *State) { state.Attempts = nil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state := base
			state.Nodes = append([]runtime.NodeRunRecord(nil), base.Nodes...)
			state.Attempts = append([]runtime.NodeAttemptRecord(nil), base.Attempts...)
			test.mutate(&state)
			if _, err := RestoreBuiltinV1(harness.Engine.snapshot, state); err == nil {
				t.Fatal("corrupt state was restored")
			}
		})
	}
	_ = attempts
}
