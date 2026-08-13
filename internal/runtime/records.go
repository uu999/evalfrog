package runtime

import (
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"time"
)

// WorkflowRunRecord is the persistence boundary for a WorkflowRun aggregate.
// It is data, not a mutation API: callers must Restore the aggregate and use
// explicit domain operations before writing a new record.
type WorkflowRunRecord struct {
	ID                string              `json:"run_id"`
	ProjectID         string              `json:"project_id"`
	WorkflowID        string              `json:"workflow_id"`
	Purpose           RunPurpose          `json:"purpose"`
	Definition        DefinitionReference `json:"definition"`
	WorkflowInput     json.RawMessage     `json:"workflow_input"`
	WorkflowOutput    json.RawMessage     `json:"workflow_output,omitempty"`
	DeadlineAt        time.Time           `json:"deadline_at"`
	CreatedAt         time.Time           `json:"created_at"`
	State             RunState            `json:"state"`
	StateVersion      uint64              `json:"state_version"`
	ExecutionNodeIDs  []string            `json:"execution_node_ids,omitempty"`
	Termination       *TerminationIntent  `json:"termination,omitempty"`
	CancelRequestedAt time.Time           `json:"cancel_requested_at,omitempty"`
}

type NodeRunRecord struct {
	RunID                string
	ExecutionNodeID      string
	Kind                 NodeKind
	State                NodeState
	StateVersion         uint64
	Activated            bool
	SelectedRoute        string
	ResolvedInputs       map[string]json.RawMessage
	EffectiveOutputs     map[string]json.RawMessage
	EffectiveAttemptID   string
	CurrentAttemptID     string
	NextAttemptSeq       uint32
	BusinessAttemptCount uint32
	RecoveryCount        uint32
	NextAttemptKind      RetryKind
	NextRetryAt          time.Time
	Failure              *Failure
	CancelReason         string
}

type NodeAttemptRecord struct {
	ID           string
	NodeRunID    string
	Sequence     uint32
	Kind         RetryKind
	State        AttemptState
	StateVersion uint64
	Result       *AttemptResult
}

func (run *WorkflowRun) Snapshot() WorkflowRunRecord {
	ids := make([]string, 0, len(run.executionNodeIDs))
	for id := range run.executionNodeIDs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return WorkflowRunRecord{
		ID: run.id, ProjectID: run.projectID, WorkflowID: run.workflowID,
		Purpose: run.purpose, Definition: run.definition,
		WorkflowInput: cloneRaw(run.workflowInput), WorkflowOutput: cloneRaw(run.workflowOutput),
		DeadlineAt: run.deadlineAt, CreatedAt: run.createdAt, State: run.state,
		StateVersion: run.stateVersion, ExecutionNodeIDs: ids,
		Termination: cloneTermination(run.termination),
	}
}

func RestoreWorkflowRun(record WorkflowRunRecord) (*WorkflowRun, error) {
	if record.ID == "" || record.ProjectID == "" || record.WorkflowID == "" || record.StateVersion == 0 {
		return nil, fmt.Errorf("%w: persisted run identity and version are required", ErrInvalidRun)
	}
	base, err := NewWorkflowRun(CreateRunCommand{
		RunID: record.ID, ProjectID: record.ProjectID, WorkflowID: record.WorkflowID,
		Purpose: record.Purpose, Definition: record.Definition,
		WorkflowInput: record.WorkflowInput, DeadlineAt: record.DeadlineAt, CreatedAt: record.CreatedAt,
	})
	if err != nil {
		return nil, err
	}
	if record.State != RunPending && record.State != RunRunning && !record.State.Terminal() {
		return nil, fmt.Errorf("%w: persisted run state is invalid", ErrInvalidRun)
	}
	if record.State == RunPending && len(record.ExecutionNodeIDs) != 0 {
		return nil, fmt.Errorf("%w: pending run cannot contain initialized nodes", ErrInvalidRun)
	}
	initializationFailed := record.State == RunFailed && len(record.ExecutionNodeIDs) == 0 && record.Termination != nil
	if record.State != RunPending && !initializationFailed && len(record.ExecutionNodeIDs) < 2 {
		return nil, fmt.Errorf("%w: initialized run requires its complete node identity set", ErrInvalidRun)
	}
	identities := make(map[string]struct{}, len(record.ExecutionNodeIDs))
	for _, id := range record.ExecutionNodeIDs {
		if id == "" {
			return nil, fmt.Errorf("%w: persisted execution node identity is empty", ErrInvalidRun)
		}
		if _, exists := identities[id]; exists {
			return nil, fmt.Errorf("%w: persisted execution node identity is duplicated", ErrInvalidRun)
		}
		identities[id] = struct{}{}
	}
	if len(record.WorkflowOutput) != 0 {
		output, outputErr := cloneJSONObject(record.WorkflowOutput)
		if outputErr != nil {
			return nil, fmt.Errorf("%w: workflow output: %v", ErrInvalidRun, outputErr)
		}
		base.workflowOutput = output
	}
	base.state = record.State
	base.stateVersion = record.StateVersion
	base.nodeRunCount = uint32(len(identities))
	base.executionNodeIDs = identities
	base.termination = cloneTermination(record.Termination)
	if record.State.Terminal() && record.State != RunSucceeded && base.termination == nil {
		return nil, fmt.Errorf("%w: unsuccessful terminal run requires termination intent", ErrInvalidRun)
	}
	if record.State == RunSucceeded && (base.termination != nil || len(base.workflowOutput) == 0) {
		return nil, fmt.Errorf("%w: succeeded run has invalid terminal facts", ErrInvalidRun)
	}
	return base, nil
}

func (node *NodeRun) Snapshot() NodeRunRecord {
	return NodeRunRecord{
		RunID: node.runID, ExecutionNodeID: node.executionNodeID, Kind: node.kind,
		State: node.state, StateVersion: node.stateVersion, Activated: node.activated,
		SelectedRoute: node.selectedRoute, ResolvedInputs: cloneValues(node.resolvedInputs),
		EffectiveOutputs: cloneValues(node.effectiveOutputs), EffectiveAttemptID: node.effectiveAttemptID,
		CurrentAttemptID: node.currentAttemptID, NextAttemptSeq: node.nextAttemptSeq,
		BusinessAttemptCount: node.businessAttemptCount, RecoveryCount: node.recoveryCount,
		NextAttemptKind: node.nextAttemptKind, NextRetryAt: node.nextRetryAt,
		Failure: cloneFailure(node.failure), CancelReason: node.cancelReason,
	}
}

func RestoreNodeRun(record NodeRunRecord) (*NodeRun, error) {
	node, err := NewNodeRun(record.RunID, record.ExecutionNodeID, record.Kind)
	if err != nil {
		return nil, err
	}
	if record.StateVersion == 0 || !validPersistedNodeState(record.State) {
		return nil, fmt.Errorf("invalid persisted node run state")
	}
	if record.State == NodeSkipped && record.Activated {
		return nil, ErrActivatedNodeSkipped
	}
	if record.EffectiveAttemptID != "" && record.State != NodeSucceeded {
		return nil, fmt.Errorf("only succeeded node can expose an effective attempt")
	}
	if record.Kind == NodeControl && (record.CurrentAttemptID != "" || record.EffectiveAttemptID != "" || record.NextAttemptSeq != 0) {
		return nil, ErrControlAttempt
	}
	if record.NextAttemptKind != AttemptInitial && record.NextAttemptKind != AttemptBusiness && record.NextAttemptKind != AttemptRecovery {
		return nil, fmt.Errorf("invalid persisted next attempt kind")
	}
	node.state = record.State
	node.stateVersion = record.StateVersion
	node.activated = record.Activated
	node.selectedRoute = record.SelectedRoute
	node.resolvedInputs = cloneValues(record.ResolvedInputs)
	node.effectiveOutputs = cloneValues(record.EffectiveOutputs)
	node.effectiveAttemptID = record.EffectiveAttemptID
	node.currentAttemptID = record.CurrentAttemptID
	node.nextAttemptSeq = record.NextAttemptSeq
	node.businessAttemptCount = record.BusinessAttemptCount
	node.recoveryCount = record.RecoveryCount
	node.nextAttemptKind = record.NextAttemptKind
	node.nextRetryAt = record.NextRetryAt
	node.failure = cloneFailure(record.Failure)
	node.cancelReason = record.CancelReason
	return node, nil
}

func (attempt *NodeAttempt) Snapshot() NodeAttemptRecord {
	var result *AttemptResult
	if attempt.result != nil {
		copy := cloneResult(*attempt.result)
		result = &copy
	}
	return NodeAttemptRecord{ID: attempt.id, NodeRunID: attempt.nodeRunID, Sequence: attempt.sequence,
		Kind: attempt.kind, State: attempt.state, StateVersion: attempt.stateVersion, Result: result}
}

func RestoreNodeAttempt(record NodeAttemptRecord) (*NodeAttempt, error) {
	attempt, err := NewNodeAttempt(record.ID, record.NodeRunID, record.Sequence, record.Kind)
	if err != nil {
		return nil, err
	}
	if record.StateVersion == 0 || !validPersistedAttemptState(record.State) {
		return nil, fmt.Errorf("invalid persisted attempt state")
	}
	if record.State.Terminal() != (record.Result != nil) {
		return nil, fmt.Errorf("persisted attempt terminal state and result disagree")
	}
	attempt.state = record.State
	attempt.stateVersion = record.StateVersion
	if record.Result != nil {
		copy := cloneResult(*record.Result)
		if copy.State != record.State {
			return nil, fmt.Errorf("persisted attempt result state disagrees with attempt")
		}
		attempt.result = &copy
	}
	return attempt, nil
}

func validPersistedNodeState(state NodeState) bool {
	switch state {
	case NodePending, NodeReady, NodeQueued, NodeRunning, NodeRetryWait, NodeSucceeded, NodeFailed, NodeTimedOut, NodeSkipped, NodeCanceled:
		return true
	default:
		return false
	}
}

func validPersistedAttemptState(state AttemptState) bool {
	switch state {
	case AttemptQueued, AttemptRunning, AttemptSucceeded, AttemptFailed, AttemptTimedOut, AttemptCanceled, AttemptLost:
		return true
	default:
		return false
	}
}

func cloneTermination(value *TerminationIntent) *TerminationIntent {
	if value == nil {
		return nil
	}
	copy := *value
	if cause := cloneFailure(&value.Cause); cause != nil {
		copy.Cause = *cause
	}
	return &copy
}

func cloneFailure(value *Failure) *Failure {
	if value == nil {
		return nil
	}
	copy := *value
	if value.Details != nil {
		copy.Details = maps.Clone(value.Details)
	}
	return &copy
}
