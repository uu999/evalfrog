package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/eventing"
	"github.com/uu999/evalfrog/internal/runtime"
)

type fakeTransactions struct{ tx *fakeRunTx }

func (manager *fakeTransactions) WithRunTransaction(_ context.Context, _ eventing.RuntimeEvent, operation func(RunTransaction) error) error {
	return operation(manager.tx)
}

type fakeRunTx struct {
	accepted          bool
	run               runtime.WorkflowRunRecord
	snapshot          Snapshot
	state             State
	initialized       bool
	advanced          bool
	failedInit        bool
	loadRunError      error
	loadSnapshotError error
	loadStateError    error
	inboxError        error
	initializeError   error
	advanceError      error
	failInitError     error
}

func (tx *fakeRunTx) AcceptInbox(context.Context, string, eventing.RuntimeEvent) (bool, error) {
	return tx.accepted, tx.inboxError
}
func (tx *fakeRunTx) LoadRun(context.Context, string, string) (runtime.WorkflowRunRecord, error) {
	return tx.run, tx.loadRunError
}
func (tx *fakeRunTx) LoadSnapshot(context.Context, string, string) (Snapshot, error) {
	return tx.snapshot, tx.loadSnapshotError
}
func (tx *fakeRunTx) LoadEngineState(context.Context, string, string) (State, error) {
	return tx.state, tx.loadStateError
}
func (tx *fakeRunTx) InitializeRun(_ context.Context, _ runtime.WorkflowRunRecord, _ State, _ time.Time) error {
	tx.initialized = true
	return tx.initializeError
}
func (tx *fakeRunTx) AdvanceRun(_ context.Context, _, _ State, _ time.Time) error {
	tx.advanced = true
	return tx.advanceError
}
func (tx *fakeRunTx) FailRunInitialization(_ context.Context, _, _ runtime.WorkflowRunRecord, _ time.Time) error {
	tx.failedInit = true
	return tx.failInitError
}

func TestConsumerInitializesPendingRunAndDeduplicatesInbox(t *testing.T) {
	harness := newTestHarness(t, linearDocument(1))
	state := harness.Engine.SnapshotState()
	pending, err := runtime.NewWorkflowRun(runtime.CreateRunCommand{
		RunID: state.Run.ID, ProjectID: state.Run.ProjectID, WorkflowID: state.Run.WorkflowID,
		Purpose: state.Run.Purpose, Definition: state.Run.Definition, WorkflowInput: state.Run.WorkflowInput,
		CreatedAt: state.Run.CreatedAt, DeadlineAt: state.Run.DeadlineAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	tx := &fakeRunTx{accepted: true, run: pending.Snapshot(), snapshot: harness.Engine.snapshot}
	consumer, _ := NewConsumer(&fakeTransactions{tx})
	event := testRuntimeEvent(eventing.RunCreated, state.Run.ID, state.Run.ID, state.Run.CreatedAt)
	if err = consumer.Consume(context.Background(), event); err != nil || !tx.initialized {
		t.Fatalf("initialized=%v err=%v", tx.initialized, err)
	}
	tx.accepted, tx.initialized = false, false
	if err = consumer.Consume(context.Background(), event); err != nil || tx.initialized {
		t.Fatalf("duplicate initialized=%v err=%v", tx.initialized, err)
	}
}

func TestConsumerFailsUnsupportedInitializationAndRollsBackMissingFact(t *testing.T) {
	harness := newTestHarness(t, linearDocument(1))
	pending, _ := runtime.NewWorkflowRun(runtime.CreateRunCommand{
		RunID: "run", ProjectID: "project", WorkflowID: "workflow", Purpose: runtime.RunPurposeTest,
		Definition:    runtime.DefinitionReference{SnapshotID: "snapshot", DefinitionHash: "hash", Source: runtime.DefinitionDraftSnapshot},
		WorkflowInput: []byte(`{}`), CreatedAt: harness.Now(), DeadlineAt: harness.Now().Add(time.Hour),
	})
	unsupported := harness.Engine.snapshot
	unsupported.DSL.Nodes[1].Operation.Version = 99
	tx := &fakeRunTx{accepted: true, run: pending.Snapshot(), snapshot: unsupported}
	consumer, _ := NewConsumer(&fakeTransactions{tx})
	event := testRuntimeEvent(eventing.RunCreated, "run", "run", harness.Now())
	if err := consumer.Consume(context.Background(), event); err != nil || !tx.failedInit {
		t.Fatalf("failedInit=%v err=%v", tx.failedInit, err)
	}
	tx = &fakeRunTx{accepted: true, loadStateError: errors.New("attempt result missing")}
	consumer, _ = NewConsumer(&fakeTransactions{tx})
	event = testRuntimeEvent(eventing.AttemptCompleted, "run", "attempt", harness.Now())
	if err := consumer.Consume(context.Background(), event); err == nil {
		t.Fatal("missing authoritative attempt fact accepted")
	}
}

func TestConsumerIgnoresTerminalRunEvent(t *testing.T) {
	harness := newTestHarness(t, linearDocument(1))
	completeAllReady(t, harness)
	tx := &fakeRunTx{accepted: true, state: harness.Engine.SnapshotState(), snapshot: harness.Engine.snapshot}
	consumer, _ := NewConsumer(&fakeTransactions{tx})
	event := testRuntimeEvent(eventing.RunDeadlineReached, "run", "run", harness.Now().Add(time.Hour))
	if err := consumer.Consume(context.Background(), event); err != nil || tx.advanced {
		t.Fatalf("advanced=%v err=%v", tx.advanced, err)
	}
}

func TestConsumerAdvancesCompletionRetryCancelAndDeadlineSignals(t *testing.T) {
	t.Run("attempt completed", func(t *testing.T) {
		harness := newTestHarness(t, linearDocument(1))
		attempts, _ := harness.StartReady()
		nodeID := harness.Engine.attemptNodes[attempts[0]]
		_, _ = harness.Engine.RecordAttemptResult(attempts[0], runtime.AttemptResult{State: runtime.AttemptSucceeded, Outputs: outputsFor(harness.Engine.nodeDefs[nodeID], 1)})
		tx := &fakeRunTx{accepted: true, state: harness.Engine.SnapshotState(), snapshot: harness.Engine.snapshot}
		consumer, _ := NewConsumer(&fakeTransactions{tx})
		event := testRuntimeEvent(eventing.AttemptCompleted, "run", attempts[0], harness.Now())
		if err := consumer.Consume(context.Background(), event); err != nil || !tx.advanced {
			t.Fatalf("advanced=%v err=%v", tx.advanced, err)
		}
	})
	t.Run("retry due", func(t *testing.T) {
		harness := newTestHarness(t, linearDocumentWithPolicy(1, dsl.ExecutionPolicy{MaxAttempts: 2, MaxRecoveries: 1, AttemptTimeoutMS: 1000, RetryBackoff: &dsl.RetryBackoff{Kind: "fixed", DelayMS: 10}, RetryableErrorCodes: []string{"TEMP"}}))
		attempts, _ := harness.StartReady()
		nodeID := harness.Engine.attemptNodes[attempts[0]]
		_, _ = harness.Engine.RecordAttemptResult(attempts[0], runtime.AttemptResult{State: runtime.AttemptFailed, ErrorCode: "TEMP"})
		_ = harness.Engine.HandleAttemptCompleted(attempts[0], harness.Now())
		tx := &fakeRunTx{accepted: true, state: harness.Engine.SnapshotState(), snapshot: harness.Engine.snapshot}
		consumer, _ := NewConsumer(&fakeTransactions{tx})
		event := testRuntimeEvent(eventing.RetryDue, "run", attempts[0], harness.Now().Add(10*time.Millisecond))
		if err := consumer.Consume(context.Background(), event); err != nil || !tx.advanced {
			t.Fatalf("node=%s advanced=%v err=%v", nodeID, tx.advanced, err)
		}
	})
	for _, signal := range []eventing.RuntimeEventType{eventing.RunCancelRequested, eventing.RunDeadlineReached} {
		t.Run(string(signal), func(t *testing.T) {
			harness := newTestHarness(t, linearDocument(1))
			tx := &fakeRunTx{accepted: true, state: harness.Engine.SnapshotState(), snapshot: harness.Engine.snapshot}
			consumer, _ := NewConsumer(&fakeTransactions{tx})
			at := harness.Now()
			if signal == eventing.RunDeadlineReached {
				at = harness.Engine.Run().DeadlineAt()
			}
			event := testRuntimeEvent(signal, "run", "run", at)
			if err := consumer.Consume(context.Background(), event); err != nil || !tx.advanced {
				t.Fatalf("advanced=%v err=%v", tx.advanced, err)
			}
		})
	}
}

func TestConsumerValidatesDependencyEventAndUnknownAttempt(t *testing.T) {
	if _, err := NewConsumer(nil); err == nil {
		t.Fatal("consumer without transactions accepted")
	}
	consumer, _ := NewConsumer(&fakeTransactions{&fakeRunTx{}})
	if err := consumer.Consume(context.Background(), eventing.RuntimeEvent{}); err == nil {
		t.Fatal("invalid event accepted")
	}
	harness := newTestHarness(t, linearDocument(1))
	tx := &fakeRunTx{accepted: true, state: harness.Engine.SnapshotState(), snapshot: harness.Engine.snapshot}
	consumer, _ = NewConsumer(&fakeTransactions{tx})
	event := testRuntimeEvent(eventing.RetryDue, "run", "missing", harness.Now())
	if err := consumer.Consume(context.Background(), event); err != nil || tx.advanced {
		t.Fatalf("unknown retry advanced=%v err=%v", tx.advanced, err)
	}
	tx = &fakeRunTx{accepted: true, state: harness.Engine.SnapshotState(), snapshot: harness.Engine.snapshot}
	consumer, _ = NewConsumer(&fakeTransactions{tx})
	unknownType := testRuntimeEvent(eventing.RuntimeEventType("unknown"), "run", "run", harness.Now())
	if err := consumer.Consume(context.Background(), unknownType); err == nil {
		t.Fatal("unknown event type accepted")
	}
}

func TestConsumerPropagatesTransactionAndStorageErrors(t *testing.T) {
	harness := newTestHarness(t, linearDocument(1))
	pending, _ := runtime.NewWorkflowRun(runtime.CreateRunCommand{
		RunID: "run", ProjectID: "project", WorkflowID: "workflow", Purpose: runtime.RunPurposeTest,
		Definition:    runtime.DefinitionReference{SnapshotID: "snapshot", DefinitionHash: "hash", Source: runtime.DefinitionDraftSnapshot},
		WorkflowInput: []byte(`{}`), CreatedAt: harness.Now(), DeadlineAt: harness.Now().Add(time.Hour),
	})
	event := testRuntimeEvent(eventing.RunCreated, "run", "run", harness.Now())
	for _, tx := range []*fakeRunTx{
		{accepted: true, loadRunError: errors.New("load run")},
		{accepted: true, run: pending.Snapshot(), loadSnapshotError: errors.New("load snapshot")},
	} {
		consumer, _ := NewConsumer(&fakeTransactions{tx})
		if err := consumer.Consume(context.Background(), event); err == nil {
			t.Fatal("invalid authoritative facts were accepted")
		}
	}
	tx := &fakeRunTx{accepted: true, loadStateError: errors.New("load state")}
	consumer, _ := NewConsumer(&fakeTransactions{tx})
	if err := consumer.Consume(context.Background(), testRuntimeEvent(eventing.RunDeadlineReached, "run", "run", harness.Now())); err == nil {
		t.Fatal("state load error was hidden")
	}
	for _, tx = range []*fakeRunTx{
		{accepted: true, inboxError: errors.New("inbox")},
		{accepted: true, run: pending.Snapshot(), snapshot: harness.Engine.snapshot, initializeError: errors.New("initialize")},
	} {
		consumer, _ = NewConsumer(&fakeTransactions{tx})
		if err := consumer.Consume(context.Background(), event); err == nil {
			t.Fatal("transaction error was hidden")
		}
	}
	unsupported := harness.Engine.snapshot
	unsupported.DSL.Nodes = append([]dsl.Node(nil), harness.Engine.snapshot.DSL.Nodes...)
	unsupported.DSL.Nodes[1].Operation.Version = 99
	tx = &fakeRunTx{accepted: true, run: pending.Snapshot(), snapshot: unsupported, failInitError: errors.New("persist failure")}
	consumer, _ = NewConsumer(&fakeTransactions{tx})
	if err := consumer.Consume(context.Background(), event); err == nil {
		t.Fatal("initialization failure persistence error was hidden")
	}
	state := harness.Engine.SnapshotState()
	tx = &fakeRunTx{accepted: true, state: state, loadSnapshotError: errors.New("load snapshot")}
	consumer, _ = NewConsumer(&fakeTransactions{tx})
	deadline := testRuntimeEvent(eventing.RunDeadlineReached, "run", "run", harness.Engine.Run().DeadlineAt())
	if err := consumer.Consume(context.Background(), deadline); err == nil {
		t.Fatal("advance snapshot error was hidden")
	}
	tx = &fakeRunTx{accepted: true, state: state, snapshot: harness.Engine.snapshot, advanceError: errors.New("advance")}
	consumer, _ = NewConsumer(&fakeTransactions{tx})
	if err := consumer.Consume(context.Background(), deadline); err == nil || !tx.advanced {
		t.Fatalf("advance persistence error was hidden: advanced=%v err=%v", tx.advanced, err)
	}
}

func testRuntimeEvent(eventType eventing.RuntimeEventType, runID, aggregateID string, at time.Time) eventing.RuntimeEvent {
	aggregateType := eventing.NodeAttemptAggregate
	if eventType == eventing.RunCreated || eventType == eventing.RunCancelRequested || eventType == eventing.RunDeadlineReached {
		aggregateType = eventing.WorkflowRunAggregate
	}
	return eventing.RuntimeEvent{MessageVersion: 1, EventID: "event", ProjectID: "project", RunID: runID, AggregateType: aggregateType, AggregateID: aggregateID, EventType: eventType, OccurredAt: at, TraceID: "trace"}
}
