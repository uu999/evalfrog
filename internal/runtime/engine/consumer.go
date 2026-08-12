package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/uu999/evalfrog/internal/dsl"
	"github.com/uu999/evalfrog/internal/eventing"
	"github.com/uu999/evalfrog/internal/runtime"
)

const ConsumerName = "runtime-engine-v1"

type TransactionManager interface {
	WithRunTransaction(context.Context, eventing.RuntimeEvent, func(RunTransaction) error) error
}

type RunTransaction interface {
	AcceptInbox(context.Context, string, eventing.RuntimeEvent) (bool, error)
	LoadRun(context.Context, string, string) (runtime.WorkflowRunRecord, error)
	LoadSnapshot(context.Context, string, string) (Snapshot, error)
	LoadEngineState(context.Context, string, string) (State, error)
	InitializeRun(context.Context, runtime.WorkflowRunRecord, State, time.Time) error
	AdvanceRun(context.Context, State, State, time.Time) error
	FailRunInitialization(context.Context, runtime.WorkflowRunRecord, runtime.WorkflowRunRecord, time.Time) error
}

type Consumer struct {
	transactions TransactionManager
}

func NewConsumer(transactions TransactionManager) (Consumer, error) {
	if transactions == nil {
		return Consumer{}, fmt.Errorf("engine transaction manager is required")
	}
	return Consumer{transactions: transactions}, nil
}

func (consumer Consumer) Consume(ctx context.Context, event eventing.RuntimeEvent) error {
	if err := event.Validate(); err != nil {
		return err
	}
	return consumer.transactions.WithRunTransaction(ctx, event, func(tx RunTransaction) error {
		accepted, err := tx.AcceptInbox(ctx, ConsumerName, event)
		if err != nil || !accepted {
			return err
		}
		switch event.EventType {
		case eventing.RunCreated:
			return consumer.initialize(ctx, tx, event)
		case eventing.AttemptCompleted, eventing.AttemptLost, eventing.RetryDue,
			eventing.RunCancelRequested, eventing.RunDeadlineReached:
			return consumer.advance(ctx, tx, event)
		default:
			return fmt.Errorf("runtime event type %q is unsupported by engine", event.EventType)
		}
	})
}

func (consumer Consumer) initialize(ctx context.Context, tx RunTransaction, event eventing.RuntimeEvent) error {
	runRecord, err := tx.LoadRun(ctx, event.ProjectID, event.RunID)
	if err != nil {
		return err
	}
	if runRecord.State != runtime.RunPending {
		return nil
	}
	snapshot, err := tx.LoadSnapshot(ctx, event.ProjectID, runRecord.Definition.SnapshotID)
	if err != nil {
		return err
	}
	instance, err := NewBuiltinV1(snapshot, runtime.CreateRunCommand{
		RunID: runRecord.ID, ProjectID: runRecord.ProjectID, WorkflowID: runRecord.WorkflowID,
		Purpose: runRecord.Purpose, Definition: runRecord.Definition, WorkflowInput: runRecord.WorkflowInput,
		DeadlineAt: runRecord.DeadlineAt, CreatedAt: runRecord.CreatedAt,
	})
	if err != nil {
		code := "RUNTIME_DSL_INVALID"
		var runtimeError *Error
		if errors.As(err, &runtimeError) && runtimeError.Code != "" {
			code = runtimeError.Code
		}
		failure := runtime.Failure{
			Code: code, Phase: "run_initialization", Retryable: false,
			RunID: runRecord.ID, SnapshotID: runRecord.Definition.SnapshotID,
			DefinitionHash: runRecord.Definition.DefinitionHash, Message: err.Error(),
		}
		if runtimeError != nil {
			failure.ExecutionNodeID = runtimeError.NodeID
			failure.DSLField = runtimeError.Field
		}
		failed, restoreErr := runtime.RestoreWorkflowRun(runRecord)
		if restoreErr != nil {
			return restoreErr
		}
		if failErr := failed.FailInitialization(failure, event.OccurredAt); failErr != nil {
			return failErr
		}
		return tx.FailRunInitialization(ctx, runRecord, failed.Snapshot(), event.OccurredAt)
	}
	return tx.InitializeRun(ctx, runRecord, instance.SnapshotState(), event.OccurredAt)
}

func (consumer Consumer) advance(ctx context.Context, tx RunTransaction, event eventing.RuntimeEvent) error {
	before, err := tx.LoadEngineState(ctx, event.ProjectID, event.RunID)
	if err != nil {
		return err
	}
	if before.Run.State.Terminal() {
		return nil
	}
	snapshot, err := tx.LoadSnapshot(ctx, event.ProjectID, before.Run.Definition.SnapshotID)
	if err != nil {
		return err
	}
	instance, err := RestoreBuiltinV1(snapshot, before)
	if err != nil {
		return err
	}
	switch event.EventType {
	case eventing.AttemptCompleted, eventing.AttemptLost:
		err = instance.HandleAttemptCompleted(event.AggregateID, event.OccurredAt)
	case eventing.RetryDue:
		nodeID, exists := attemptNodeID(before, event.AggregateID)
		if !exists {
			return nil
		}
		err = instance.RetryDue(nodeID, event.OccurredAt)
	case eventing.RunCancelRequested:
		_, err = instance.RequestCancel(event.OccurredAt, "run cancellation requested")
	case eventing.RunDeadlineReached:
		_, err = instance.DeadlineReached(event.OccurredAt)
	}
	if err != nil {
		return err
	}
	return tx.AdvanceRun(ctx, before, instance.SnapshotState(), event.OccurredAt)
}

func attemptNodeID(state State, attemptID string) (dsl.NodeID, bool) {
	for _, attempt := range state.Attempts {
		if attempt.ID != attemptID {
			continue
		}
		for _, node := range state.Nodes {
			if attempt.NodeRunID == node.RunID+":"+node.ExecutionNodeID {
				return dsl.NodeID(node.ExecutionNodeID), true
			}
		}
	}
	return "", false
}
