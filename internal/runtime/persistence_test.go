package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/uu999/evalfrog/internal/access"
	"github.com/uu999/evalfrog/internal/platform/clock"
)

type runIDs struct {
	values []string
	err    error
}

func (ids *runIDs) New() (string, error) {
	if ids.err != nil {
		return "", ids.err
	}
	value := ids.values[0]
	ids.values = ids.values[1:]
	return value, nil
}

type runAccess struct{ err error }

func (value runAccess) Require(context.Context, string, string, access.Permission) error {
	return value.err
}

type runRepository struct {
	record CreatePendingRunRecord
	result WorkflowRunRecord
	err    error
}

func (repository *runRepository) CreatePendingRun(_ context.Context, record CreatePendingRunRecord) (WorkflowRunRecord, error) {
	repository.record = record
	return repository.result, repository.err
}

func TestRunCreatorBuildsTestAndProductionRecords(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 123456789, time.UTC)
	repository := &runRepository{result: WorkflowRunRecord{ID: "run"}}
	creator, err := NewRunCreator(repository, runAccess{}, &runIDs{values: []string{"run", "event", "run2", "event2"}}, clock.NewFake(now))
	if err != nil {
		t.Fatal(err)
	}
	command := TestDraftRunCommand{
		ProjectID: "project", PrincipalID: "principal", WorkflowID: "workflow", SnapshotID: "snapshot", DraftRevisionNumber: 3,
		WorkflowInput: json.RawMessage(`{"value":1}`), DeadlineAt: now.Add(time.Hour), IdempotencyKey: "test-key-1", TraceID: "trace",
	}
	if _, err = creator.TestDraft(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	if repository.record.Purpose != RunPurposeTest || repository.record.RunID != "run" || repository.record.EventID != "event" || repository.record.DraftRevisionNumber != 3 || repository.record.RequestHash == "" || repository.record.CreatedAt.Nanosecond()%1000 != 0 {
		t.Fatalf("test record=%+v", repository.record)
	}
	if _, err = creator.CreateProduction(context.Background(), ProductionRunCommand{
		ProjectID: "project", PrincipalID: "principal", WorkflowID: "workflow", WorkflowInput: json.RawMessage(`{}`),
		DeadlineAt: now.Add(time.Hour), IdempotencyKey: "production-key", TraceID: "trace",
	}); err != nil || repository.record.Purpose != RunPurposeProduction || repository.record.SnapshotID != "" {
		t.Fatalf("production record=%+v err=%v", repository.record, err)
	}
}

func TestRunCreatorRejectsInvalidCommandsPermissionsAndIDFailures(t *testing.T) {
	if _, err := NewRunCreator(nil, runAccess{}, &runIDs{}, clock.NewFake(time.Now())); err == nil {
		t.Fatal("missing repository accepted")
	}
	repository := &runRepository{}
	permissionErr := errors.New("denied")
	creator, _ := NewRunCreator(repository, runAccess{err: permissionErr}, &runIDs{}, clock.NewFake(time.Now()))
	if _, err := creator.TestDraft(context.Background(), TestDraftRunCommand{}); !errors.Is(err, permissionErr) {
		t.Fatalf("permission error=%v", err)
	}
	now := time.Now().UTC()
	creator, _ = NewRunCreator(repository, runAccess{}, &runIDs{values: []string{"run", "event"}}, clock.NewFake(now))
	invalid := []TestDraftRunCommand{
		{},
		{ProjectID: "p", PrincipalID: "u", WorkflowID: "w", SnapshotID: "s", DraftRevisionNumber: 1, WorkflowInput: json.RawMessage(`[]`), DeadlineAt: now.Add(time.Hour), IdempotencyKey: "long-key", TraceID: "t"},
		{ProjectID: "p", PrincipalID: "u", WorkflowID: "w", SnapshotID: "s", DraftRevisionNumber: 1, WorkflowInput: json.RawMessage(`{}`), DeadlineAt: now, IdempotencyKey: "long-key", TraceID: "t"},
	}
	for _, command := range invalid {
		if _, err := creator.TestDraft(context.Background(), command); err == nil {
			t.Fatalf("invalid command accepted: %+v", command)
		}
	}
	creator, _ = NewRunCreator(repository, runAccess{}, &runIDs{err: errors.New("id failed")}, clock.NewFake(now))
	if _, err := creator.TestDraft(context.Background(), TestDraftRunCommand{ProjectID: "p", PrincipalID: "u", WorkflowID: "w", SnapshotID: "s", DraftRevisionNumber: 1, WorkflowInput: json.RawMessage(`{}`), DeadlineAt: now.Add(time.Hour), IdempotencyKey: "long-key", TraceID: "t"}); err == nil {
		t.Fatal("id failure hidden")
	}
	if value := NewBuiltinRunCreator(repository, runAccess{}); value.repository == nil {
		t.Fatal("builtin run creator not wired")
	}
}
