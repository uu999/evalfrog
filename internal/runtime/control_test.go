package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/uu999/evalfrog/internal/access"
	"github.com/uu999/evalfrog/internal/platform/clock"
)

type cancellationRepositoryStub struct {
	record  CancelRunRecord
	run     WorkflowRunRecord
	applied bool
	err     error
}

func (stub *cancellationRepositoryStub) RequestCancellation(_ context.Context, record CancelRunRecord) (WorkflowRunRecord, bool, error) {
	stub.record = record
	return stub.run, stub.applied, stub.err
}

type runAccessStub struct{ err error }

func (stub runAccessStub) Require(context.Context, string, string, access.Permission) error {
	return stub.err
}

type sequenceIDGenerator struct{ values []string }

func (generator *sequenceIDGenerator) New() (string, error) {
	if len(generator.values) == 0 {
		return "", errors.New("no test identifiers left")
	}
	value := generator.values[0]
	generator.values = generator.values[1:]
	return value, nil
}

func TestRunControlPersistsOnlyCancellationIntentAfterAuthorization(t *testing.T) {
	now := time.Date(2026, 8, 13, 9, 0, 0, 0, time.UTC)
	repository := &cancellationRepositoryStub{run: WorkflowRunRecord{ID: "run-1"}, applied: true}
	control, err := NewRunControl(repository, runAccessStub{}, &sequenceIDGenerator{values: []string{"event-1"}}, clock.NewFake(now))
	if err != nil {
		t.Fatal(err)
	}
	result, applied, err := control.Cancel(context.Background(), "project-1", "principal-1", "run-1", "trace-1")
	if err != nil || !applied || result.ID != "run-1" {
		t.Fatalf("result=%+v applied=%v err=%v", result, applied, err)
	}
	if repository.record.EventID != "event-1" || repository.record.RequestedAt != now || repository.record.TraceID != "trace-1" {
		t.Fatalf("unexpected durable command: %+v", repository.record)
	}
}

func TestRunControlDoesNotPersistWhenPermissionOrIdentityIsInvalid(t *testing.T) {
	for _, value := range []struct {
		name   string
		access error
		trace  string
	}{
		{name: "permission", access: access.ErrPermissionDenied, trace: "trace-1"},
		{name: "identity", trace: ""},
	} {
		t.Run(value.name, func(t *testing.T) {
			repository := &cancellationRepositoryStub{}
			control, err := NewRunControl(repository, runAccessStub{err: value.access}, &sequenceIDGenerator{values: []string{"event-1"}}, clock.NewFake(time.Now()))
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = control.Cancel(context.Background(), "project-1", "principal-1", "run-1", value.trace)
			if err == nil || repository.record.EventID != "" {
				t.Fatalf("err=%v command=%+v", err, repository.record)
			}
		})
	}
}
