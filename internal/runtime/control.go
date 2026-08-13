package runtime

import (
	"context"
	"fmt"
	"time"

	"github.com/uu999/evalfrog/internal/access"
	"github.com/uu999/evalfrog/internal/platform/clock"
	"github.com/uu999/evalfrog/internal/platform/identity"
)

// CancelRunRecord is a transport-neutral command. The repository persists a
// durable RunCancelRequested Outbox event; it does not mutate Engine state.
type CancelRunRecord struct {
	ProjectID   string
	RunID       string
	PrincipalID string
	EventID     string
	TraceID     string
	RequestedAt time.Time
}

type RunControlRepository interface {
	RequestCancellation(context.Context, CancelRunRecord) (WorkflowRunRecord, bool, error)
}

type RunControl struct {
	repository RunControlRepository
	access     RunAccessControl
	ids        identity.Generator
	clock      clock.Clock
}

func NewRunControl(repository RunControlRepository, accessControl RunAccessControl, ids identity.Generator, valueClock clock.Clock) (RunControl, error) {
	if repository == nil || accessControl == nil || ids == nil || valueClock == nil {
		return RunControl{}, fmt.Errorf("run control dependencies are required")
	}
	return RunControl{repository: repository, access: accessControl, ids: ids, clock: valueClock}, nil
}

func NewBuiltinRunControl(repository RunControlRepository, accessControl RunAccessControl) RunControl {
	value, err := NewRunControl(repository, accessControl, identity.UUIDv7Generator{}, clock.System{})
	if err != nil {
		panic(err)
	}
	return value
}

// Cancel is idempotent by Run identity. Only its first accepted call creates
// the durable wake-up event; repeat calls observe the same authority state.
func (control RunControl) Cancel(ctx context.Context, projectID, principalID, runID, traceID string) (WorkflowRunRecord, bool, error) {
	if err := control.access.Require(ctx, projectID, principalID, access.PermissionRunCancel); err != nil {
		return WorkflowRunRecord{}, false, err
	}
	if projectID == "" || principalID == "" || runID == "" || traceID == "" {
		return WorkflowRunRecord{}, false, fmt.Errorf("%w: cancellation identity and trace are required", ErrInvalidRun)
	}
	eventID, err := control.ids.New()
	if err != nil {
		return WorkflowRunRecord{}, false, err
	}
	return control.repository.RequestCancellation(ctx, CancelRunRecord{
		ProjectID: projectID, RunID: runID, PrincipalID: principalID, EventID: eventID,
		TraceID: traceID, RequestedAt: control.clock.Now().UTC(),
	})
}
